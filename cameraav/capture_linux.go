//go:build linux && (amd64 || arm64)

package cameraav

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"unsafe"

	"github.com/thesyncim/goav/av"
	"golang.org/x/sys/unix"
)

// Linux UAPI layouts below target the 64-bit little-endian ABIs. Single-planar
// VIDEO_CAPTURE includes contiguous NV12 and packed YUYV USB-camera formats.
// Multiplanar-only devices are refused instead of interpreting a different ABI.
type v4lCapability struct {
	Driver                            [16]byte
	Card                              [32]byte
	Bus                               [32]byte
	Version, Capabilities, DeviceCaps uint32
	Reserved                          [3]uint32
}
type v4lFormat struct {
	Type uint32
	_    uint32
	Data [200]byte
}
type v4lRequest struct {
	Count, Type, Memory, Capabilities uint32
	Flags                             uint8
	Reserved                          [3]byte
}
type v4lBuffer struct {
	Index, Type, BytesUsed, Flags, Field uint32
	_                                    uint32
	Time                                 unix.Timeval
	Timecode                             [16]byte
	Sequence, Memory                     uint32
	Offset                               uint64
	Length, Reserved                     uint32
	RequestFD                            int32
	_                                    uint32
}

const (
	v4lCapture = 1
	v4lMmap    = 1
	fourNV12   = 0x3231564e
	fourYUYV   = 0x56595559
)

func v4lIoctl(fd int, nr, dir uint32, p unsafe.Pointer, size uintptr) error {
	request := uintptr(dir<<30 | uint32(size)<<16 | uint32('V')<<8 | nr)
	for {
		_, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), request, uintptr(p))
		if errno == unix.EINTR {
			continue
		}
		if errno != 0 {
			return errno
		}
		return nil
	}
}
func v4lCaps(fd int) (v4lCapability, error) {
	var c v4lCapability
	err := v4lIoctl(fd, 0, 2, unsafe.Pointer(&c), unsafe.Sizeof(c))
	if err != nil {
		return c, err
	}
	caps := c.Capabilities
	if caps&0x80000000 != 0 {
		caps = c.DeviceCaps
	}
	if caps&1 == 0 || caps&0x04000000 == 0 {
		return c, ErrUnsupported
	}
	return c, nil
}
func devices(ctx context.Context) ([]Device, error) {
	paths, err := filepath.Glob("/dev/video*")
	if err != nil {
		return nil, err
	}
	result := []Device{}
	for _, path := range paths {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
		if err != nil {
			continue
		}
		c, err := v4lCaps(fd)
		unix.Close(fd)
		if err == nil {
			result = append(result, Device{ID: path, Name: strings.TrimRight(string(c.Card[:]), "\x00")})
		}
	}
	return result, nil
}
func requestAccess(ctx context.Context) error { return ctx.Err() }

type linuxCapture struct {
	fd        int
	config    Config
	format    uint32
	stride    int
	buffers   [][]byte
	streaming bool
}

func openDevice(ctx context.Context, c Config) (captureDevice, error) {
	path := c.Device
	if path == "" {
		ds, err := devices(ctx)
		if err != nil {
			return nil, err
		}
		if len(ds) == 0 {
			return nil, ErrNoDevice
		}
		path = ds[0].ID
	}
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, unix.EACCES) {
			return nil, fmt.Errorf("%w: %s", ErrPermission, path)
		}
		return nil, err
	}
	d := &linuxCapture{fd: fd, config: c}
	ok := false
	defer func() {
		if !ok {
			_ = d.Close()
		}
	}()
	if _, err = v4lCaps(fd); err != nil {
		return nil, err
	}
	for _, format := range []uint32{fourNV12, fourYUYV} {
		f := v4lFormat{Type: v4lCapture}
		put := binary.LittleEndian.PutUint32
		put(f.Data[0:], uint32(c.Width))
		put(f.Data[4:], uint32(c.Height))
		put(f.Data[8:], format)
		put(f.Data[12:], 1)          // V4L2_FIELD_NONE
		put(f.Data[28:], 0xfeedcafe) // V4L2_PIX_FMT_PRIV_MAGIC: extended fields valid.
		put(f.Data[40:], 2)          // V4L2_QUANTIZATION_LIM_RANGE
		if err = v4lIoctl(fd, 5, 3, unsafe.Pointer(&f), unsafe.Sizeof(f)); err != nil {
			continue
		}
		get := binary.LittleEndian.Uint32
		if get(f.Data[0:]) != uint32(c.Width) || get(f.Data[4:]) != uint32(c.Height) || get(f.Data[8:]) != format || get(f.Data[12:]) != 1 {
			continue
		}
		// YCbCr defaults to limited range except COLORSPACE_JPEG. Do not
		// silently advertise full-range native pixels as ordinary yuv420p.
		quant, color := get(f.Data[40:]), get(f.Data[24:])
		if quant != 2 && (quant != 0 || color == 7) {
			continue
		}
		d.format = format
		d.stride = int(get(f.Data[16:]))
		break
	}
	if d.format == 0 {
		return nil, fmt.Errorf("%w: need progressive %dx%d NV12 or YUYV", ErrUnsupported, c.Width, c.Height)
	}
	// S_PARM returns the negotiated rate. Never advertise a mode the driver changed.
	var parm [204]byte
	put := binary.LittleEndian.PutUint32
	put(parm[:], v4lCapture)
	put(parm[12:], 1)
	put(parm[16:], uint32(c.FPS))
	if err = v4lIoctl(fd, 22, 3, unsafe.Pointer(&parm), unsafe.Sizeof(parm)); err != nil {
		return nil, fmt.Errorf("cameraav: frame rate: %w", err)
	}
	num, den := binary.LittleEndian.Uint32(parm[12:]), binary.LittleEndian.Uint32(parm[16:])
	if num == 0 || uint64(den) != uint64(num)*uint64(c.FPS) {
		return nil, fmt.Errorf("%w: requested %d fps; device returned %d/%d", ErrUnsupported, c.FPS, den, num)
	}
	req := v4lRequest{Count: 4, Type: v4lCapture, Memory: v4lMmap}
	if err = v4lIoctl(fd, 8, 3, unsafe.Pointer(&req), unsafe.Sizeof(req)); err != nil {
		return nil, err
	}
	if req.Count < 2 || req.Count > 32 {
		return nil, fmt.Errorf("cameraav: invalid native buffer count %d", req.Count)
	}
	for i := uint32(0); i < req.Count; i++ {
		b := v4lBuffer{Index: i, Type: v4lCapture, Memory: v4lMmap}
		if err = v4lIoctl(fd, 9, 3, unsafe.Pointer(&b), unsafe.Sizeof(b)); err != nil {
			return nil, err
		}
		if b.Length == 0 || b.Length > 128<<20 {
			return nil, fmt.Errorf("cameraav: invalid native buffer length")
		}
		data, e := unix.Mmap(fd, int64(b.Offset), int(b.Length), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
		if e != nil {
			return nil, e
		}
		d.buffers = append(d.buffers, data)
	}
	ok = true
	return d, nil
}
func (d *linuxCapture) Run(ctx context.Context, emit func(*av.Frame)) error {
	var wake [2]int
	if err := unix.Pipe2(wake[:], unix.O_NONBLOCK|unix.O_CLOEXEC); err != nil {
		return err
	}
	woke := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _, _ = unix.Write(wake[1], []byte{1}); close(woke) })
	defer func() {
		if !stop() {
			<-woke
		}
		unix.Close(wake[0])
		unix.Close(wake[1])
	}()
	for i := range d.buffers {
		b := v4lBuffer{Index: uint32(i), Type: v4lCapture, Memory: v4lMmap}
		if err := v4lIoctl(d.fd, 15, 3, unsafe.Pointer(&b), unsafe.Sizeof(b)); err != nil {
			return err
		}
	}
	typ := uint32(v4lCapture)
	if err := v4lIoctl(d.fd, 18, 1, unsafe.Pointer(&typ), unsafe.Sizeof(typ)); err != nil {
		return err
	}
	d.streaming = true
	fds := []unix.PollFd{{Fd: int32(d.fd), Events: unix.POLLIN}, {Fd: int32(wake[0]), Events: unix.POLLIN}}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := unix.Poll(fds, -1)
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if fds[0].Revents&(unix.POLLERR|unix.POLLHUP|unix.POLLNVAL) != 0 {
			return fmt.Errorf("cameraav: camera disconnected")
		}
		b := v4lBuffer{Type: v4lCapture, Memory: v4lMmap}
		if err = v4lIoctl(d.fd, 17, 3, unsafe.Pointer(&b), unsafe.Sizeof(b)); err == unix.EAGAIN {
			continue
		} else if err != nil {
			return err
		}
		if int(b.Index) >= len(d.buffers) || int(b.BytesUsed) > len(d.buffers[b.Index]) {
			return fmt.Errorf("cameraav: invalid capture buffer")
		}
		var frame *av.Frame
		if b.Flags&0x40 == 0 { // V4L2_BUF_FLAG_ERROR: drop corrupted device buffers.
			data := d.buffers[b.Index][:b.BytesUsed]
			pts := b.Time.Sec*1e9 + b.Time.Usec*1e3
			if d.format == fourNV12 {
				offset := d.stride * d.config.Height
				if offset < 0 || offset > len(data) {
					return fmt.Errorf("cameraav: short NV12 buffer")
				}
				frame, err = copyNV12(d.config, pts, data[:offset], data[offset:], d.stride, d.stride)
			} else {
				frame, err = copyYUYV(d.config, pts, data, d.stride)
			}
		}
		queueErr := v4lIoctl(d.fd, 15, 3, unsafe.Pointer(&b), unsafe.Sizeof(b))
		if err != nil || queueErr != nil {
			if frame != nil {
				releaseFrame(frame)
			}
			return errors.Join(err, queueErr)
		}
		if frame != nil {
			emit(frame)
		}
	}
}
func (d *linuxCapture) Close() error {
	var err error
	if d.streaming {
		typ := uint32(v4lCapture)
		err = v4lIoctl(d.fd, 19, 1, unsafe.Pointer(&typ), unsafe.Sizeof(typ))
		d.streaming = false
	}
	for _, data := range d.buffers {
		err = errors.Join(err, unix.Munmap(data))
	}
	d.buffers = nil
	if d.fd >= 0 {
		err = errors.Join(err, unix.Close(d.fd))
		d.fd = -1
	}
	return err
}
