//go:build linux && (amd64 || arm64)

package cameraav

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
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

	v4lBufferError        = 0x40
	v4lTimestampMask      = 0xe000
	v4lTimestampMonotonic = 0x2000
	maxNativeBuffer       = 128 << 20
)

// These operations are fixed for each opened device. Tests replace the native
// boundary without replacing the validation, copy, requeue, or clock logic.
type linuxOps struct {
	glob    func(string) ([]string, error)
	stat    func(string, *unix.Stat_t) error
	fstat   func(int, *unix.Stat_t) error
	open    func(string, int, uint32) (int, error)
	close   func(int) error
	ioctl   func(int, uint32, uint32, unsafe.Pointer, uintptr) error
	mmap    func(int, int64, int, int, int) ([]byte, error)
	munmap  func([]byte) error
	gettime func(int32, *unix.Timespec) error
}

var nativeLinux = linuxOps{
	glob: filepath.Glob, stat: unix.Stat, fstat: unix.Fstat,
	open: unix.Open, close: unix.Close, ioctl: v4lIoctl,
	mmap: unix.Mmap, munmap: unix.Munmap, gettime: unix.ClockGettime,
}

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
func v4lCaps(fd int, ops *linuxOps) (v4lCapability, error) {
	var c v4lCapability
	err := ops.ioctl(fd, 0, 2, unsafe.Pointer(&c), unsafe.Sizeof(c))
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
	return linuxDevices(ctx, &nativeLinux)
}

func linuxDevices(ctx context.Context, ops *linuxOps) ([]Device, error) {
	paths, err := ops.glob("/dev/video*")
	if err != nil {
		return nil, err
	}
	result := []Device{}
	var denied error
	for _, path := range paths {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		fd, err := openV4LDevice(path, unix.O_RDONLY, ops)
		if err != nil {
			if errors.Is(err, ErrPermission) {
				denied = err
			}
			continue
		}
		c, err := v4lCaps(fd, ops)
		ops.close(fd)
		if err == nil {
			result = append(result, Device{ID: path, Name: strings.TrimRight(string(c.Card[:]), "\x00")})
		} else if errors.Is(err, unix.EACCES) || errors.Is(err, unix.EPERM) {
			denied = linuxDeviceError(path, err)
		}
	}
	if len(result) == 0 && denied != nil {
		return nil, denied
	}
	return result, nil
}
func requestAccess(ctx context.Context) error { return ctx.Err() }

func linuxDeviceError(path string, err error) error {
	if errors.Is(err, unix.EACCES) || errors.Is(err, unix.EPERM) {
		return fmt.Errorf("%w: %s: %w", ErrPermission, path, err)
	}
	return err
}

func validV4LDevice(stat *unix.Stat_t) bool {
	return stat.Mode&unix.S_IFMT == unix.S_IFCHR && unix.Major(uint64(stat.Rdev)) == 81
}

// O_PATH pins an inode without invoking its device open operation. Reopening
// that validated inode through procfs prevents a symlink/path replacement from
// changing the target to, for example, a watchdog between Stat and O_RDWR.
func openV4LDevice(path string, access int, ops *linuxOps) (int, error) {
	var before unix.Stat_t
	if err := ops.stat(path, &before); err != nil {
		return -1, linuxDeviceError(path, err)
	}
	if !validV4LDevice(&before) {
		return -1, fmt.Errorf("%w: %s is not a V4L2 character device", ErrUnsupported, path)
	}
	pinned, err := ops.open(path, unix.O_PATH|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, linuxDeviceError(path, err)
	}
	defer ops.close(pinned)
	var held unix.Stat_t
	if err := ops.fstat(pinned, &held); err != nil {
		return -1, linuxDeviceError(path, err)
	}
	if !validV4LDevice(&held) || before.Dev != held.Dev || before.Ino != held.Ino || before.Rdev != held.Rdev {
		return -1, fmt.Errorf("%w: camera device changed while opening %s", ErrNoDevice, path)
	}
	fd, err := ops.open(fmt.Sprintf("/proc/self/fd/%d", pinned), access|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, linuxDeviceError(path, err)
	}
	var opened unix.Stat_t
	if err = ops.fstat(fd, &opened); err != nil {
		ops.close(fd)
		return -1, linuxDeviceError(path, err)
	}
	if !validV4LDevice(&opened) || held.Dev != opened.Dev || held.Ino != opened.Ino || held.Rdev != opened.Rdev {
		ops.close(fd)
		return -1, fmt.Errorf("%w: camera device identity changed for %s", ErrNoDevice, path)
	}
	return fd, nil
}

type linuxCapture struct {
	fd        int
	ops       *linuxOps
	config    Config
	format    uint32
	stride    int
	minBytes  int
	sizeImage int
	buffers   [][]byte
	streaming bool

	// Reused syscall arguments keep dynamic syscall dispatch from allocating
	// a fresh escaping buffer or timespec on every frame.
	buffer                 v4lBuffer
	clock                  unix.Timespec
	lastPTS, lastNativePTS int64
	havePTS, haveNativePTS bool
}

func openDevice(ctx context.Context, c Config) (captureDevice, error) {
	return openLinuxDevice(ctx, c, &nativeLinux)
}

func openLinuxDevice(ctx context.Context, c Config, ops *linuxOps) (captureDevice, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path := c.Device
	if path == "" {
		ds, err := linuxDevices(ctx, ops)
		if err != nil {
			return nil, err
		}
		if len(ds) == 0 {
			return nil, ErrNoDevice
		}
		path = ds[0].ID
	}
	fd, err := openV4LDevice(path, unix.O_RDWR, ops)
	if err != nil {
		return nil, err
	}
	d := &linuxCapture{fd: fd, config: c, ops: ops}
	ok := false
	defer func() {
		if !ok {
			_ = d.Close()
		}
	}()
	if _, err = v4lCaps(fd, ops); err != nil {
		return nil, err
	}
	if err = d.setFormat(); err != nil {
		return nil, err
	}
	// S_PARM returns the negotiated rate. Never advertise a mode the driver changed.
	var parm [204]byte
	put := binary.LittleEndian.PutUint32
	put(parm[:], v4lCapture)
	put(parm[12:], 1)
	put(parm[16:], uint32(c.FPS))
	if err = ops.ioctl(fd, 22, 3, unsafe.Pointer(&parm), unsafe.Sizeof(parm)); err != nil {
		return nil, fmt.Errorf("cameraav: frame rate: %w", err)
	}
	num, den := binary.LittleEndian.Uint32(parm[12:]), binary.LittleEndian.Uint32(parm[16:])
	if num == 0 || uint64(den) != uint64(num)*uint64(c.FPS) {
		return nil, fmt.Errorf("%w: requested %d fps; device returned %d/%d", ErrUnsupported, c.FPS, den, num)
	}
	req := v4lRequest{Count: 4, Type: v4lCapture, Memory: v4lMmap}
	if err = ops.ioctl(fd, 8, 3, unsafe.Pointer(&req), unsafe.Sizeof(req)); err != nil {
		return nil, err
	}
	if req.Count < 2 || req.Count > 32 {
		return nil, fmt.Errorf("cameraav: invalid native buffer count %d", req.Count)
	}
	var mappedBytes uint64
	for i := uint32(0); i < req.Count; i++ {
		b := v4lBuffer{Index: i, Type: v4lCapture, Memory: v4lMmap}
		if err = ops.ioctl(fd, 9, 3, unsafe.Pointer(&b), unsafe.Sizeof(b)); err != nil {
			return nil, err
		}
		if uint64(b.Length) < uint64(d.sizeImage) || b.Length > maxNativeBuffer {
			return nil, fmt.Errorf("cameraav: native buffer length %d is incompatible with sizeimage %d", b.Length, d.sizeImage)
		}
		if mappedBytes+uint64(b.Length) > maxNativeBuffer {
			return nil, fmt.Errorf("cameraav: native buffers exceed %d bytes", maxNativeBuffer)
		}
		data, e := ops.mmap(fd, int64(b.Offset), int(b.Length), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
		if e != nil {
			return nil, e
		}
		d.buffers = append(d.buffers, data)
		mappedBytes += uint64(b.Length)
	}
	ok = true
	return d, nil
}

func (d *linuxCapture) setFormat() error {
	c := d.config
	var layoutErr error
	for _, format := range []uint32{fourNV12, fourYUYV} {
		f := v4lFormat{Type: v4lCapture}
		put := binary.LittleEndian.PutUint32
		put(f.Data[0:], uint32(c.Width))
		put(f.Data[4:], uint32(c.Height))
		put(f.Data[8:], format)
		put(f.Data[12:], 1)          // V4L2_FIELD_NONE
		put(f.Data[28:], 0xfeedcafe) // V4L2_PIX_FMT_PRIV_MAGIC: extended fields valid.
		put(f.Data[40:], 2)          // V4L2_QUANTIZATION_LIM_RANGE
		if err := d.ops.ioctl(d.fd, 5, 3, unsafe.Pointer(&f), unsafe.Sizeof(f)); err != nil {
			if errors.Is(err, unix.EINVAL) {
				continue
			}
			return fmt.Errorf("cameraav: set format: %w", err)
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
		stride, sizeImage := int64(get(f.Data[16:])), int64(get(f.Data[20:]))
		minStride := int64(c.Width)
		rows := int64(c.Height) * 3 / 2
		if format == fourYUYV {
			minStride *= 2
			rows = int64(c.Height)
		}
		if stride < minStride || stride*rows > maxNativeBuffer || sizeImage < stride*rows || sizeImage > maxNativeBuffer {
			layoutErr = fmt.Errorf("%w: invalid native stride %d or sizeimage %d", ErrUnsupported, stride, sizeImage)
			continue
		}
		d.format = format
		d.stride, d.sizeImage = int(stride), int(sizeImage)
		// A payload may omit padding after the last row, but must contain
		// every active pixel. NV12's UV plane starts after the full Y stride.
		d.minBytes = int((rows-1)*stride + minStride)
		return nil
	}
	if layoutErr != nil {
		return layoutErr
	}
	return fmt.Errorf("%w: need progressive %dx%d NV12 or YUYV", ErrUnsupported, c.Width, c.Height)
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
		if err := d.ops.ioctl(d.fd, 15, 3, unsafe.Pointer(&b), unsafe.Sizeof(b)); err != nil {
			return err
		}
	}
	typ := uint32(v4lCapture)
	if err := d.ops.ioctl(d.fd, 18, 1, unsafe.Pointer(&typ), unsafe.Sizeof(typ)); err != nil {
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
		if err := d.readFrame(emit); err != nil {
			return err
		}
	}
}

// readFrame owns a dequeued buffer until QBUF succeeds. Native pixels never
// escape: conversion copies into a fixed pool slot before the driver reuses it.
func (d *linuxCapture) readFrame(emit func(*av.Frame)) error {
	d.buffer = v4lBuffer{Type: v4lCapture, Memory: v4lMmap}
	b := &d.buffer
	if err := d.ops.ioctl(d.fd, 17, 3, unsafe.Pointer(b), unsafe.Sizeof(*b)); err != nil {
		if errors.Is(err, unix.EAGAIN) {
			return nil
		}
		return err
	}
	if uint64(b.Index) >= uint64(len(d.buffers)) {
		return fmt.Errorf("cameraav: invalid capture buffer index %d", b.Index)
	}
	frame, copyErr := d.copyBuffer(b)
	*b = v4lBuffer{Index: b.Index, Type: v4lCapture, Memory: v4lMmap}
	queueErr := d.ops.ioctl(d.fd, 15, 3, unsafe.Pointer(b), unsafe.Sizeof(*b))
	if copyErr != nil || queueErr != nil {
		if frame != nil {
			releaseFrame(frame)
		}
		return errors.Join(copyErr, queueErr)
	}
	if frame != nil {
		d.lastPTS, d.havePTS = frame.PTS.Value, true
		emit(frame)
	}
	return nil
}

func (d *linuxCapture) copyBuffer(b *v4lBuffer) (*av.Frame, error) {
	if b.Flags&v4lBufferError != 0 || uint64(b.BytesUsed) < uint64(d.minBytes) {
		d.config.buffers.dropped.Add(1)
		return nil, nil
	}
	if uint64(b.BytesUsed) > uint64(len(d.buffers[b.Index])) {
		return nil, fmt.Errorf("cameraav: capture bytesused exceeds its mapped buffer")
	}
	pts, keep, err := d.timestamp(b)
	if err != nil {
		return nil, err
	}
	if !keep {
		d.config.buffers.dropped.Add(1)
		return nil, nil
	}
	data := d.buffers[b.Index][:b.BytesUsed]
	if d.format == fourNV12 {
		offset := d.stride * d.config.Height
		return copyNV12(d.config, pts, data[:offset], data[offset:], d.stride, d.stride)
	}
	return copyYUYV(d.config, pts, data, d.stride)
}

// UNKNOWN timestamps may be realtime or monotonic. COPY belongs to mem2mem
// outputs, not camera capture. Neither is a capture clock we can safely infer.
// Use monotonic arrival time for these and zero timestamps. If a later native
// capture timestamp predates that arrival, drop the frame until capture catches
// up, preserving the exact timestamps/gaps of every native frame we deliver.
// A regression within the native monotonic clock itself remains a real error.
func (d *linuxCapture) timestamp(b *v4lBuffer) (int64, bool, error) {
	native := b.Flags&v4lTimestampMask == v4lTimestampMonotonic && (b.Time.Sec != 0 || b.Time.Usec != 0)
	var pts int64
	if native {
		if b.Time.Sec < 0 || b.Time.Usec < 0 || b.Time.Usec >= 1e6 || b.Time.Sec > (math.MaxInt64-b.Time.Usec*1000)/1e9 {
			return 0, false, errors.New("cameraav: invalid native capture timestamp")
		}
		pts = b.Time.Sec*1e9 + b.Time.Usec*1000
		if d.haveNativePTS && pts < d.lastNativePTS {
			return 0, false, errors.New("cameraav: native capture timestamp regressed")
		}
		d.lastNativePTS, d.haveNativePTS = pts, true
		if d.havePTS && pts < d.lastPTS {
			return 0, false, nil
		}
	} else {
		if err := d.ops.gettime(unix.CLOCK_MONOTONIC, &d.clock); err != nil {
			return 0, false, fmt.Errorf("cameraav: monotonic capture clock: %w", err)
		}
		if d.clock.Sec < 0 || d.clock.Nsec < 0 || d.clock.Nsec >= 1e9 || d.clock.Sec > (math.MaxInt64-d.clock.Nsec)/1e9 {
			return 0, false, errors.New("cameraav: invalid monotonic capture clock")
		}
		pts = d.clock.Sec*1e9 + d.clock.Nsec
		if d.havePTS && pts < d.lastPTS {
			return 0, false, errors.New("cameraav: monotonic arrival timestamp regressed")
		}
	}
	return pts, true, nil
}

func (d *linuxCapture) Close() error {
	var err error
	if d.streaming {
		typ := uint32(v4lCapture)
		err = d.ops.ioctl(d.fd, 19, 1, unsafe.Pointer(&typ), unsafe.Sizeof(typ))
		d.streaming = false
	}
	for _, data := range d.buffers {
		err = errors.Join(err, d.ops.munmap(data))
	}
	d.buffers = nil
	if d.fd >= 0 {
		err = errors.Join(err, d.ops.close(d.fd))
		d.fd = -1
	}
	return err
}
