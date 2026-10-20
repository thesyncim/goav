//go:build windows && (amd64 || arm64)

package cameraav

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/thesyncim/goav/av"
	"golang.org/x/sys/windows"
)

// Media Foundation/IUnknown ABI constants from the Windows SDK. COM activity
// belongs to a locked MTA thread; callbacks only retain and hand off one sample.
var (
	mfplat             = windows.NewLazySystemDLL("mfplat.dll")
	mfdll              = windows.NewLazySystemDLL("mf.dll")
	mfread             = windows.NewLazySystemDLL("mfreadwrite.dll")
	ole                = windows.NewLazySystemDLL("ole32.dll")
	mfStartup          = mfplat.NewProc("MFStartup")
	mfShutdown         = mfplat.NewProc("MFShutdown")
	mfCreateAttributes = mfplat.NewProc("MFCreateAttributes")
	mfEnum             = mfdll.NewProc("MFEnumDeviceSources")
	mfReader           = mfread.NewProc("MFCreateSourceReaderFromMediaSource")
	coInit             = ole.NewProc("CoInitializeEx")
	coUninit           = ole.NewProc("CoUninitialize")
	coFree             = ole.NewProc("CoTaskMemFree")
	sourceType         = guid("c60ac5fe-252a-478f-a0ef-bc8fa5f7cad3")
	videoCapture       = guid("8ac3587a-4ae7-42d8-99e0-0a6013eef90f")
	deviceName         = guid("60d0e559-52f8-4fa2-bbce-acdb34a8ec01")
	deviceLink         = guid("58f0aad8-22bf-4f8a-bb3d-d2c4978c6e2f")
	mediaSourceIID     = guid("279a808d-aec7-40c8-9c6b-a6b492c78a66")
	callbackIID        = guid("deec8d99-fa1d-4d82-84c2-2c8969944867")
	unknownIID         = guid("00000000-0000-0000-c000-000000000046")
	asyncCallback      = guid("1e3dbeac-bb43-4c35-b507-cd644464c965")
	subtype            = guid("f7e34c9a-42e8-4714-b74b-cb29d72c35e5")
	frameSize          = guid("1652c33d-d6b2-4012-b834-72030849a37d")
	frameRate          = guid("c459a2e8-3d2c-4e44-b132-fee5156c7bb0")
	interlaceMode      = guid("e2724bb8-e676-4806-b4b2-a8d6efb44ccd")
	nominalRange       = guid("c21b8ee5-b956-4071-8daf-325edf5cab11")
	buffer2DIID        = guid("33ae5ea6-4316-436f-8ddd-d73d22f829ec")
	nv12Type           = guid("3231564e-0000-0010-8000-00aa00389b71")
	yuy2Type           = guid("32595559-0000-0010-8000-00aa00389b71")
)

const (
	firstVideo     = 0xfffffffc
	eNoInterface   = 0x80004002
	eAccessDenied  = 0x80070005
	mfFlushTimeout = 2 * time.Second
)

func guid(s string) windows.GUID {
	v, err := windows.GUIDFromString("{" + s + "}")
	if err != nil {
		panic(err)
	}
	return v
}
func ptr[T any](v *T) uintptr { return uintptr(unsafe.Pointer(v)) }
func hr(result uintptr) error {
	if uint32(result) == eAccessDenied {
		return fmt.Errorf("%w: HRESULT 0x%08x", ErrPermission, uint32(result))
	}
	if int32(result) < 0 {
		return fmt.Errorf("cameraav: HRESULT 0x%08x", uint32(result))
	}
	return nil
}

var comArguments = sync.Pool{New: func() any { return new([12]uintptr) }}

func comFunction(obj uintptr, index int) uintptr {
	vt := *(*uintptr)(nativePointer(obj))
	return *(*uintptr)(nativePointer(vt + uintptr(index)*unsafe.Sizeof(uintptr(0))))
}

// Cold COM calls may be concurrent. Capture-thread calls use windowsCapture's
// fixed argument storage instead, so a GC clearing this pool cannot allocate
// on the successful sample-copy path.
func com(obj uintptr, index int, args ...uintptr) uintptr {
	fn := comFunction(obj, index)
	values := comArguments.Get().(*[12]uintptr)
	values[0] = obj
	copy(values[1:], args)
	result, _, _ := syscall.SyscallN(fn, values[:len(args)+1]...)
	comArguments.Put(values)
	return result
}
func comRelease(obj uintptr) {
	if obj != 0 {
		com(obj, 2)
	}
}
func startMF() error {
	for _, p := range []*windows.LazyProc{mfStartup, mfShutdown, mfCreateAttributes, mfEnum, mfReader, coInit, coUninit, coFree} {
		if err := p.Find(); err != nil {
			return fmt.Errorf("%w: %v", ErrUnsupported, err)
		}
	}
	result, _, _ := coInit.Call(0, 0)
	if err := hr(result); err != nil {
		return err
	}
	result, _, _ = mfStartup.Call(0x20070, 0)
	if err := hr(result); err != nil {
		coUninit.Call()
		return err
	}
	return nil
}
func stopMF() { mfShutdown.Call(); coUninit.Call() }
func attributes() (uintptr, error) {
	var a uintptr
	var pins runtime.Pinner
	pins.Pin(&a)
	defer pins.Unpin()
	r, _, _ := mfCreateAttributes.Call(ptr(&a), 4)
	return a, hr(r)
}
func enumerateMF() ([]uintptr, error) {
	a, err := attributes()
	if err != nil {
		return nil, err
	}
	defer comRelease(a)
	if err = hr(com(a, 24, ptr(&sourceType), ptr(&videoCapture))); err != nil {
		return nil, err
	}
	var list uintptr
	var count uint32
	var pins runtime.Pinner
	pins.Pin(&list)
	pins.Pin(&count)
	defer pins.Unpin()
	result, _, _ := mfEnum.Call(a, ptr(&list), ptr(&count))
	if err = hr(result); err != nil {
		return nil, err
	}
	defer coFree.Call(list)
	if count > 4096 {
		return nil, errors.New("cameraav: invalid device count")
	}
	out := append([]uintptr(nil), unsafe.Slice((*uintptr)(nativePointer(list)), int(count))...)
	return out, nil
}
func mfString(obj uintptr, key *windows.GUID) (string, error) {
	var data *uint16
	var count uint32
	var pins runtime.Pinner
	pins.Pin(&data)
	pins.Pin(&count)
	defer pins.Unpin()
	if err := hr(com(obj, 13, ptr(key), ptr(&data), ptr(&count))); err != nil {
		return "", err
	}
	defer coFree.Call(ptr(data))
	if count > 1<<20 {
		return "", errors.New("cameraav: invalid device string")
	}
	return windows.UTF16ToString(unsafe.Slice(data, int(count))), nil
}
func devices(ctx context.Context) ([]Device, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if err := startMF(); err != nil {
		return nil, err
	}
	defer stopMF()
	list, err := enumerateMF()
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, d := range list {
			comRelease(d)
		}
	}()
	out := make([]Device, 0, len(list))
	for _, d := range list {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		id, e := mfString(d, &deviceLink)
		if e != nil {
			return nil, e
		}
		name, e := mfString(d, &deviceName)
		if e != nil {
			return nil, e
		}
		out = append(out, Device{ID: id, Name: name})
	}
	return out, nil
}
func requestAccess(ctx context.Context) error { return ctx.Err() }

type winSample struct {
	sample uintptr
	status uint32
	flags  uint32
	pts    int64
}
type winCallback struct {
	refs    atomic.Int32
	samples chan winSample
	flushed chan struct{}
	mu      sync.Mutex
	closed  bool
	// Only offer uses this array, while holding mu. SyscallN requires its
	// argument backing storage to escape; keeping it here avoids a per-frame
	// allocation without relying on sync.Pool surviving a collection.
	retainArguments [1]uintptr
}

// OnReadSample lends its sample until the callback returns. Retain only an
// accepted handoff; closing and queue publication share the same lock.
func (c *winCallback) offer(v winSample) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	if v.sample != 0 {
		c.retainArguments[0] = v.sample
		syscall.SyscallN(comFunction(v.sample, 1), c.retainArguments[:]...)
	}
	select {
	case c.samples <- v:
		c.mu.Unlock()
	default:
		c.mu.Unlock()
		comRelease(v.sample)
	}
}

// Set closed before draining, so a callback that looked this object up before
// cleanup cannot enqueue after the drain. Release native objects outside mu:
// their final Release may itself call into COM or release this callback.
func (c *winCallback) close() {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	for {
		c.mu.Lock()
		select {
		case v := <-c.samples:
			c.mu.Unlock()
			comRelease(v.sample)
		default:
			c.mu.Unlock()
			return
		}
	}
}

var callbacks struct {
	sync.Mutex
	objects map[uintptr]*winCallback
	once    sync.Once
	vtable  uintptr
	err     error
}

func callbackAt(self uintptr) *winCallback {
	callbacks.Lock()
	v := callbacks.objects[self]
	callbacks.Unlock()
	return v
}
func newCallback() (uintptr, *winCallback, error) {
	callbacks.once.Do(func() {
		callbacks.objects = make(map[uintptr]*winCallback)
		callbacks.vtable, callbacks.err = windows.LocalAlloc(0, uint32(6*unsafe.Sizeof(uintptr(0))))
		if callbacks.err != nil {
			return
		}
		table := unsafe.Slice((*uintptr)(nativePointer(callbacks.vtable)), 6)
		table[0] = syscall.NewCallback(func(self, key, out uintptr) uintptr {
			id := *(*windows.GUID)(nativePointer(key))
			if id != unknownIID && id != callbackIID {
				*(*uintptr)(nativePointer(out)) = 0
				return 0x80004002
			}
			if c := callbackAt(self); c != nil {
				c.refs.Add(1)
				*(*uintptr)(nativePointer(out)) = self
				return 0
			}
			return 0x80004003
		})
		table[1] = syscall.NewCallback(func(self uintptr) uintptr {
			if c := callbackAt(self); c != nil {
				return uintptr(c.refs.Add(1))
			}
			return 0
		})
		table[2] = syscall.NewCallback(func(self uintptr) uintptr {
			c := callbackAt(self)
			if c == nil {
				return 0
			}
			n := c.refs.Add(-1)
			if n == 0 {
				c.close()
				callbacks.Lock()
				delete(callbacks.objects, self)
				callbacks.Unlock()
				windows.LocalFree(windows.Handle(self))
			}
			return uintptr(n)
		})
		table[3] = syscall.NewCallback(func(self, status, stream, flags, timestamp, sample uintptr) uintptr {
			if c := callbackAt(self); c != nil {
				c.offer(winSample{sample: sample, status: uint32(status), flags: uint32(flags), pts: int64(timestamp)})
			}
			return 0
		})
		table[4] = syscall.NewCallback(func(self, stream uintptr) uintptr {
			if c := callbackAt(self); c != nil {
				select {
				case c.flushed <- struct{}{}:
				default:
				}
			}
			return 0
		})
		table[5] = syscall.NewCallback(func(self, stream, event uintptr) uintptr { return 0 })
	})
	if callbacks.err != nil {
		return 0, nil, callbacks.err
	}
	obj, err := windows.LocalAlloc(0, uint32(unsafe.Sizeof(uintptr(0))))
	if err != nil {
		return 0, nil, err
	}
	*(*uintptr)(nativePointer(obj)) = callbacks.vtable
	c := &winCallback{samples: make(chan winSample, 1), flushed: make(chan struct{}, 1)}
	c.refs.Store(1)
	callbacks.Lock()
	callbacks.objects[obj] = c
	callbacks.Unlock()
	return obj, c, nil
}

type winRun struct {
	ctx    context.Context
	emit   func(*av.Frame)
	result chan error
}
type windowsCapture struct {
	config                   Config
	source, reader, callback uintptr
	cb                       *winCallback
	nv12                     bool
	start                    chan winRun
	stop, done               chan struct{}
	once                     sync.Once
	closeErr                 error
	// This object is pinned for the MTA thread's lifetime. Only that thread
	// uses these native outputs and COM arguments; callbacks have their own
	// storage. Successful reads/copies need no per-call Go allocation.
	arguments                           [12]uintptr
	buffer, buffer2D, data, bufferStart uintptr
	maximum, length                     uint32
	pitch                               int32
}

func (d *windowsCapture) call(obj uintptr, index int, args ...uintptr) uintptr {
	d.arguments[0] = obj
	copy(d.arguments[1:], args)
	result, _, _ := syscall.SyscallN(comFunction(obj, index), d.arguments[:len(args)+1]...)
	return result
}
func (d *windowsCapture) release(obj uintptr) {
	if obj != 0 {
		d.call(obj, 2)
	}
}

func openDevice(ctx context.Context, c Config) (captureDevice, error) {
	d := &windowsCapture{config: c, start: make(chan winRun), stop: make(chan struct{}), done: make(chan struct{})}
	ready := make(chan error, 1)
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		defer close(d.done)
		var pins runtime.Pinner
		pins.Pin(d)
		defer pins.Unpin()
		if err := startMF(); err != nil {
			ready <- err
			return
		}
		defer stopMF()
		if err := d.open(); err != nil {
			_ = d.cleanup()
			ready <- err
			return
		}
		ready <- nil
		select {
		case run := <-d.start:
			run.result <- d.capture(run.ctx, run.emit)
			<-d.stop
		case <-d.stop:
		}
		d.closeErr = errors.Join(d.closeErr, d.cleanup())
	}()
	select {
	case err := <-ready:
		if err != nil {
			<-d.done
			return nil, err
		}
		return d, nil
	case <-ctx.Done():
		close(d.stop)
		<-d.done
		return nil, ctx.Err()
	}
}
func (d *windowsCapture) open() error {
	list, err := enumerateMF()
	if err != nil {
		return err
	}
	defer func() {
		for _, v := range list {
			comRelease(v)
		}
	}()
	var selected uintptr
	for _, v := range list {
		id, e := mfString(v, &deviceLink)
		if e != nil {
			return e
		}
		if d.config.Device == "" || id == d.config.Device {
			selected = v
			break
		}
	}
	if selected == 0 {
		return ErrNoDevice
	}
	if err = hr(com(selected, 33, ptr(&mediaSourceIID), ptr(&d.source))); err != nil {
		return err
	}
	d.callback, d.cb, err = newCallback()
	if err != nil {
		return err
	}
	attrs, err := attributes()
	if err != nil {
		return err
	}
	defer comRelease(attrs)
	if err = hr(com(attrs, 27, ptr(&asyncCallback), d.callback)); err != nil {
		return err
	}
	result, _, _ := mfReader.Call(d.source, attrs, ptr(&d.reader))
	if err = hr(result); err != nil {
		return err
	}
	if err = hr(com(d.reader, 4, 0xfffffffe, 0)); err != nil {
		return err
	}
	if err = hr(com(d.reader, 4, firstVideo, 1)); err != nil {
		return err
	}
	for i := uintptr(0); i < 4096; i++ {
		var media uintptr
		var modePins runtime.Pinner
		modePins.Pin(&media)
		if err = hr(com(d.reader, 5, firstVideo, i, ptr(&media))); err != nil {
			modePins.Unpin()
			break
		}
		var size, rate uint64
		var typ windows.GUID
		modePins.Pin(&size)
		modePins.Pin(&rate)
		modePins.Pin(&typ)
		valid := com(media, 8, ptr(&frameSize), ptr(&size)) == 0 && com(media, 8, ptr(&frameRate), ptr(&rate)) == 0 && com(media, 10, ptr(&subtype), ptr(&typ)) == 0
		if valid && int(size>>32) == d.config.Width && int(uint32(size)) == d.config.Height && uint32(rate) != 0 && uint64(rate>>32) == uint64(uint32(rate))*uint64(d.config.FPS) && (typ == nv12Type || typ == yuy2Type) {
			err = hr(com(d.reader, 7, firstVideo, 0, media))
			comRelease(media)
			modePins.Unpin()
			if err != nil {
				return err
			}
			d.nv12 = typ == nv12Type
			var actual uintptr
			var actualPins runtime.Pinner
			actualPins.Pin(&actual)
			defer actualPins.Unpin()
			if err = hr(com(d.reader, 6, firstVideo, ptr(&actual))); err != nil {
				return err
			}
			defer comRelease(actual)
			// Confirm the negotiated type instead of assuming SetCurrentMediaType
			// preserved the selected native mode. Reject interlaced/full-range data.
			var actualSize, actualRate uint64
			var actualType windows.GUID
			var interlace, nominal uint32
			actualPins.Pin(&actualSize)
			actualPins.Pin(&actualRate)
			actualPins.Pin(&actualType)
			actualPins.Pin(&interlace)
			actualPins.Pin(&nominal)
			if com(actual, 8, ptr(&frameSize), ptr(&actualSize)) != 0 || com(actual, 8, ptr(&frameRate), ptr(&actualRate)) != 0 || com(actual, 10, ptr(&subtype), ptr(&actualType)) != 0 || actualSize != size || actualRate != rate || actualType != typ {
				return ErrFormatChanged
			}
			if com(actual, 7, ptr(&interlaceMode), ptr(&interlace)) == 0 && interlace != 0 && interlace != 2 {
				return fmt.Errorf("%w: interlaced camera mode", ErrUnsupported)
			}
			if com(actual, 7, ptr(&nominalRange), ptr(&nominal)) == 0 && nominal != 0 && nominal != 2 {
				return fmt.Errorf("%w: camera range must be 16-235", ErrUnsupported)
			}
			return nil
		}
		comRelease(media)
		modePins.Unpin()
	}
	return fmt.Errorf("%w: need native NV12/YUY2 %dx%d at %d fps", ErrUnsupported, d.config.Width, d.config.Height, d.config.FPS)
}
func (d *windowsCapture) Run(ctx context.Context, emit func(*av.Frame)) error {
	result := make(chan error, 1)
	d.start <- winRun{ctx: ctx, emit: emit, result: result}
	return <-result
}

// Flush is asynchronous for a reader with a callback. A broken device may
// never call OnFlush; keep that wait bounded and retain the failure for Close.
func (d *windowsCapture) flush(timeout time.Duration) (result error) {
	defer func() { d.closeErr = errors.Join(d.closeErr, result) }()
	d.cb.close()
	if err := hr(d.call(d.reader, 10, firstVideo)); err != nil {
		return err
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-d.cb.flushed:
		return nil
	case <-timer.C:
		return fmt.Errorf("cameraav: source-reader flush: %w", context.DeadlineExceeded)
	}
}

func (d *windowsCapture) capture(ctx context.Context, emit func(*av.Frame)) (result error) {
	defer func() {
		if err := d.flush(mfFlushTimeout); err != nil {
			result = errors.Join(result, err)
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := hr(d.call(d.reader, 9, firstVideo, 0, 0, 0, 0, 0)); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case s := <-d.cb.samples:
			if s.status&0x80000000 != 0 {
				d.release(s.sample)
				return hr(uintptr(s.status))
			}
			if s.flags&0x33 != 0 {
				d.release(s.sample)
				return fmt.Errorf("%w: source-reader flags 0x%x", ErrFormatChanged, s.flags)
			}
			if s.sample == 0 {
				continue
			}
			frame, err := d.copySample(s.sample, s.pts)
			d.release(s.sample)
			if err != nil {
				return err
			}
			if frame != nil {
				emit(frame)
			}
		}
	}
}
func (d *windowsCapture) copySample(sample uintptr, pts int64) (*av.Frame, error) {
	if pts > int64(^uint64(0)>>1)/100 || pts < (-int64(^uint64(0)>>1)-1)/100 {
		return nil, errors.New("cameraav: invalid sample timestamp")
	}
	d.buffer = 0
	if err := hr(d.call(sample, 41, ptr(&d.buffer))); err != nil {
		return nil, err
	}
	defer d.release(d.buffer)
	d.buffer2D = 0
	result := d.call(d.buffer, 0, ptr(&buffer2DIID), ptr(&d.buffer2D))
	if uint32(result) == eNoInterface {
		return d.copyContiguousBuffer(pts * 100)
	}
	if err := hr(result); err != nil {
		return nil, err
	}
	defer d.release(d.buffer2D)
	// IMF2DBuffer2::Lock2DSize, read-only. Its bounds describe the native
	// allocation; IMFMediaBuffer lengths instead describe packed data.
	if err := hr(d.call(d.buffer2D, 10, 1, ptr(&d.data), ptr(&d.pitch), ptr(&d.bufferStart), ptr(&d.length))); err != nil {
		return nil, err
	}
	defer d.call(d.buffer2D, 4) // IMF2DBuffer::Unlock2D
	if d.pitch <= 0 || d.pitch > 1<<20 {
		return nil, fmt.Errorf("%w: camera buffer pitch %d", ErrUnsupported, d.pitch)
	}
	if d.bufferStart == 0 || d.data < d.bufferStart || d.length > 128<<20 || uintptr(d.length) > ^uintptr(0)-d.bufferStart || d.data-d.bufferStart > uintptr(d.length) {
		return nil, errors.New("cameraav: invalid 2D sample bounds")
	}
	length := uintptr(d.length) - (d.data - d.bufferStart)
	data := unsafe.Slice((*byte)(nativePointer(d.data)), int(length))
	return d.copyPixels(pts*100, data, int(d.pitch))
}

func (d *windowsCapture) copyContiguousBuffer(pts int64) (*av.Frame, error) {
	if err := hr(d.call(d.buffer, 3, ptr(&d.data), ptr(&d.maximum), ptr(&d.length))); err != nil {
		return nil, err
	}
	defer d.call(d.buffer, 4)
	if d.data == 0 || d.length > d.maximum || d.length > 128<<20 || uintptr(d.length) > ^uintptr(0)-d.data {
		return nil, errors.New("cameraav: invalid sample buffer")
	}
	// IMFMediaBuffer::Lock returns the contiguous representation, even for
	// a 2D buffer whose native pitch has padding. NV12/YUY2 use minimum stride.
	stride := d.config.Width
	if !d.nv12 {
		stride *= 2
	}
	return d.copyPixels(pts, unsafe.Slice((*byte)(nativePointer(d.data)), int(d.length)), stride)
}

func (d *windowsCapture) copyPixels(pts int64, data []byte, stride int) (*av.Frame, error) {
	if d.nv12 {
		offset := stride * d.config.Height
		if offset > len(data) {
			return nil, errors.New("cameraav: short NV12 buffer")
		}
		return copyNV12(d.config, pts, data[:offset], data[offset:], stride, stride)
	}
	return copyYUYV(d.config, pts, data, stride)
}
func (d *windowsCapture) cleanup() error {
	if d.cb != nil {
		d.cb.close()
	}
	var err error
	if d.source != 0 {
		err = hr(d.call(d.source, 12))
	}
	d.release(d.reader)
	d.release(d.source)
	d.reader = 0
	d.source = 0
	d.release(d.callback) // final Release also closes and drains the callback
	d.callback = 0
	return err
}
func (d *windowsCapture) Close() error {
	d.once.Do(func() { close(d.stop) })
	<-d.done
	return d.closeErr
}
