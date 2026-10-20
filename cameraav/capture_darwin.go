//go:build darwin && (amd64 || arm64)

package cameraav

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/ebitengine/purego/objc"
	"github.com/thesyncim/goav/av"
)

type cmTime struct {
	Value int64
	Scale int32
	Flags uint32
	Epoch int64
}
type cmTiming struct{ Duration, PTS, DTS cmTime }
type darwinAPI struct {
	retain, release, free                                                                uintptr
	image, timing, dimensions                                                            uintptr
	lock, unlock, width, height, format, planeCount, planeBase, planeStride, planeHeight uintptr
	queueCreate, queueSync, dispatchRelease, semCreate, semWait, semSignal               uintptr
	mediaVideo, pixelKey, inputPriority                                                  objc.ID
	delegate                                                                             objc.Class
	calloc                                                                               func(uintptr, uintptr) unsafe.Pointer
	noop                                                                                 uintptr
}

var mac darwinAPI
var macOnce sync.Once
var macErr error

// purego.SyscallN retains its argument slice for the duration of the native
// call. Reuse owned argument storage rather than escaping a new variadic array
// on every pixel-buffer operation. Concurrent callbacks borrow different slots.
var nativeArguments = sync.Pool{New: func() any { return new([16]uintptr) }}

func nativeCall(fn uintptr, args ...uintptr) uintptr {
	storage := nativeArguments.Get().(*[16]uintptr)
	copy(storage[:], args)
	result, _, _ := purego.SyscallN(fn, storage[:len(args)]...)
	nativeArguments.Put(storage)
	return result
}

func sel(name string) objc.SEL { return objc.RegisterName(name) }
func object(id objc.ID, name string, args ...any) objc.ID {
	return objc.Send[objc.ID](id, sel(name), args...)
}
func class(name string) objc.ID    { return objc.ID(objc.GetClass(name)) }
func nsString(text string) objc.ID { return object(class("NSString"), "stringWithUTF8String:", text) }
func goString(value objc.ID) string {
	p := objc.Send[unsafe.Pointer](value, sel("UTF8String"))
	if p == nil {
		return ""
	}
	n := 0
	for n < 1<<20 && *(*byte)(unsafe.Add(p, n)) != 0 {
		n++
	}
	return string(unsafe.Slice((*byte)(p), n))
}
func pool() objc.ID { return object(object(class("NSAutoreleasePool"), "alloc"), "init") }
func releaseObject(obj objc.ID) {
	if obj != 0 {
		object(obj, "release")
	}
}
func loadMac() error {
	macOnce.Do(func() {
		bind := func(lib string, syms map[string]*uintptr) uintptr {
			if macErr != nil {
				return 0
			}
			h, err := purego.Dlopen(lib, purego.RTLD_NOW|purego.RTLD_GLOBAL)
			if err != nil {
				macErr = err
				return 0
			}
			for name, target := range syms {
				*target, err = purego.Dlsym(h, name)
				if err != nil {
					macErr = err
					return 0
				}
			}
			return h
		}
		cf := bind("/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation", map[string]*uintptr{"CFRetain": &mac.retain, "CFRelease": &mac.release})
		_ = cf
		bind("/System/Library/Frameworks/CoreMedia.framework/CoreMedia", map[string]*uintptr{"CMSampleBufferGetImageBuffer": &mac.image, "CMSampleBufferGetSampleTimingInfo": &mac.timing, "CMVideoFormatDescriptionGetDimensions": &mac.dimensions})
		cv := bind("/System/Library/Frameworks/CoreVideo.framework/CoreVideo", map[string]*uintptr{"CVPixelBufferLockBaseAddress": &mac.lock, "CVPixelBufferUnlockBaseAddress": &mac.unlock, "CVPixelBufferGetWidth": &mac.width, "CVPixelBufferGetHeight": &mac.height, "CVPixelBufferGetPixelFormatType": &mac.format, "CVPixelBufferGetPlaneCount": &mac.planeCount, "CVPixelBufferGetBaseAddressOfPlane": &mac.planeBase, "CVPixelBufferGetBytesPerRowOfPlane": &mac.planeStride, "CVPixelBufferGetHeightOfPlane": &mac.planeHeight})
		af := bind("/System/Library/Frameworks/AVFoundation.framework/AVFoundation", nil)
		sys := bind("/usr/lib/libSystem.B.dylib", map[string]*uintptr{"free": &mac.free, "dispatch_queue_create": &mac.queueCreate, "dispatch_sync_f": &mac.queueSync, "dispatch_release": &mac.dispatchRelease, "dispatch_semaphore_create": &mac.semCreate, "dispatch_semaphore_wait": &mac.semWait, "dispatch_semaphore_signal": &mac.semSignal})
		if macErr != nil {
			return
		}
		purego.RegisterLibFunc(&mac.calloc, sys, "calloc")
		constant := func(lib uintptr, name string) objc.ID {
			p, e := purego.Dlsym(lib, name)
			if e != nil {
				macErr = e
				return 0
			}
			return *(*objc.ID)(nativePointer(p))
		}
		mac.mediaVideo = constant(af, "AVMediaTypeVideo")
		mac.inputPriority = constant(af, "AVCaptureSessionPresetInputPriority")
		mac.pixelKey = constant(cv, "kCVPixelBufferPixelFormatTypeKey")
		if macErr != nil {
			return
		}
		mac.delegate, macErr = objc.RegisterClass("GoAVCameraCaptureDelegateV1", objc.GetClass("NSObject"), []*objc.Protocol{objc.GetProtocol("AVCaptureVideoDataOutputSampleBufferDelegate")}, []objc.FieldDef{{Name: "state", Type: reflect.TypeFor[uintptr](), Attribute: objc.ReadWrite}}, nil)
		if macErr != nil {
			return
		}
		if mac.delegate.InstanceVariable("state").Offset() != 8 {
			macErr = errors.New("cameraav: unexpected Objective-C object layout")
			return
		}
		if !mac.delegate.AddMethod(sel("captureOutput:didOutputSampleBuffer:fromConnection:"), objc.IMP(nativeCaptureAddress()), "v@:@@@") {
			macErr = errors.New("cameraav: cannot install capture delegate")
			return
		}
		mac.noop = purego.NewCallback(func(uintptr) {}) // shutdown barrier only, never per-frame.
	})
	return macErr
}
func devices(ctx context.Context) ([]Device, error) {
	if err := loadMac(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	p := pool()
	defer releaseObject(p)
	list := object(class("AVCaptureDevice"), "devicesWithMediaType:", mac.mediaVideo)
	result := make([]Device, 0, int(objc.Send[uintptr](list, sel("count"))))
	for i := uintptr(0); i < objc.Send[uintptr](list, sel("count")); i++ {
		d := object(list, "objectAtIndex:", i)
		result = append(result, Device{ID: goString(object(d, "uniqueID")), Name: goString(object(d, "localizedName"))})
	}
	return result, nil
}
func requestAccess(ctx context.Context) error {
	if err := loadMac(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	runtime.LockOSThread()
	p := pool()
	status := objc.Send[int64](class("AVCaptureDevice"), sel("authorizationStatusForMediaType:"), mac.mediaVideo)
	if status == 3 {
		releaseObject(p)
		runtime.UnlockOSThread()
		return nil
	}
	if status != 0 {
		releaseObject(p)
		runtime.UnlockOSThread()
		return ErrPermission
	}
	bundle := object(class("NSBundle"), "mainBundle")
	usage := object(bundle, "objectForInfoDictionaryKey:", nsString("NSCameraUsageDescription"))
	if usage == 0 || objc.Send[uintptr](usage, sel("length")) == 0 {
		releaseObject(p)
		runtime.UnlockOSThread()
		return fmt.Errorf("%w: app Info.plist needs NSCameraUsageDescription; use the packaged goav camera CLI", ErrPermission)
	}
	done := make(chan bool, 1)
	block := objc.NewBlock(func(_ objc.Block, granted bool) { done <- granted })
	object(class("AVCaptureDevice"), "requestAccessForMediaType:completionHandler:", mac.mediaVideo, block)
	block.Release()
	releaseObject(p)
	runtime.UnlockOSThread()
	select {
	case granted := <-done:
		if granted {
			return nil
		}
		return ErrPermission
	case <-ctx.Done():
		return ctx.Err()
	}
}

type macCapture struct {
	config                    Config
	session, output, delegate objc.ID
	queue                     uintptr
	pending                   *nativeSample
	timing                    cmTiming
	notifications             []objc.ID
	errors                    chan error
	notifyMu                  sync.Mutex
	closing                   bool
}

func openDevice(ctx context.Context, c Config) (captureDevice, error) {
	if err := loadMac(); err != nil {
		return nil, err
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	p := pool()
	defer releaseObject(p)
	if objc.Send[int64](class("AVCaptureDevice"), sel("authorizationStatusForMediaType:"), mac.mediaVideo) != 3 {
		return nil, fmt.Errorf("%w: run goav cameras --request-access or enable camera access in System Settings", ErrPermission)
	}
	var device objc.ID
	if c.Device == "" {
		device = object(class("AVCaptureDevice"), "defaultDeviceWithMediaType:", mac.mediaVideo)
	} else {
		device = object(class("AVCaptureDevice"), "deviceWithUniqueID:", nsString(c.Device))
	}
	if device == 0 {
		return nil, ErrNoDevice
	}
	var selected objc.ID
	formats := object(device, "formats")
	for i := uintptr(0); i < objc.Send[uintptr](formats, sel("count")); i++ {
		f := object(formats, "objectAtIndex:", i)
		desc := object(f, "formatDescription")
		dims := nativeCall(mac.dimensions, uintptr(desc))
		if int(uint32(dims)) != c.Width || int(uint32(dims>>32)) != c.Height {
			continue
		}
		ranges := object(f, "videoSupportedFrameRateRanges")
		for j := uintptr(0); j < objc.Send[uintptr](ranges, sel("count")); j++ {
			r := object(ranges, "objectAtIndex:", j)
			if float64(c.FPS) >= objc.Send[float64](r, sel("minFrameRate")) && float64(c.FPS) <= objc.Send[float64](r, sel("maxFrameRate")) {
				selected = f
				break
			}
		}
		if selected != 0 {
			break
		}
	}
	if selected == 0 {
		return nil, fmt.Errorf("%w: %dx%d at %d fps", ErrUnsupported, c.Width, c.Height, c.FPS)
	}
	d := &macCapture{config: c, errors: make(chan error, 1)}
	ok := false
	defer func() {
		if !ok {
			_ = d.Close()
		}
	}()
	d.session = object(object(class("AVCaptureSession"), "alloc"), "init")
	var nativeErr objc.ID
	input := object(class("AVCaptureDeviceInput"), "deviceInputWithDevice:error:", device, &nativeErr)
	if input == 0 {
		return nil, fmt.Errorf("cameraav: input: %s", goString(object(nativeErr, "localizedDescription")))
	}
	d.output = object(object(class("AVCaptureVideoDataOutput"), "alloc"), "init")
	object(d.output, "setAlwaysDiscardsLateVideoFrames:", true)
	pixel := object(class("NSNumber"), "numberWithUnsignedInt:", uint32(0x34323076)) // video-range NV12 (420v).
	settings := object(class("NSDictionary"), "dictionaryWithObject:forKey:", pixel, mac.pixelKey)
	object(d.output, "setVideoSettings:", settings)
	object(d.session, "beginConfiguration")
	if !objc.Send[bool](d.session, sel("canAddInput:"), input) || !objc.Send[bool](d.session, sel("canAddOutput:"), d.output) {
		object(d.session, "commitConfiguration")
		return nil, ErrUnsupported
	}
	object(d.session, "addInput:", input)
	object(d.session, "addOutput:", d.output)
	if !objc.Send[bool](d.session, sel("canSetSessionPreset:"), mac.inputPriority) {
		object(d.session, "commitConfiguration")
		return nil, ErrUnsupported
	}
	object(d.session, "setSessionPreset:", mac.inputPriority)
	if !objc.Send[bool](device, sel("lockForConfiguration:"), &nativeErr) {
		object(d.session, "commitConfiguration")
		return nil, fmt.Errorf("cameraav: lock device: %s", goString(object(nativeErr, "localizedDescription")))
	}
	object(device, "setActiveFormat:", selected)
	duration := cmTime{Value: 1, Scale: int32(c.FPS), Flags: 1}
	object(device, "setActiveVideoMinFrameDuration:", duration)
	object(device, "setActiveVideoMaxFrameDuration:", duration)
	object(device, "unlockForConfiguration")
	object(d.session, "commitConfiguration")
	d.pending = (*nativeSample)(mac.calloc(1, unsafe.Sizeof(nativeSample{})))
	if d.pending == nil {
		return nil, errors.New("cameraav: native state allocation failed")
	}
	d.pending.retain = mac.retain
	d.pending.release = mac.release
	d.pending.signal = mac.semSignal
	d.pending.semaphore = nativeCall(mac.semCreate, 0)
	d.queue = nativeCall(mac.queueCreate, 0, 0)
	if d.queue == 0 || d.pending.semaphore == 0 {
		return nil, errors.New("cameraav: native queue allocation failed")
	}
	d.delegate = object(object(objc.ID(mac.delegate), "alloc"), "init")
	object(d.delegate, "setState:", uintptr(unsafe.Pointer(d.pending)))
	object(d.output, "setSampleBufferDelegate:queue:", d.delegate, d.queue)
	center := object(class("NSNotificationCenter"), "defaultCenter")
	for _, name := range []string{"AVCaptureSessionRuntimeErrorNotification", "AVCaptureSessionWasInterruptedNotification"} {
		block := objc.NewBlock(func(_ objc.Block, note objc.ID) {
			d.notifyMu.Lock()
			defer d.notifyMu.Unlock()
			if d.closing {
				return
			}
			reason := "camera session interrupted"
			info := object(note, "userInfo")
			e := object(info, "objectForKey:", nsString("AVCaptureSessionErrorKey"))
			if e != 0 {
				reason = goString(object(e, "localizedDescription"))
			}
			select {
			case d.errors <- fmt.Errorf("cameraav: %s", reason):
			default:
			}
			nativeCall(mac.semSignal, d.pending.semaphore)
		})
		observer := object(center, "addObserverForName:object:queue:usingBlock:", nsString(name), d.session, objc.ID(0), block)
		block.Release()
		if observer != 0 {
			d.notifications = append(d.notifications, observer)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ok = true
	return d, nil
}
func (d *macCapture) Run(ctx context.Context, emit func(*av.Frame)) error {
	wakeDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { nativeCall(mac.semSignal, d.pending.semaphore); close(wakeDone) })
	defer func() {
		if !stop() {
			<-wakeDone
		}
	}()
	runtime.LockOSThread()
	p := pool()
	object(d.session, "startRunning")
	running := objc.Send[bool](d.session, sel("isRunning"))
	releaseObject(p)
	runtime.UnlockOSThread()
	if !running {
		return errors.New("cameraav: capture session did not start")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case err := <-d.errors:
			return err
		default:
		}
		nativeCall(mac.semWait, d.pending.semaphore, ^uintptr(0))
		sample := atomic.SwapUintptr(&d.pending.sample, 0)
		if sample == 0 {
			continue
		}
		frame, err := d.copySample(sample)
		nativeCall(mac.release, sample)
		if err != nil {
			return err
		}
		if frame != nil {
			emit(frame)
		}
	}
}
func (d *macCapture) copySample(sample uintptr) (*av.Frame, error) {
	buffer := nativeCall(mac.image, sample)
	if buffer == 0 || int(nativeCall(mac.width, buffer)) != d.config.Width || int(nativeCall(mac.height, buffer)) != d.config.Height || nativeCall(mac.format, buffer) != 0x34323076 || nativeCall(mac.planeCount, buffer) != 2 {
		return nil, ErrFormatChanged
	}
	if status := int32(nativeCall(mac.timing, sample, 0, uintptr(unsafe.Pointer(&d.timing)))); status != 0 {
		return nil, fmt.Errorf("cameraav: sample timing %d", status)
	}
	t := d.timing.PTS
	if t.Flags&1 == 0 || t.Flags&0x1c != 0 || t.Scale <= 0 {
		return nil, errors.New("cameraav: invalid capture timestamp")
	}
	pts, ok := av.RescaleValue(t.Value, av.TimeBase{Num: 1, Den: int64(t.Scale)}, nanoseconds)
	if !ok {
		return nil, errors.New("cameraav: invalid capture timebase")
	}
	if status := int32(nativeCall(mac.lock, buffer, 1)); status != 0 {
		return nil, fmt.Errorf("cameraav: pixel lock %d", status)
	}
	defer nativeCall(mac.unlock, buffer, 1)
	ys, uvs := int(nativeCall(mac.planeStride, buffer, 0)), int(nativeCall(mac.planeStride, buffer, 1))
	yh, uvh := int(nativeCall(mac.planeHeight, buffer, 0)), int(nativeCall(mac.planeHeight, buffer, 1))
	if ys < d.config.Width || uvs < d.config.Width || ys > 1<<20 || uvs > 1<<20 || yh != d.config.Height || uvh != d.config.Height/2 {
		return nil, ErrFormatChanged
	}
	y, uv := nativeCall(mac.planeBase, buffer, 0), nativeCall(mac.planeBase, buffer, 1)
	if y == 0 || uv == 0 {
		return nil, errors.New("cameraav: missing pixel planes")
	}
	return copyNV12(d.config, pts, unsafe.Slice((*byte)(nativePointer(y)), ys*yh), unsafe.Slice((*byte)(nativePointer(uv)), uvs*uvh), ys, uvs)
}
func (d *macCapture) Close() error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	p := pool()
	defer releaseObject(p)
	d.notifyMu.Lock()
	d.closing = true
	d.notifyMu.Unlock()
	if d.session != 0 {
		object(d.session, "stopRunning")
	}
	if d.output != 0 {
		object(d.output, "setSampleBufferDelegate:queue:", objc.ID(0), uintptr(0))
	}
	if d.queue != 0 {
		nativeCall(mac.queueSync, d.queue, 0, mac.noop)
	}
	center := object(class("NSNotificationCenter"), "defaultCenter")
	for _, n := range d.notifications {
		object(center, "removeObserver:", n)
	}
	d.notifications = nil
	releaseObject(d.delegate)
	releaseObject(d.output)
	releaseObject(d.session)
	d.delegate = 0
	d.output = 0
	d.session = 0
	if d.queue != 0 {
		nativeCall(mac.dispatchRelease, d.queue)
		d.queue = 0
	}
	if d.pending != nil {
		if sample := atomic.SwapUintptr(&d.pending.sample, 0); sample != 0 {
			nativeCall(mac.release, sample)
		}
		if d.pending.semaphore != 0 {
			nativeCall(mac.dispatchRelease, d.pending.semaphore)
		}
		nativeCall(mac.free, uintptr(unsafe.Pointer(d.pending)))
		d.pending = nil
	}
	return nil
}
