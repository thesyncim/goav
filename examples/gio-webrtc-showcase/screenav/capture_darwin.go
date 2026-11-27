//go:build darwin && (amd64 || arm64)

package screenav

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
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
	image, timing                                                                        uintptr
	lock, unlock, width, height, format, planeCount, planeBase, planeStride, planeHeight uintptr
	queueCreate, queueSync, dispatchRelease, semCreate, semWait, semSignal               uintptr

	cgMainDisplay       func() uint32
	cgGetActiveDisplays func(uint32, *uint32, *uint32) int32
	cgDisplayWide       func(uint32) uintptr
	cgDisplayHigh       func(uint32) uintptr
	cgPreflightAccess   func() bool
	cgRequestAccess     func() bool

	pixelKey, widthKey, heightKey objc.ID
	runtimeError, stopped, errKey objc.ID
	delegate                      objc.Class
	calloc                        func(uintptr, uintptr) unsafe.Pointer
	noop                          uintptr
}

var (
	mac     darwinAPI
	macOnce sync.Once
	macErr  error
)

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
		bind("/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation", map[string]*uintptr{
			"CFRetain":  &mac.retain,
			"CFRelease": &mac.release,
		})
		bind("/System/Library/Frameworks/CoreMedia.framework/CoreMedia", map[string]*uintptr{
			"CMSampleBufferGetImageBuffer":      &mac.image,
			"CMSampleBufferGetSampleTimingInfo": &mac.timing,
		})
		cv := bind("/System/Library/Frameworks/CoreVideo.framework/CoreVideo", map[string]*uintptr{
			"CVPixelBufferLockBaseAddress":       &mac.lock,
			"CVPixelBufferUnlockBaseAddress":     &mac.unlock,
			"CVPixelBufferGetWidth":              &mac.width,
			"CVPixelBufferGetHeight":             &mac.height,
			"CVPixelBufferGetPixelFormatType":    &mac.format,
			"CVPixelBufferGetPlaneCount":         &mac.planeCount,
			"CVPixelBufferGetBaseAddressOfPlane": &mac.planeBase,
			"CVPixelBufferGetBytesPerRowOfPlane": &mac.planeStride,
			"CVPixelBufferGetHeightOfPlane":      &mac.planeHeight,
		})
		cg := bind("/System/Library/Frameworks/CoreGraphics.framework/CoreGraphics", nil)
		af := bind("/System/Library/Frameworks/AVFoundation.framework/AVFoundation", nil)
		sys := bind("/usr/lib/libSystem.B.dylib", map[string]*uintptr{
			"free":                      &mac.free,
			"dispatch_queue_create":     &mac.queueCreate,
			"dispatch_sync_f":           &mac.queueSync,
			"dispatch_release":          &mac.dispatchRelease,
			"dispatch_semaphore_create": &mac.semCreate,
			"dispatch_semaphore_wait":   &mac.semWait,
			"dispatch_semaphore_signal": &mac.semSignal,
		})
		if macErr != nil {
			return
		}
		purego.RegisterLibFunc(&mac.calloc, sys, "calloc")
		purego.RegisterLibFunc(&mac.cgMainDisplay, cg, "CGMainDisplayID")
		purego.RegisterLibFunc(&mac.cgGetActiveDisplays, cg, "CGGetActiveDisplayList")
		purego.RegisterLibFunc(&mac.cgDisplayWide, cg, "CGDisplayPixelsWide")
		purego.RegisterLibFunc(&mac.cgDisplayHigh, cg, "CGDisplayPixelsHigh")
		purego.RegisterLibFunc(&mac.cgPreflightAccess, cg, "CGPreflightScreenCaptureAccess")
		purego.RegisterLibFunc(&mac.cgRequestAccess, cg, "CGRequestScreenCaptureAccess")

		constant := func(lib uintptr, name string) objc.ID {
			p, e := purego.Dlsym(lib, name)
			if e != nil {
				macErr = e
				return 0
			}
			return *(*objc.ID)(nativePointer(p))
		}
		mac.pixelKey = constant(cv, "kCVPixelBufferPixelFormatTypeKey")
		mac.widthKey = constant(cv, "kCVPixelBufferWidthKey")
		mac.heightKey = constant(cv, "kCVPixelBufferHeightKey")
		mac.runtimeError = constant(af, "AVCaptureSessionRuntimeErrorNotification")
		mac.stopped = constant(af, "AVCaptureSessionDidStopRunningNotification")
		mac.errKey = constant(af, "AVCaptureSessionErrorKey")
		if macErr != nil {
			return
		}
		mac.delegate, macErr = objc.RegisterClass(
			"GoAVScreenCaptureDelegateV1",
			objc.GetClass("NSObject"),
			[]*objc.Protocol{objc.GetProtocol("AVCaptureVideoDataOutputSampleBufferDelegate")},
			[]objc.FieldDef{{Name: "state", Type: reflect.TypeFor[uintptr](), Attribute: objc.ReadWrite}},
			[]objc.MethodDef{
				{
					Cmd: sel("captureOutput:didOutputSampleBuffer:fromConnection:"),
					Fn: func(self objc.ID, _cmd objc.SEL, _out, sample, _conn objc.ID) {
						statePtr := objc.Send[uintptr](self, sel("state"))
						if statePtr == 0 || sample == 0 {
							return
						}
						st := (*nativeSample)(nativePointer(statePtr))
						nativeCall(st.retain, uintptr(sample))
						if old := atomic.SwapUintptr(&st.sample, uintptr(sample)); old != 0 {
							nativeCall(st.release, old)
						} else {
							nativeCall(st.signal, st.semaphore)
						}
					},
				},
			},
		)
		if macErr != nil {
			return
		}
		mac.noop = purego.NewCallback(func(uintptr) {})
	})
	return macErr
}

func hasAccess() bool {
	if err := loadMac(); err != nil {
		return false
	}
	return mac.cgPreflightAccess()
}

func requestAccess() bool {
	if err := loadMac(); err != nil {
		return false
	}
	return mac.cgRequestAccess()
}

func displays(ctx context.Context) ([]Display, error) {
	if err := loadMac(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var ids [16]uint32
	var count uint32
	if rc := mac.cgGetActiveDisplays(uint32(len(ids)), &ids[0], &count); rc != 0 {
		return nil, fmt.Errorf("screenav: CGGetActiveDisplayList failed (%d)", rc)
	}
	mainID := mac.cgMainDisplay()
	out := make([]Display, 0, count)
	for i := uint32(0); i < count; i++ {
		id := ids[i]
		w := int(mac.cgDisplayWide(id))
		h := int(mac.cgDisplayHigh(id))
		name := fmt.Sprintf("Display %d (%dx%d)", i+1, w, h)
		if id == mainID {
			name = fmt.Sprintf("Main Display (%dx%d)", w, h)
		}
		out = append(out, Display{
			ID:     id,
			Name:   name,
			Width:  w,
			Height: h,
			Main:   id == mainID,
		})
	}
	return out, nil
}

type macScreenCapture struct {
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

func openDisplay(ctx context.Context, c Config) (captureDevice, error) {
	if err := loadMac(); err != nil {
		return nil, err
	}
	if !mac.cgPreflightAccess() {
		return nil, fmt.Errorf("%w: grant Screen Recording access in System Settings or click Grant Screen Access", ErrPermission)
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	p := pool()
	defer releaseObject(p)

	displayID := c.DisplayID
	if displayID == 0 {
		displayID = mac.cgMainDisplay()
	}
	if displayID == 0 {
		return nil, ErrNoDisplay
	}

	d := &macScreenCapture{config: c, errors: make(chan error, 1)}
	ok := false
	defer func() {
		if !ok {
			_ = d.Close()
		}
	}()

	d.session = object(object(class("AVCaptureSession"), "alloc"), "init")
	screenInput := object(object(class("AVCaptureScreenInput"), "alloc"), "initWithDisplayID:", displayID)
	if screenInput == 0 {
		return nil, fmt.Errorf("%w: AVCaptureScreenInput unavailable for display %d", ErrUnsupported, displayID)
	}
	defer releaseObject(screenInput)

	minDur := cmTime{Value: 1, Scale: int32(c.FPS), Flags: 1}
	object(screenInput, "setMinFrameDuration:", minDur)
	object(screenInput, "setCapturesCursor:", c.CaptureCursor)
	object(screenInput, "setCapturesMouseClicks:", false)

	d.output = object(object(class("AVCaptureVideoDataOutput"), "alloc"), "init")
	object(d.output, "setAlwaysDiscardsLateVideoFrames:", true)

	// Request hardware-converted NV12 at target dimensions.
	settings := object(class("NSMutableDictionary"), "dictionaryWithCapacity:", uintptr(3))
	pixel := object(class("NSNumber"), "numberWithUnsignedInt:", uint32(0x34323076)) // '420v' NV12
	widthNum := object(class("NSNumber"), "numberWithInt:", int32(c.Width))
	heightNum := object(class("NSNumber"), "numberWithInt:", int32(c.Height))
	object(settings, "setObject:forKey:", pixel, mac.pixelKey)
	object(settings, "setObject:forKey:", widthNum, mac.widthKey)
	object(settings, "setObject:forKey:", heightNum, mac.heightKey)
	object(d.output, "setVideoSettings:", settings)

	object(d.session, "beginConfiguration")
	if !objc.Send[bool](d.session, sel("canAddInput:"), screenInput) || !objc.Send[bool](d.session, sel("canAddOutput:"), d.output) {
		object(d.session, "commitConfiguration")
		return nil, ErrUnsupported
	}
	object(d.session, "addInput:", screenInput)
	object(d.session, "addOutput:", d.output)
	object(d.session, "commitConfiguration")

	d.pending = (*nativeSample)(mac.calloc(1, unsafe.Sizeof(nativeSample{})))
	if d.pending == nil {
		return nil, errors.New("screenav: native state allocation failed")
	}
	d.pending.retain = mac.retain
	d.pending.release = mac.release
	d.pending.signal = mac.semSignal
	d.pending.semaphore = nativeCall(mac.semCreate, 0)
	d.queue = nativeCall(mac.queueCreate, 0, 0)
	if d.queue == 0 || d.pending.semaphore == 0 {
		return nil, errors.New("screenav: native queue allocation failed")
	}

	d.delegate = object(object(objc.ID(mac.delegate), "alloc"), "init")
	object(d.delegate, "setState:", uintptr(unsafe.Pointer(d.pending)))
	object(d.output, "setSampleBufferDelegate:queue:", d.delegate, d.queue)
	d.observeSession()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ok = true
	return d, nil
}

func (d *macScreenCapture) observeSession() {
	center := object(class("NSNotificationCenter"), "defaultCenter")
	for _, name := range []objc.ID{mac.runtimeError, mac.stopped} {
		block := objc.NewBlock(func(_ objc.Block, note objc.ID) {
			d.notifyMu.Lock()
			defer d.notifyMu.Unlock()
			if d.closing {
				return
			}
			reason := "screen capture session stopped"
			info := object(note, "userInfo")
			if info != 0 {
				if e := object(info, "objectForKey:", mac.errKey); e != 0 {
					reason = goString(object(e, "localizedDescription"))
				}
			}
			select {
			case d.errors <- fmt.Errorf("screenav: %s", reason):
			default:
			}
			nativeCall(mac.semSignal, d.pending.semaphore)
		})
		observer := object(center, "addObserverForName:object:queue:usingBlock:", name, d.session, objc.ID(0), block)
		block.Release()
		if observer != 0 {
			d.notifications = append(d.notifications, observer)
		}
	}
}

func (d *macScreenCapture) Run(ctx context.Context, emit func(*av.Frame)) error {
	var pins runtime.Pinner
	pins.Pin(d)
	defer pins.Unpin()

	wakeDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		nativeCall(mac.semSignal, d.pending.semaphore)
		close(wakeDone)
	})
	defer func() {
		if !stop() {
			<-wakeDone
		}
	}()

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	p := pool()
	object(d.session, "startRunning")
	releaseObject(p)

	const dispatchTimeForever = ^uintptr(0)
	for {
		nativeCall(mac.semWait, d.pending.semaphore, dispatchTimeForever)
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case err := <-d.errors:
			return err
		default:
		}
		sample := atomic.SwapUintptr(&d.pending.sample, 0)
		if sample == 0 {
			continue
		}
		frame := d.copySample(sample)
		nativeCall(mac.release, sample)
		if frame != nil {
			emit(frame)
		}
	}
}

func (d *macScreenCapture) copySample(sample uintptr) *av.Frame {
	img := nativeCall(mac.image, sample)
	if img == 0 {
		return nil
	}
	pts := time.Now().UnixNano()
	if nativeCall(mac.timing, sample, 0, uintptr(unsafe.Pointer(&d.timing))) == 0 && d.timing.PTS.Scale > 0 && d.timing.PTS.Flags&1 != 0 {
		pts = (d.timing.PTS.Value * int64(time.Second)) / int64(d.timing.PTS.Scale)
	}
	const lockReadOnly = 1
	if nativeCall(mac.lock, img, lockReadOnly) != 0 {
		return nil
	}
	defer nativeCall(mac.unlock, img, lockReadOnly)

	w := int(nativeCall(mac.width, img))
	h := int(nativeCall(mac.height, img))
	planes := int(nativeCall(mac.planeCount, img))
	if planes < 2 || w < 2 || h < 2 {
		return nil
	}
	yBase := nativeCall(mac.planeBase, img, 0)
	uvBase := nativeCall(mac.planeBase, img, 1)
	if yBase == 0 || uvBase == 0 {
		return nil
	}
	yStride := int(nativeCall(mac.planeStride, img, 0))
	uvStride := int(nativeCall(mac.planeStride, img, 1))
	yH := int(nativeCall(mac.planeHeight, img, 0))
	uvH := int(nativeCall(mac.planeHeight, img, 1))
	if yH < h || uvH < h/2 {
		return nil
	}
	ySlice := unsafe.Slice((*byte)(nativePointer(yBase)), yStride*yH)
	uvSlice := unsafe.Slice((*byte)(nativePointer(uvBase)), uvStride*uvH)
	return copyNV12(d.config, pts, w, h, ySlice, uvSlice, yStride, uvStride)
}

func (d *macScreenCapture) Close() error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	p := pool()
	defer releaseObject(p)

	d.notifyMu.Lock()
	d.closing = true
	d.notifyMu.Unlock()

	if len(d.notifications) > 0 {
		center := object(class("NSNotificationCenter"), "defaultCenter")
		for _, obs := range d.notifications {
			object(center, "removeObserver:", obs)
		}
		d.notifications = nil
	}
	if d.session != 0 {
		object(d.session, "stopRunning")
	}
	if d.output != 0 {
		object(d.output, "setSampleBufferDelegate:queue:", objc.ID(0), uintptr(0))
	}
	if d.queue != 0 {
		nativeCall(mac.queueSync, d.queue, 0, mac.noop)
		nativeCall(mac.dispatchRelease, d.queue)
		d.queue = 0
	}
	if d.pending != nil {
		if old := atomic.SwapUintptr(&d.pending.sample, 0); old != 0 {
			nativeCall(mac.release, old)
		}
		if d.pending.semaphore != 0 {
			nativeCall(mac.dispatchRelease, d.pending.semaphore)
		}
		nativeCall(mac.free, uintptr(unsafe.Pointer(d.pending)))
		d.pending = nil
	}
	if d.delegate != 0 {
		object(d.delegate, "setState:", uintptr(0))
		releaseObject(d.delegate)
		d.delegate = 0
	}
	releaseObject(d.output)
	d.output = 0
	releaseObject(d.session)
	d.session = 0
	return nil
}
