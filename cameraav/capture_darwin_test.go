//go:build darwin && (amd64 || arm64)

package cameraav

import (
	"github.com/ebitengine/purego"
	"github.com/ebitengine/purego/objc"
	"runtime"
	"sync/atomic"
	"testing"
	"unsafe"
)

// Exercise the actual C ABI without activating a camera or asking permission.
func TestNativeCaptureBridgeZeroAllocs(t *testing.T) {
	if err := loadMac(); err != nil {
		t.Fatal(err)
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	p := pool()
	defer releaseObject(p)
	state := (*nativeSample)(mac.calloc(1, unsafe.Sizeof(nativeSample{})))
	defer nativeCall(mac.free, uintptr(unsafe.Pointer(state)))
	state.retain = mac.retain
	state.release = mac.release
	state.signal = mac.semSignal
	state.semaphore = nativeCall(mac.semCreate, 0)
	defer nativeCall(mac.dispatchRelease, state.semaphore)
	delegate := object(object(objc.ID(mac.delegate), "alloc"), "init")
	defer releaseObject(delegate)
	object(delegate, "setState:", uintptr(unsafe.Pointer(state)))
	sample := nsString("bridge-test-object")
	deliver := func() { nativeCall(nativeCaptureAddress(), uintptr(delegate), 0, 0, uintptr(sample), 0) }
	deliver()
	deliver()
	if got := atomic.SwapUintptr(&state.sample, 0); got != uintptr(sample) {
		t.Fatalf("sample=%x", got)
	} else {
		nativeCall(mac.release, got)
	}
	nativeCall(mac.semWait, state.semaphore, 0)
	if raceEnabled {
		return
	} // The race runtime deliberately discards sync.Pool entries.
	allocs := testing.AllocsPerRun(1000, func() {
		deliver()
		nativeCall(mac.semWait, state.semaphore, 0)
		got := atomic.SwapUintptr(&state.sample, 0)
		nativeCall(mac.release, got)
	})
	if allocs != 0 {
		t.Fatalf("native callback handoff = %g allocations/frame", allocs)
	}
}

// Real CoreVideo/CoreMedia allocations are setup-only; repeated read/copy uses
// the production pixel-buffer path with no camera permission or image capture.
func TestNativePixelBufferCopyZeroAllocs(t *testing.T) {
	if err := loadMac(); err != nil {
		t.Fatal(err)
	}
	cv, err := purego.Dlopen("/System/Library/Frameworks/CoreVideo.framework/CoreVideo", purego.RTLD_NOW)
	if err != nil {
		t.Fatal(err)
	}
	cm, err := purego.Dlopen("/System/Library/Frameworks/CoreMedia.framework/CoreMedia", purego.RTLD_NOW)
	if err != nil {
		t.Fatal(err)
	}
	symbol := func(lib uintptr, name string) uintptr {
		p, e := purego.Dlsym(lib, name)
		if e != nil {
			t.Fatal(e)
		}
		return p
	}
	c := testConfig(640, 480)
	defer c.buffers.close()
	var pixel, description, sample uintptr
	var pins runtime.Pinner
	pins.Pin(&pixel)
	pins.Pin(&description)
	pins.Pin(&sample)
	defer pins.Unpin()
	if status := nativeCall(symbol(cv, "CVPixelBufferCreate"), 0, uintptr(c.Width), uintptr(c.Height), 0x34323076, 0, uintptr(unsafe.Pointer(&pixel))); status != 0 {
		t.Fatalf("create pixel=%d", status)
	}
	defer nativeCall(mac.release, pixel)
	if status := nativeCall(symbol(cm, "CMVideoFormatDescriptionCreateForImageBuffer"), 0, pixel, uintptr(unsafe.Pointer(&description))); status != 0 {
		t.Fatalf("format=%d", status)
	}
	defer nativeCall(mac.release, description)
	timing := cmTiming{Duration: cmTime{Value: 1, Scale: 30, Flags: 1}, PTS: cmTime{Value: 42, Scale: 30, Flags: 1}}
	pins.Pin(&timing)
	if status := nativeCall(symbol(cm, "CMSampleBufferCreateForImageBuffer"), 0, pixel, 1, 0, 0, description, uintptr(unsafe.Pointer(&timing)), uintptr(unsafe.Pointer(&sample))); status != 0 {
		t.Fatalf("sample=%d", status)
	}
	defer nativeCall(mac.release, sample)
	d := &macCapture{config: c}
	pins.Pin(d)
	f, err := d.copySample(sample)
	if err != nil {
		t.Fatal(err)
	}
	if f.PTS.Value != 1400000000 {
		t.Fatal(f.PTS)
	}
	releaseFrame(f)
	if raceEnabled {
		return
	}
	allocs := testing.AllocsPerRun(100, func() {
		f, e := d.copySample(sample)
		if e != nil {
			panic(e)
		}
		releaseFrame(f)
	})
	if allocs != 0 {
		t.Fatalf("CoreMedia sample -> owned Go frame = %g allocations/frame", allocs)
	}
}
