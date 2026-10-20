//go:build windows && (amd64 || arm64)

package cameraav

import (
	"runtime"
	"testing"
	"unsafe"
)

func TestMediaFoundationCallbackHandoff(t *testing.T) {
	obj, cb, err := newCallback()
	if err != nil {
		t.Fatal(err)
	}
	defer comRelease(obj)
	deliver := func() {
		if result := com(obj, 3, 0, firstVideo, 0, 123, 0); result != 0 {
			panic(result)
		}
		s := <-cb.samples
		if s.pts != 123 || s.sample != 0 {
			panic("callback ABI")
		}
	}
	deliver()
	if !raceEnabled {
		if n := testing.AllocsPerRun(1000, deliver); n != 0 {
			t.Fatalf("callback handoff=%g allocations", n)
		}
	}
}
func TestMediaFoundationLoadsWithoutCamera(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if err := startMF(); err != nil {
		t.Fatal(err)
	}
	stopMF()
}

// Real Media Foundation objects exercise COM calls and the copy path without
// camera permission, hardware, or synthetic Go implementations of COM methods.
func TestMediaFoundationSampleCopyZeroAllocs(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if err := startMF(); err != nil {
		t.Fatal(err)
	}
	defer stopMF()
	createBuffer, createSample := mfplat.NewProc("MFCreateMemoryBuffer"), mfplat.NewProc("MFCreateSample")
	if err := createBuffer.Find(); err != nil {
		t.Fatal(err)
	}
	if err := createSample.Find(); err != nil {
		t.Fatal(err)
	}
	c := testConfig(4, 2)
	defer c.buffers.close()
	d := &windowsCapture{config: c, stride: 4, nv12: true}
	var buffer, sample uintptr
	var pins runtime.Pinner
	pins.Pin(d)
	pins.Pin(&buffer)
	pins.Pin(&sample)
	defer pins.Unpin()
	result, _, _ := createBuffer.Call(12, ptr(&buffer))
	if err := hr(result); err != nil {
		t.Fatal(err)
	}
	defer comRelease(buffer)
	result, _, _ = createSample.Call(ptr(&sample))
	if err := hr(result); err != nil {
		t.Fatal(err)
	}
	defer comRelease(sample)
	if err := hr(com(buffer, 3, ptr(&d.data), ptr(&d.maximum), ptr(&d.length))); err != nil {
		t.Fatal(err)
	}
	copy(unsafe.Slice((*byte)(nativePointer(d.data)), 12), []byte{1, 2, 3, 4, 5, 6, 7, 8, 10, 20, 11, 21})
	if err := hr(com(buffer, 4)); err != nil {
		t.Fatal(err)
	}
	if err := hr(com(buffer, 6, 12)); err != nil {
		t.Fatal(err)
	} // SetCurrentLength
	if err := hr(com(sample, 42, buffer)); err != nil {
		t.Fatal(err)
	} // AddBuffer
	copyFrame := func() {
		f, err := d.copySample(sample, 123)
		if err != nil {
			panic(err)
		}
		if f.PTS.Value != 12300 || f.Planes[0].Buffer.Bytes[7] != 8 || f.Planes[1].Buffer.Bytes[1] != 11 || f.Planes[2].Buffer.Bytes[1] != 21 {
			panic("native sample copy")
		}
		releaseFrame(f)
	}
	copyFrame()
	if !raceEnabled {
		if n := testing.AllocsPerRun(1000, copyFrame); n != 0 {
			t.Fatalf("native sample copy=%g allocations", n)
		}
	}
}
