//go:build windows && (amd64 || arm64)

package cameraav

import (
	"runtime"
	"testing"
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
