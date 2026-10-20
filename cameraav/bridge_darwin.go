//go:build darwin && (amd64 || arm64)

package cameraav

// nativeSample is allocated with calloc, never in the Go heap. The Objective-C
// delegate's first ivar points here. The ABI bridge retains a sample, atomically
// replaces the pending sample, releases the displaced sample, and signals only
// on an empty->occupied transition. It never enters Go or stores a Go pointer.
//
// The Go consumer atomically takes ownership and copies active pixels before
// releasing that sample. A slow consumer therefore retains at most one pending
// native sample, plus the sample it is currently copying.
type nativeSample struct {
	sample                             uintptr
	retain, release, signal, semaphore uintptr
}

func nativeCaptureAddress() uintptr
