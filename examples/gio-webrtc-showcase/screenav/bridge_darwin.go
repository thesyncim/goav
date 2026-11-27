//go:build darwin && (amd64 || arm64)

package screenav

import "unsafe"

type nativeSample struct {
	sample                             uintptr
	retain, release, signal, semaphore uintptr
}

func nativeCaptureAddress() uintptr

// nativePointer turns a C/ObjC symbol address returned by dlsym into unsafe.Pointer.
//
//go:nosplit
func nativePointer(addr uintptr) unsafe.Pointer {
	return *(*unsafe.Pointer)(unsafe.Pointer(&addr))
}
