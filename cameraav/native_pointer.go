package cameraav

import "unsafe"

// nativePointer interprets an address returned by an OS API. It must never be
// used to hide a Go pointer from the collector. Ownership, bounds and validity
// of these native allocations are checked at their call sites.
func nativePointer(address uintptr) unsafe.Pointer {
	return *(*unsafe.Pointer)(unsafe.Pointer(&address))
}
