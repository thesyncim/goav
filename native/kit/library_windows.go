//go:build windows

package kit

import (
	"syscall"
	"unsafe"
)

// The loader flags: resolve the library's own dependencies from its directory
// and System32 only, never the working directory or PATH.
const (
	loadLibrarySearchDLLLoadDir = 0x00000100
	loadLibrarySearchSystem32   = 0x00000800
)

var loadLibraryExW = syscall.NewLazyDLL("kernel32.dll").NewProc("LoadLibraryExW")

func open(path string) (uintptr, error) {
	name, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	handle, _, callErr := loadLibraryExW.Call(uintptr(unsafe.Pointer(name)), 0, loadLibrarySearchDLLLoadDir|loadLibrarySearchSystem32)
	if handle == 0 {
		return 0, callErr
	}
	return handle, nil
}

func lookup(handle uintptr, name string) (uintptr, error) {
	return syscall.GetProcAddress(syscall.Handle(handle), name)
}
