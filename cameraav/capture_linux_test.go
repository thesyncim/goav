//go:build linux && (amd64 || arm64)

package cameraav

import (
	"testing"
	"unsafe"
)

func TestV4L2Layouts(t *testing.T) {
	if unsafe.Sizeof(v4lCapability{}) != 104 || unsafe.Sizeof(v4lFormat{}) != 208 || unsafe.Sizeof(v4lRequest{}) != 20 || unsafe.Sizeof(v4lBuffer{}) != 88 {
		t.Fatal("V4L2 ABI size mismatch")
	}
	b := v4lBuffer{}
	if unsafe.Offsetof(b.Time) != 24 || unsafe.Offsetof(b.Offset) != 64 || unsafe.Offsetof(b.Length) != 72 {
		t.Fatal("V4L2 ABI offset mismatch")
	}
}
