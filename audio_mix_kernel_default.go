//go:build !arm64 && !amd64

package goav

func mixS16ArchKernel() (mixS16Kernel, bool) {
	return mixS16Kernel{}, false
}
