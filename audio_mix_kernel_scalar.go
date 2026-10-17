//go:build !goexperiment.simd || purego

package goav

// mixS16VectorKernel reports no vector kernel: default builds (no
// GOEXPERIMENT=simd) and purego builds mix with the scalar loop.
func mixS16VectorKernel() (mixS16Kernel, bool) {
	return mixS16Kernel{}, false
}
