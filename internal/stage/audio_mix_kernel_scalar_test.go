//go:build !goexperiment.simd || purego

package stage

func wantMixS16KernelPrefix() string { return "scalar" }
