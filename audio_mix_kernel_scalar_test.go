//go:build !goexperiment.simd || purego

package goav

func wantMixS16KernelPrefix() string { return "scalar" }
