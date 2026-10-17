//go:build goexperiment.simd && !purego

package goav

import "simd"

func wantMixS16KernelPrefix() string {
	if simd.Emulated() {
		return "scalar"
	}
	return "simd/"
}
