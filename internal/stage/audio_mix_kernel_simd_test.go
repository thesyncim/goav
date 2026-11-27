//go:build goexperiment.simd && !purego

package stage

import "simd"

func wantMixS16KernelPrefix() string {
	if simd.Emulated() {
		return "scalar"
	}
	return "simd/"
}
