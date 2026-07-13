package goav

import (
	"encoding/binary"
	"fmt"
	"reflect"
	goruntime "runtime"
	"strings"
	"testing"
)

func TestMixS16KernelDispatch(t *testing.T) {
	switch goruntime.GOARCH {
	case "arm64":
		if !strings.HasPrefix(mixS16KernelName(), "arm64/") {
			t.Fatalf("mix kernel = %q, want arm64 dispatch", mixS16KernelName())
		}
	case "amd64":
		if !strings.HasPrefix(mixS16KernelName(), "amd64/") {
			t.Fatalf("mix kernel = %q, want amd64 dispatch", mixS16KernelName())
		}
	}
}

func TestMixSIMDMatchesScalar(t *testing.T) {
	corpus := []struct {
		arms    int
		samples int
		pattern func(arm int, sample int) int16
	}{
		{arms: 1, samples: 1, pattern: func(int, int) int16 { return 123 }},
		{arms: 2, samples: 8, pattern: func(arm int, sample int) int16 {
			return int16((arm+1)*1000 - sample*200)
		}},
		// Saturation case: both arms at +30000 forces the positive clamp.
		{arms: 2, samples: 8, pattern: func(int, int) int16 { return 30000 }},
		{arms: 2, samples: 17, pattern: func(arm int, sample int) int16 {
			if arm == 0 {
				return int16(32000 - sample*17)
			}
			return int16(1000 + sample*31)
		}},
		{arms: 3, samples: 17, pattern: func(arm int, sample int) int16 {
			return int16((arm-1)*12000 + sample*137 - 2000)
		}},
		{arms: 8, samples: 32, pattern: func(arm int, sample int) int16 {
			return int16((sample%7)*2048 - arm*700 - 4096)
		}},
	}
	for _, tt := range corpus {
		t.Run(fmt.Sprintf("arms=%d/samples=%d/kernel=%s", tt.arms, tt.samples, mixS16KernelName()), func(t *testing.T) {
			inputs := mixKernelCorpusInputs(tt.arms, tt.samples, tt.pattern)
			n := len(inputs[0])
			got := make([]byte, n)
			want := make([]byte, n)
			mixS16Into(got, inputs, n)
			mixS16ScalarKernel(want, inputs, n)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("kernel output = %v, scalar = %v", mixKernelReadS16(got), mixKernelReadS16(want))
			}
		})
	}
}

func BenchmarkMixS16Kernel(b *testing.B) {
	for _, arms := range []int{2, 8} {
		inputs := mixKernelCorpusInputs(arms, 960, func(arm int, sample int) int16 {
			return int16((sample%257)*64 - arm*97 - 8192)
		})
		n := len(inputs[0])
		b.Run(fmt.Sprintf("selected/%s/arms=%d", mixS16KernelName(), arms), func(b *testing.B) {
			dst := make([]byte, n)
			b.ReportAllocs()
			b.SetBytes(int64(n * arms))
			for i := 0; i < b.N; i++ {
				mixS16Into(dst, inputs, n)
			}
		})
		b.Run(fmt.Sprintf("scalar/arms=%d", arms), func(b *testing.B) {
			dst := make([]byte, n)
			b.ReportAllocs()
			b.SetBytes(int64(n * arms))
			for i := 0; i < b.N; i++ {
				mixS16ScalarKernel(dst, inputs, n)
			}
		})
	}
}

func mixKernelCorpusInputs(arms int, samples int, pattern func(arm int, sample int) int16) [][]byte {
	inputs := make([][]byte, arms)
	for arm := 0; arm < arms; arm++ {
		payload := make([]byte, samples*2)
		for sample := 0; sample < samples; sample++ {
			binary.LittleEndian.PutUint16(payload[sample*2:], uint16(pattern(arm, sample)))
		}
		inputs[arm] = payload
	}
	return inputs
}

func mixKernelReadS16(payload []byte) []int16 {
	out := make([]int16, len(payload)/2)
	for i := range out {
		out[i] = int16(binary.LittleEndian.Uint16(payload[i*2:]))
	}
	return out
}
