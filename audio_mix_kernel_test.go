package goav

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"
)

func TestMixS16KernelDispatch(t *testing.T) {
	if want := wantMixS16KernelPrefix(); !strings.HasPrefix(mixS16KernelName(), want) {
		t.Fatalf("mix kernel = %q, want prefix %q", mixS16KernelName(), want)
	}
}

func TestMixS16KernelMatchesScalar(t *testing.T) {
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
		// Three arms where an early pairwise saturation would differ from the
		// clamped full sum: 30000 + 30000 - 30000 must mix to 30000.
		{arms: 3, samples: 64, pattern: func(arm int, _ int) int16 {
			if arm == 2 {
				return -30000
			}
			return 30000
		}},
		{arms: 8, samples: 32, pattern: func(arm int, sample int) int16 {
			return int16((sample%7)*2048 - arm*700 - 4096)
		}},
	}
	for _, tt := range corpus {
		t.Run(fmt.Sprintf("arms=%d/samples=%d/kernel=%s", tt.arms, tt.samples, mixS16KernelName()), func(t *testing.T) {
			assertMixMatchesScalar(t, mixKernelCorpusInputs(tt.arms, tt.samples, tt.pattern))
		})
	}
}

// TestMixS16KernelMatchesScalarRandom sweeps arm counts, lengths that land on
// and around every vector width, extreme sample values, and byte offsets that
// leave the buffers unaligned.
func TestMixS16KernelMatchesScalarRandom(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	extremes := []int16{-32768, -32767, -1, 0, 1, 32766, 32767}
	for arms := 1; arms <= 9; arms++ {
		for _, samples := range []int{0, 1, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 129, 480, 961} {
			for offset := range 3 {
				inputs := make([][]byte, arms)
				for arm := range inputs {
					backing := make([]byte, offset+samples*2)
					payload := backing[offset:]
					for sample := range samples {
						value := int16(rng.Uint32())
						if rng.IntN(3) == 0 {
							value = extremes[rng.IntN(len(extremes))]
						}
						binary.LittleEndian.PutUint16(payload[sample*2:], uint16(value))
					}
					inputs[arm] = payload
				}
				if !t.Run(fmt.Sprintf("arms=%d/samples=%d/offset=%d", arms, samples, offset), func(t *testing.T) {
					assertMixMatchesScalar(t, inputs)
				}) {
					return
				}
			}
		}
	}
}

func assertMixMatchesScalar(t *testing.T, inputs [][]byte) {
	t.Helper()
	n := len(inputs[0])
	got := make([]byte, n)
	want := make([]byte, n)
	mixS16Into(got, inputs, n)
	mixS16ScalarKernel(want, inputs, n)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("kernel %s output = %v, scalar = %v", mixS16KernelName(), mixKernelReadS16(got), mixKernelReadS16(want))
	}
}

func BenchmarkMixS16Kernel(b *testing.B) {
	for _, arms := range []int{2, 3, 8} {
		inputs := mixKernelCorpusInputs(arms, 960, func(arm int, sample int) int16 {
			return int16((sample%257)*64 - arm*97 - 8192)
		})
		n := len(inputs[0])
		b.Run(fmt.Sprintf("selected/%s/arms=%d", mixS16KernelName(), arms), func(b *testing.B) {
			dst := make([]byte, n)
			b.ReportAllocs()
			b.SetBytes(int64(n * arms))
			for b.Loop() {
				mixS16Into(dst, inputs, n)
			}
		})
		b.Run(fmt.Sprintf("scalar/arms=%d", arms), func(b *testing.B) {
			dst := make([]byte, n)
			b.ReportAllocs()
			b.SetBytes(int64(n * arms))
			for b.Loop() {
				mixS16ScalarKernel(dst, inputs, n)
			}
		})
	}
}

func mixKernelCorpusInputs(arms int, samples int, pattern func(arm int, sample int) int16) [][]byte {
	inputs := make([][]byte, arms)
	for arm := range arms {
		payload := make([]byte, samples*2)
		for sample := range samples {
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
