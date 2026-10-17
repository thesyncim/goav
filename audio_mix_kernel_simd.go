//go:build goexperiment.simd && !purego

package goav

import (
	"simd"
	"strconv"
)

// mixS16VectorKernel binds the portable Go SIMD mix kernel when the simd
// package runs on hardware vectors (NEON on arm64, AVX/AVX2/AVX-512 on amd64,
// SIMD128 on wasm). Emulated vectors lose to the scalar loop, so they keep it.
func mixS16VectorKernel() (mixS16Kernel, bool) {
	if simd.Emulated() {
		return mixS16Kernel{}, false
	}
	return mixS16Kernel{name: "simd/" + strconv.Itoa(simd.VectorBitSize()), fn: mixS16SIMDKernel}, true
}

// mixS16SIMDKernel is the vector form of mixS16ScalarKernel. Samples are
// little-endian S16 in byte slices; reshaping a byte vector reinterprets
// lanes in place, so no unsafe slice casts are needed.
//
// Bit-exactness with the scalar reference, which clamps the full int32 sum:
//   - two arms use one saturating int16 add, which is exactly
//     clamp(a+b) — saturation cannot occur early with a single add;
//   - three or more arms widen each pair of samples into 32-bit lanes (the
//     low sample sign-extended as ((v&0xffff)^0x8000)-0x8000, the high one
//     by arithmetic shift), sum in int32 with no intermediate clamp, then
//     clamp once and repack. int32 lanes cannot overflow below 65537 arms.
//
// Both paths work in blocks of four vectors so the per-slice bounds checks
// amortize over 4x the bytes; leftover whole vectors take a one-vector loop
// and the final partial vector falls back to the scalar range loop. The
// broadcast constants live in mixS16Consts because the compiler does not
// hoist them out of loops on its own. They are passed by value: through a
// pointer, go1.27.2 folds them into AVX-512 VPTERNLOGD memory operands it
// cannot encode, and the amd64 build fails.
func mixS16SIMDKernel(dst []byte, inputs [][]byte, n int) {
	if len(inputs) == 1 {
		copy(dst[:n], inputs[0][:n])
		return
	}
	step := simd.Uint8s{}.Len()
	vec := n - n%step
	block := vec - vec%(4*step)
	if len(inputs) == 2 {
		a, b := inputs[0], inputs[1]
		for off := 0; off < block; off += 4 * step {
			a4, b4, d4 := a[off:off+4*step], b[off:off+4*step], dst[off:off+4*step]
			mixS16Add2(d4, a4, b4)
			mixS16Add2(d4[step:], a4[step:], b4[step:])
			mixS16Add2(d4[2*step:], a4[2*step:], b4[2*step:])
			mixS16Add2(d4[3*step:], a4[3*step:], b4[3*step:])
		}
		for off := block; off < vec; off += step {
			mixS16Add2(dst[off:], a[off:], b[off:])
		}
	} else {
		k := mixS16Consts{
			low:  simd.BroadcastInt32s(0xffff),
			sign: simd.BroadcastInt32s(0x8000),
			min:  simd.BroadcastInt32s(-32768),
			max:  simd.BroadcastInt32s(32767),
		}
		for off := 0; off < block; off += 4 * step {
			var lo0, hi0, lo1, hi1, lo2, hi2, lo3, hi3 simd.Int32s
			for _, in := range inputs {
				in4 := in[off : off+4*step]
				lo0, hi0 = mixS16Widen(in4, lo0, hi0, k)
				lo1, hi1 = mixS16Widen(in4[step:], lo1, hi1, k)
				lo2, hi2 = mixS16Widen(in4[2*step:], lo2, hi2, k)
				lo3, hi3 = mixS16Widen(in4[3*step:], lo3, hi3, k)
			}
			d4 := dst[off : off+4*step]
			mixS16Narrow(d4, lo0, hi0, k)
			mixS16Narrow(d4[step:], lo1, hi1, k)
			mixS16Narrow(d4[2*step:], lo2, hi2, k)
			mixS16Narrow(d4[3*step:], lo3, hi3, k)
		}
		for off := block; off < vec; off += step {
			var lo, hi simd.Int32s
			for _, in := range inputs {
				lo, hi = mixS16Widen(in[off:], lo, hi, k)
			}
			mixS16Narrow(dst[off:], lo, hi, k)
		}
	}
	mixS16ScalarRange(dst, inputs, vec, n)
}

// mixS16Add2 stores clamp(a+b) for one vector of samples.
func mixS16Add2(dst []byte, a []byte, b []byte) {
	x := simd.LoadUint8s(a).ReshapeToUint16s().BitsToInt16()
	y := simd.LoadUint8s(b).ReshapeToUint16s().BitsToInt16()
	x.AddSaturated(y).ToBits().ReshapeToUint8s().Store(dst)
}

// mixS16Widen adds one vector of samples into the low/high int32 lane sums.
func mixS16Widen(in []byte, lo simd.Int32s, hi simd.Int32s, k mixS16Consts) (simd.Int32s, simd.Int32s) {
	v := simd.LoadUint8s(in).ReshapeToUint32s().BitsToInt32()
	return lo.Add(v.And(k.low).Xor(k.sign).Sub(k.sign)), hi.Add(v.ShiftAllRight(16))
}

// mixS16Narrow clamps the int32 lane sums to S16 and repacks them in place.
func mixS16Narrow(dst []byte, lo simd.Int32s, hi simd.Int32s, k mixS16Consts) {
	lo = lo.Max(k.min).Min(k.max).And(k.low)
	hi = hi.Max(k.min).Min(k.max).ShiftAllLeft(16)
	hi.Or(lo).ToBits().ReshapeToUint8s().Store(dst)
}

// mixS16Consts holds the broadcast masks hoisted out of the widen loop.
type mixS16Consts struct {
	low, sign, min, max simd.Int32s
}
