//go:build goexperiment.simd && !purego

package resize

import "simd"

func useSIMDResize() bool {
	return !simd.Emulated()
}

// scaleHalfBoxSIMD computes the bit-exact 2x2 area average
// (s00 + s01 + s10 + s11 + 2) >> 2 using portable Go SIMD vectors.
//
// Each Uint8s load of 2*half source bytes reshapes to Uint16s where every
// lane holds a horizontal pair (low byte = s[2k], high byte = s[2k+1]).
// Adding low and high bytes for both source rows yields 0..1020 in uint16,
// then +2 >> 2 produces the exact output byte in each lane's low byte.
func scaleHalfBox(dst, src planeBuf, rect scaleRect) {
	if !useSIMDResize() {
		scaleHalfBoxScalar(dst, src, rect)
		return
	}
	dstData, dstStride := dst.data, dst.stride
	srcData, srcStride := src.data, src.stride
	srcX, srcY, dstWidth, dstHeight := rect.srcX, rect.srcY, rect.dstW, rect.dstH
	step := simd.Uint8s{}.Len()
	half := step / 2
	vec := dstWidth - dstWidth%half
	block := vec - vec%(4*half)
	maskFF := simd.BroadcastUint16s(0x00ff)
	two := simd.BroadcastUint16s(2)
	var buf [256]uint16

	for y := range dstHeight {
		s0 := srcData[(srcY+2*y)*srcStride+srcX:]
		s1 := srcData[(srcY+2*y+1)*srcStride+srcX:]
		out := dstData[y*dstStride : y*dstStride+dstWidth]

		x := 0
		for ; x < block; x += 4 * half {
			sx := 2 * x
			r00 := simd.LoadUint8s(s0[sx:]).ReshapeToUint16s()
			r10 := simd.LoadUint8s(s1[sx:]).ReshapeToUint16s()
			r01 := simd.LoadUint8s(s0[sx+step:]).ReshapeToUint16s()
			r11 := simd.LoadUint8s(s1[sx+step:]).ReshapeToUint16s()
			r02 := simd.LoadUint8s(s0[sx+2*step:]).ReshapeToUint16s()
			r12 := simd.LoadUint8s(s1[sx+2*step:]).ReshapeToUint16s()
			r03 := simd.LoadUint8s(s0[sx+3*step:]).ReshapeToUint16s()
			r13 := simd.LoadUint8s(s1[sx+3*step:]).ReshapeToUint16s()

			a0 := r00.And(maskFF).Add(r00.ShiftAllRight(8)).Add(r10.And(maskFF)).Add(r10.ShiftAllRight(8)).Add(two).ShiftAllRight(2)
			a1 := r01.And(maskFF).Add(r01.ShiftAllRight(8)).Add(r11.And(maskFF)).Add(r11.ShiftAllRight(8)).Add(two).ShiftAllRight(2)
			a2 := r02.And(maskFF).Add(r02.ShiftAllRight(8)).Add(r12.And(maskFF)).Add(r12.ShiftAllRight(8)).Add(two).ShiftAllRight(2)
			a3 := r03.And(maskFF).Add(r03.ShiftAllRight(8)).Add(r13.And(maskFF)).Add(r13.ShiftAllRight(8)).Add(two).ShiftAllRight(2)

			a0.Store(buf[:half])
			a1.Store(buf[half : 2*half])
			a2.Store(buf[2*half : 3*half])
			a3.Store(buf[3*half : 4*half])
			o4 := out[x : x+4*half]
			for j := range 4 * half {
				o4[j] = byte(buf[j])
			}
		}
		for ; x < vec; x += half {
			sx := 2 * x
			r0 := simd.LoadUint8s(s0[sx:]).ReshapeToUint16s()
			r1 := simd.LoadUint8s(s1[sx:]).ReshapeToUint16s()
			a := r0.And(maskFF).Add(r0.ShiftAllRight(8)).Add(r1.And(maskFF)).Add(r1.ShiftAllRight(8)).Add(two).ShiftAllRight(2)
			a.Store(buf[:half])
			oh := out[x : x+half]
			for j := range half {
				oh[j] = byte(buf[j])
			}
		}
		for ; x < dstWidth; x++ {
			sx := 2 * x
			out[x] = byte((uint32(s0[sx]) + uint32(s0[sx+1]) + uint32(s1[sx]) + uint32(s1[sx+1]) + 2) >> 2)
		}
	}
}

func (p *planeScaler) scaleVertical(dst []byte, dstStride int, tmp []uint16, dstWidth, y0, y1, srcFirst int) {
	if !useSIMDResize() {
		p.scaleVerticalScalar(dst, dstStride, tmp, dstWidth, y0, y1, srcFirst)
		return
	}
	scaleVerticalSIMD(dst, dstStride, tmp, dstWidth, y0, y1, srcFirst, p.y.start, p.y.weights, p.y.taps)
}

// scaleVerticalSIMD runs the separable vertical pass in-register across chunks
// of Uint16s.Len() columns, eliminating per-tap accumulator memory traffic.
func scaleVerticalSIMD(dst []byte, dstStride int, tmp []uint16, dstWidth, y0, y1, srcFirst int, starts []int32, allWeights []int32, yTaps int) {
	const shift = weightBits + intermediateBits
	const round = 1 << (shift - 1)

	u16Len := simd.Uint16s{}.Len()
	u32Len := simd.Uint32s{}.Len()
	vec := dstWidth - dstWidth%u16Len
	roundVec := simd.BroadcastUint32s(round)
	maskFFFF := simd.BroadcastUint32s(0xffff)
	var buf [64]uint32

	for i := y0; i < y1; i++ {
		base := int(starts[i]) - srcFirst
		weights := allWeights[i*yTaps : (i+1)*yTaps]
		out := dst[i*dstStride : i*dstStride+dstWidth]

		for x := 0; x < vec; x += u16Len {
			even := roundVec
			odd := roundVec
			for k, w := range weights {
				if w == 0 {
					continue
				}
				v := simd.LoadUint16s(tmp[(base+k)*dstWidth+x:]).ReshapeToUint32s()
				wv := simd.BroadcastUint32s(uint32(w))
				even = even.Add(v.And(maskFFFF).Mul(wv))
				odd = odd.Add(v.ShiftAllRight(16).Mul(wv))
			}
			packed := even.ShiftAllRight(shift).Or(odd.ShiftAllRight(shift).ShiftAllLeft(8))
			packed.Store(buf[:u32Len])
			ox := out[x : x+u16Len]
			for j := range u32Len {
				val := buf[j]
				ox[2*j] = byte(val)
				ox[2*j+1] = byte(val >> 8)
			}
		}

		for x := vec; x < dstWidth; x++ {
			acc := uint32(round)
			for k, w := range weights {
				if w == 0 {
					continue
				}
				acc += uint32(tmp[(base+k)*dstWidth+x]) * uint32(w)
			}
			out[x] = byte(acc >> shift)
		}
	}
}
