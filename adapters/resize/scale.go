package resize

import (
	"encoding/binary"
	"math"
)

// Resampling is separable: a horizontal pass filters each source row into a
// 16-bit intermediate, then a vertical pass filters the intermediate's
// columns into the output plane. Each axis picks its own filter. An axis
// that shrinks averages area: an output sample is the mean of the source span
// it covers, each source sample weighted by its coverage, so downscaled
// renditions do not alias the way point sampling does. An axis that grows
// interpolates linearly between the two nearest source samples. Weights are
// non-negative Q14 fixed point summing to exactly one per output sample, so
// the arithmetic needs no clamping and stays in 32 bits.
const (
	weightBits = 14
	weightOne  = 1 << weightBits
	// The intermediate keeps intermediateBits fractional bits of each
	// horizontally filtered sample (255 << 8 still fits a uint16).
	intermediateBits = 8
)

// axisFilter maps one axis of srcLen samples onto dstLen samples. Output i
// reads taps consecutive source samples from start[i] with weights
// weights[i*taps:(i+1)*taps].
type axisFilter struct {
	srcLen, dstLen int
	taps           int
	start          []int32
	weights        []int32
}

// reset rebuilds the filter for a new axis length pair, reusing its slices.
// An unchanged pair keeps the built coefficients, so a steady stream builds
// them once.
func (a *axisFilter) reset(srcLen, dstLen int) {
	if a.srcLen == srcLen && a.dstLen == dstLen && a.taps != 0 {
		return
	}
	a.srcLen, a.dstLen = srcLen, dstLen
	scale := float64(srcLen) / float64(dstLen)
	a.taps = min(2, srcLen)
	if scale > 1 {
		// A span of scale samples touches ceil(scale) of them when it starts
		// on a sample boundary (integer ratios) and one more otherwise.
		a.taps = int(math.Ceil(scale))
		if scale != math.Trunc(scale) {
			a.taps++
		}
		a.taps = min(a.taps, srcLen)
	}
	a.start = grow(a.start, dstLen)
	a.weights = grow(a.weights, dstLen*a.taps)
	maxUsed := 1
	for i := range dstLen {
		row := a.weights[i*a.taps : (i+1)*a.taps]
		clear(row)
		if scale > 1 {
			a.start[i] = a.areaTaps(row, i, scale)
		} else {
			a.start[i] = a.linearTaps(row, i, scale)
		}
		normalize(row)
		for k := len(row) - 1; k >= maxUsed; k-- {
			if row[k] != 0 {
				maxUsed = k + 1
				break
			}
		}
	}
	if maxUsed < a.taps {
		oldTaps := a.taps
		a.taps = maxUsed
		for i := range dstLen {
			copy(a.weights[i*maxUsed:(i+1)*maxUsed], a.weights[i*oldTaps:i*oldTaps+maxUsed])
		}
		a.weights = a.weights[:dstLen*maxUsed]
	}
	for i := range dstLen {
		row := a.weights[i*a.taps : (i+1)*a.taps]
		a.start[i] = fitWindow(row, a.start[i], srcLen)
	}
}

// fitWindow shifts a tap window that runs past the source end back inside it,
// moving the weights with it (the taps past the end carry zero weight), so
// the filter loops index the source without clamping.
func fitWindow(row []int32, start int32, srcLen int) int32 {
	over := int(start) + len(row) - srcLen
	if over <= 0 {
		return start
	}
	copy(row[over:], row[:len(row)-over])
	clear(row[:over])
	return start - int32(over)
}

// areaTaps weights the source samples covering output i, the span
// [i*scale, (i+1)*scale), by how much of each sample the span covers.
func (a *axisFilter) areaTaps(row []int32, i int, scale float64) int32 {
	lo, hi := float64(i)*scale, float64(i+1)*scale
	first := int(lo)
	for k := range row {
		j := first + k
		if j >= a.srcLen {
			break
		}
		overlap := min(hi, float64(j+1)) - max(lo, float64(j))
		if overlap > 0 {
			row[k] = int32(math.Round(overlap / scale * weightOne))
		}
	}
	return int32(first)
}

// linearTaps interpolates output i between its two nearest source samples,
// with centers aligned (sample i of n sits at (i+0.5)/n of the axis) and the
// edges clamped.
func (a *axisFilter) linearTaps(row []int32, i int, scale float64) int32 {
	if a.srcLen == 1 {
		row[0] = weightOne
		return 0
	}
	pos := (float64(i)+0.5)*scale - 0.5
	pos = min(max(pos, 0), float64(a.srcLen-1))
	first := int(pos)
	if first >= a.srcLen-1 {
		first = max(a.srcLen-2, 0)
	}
	frac := pos - float64(first)
	row[1] = int32(math.Round(frac * weightOne))
	row[0] = weightOne - row[1]
	return int32(first)
}

// normalize makes a tap row sum to exactly weightOne, moving any rounding
// residue onto its largest weight.
func normalize(row []int32) {
	sum, largest := int32(0), 0
	for k, w := range row {
		sum += w
		if w > row[largest] {
			largest = k
		}
	}
	row[largest] += weightOne - sum
}

func grow[T any](s []T, n int) []T {
	if cap(s) < n {
		return make([]T, n)
	}
	return s[:n]
}

// planeScaler holds one plane's axis filters and the vertical pass's row
// accumulator.
type planeScaler struct {
	x, y axisFilter
	acc  []uint32
}

// planeBuf is a tightly- or strided-packed 2D byte plane passed by value so it
// stays on the stack / in registers with zero heap allocations.
type planeBuf struct {
	data   []byte
	stride int
}

// scaleRect describes a source crop region and destination dimensions.
type scaleRect struct {
	srcX, srcY int
	srcW, srcH int
	dstW, dstH int
}

// scale resamples the rect.srcW x rect.srcH region at (rect.srcX, rect.srcY) of
// src into the rect.dstW x rect.dstH plane dst, using tmp (grown as needed) as
// the intermediate. It returns tmp for reuse.
func (p *planeScaler) scale(dst, src planeBuf, rect scaleRect, tmp []uint16) []uint16 {
	if rect.srcW == 2*rect.dstW && rect.srcH == 2*rect.dstH {
		scaleHalfBox(dst, src, rect)
		return tmp
	}
	return p.scaleGeneral(dst, src, rect, tmp)
}

func (p *planeScaler) scaleGeneral(dst, src planeBuf, rect scaleRect, tmp []uint16) []uint16 {
	p.x.reset(rect.srcW, rect.dstW)
	p.y.reset(rect.srcH, rect.dstH)

	// Process in L1-cache-sized output row strips so the horizontal pass writes
	// into a small intermediate that the vertical pass consumes immediately.
	const stripDstH = 16
	yTaps := p.y.taps
	yStarts := p.y.start[:rect.dstH]
	prevFirst, prevLast := 0, 0
	for y0 := 0; y0 < rect.dstH; y0 += stripDstH {
		y1 := min(y0+stripDstH, rect.dstH)
		srcFirst := int(yStarts[y0])
		srcLast := int(yStarts[y1-1]) + yTaps
		stripSrcH := srcLast - srcFirst
		oldTmp := tmp
		tmp = grow(tmp, stripSrcH*rect.dstW)

		// Reuse any source rows already filtered at the end of the previous strip.
		filterFrom := srcFirst
		if prevLast > srcFirst && prevFirst <= srcFirst {
			reuseEnd := min(prevLast, srcLast)
			overlap := reuseEnd - srcFirst
			srcOff := (srcFirst - prevFirst) * rect.dstW
			copy(tmp[:overlap*rect.dstW], oldTmp[srcOff:srcOff+overlap*rect.dstW])
			filterFrom = reuseEnd
		}
		for r := filterFrom; r < srcLast; r++ {
			base := (rect.srcY+r)*src.stride + rect.srcX
			dstOff := (r - srcFirst) * rect.dstW
			p.x.filterRow(tmp[dstOff:dstOff+rect.dstW], src.data[base:base+rect.srcW])
		}
		p.scaleVertical(dst.data, dst.stride, tmp, rect.dstW, y0, y1, srcFirst)
		prevFirst, prevLast = srcFirst, srcLast
	}
	return tmp
}

// scaleHalfBoxScalar computes the exact 2x2 box downscale in a single pass.
// Because Q14 weights for a 2:1 ratio are [8192, 8192] on both axes, the
// separable two-pass result is bit-identical to (s00 + s01 + s10 + s11 + 2) >> 2.
//
// Processes 8 output pixels (16 source bytes per row) at a time using 64-bit
// SWAR arithmetic: each uint64 load splits into 4 uint16 lanes holding the
// even/odd byte pairs, sums both rows plus rounding constant 2 with zero
// cross-lane carry (max 1022 < 65535), shifts right by 2, and packs back to
// consecutive output bytes.
func scaleHalfBoxScalar(dst, src planeBuf, rect scaleRect) {
	const (
		maskFF   = 0x00ff00ff00ff00ff
		round2   = 0x0002000200020002
		maskFFFF = 0x0000ffff0000ffff
	)
	dstData, dstStride := dst.data, dst.stride
	srcData, srcStride := src.data, src.stride
	srcX, srcY, dstWidth, dstHeight := rect.srcX, rect.srcY, rect.dstW, rect.dstH
	srcWidth := 2 * dstWidth
	for y := range dstHeight {
		s0Off := (srcY+2*y)*srcStride + srcX
		s1Off := s0Off + srcStride
		s0 := srcData[s0Off : s0Off+srcWidth]
		s1 := srcData[s1Off : s1Off+srcWidth]
		out := dstData[y*dstStride : y*dstStride+dstWidth]
		x := 0
		for ; x+8 <= dstWidth; x += 8 {
			sx := 2 * x
			s0w := s0[sx : sx+16]
			s1w := s1[sx : sx+16]
			a0 := binary.LittleEndian.Uint64(s0w)
			b0 := binary.LittleEndian.Uint64(s1w)
			a1 := binary.LittleEndian.Uint64(s0w[8:])
			b1 := binary.LittleEndian.Uint64(s1w[8:])

			sum0 := (((a0 & maskFF) + ((a0 >> 8) & maskFF) + (b0 & maskFF) + ((b0 >> 8) & maskFF) + round2) >> 2) & maskFF
			sum1 := (((a1 & maskFF) + ((a1 >> 8) & maskFF) + (b1 & maskFF) + ((b1 >> 8) & maskFF) + round2) >> 2) & maskFF

			t0 := (sum0 | (sum0 >> 8)) & maskFFFF
			t1 := (sum1 | (sum1 >> 8)) & maskFFFF
			p0 := uint32(t0 | (t0 >> 16))
			p1 := uint32(t1 | (t1 >> 16))
			binary.LittleEndian.PutUint64(out[x:], uint64(p0)|uint64(p1)<<32)
		}
		if x+4 <= dstWidth {
			sx := 2 * x
			a0 := binary.LittleEndian.Uint64(s0[sx:])
			b0 := binary.LittleEndian.Uint64(s1[sx:])
			sum0 := (((a0 & maskFF) + ((a0 >> 8) & maskFF) + (b0 & maskFF) + ((b0 >> 8) & maskFF) + round2) >> 2) & maskFF
			t0 := (sum0 | (sum0 >> 8)) & maskFFFF
			binary.LittleEndian.PutUint32(out[x:], uint32(t0|(t0>>16)))
			x += 4
		}
		for ; x < dstWidth; x++ {
			sx := 2 * x
			out[x] = byte((uint32(s0[sx]) + uint32(s0[sx+1]) + uint32(s1[sx]) + uint32(s1[sx+1]) + 2) >> 2)
		}
	}
}

func (p *planeScaler) scaleVerticalScalar(dst []byte, dstStride int, tmp []uint16, dstWidth, y0, y1, srcFirst int) {
	// For common tap counts (2..4), accumulate directly in registers in a single
	// pass over the intermediate rows, avoiding p.acc read/write traffic.
	const (
		shift = weightBits + intermediateBits
		round = 1 << (shift - 1)
	)
	starts := p.y.start[:y1]
	yTaps := p.y.taps
	allWeights := p.y.weights[:y1*yTaps]

	switch yTaps {
	case 2:
		for i := y0; i < y1; i++ {
			base := int(starts[i]) - srcFirst
			w0 := uint32(allWeights[2*i])
			w1 := uint32(allWeights[2*i+1])
			r0 := tmp[base*dstWidth : (base+1)*dstWidth]
			r1 := tmp[(base+1)*dstWidth : (base+2)*dstWidth]
			out := dst[i*dstStride : i*dstStride+dstWidth]
			scaleVerticalRow2(out, r0, r1, w0, w1, dstWidth)
		}
		return
	case 3:
		for i := y0; i < y1; i++ {
			base := int(starts[i]) - srcFirst
			wi := 3 * i
			w0 := uint32(allWeights[wi])
			w1 := uint32(allWeights[wi+1])
			w2 := uint32(allWeights[wi+2])
			out := dst[i*dstStride : i*dstStride+dstWidth]
			if w2 == 0 {
				r0 := tmp[base*dstWidth : (base+1)*dstWidth]
				r1 := tmp[(base+1)*dstWidth : (base+2)*dstWidth]
				scaleVerticalRow2(out, r0, r1, w0, w1, dstWidth)
				continue
			}
			if w0 == 0 {
				r1 := tmp[(base+1)*dstWidth : (base+2)*dstWidth]
				r2 := tmp[(base+2)*dstWidth : (base+3)*dstWidth]
				scaleVerticalRow2(out, r1, r2, w1, w2, dstWidth)
				continue
			}
			r0 := tmp[base*dstWidth : (base+1)*dstWidth]
			r1 := tmp[(base+1)*dstWidth : (base+2)*dstWidth]
			r2 := tmp[(base+2)*dstWidth : (base+3)*dstWidth]
			scaleVerticalRow3(out, r0, r1, r2, w0, w1, w2, dstWidth)
		}
		return
	case 4:
		for i := y0; i < y1; i++ {
			base := int(starts[i]) - srcFirst
			wi := 4 * i
			w0 := uint32(allWeights[wi])
			w1 := uint32(allWeights[wi+1])
			w2 := uint32(allWeights[wi+2])
			w3 := uint32(allWeights[wi+3])
			out := dst[i*dstStride : i*dstStride+dstWidth]
			if w3 == 0 {
				r0 := tmp[base*dstWidth : (base+1)*dstWidth]
				r1 := tmp[(base+1)*dstWidth : (base+2)*dstWidth]
				r2 := tmp[(base+2)*dstWidth : (base+3)*dstWidth]
				scaleVerticalRow3(out, r0, r1, r2, w0, w1, w2, dstWidth)
				continue
			}
			if w0 == 0 {
				r1 := tmp[(base+1)*dstWidth : (base+2)*dstWidth]
				r2 := tmp[(base+2)*dstWidth : (base+3)*dstWidth]
				r3 := tmp[(base+3)*dstWidth : (base+4)*dstWidth]
				scaleVerticalRow3(out, r1, r2, r3, w1, w2, w3, dstWidth)
				continue
			}
			r0 := tmp[base*dstWidth : (base+1)*dstWidth]
			r1 := tmp[(base+1)*dstWidth : (base+2)*dstWidth]
			r2 := tmp[(base+2)*dstWidth : (base+3)*dstWidth]
			r3 := tmp[(base+3)*dstWidth : (base+4)*dstWidth]
			scaleVerticalRow4(out, r0, r1, r2, r3, w0, w1, w2, w3, dstWidth)
		}
		return
	}

	// General path for 1 or >=5 taps: accumulate whole intermediate rows into
	// p.acc, fusing the first non-zero tap with round initialization.
	p.acc = grow(p.acc, dstWidth)
	acc := p.acc[:dstWidth]
	for i := y0; i < y1; i++ {
		base := int(starts[i]) - srcFirst
		first := true
		for k, w := range allWeights[i*yTaps : (i+1)*yTaps] {
			if w == 0 {
				continue
			}
			row := tmp[(base+k)*dstWidth : (base+k+1)*dstWidth]
			weight := uint32(w)
			if first {
				first = false
				x := 0
				for ; x+4 <= dstWidth; x += 4 {
					acc[x] = round + uint32(row[x])*weight
					acc[x+1] = round + uint32(row[x+1])*weight
					acc[x+2] = round + uint32(row[x+2])*weight
					acc[x+3] = round + uint32(row[x+3])*weight
				}
				for ; x < dstWidth; x++ {
					acc[x] = round + uint32(row[x])*weight
				}
				continue
			}
			x := 0
			for ; x+4 <= dstWidth; x += 4 {
				acc[x] += uint32(row[x]) * weight
				acc[x+1] += uint32(row[x+1]) * weight
				acc[x+2] += uint32(row[x+2]) * weight
				acc[x+3] += uint32(row[x+3]) * weight
			}
			for ; x < dstWidth; x++ {
				acc[x] += uint32(row[x]) * weight
			}
		}
		out := dst[i*dstStride : i*dstStride+dstWidth]
		if first {
			clear(out)
			continue
		}
		x := 0
		for ; x+4 <= dstWidth; x += 4 {
			out[x] = byte(acc[x] >> shift)
			out[x+1] = byte(acc[x+1] >> shift)
			out[x+2] = byte(acc[x+2] >> shift)
			out[x+3] = byte(acc[x+3] >> shift)
		}
		for ; x < dstWidth; x++ {
			out[x] = byte(acc[x] >> shift)
		}
	}
}

func scaleVerticalRow2(out []byte, r0, r1 []uint16, w0, w1 uint32, n int) {
	const (
		shift = weightBits + intermediateBits
		round = 1 << (shift - 1)
	)
	out, r0, r1 = out[:n], r0[:n], r1[:n]
	x := 0
	for ; x+8 <= n; x += 8 {
		a8 := r0[x : x+8]
		b8 := r1[x : x+8]
		p0 := (uint32(a8[0])*w0 + uint32(b8[0])*w1 + round) >> shift
		p1 := (uint32(a8[1])*w0 + uint32(b8[1])*w1 + round) >> shift
		p2 := (uint32(a8[2])*w0 + uint32(b8[2])*w1 + round) >> shift
		p3 := (uint32(a8[3])*w0 + uint32(b8[3])*w1 + round) >> shift
		p4 := (uint32(a8[4])*w0 + uint32(b8[4])*w1 + round) >> shift
		p5 := (uint32(a8[5])*w0 + uint32(b8[5])*w1 + round) >> shift
		p6 := (uint32(a8[6])*w0 + uint32(b8[6])*w1 + round) >> shift
		p7 := (uint32(a8[7])*w0 + uint32(b8[7])*w1 + round) >> shift
		binary.LittleEndian.PutUint64(out[x:],
			uint64(p0)|uint64(p1)<<8|uint64(p2)<<16|uint64(p3)<<24|
				uint64(p4)<<32|uint64(p5)<<40|uint64(p6)<<48|uint64(p7)<<56)
	}
	if x+4 <= n {
		a4 := r0[x : x+4]
		b4 := r1[x : x+4]
		p0 := (uint32(a4[0])*w0 + uint32(b4[0])*w1 + round) >> shift
		p1 := (uint32(a4[1])*w0 + uint32(b4[1])*w1 + round) >> shift
		p2 := (uint32(a4[2])*w0 + uint32(b4[2])*w1 + round) >> shift
		p3 := (uint32(a4[3])*w0 + uint32(b4[3])*w1 + round) >> shift
		binary.LittleEndian.PutUint32(out[x:], p0|p1<<8|p2<<16|p3<<24)
		x += 4
	}
	for ; x < n; x++ {
		out[x] = byte((uint32(r0[x])*w0 + uint32(r1[x])*w1 + round) >> shift)
	}
}

func scaleVerticalRow3(out []byte, r0, r1, r2 []uint16, w0, w1, w2 uint32, n int) {
	const (
		shift = weightBits + intermediateBits
		round = 1 << (shift - 1)
	)
	out, r0, r1, r2 = out[:n], r0[:n], r1[:n], r2[:n]
	x := 0
	for ; x+4 <= n; x += 4 {
		o4 := out[x : x+4]
		a4 := r0[x : x+4]
		b4 := r1[x : x+4]
		c4 := r2[x : x+4]
		o4[0] = byte((uint32(a4[0])*w0 + uint32(b4[0])*w1 + uint32(c4[0])*w2 + round) >> shift)
		o4[1] = byte((uint32(a4[1])*w0 + uint32(b4[1])*w1 + uint32(c4[1])*w2 + round) >> shift)
		o4[2] = byte((uint32(a4[2])*w0 + uint32(b4[2])*w1 + uint32(c4[2])*w2 + round) >> shift)
		o4[3] = byte((uint32(a4[3])*w0 + uint32(b4[3])*w1 + uint32(c4[3])*w2 + round) >> shift)
	}
	for ; x < n; x++ {
		out[x] = byte((uint32(r0[x])*w0 + uint32(r1[x])*w1 + uint32(r2[x])*w2 + round) >> shift)
	}
}

func scaleVerticalRow4(out []byte, r0, r1, r2, r3 []uint16, w0, w1, w2, w3 uint32, n int) {
	const (
		shift = weightBits + intermediateBits
		round = 1 << (shift - 1)
	)
	out, r0, r1, r2, r3 = out[:n], r0[:n], r1[:n], r2[:n], r3[:n]
	x := 0
	for ; x+4 <= n; x += 4 {
		o4 := out[x : x+4]
		a4 := r0[x : x+4]
		b4 := r1[x : x+4]
		c4 := r2[x : x+4]
		d4 := r3[x : x+4]
		o4[0] = byte((uint32(a4[0])*w0 + uint32(b4[0])*w1 + uint32(c4[0])*w2 + uint32(d4[0])*w3 + round) >> shift)
		o4[1] = byte((uint32(a4[1])*w0 + uint32(b4[1])*w1 + uint32(c4[1])*w2 + uint32(d4[1])*w3 + round) >> shift)
		o4[2] = byte((uint32(a4[2])*w0 + uint32(b4[2])*w1 + uint32(c4[2])*w2 + uint32(d4[2])*w3 + round) >> shift)
		o4[3] = byte((uint32(a4[3])*w0 + uint32(b4[3])*w1 + uint32(c4[3])*w2 + uint32(d4[3])*w3 + round) >> shift)
	}
	for ; x < n; x++ {
		out[x] = byte((uint32(r0[x])*w0 + uint32(r1[x])*w1 + uint32(r2[x])*w2 + uint32(r3[x])*w3 + round) >> shift)
	}
}

// filterRow filters one source row into the intermediate, keeping
// intermediateBits fractional bits of each output sample.
func (a *axisFilter) filterRow(out []uint16, line []byte) {
	const half = 1 << (weightBits - intermediateBits - 1)
	const shift = weightBits - intermediateBits
	n := len(out)
	if n == 0 {
		return
	}
	starts := a.start[:n]
	taps := a.taps
	weights := a.weights[:n*taps]

	switch taps {
	case 2:
		if 2*a.srcLen == 3*n {
			filterRow3to2(out, line)
			return
		}
		i, wi := 0, 0
		for ; i+4 <= n; i, wi = i+4, wi+8 {
			w8 := weights[wi : wi+8]
			st4 := starts[i : i+4]
			o4 := out[i : i+4]
			v0 := binary.LittleEndian.Uint16(line[int(st4[0]):])
			v1 := binary.LittleEndian.Uint16(line[int(st4[1]):])
			v2 := binary.LittleEndian.Uint16(line[int(st4[2]):])
			v3 := binary.LittleEndian.Uint16(line[int(st4[3]):])
			o4[0] = uint16((int32(v0&0xff)*w8[0] + int32(v0>>8)*w8[1] + half) >> shift)
			o4[1] = uint16((int32(v1&0xff)*w8[2] + int32(v1>>8)*w8[3] + half) >> shift)
			o4[2] = uint16((int32(v2&0xff)*w8[4] + int32(v2>>8)*w8[5] + half) >> shift)
			o4[3] = uint16((int32(v3&0xff)*w8[6] + int32(v3>>8)*w8[7] + half) >> shift)
		}
		for ; i < n; i, wi = i+1, wi+2 {
			b := int(starts[i])
			v := binary.LittleEndian.Uint16(line[b:])
			out[i] = uint16((int32(v&0xff)*weights[wi] + int32(v>>8)*weights[wi+1] + half) >> shift)
		}
		return
	case 3:
		i, wi := 0, 0
		for ; i+2 <= n; i, wi = i+2, wi+6 {
			w6 := weights[wi : wi+6]
			b0 := int(starts[i])
			b1 := int(starts[i+1])
			s0 := line[b0 : b0+3 : b0+3]
			s1 := line[b1 : b1+3 : b1+3]
			out[i] = uint16((int32(s0[0])*w6[0] + int32(s0[1])*w6[1] + int32(s0[2])*w6[2] + half) >> shift)
			out[i+1] = uint16((int32(s1[0])*w6[3] + int32(s1[1])*w6[4] + int32(s1[2])*w6[5] + half) >> shift)
		}
		for ; i < n; i, wi = i+1, wi+3 {
			b := int(starts[i])
			s := line[b : b+3 : b+3]
			out[i] = uint16((int32(s[0])*weights[wi] + int32(s[1])*weights[wi+1] + int32(s[2])*weights[wi+2] + half) >> shift)
		}
		return
	case 4:
		i, wi := 0, 0
		for ; i+2 <= n; i, wi = i+2, wi+8 {
			w8 := weights[wi : wi+8]
			b0 := int(starts[i])
			b1 := int(starts[i+1])
			v0 := binary.LittleEndian.Uint32(line[b0:])
			v1 := binary.LittleEndian.Uint32(line[b1:])
			out[i] = uint16((int32(v0&0xff)*w8[0] + int32((v0>>8)&0xff)*w8[1] + int32((v0>>16)&0xff)*w8[2] + int32(v0>>24)*w8[3] + half) >> shift)
			out[i+1] = uint16((int32(v1&0xff)*w8[4] + int32((v1>>8)&0xff)*w8[5] + int32((v1>>16)&0xff)*w8[6] + int32(v1>>24)*w8[7] + half) >> shift)
		}
		for ; i < n; i, wi = i+1, wi+4 {
			b := int(starts[i])
			v := binary.LittleEndian.Uint32(line[b:])
			out[i] = uint16((int32(v&0xff)*weights[wi] + int32((v>>8)&0xff)*weights[wi+1] + int32((v>>16)&0xff)*weights[wi+2] + int32(v>>24)*weights[wi+3] + half) >> shift)
		}
		return
	}
	for i := range n {
		b := int(starts[i])
		w := weights[i*taps : (i+1)*taps]
		acc := int32(half)
		for k, s := range line[b : b+taps] {
			acc += int32(s) * w[k]
		}
		out[i] = uint16(acc >> shift)
	}
}

// filterRow3to2 computes the bit-exact Q14 horizontal pass for 3:2 downscaling
// (e.g. 1080p -> 720p). Weights alternate [10923, 5461] and [5461, 10923], so
// each 3 source bytes produce 2 outputs with the middle sample's product shared
// and no starts/weights table loads.
func filterRow3to2(out []uint16, line []byte) {
	const (
		half  = 1 << (weightBits - intermediateBits - 1)
		shift = weightBits - intermediateBits
		wHi   = 10923
		wLo   = 5461
	)
	n := len(out)
	srcLen := len(line)
	i, sx := 0, 0
	for ; i+4 <= n && sx+8 <= srcLen; i, sx = i+4, sx+6 {
		v := binary.LittleEndian.Uint64(line[sx:])
		m1 := uint32(byte(v>>8))*wLo + half
		m4 := uint32(byte(v>>32))*wLo + half
		o4 := out[i : i+4]
		o4[0] = uint16((uint32(byte(v))*wHi + m1) >> shift)
		o4[1] = uint16((m1 + uint32(byte(v>>16))*wHi) >> shift)
		o4[2] = uint16((uint32(byte(v>>24))*wHi + m4) >> shift)
		o4[3] = uint16((m4 + uint32(byte(v>>40))*wHi) >> shift)
	}
	for ; i+2 <= n; i, sx = i+2, sx+3 {
		s := line[sx : sx+3 : sx+3]
		m1 := uint32(s[1])*wLo + half
		out[i] = uint16((uint32(s[0])*wHi + m1) >> shift)
		out[i+1] = uint16((m1 + uint32(s[2])*wHi) >> shift)
	}
}
