package resize

import "math"

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
	for i := range dstLen {
		row := a.weights[i*a.taps : (i+1)*a.taps]
		clear(row)
		if scale > 1 {
			a.start[i] = a.areaTaps(row, i, scale)
		} else {
			a.start[i] = a.linearTaps(row, i, scale)
		}
		normalize(row)
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

// scale resamples the srcWidth x srcHeight region at (srcX, srcY) of src into
// the dstWidth x dstHeight plane dst, using tmp (grown as needed) as the
// intermediate. It returns tmp for reuse.
func (p *planeScaler) scale(dst []byte, dstStride int, src []byte, srcStride int, srcX, srcY, srcWidth, srcHeight, dstWidth, dstHeight int, tmp []uint16) []uint16 {
	p.x.reset(srcWidth, dstWidth)
	p.y.reset(srcHeight, dstHeight)
	tmp = grow(tmp, srcHeight*dstWidth)

	for r := range srcHeight {
		base := (srcY+r)*srcStride + srcX
		p.x.filterRow(tmp[r*dstWidth:(r+1)*dstWidth], src[base:base+srcWidth])
	}

	// The vertical pass accumulates whole intermediate rows into one output
	// row, so every read and write walks memory in order.
	const round = 1 << (weightBits + intermediateBits - 1)
	p.acc = grow(p.acc, dstWidth)
	acc := p.acc
	yTaps := p.y.taps
	for i := range dstHeight {
		for x := range acc {
			acc[x] = round
		}
		base := int(p.y.start[i])
		for k, w := range p.y.weights[i*yTaps : (i+1)*yTaps] {
			if w == 0 {
				continue
			}
			row := tmp[(base+k)*dstWidth : (base+k+1)*dstWidth]
			weight := uint32(w)
			for x, v := range row[:len(acc)] {
				acc[x] += uint32(v) * weight
			}
		}
		out := dst[i*dstStride : i*dstStride+dstWidth]
		for x, v := range acc[:len(out)] {
			out[x] = byte(v >> (weightBits + intermediateBits))
		}
	}
	return tmp
}

// filterRow filters one source row into the intermediate, keeping
// intermediateBits fractional bits of each output sample.
func (a *axisFilter) filterRow(out []uint16, line []byte) {
	const half = 1 << (weightBits - intermediateBits - 1)
	const shift = weightBits - intermediateBits
	if a.taps == 2 {
		for i := range out {
			b := int(a.start[i])
			w := a.weights[2*i : 2*i+2 : 2*i+2]
			s := line[b : b+2 : b+2]
			out[i] = uint16((int32(s[0])*w[0] + int32(s[1])*w[1] + half) >> shift)
		}
		return
	}
	taps := a.taps
	for i := range out {
		b := int(a.start[i])
		w := a.weights[i*taps : (i+1)*taps]
		acc := int32(half)
		for k, s := range line[b : b+taps] {
			acc += int32(s) * w[k]
		}
		out[i] = uint16(acc >> shift)
	}
}
