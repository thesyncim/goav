//go:build !goexperiment.simd || purego

package resize

func scaleHalfBox(dst, src planeBuf, rect scaleRect) {
	scaleHalfBoxScalar(dst, src, rect)
}

func (p *planeScaler) scaleVertical(dst []byte, dstStride int, tmp []uint16, dstWidth, y0, y1, srcFirst int) {
	p.scaleVerticalScalar(dst, dstStride, tmp, dstWidth, y0, y1, srcFirst)
}
