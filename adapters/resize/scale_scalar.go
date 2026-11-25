//go:build !goexperiment.simd || purego

package resize

func scaleHalfBox(dst []byte, dstStride int, src []byte, srcStride int, srcX, srcY, dstWidth, dstHeight int) {
	scaleHalfBoxScalar(dst, dstStride, src, srcStride, srcX, srcY, dstWidth, dstHeight)
}

func (p *planeScaler) scaleVertical(dst []byte, dstStride int, tmp []uint16, dstWidth, dstHeight int) {
	p.scaleVerticalScalar(dst, dstStride, tmp, dstWidth, dstHeight)
}
