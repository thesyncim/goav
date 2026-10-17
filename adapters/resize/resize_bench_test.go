package resize

import (
	"fmt"
	"testing"

	"github.com/thesyncim/goav/av"
)

func BenchmarkScalePlaneKernel(b *testing.B) {
	for _, tt := range []struct {
		name      string
		srcW      int
		srcH      int
		dstW      int
		dstH      int
		srcX      int
		srcY      int
		cropW     int
		cropH     int
		srcStride int
		dstStride int
	}{
		{name: "luma_downscale/320x180_to_160x90", srcW: 320, srcH: 180, dstW: 160, dstH: 90, cropW: 320, cropH: 180},
		{name: "chroma_downscale/160x90_to_80x45", srcW: 160, srcH: 90, dstW: 80, dstH: 45, cropW: 160, cropH: 90},
		{name: "passthrough/320x180", srcW: 320, srcH: 180, dstW: 320, dstH: 180, cropW: 320, cropH: 180},
		{name: "fill_crop/320x180_to_160x90", srcW: 320, srcH: 180, dstW: 160, dstH: 90, srcX: 40, cropW: 240, cropH: 180},
		{name: "strided_downscale/320x180_to_160x90", srcW: 320, srcH: 180, dstW: 160, dstH: 90, cropW: 320, cropH: 180, srcStride: 384, dstStride: 192},
	} {
		b.Run(tt.name, func(b *testing.B) {
			srcStride := tt.srcStride
			if srcStride == 0 {
				srcStride = tt.srcW
			}
			dstStride := tt.dstStride
			if dstStride == 0 {
				dstStride = tt.dstW
			}
			src := av.Plane{Buffer: av.Buffer{Bytes: resizeBenchPlane(tt.srcH, srcStride)}, Stride: srcStride}
			dst := av.Plane{Buffer: av.Buffer{Bytes: make([]byte, tt.dstH*dstStride)}, Stride: dstStride}
			var f Filter
			b.ReportAllocs()
			b.SetBytes(int64(tt.dstW * tt.dstH))
			for i := 0; i < b.N; i++ {
				f.scalePlane(&f.luma, &dst, &src, tt.srcW, tt.srcX, tt.srcY, tt.cropW, tt.cropH, tt.dstW, tt.dstH)
			}
		})
	}
}

func BenchmarkScaleI420Kernel(b *testing.B) {
	for _, width := range []int{320, 640} {
		height := width * 9 / 16
		b.Run(fmt.Sprintf("%dx%d_to_half", width, height), func(b *testing.B) {
			src := resizeBenchI420Planes(width, height)
			dstW, dstH := width/2, height/2
			dst := []av.Plane{
				{Buffer: av.Buffer{Bytes: make([]byte, dstW*dstH)}, Stride: dstW},
				{Buffer: av.Buffer{Bytes: make([]byte, dstW*dstH/4)}, Stride: dstW / 2},
				{Buffer: av.Buffer{Bytes: make([]byte, dstW*dstH/4)}, Stride: dstW / 2},
			}
			g := geometry{inputWidth: width, outputWidth: dstW, outputHeight: dstH, cropWidth: width, cropHeight: height}
			var f Filter
			b.ReportAllocs()
			b.SetBytes(int64(dstW * dstH * 3 / 2))
			for i := 0; i < b.N; i++ {
				f.scaleI420(dst, src, g)
			}
		})
	}
}

func resizeBenchPlane(height int, stride int) []byte {
	plane := make([]byte, height*stride)
	for i := range plane {
		plane[i] = byte(i)
	}
	return plane
}

func resizeBenchI420Planes(width int, height int) []av.Plane {
	chromaW, chromaH := width/2, height/2
	return []av.Plane{
		{Buffer: av.Buffer{Bytes: resizeBenchPlane(height, width)}, Stride: width},
		{Buffer: av.Buffer{Bytes: resizeBenchPlane(chromaH, chromaW)}, Stride: chromaW},
		{Buffer: av.Buffer{Bytes: resizeBenchPlane(chromaH, chromaW)}, Stride: chromaW},
	}
}
