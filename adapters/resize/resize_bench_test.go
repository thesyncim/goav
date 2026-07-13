package resize

import (
	"fmt"
	"testing"
)

func BenchmarkScalePlaneNearestKernel(b *testing.B) {
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
			src := resizeBenchPlane(tt.srcH, srcStride)
			dst := make([]byte, tt.dstH*dstStride)
			b.ReportAllocs()
			b.SetBytes(int64(tt.dstW * tt.dstH))
			for i := 0; i < b.N; i++ {
				scalePlaneNearest(dst, dstStride, src, srcStride, tt.srcX, tt.srcY, tt.cropW, tt.cropH, tt.dstW, tt.dstH)
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
			dst := []planeBench{
				{bytes: make([]byte, dstW*dstH), stride: dstW},
				{bytes: make([]byte, dstW*dstH/4), stride: dstW / 2},
				{bytes: make([]byte, dstW*dstH/4), stride: dstW / 2},
			}
			g := geometry{inputWidth: width, outputWidth: dstW, outputHeight: dstH, cropWidth: width, cropHeight: height}
			b.ReportAllocs()
			b.SetBytes(int64(dstW * dstH * 3 / 2))
			for i := 0; i < b.N; i++ {
				scalePlaneNearest(dst[0].bytes, dst[0].stride, src[0].bytes, src[0].stride, g.cropX, g.cropY, g.cropWidth, g.cropHeight, g.outputWidth, g.outputHeight)
				scalePlaneNearest(dst[1].bytes, dst[1].stride, src[1].bytes, src[1].stride, g.cropX/2, g.cropY/2, g.cropWidth/2, g.cropHeight/2, g.outputWidth/2, g.outputHeight/2)
				scalePlaneNearest(dst[2].bytes, dst[2].stride, src[2].bytes, src[2].stride, g.cropX/2, g.cropY/2, g.cropWidth/2, g.cropHeight/2, g.outputWidth/2, g.outputHeight/2)
			}
		})
	}
}

type planeBench struct {
	bytes  []byte
	stride int
}

func resizeBenchPlane(height int, stride int) []byte {
	plane := make([]byte, height*stride)
	for i := range plane {
		plane[i] = byte(i)
	}
	return plane
}

func resizeBenchI420Planes(width int, height int) []planeBench {
	chromaW, chromaH := width/2, height/2
	return []planeBench{
		{bytes: resizeBenchPlane(height, width), stride: width},
		{bytes: resizeBenchPlane(chromaH, chromaW), stride: chromaW},
		{bytes: resizeBenchPlane(chromaH, chromaW), stride: chromaW},
	}
}
