package goav

import (
	"fmt"
	"testing"

	"github.com/thesyncim/goav/av"
)

func BenchmarkCopyPlaneKernel(b *testing.B) {
	for _, tt := range []struct {
		name      string
		dstW      int
		dstH      int
		srcW      int
		srcH      int
		srcStride int
		dstX      int
		dstY      int
	}{
		{name: "inside_tight/160x90", dstW: 160, dstH: 90, srcW: 160, srcH: 90, srcStride: 160},
		{name: "inside_strided/160x90", dstW: 320, dstH: 180, srcW: 160, srcH: 90, srcStride: 192, dstX: 80, dstY: 45},
		{name: "clipped_right/160x90", dstW: 160, dstH: 90, srcW: 160, srcH: 90, srcStride: 160, dstX: 80},
	} {
		b.Run(tt.name, func(b *testing.B) {
			src := make([]byte, tt.srcStride*tt.srcH)
			for i := range src {
				src[i] = byte(i)
			}
			dst := make([]byte, tt.dstW*tt.dstH)
			plane := av.Plane{Buffer: av.Buffer{Bytes: src, Ownership: av.BufferImmutable}, Stride: tt.srcStride}
			b.ReportAllocs()
			b.SetBytes(int64(tt.srcW * tt.srcH))
			for i := 0; i < b.N; i++ {
				copyPlane(dst, tt.dstW, tt.dstH, &plane, tt.srcW, tt.srcH, tt.dstX, tt.dstY)
			}
		})
	}
}

func BenchmarkCompositeI420BlitKernel(b *testing.B) {
	for _, width := range []int{160, 320} {
		height := width * 9 / 16
		b.Run(fmt.Sprintf("two_arms/%dx%d", width, height), func(b *testing.B) {
			frameA := compositeTestI420Frame("a", width, height, 100, 10, 20)
			frameB := compositeTestI420Frame("b", width, height, 200, 30, 40)
			dst := &av.Frame{Planes: []av.Plane{
				{Buffer: av.Buffer{Bytes: make([]byte, 0, width*height*2)}},
				{Buffer: av.Buffer{Bytes: make([]byte, 0, width*height/2)}},
				{Buffer: av.Buffer{Bytes: make([]byte, 0, width*height/2)}},
			}}
			video := &av.VideoFrame{}
			frames := []*av.Frame{frameA, frameB}
			layout := []compositeLayout{{X: 0, Y: 0}, {X: width, Y: 0}}
			b.ReportAllocs()
			b.SetBytes(int64(width * height * 3))
			for i := 0; i < b.N; i++ {
				if _, err := compositeI420FramesInto(frames, layout, 0, 0, "out", dst, video); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
