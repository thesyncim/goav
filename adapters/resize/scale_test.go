package resize

import (
	"bytes"
	"testing"
)

// scaleRow resamples one row (height 1, so the vertical axis is identity).
func scaleRow(t *testing.T, src []byte, dstWidth int) []byte {
	t.Helper()
	var p planeScaler
	dst := make([]byte, dstWidth)
	p.scale(planeBuf{data: dst, stride: dstWidth}, planeBuf{data: src, stride: len(src)}, scaleRect{srcW: len(src), srcH: 1, dstW: dstWidth, dstH: 1}, nil)
	return dst
}

// TestScaleDownscaleDoesNotAlias pins why downscaling averages area: a
// pattern finer than the output grid (alternating black and white columns)
// becomes its mean, where point sampling would keep only one phase and
// render a solid black or white plane.
func TestScaleDownscaleDoesNotAlias(t *testing.T) {
	src := make([]byte, 64)
	for i := range src {
		if i%2 == 1 {
			src[i] = 255
		}
	}
	for _, got := range scaleRow(t, src, 32) {
		if got != 128 {
			t.Fatalf("2:1 downscale of alternating 0/255 = %d, want the mean 128", got)
		}
	}
}

// TestScaleAreaWeightsCoverage pins coverage weighting at a non-integer
// ratio: 3 samples onto 2, each output covering 1.5 source samples.
func TestScaleAreaWeightsCoverage(t *testing.T) {
	got := scaleRow(t, []byte{0, 90, 180}, 2)
	// out0 = (0*1 + 90*0.5) / 1.5 = 30; out1 = (90*0.5 + 180*1) / 1.5 = 150.
	if want := []byte{30, 150}; !bytes.Equal(got, want) {
		t.Fatalf("3->2 = %v, want %v", got, want)
	}
}

// TestScaleUpscaleInterpolatesLinearly pins upscaling: centers aligned,
// linear between the nearest samples, edges clamped.
func TestScaleUpscaleInterpolatesLinearly(t *testing.T) {
	got := scaleRow(t, []byte{0, 200}, 4)
	if want := []byte{0, 50, 150, 200}; !bytes.Equal(got, want) {
		t.Fatalf("2->4 = %v, want %v", got, want)
	}
}

// TestScaleSeparableAxes pins that each axis picks its own filter: a plane
// shrunk horizontally and grown vertically averages columns and
// interpolates rows.
func TestScaleSeparableAxes(t *testing.T) {
	src := []byte{
		0, 100,
		200, 100,
	}
	var p planeScaler
	dst := make([]byte, 4)
	p.scale(planeBuf{data: dst, stride: 1}, planeBuf{data: src, stride: 2}, scaleRect{srcW: 2, srcH: 2, dstW: 1, dstH: 4}, nil)
	// Columns average to 50 and 150, then rows interpolate 50 -> 150.
	if want := []byte{50, 75, 125, 150}; !bytes.Equal(dst, want) {
		t.Fatalf("2x2 -> 1x4 = %v, want %v", dst, want)
	}
}

// TestScaleFastPathsMatchScalarReference pins that both the single-pass 2x2
// box kernel and the vectorized vertical pass produce bit-identical output to
// the full scalar Q14 separable reference across aligned, unaligned, strided,
// cropped, downscaled, and upscaled planes.
func TestScaleFastPathsMatchScalarReference(t *testing.T) {
	cases := []struct {
		name      string
		srcW      int
		srcH      int
		srcX      int
		srcY      int
		cropW     int
		cropH     int
		dstW      int
		dstH      int
		srcStride int
		dstStride int
	}{
		{name: "half_small_odd", srcW: 14, srcH: 6, cropW: 14, cropH: 6, dstW: 7, dstH: 3},
		{name: "half_vector_unaligned", srcW: 150, srcH: 42, cropW: 150, cropH: 42, dstW: 75, dstH: 21},
		{name: "half_cropped_strided", srcW: 160, srcH: 60, srcX: 12, srcY: 6, cropW: 138, cropH: 46, dstW: 69, dstH: 23, srcStride: 192, dstStride: 80},
		{name: "arbitrary_downscale", srcW: 127, srcH: 53, cropW: 127, cropH: 53, dstW: 71, dstH: 29},
		{name: "hd_ratio_downscale", srcW: 192, srcH: 108, cropW: 192, cropH: 108, dstW: 128, dstH: 72},
		{name: "upscale", srcW: 37, srcH: 19, cropW: 37, cropH: 19, dstW: 83, dstH: 41},
		{name: "mixed_axes", srcW: 99, srcH: 17, cropW: 99, cropH: 17, dstW: 45, dstH: 39},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srcStride := tc.srcStride
			if srcStride == 0 {
				srcStride = tc.srcW
			}
			dstStride := tc.dstStride
			if dstStride == 0 {
				dstStride = tc.dstW
			}
			src := make([]byte, tc.srcH*srcStride)
			// Deterministic non-periodic pattern covering 0..255 extremes.
			var x uint32 = 0x12345678
			for i := range src {
				x = x*1664525 + 1013904223
				src[i] = byte(x >> 24)
			}

			got := make([]byte, tc.dstH*dstStride)
			want := make([]byte, tc.dstH*dstStride)

			var fast planeScaler
			fast.scale(planeBuf{data: got, stride: dstStride}, planeBuf{data: src, stride: srcStride}, scaleRect{srcX: tc.srcX, srcY: tc.srcY, srcW: tc.cropW, srcH: tc.cropH, dstW: tc.dstW, dstH: tc.dstH}, nil)

			// Full scalar two-pass Q14 reference.
			var ref planeScaler
			ref.x.reset(tc.cropW, tc.dstW)
			ref.y.reset(tc.cropH, tc.dstH)
			tmp := make([]uint16, tc.cropH*tc.dstW)
			for r := range tc.cropH {
				base := (tc.srcY+r)*srcStride + tc.srcX
				ref.x.filterRow(tmp[r*tc.dstW:(r+1)*tc.dstW], src[base:base+tc.cropW])
			}
			ref.scaleVerticalScalar(want, dstStride, tmp, tc.dstW, tc.dstH)

			if !bytes.Equal(got, want) {
				for y := range tc.dstH {
					for col := range tc.dstW {
						idx := y*dstStride + col
						if got[idx] != want[idx] {
							t.Fatalf("mismatch at (%d,%d): got %d, want %d", col, y, got[idx], want[idx])
						}
					}
				}
			}
		})
	}
}
