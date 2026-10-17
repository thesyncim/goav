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
	p.scale(dst, dstWidth, src, len(src), 0, 0, len(src), 1, dstWidth, 1, nil)
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
	p.scale(dst, 1, src, 2, 0, 0, 2, 2, 1, 4, nil)
	// Columns average to 50 and 150, then rows interpolate 50 -> 150.
	if want := []byte{50, 75, 125, 150}; !bytes.Equal(dst, want) {
		t.Fatalf("2x2 -> 1x4 = %v, want %v", dst, want)
	}
}
