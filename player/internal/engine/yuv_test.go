package engine

import (
	"math"
	"testing"

	"github.com/thesyncim/goav/av"
)

// referenceRGB is the textbook limited-range conversion, derived from Kr and
// Kb in floating point, independently of the fixed-point table.
func referenceRGB(y, u, v byte, kr, kb float64) [3]float64 {
	kg := 1 - kr - kb
	yf := (float64(y) - 16) * 255 / 219
	pb := (float64(u) - 128) * 255 / 224
	pr := (float64(v) - 128) * 255 / 224
	r := yf + 2*(1-kr)*pr
	g := yf - 2*kb*(1-kb)/kg*pb - 2*kr*(1-kr)/kg*pr
	b := yf + 2*(1-kb)*pb
	clamp := func(x float64) float64 { return math.Max(0, math.Min(255, x)) }
	return [3]float64{clamp(r), clamp(g), clamp(b)}
}

// planarFrame builds a frame of the given format whose planes sit at odd
// offsets with padded strides, the layouts decoders hand out.
func planarFrame(format string, w, h int, yv, uv, vv func(x, y int) byte) *av.Frame {
	xs, ys, gray, err := subsampling(format)
	if err != nil {
		panic(err)
	}
	cw, chh := (w+(1<<xs)-1)>>xs, (h+(1<<ys)-1)>>ys
	plane := func(pw, ph int, at func(x, y int) byte) av.Plane {
		stride, offset := pw+7, 3
		buf := make([]byte, offset+stride*ph)
		for y := range ph {
			for x := range pw {
				buf[offset+y*stride+x] = at(x, y)
			}
		}
		return av.Plane{Buffer: av.Buffer{Bytes: buf}, Stride: stride, Offset: offset}
	}
	f := &av.Frame{Type: av.MediaVideo, Video: &av.VideoFrame{Width: w, Height: h, PixelFormat: format}}
	f.Planes = append(f.Planes, plane(w, h, yv))
	if !gray {
		f.Planes = append(f.Planes, plane(cw, chh, uv), plane(cw, chh, vv))
	}
	return f
}

func convert(t *testing.T, f *av.Frame, m Matrix) []byte {
	t.Helper()
	dst := make([]byte, 4*f.Video.Width*f.Video.Height)
	if err := ConvertRGBA(dst, f, m); err != nil {
		t.Fatal(err)
	}
	return dst
}

func TestConvertMatchesReferenceAcrossTheGamut(t *testing.T) {
	for _, tc := range []struct {
		name   string
		m      Matrix
		kr, kb float64
	}{
		{"bt601", BT601, 0.299, 0.114},
		{"bt709", BT709, 0.2126, 0.0722},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// One pixel per (Y, Cb, Cr) sample on a lattice over the legal range.
			var ys, cs []byte
			for v := 16; v <= 235; v += 9 {
				ys = append(ys, byte(v))
			}
			for v := 16; v <= 240; v += 14 {
				cs = append(cs, byte(v))
			}
			w := len(ys)
			h := len(cs) * len(cs)
			f := planarFrame(av.PixelFormatI444, w, h,
				func(x, _ int) byte { return ys[x] },
				func(_, y int) byte { return cs[y/len(cs)] },
				func(_, y int) byte { return cs[y%len(cs)] })
			got := convert(t, f, tc.m)
			worst := 0.0
			for y := range h {
				for x := range w {
					want := referenceRGB(ys[x], cs[y/len(cs)], cs[y%len(cs)], tc.kr, tc.kb)
					for c := range 3 {
						worst = math.Max(worst, math.Abs(float64(got[4*(y*w+x)+c])-want[c]))
					}
					if got[4*(y*w+x)+3] != 255 {
						t.Fatalf("alpha at %d,%d = %d", x, y, got[4*(y*w+x)+3])
					}
				}
			}
			if worst > 1 {
				t.Fatalf("worst channel error against the float reference = %.2f, want <= 1", worst)
			}
		})
	}
}

func TestConvertKnownColors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		m       Matrix
		y, u, v byte
		want    [3]byte
	}{
		{"black", BT601, 16, 128, 128, [3]byte{0, 0, 0}},
		{"white", BT601, 235, 128, 128, [3]byte{255, 255, 255}},
		{"grey", BT709, 126, 128, 128, [3]byte{128, 128, 128}},
		{"601 red", BT601, 81, 90, 240, [3]byte{255, 0, 0}},
		{"601 green", BT601, 145, 54, 34, [3]byte{0, 255, 0}},
		{"601 blue", BT601, 41, 240, 110, [3]byte{0, 0, 255}},
		{"709 red", BT709, 63, 102, 240, [3]byte{255, 0, 0}},
		{"709 green", BT709, 173, 42, 26, [3]byte{0, 255, 0}},
		{"709 blue", BT709, 32, 240, 118, [3]byte{0, 0, 255}},
		{"below black clamps", BT601, 0, 128, 128, [3]byte{0, 0, 0}},
		{"above white clamps", BT601, 255, 128, 128, [3]byte{255, 255, 255}},
	} {
		f := planarFrame(av.PixelFormatI420, 2, 2,
			func(int, int) byte { return tc.y }, func(int, int) byte { return tc.u }, func(int, int) byte { return tc.v })
		got := convert(t, f, tc.m)
		for c := range 3 {
			if d := int(got[c]) - int(tc.want[c]); d < -2 || d > 2 {
				t.Errorf("%s: RGB = %v, want %v (±2)", tc.name, got[:3], tc.want)
				break
			}
		}
	}
}

func TestConvertSubsamplingAddressesTheRightChroma(t *testing.T) {
	// Odd 5x3 frame: 4:2:0 chroma is 3x2, 4:2:2 is 3x3. Each chroma sample
	// gets a distinct Cr; pixel (x, y) must read sample (x>>1, y>>ys).
	cr := func(x, y int) byte { return byte(128 + 10*x + 40*y) }
	for _, tc := range []struct {
		format string
		ys     int
	}{{av.PixelFormatI420, 1}, {av.PixelFormatYUV422P, 0}} {
		f := planarFrame(tc.format, 5, 3, func(int, int) byte { return 126 }, func(int, int) byte { return 128 }, cr)
		got := convert(t, f, BT601)
		for y := range 3 {
			for x := range 5 {
				want := referenceRGB(126, 128, cr(x>>1, y>>tc.ys), 0.299, 0.114)
				if d := math.Abs(float64(got[4*(y*5+x)]) - want[0]); d > 1 {
					t.Fatalf("%s pixel %d,%d red = %d, want %.1f", tc.format, x, y, got[4*(y*5+x)], want[0])
				}
			}
		}
	}
}

func TestConvertGray(t *testing.T) {
	f := planarFrame(av.PixelFormatGray8, 3, 1, func(x, _ int) byte { return []byte{16, 126, 235}[x] }, nil, nil)
	got := convert(t, f, BT601)
	for x, want := range []byte{0, 128, 255} {
		if g := got[4*x]; g != want || got[4*x+1] != want || got[4*x+2] != want {
			t.Fatalf("gray pixel %d = %v, want %d", x, got[4*x:4*x+3], want)
		}
	}
}

func TestConvertParallelBandsMatchOnePass(t *testing.T) {
	w, h := 1024, 515
	at := func(seed int) func(x, y int) byte { return func(x, y int) byte { return byte((x*seed + y*7) % 256) } }
	f := planarFrame(av.PixelFormatI420, w, h, at(3), at(5), at(11))
	banded := convert(t, f, BT709)
	single := make([]byte, 4*w*h)
	job := convertJob{dst: single, f: f, c: matrices[BT709], w: w, xs: 1, ys: 1}
	job.rows(0, h)
	for i := range single {
		if single[i] != banded[i] {
			t.Fatalf("banded conversion differs at byte %d", i)
		}
	}
}

func TestConvertRefusesBadInput(t *testing.T) {
	f := planarFrame(av.PixelFormatI420, 4, 4, func(int, int) byte { return 0 }, func(int, int) byte { return 0 }, func(int, int) byte { return 0 })
	if err := ConvertRGBA(make([]byte, 10), f, BT601); err == nil {
		t.Fatal("short destination accepted")
	}
	f.Video.PixelFormat = "nv12"
	if err := ConvertRGBA(make([]byte, 64), f, BT601); err == nil {
		t.Fatal("unsupported pixel format accepted")
	}
	f.Video.PixelFormat = av.PixelFormatI420
	f.Planes[1].Buffer.Bytes = f.Planes[1].Buffer.Bytes[:2]
	if err := ConvertRGBA(make([]byte, 64), f, BT601); err == nil {
		t.Fatal("truncated chroma plane accepted")
	}
}

func TestMatrixFor(t *testing.T) {
	if MatrixFor(480) != BT601 || MatrixFor(720) != BT709 || MatrixFor(2160) != BT709 {
		t.Fatal("matrix selection by height is wrong")
	}
}
