package engine

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/thesyncim/goav/av"
)

// Matrix is a limited-range Y'CbCr to RGB conversion matrix.
type Matrix uint8

const (
	// BT601 is the standard-definition matrix (Kr=0.299, Kb=0.114).
	BT601 Matrix = iota
	// BT709 is the high-definition matrix (Kr=0.2126, Kb=0.0722).
	BT709
)

// MatrixFor picks the matrix for an untagged frame the way common players do:
// BT.709 from 720 lines up, BT.601 below.
func MatrixFor(height int) Matrix {
	if height >= 720 {
		return BT709
	}
	return BT601
}

// coefficients are the 16.16 fixed-point factors of one matrix: luma scale,
// Cr→R, Cb→G, Cr→G, Cb→B. Derived from Kr/Kb with 255/219 luma and 255/224
// chroma expansion (limited range).
type coefficients struct{ y, rv, gu, gv, bu int32 }

var matrices = [...]coefficients{
	BT601: {y: 76309, rv: 104597, gu: 25675, gv: 53279, bu: 132201},
	BT709: {y: 76309, rv: 117489, gu: 13975, gv: 34925, bu: 138438},
}

// subsampling reports the chroma shift for a pixel format, and whether it is
// luma only.
func subsampling(format string) (xs, ys int, gray bool, err error) {
	switch format {
	case av.PixelFormatI420, av.PixelFormatYUV420P:
		return 1, 1, false, nil
	case av.PixelFormatI422, av.PixelFormatYUV422P:
		return 1, 0, false, nil
	case av.PixelFormatI444, av.PixelFormatYUV444P:
		return 0, 0, false, nil
	case av.PixelFormatGray8:
		return 0, 0, true, nil
	}
	return 0, 0, false, fmt.Errorf("player: unsupported pixel format %q (want i420, i422, i444, or gray8)", format)
}

// ConvertRGBA writes frame f as packed RGBA into dst, which must hold
// 4*width*height bytes. Large frames convert in parallel row bands.
func ConvertRGBA(dst []byte, f *av.Frame, m Matrix) error {
	if f == nil || f.Video == nil {
		return fmt.Errorf("player: not a video frame")
	}
	w, h := f.Video.Width, f.Video.Height
	if w <= 0 || h <= 0 {
		return fmt.Errorf("player: invalid frame size %dx%d", w, h)
	}
	if len(dst) < 4*w*h {
		return fmt.Errorf("player: RGBA buffer holds %d bytes, need %d", len(dst), 4*w*h)
	}
	xs, ys, gray, err := subsampling(f.Video.PixelFormat)
	if err != nil {
		return err
	}
	planes := 3
	if gray {
		planes = 1
	}
	if len(f.Planes) < planes {
		return fmt.Errorf("player: %s frame carries %d planes, need %d", f.Video.PixelFormat, len(f.Planes), planes)
	}
	cw, chh := (w+(1<<xs)-1)>>xs, (h+(1<<ys)-1)>>ys
	for i := 0; i < planes; i++ {
		pw, ph := w, h
		if i > 0 {
			pw, ph = cw, chh
		}
		p := f.Planes[i]
		if p.Stride < pw || p.Offset < 0 || p.Offset+(ph-1)*p.Stride+pw > len(p.Buffer.Bytes) {
			return fmt.Errorf("player: plane %d is too small for %dx%d", i, pw, ph)
		}
	}
	job := convertJob{dst: dst, f: f, c: matrices[m], w: w, xs: xs, ys: ys, gray: gray}
	bands := 1
	if w*h >= 1<<18 {
		bands = min(runtime.GOMAXPROCS(0), 8)
	}
	if bands <= 1 {
		job.rows(0, h)
		return nil
	}
	// Bands start on even rows so a 4:2:0 chroma row is never split.
	step := ((h+bands-1)/bands + 1) &^ 1
	var wg sync.WaitGroup
	for y0 := 0; y0 < h; y0 += step {
		wg.Go(func() { job.rows(y0, min(y0+step, h)) })
	}
	wg.Wait()
	return nil
}

type convertJob struct {
	dst    []byte
	f      *av.Frame
	c      coefficients
	w      int
	xs, ys int
	gray   bool
}

func (j *convertJob) rows(y0, y1 int) {
	yp := j.f.Planes[0]
	c := j.c
	for y := y0; y < y1; y++ {
		yrow := yp.Buffer.Bytes[yp.Offset+y*yp.Stride:]
		out := j.dst[4*j.w*y : 4*j.w*(y+1)]
		if j.gray {
			for x := 0; x < j.w; x++ {
				l := clamp8((int32(yrow[x])-16)*c.y + 1<<15)
				o := out[4*x : 4*x+4 : 4*x+4]
				o[0], o[1], o[2], o[3] = l, l, l, 255
			}
			continue
		}
		up, vp := j.f.Planes[1], j.f.Planes[2]
		cy := y >> j.ys
		urow := up.Buffer.Bytes[up.Offset+cy*up.Stride:]
		vrow := vp.Buffer.Bytes[vp.Offset+cy*vp.Stride:]
		for x := 0; x < j.w; x++ {
			cx := x >> j.xs
			l := (int32(yrow[x])-16)*c.y + 1<<15
			u := int32(urow[cx]) - 128
			v := int32(vrow[cx]) - 128
			o := out[4*x : 4*x+4 : 4*x+4]
			o[0] = clamp8(l + c.rv*v)
			o[1] = clamp8(l - c.gu*u - c.gv*v)
			o[2] = clamp8(l + c.bu*u)
			o[3] = 255
		}
	}
}

// clamp8 shifts a 16.16 fixed-point value to a byte, saturating.
func clamp8(v int32) byte {
	v >>= 16
	if v < 0 {
		return 0
	}
	if v > 255 {
		return 255
	}
	return byte(v)
}
