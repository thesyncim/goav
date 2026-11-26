package engine

import (
	"encoding/binary"
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
	case av.PixelFormatI420:
		return 1, 1, false, nil
	case av.PixelFormatI422:
		return 1, 0, false, nil
	case av.PixelFormatI444:
		return 0, 0, false, nil
	case av.PixelFormatGray8:
		return 0, 0, true, nil
	}
	return 0, 0, false, fmt.Errorf("player: unsupported pixel format %q (want i420, i422, i444, or gray8)", format)
}

// ConvertRGBA writes frame f as packed RGBA into dst, which must hold
// 4*width*height bytes. Large frames convert in parallel row bands using a
// reusable worker pool with zero per-frame allocations.
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
		bands = min(runtime.GOMAXPROCS(0), maxConvertWorkers)
	}
	if bands <= 1 {
		job.rows(0, h)
		return nil
	}
	runConvertBands(job, h, bands)
	return nil
}

const maxConvertWorkers = 8

type convertBand struct {
	y0 int
	y1 int
}

type convertPool struct {
	mu      sync.Mutex
	started bool
	workers int
	job     convertJob
	work    [maxConvertWorkers]chan convertBand
	done    [maxConvertWorkers]chan struct{}
}

var yuvPool convertPool

func runConvertBands(job convertJob, h, bands int) {
	// Bands start on even rows so a 4:2:0 chroma row is never split.
	step := ((h+bands-1)/bands + 1) &^ 1
	if step >= h {
		job.rows(0, h)
		return
	}
	yuvPool.mu.Lock()
	defer yuvPool.mu.Unlock()
	yuvPool.job = job
	if !yuvPool.started {
		yuvPool.workers = maxConvertWorkers
		for i := range maxConvertWorkers {
			wch := make(chan convertBand)
			dch := make(chan struct{})
			yuvPool.work[i] = wch
			yuvPool.done[i] = dch
			go func() {
				for b := range wch {
					yuvPool.job.rows(b.y0, b.y1)
					dch <- struct{}{}
				}
			}()
		}
		yuvPool.started = true
	}
	// Dispatch bands 1..N-1 to pooled workers; run band 0 on the caller goroutine.
	dispatched := 0
	for y0 := step; y0 < h && dispatched < yuvPool.workers; y0 += step {
		y1 := min(y0+step, h)
		if dispatched == yuvPool.workers-1 {
			y1 = h
		}
		yuvPool.work[dispatched] <- convertBand{y0: y0, y1: y1}
		dispatched++
		if y1 == h {
			break
		}
	}
	yuvPool.job.rows(0, min(step, h))
	for i := range dispatched {
		<-yuvPool.done[i]
	}
	yuvPool.job = convertJob{}
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
	if !j.gray && j.xs == 1 && j.ys == 1 {
		j.rowsI420(y0, y1)
		return
	}
	yp := j.f.Planes[0]
	c := j.c
	for y := y0; y < y1; y++ {
		yrow := yp.Buffer.Bytes[yp.Offset+y*yp.Stride:]
		out := j.dst[4*j.w*y : 4*j.w*(y+1)]
		if j.gray {
			_ = yrow[j.w-1]
			_ = out[4*j.w-1]
			for x := 0; x < j.w; x++ {
				l := clamp8((int32(yrow[x])-16)*c.y + 1<<15)
				o := 4 * x
				out[o], out[o+1], out[o+2], out[o+3] = l, l, l, 255
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
			o := 4 * x
			out[o] = clamp8(l + c.rv*v)
			out[o+1] = clamp8(l - c.gu*u - c.gv*v)
			out[o+2] = clamp8(l + c.bu*u)
			out[o+3] = 255
		}
	}
}

// rowsI420 processes 2x2 pixel blocks so each chroma sample and its three
// matrix products are computed once for 4 pixels instead of 4 times.
func (j *convertJob) rowsI420(y0, y1 int) {
	yp, up, vp := j.f.Planes[0], j.f.Planes[1], j.f.Planes[2]
	c := j.c
	w := j.w
	pairs := w / 2
	oddW := w&1 != 0

	y := y0
	for ; y+1 < y1; y += 2 {
		yrow0 := yp.Buffer.Bytes[yp.Offset+y*yp.Stride : yp.Offset+y*yp.Stride+w]
		yrow1 := yp.Buffer.Bytes[yp.Offset+(y+1)*yp.Stride : yp.Offset+(y+1)*yp.Stride+w]
		cy := y >> 1
		cw := (w + 1) >> 1
		urow := up.Buffer.Bytes[up.Offset+cy*up.Stride : up.Offset+cy*up.Stride+cw]
		vrow := vp.Buffer.Bytes[vp.Offset+cy*vp.Stride : vp.Offset+cy*vp.Stride+cw]
		out0 := j.dst[4*w*y : 4*w*(y+1)]
		out1 := j.dst[4*w*(y+1) : 4*w*(y+2)]
		cx := 0
		for ; cx+2 <= pairs; cx += 2 {
			u2 := binary.LittleEndian.Uint16(urow[cx:])
			v2 := binary.LittleEndian.Uint16(vrow[cx:])
			u0 := int32(byte(u2)) - 128
			v0 := int32(byte(v2)) - 128
			u1 := int32(byte(u2>>8)) - 128
			v1 := int32(byte(v2>>8)) - 128

			dr0 := c.rv * v0
			dg0 := -c.gu*u0 - c.gv*v0
			db0 := c.bu * u0
			dr1 := c.rv * v1
			dg1 := -c.gu*u1 - c.gv*v1
			db1 := c.bu * u1

			x := cx * 2
			o := cx * 8

			y0w := binary.LittleEndian.Uint32(yrow0[x:])
			l00 := (int32(byte(y0w))-16)*c.y + 1<<15
			l01 := (int32(byte(y0w>>8))-16)*c.y + 1<<15
			l02 := (int32(byte(y0w>>16))-16)*c.y + 1<<15
			l03 := (int32(byte(y0w>>24))-16)*c.y + 1<<15
			p00 := uint32(clamp8(l00+dr0)) | uint32(clamp8(l00+dg0))<<8 | uint32(clamp8(l00+db0))<<16 | 0xff000000
			p01 := uint32(clamp8(l01+dr0)) | uint32(clamp8(l01+dg0))<<8 | uint32(clamp8(l01+db0))<<16 | 0xff000000
			p02 := uint32(clamp8(l02+dr1)) | uint32(clamp8(l02+dg1))<<8 | uint32(clamp8(l02+db1))<<16 | 0xff000000
			p03 := uint32(clamp8(l03+dr1)) | uint32(clamp8(l03+dg1))<<8 | uint32(clamp8(l03+db1))<<16 | 0xff000000
			o0w := out0[o : o+16]
			binary.LittleEndian.PutUint64(o0w, uint64(p00)|uint64(p01)<<32)
			binary.LittleEndian.PutUint64(o0w[8:], uint64(p02)|uint64(p03)<<32)

			y1w := binary.LittleEndian.Uint32(yrow1[x:])
			l10 := (int32(byte(y1w))-16)*c.y + 1<<15
			l11 := (int32(byte(y1w>>8))-16)*c.y + 1<<15
			l12 := (int32(byte(y1w>>16))-16)*c.y + 1<<15
			l13 := (int32(byte(y1w>>24))-16)*c.y + 1<<15
			p10 := uint32(clamp8(l10+dr0)) | uint32(clamp8(l10+dg0))<<8 | uint32(clamp8(l10+db0))<<16 | 0xff000000
			p11 := uint32(clamp8(l11+dr0)) | uint32(clamp8(l11+dg0))<<8 | uint32(clamp8(l11+db0))<<16 | 0xff000000
			p12 := uint32(clamp8(l12+dr1)) | uint32(clamp8(l12+dg1))<<8 | uint32(clamp8(l12+db1))<<16 | 0xff000000
			p13 := uint32(clamp8(l13+dr1)) | uint32(clamp8(l13+dg1))<<8 | uint32(clamp8(l13+db1))<<16 | 0xff000000
			o1w := out1[o : o+16]
			binary.LittleEndian.PutUint64(o1w, uint64(p10)|uint64(p11)<<32)
			binary.LittleEndian.PutUint64(o1w[8:], uint64(p12)|uint64(p13)<<32)
		}
		for ; cx < pairs; cx++ {
			u := int32(urow[cx]) - 128
			v := int32(vrow[cx]) - 128
			dr := c.rv * v
			dg := -c.gu*u - c.gv*v
			db := c.bu * u

			x := cx * 2
			o := cx * 8

			y0 := binary.LittleEndian.Uint16(yrow0[x:])
			l00 := (int32(y0&0xff)-16)*c.y + 1<<15
			l01 := (int32(y0>>8)-16)*c.y + 1<<15
			p00 := uint32(clamp8(l00+dr)) | uint32(clamp8(l00+dg))<<8 | uint32(clamp8(l00+db))<<16 | 0xff000000
			p01 := uint32(clamp8(l01+dr)) | uint32(clamp8(l01+dg))<<8 | uint32(clamp8(l01+db))<<16 | 0xff000000
			binary.LittleEndian.PutUint64(out0[o:], uint64(p00)|uint64(p01)<<32)

			y1v := binary.LittleEndian.Uint16(yrow1[x:])
			l10 := (int32(y1v&0xff)-16)*c.y + 1<<15
			l11 := (int32(y1v>>8)-16)*c.y + 1<<15
			p10 := uint32(clamp8(l10+dr)) | uint32(clamp8(l10+dg))<<8 | uint32(clamp8(l10+db))<<16 | 0xff000000
			p11 := uint32(clamp8(l11+dr)) | uint32(clamp8(l11+dg))<<8 | uint32(clamp8(l11+db))<<16 | 0xff000000
			binary.LittleEndian.PutUint64(out1[o:], uint64(p10)|uint64(p11)<<32)
		}
		if oddW {
			u := int32(urow[pairs]) - 128
			v := int32(vrow[pairs]) - 128
			dr := c.rv * v
			dg := -c.gu*u - c.gv*v
			db := c.bu * u
			x := w - 1
			o := 4 * x
			l0 := (int32(yrow0[x])-16)*c.y + 1<<15
			out0[o], out0[o+1], out0[o+2], out0[o+3] = clamp8(l0+dr), clamp8(l0+dg), clamp8(l0+db), 255
			l1 := (int32(yrow1[x])-16)*c.y + 1<<15
			out1[o], out1[o+1], out1[o+2], out1[o+3] = clamp8(l1+dr), clamp8(l1+dg), clamp8(l1+db), 255
		}
	}
	if y < y1 {
		yrow := yp.Buffer.Bytes[yp.Offset+y*yp.Stride : yp.Offset+y*yp.Stride+w]
		cy := y >> 1
		cw := (w + 1) >> 1
		urow := up.Buffer.Bytes[up.Offset+cy*up.Stride : up.Offset+cy*up.Stride+cw]
		vrow := vp.Buffer.Bytes[vp.Offset+cy*vp.Stride : vp.Offset+cy*vp.Stride+cw]
		out := j.dst[4*w*y : 4*w*(y+1)]
		for x := range w {
			cx := x >> 1
			l := (int32(yrow[x])-16)*c.y + 1<<15
			u := int32(urow[cx]) - 128
			v := int32(vrow[cx]) - 128
			o := 4 * x
			out[o], out[o+1], out[o+2], out[o+3] = clamp8(l+c.rv*v), clamp8(l-c.gu*u-c.gv*v), clamp8(l+c.bu*u), 255
		}
	}
}

// clamp8 shifts a 16.16 fixed-point value to a byte, saturating.
func clamp8(v int32) byte {
	v >>= 16
	if uint32(v) > 255 {
		if v < 0 {
			return 0
		}
		return 255
	}
	return byte(v)
}
