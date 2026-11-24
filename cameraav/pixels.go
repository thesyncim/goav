package cameraav

import (
	"fmt"

	"github.com/thesyncim/goav/av"
)

// Each output plane owns a distinct pooled buffer. Copying active pixels only
// prevents native row padding from leaking, and permits immediate buffer reuse.
func planarFrame(c Config, pts int64) *av.Frame { return c.buffers.take(pts) }

func copyNV12(c Config, pts int64, y, uv []byte, yStride, uvStride int) (*av.Frame, error) {
	w, h := c.Width, c.Height
	if yStride < w || uvStride < w || len(y) < (h-1)*yStride+w || len(uv) < (h/2-1)*uvStride+w {
		return nil, fmt.Errorf("cameraav: truncated NV12 frame")
	}
	f := planarFrame(c, pts)
	if f == nil {
		return nil, nil
	}
	yDst := f.Planes[0].Buffer.Bytes
	if yStride == w {
		copy(yDst[:w*h], y[:w*h])
	} else {
		for row := range h {
			copy(yDst[row*w:(row+1)*w], y[row*yStride:row*yStride+w])
		}
	}
	uDst := f.Planes[1].Buffer.Bytes
	vDst := f.Planes[2].Buffer.Bytes
	cw, ch := w/2, h/2
	for row := range ch {
		src := uv[row*uvStride : row*uvStride+cw*2]
		uRow := uDst[row*cw : (row+1)*cw]
		vRow := vDst[row*cw : (row+1)*cw]
		col := 0
		for ; col+3 < cw; col += 4 {
			j := col * 2
			uRow[col], vRow[col] = src[j], src[j+1]
			uRow[col+1], vRow[col+1] = src[j+2], src[j+3]
			uRow[col+2], vRow[col+2] = src[j+4], src[j+5]
			uRow[col+3], vRow[col+3] = src[j+6], src[j+7]
		}
		for ; col < cw; col++ {
			j := col * 2
			uRow[col], vRow[col] = src[j], src[j+1]
		}
	}
	return f, nil
}

func copyYUYV(c Config, pts int64, data []byte, stride int) (*av.Frame, error) {
	w, h := c.Width, c.Height
	if stride < w*2 || len(data) < (h-1)*stride+w*2 {
		return nil, fmt.Errorf("cameraav: truncated YUYV frame")
	}
	f := planarFrame(c, pts)
	if f == nil {
		return nil, nil
	}
	yDst := f.Planes[0].Buffer.Bytes
	uDst := f.Planes[1].Buffer.Bytes
	vDst := f.Planes[2].Buffer.Bytes
	cw, ch := w/2, h/2
	// Single pass over pairs of rows: extract both luma rows and average chroma.
	for row := range ch {
		r0 := row * 2
		src0 := data[r0*stride : r0*stride+w*2]
		src1 := data[(r0+1)*stride : (r0+1)*stride+w*2]
		yRow0 := yDst[r0*w : (r0+1)*w]
		yRow1 := yDst[(r0+1)*w : (r0+2)*w]
		uRow := uDst[row*cw : (row+1)*cw]
		vRow := vDst[row*cw : (row+1)*cw]
		for cx := range cw {
			j := cx * 4
			x := cx * 2
			yRow0[x] = src0[j]
			yRow0[x+1] = src0[j+2]
			yRow1[x] = src1[j]
			yRow1[x+1] = src1[j+2]
			uRow[cx] = byte((uint16(src0[j+1]) + uint16(src1[j+1]) + 1) >> 1)
			vRow[cx] = byte((uint16(src0[j+3]) + uint16(src1[j+3]) + 1) >> 1)
		}
	}
	return f, nil
}
