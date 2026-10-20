package cameraav

import (
	"fmt"
	"github.com/thesyncim/goav/av"
)

// Each output plane owns a distinct pooled buffer. Copying active pixels only
// prevents native row padding from leaking, and permits immediate buffer reuse.
func planarFrame(c Config, pts int64) *av.Frame { return c.buffers.take(pts) }
func copyNV12(c Config, pts int64, y, uv []byte, yStride, uvStride int) (*av.Frame, error) {
	if yStride < c.Width || uvStride < c.Width || len(y) < (c.Height-1)*yStride+c.Width || len(uv) < (c.Height/2-1)*uvStride+c.Width {
		return nil, fmt.Errorf("cameraav: truncated NV12 frame")
	}
	f := planarFrame(c, pts)
	if f == nil {
		return nil, nil
	}
	for row := 0; row < c.Height; row++ {
		copy(f.Planes[0].Buffer.Bytes[row*c.Width:(row+1)*c.Width], y[row*yStride:row*yStride+c.Width])
	}
	for row := 0; row < c.Height/2; row++ {
		for col := 0; col < c.Width/2; col++ {
			i := row*c.Width/2 + col
			j := row*uvStride + 2*col
			f.Planes[1].Buffer.Bytes[i] = uv[j]
			f.Planes[2].Buffer.Bytes[i] = uv[j+1]
		}
	}
	return f, nil
}
func copyYUYV(c Config, pts int64, data []byte, stride int) (*av.Frame, error) {
	if stride < c.Width*2 || len(data) < (c.Height-1)*stride+c.Width*2 {
		return nil, fmt.Errorf("cameraav: truncated YUYV frame")
	}
	f := planarFrame(c, pts)
	if f == nil {
		return nil, nil
	}
	for row := 0; row < c.Height; row++ {
		for col := 0; col < c.Width; col++ {
			f.Planes[0].Buffer.Bytes[row*c.Width+col] = data[row*stride+col*2]
		}
	}
	for row := 0; row < c.Height; row += 2 {
		for col := 0; col < c.Width; col += 2 {
			i := row/2*c.Width/2 + col/2
			j := row*stride + col*2
			f.Planes[1].Buffer.Bytes[i] = byte((uint16(data[j+1]) + uint16(data[j+stride+1]) + 1) / 2)
			f.Planes[2].Buffer.Bytes[i] = byte((uint16(data[j+3]) + uint16(data[j+stride+3]) + 1) / 2)
		}
	}
	return f, nil
}
