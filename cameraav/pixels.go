package cameraav

import (
	"encoding/binary"
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
	const (
		maskFF   = 0x00ff00ff00ff00ff
		maskFFFF = 0x0000ffff0000ffff
	)
	for row := range ch {
		src := uv[row*uvStride : row*uvStride+cw*2]
		uRow := uDst[row*cw : (row+1)*cw]
		vRow := vDst[row*cw : (row+1)*cw]
		col := 0
		for ; col+16 <= cw; col += 16 {
			s32 := src[col*2 : col*2+32]
			u16 := uRow[col : col+16]
			v16 := vRow[col : col+16]

			w0 := binary.LittleEndian.Uint64(s32)
			w1 := binary.LittleEndian.Uint64(s32[8:])
			w2 := binary.LittleEndian.Uint64(s32[16:])
			w3 := binary.LittleEndian.Uint64(s32[24:])

			eu0 := w0 & maskFF
			tu0 := (eu0 | (eu0 >> 8)) & maskFFFF
			eu1 := w1 & maskFF
			tu1 := (eu1 | (eu1 >> 8)) & maskFFFF
			binary.LittleEndian.PutUint64(u16, uint64(uint32(tu0|(tu0>>16)))|uint64(uint32(tu1|(tu1>>16)))<<32)

			eu2 := w2 & maskFF
			tu2 := (eu2 | (eu2 >> 8)) & maskFFFF
			eu3 := w3 & maskFF
			tu3 := (eu3 | (eu3 >> 8)) & maskFFFF
			binary.LittleEndian.PutUint64(u16[8:], uint64(uint32(tu2|(tu2>>16)))|uint64(uint32(tu3|(tu3>>16)))<<32)

			ev0 := (w0 >> 8) & maskFF
			tv0 := (ev0 | (ev0 >> 8)) & maskFFFF
			ev1 := (w1 >> 8) & maskFF
			tv1 := (ev1 | (ev1 >> 8)) & maskFFFF
			binary.LittleEndian.PutUint64(v16, uint64(uint32(tv0|(tv0>>16)))|uint64(uint32(tv1|(tv1>>16)))<<32)

			ev2 := (w2 >> 8) & maskFF
			tv2 := (ev2 | (ev2 >> 8)) & maskFFFF
			ev3 := (w3 >> 8) & maskFF
			tv3 := (ev3 | (ev3 >> 8)) & maskFFFF
			binary.LittleEndian.PutUint64(v16[8:], uint64(uint32(tv2|(tv2>>16)))|uint64(uint32(tv3|(tv3>>16)))<<32)
		}
		for ; col+8 <= cw; col += 8 {
			s16 := src[col*2 : col*2+16]
			w0 := binary.LittleEndian.Uint64(s16)
			w1 := binary.LittleEndian.Uint64(s16[8:])

			eu0 := w0 & maskFF
			tu0 := (eu0 | (eu0 >> 8)) & maskFFFF
			u0 := uint32(tu0 | (tu0 >> 16))
			eu1 := w1 & maskFF
			tu1 := (eu1 | (eu1 >> 8)) & maskFFFF
			u1 := uint32(tu1 | (tu1 >> 16))
			binary.LittleEndian.PutUint64(uRow[col:], uint64(u0)|uint64(u1)<<32)

			ev0 := (w0 >> 8) & maskFF
			tv0 := (ev0 | (ev0 >> 8)) & maskFFFF
			v0 := uint32(tv0 | (tv0 >> 16))
			ev1 := (w1 >> 8) & maskFF
			tv1 := (ev1 | (ev1 >> 8)) & maskFFFF
			v1 := uint32(tv1 | (tv1 >> 16))
			binary.LittleEndian.PutUint64(vRow[col:], uint64(v0)|uint64(v1)<<32)
		}
		if col+4 <= cw {
			w0 := binary.LittleEndian.Uint64(src[col*2:])
			eu0 := w0 & maskFF
			tu0 := (eu0 | (eu0 >> 8)) & maskFFFF
			binary.LittleEndian.PutUint32(uRow[col:], uint32(tu0|(tu0>>16)))
			ev0 := (w0 >> 8) & maskFF
			tv0 := (ev0 | (ev0 >> 8)) & maskFFFF
			binary.LittleEndian.PutUint32(vRow[col:], uint32(tv0|(tv0>>16)))
			col += 4
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
	const (
		maskFF   = 0x00ff00ff00ff00ff
		round1   = 0x0001000100010001
		maskFFFF = 0x0000ffff0000ffff
	)
	// Single pass over pairs of rows: extract both luma rows and average chroma.
	for row := range ch {
		r0 := row * 2
		src0 := data[r0*stride : r0*stride+w*2]
		src1 := data[(r0+1)*stride : (r0+1)*stride+w*2]
		yRow0 := yDst[r0*w : (r0+1)*w]
		yRow1 := yDst[(r0+1)*w : (r0+2)*w]
		uRow := uDst[row*cw : (row+1)*cw]
		vRow := vDst[row*cw : (row+1)*cw]
		cx := 0
		for ; cx+2 <= cw; cx += 2 {
			j := cx * 4
			x := cx * 2
			a := binary.LittleEndian.Uint64(src0[j:])
			b := binary.LittleEndian.Uint64(src1[j:])

			ya := a & maskFF
			tya := (ya | (ya >> 8)) & maskFFFF
			binary.LittleEndian.PutUint32(yRow0[x:], uint32(tya|(tya>>16)))

			yb := b & maskFF
			tyb := (yb | (yb >> 8)) & maskFFFF
			binary.LittleEndian.PutUint32(yRow1[x:], uint32(tyb|(tyb>>16)))

			avg := (((a >> 8) & maskFF) + ((b >> 8) & maskFF) + round1) >> 1
			uRow[cx] = byte(avg)
			vRow[cx] = byte(avg >> 16)
			uRow[cx+1] = byte(avg >> 32)
			vRow[cx+1] = byte(avg >> 48)
		}
		for ; cx < cw; cx++ {
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
