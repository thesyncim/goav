package screenav

import (
	"sync"
	"sync/atomic"

	"github.com/thesyncim/goav/av"
)

var frameSlots sync.Map // *av.Frame -> *frameSlot

type framePool struct {
	slots   []frameSlot
	next    int
	dropped atomic.Uint64
	evict   func() bool
}

type frameSlot struct {
	frame          av.Frame
	video          av.VideoFrame
	planes         [3]av.Plane
	owners         [3]screenPlaneOwner
	busy           atomic.Bool
	frameTemplate  av.Frame
	videoTemplate  av.VideoFrame
	planeTemplates [3]av.Plane
}

type screenPlaneOwner struct {
	slot *frameSlot
	refs atomic.Int32
}

func (o *screenPlaneOwner) Retain() {
	if o.refs.Add(1) <= 1 {
		panic("screenav: retain after release")
	}
}

func (o *screenPlaneOwner) Release() {
	if o.refs.Add(-1) < 0 {
		panic("screenav: double release")
	}
}

func (o *screenPlaneOwner) Refs() int { return int(o.refs.Load()) }

func newFramePool(c Config) *framePool {
	p := &framePool{slots: make([]frameSlot, c.Queue+2)}
	for i := range p.slots {
		s := &p.slots[i]
		s.video = av.VideoFrame{Width: c.Width, Height: c.Height, PixelFormat: av.PixelFormatYUV420P}
		s.frame = av.Frame{
			StreamID: "screen",
			Type:     av.MediaVideo,
			Video:    &s.video,
			Planes:   s.planes[:],
			Duration: av.Duration{Value: 1, Base: av.TimeBase{Num: 1, Den: int64(c.FPS)}},
		}
		ysize := c.Width * c.Height
		data := make([]byte, ysize*3/2)
		spans := [][]byte{
			data[:ysize:ysize],
			data[ysize : ysize+ysize/4 : ysize+ysize/4],
			data[ysize+ysize/4:],
		}
		for j := range s.owners {
			s.owners[j].slot = s
			s.owners[j].refs.Store(1)
			s.planes[j] = av.Plane{Buffer: av.Buffer{Bytes: spans[j], Owner: &s.owners[j]}, Stride: c.Width}
			if j != 0 {
				s.planes[j].Stride /= 2
			}
		}
		s.frameTemplate, s.videoTemplate, s.planeTemplates = s.frame, s.video, s.planes
		frameSlots.Store(&s.frame, s)
	}
	return p
}

func (p *framePool) take(pts int64) *av.Frame {
	if f := p.takeAvailable(pts); f != nil {
		return f
	}
	if p.evict != nil && p.evict() {
		if f := p.takeAvailable(pts); f != nil {
			return f
		}
	}
	p.dropped.Add(1)
	return nil
}

func (p *framePool) takeAvailable(pts int64) *av.Frame {
	for range p.slots {
		s := &p.slots[p.next]
		p.next = (p.next + 1) % len(p.slots)
		if !s.busy.CompareAndSwap(false, true) {
			continue
		}
		if s.owners[0].refs.Load() != 1 || s.owners[1].refs.Load() != 1 || s.owners[2].refs.Load() != 1 {
			s.busy.Store(false)
			continue
		}
		for i := range s.owners {
			s.owners[i].Retain()
		}
		s.frame, s.video, s.planes = s.frameTemplate, s.videoTemplate, s.planeTemplates
		s.frame.PTS = av.Timestamp{Value: pts, Base: nanoseconds}
		return &s.frame
	}
	return nil
}

func releaseFrame(f *av.Frame) {
	v, ok := frameSlots.Load(f)
	if !ok {
		return
	}
	slot := v.(*frameSlot)
	for i := range slot.owners {
		slot.owners[i].Release()
	}
	slot.busy.Store(false)
}

func (p *framePool) close() {
	for i := range p.slots {
		frameSlots.Delete(&p.slots[i].frame)
		for j := range p.slots[i].owners {
			p.slots[i].owners[j].Release()
		}
	}
}

// copyNV12 copies an NV12 pixel buffer (srcW x srcH) into a planar YUV420P frame
// of target size (c.Width x c.Height), scaling nearest-neighbor if dimensions differ.
func copyNV12(c Config, pts int64, srcW, srcH int, y, uv []byte, yStride, uvStride int) *av.Frame {
	if srcW < 2 || srcH < 2 || yStride < srcW || uvStride < srcW {
		return nil
	}
	if len(y) < (srcH-1)*yStride+srcW || len(uv) < (srcH/2-1)*uvStride+srcW {
		return nil
	}
	f := c.buffers.take(pts)
	if f == nil {
		return nil
	}
	dstW, dstH := c.Width, c.Height
	yDst := f.Planes[0].Buffer.Bytes
	uDst := f.Planes[1].Buffer.Bytes
	vDst := f.Planes[2].Buffer.Bytes

	if srcW == dstW && srcH == dstH {
		if yStride == dstW {
			copy(yDst[:dstW*dstH], y[:dstW*dstH])
		} else {
			for row := range dstH {
				copy(yDst[row*dstW:(row+1)*dstW], y[row*yStride:row*yStride+dstW])
			}
		}
		cw, ch := dstW/2, dstH/2
		for row := range ch {
			src := uv[row*uvStride : row*uvStride+cw*2]
			uRow := uDst[row*cw : (row+1)*cw]
			vRow := vDst[row*cw : (row+1)*cw]
			for col := range cw {
				uRow[col] = src[col*2]
				vRow[col] = src[col*2+1]
			}
		}
		return f
	}

	for row := range dstH {
		sy := (row * srcH) / dstH
		sRow := y[sy*yStride:]
		dRow := yDst[row*dstW:]
		for col := range dstW {
			sx := (col * srcW) / dstW
			dRow[col] = sRow[sx]
		}
	}
	dcw, dch := dstW/2, dstH/2
	scw, sch := srcW/2, srcH/2
	for row := range dch {
		sy := (row * sch) / dch
		sRow := uv[sy*uvStride:]
		uRow := uDst[row*dcw:]
		vRow := vDst[row*dcw:]
		for col := range dcw {
			sx := (col * scw) / dcw
			uRow[col] = sRow[sx*2]
			vRow[col] = sRow[sx*2+1]
		}
	}
	return f
}
