package cameraav

import (
	"sync/atomic"
	"unsafe"

	"github.com/thesyncim/goav/av"
)

// Fixed storage makes the capture memory budget independent of GC, consumer
// retention, and frame rate. A slot is reusable only after the producer has
// returned it and all downstream references have gone. Exhaustion drops the
// incoming frame; it never grows the pool or overwrites borrowed pixel data.
type framePool struct {
	slots   []frameSlot
	next    int
	dropped atomic.Uint64
	evict   func() bool
}
type frameSlot struct {
	frame          av.Frame // Must remain first: releaseFrame recovers the owning slot.
	video          av.VideoFrame
	planes         [3]av.Plane
	owners         [3]cameraPlaneOwner
	busy           atomic.Bool
	frameTemplate  av.Frame
	videoTemplate  av.VideoFrame
	planeTemplates [3]av.Plane
}

// Enforce the containing-slot conversion used by releaseFrame at compile time.
var _ [0]byte = [unsafe.Offsetof(frameSlot{}.frame)]byte{}

type cameraPlaneOwner struct {
	refs atomic.Int32
}

func (o *cameraPlaneOwner) Retain() {
	if o.refs.Add(1) <= 1 {
		panic("cameraav: retain after release")
	}
}
func (o *cameraPlaneOwner) Release() {
	if o.refs.Add(-1) < 0 {
		panic("cameraav: double release")
	}
}
func (o *cameraPlaneOwner) Refs() int { return int(o.refs.Load()) }
func newFramePool(c Config) *framePool {
	p := &framePool{slots: make([]frameSlot, c.Queue+2)}
	for i := range p.slots {
		s := &p.slots[i]
		s.video = av.VideoFrame{Width: c.Width, Height: c.Height, PixelFormat: av.PixelFormatYUV420P}
		s.frame = av.Frame{StreamID: "camera", Type: av.MediaVideo, Video: &s.video, Planes: s.planes[:], Duration: av.Duration{Value: 1, Base: av.TimeBase{Num: 1, Den: int64(c.FPS)}}}
		ysize := c.Width * c.Height
		data := make([]byte, ysize*3/2)
		spans := [][]byte{data[:ysize:ysize], data[ysize : ysize+ysize/4 : ysize+ysize/4], data[ysize+ysize/4:]}
		for j := range s.owners {
			s.owners[j].refs.Store(1)
			s.planes[j] = av.Plane{Buffer: av.Buffer{Bytes: spans[j], Owner: &s.owners[j]}, Stride: c.Width}
			if j != 0 {
				s.planes[j].Stride /= 2
			}
		}
		s.frameTemplate, s.videoTemplate, s.planeTemplates = s.frame, s.video, s.planes
	}
	return p
}
func (p *framePool) take(pts int64) *av.Frame {
	if f := p.takeAvailable(pts); f != nil {
		return f
	}
	// Prefer a fresh sample to an old queued frame. Retained downstream frames
	// cannot be reclaimed; if there is nothing queued, drop the incoming sample.
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

// f is always the original borrowed frame produced by this pool, never a Ref.
// The frame is the first struct member, so its address is the slot address.
// This stays a Go pointer throughout; no uintptr or hidden GC reference is used.
func releaseFrame(f *av.Frame) {
	slot := (*frameSlot)(unsafe.Pointer(f))
	for i := range slot.owners {
		slot.owners[i].Release()
	}
	slot.busy.Store(false)
}
func (p *framePool) close() {
	for i := range p.slots {
		for j := range p.slots[i].owners {
			p.slots[i].owners[j].Release()
		}
	}
}
