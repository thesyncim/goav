package goav

import (
	"context"
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

// compositeLayout places one input's top-left corner on the output canvas. One
// layout entry pairs with each input arm, so the compositor knows where to copy
// that arm's frame.
type compositeLayout struct {
	X, Y int
}

// videoCompositeStage overlays N synchronized video inputs into one output frame
// — the video dual of audioMixStage. Each input arm is identified by its frame
// StreamID and buffered until every arm has a frame, then one composite step
// advances. It is a normal pipeline.Stage: the buffered runner calls Handle
// serially per node, so the per-input queues need no locking (lock-free by
// design — the hot path takes no mutex).
//
// First-slice contract: inputs are I420 (YUV 4:2:0) and frame-aligned (one frame
// per arm advances one composite step). PTS/Duration come from the first input.
// A non-I420 frame fails the composite step.
type videoCompositeStage struct {
	name    string
	inputs  []av.StreamID
	out     av.StreamID
	layout  []compositeLayout
	pending map[av.StreamID][]*av.Frame
	eos     map[av.StreamID]struct{}
}

func newVideoCompositeStage(name string, inputs []av.StreamID, out av.StreamID, layout []compositeLayout) *videoCompositeStage {
	return &videoCompositeStage{
		name:    name,
		inputs:  append([]av.StreamID(nil), inputs...),
		out:     out,
		layout:  append([]compositeLayout(nil), layout...),
		pending: make(map[av.StreamID][]*av.Frame, len(inputs)),
		eos:     make(map[av.StreamID]struct{}, len(inputs)),
	}
}

func (s *videoCompositeStage) Name() string { return s.name }

func (s *videoCompositeStage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	switch msg.Kind {
	case pipeline.MessageFrame:
		if msg.Frame == nil || msg.Frame.Video == nil {
			return nil
		}
		id := msg.Frame.StreamID
		s.pending[id] = append(s.pending[id], cloneCompositeFrame(msg.Frame))
		return s.drain(ctx, emitter)
	case pipeline.MessageEvent:
		if msg.Event != nil && msg.Event.Type == av.EventEndOfStream {
			s.eos[msg.Event.StreamID] = struct{}{}
			if len(s.eos) >= len(s.inputs) {
				out := av.Event{Type: av.EventEndOfStream, StreamID: s.out}
				return emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageEvent, Event: &out})
			}
		}
		return nil
	default:
		return nil
	}
}

func (s *videoCompositeStage) Close() error { return nil }

// ready reports whether every input arm has at least one buffered frame.
func (s *videoCompositeStage) ready() bool {
	for i := range s.inputs {
		if len(s.pending[s.inputs[i]]) == 0 {
			return false
		}
	}
	return true
}

func (s *videoCompositeStage) drain(ctx context.Context, emitter pipeline.Emitter) error {
	for s.ready() {
		frames := make([]*av.Frame, len(s.inputs))
		for i := range s.inputs {
			id := s.inputs[i]
			frames[i] = s.pending[id][0]
			s.pending[id] = s.pending[id][1:]
		}
		composited, err := compositeI420Frames(frames, s.layout, s.out)
		if err != nil {
			return err
		}
		if err := emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: composited}); err != nil {
			return err
		}
	}
	return nil
}

// compositeI420Frames overlays each input's I420 frame onto a black canvas at its
// layout offset. The canvas is the bounding box of all placed inputs; planes are
// zero-filled (black: Y=0, and U/V=0 is greenish but fine for a first version).
func compositeI420Frames(frames []*av.Frame, layout []compositeLayout, out av.StreamID) (*av.Frame, error) {
	base := frames[0]
	// Bounding-box canvas over all inputs.
	canvasW, canvasH := 0, 0
	for i := range frames {
		f := frames[i]
		if f.Video == nil {
			return nil, fmt.Errorf("goav: video composite input has no video frame")
		}
		if f.Video.PixelFormat != av.PixelFormatI420 {
			return nil, fmt.Errorf("goav: video composite requires %s, got %q", av.PixelFormatI420, f.Video.PixelFormat)
		}
		if len(f.Planes) < 3 {
			return nil, fmt.Errorf("goav: video composite I420 input needs 3 planes, got %d", len(f.Planes))
		}
		canvasW = max(canvasW, layout[i].X+f.Video.Width)
		canvasH = max(canvasH, layout[i].Y+f.Video.Height)
	}
	if canvasW <= 0 || canvasH <= 0 {
		return nil, fmt.Errorf("goav: video composite canvas is empty (%dx%d)", canvasW, canvasH)
	}

	// I420: Y is full-res, U/V are half-res (rounded up to cover odd dimensions).
	chromaW := (canvasW + 1) / 2
	chromaH := (canvasH + 1) / 2
	yPlane := make([]byte, canvasW*canvasH)
	uPlane := make([]byte, chromaW*chromaH)
	vPlane := make([]byte, chromaW*chromaH)

	for i := range frames {
		f := frames[i]
		// Even-only offsets keep luma and chroma aligned; round odd ones down.
		dstX := layout[i].X &^ 1
		dstY := layout[i].Y &^ 1
		copyPlane(yPlane, canvasW, canvasH, &f.Planes[0], f.Video.Width, f.Video.Height, dstX, dstY)
		srcChromaW := (f.Video.Width + 1) / 2
		srcChromaH := (f.Video.Height + 1) / 2
		copyPlane(uPlane, chromaW, chromaH, &f.Planes[1], srcChromaW, srcChromaH, dstX/2, dstY/2)
		copyPlane(vPlane, chromaW, chromaH, &f.Planes[2], srcChromaW, srcChromaH, dstX/2, dstY/2)
	}

	return &av.Frame{
		StreamID: out,
		Type:     av.MediaVideo,
		PTS:      base.PTS,
		Duration: base.Duration,
		Video:    &av.VideoFrame{Width: canvasW, Height: canvasH, PixelFormat: av.PixelFormatI420},
		Planes: []av.Plane{
			{Buffer: av.Buffer{Bytes: yPlane, Ownership: av.BufferOwned}, Stride: canvasW},
			{Buffer: av.Buffer{Bytes: uPlane, Ownership: av.BufferOwned}, Stride: chromaW},
			{Buffer: av.Buffer{Bytes: vPlane, Ownership: av.BufferOwned}, Stride: chromaW},
		},
	}, nil
}

// copyPlane copies a tightly-packed source plane region into a tightly-packed
// destination plane at (dstX, dstY), clipping to the destination bounds and
// respecting the source plane's Stride and Offset.
func copyPlane(dst []byte, dstW, dstH int, src *av.Plane, srcW, srcH, dstX, dstY int) {
	srcStride := src.Stride
	if srcStride <= 0 {
		srcStride = srcW
	}
	srcBytes := src.Buffer.Bytes
	for row := 0; row < srcH; row++ {
		dy := dstY + row
		if dy < 0 || dy >= dstH {
			continue
		}
		// Per-row horizontal clip against the destination canvas.
		copyW := srcW
		srcX := 0
		dx := dstX
		if dx < 0 {
			srcX = -dx
			copyW += dx
			dx = 0
		}
		if dx+copyW > dstW {
			copyW = dstW - dx
		}
		if copyW <= 0 {
			continue
		}
		srcStart := src.Offset + row*srcStride + srcX
		if srcStart < 0 || srcStart+copyW > len(srcBytes) {
			// Guard against malformed strides/offsets rather than panic.
			continue
		}
		dstStart := dy*dstW + dx
		copy(dst[dstStart:dstStart+copyW], srcBytes[srcStart:srcStart+copyW])
	}
}

// cloneCompositeFrame deep-copies a frame so buffering survives producer reuse of
// the underlying buffers (mirrors cloneMixFrame for video frames).
func cloneCompositeFrame(frame *av.Frame) *av.Frame {
	clone := *frame
	if frame.Video != nil {
		video := *frame.Video
		clone.Video = &video
	}
	clone.Planes = make([]av.Plane, len(frame.Planes))
	for i := range frame.Planes {
		clone.Planes[i] = frame.Planes[i]
		src := frame.Planes[i].Buffer.Bytes
		bytes := make([]byte, len(src))
		copy(bytes, src)
		clone.Planes[i].Buffer = av.Buffer{Bytes: bytes, Ownership: av.BufferOwned}
	}
	return &clone
}
