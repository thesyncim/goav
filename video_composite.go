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
// StreamID. It is a normal pipeline.Stage: the buffered runner calls Handle
// serially per node, so the per-input queues need no locking (lock-free by
// design — the hot path takes no mutex).
//
// Contract: inputs are I420 (YUV 4:2:0); a non-I420 frame fails the composite
// step. Arm alignment follows the joinSyncState mode: arrival (default — one
// frame per arm per step) or PTS (SyncByPTS — arms join the step their head
// timestamp matches; an arm whose head is newer is simply not painted that
// step, and the canvas keeps covering its last-known extent so the output
// geometry stays stable across gap steps). An arm that ends keeps the others
// compositing without it. PTS/Duration come from the step's timing reference
// (arrival: the first input, exactly as before).
type videoCompositeStage struct {
	name   string
	out    av.StreamID
	layout []compositeLayout
	// sizes remembers each arm's most recent frame dimensions, so absent arms
	// (gap or ended) still reserve their canvas region.
	sizes map[av.StreamID]compositeArmSize
	sync  joinSyncState
}

type compositeArmSize struct {
	w, h int
}

func newVideoCompositeStage(name string, inputs []av.StreamID, out av.StreamID, layout []compositeLayout, mode joinSyncMode) *videoCompositeStage {
	return &videoCompositeStage{
		name:   name,
		out:    out,
		layout: append([]compositeLayout(nil), layout...),
		sizes:  make(map[av.StreamID]compositeArmSize, len(inputs)),
		sync:   newJoinSyncState(mode, inputs),
	}
}

func (s *videoCompositeStage) Name() string { return s.name }

// DescribeNode reports the same detail the join planner records on the planned
// node, keeping Describe() ≡ Build() when SyncByPTS annotates the join.
func (s *videoCompositeStage) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: s.name, Kind: pipeline.NodeStage, Detail: joinSyncNodeDetail(s.sync.mode)}
}

func (s *videoCompositeStage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	switch msg.Kind {
	case pipeline.MessageFrame:
		if msg.Frame == nil || msg.Frame.Video == nil {
			return nil
		}
		s.sizes[msg.Frame.StreamID] = compositeArmSize{w: msg.Frame.Video.Width, h: msg.Frame.Video.Height}
		s.sync.buffer(cloneCompositeFrame(msg.Frame))
		return s.drain(ctx, emitter)
	case pipeline.MessageEvent:
		if msg.Event == nil {
			return nil
		}
		switch msg.Event.Type {
		case av.EventEndOfStream:
			ended := s.sync.markEOS(msg.Event.StreamID)
			// The ended arm stops gating: drain what the remaining arms can
			// paint before (possibly) ending the joined stream.
			if err := s.drain(ctx, emitter); err != nil {
				return err
			}
			if ended {
				out := av.Event{Type: av.EventEndOfStream, StreamID: s.out}
				return emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageEvent, Event: &out})
			}
			return nil
		case av.EventDiscontinuity:
			if !s.sync.discontinuity(msg.Event.StreamID) {
				return nil
			}
			// The joined timeline jumps with the repositioned arm: forward one
			// discontinuity re-stamped to the join output, the same signal a
			// seeked single-source chain delivers to its sink.
			out := *msg.Event
			out.StreamID = s.out
			return emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageEvent, Event: &out})
		}
		if msg.Event.Reason == selectorActiveReason {
			// Control-plane events ride the data path through joins UNCHANGED:
			// a SelectActive heading for a selector downstream of this join
			// carries its target arm in Event.StreamID and is consumed by the
			// selector — re-stamping it here would erase the target.
			return emitter.Emit(ctx, msg)
		}
		return nil
	default:
		return nil
	}
}

func (s *videoCompositeStage) Close() error { return nil }

func (s *videoCompositeStage) drain(ctx context.Context, emitter pipeline.Emitter) error {
	for {
		frames, ref, ok := s.sync.step()
		if !ok {
			return nil
		}
		present := make([]*av.Frame, 0, len(frames))
		layouts := make([]compositeLayout, 0, len(frames))
		floorW, floorH := 0, 0
		for i := range frames {
			if frames[i] != nil {
				present = append(present, frames[i])
				layouts = append(layouts, s.layout[i])
				continue
			}
			// Absent arm (gap or ended): keep the canvas covering its
			// last-known extent so geometry stays stable across gap steps.
			if size, known := s.sizes[s.sync.inputs[i]]; known {
				floorW = max(floorW, s.layout[i].X+size.w)
				floorH = max(floorH, s.layout[i].Y+size.h)
			}
		}
		composited, err := compositeI420Frames(present, layouts, floorW, floorH, s.out)
		if err != nil {
			return err
		}
		// The step's timing reference stamps the output (in arrival mode this
		// is the first arm, exactly as before).
		composited.PTS = frames[ref].PTS
		composited.Duration = frames[ref].Duration
		if err := emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: composited}); err != nil {
			return err
		}
	}
}

// compositeI420Frames overlays each input's I420 frame onto a black canvas at its
// layout offset. The canvas is the bounding box of all placed inputs, never
// smaller than the (floorW, floorH) reserved for arms absent this step; planes
// are zero-filled (black: Y=0, and U/V=0 is greenish but fine for a first
// version).
func compositeI420Frames(frames []*av.Frame, layout []compositeLayout, floorW, floorH int, out av.StreamID) (*av.Frame, error) {
	base := frames[0]
	// Bounding-box canvas over all inputs.
	canvasW, canvasH := floorW, floorH
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
		// The canvas planes are freshly allocated per step and never written
		// again — published immutable, so a buffered graph (a Select over
		// sub-composites) may queue them by reference.
		Planes: []av.Plane{
			{Buffer: av.Buffer{Bytes: yPlane, Ownership: av.BufferImmutable}, Stride: canvasW},
			{Buffer: av.Buffer{Bytes: uPlane, Ownership: av.BufferImmutable}, Stride: chromaW},
			{Buffer: av.Buffer{Bytes: vPlane, Ownership: av.BufferImmutable}, Stride: chromaW},
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
