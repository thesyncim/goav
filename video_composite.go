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
	sizes   map[av.StreamID]compositeArmSize
	sync    *joinSyncState
	free    map[av.StreamID][]*av.Frame
	output  av.Frame
	video   av.VideoFrame
	message pipeline.Message
}

type compositeArmSize struct {
	w, h int
}

func newVideoCompositeStage(name string, inputs []av.StreamID, out av.StreamID, layout []compositeLayout, mode joinSyncMode) *videoCompositeStage {
	return newVideoCompositeStageWithPrealloc(name, inputs, out, layout, mode, 1)
}

func newVideoCompositeStageWithPrealloc(name string, inputs []av.StreamID, out av.StreamID, layout []compositeLayout, mode joinSyncMode, preallocDepth int) *videoCompositeStage {
	if preallocDepth < 1 {
		preallocDepth = 1
	}
	stage := &videoCompositeStage{
		name:   name,
		out:    out,
		layout: append([]compositeLayout(nil), layout...),
		sizes:  make(map[av.StreamID]compositeArmSize, len(inputs)),
		sync:   newJoinSyncStateWithPendingCap(mode, inputs, preallocDepth),
		free:   make(map[av.StreamID][]*av.Frame, len(inputs)),
	}
	stage.output.Planes = make([]av.Plane, 3)
	stage.sync.recycle = stage.recycleCompositeFrame
	for i := range inputs {
		free := make([]*av.Frame, 0, preallocDepth)
		for j := 0; j < preallocDepth; j++ {
			free = append(free, &av.Frame{
				Video:  &av.VideoFrame{},
				Planes: make([]av.Plane, 3),
			})
		}
		stage.free[inputs[i]] = free
	}
	return stage
}

func (s *videoCompositeStage) Name() string { return s.name }

// DescribeNode reports the same detail the join planner records on the planned
// node, keeping Describe() ≡ Build() when SyncByPTS annotates the join.
func (s *videoCompositeStage) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: s.name, Kind: pipeline.NodeStage, Detail: joinSyncNodeDetail(s.sync.mode)}
}

// DroppedMessages implements pipeline.DropReporter: frames the join discarded
// to stay aligned (stale catch-up heads, discontinuity flushes), surfaced as
// the join node's Dropped count (reason "sync") in Stats and snapshots.
func (s *videoCompositeStage) DroppedMessages() uint64 { return s.sync.droppedFrames() }

func (s *videoCompositeStage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	switch msg.Kind {
	case pipeline.MessageFrame:
		if msg.Frame == nil || msg.Frame.Video == nil {
			return nil
		}
		s.sizes[msg.Frame.StreamID] = compositeArmSize{w: msg.Frame.Video.Width, h: msg.Frame.Video.Height}
		s.sync.buffer(s.cloneCompositeFrame(msg.Frame))
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
		floorW, floorH := 0, 0
		for i := range frames {
			if frames[i] != nil {
				continue
			}
			// Absent arm (gap or ended): keep the canvas covering its
			// last-known extent so geometry stays stable across gap steps.
			if size, known := s.sizes[s.sync.inputs[i]]; known {
				floorW = max(floorW, s.layout[i].X+size.w)
				floorH = max(floorH, s.layout[i].Y+size.h)
			}
		}
		composited, err := compositeI420FramesInto(frames, s.layout, floorW, floorH, s.out, &s.output, &s.video)
		if err != nil {
			s.recycleStepFrames(frames)
			return err
		}
		// The step's timing reference stamps the output (in arrival mode this
		// is the first arm, exactly as before).
		composited.PTS = frames[ref].PTS
		composited.Duration = frames[ref].Duration
		s.recycleStepFrames(frames)
		s.message.Kind = pipeline.MessageFrame
		s.message.Packet = nil
		s.message.Frame = composited
		s.message.Event = nil
		if err := emitter.Emit(ctx, &s.message); err != nil {
			return err
		}
	}
}

func compositeI420FramesInto(frames []*av.Frame, layout []compositeLayout, floorW, floorH int, out av.StreamID, dst *av.Frame, video *av.VideoFrame) (*av.Frame, error) {
	base := firstCompositeFrame(frames)
	if base == nil {
		return nil, fmt.Errorf("goav: video composite has no participating frames")
	}
	// Bounding-box canvas over all inputs.
	canvasW, canvasH := floorW, floorH
	for i := range frames {
		f := frames[i]
		if f == nil {
			continue
		}
		if f.Video == nil {
			return nil, fmt.Errorf("goav: composite arm %q delivered a frame with no video plane", f.StreamID)
		}
		if f.Video.PixelFormat != av.PixelFormatI420 {
			return nil, fmt.Errorf("goav: video composite requires %s, got %q on arm %q", av.PixelFormatI420, f.Video.PixelFormat, f.StreamID)
		}
		if len(f.Planes) < 3 {
			return nil, fmt.Errorf("goav: video composite I420 arm %q needs 3 planes, got %d", f.StreamID, len(f.Planes))
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
	if cap(dst.Planes) < 3 {
		dst.Planes = make([]av.Plane, 3)
	}
	dst.Planes = dst.Planes[:3]
	yPlane := compositePlaneBytes(dst.Planes[0].Buffer.Bytes, canvasW*canvasH)
	uPlane := compositePlaneBytes(dst.Planes[1].Buffer.Bytes, chromaW*chromaH)
	vPlane := compositePlaneBytes(dst.Planes[2].Buffer.Bytes, chromaW*chromaH)
	clear(yPlane)
	clear(uPlane)
	clear(vPlane)

	for i := range frames {
		f := frames[i]
		if f == nil {
			continue
		}
		// Even-only offsets keep luma and chroma aligned; round odd ones down.
		dstX := layout[i].X &^ 1
		dstY := layout[i].Y &^ 1
		copyPlane(yPlane, canvasW, canvasH, &f.Planes[0], f.Video.Width, f.Video.Height, dstX, dstY)
		srcChromaW := (f.Video.Width + 1) / 2
		srcChromaH := (f.Video.Height + 1) / 2
		copyPlane(uPlane, chromaW, chromaH, &f.Planes[1], srcChromaW, srcChromaH, dstX/2, dstY/2)
		copyPlane(vPlane, chromaW, chromaH, &f.Planes[2], srcChromaW, srcChromaH, dstX/2, dstY/2)
	}

	*video = av.VideoFrame{Width: canvasW, Height: canvasH, PixelFormat: av.PixelFormatI420}
	dst.StreamID = out
	dst.Type = av.MediaVideo
	dst.PTS = base.PTS
	dst.Duration = base.Duration
	dst.Audio = nil
	dst.Video = video
	dst.Metadata = nil
	dst.Planes[0] = av.Plane{Buffer: av.Buffer{Bytes: yPlane, Ownership: av.BufferBorrowed}, Stride: canvasW}
	dst.Planes[1] = av.Plane{Buffer: av.Buffer{Bytes: uPlane, Ownership: av.BufferBorrowed}, Stride: chromaW}
	dst.Planes[2] = av.Plane{Buffer: av.Buffer{Bytes: vPlane, Ownership: av.BufferBorrowed}, Stride: chromaW}
	return dst, nil
}

func firstCompositeFrame(frames []*av.Frame) *av.Frame {
	for i := range frames {
		if frames[i] != nil {
			return frames[i]
		}
	}
	return nil
}

func compositePlaneBytes(bytes []byte, n int) []byte {
	if cap(bytes) < n {
		return make([]byte, n)
	}
	return bytes[:n]
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

// cloneCompositeFrame deep-copies a frame into a reusable per-arm slot so
// buffering survives producer reuse of the underlying buffers.
func (s *videoCompositeStage) cloneCompositeFrame(frame *av.Frame) *av.Frame {
	clone := s.takeCompositeFrame(frame.StreamID)
	planes := clone.Planes
	video := clone.Video
	*clone = *frame
	if frame.Video != nil {
		if video == nil {
			video = &av.VideoFrame{}
		}
		*video = *frame.Video
	}
	clone.Video = video
	clone.Audio = nil
	clone.Planes = planes
	if cap(clone.Planes) < len(frame.Planes) {
		clone.Planes = make([]av.Plane, len(frame.Planes))
	}
	clone.Planes = clone.Planes[:len(frame.Planes)]
	for i := range frame.Planes {
		bytes := clone.Planes[i].Buffer.Bytes
		clone.Planes[i] = frame.Planes[i]
		src := frame.Planes[i].Buffer.Bytes
		if frame.Planes[i].Buffer.Ownership == av.BufferImmutable {
			continue
		}
		if cap(bytes) < len(src) {
			bytes = make([]byte, len(src))
		} else {
			bytes = bytes[:len(src)]
		}
		copy(bytes, src)
		clone.Planes[i].Buffer = av.Buffer{Bytes: bytes, Ownership: av.BufferOwned}
	}
	return clone
}

func (s *videoCompositeStage) takeCompositeFrame(id av.StreamID) *av.Frame {
	free := s.free[id]
	if len(free) == 0 {
		return &av.Frame{}
	}
	index := len(free) - 1
	frame := free[index]
	free[index] = nil
	s.free[id] = free[:index]
	return frame
}

func (s *videoCompositeStage) recycleStepFrames(frames []*av.Frame) {
	for i := range frames {
		s.recycleCompositeFrame(frames[i])
		frames[i] = nil
	}
}

func (s *videoCompositeStage) recycleCompositeFrame(frame *av.Frame) {
	if frame == nil {
		return
	}
	id := frame.StreamID
	s.free[id] = append(s.free[id], frame)
}
