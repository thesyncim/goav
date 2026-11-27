package stage

import (
	"context"
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/pipeline"
)

// videoCompositeStage overlays N synchronized video inputs into one output frame
// — the video dual of audioMixStage. Each input arm is identified by its frame
// StreamID. It is a normal pipeline.Stage: the buffered runner calls Handle
// serially per node, so the per-input queues need no locking (lock-free by
// design — the hot path takes no mutex).
type VideoComposite struct {
	joinStageBase
	layout []compile.CompositeLayout
	// sizes remembers each arm's most recent frame dimensions (indexed by armIdx),
	// so absent arms (gap or ended) still reserve their canvas region.
	sizes []compositeArmSize
	video av.VideoFrame
}

type compositeArmSize struct {
	w, h  int
	known bool
}

func NewVideoComposite(name string, inputs []av.StreamID, out av.StreamID, layout []compile.CompositeLayout, mode compile.JoinSyncMode) *VideoComposite {
	return NewVideoCompositeWithPrealloc(name, inputs, out, layout, mode, 1)
}

func NewVideoCompositeWithPrealloc(name string, inputs []av.StreamID, out av.StreamID, layout []compile.CompositeLayout, mode compile.JoinSyncMode, preallocDepth int) *VideoComposite {
	return NewVideoCompositeWithPreallocAndPools(name, inputs, out, layout, mode, preallocDepth, nil)
}

func NewVideoCompositeWithPreallocAndPools(name string, inputs []av.StreamID, out av.StreamID, layout []compile.CompositeLayout, mode compile.JoinSyncMode, preallocDepth int, pools *MediaPools) *VideoComposite {
	if preallocDepth < 1 {
		preallocDepth = 1
	}
	stage := &VideoComposite{
		joinStageBase: joinStageBase{
			name:  name,
			out:   out,
			sync:  NewJoinSyncStateWithPendingCap(mode, inputs, preallocDepth),
			free:  make([][]*av.Frame, len(inputs)),
			pools: pools,
		},
		layout: append([]compile.CompositeLayout(nil), layout...),
		sizes:  make([]compositeArmSize, len(inputs)),
	}
	stage.output.Planes = takeMediaPlanes(pools, 3)
	stage.sync.recycle = stage.recycleArmFrame
	for i := range inputs {
		free := make([]*av.Frame, 0, preallocDepth)
		for range preallocDepth {
			free = append(free, &av.Frame{
				Video:  &av.VideoFrame{},
				Planes: takeMediaPlanes(pools, 3),
			})
		}
		stage.free[i] = free
	}
	return stage
}

func (s *VideoComposite) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	if s.closed {
		return pipeline.ErrClosed
	}
	var err error
	switch msg.Kind {
	case pipeline.MessageFrame:
		if msg.Frame != nil && msg.Frame.Video != nil {
			if armIdx := s.sync.armIndex(msg.Frame.StreamID); armIdx >= 0 {
				s.sizes[armIdx] = compositeArmSize{w: msg.Frame.Video.Width, h: msg.Frame.Video.Height, known: true}
				if !s.sync.eosEmitted && !s.sync.armStates[armIdx].eos {
					s.sync.bufferAt(armIdx, s.cloneCompositeFrame(armIdx, msg.Frame))
				}
			}
			err = s.drain(ctx, emitter)
		}
	case pipeline.MessageEvent:
		err = s.handleEvent(ctx, msg, emitter, s.drain)
	}
	s.sync.publishDepths()
	return err
}

func (s *VideoComposite) drain(ctx context.Context, emitter pipeline.Emitter) error {
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
			if size := s.sizes[i]; size.known {
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
		err = emitter.Emit(ctx, &s.message)
		// The canvas is the stage's own; downstream keeps it by reference.
		composited.Release()
		if err != nil {
			return err
		}
	}
}

func compositeI420FramesInto(frames []*av.Frame, layout []compile.CompositeLayout, floorW, floorH int, out av.StreamID, dst *av.Frame, video *av.VideoFrame) (*av.Frame, error) {
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
		if f.Video.PixelFormat != av.PixelFormatI420 && f.Video.PixelFormat != av.PixelFormatYUV420P {
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
	// Each output canvas is new pooled memory: the previous one may still be
	// queued or kept downstream.
	y, u, v := i420Canvas(canvasW*canvasH, chromaW*chromaH)
	yPlane, uPlane, vPlane := y.Bytes, u.Bytes, v.Bytes
	// Paint the uncovered canvas video black (Y=16, U=V=128 in limited range).
	// Zeroed chroma would render every gap between arms green. Skip the full
	// canvas fill when a participating arm already covers the entire canvas
	// (for example, a full-frame PiP background).
	if !compositeCoversCanvas(frames, layout, canvasW, canvasH) {
		fillPlane(yPlane, compositeBlackLuma)
		fillPlane(uPlane, compositeNeutralChroma)
		fillPlane(vPlane, compositeNeutralChroma)
	}

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
	dst.Planes[0] = av.Plane{Buffer: y, Stride: canvasW}
	dst.Planes[1] = av.Plane{Buffer: u, Stride: chromaW}
	dst.Planes[2] = av.Plane{Buffer: v, Stride: chromaW}
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

// compositeCoversCanvas reports whether at least one participating arm fully
// covers (0,0)..(canvasW,canvasH), making the background black fill redundant.
func compositeCoversCanvas(frames []*av.Frame, layout []compile.CompositeLayout, canvasW, canvasH int) bool {
	for i, f := range frames {
		if f == nil || f.Video == nil {
			continue
		}
		dstX := layout[i].X &^ 1
		dstY := layout[i].Y &^ 1
		if dstX <= 0 && dstY <= 0 && dstX+f.Video.Width >= canvasW && dstY+f.Video.Height >= canvasH {
			return true
		}
	}
	return false
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
	if dstX >= 0 && dstY >= 0 && dstX+srcW <= dstW && dstY+srcH <= dstH && copyPlaneInside(dst, dstW, srcBytes, srcStride, src.Offset, srcW, srcH, dstX, dstY) {
		return
	}
	for row := range srcH {
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

func copyPlaneInside(dst []byte, dstW int, src []byte, srcStride int, srcOffset int, srcW int, srcH int, dstX int, dstY int) bool {
	if srcW <= 0 || srcH <= 0 || dstW <= 0 || srcStride < srcW || srcOffset < 0 {
		return false
	}
	srcEnd := srcOffset + (srcH-1)*srcStride + srcW
	if srcEnd > len(src) {
		return false
	}
	dstEnd := (dstY+srcH-1)*dstW + dstX + srcW
	if dstEnd > len(dst) {
		return false
	}
	if srcStride == srcW && dstX == 0 && dstW == srcW {
		copy(dst[dstY*dstW:dstY*dstW+srcW*srcH], src[srcOffset:srcOffset+srcW*srcH])
		return true
	}
	for row := range srcH {
		srcStart := srcOffset + row*srcStride
		dstStart := (dstY+row)*dstW + dstX
		copy(dst[dstStart:dstStart+srcW], src[srcStart:srcStart+srcW])
	}
	return true
}

// cloneCompositeFrame keeps an arm frame past the call that lent it: a
// pooled header with the frame's fields and its own reference to each plane.
func (s *VideoComposite) cloneCompositeFrame(armIdx int, frame *av.Frame) *av.Frame {
	clone := s.takeCompositeFrame(armIdx)
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
	clone.Planes = append(planes[:0], frame.Planes...)
	for i := range clone.Planes {
		clone.Planes[i].Buffer.Retain()
	}
	return clone
}

func (s *VideoComposite) takeCompositeFrame(armIdx int) *av.Frame {
	if armIdx < 0 || armIdx >= len(s.free) {
		return &av.Frame{
			Video:  &av.VideoFrame{},
			Planes: takeMediaPlanes(s.pools, 3),
		}
	}
	free := s.free[armIdx]
	if len(free) == 0 {
		return &av.Frame{
			Video:  &av.VideoFrame{},
			Planes: takeMediaPlanes(s.pools, 3),
		}
	}
	index := len(free) - 1
	frame := free[index]
	free[index] = nil
	s.free[armIdx] = free[:index]
	return frame
}

// Limited-range I420 black: the canvas color wherever no arm paints.
const (
	CompositeBlackLuma     = 16
	CompositeNeutralChroma = 128
	compositeBlackLuma     = CompositeBlackLuma
	compositeNeutralChroma = CompositeNeutralChroma
)

// fillPlane sets every byte of plane to value. Doubling copies keep it a few
// memmoves per plane rather than a per-byte loop.
func fillPlane(plane []byte, value byte) {
	if len(plane) == 0 {
		return
	}
	plane[0] = value
	for filled := 1; filled < len(plane); filled *= 2 {
		copy(plane[filled:], plane[:filled])
	}
}

// i420Canvas carves the Y, U, and V planes of one canvas out of a single
// pooled block: one pool round trip per output instead of three. Each plane
// holds its own reference, as av.Frame.Release expects.
func i420Canvas(lumaSize, chromaSize int) (y, u, v av.Buffer) {
	block := av.NewBuffer(lumaSize + 2*chromaSize)
	block.Retain()
	block.Retain()
	bytes := block.Bytes
	y = av.Buffer{Bytes: bytes[:lumaSize:lumaSize], Owner: block.Owner}
	u = av.Buffer{Bytes: bytes[lumaSize : lumaSize+chromaSize : lumaSize+chromaSize], Owner: block.Owner}
	v = av.Buffer{Bytes: bytes[lumaSize+chromaSize:], Owner: block.Owner}
	return y, u, v
}
