package goav

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

// compositeTestI420Frame builds a tightly-packed I420 frame whose Y/U/V planes are
// each filled with a constant value, so a copied pixel is easy to identify.
func compositeTestI420Frame(id av.StreamID, w, h int, y, u, v byte) *av.Frame {
	chromaW := (w + 1) / 2
	chromaH := (h + 1) / 2
	yPlane := make([]byte, w*h)
	uPlane := make([]byte, chromaW*chromaH)
	vPlane := make([]byte, chromaW*chromaH)
	for i := range yPlane {
		yPlane[i] = y
	}
	for i := range uPlane {
		uPlane[i] = u
	}
	for i := range vPlane {
		vPlane[i] = v
	}
	return &av.Frame{
		StreamID: id,
		Type:     av.MediaVideo,
		PTS:      av.Timestamp{Value: 7},
		Duration: av.Duration{Value: 3},
		Video:    &av.VideoFrame{Width: w, Height: h, PixelFormat: av.PixelFormatI420},
		Planes: []av.Plane{
			{Buffer: av.Buffer{Bytes: yPlane, Ownership: av.BufferImmutable}, Stride: w},
			{Buffer: av.Buffer{Bytes: uPlane, Ownership: av.BufferImmutable}, Stride: chromaW},
			{Buffer: av.Buffer{Bytes: vPlane, Ownership: av.BufferImmutable}, Stride: chromaW},
		},
	}
}

func compositeTestYAt(frame *av.Frame, x, y int) byte {
	return frame.Planes[0].Buffer.Bytes[y*frame.Video.Width+x]
}

func TestVideoCompositeOverlaysTwoI420Frames(t *testing.T) {
	stage := newVideoCompositeStage("composite", []av.StreamID{"a", "b"}, "out",
		[]compositeLayout{{X: 0, Y: 0}, {X: 4, Y: 0}}, joinSyncArrival)
	emit := &mixTestEmitter{}
	ctx := context.Background()
	if got := stage.DroppedMessages(); got != 0 {
		t.Fatalf("initial dropped messages = %d, want 0", got)
	}

	// One arm ready is not enough — composite advances only when every arm has a frame.
	if err := stage.Handle(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: compositeTestI420Frame("a", 4, 4, 100, 10, 20)}, emit); err != nil {
		t.Fatal(err)
	}
	if len(emit.frames) != 0 {
		t.Fatalf("emitted %d before both inputs ready, want 0", len(emit.frames))
	}
	if err := stage.Handle(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: compositeTestI420Frame("b", 4, 4, 200, 30, 40)}, emit); err != nil {
		t.Fatal(err)
	}
	if len(emit.frames) != 1 {
		t.Fatalf("frames=%d, want 1", len(emit.frames))
	}

	out := emit.frames[0]
	if out.StreamID != "out" {
		t.Fatalf("output stream=%s, want out", out.StreamID)
	}
	if out.Video == nil || out.Video.Width != 8 || out.Video.Height != 4 {
		t.Fatalf("canvas=%+v, want 8x4", out.Video)
	}
	if out.Video.PixelFormat != av.PixelFormatI420 {
		t.Fatalf("pixel format=%q, want %q", out.Video.PixelFormat, av.PixelFormatI420)
	}
	if out.PTS.Value != 7 || out.Duration.Value != 3 {
		t.Fatalf("pts/dur from first input not carried: pts=%+v dur=%+v", out.PTS, out.Duration)
	}
	if len(out.Planes) != 3 {
		t.Fatalf("planes=%d, want 3", len(out.Planes))
	}

	// Y plane: input "a" (value 100) covers the left 4x4, input "b" (200) the right 4x4.
	if got := compositeTestYAt(out, 0, 0); got != 100 {
		t.Fatalf("Y at (0,0)=%d, want 100 (input a top-left)", got)
	}
	if got := compositeTestYAt(out, 3, 3); got != 100 {
		t.Fatalf("Y at (3,3)=%d, want 100 (input a bottom-right)", got)
	}
	if got := compositeTestYAt(out, 4, 0); got != 200 {
		t.Fatalf("Y at (4,0)=%d, want 200 (input b top-left)", got)
	}
	if got := compositeTestYAt(out, 7, 3); got != 200 {
		t.Fatalf("Y at (7,3)=%d, want 200 (input b bottom-right)", got)
	}

	// Chroma planes are half-res: 4x2, U from "a"=10 left, "b"=30 right.
	uW := (8 + 1) / 2
	if out.Planes[1].Buffer.Bytes[0] != 10 {
		t.Fatalf("U at (0,0)=%d, want 10 (input a)", out.Planes[1].Buffer.Bytes[0])
	}
	if out.Planes[1].Buffer.Bytes[uW/2] != 30 {
		t.Fatalf("U at (2,0)=%d, want 30 (input b)", out.Planes[1].Buffer.Bytes[uW/2])
	}
	if out.Planes[2].Buffer.Bytes[0] != 20 {
		t.Fatalf("V at (0,0)=%d, want 20 (input a)", out.Planes[2].Buffer.Bytes[0])
	}
}

func TestVideoCompositeRejectsNonI420(t *testing.T) {
	stage := newVideoCompositeStage("composite", []av.StreamID{"a", "b"}, "out",
		[]compositeLayout{{X: 0, Y: 0}, {X: 4, Y: 0}}, joinSyncArrival)
	emit := &mixTestEmitter{}
	ctx := context.Background()

	rgb := compositeTestI420Frame("a", 4, 4, 100, 10, 20)
	rgb.Video.PixelFormat = "rgb24"

	if err := stage.Handle(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: compositeTestI420Frame("b", 4, 4, 200, 30, 40)}, emit); err != nil {
		t.Fatal(err)
	}
	err := stage.Handle(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: rgb}, emit)
	if err == nil {
		t.Fatal("composite of a non-I420 frame should error")
	}
}

func TestVideoCompositeEmitsEOSWhenAllInputsEnd(t *testing.T) {
	stage := newVideoCompositeStage("composite", []av.StreamID{"a", "b"}, "out",
		[]compositeLayout{{X: 0, Y: 0}, {X: 4, Y: 0}}, joinSyncArrival)
	emit := &mixTestEmitter{}
	ctx := context.Background()
	eos := func(id av.StreamID) *pipeline.Message {
		return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventEndOfStream, StreamID: id}}
	}

	if err := stage.Handle(ctx, eos("a"), emit); err != nil {
		t.Fatal(err)
	}
	if len(emit.events) != 0 {
		t.Fatal("emitted EOS before all inputs ended")
	}
	if err := stage.Handle(ctx, eos("b"), emit); err != nil {
		t.Fatal(err)
	}
	if len(emit.events) != 1 || emit.events[0].Type != av.EventEndOfStream || emit.events[0].StreamID != "out" {
		t.Fatalf("events=%+v, want one out EOS", emit.events)
	}
}

func TestVideoCompositeHandleEventAndNilContracts(t *testing.T) {
	stage := newVideoCompositeStageWithPrealloc("composite", []av.StreamID{"a", "b"}, "out",
		[]compositeLayout{{X: 0, Y: 0}, {X: 4, Y: 0}}, joinSyncPTS, 0)
	emit := &mixTestEmitter{}
	ctx := context.Background()

	for _, msg := range []*pipeline.Message{
		{Kind: pipeline.MessageFrame},
		{Kind: pipeline.MessageFrame, Frame: &av.Frame{}},
		{Kind: pipeline.MessageEvent},
		{Kind: pipeline.MessagePacket},
	} {
		if err := stage.Handle(ctx, msg, emit); err != nil {
			t.Fatalf("Handle(%+v) error = %v", msg, err)
		}
	}
	if got := len(stage.free["a"]); got != 1 {
		t.Fatalf("prealloc depth clamp left %d free frames, want 1", got)
	}

	discontinuity := av.Event{Type: av.EventDiscontinuity, StreamID: "a", Reason: "seek"}
	if err := stage.Handle(ctx, &pipeline.Message{Kind: pipeline.MessageEvent, Event: &discontinuity}, emit); err != nil {
		t.Fatalf("discontinuity error = %v", err)
	}
	if len(emit.events) != 1 || emit.events[0].Type != av.EventDiscontinuity || emit.events[0].StreamID != "out" || emit.events[0].Reason != "seek" {
		t.Fatalf("discontinuity events = %+v", emit.events)
	}

	selectActive := av.Event{Type: av.EventStats, StreamID: "camera_b", Reason: selectorActiveReason}
	if err := stage.Handle(ctx, &pipeline.Message{Kind: pipeline.MessageEvent, Event: &selectActive}, emit); err != nil {
		t.Fatalf("select event error = %v", err)
	}
	if len(emit.events) != 2 || emit.events[1].StreamID != "camera_b" || emit.events[1].Reason != selectorActiveReason {
		t.Fatalf("selector events = %+v", emit.events)
	}

	arrival := newVideoCompositeStage("arrival", []av.StreamID{"a", "b"}, "out",
		[]compositeLayout{{X: 0, Y: 0}, {X: 4, Y: 0}}, joinSyncArrival)
	if err := arrival.Handle(ctx, &pipeline.Message{Kind: pipeline.MessageEvent, Event: &discontinuity}, emit); err != nil {
		t.Fatalf("arrival discontinuity error = %v", err)
	}
	ordinary := av.Event{Type: av.EventStats, StreamID: "a", Reason: "periodic"}
	if err := arrival.Handle(ctx, &pipeline.Message{Kind: pipeline.MessageEvent, Event: &ordinary}, emit); err != nil {
		t.Fatalf("ordinary event error = %v", err)
	}
	if len(emit.events) != 2 {
		t.Fatalf("arrival/ordinary events changed output: %+v", emit.events)
	}
}

func TestVideoCompositeDirectValidationAndReuseContracts(t *testing.T) {
	dst := &av.Frame{}
	video := &av.VideoFrame{}
	if _, err := compositeI420FramesInto(nil, nil, 0, 0, "out", dst, video); err == nil || !strings.Contains(err.Error(), "no participating frames") {
		t.Fatalf("empty composite err = %v", err)
	}
	if _, err := compositeI420FramesInto([]*av.Frame{{StreamID: "a"}}, []compositeLayout{{}}, 0, 0, "out", dst, video); err == nil || !strings.Contains(err.Error(), "no video plane") {
		t.Fatalf("no video err = %v", err)
	}

	missingPlanes := compositeTestI420Frame("a", 2, 2, 1, 2, 3)
	missingPlanes.Planes = missingPlanes.Planes[:2]
	if _, err := compositeI420FramesInto([]*av.Frame{missingPlanes}, []compositeLayout{{}}, 0, 0, "out", dst, video); err == nil || !strings.Contains(err.Error(), "needs 3 planes") {
		t.Fatalf("missing planes err = %v", err)
	}

	emptyCanvas := compositeTestI420Frame("a", 0, 0, 1, 2, 3)
	if _, err := compositeI420FramesInto([]*av.Frame{emptyCanvas}, []compositeLayout{{}}, 0, 0, "out", dst, video); err == nil || !strings.Contains(err.Error(), "canvas is empty") {
		t.Fatalf("empty canvas err = %v", err)
	}

	odd := compositeTestI420Frame("a", 3, 3, 9, 8, 7)
	out, err := compositeI420FramesInto([]*av.Frame{odd}, []compositeLayout{{X: 1, Y: 1}}, 0, 0, "out", dst, video)
	if err != nil {
		t.Fatalf("odd composite error = %v", err)
	}
	if out.Video.Width != 4 || out.Video.Height != 4 {
		t.Fatalf("odd canvas = %+v, want 4x4", out.Video)
	}
	if out.Planes[0].Stride != 4 || len(out.Planes[1].Buffer.Bytes) != 4 || len(out.Planes[2].Buffer.Bytes) != 4 {
		t.Fatalf("odd planes = %+v", out.Planes)
	}
	out, err = compositeI420FramesInto([]*av.Frame{odd}, []compositeLayout{{}}, 0, 0, "out", dst, video)
	if err != nil {
		t.Fatalf("reused composite error = %v", err)
	}
	if out.Video.Width != 3 || out.Video.Height != 3 {
		t.Fatalf("reused canvas = %+v, want 3x3", out.Video)
	}
}

func TestVideoCompositeCopyPlaneClipsAndGuards(t *testing.T) {
	src := &av.Plane{Buffer: av.Buffer{Bytes: []byte{1, 2, 3, 4, 5, 6}}, Offset: 1}
	dst := make([]byte, 9)
	copyPlane(dst, 3, 3, src, 2, 2, -1, -1)
	if dst[0] != 5 || dst[1] != 0 || dst[3] != 0 {
		t.Fatalf("negative clipped dst = %v, want only visible source sample copied", dst)
	}

	dst = make([]byte, 9)
	copyPlane(dst, 3, 3, &av.Plane{Buffer: av.Buffer{Bytes: []byte{7, 8, 9, 10}}, Stride: 2}, 2, 2, 2, 1)
	if dst[5] != 7 || dst[8] != 9 {
		t.Fatalf("right clipped dst = %v, want last column copied", dst)
	}

	dst = []byte{1, 1, 1, 1}
	copyPlane(dst, 2, 2, &av.Plane{Buffer: av.Buffer{Bytes: []byte{9, 9}}, Offset: 4}, 2, 1, 0, 0)
	copyPlane(dst, 2, 2, &av.Plane{Buffer: av.Buffer{Bytes: []byte{8, 8}}}, 2, 1, 4, 0)
	if got := dst; got[0] != 1 || got[1] != 1 || got[2] != 1 || got[3] != 1 {
		t.Fatalf("guarded dst = %v, want unchanged", got)
	}
}

func TestVideoCompositeCloneAndEmitterErrorContracts(t *testing.T) {
	stage := newVideoCompositeStage("composite", []av.StreamID{"a", "b"}, "out",
		[]compositeLayout{{X: 0, Y: 0}, {X: 4, Y: 0}}, joinSyncArrival)

	mutable := compositeTestI420Frame("a", 2, 2, 11, 12, 13)
	for i := range mutable.Planes {
		mutable.Planes[i].Buffer.Ownership = av.BufferOwned
	}
	clone := stage.cloneCompositeFrame(mutable)
	mutable.Planes[0].Buffer.Bytes[0] = 99
	if clone.Planes[0].Buffer.Bytes[0] != 11 || clone.Planes[0].Buffer.Ownership != av.BufferOwned {
		t.Fatalf("clone plane = %+v, want owned deep copy", clone.Planes[0].Buffer)
	}
	stage.recycleCompositeFrame(clone)
	if got := len(stage.free["a"]); got != 1 {
		t.Fatalf("recycled free frames = %d, want 1", got)
	}
	clone = stage.cloneCompositeFrame(mutable)
	if clone.Planes[0].Buffer.Bytes[0] != 99 || clone.Planes[0].Buffer.Ownership != av.BufferOwned {
		t.Fatalf("reused clone plane = %+v, want owned reused buffer", clone.Planes[0].Buffer)
	}
	stage.recycleCompositeFrame(clone)

	extra := compositeTestI420Frame("new-arm", 1, 1, 1, 2, 3)
	extra.Planes = append(extra.Planes, av.Plane{Buffer: av.Buffer{Bytes: []byte{4}}})
	clone = stage.cloneCompositeFrame(extra)
	if clone.Video == nil || len(clone.Planes) != 4 {
		t.Fatalf("unknown-arm clone = %+v, want allocated video and 4 planes", clone)
	}

	emitErr := errors.New("emit failed")
	failEmit := &videoCompositeFailEmitter{err: emitErr}
	if err := stage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessageFrame, Frame: compositeTestI420Frame("a", 4, 4, 1, 2, 3)}, failEmit); err != nil {
		t.Fatalf("first frame error = %v", err)
	}
	err := stage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessageFrame, Frame: compositeTestI420Frame("b", 4, 4, 4, 5, 6)}, failEmit)
	if !errors.Is(err, emitErr) {
		t.Fatalf("emitter error = %v, want %v", err, emitErr)
	}
	if closeErr := stage.Close(); closeErr != nil {
		t.Fatalf("Close error = %v", closeErr)
	}
}

func TestVideoCompositeEOSErrorDuringDrain(t *testing.T) {
	stage := newVideoCompositeStage("composite", []av.StreamID{"a", "b"}, "out",
		[]compositeLayout{{X: 0, Y: 0}, {X: 4, Y: 0}}, joinSyncArrival)
	if err := stage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessageFrame, Frame: compositeTestI420Frame("a", 4, 4, 1, 2, 3)}, &mixTestEmitter{}); err != nil {
		t.Fatalf("frame error = %v", err)
	}
	emitErr := errors.New("emit failed")
	err := stage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventEndOfStream, StreamID: "b"}}, &videoCompositeFailEmitter{err: emitErr})
	if !errors.Is(err, emitErr) {
		t.Fatalf("EOS drain error = %v, want %v", err, emitErr)
	}
}

type videoCompositeFailEmitter struct {
	err error
}

func (e *videoCompositeFailEmitter) Emit(context.Context, *pipeline.Message) error {
	return e.err
}
