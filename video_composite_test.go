package goav

import (
	"context"
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
		[]compositeLayout{{X: 0, Y: 0}, {X: 4, Y: 0}})
	emit := &mixTestEmitter{}
	ctx := context.Background()

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
		[]compositeLayout{{X: 0, Y: 0}, {X: 4, Y: 0}})
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
		[]compositeLayout{{X: 0, Y: 0}, {X: 4, Y: 0}})
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
