package goav

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// compositeTestVideoSource emits a single solid-color I420 frame then EOS, so a
// Composite arm has a deterministic frame to place on the canvas. It mirrors
// mixTestAudioSource (frame source → Sink), reusing compositeTestI420Frame.
func compositeTestVideoSource(id av.StreamID, w, h int, y, u, v byte) InputSpec {
	return Source(string(id),
		shape.Frame(av.MediaVideo, shape.Video(w, h, av.PixelFormatI420), shape.Stream(id)),
		func(_ context.Context, push SourcePush) error {
			if err := push.Frame(compositeTestI420Frame(id, w, h, y, u, v)); err != nil {
				return err
			}
			return push.EOS()
		})
}

func TestCompositeRunsTwoVideoSourcesIntoSink(t *testing.T) {
	ctx := context.Background()
	var got []*av.Frame
	sink := Sink(SinkFunc("out", func(_ context.Context, m Message) error {
		if m.Kind == pipeline.MessageFrame && m.Frame != nil && m.Frame.Video != nil {
			got = append(got, m.Frame)
		}
		return nil
	}))

	// arm "a" at top-left, arm "b" offset 4px right: the canvas is 8x4 and each
	// 4x4 input lands at its Region.
	task, err := Composite(
		From(compositeTestVideoSource("a", 4, 4, 100, 10, 20)).Video().Region(0, 0),
		From(compositeTestVideoSource("b", 4, 4, 200, 30, 40)).Video().Region(4, 0),
	).To(sink).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}

	if len(got) != 1 {
		t.Fatalf("composited frames at sink = %d, want 1", len(got))
	}
	out := got[0]
	if out.Video == nil || out.Video.Width != 8 || out.Video.Height != 4 {
		t.Fatalf("canvas = %+v, want 8x4", out.Video)
	}
	if out.Video.PixelFormat != av.PixelFormatI420 {
		t.Fatalf("pixel format = %q, want %q", out.Video.PixelFormat, av.PixelFormatI420)
	}
	if y := compositeTestYAt(out, 0, 0); y != 100 {
		t.Fatalf("Y at (0,0) = %d, want 100 (arm a top-left)", y)
	}
	if y := compositeTestYAt(out, 4, 0); y != 200 {
		t.Fatalf("Y at (4,0) = %d, want 200 (arm b at Region(4,0))", y)
	}
}

func TestCompositeRequiresTwoArms(t *testing.T) {
	_, err := Composite(From(compositeTestVideoSource("a", 4, 4, 100, 10, 20)).Video()).
		To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		Build(context.Background())
	var buildErr *BuildError
	if !errorsAsComposite(err, &buildErr) || buildErr.Code != "composite_inputs" {
		t.Fatalf("err = %v, want composite_inputs", err)
	}
}

func errorsAsComposite(err error, target **BuildError) bool { return errors.As(err, target) }

func TestCompositeRejectsDuplicateStreamIDs(t *testing.T) {
	_, err := Composite(
		From(compositeTestVideoSource("dup", 4, 4, 100, 10, 20)).Video().Region(0, 0),
		From(compositeTestVideoSource("dup", 4, 4, 200, 30, 40)).Video().Region(4, 0),
	).To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		Build(context.Background())
	var buildErr *BuildError
	if !errorsAsComposite(err, &buildErr) || buildErr.Code != "composite_arm" {
		t.Fatalf("err = %v, want composite_arm (distinct stream ids)", err)
	}
}
