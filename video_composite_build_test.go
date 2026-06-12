package goav

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
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
			if _, err := push.Frame(compositeTestI420Frame(id, w, h, y, u, v)); err != nil {
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
			got = append(got, cloneMixTestFrame(m.Frame))
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

func TestCompositeBuilderOptionsCarryIntoJoinSpec(t *testing.T) {
	var nilComposite *compositeStream
	if arm := nilComposite.joinArm(); arm.join != nil || arm.region != nil {
		t.Fatalf("nil composite join arm = %+v, want zero", arm)
	}

	require := shape.Frame(av.MediaVideo, shape.Video(8, 4, av.PixelFormatI420))
	prefer := shape.Frame(av.MediaVideo, shape.Video(16, 8, av.PixelFormatI420))
	composite := Composite(
		From(compositeTestVideoSource("a", 4, 4, 100, 10, 20)).Video().Region(0, 0),
		From(compositeTestVideoSource("b", 4, 4, 200, 30, 40)).Video().Region(4, 0),
	).
		SyncByPTS().
		Auto(shape.AllowResize(), shape.AllowConvert()).
		Require(require).
		Prefer(prefer).
		Encode(codec.VP8(codec.Bitrate(750_000))).
		Tap(FrameTap("canvas")).
		Region(2, 3)

	if composite.sync != joinSyncPTS {
		t.Fatalf("sync = %v, want PTS", composite.sync)
	}
	if composite.encode == nil || composite.encode.ID != av.CodecVP8 || composite.encode.Settings.Bitrate != 750_000 {
		t.Fatalf("encode = %+v", composite.encode)
	}
	if len(composite.taps) != 1 || composite.taps[0].name != "canvas" {
		t.Fatalf("taps = %+v", composite.taps)
	}
	if composite.region == nil || composite.region.x != 2 || composite.region.y != 3 {
		t.Fatalf("region = %+v", composite.region)
	}
	if len(composite.operations) != 3 {
		t.Fatalf("operations = %+v, want auto/require/prefer", composite.operations)
	}
	if composite.operations[0].Auto == nil ||
		!composite.operations[0].Auto.AllowsResize() ||
		!composite.operations[0].Auto.AllowsConvert() ||
		composite.operations[0].Auto.AllowsResample() {
		t.Fatalf("auto operation = %+v", composite.operations[0])
	}
	if composite.operations[1].Require == nil || *composite.operations[1].Require != require {
		t.Fatalf("require operation = %+v", composite.operations[1])
	}
	if composite.operations[2].Prefer == nil || *composite.operations[2].Prefer != prefer {
		t.Fatalf("prefer operation = %+v", composite.operations[2])
	}

	arm := composite.joinArm()
	if arm.region == nil || arm.region.x != 2 || arm.region.y != 3 {
		t.Fatalf("join arm region = %+v", arm.region)
	}
	if arm.join == nil ||
		arm.join.kind != joinComposite ||
		arm.join.sync != joinSyncPTS ||
		arm.join.encode == nil ||
		arm.join.encode.ID != av.CodecVP8 ||
		len(arm.join.taps) != 1 ||
		len(arm.join.operations) != 3 {
		t.Fatalf("join arm = %+v", arm.join)
	}

	composite.operations[0] = operationSpec{}
	if arm.join.operations[0].Auto == nil || !arm.join.operations[0].Auto.AllowsResize() {
		t.Fatalf("join arm operations were not cloned: %+v", arm.join.operations)
	}
}
