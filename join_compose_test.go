package goav

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// These tests pin the north-star rule for joins: a Mix/Composite/Select output
// is a NORMAL stream point — it can be tapped and fanned out to planned
// branches with the same grammar (and the same lowering code) as any other
// chain point.

func joinTestCollectSink(name string, into *[][]int16) Destination {
	return Sink(SinkFunc(name, func(_ context.Context, m Message) error {
		if m.Kind == pipeline.MessageFrame && m.Frame != nil && m.Frame.Audio != nil {
			*into = append(*into, mixTestReadS16(m.Frame))
		}
		return nil
	}))
}

func TestMixTapDeliversMixedFramesToRuntimeBranch(t *testing.T) {
	ctx := context.Background()
	var main [][]int16
	task, err := Mix(
		From(mixTestAudioSource("a", 100, 200)).Audio(),
		From(mixTestAudioSource("b", 50, -50)).Audio(),
	).Tap(FrameTap("mixed")).To(joinTestCollectSink("out", &main)).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	// The join tap is a first-class task tap: typed, anchored on the join node.
	tap, ok := findTap(task.Taps(), "mixed")
	if !ok {
		t.Fatalf("taps = %+v, want mixed", task.Taps())
	}
	if tap.Domain != shape.DomainFrame || tap.MediaKind != av.MediaAudio || tap.Node != "mix" {
		t.Fatalf("tap = %+v, want frame audio tap on mix node", tap)
	}
	if tap.Shape.SampleRate != 48000 || tap.Shape.StreamID != "mix" {
		t.Fatalf("tap shape = %+v, want 48k stream mix", tap.Shape)
	}

	// Attaching a runtime branch from the join tap uses the same machinery as
	// chain taps and receives the MIXED frames.
	var tapped [][]int16
	attachment, err := task.Attach(ctx, Branch("monitor").
		From(FrameTap("mixed")).
		To(joinTestCollectSink("monitor", &tapped)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = task.Detach(context.Background(), attachment) }()

	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(main) != 1 || !reflect.DeepEqual(main[0], []int16{150, 150}) {
		t.Fatalf("main sink = %v, want [[150 150]]", main)
	}
	if len(tapped) != 1 || !reflect.DeepEqual(tapped[0], []int16{150, 150}) {
		t.Fatalf("tap branch = %v, want [[150 150]] (mixed frames)", tapped)
	}
}

func TestMixBranchesFanOutMixedStream(t *testing.T) {
	ctx := context.Background()
	var xs, ys [][]int16
	task, err := Mix(
		From(mixTestAudioSource("a", 100, 200)).Audio(),
		From(mixTestAudioSource("b", 50, -50)).Audio(),
	).Branches(
		Branch("x").To(joinTestCollectSink("x-sink", &xs)),
		Branch("y").To(joinTestCollectSink("y-sink", &ys)),
	).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(xs) != 1 || !reflect.DeepEqual(xs[0], []int16{150, 150}) {
		t.Fatalf("branch x = %v, want [[150 150]]", xs)
	}
	if len(ys) != 1 || !reflect.DeepEqual(ys[0], []int16{150, 150}) {
		t.Fatalf("branch y = %v, want [[150 150]]", ys)
	}

	// The fanout is the normal branch-compose topology hung off the join node.
	spec := task.Describe()
	hasJoin := false
	for _, n := range spec.Nodes {
		if n.Name == "mix" {
			hasJoin = true
		}
	}
	if !hasJoin {
		t.Fatalf("graph has no mix node: %+v", spec.Nodes)
	}
	intoSinks := 0
	for _, e := range spec.Edges {
		if string(e.To) == "x-sink" || string(e.To) == "y-sink" {
			intoSinks++
		}
	}
	if intoSinks != 2 {
		t.Fatalf("edges into branch sinks = %d, want 2; spec=%+v", intoSinks, spec.Edges)
	}
}

func TestMixBranchesEncodeAndMonitorIndependently(t *testing.T) {
	ctx := context.Background()
	muxers := &remuxTestMuxerFactory{}
	rt := New(
		withTestFormats(testFormatMuxer(av.FormatOgg, muxers)),
		WithEncoder(CodecDescriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
	)

	var monitor [][]int16
	task, err := Mix(
		From(mixTestAudioSource("a", 100, 200)).Audio(),
		From(mixTestAudioSource("b", 50, -50)).Audio(),
	).Branches(
		Branch("rec").Encode(codec.Opus()).To(File("mix.ogg", io.Discard, Format(av.FormatOgg))),
		Branch("mon").To(joinTestCollectSink("monitor", &monitor)),
	).UseRuntime(rt).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(muxers.muxers) != 1 || muxers.muxers[0].writes < 1 {
		t.Fatalf("muxer writes = %+v, want one mux with >=1 write (mix -> branch encode -> file)", muxers.muxers)
	}
	if len(monitor) != 1 || !reflect.DeepEqual(monitor[0], []int16{150, 150}) {
		t.Fatalf("monitor branch = %v, want [[150 150]] (raw mixed frames)", monitor)
	}
}

func TestMixBranchAnchorsFromJoinTap(t *testing.T) {
	ctx := context.Background()
	var got [][]int16
	task, err := Mix(
		From(mixTestAudioSource("a", 100, 200)).Audio(),
		From(mixTestAudioSource("b", 50, -50)).Audio(),
	).Tap(FrameTap("mixed")).Branches(
		Branch("x").From(FrameTap("mixed")).To(joinTestCollectSink("x-sink", &got)),
	).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !reflect.DeepEqual(got[0], []int16{150, 150}) {
		t.Fatalf("branch from join tap = %v, want [[150 150]]", got)
	}
	if _, ok := findTap(task.Taps(), "mixed"); !ok {
		t.Fatalf("taps = %+v, want mixed", task.Taps())
	}
}

func TestMixBranchesRequireAtLeastOneBranch(t *testing.T) {
	_, err := Mix(
		From(mixTestAudioSource("a", 1)).Audio(),
		From(mixTestAudioSource("b", 1)).Audio(),
	).Branches().Build(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "branch_missing" {
		t.Fatalf("err = %v, want branch_missing", err)
	}
}

func TestMixEncodeIsTerminalForBranches(t *testing.T) {
	_, err := Mix(
		From(mixTestAudioSource("a", 1)).Audio(),
		From(mixTestAudioSource("b", 1)).Audio(),
	).Encode(codec.Opus()).Branches(
		Branch("x").To(Sink(SinkFunc("x", func(context.Context, Message) error { return nil }))),
	).Build(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_branch_source_invalid" {
		t.Fatalf("err = %v, want encode_branch_source_invalid (same as the chain rule)", err)
	}
}

func TestMixBranchesPreserveArmCountError(t *testing.T) {
	_, err := Mix(From(mixTestAudioSource("a", 1)).Audio()).Branches(
		Branch("x").To(Sink(SinkFunc("x", func(context.Context, Message) error { return nil }))),
	).Build(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "mix_inputs" {
		t.Fatalf("err = %v, want mix_inputs", err)
	}
}

func TestMixBranchFromUnknownTapRejected(t *testing.T) {
	_, err := Mix(
		From(mixTestAudioSource("a", 1)).Audio(),
		From(mixTestAudioSource("b", 1)).Audio(),
	).Branches(
		Branch("x").From(FrameTap("nope")).To(Sink(SinkFunc("x", func(context.Context, Message) error { return nil }))),
	).Build(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "branch_tap_missing" {
		t.Fatalf("err = %v, want branch_tap_missing", err)
	}
}

func TestMixBranchMuxDestinationRequiresEncode(t *testing.T) {
	_, err := Mix(
		From(mixTestAudioSource("a", 1)).Audio(),
		From(mixTestAudioSource("b", 1)).Audio(),
	).Branches(
		Branch("x").To(File("x.ogg", io.Discard, Format(av.FormatOgg))),
	).Build(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_missing" {
		t.Fatalf("err = %v, want encode_missing (same as the chain rule)", err)
	}
}

func TestMixBranchesThenToRejected(t *testing.T) {
	_, err := Mix(
		From(mixTestAudioSource("a", 1)).Audio(),
		From(mixTestAudioSource("b", 1)).Audio(),
	).Branches(
		Branch("x").To(Sink(SinkFunc("x", func(context.Context, Message) error { return nil }))),
	).To(Sink(SinkFunc("late", func(context.Context, Message) error { return nil }))).
		Build(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_scope_mixed" {
		t.Fatalf("err = %v, want output_scope_mixed (same as the chain rule)", err)
	}
}

func TestMixTapRejectsPacketDomain(t *testing.T) {
	_, err := Mix(
		From(mixTestAudioSource("a", 1)).Audio(),
		From(mixTestAudioSource("b", 1)).Audio(),
	).Tap(PacketTap("mixed")).
		To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		Build(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "tap_domain_mismatch" {
		t.Fatalf("err = %v, want tap_domain_mismatch (the mix output is frames)", err)
	}
}

func TestCompositeBranchesFanOutCompositedStream(t *testing.T) {
	ctx := context.Background()
	var xs, ys []*av.Frame
	collect := func(name string, into *[]*av.Frame) Destination {
		return Sink(SinkFunc(name, func(_ context.Context, m Message) error {
			if m.Kind == pipeline.MessageFrame && m.Frame != nil && m.Frame.Video != nil {
				*into = append(*into, m.Frame)
			}
			return nil
		}))
	}
	task, err := Composite(
		From(compositeTestVideoSource("a", 4, 4, 100, 10, 20)).Video().Region(0, 0),
		From(compositeTestVideoSource("b", 4, 4, 200, 30, 40)).Video().Region(4, 0),
	).Branches(
		Branch("x").To(collect("x-sink", &xs)),
		Branch("y").To(collect("y-sink", &ys)),
	).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(xs) != 1 || len(ys) != 1 {
		t.Fatalf("branch frames x=%d y=%d, want 1 each", len(xs), len(ys))
	}
	for _, out := range []*av.Frame{xs[0], ys[0]} {
		if out.Video == nil || out.Video.Width != 8 || out.Video.Height != 4 {
			t.Fatalf("canvas = %+v, want 8x4 on both branches", out.Video)
		}
		if y := compositeTestYAt(out, 4, 0); y != 200 {
			t.Fatalf("Y at (4,0) = %d, want 200 (composited canvas on both branches)", y)
		}
	}
}

func TestSelectTapNamesSwitchedStream(t *testing.T) {
	ctx := context.Background()
	var got [][]int16
	task, err := Select(
		From(selectTestOneShotSource("a", 100)).Audio(),
		From(selectTestOneShotSource("b", 200)).Audio(),
	).Tap(Tap("switched")).To(joinTestCollectSink("out", &got)).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	tap, ok := findTap(task.Taps(), "switched")
	if !ok {
		t.Fatalf("taps = %+v, want switched", task.Taps())
	}
	if tap.Domain != shape.DomainFrame || tap.Node != "select" {
		t.Fatalf("tap = %+v, want frame tap on select node (frame arms switch frames)", tap)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !reflect.DeepEqual(got[0], []int16{100}) {
		t.Fatalf("forwarded = %v, want [[100]] (default arm)", got)
	}
}
