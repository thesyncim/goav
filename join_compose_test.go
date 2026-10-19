package goav

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
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
	task, err := mustNew().Build(ctx,
		Mix(mixTestAudioSource("a", 100, 200).Audio(), mixTestAudioSource("b", 50, -50).Audio()).
			Tap("mixed").
			To(joinTestCollectSink("out", &main)),
	)
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
	attachment, err := task.Attach(ctx, task.Tap("mixed").As("monitor").To(joinTestCollectSink("monitor", &tapped)))
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
	fork := Mix(mixTestAudioSource("a", 100, 200).Audio(), mixTestAudioSource("b", 50, -50).Audio())
	task, err := mustNew().Build(ctx,
		fork.As("x").To(joinTestCollectSink("x-sink", &xs)),
		fork.As("y").To(joinTestCollectSink("y-sink", &ys)),
	)
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
	spec := task.Spec()
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

func TestJoinOutputBranches(t *testing.T) {
	ctx := context.Background()
	var encoded, monitor [][]int16
	fork := Mix(mixTestAudioSource("a", 30, 40).Audio(), mixTestAudioSource("b", 5, 6).Audio())
	task, err := mustNew().Build(ctx,
		fork.As("encoded").To(joinTestCollectSink("encoded", &encoded)),
		fork.As("monitor").To(joinTestCollectSink("monitor", &monitor)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	want := []int16{35, 46}
	if len(encoded) != 1 || !reflect.DeepEqual(encoded[0], want) {
		t.Fatalf("encoded branch = %v, want %v", encoded, want)
	}
	if len(monitor) != 1 || !reflect.DeepEqual(monitor[0], want) {
		t.Fatalf("monitor branch = %v, want %v", monitor, want)
	}
}

func TestMixBranchesEncodeAndMonitorIndependently(t *testing.T) {
	ctx := context.Background()
	muxers := &remuxTestMuxerFactory{}
	rt := mustNew(
		withTestFormats(testFormatMuxer(av.FormatOgg, muxers)),
		WithEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
	)

	var monitor [][]int16
	fork := Mix(mixTestAudioSource("a", 100, 200).Audio(), mixTestAudioSource("b", 50, -50).Audio())
	task, err := rt.Build(ctx,
		fork.Encode(codec.Opus()).As("rec").To(Write("mix.ogg", io.Discard, Format(av.FormatOgg))),
		fork.As("mon").To(joinTestCollectSink("monitor", &monitor)),
	)
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
	task, err := mustNew().Build(ctx,
		Mix(mixTestAudioSource("a", 100, 200).Audio(), mixTestAudioSource("b", 50, -50).Audio()).
			Tap("mixed").
			As("x").
			To(joinTestCollectSink("x-sink", &got)),
	)
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

// TestMixForksAfterItsEncoder pins that a joined stream forks after its
// encoder like any stream: the fork reads the encoded packets.
func TestMixForksAfterItsEncoder(t *testing.T) {
	spec, err := mustNew().Describe(
		Mix(mixTestAudioSource("a", 1).Audio(), mixTestAudioSource("b", 1).Audio()).
			Encode(codec.Opus()).
			As("x").
			To(Sink(SinkFunc("x", func(context.Context, Message) error { return nil }))),
	)
	if err != nil {
		t.Fatal(err)
	}
	if text := specText(spec); !strings.Contains(text, "mix -> encode-x") || !strings.Contains(text, "encode-x -> x") {
		t.Fatalf("spec:\n%s", text)
	}
}

func TestMixBranchesPreserveArmCountError(t *testing.T) {
	_, err := mustNew().Build(context.Background(),
		Mix(mixTestAudioSource("a", 1).Audio()).
			As("x").
			To(Sink(SinkFunc("x", func(context.Context, Message) error { return nil }))),
	)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "mix_inputs" {
		t.Fatalf("err = %v, want mix_inputs", err)
	}
}

func TestMixBranchMuxDestinationRequiresEncode(t *testing.T) {
	_, err := mustNew().Build(context.Background(),
		Mix(mixTestAudioSource("a", 1).Audio(), mixTestAudioSource("b", 1).Audio()).
			As("x").
			To(Write("x.ogg", io.Discard, Format(av.FormatOgg))),
	)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_missing" {
		t.Fatalf("err = %v, want encode_missing (same as the chain rule)", err)
	}
}

func TestCompositeBranchesFanOutCompositedStream(t *testing.T) {
	ctx := context.Background()
	var xs, ys []*av.Frame
	collect := func(name string, into *[]*av.Frame) Destination {
		return Sink(SinkFunc(name, func(_ context.Context, m Message) error {
			if m.Kind == pipeline.MessageFrame && m.Frame != nil && m.Frame.Video != nil {
				*into = append(*into, cloneMixTestFrame(m.Frame))
			}
			return nil
		}))
	}
	fork := Composite(At(compositeTestVideoSource("a", 4, 4, 100, 10, 20).Video(), 0, 0), At(compositeTestVideoSource("b", 4, 4, 200, 30, 40).Video(), 4, 0))
	task, err := mustNew().Build(ctx,
		fork.As("x").To(collect("x-sink", &xs)),
		fork.As("y").To(collect("y-sink", &ys)),
	)
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
	task, err := mustNew().Build(ctx,
		Select(selectTestOneShotSource("a", 100).Audio(), selectTestOneShotSource("b", 200).Audio()).
			Tap("switched").
			To(joinTestCollectSink("out", &got)),
	)
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
