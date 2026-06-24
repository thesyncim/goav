package goav

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// These tests pin nested joins: a join is a JoinArm, so it can stand as an arm
// of another join — Mix(Mix(a, b), c) sub-mixes two arms and mixes the result
// with a third, Select(Mix(a, b), Mix(c, d)) switches between two live mixes,
// and composites nest as sub-canvases. A nested join contributes its JOINED
// output stream under its output id (the planned node name: mix, mix-2, ...).

// TestMixOfMixSumsBothStages pins the summing semantics of a nested mix:
// (a+b)+c, with S16 clamping applied independently at EACH mix stage (the
// inner mix emits clamped S16, the outer sums those samples and clamps again).
func TestMixOfMixSumsBothStages(t *testing.T) {
	ctx := context.Background()
	var got [][]int16
	task, err := Mix(
		Mix(
			From(mixTestAudioSource("a", 100, 200)).Audio(),
			From(mixTestAudioSource("b", 50, -50)).Audio(),
		),
		From(mixTestAudioSource("c", 5, 5)).Audio(),
	).To(joinTestCollectSink("out", &got)).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	// inner: (100+50, 200-50) = (150, 150); outer: (150+5, 150+5) = (155, 155).
	if len(got) != 1 || !reflect.DeepEqual(got[0], []int16{155, 155}) {
		t.Fatalf("nested mix = %v, want [[155 155]] ((a+b)+c)", got)
	}
}

// TestNestedMixClampsPerStage pins that clamping happens at each stage: the
// inner mix saturates to 32767 BEFORE the outer mix sums it with c, so the
// final sample is 32767-30000, not (30000+30000)-30000.
func TestNestedMixClampsPerStage(t *testing.T) {
	ctx := context.Background()
	var got [][]int16
	task, err := Mix(
		Mix(
			From(mixTestAudioSource("a", 30000)).Audio(),
			From(mixTestAudioSource("b", 30000)).Audio(),
		),
		From(mixTestAudioSource("c", -30000)).Audio(),
	).To(joinTestCollectSink("out", &got)).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !reflect.DeepEqual(got[0], []int16{2767}) {
		t.Fatalf("nested mix = %v, want [[2767]] (inner clamps to 32767 before the outer sums -30000)", got)
	}
}

// TestSelectOfMixesSwitchesLive switches a running Select between two live
// sub-mixes through the control plane. The arm ids are the sub-joins' output
// ids — their planned node names (mix, mix-2) — and the SelectActive event
// rides the data path THROUGH the mix stages to the selector unchanged.
func TestSelectOfMixesSwitchesLive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	sink := newSelectSwitchSink("out")
	tk, err := Select(
		Mix(
			From(selectTestLiveSource("a", 100)).Audio(),
			From(selectTestLiveSource("b", 50)).Audio(),
		),
		Mix(
			From(selectTestLiveSource("c", 10)).Audio(),
			From(selectTestLiveSource("d", 20)).Audio(),
		),
	).To(Sink(sink)).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tk.Close() })

	runErr := make(chan error, 1)
	go func() { runErr <- tk.Run(ctx) }()

	// Default active is the first arm "mix": its mixed samples (100+50) flow.
	if err := sink.waitFor(ctx, 150); err != nil {
		t.Fatalf("default sub-mix never forwarded: %v", err)
	}

	// Switch live to the second sub-mix "mix-2" (10+20).
	sink.resetSeen()
	if err := controlUntilAccepted(ctx, tk.(*task), SelectActive("mix-2")); err != nil {
		t.Fatalf("SelectActive to mix-2: %v", err)
	}
	if err := sink.waitFor(ctx, 30); err != nil {
		t.Fatalf("after switch, sub-mix mix-2 never forwarded: %v", err)
	}

	// And back, proving the switch works both ways across nested joins.
	sink.resetSeen()
	if err := tk.Control(ctx, SelectActive("mix")); err != nil {
		t.Fatalf("SelectActive back to mix: %v", err)
	}
	if err := sink.waitFor(ctx, 150); err != nil {
		t.Fatalf("after switch back, sub-mix mix never forwarded: %v", err)
	}

	cancel()
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v", err)
	}
}

func joinNestedTestFrameSink(name string, into *[]*av.Frame) Destination {
	return Sink(SinkFunc(name, func(_ context.Context, m Message) error {
		if m.Kind == pipeline.MessageFrame && m.Frame != nil && m.Frame.Video != nil {
			*into = append(*into, cloneMixTestFrame(m.Frame))
		}
		return nil
	}))
}

// TestCompositeOfCompositesPaintsNestedCanvas paints a sub-composite as one
// arm of an outer composite: the inner 8x4 canvas lands at the default
// top-left corner and the leaf arm lands at its Region below it.
func TestCompositeOfCompositesPaintsNestedCanvas(t *testing.T) {
	ctx := context.Background()
	var got []*av.Frame
	task, err := Composite(
		Composite(
			From(compositeTestVideoSource("a", 4, 4, 100, 10, 20)).Video().Region(0, 0),
			From(compositeTestVideoSource("b", 4, 4, 200, 30, 40)).Video().Region(4, 0),
		),
		From(compositeTestVideoSource("c", 4, 4, 50, 10, 20)).Video().Region(0, 4),
	).To(joinNestedTestFrameSink("out", &got)).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Video == nil || got[0].Video.Width != 8 || got[0].Video.Height != 8 {
		t.Fatalf("canvas = %+v, want one 8x8 frame", got)
	}
	out := got[0]
	if y := compositeTestYAt(out, 0, 0); y != 100 {
		t.Fatalf("Y at (0,0) = %d, want 100 (inner arm a)", y)
	}
	if y := compositeTestYAt(out, 4, 0); y != 200 {
		t.Fatalf("Y at (4,0) = %d, want 200 (inner arm b)", y)
	}
	if y := compositeTestYAt(out, 0, 4); y != 50 {
		t.Fatalf("Y at (0,4) = %d, want 50 (outer leaf c)", y)
	}
	if y := compositeTestYAt(out, 4, 4); y != 0 {
		t.Fatalf("Y at (4,4) = %d, want 0 (black canvas)", y)
	}
}

// TestNestedCompositeRegionPlacesSubCanvas places the sub-composite itself
// with .Region(x, y) on the outer canvas.
func TestNestedCompositeRegionPlacesSubCanvas(t *testing.T) {
	ctx := context.Background()
	var got []*av.Frame
	task, err := Composite(
		From(compositeTestVideoSource("c", 4, 4, 50, 10, 20)).Video().Region(0, 0),
		Composite(
			From(compositeTestVideoSource("a", 4, 4, 100, 10, 20)).Video().Region(0, 0),
			From(compositeTestVideoSource("b", 4, 4, 200, 30, 40)).Video().Region(4, 0),
		).Region(0, 4),
	).To(joinNestedTestFrameSink("out", &got)).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Video == nil || got[0].Video.Width != 8 || got[0].Video.Height != 8 {
		t.Fatalf("canvas = %+v, want one 8x8 frame", got)
	}
	out := got[0]
	if y := compositeTestYAt(out, 0, 0); y != 50 {
		t.Fatalf("Y at (0,0) = %d, want 50 (leaf c)", y)
	}
	if y := compositeTestYAt(out, 0, 4); y != 100 {
		t.Fatalf("Y at (0,4) = %d, want 100 (sub-canvas arm a at nested Region)", y)
	}
	if y := compositeTestYAt(out, 4, 4); y != 200 {
		t.Fatalf("Y at (4,4) = %d, want 200 (sub-canvas arm b at nested Region)", y)
	}
}

// TestSelectRegionPlacesSwitchedArmOnComposite pins Select's .Region(x, y):
// a one-of-N switch standing as a Composite arm — the active-speaker tile —
// lands at its declared canvas position, mirroring .Region on source chains
// and nested composites. The switch forwards its first arm by default. The
// Select forces a buffered graph that recycles frames after delivery, so the
// sink copies the canvas facts instead of retaining the frame.
func TestSelectRegionPlacesSwitchedArmOnComposite(t *testing.T) {
	ctx := context.Background()
	type canvas struct {
		width, height int
		yPlane        []byte
	}
	var got []canvas
	sink := Sink(SinkFunc("out", func(_ context.Context, m Message) error {
		if m.Kind == pipeline.MessageFrame && m.Frame != nil && m.Frame.Video != nil {
			got = append(got, canvas{
				width:  m.Frame.Video.Width,
				height: m.Frame.Video.Height,
				yPlane: append([]byte(nil), m.Frame.Planes[0].Buffer.Bytes...),
			})
		}
		return nil
	}))
	task, err := Composite(
		From(compositeTestVideoSource("c", 4, 4, 50, 10, 20)).Video().Region(0, 0),
		Select(
			From(compositeTestVideoSource("a", 4, 4, 100, 10, 20)).Video(),
			From(compositeTestVideoSource("b", 4, 4, 200, 30, 40)).Video(),
		).Region(0, 4),
	).To(sink).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].width != 4 || got[0].height != 8 {
		t.Fatalf("canvas = %+v, want one 4x8 frame", got)
	}
	out := got[0]
	if y := out.yPlane[0*out.width+0]; y != 50 {
		t.Fatalf("Y at (0,0) = %d, want 50 (leaf c)", y)
	}
	if y := out.yPlane[4*out.width+0]; y != 100 {
		t.Fatalf("Y at (0,4) = %d, want 100 (switched arm a at the Select Region)", y)
	}
}

// TestMixResamplesNestedMixOutput pins the solver path for nested arms: the
// outer mix's first arm sets a 48kHz target, and the 24kHz sub-mix OUTPUT is
// auto-resampled through the same armPolicy solver path as any leaf arm.
func TestMixResamplesNestedMixOutput(t *testing.T) {
	ctx := context.Background()
	rt := MustNew(testStdFilters())

	var frames int
	sink := Sink(SinkFunc("out", func(_ context.Context, m Message) error {
		if m.Kind == pipeline.MessageFrame && m.Frame != nil && m.Frame.Audio != nil {
			frames++
			if got := m.Frame.Audio.SampleRate; got != 48000 {
				t.Errorf("mixed frame sample rate = %d, want 48000 (the 24kHz sub-mix output must be resampled)", got)
			}
		}
		return nil
	}))

	job := Mix(
		From(mixTestAudioSourceRate("a", 48000)).Audio(),
		Mix(
			From(mixTestAudioSourceRate("b", 24000)).Audio(),
			From(mixTestAudioSourceRate("c", 24000)).Audio(),
		),
	).To(sink).UseRuntime(rt)

	planned, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(specText(planned), "mix-resample-mix-2") {
		t.Fatalf("planned spec missing the nested-arm resample:\n%s", specText(planned))
	}

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if frames < 1 {
		t.Fatalf("mixed frames at sink = %d, want >=1 (24kHz sub-mix auto-resampled to 48kHz, then mixed)", frames)
	}
}

// TestJoinDescribeEqualsBuildNestedMix is the nested Describe() ≡ Build()
// guard: a sub-mix arm (with its own SyncByPTS) under an outer mix that
// resamples the sub-mix output, planned and built from the one plan.
func TestJoinDescribeEqualsBuildNestedMix(t *testing.T) {
	job := Mix(
		From(mixTestAudioSourceRate("a", 48000)).Audio(),
		Mix(
			From(mixTestAudioSourceRate("b", 24000)).Audio(),
			From(mixTestAudioSourceRate("c", 24000)).Audio(),
		).SyncByPTS(),
	).To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		UseRuntime(MustNew(testStdFilters()))

	planned := joinPlanGuard(t, job)
	text := specText(planned)
	for _, want := range []string{
		"mix-2 [sync=pts]",            // the nested join node, SyncByPTS scoped to ITS arms
		"b -> mix-2",                  // sub-mix convergence
		"c -> mix-2",                  //
		"mix-2 -> mix-resample-mix-2", // outer join resamples the sub-mix OUTPUT
		"mix-resample-mix-2 -> mix",   //
		"a -> mix",                    // leaf arm feeds the outer join directly
		"mix -> out",                  // joined chain to the sink
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("planned nested mix spec missing %q:\n%s", want, text)
		}
	}
}

// TestJoinDescribeEqualsBuildSelectOfMixes guards the planned shape of a
// Select switching between two sub-mixes.
func TestJoinDescribeEqualsBuildSelectOfMixes(t *testing.T) {
	job := Select(
		Mix(
			From(mixTestAudioSource("a", 1)).Audio(),
			From(mixTestAudioSource("b", 1)).Audio(),
		),
		Mix(
			From(mixTestAudioSource("c", 1)).Audio(),
			From(mixTestAudioSource("d", 1)).Audio(),
		),
	).To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil })))

	planned := joinPlanGuard(t, job)
	text := specText(planned)
	for _, want := range []string{
		"mix -> select",   // first sub-mix arm
		"mix-2 -> select", // second sub-mix arm (auto-disambiguated id)
		"select -> out",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("planned select-of-mixes spec missing %q:\n%s", want, text)
		}
	}
}

// TestNestedMixTapAnchorsOnSubJoinNode pins that a tap declared on a nested
// join is a first-class task tap anchored on the SUB join's node, attachable
// at runtime like any join tap.
func TestNestedMixTapAnchorsOnSubJoinNode(t *testing.T) {
	ctx := context.Background()
	var main [][]int16
	task, err := Mix(
		Mix(
			From(mixTestAudioSource("a", 100, 200)).Audio(),
			From(mixTestAudioSource("b", 50, -50)).Audio(),
		).Tap(FrameTap("submix")),
		From(mixTestAudioSource("c", 5, 5)).Audio(),
	).To(joinTestCollectSink("out", &main)).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	tap, ok := findTap(task.Taps(), "submix")
	if !ok {
		t.Fatalf("taps = %+v, want submix", task.Taps())
	}
	if tap.Domain != shape.DomainFrame || tap.Node != "mix-2" {
		t.Fatalf("tap = %+v, want frame tap on the nested mix-2 node", tap)
	}

	var tapped [][]int16
	attachment, err := task.Attach(ctx, Branch("monitor").
		From(FrameTap("submix")).
		To(joinTestCollectSink("monitor", &tapped)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = task.Detach(context.Background(), attachment) }()

	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(main) != 1 || !reflect.DeepEqual(main[0], []int16{155, 155}) {
		t.Fatalf("main sink = %v, want [[155 155]]", main)
	}
	if len(tapped) != 1 || !reflect.DeepEqual(tapped[0], []int16{150, 150}) {
		t.Fatalf("tap branch = %v, want [[150 150]] (the INNER mix output)", tapped)
	}
}

// TestNestedMixEncodesToFile pins the README shape: the OUTER join encodes
// the nested result and muxes it to a file — encode belongs to the outer
// join, never to the arm.
func TestNestedMixEncodesToFile(t *testing.T) {
	ctx := context.Background()
	muxers := &remuxTestMuxerFactory{}
	rt := MustNew(
		withTestFormats(testFormatMuxer(av.FormatOgg, muxers)),
		WithEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
	)

	task, err := Mix(
		Mix(
			From(mixTestAudioSource("a", 100, 200)).Audio(),
			From(mixTestAudioSource("b", 50, -50)).Audio(),
		),
		From(mixTestAudioSource("c", 5, 5)).Audio(),
	).Encode(codec.Opus()).To(File("mix.ogg", io.Discard, Format(av.FormatOgg))).UseRuntime(rt).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(muxers.muxers) != 1 || muxers.muxers[0].writes < 1 {
		t.Fatalf("muxer writes = %+v, want one mux with >=1 write (nested mix -> encode -> file)", muxers.muxers)
	}
}

// TestNestedJoinArmRejectsEncode pins that a join used as an arm is not a
// terminal: it cannot carry its own encoder.
func TestNestedJoinArmRejectsEncode(t *testing.T) {
	_, err := Mix(
		Mix(
			From(mixTestAudioSource("a", 1)).Audio(),
			From(mixTestAudioSource("b", 1)).Audio(),
		).Encode(codec.Opus()),
		From(mixTestAudioSource("c", 1)).Audio(),
	).To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		Build(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "mix_arm" {
		t.Fatalf("err = %v, want mix_arm (a nested join arm is not a terminal)", err)
	}
}

// TestNestedJoinArmsRequireDistinctOutputIDs pins that a nested join's output
// id participates in the sibling distinct-id check.
func TestNestedJoinArmsRequireDistinctOutputIDs(t *testing.T) {
	_, err := Mix(
		Mix(
			From(mixTestAudioSource("a", 1)).Audio(),
			From(mixTestAudioSource("b", 1)).Audio(),
		),
		// The leaf arm collides with the nested mix's auto-assigned output id.
		From(mixTestAudioSource("mix-2", 1)).Audio(),
	).To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		Build(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "mix_arm" {
		t.Fatalf("err = %v, want mix_arm (distinct ids across nested output and siblings)", err)
	}
}

// TestNestedJoinRequiresTwoArms pins the arm-count rule on nested joins.
func TestNestedJoinRequiresTwoArms(t *testing.T) {
	_, err := Mix(
		Mix(From(mixTestAudioSource("a", 1)).Audio()),
		From(mixTestAudioSource("c", 1)).Audio(),
	).To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		Build(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "mix_inputs" {
		t.Fatalf("err = %v, want mix_inputs (nested joins need two arms too)", err)
	}
}

// TestMixRejectsNestedCompositeArm pins the media discipline: a Mix arm must
// produce audio, so a nested composite (video) is rejected with the arm error.
func TestMixRejectsNestedCompositeArm(t *testing.T) {
	_, err := Mix(
		Composite(
			From(compositeTestVideoSource("a", 4, 4, 100, 10, 20)).Video(),
			From(compositeTestVideoSource("b", 4, 4, 200, 30, 40)).Video(),
		),
		From(mixTestAudioSource("c", 1)).Audio(),
	).To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		Build(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "mix_arm" {
		t.Fatalf("err = %v, want mix_arm (a mix arm must produce audio)", err)
	}
}
