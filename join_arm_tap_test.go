package goav

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// These tests pin the tap-arm contract: a chain arm keeps its declared
// .Decode()/.Tap(...) (the tap installs on the task, anchored mid-graph), and
// a tap-reference arm converges that already-flowing point again — the join-side
// dual of Branch().From(tap). One decode, any number of consumers.

type tapArmTestDecoderFactory struct {
	decoder *tapArmTestDecoder
}

func (f tapArmTestDecoderFactory) NewDecoder(_ context.Context, _ codec.DecodeConfig) (codec.Decoder, error) {
	return f.decoder, nil
}

// tapArmTestDecoder turns each packet's payload bytes into one mono S16 frame
// verbatim, so mixed sums are exact and decode invocations are countable.
type tapArmTestDecoder struct {
	decodes int
}

func (d *tapArmTestDecoder) Descriptor() codec.Descriptor {
	return codec.Descriptor{ID: "x_pcm_mono"}
}

func (d *tapArmTestDecoder) Open(context.Context, codec.DecodeConfig) error { return nil }

func (d *tapArmTestDecoder) DecodeInto(_ context.Context, packet *av.Packet, out *codec.DecodeResult) error {
	if packet == nil {
		return nil
	}
	if len(out.Frames) == cap(out.Frames) {
		return codec.ErrResultFull
	}
	index := len(out.Frames)
	out.Frames = out.Frames[:index+1]
	frame := &out.Frames[index]
	frame.Reset()
	frame.StreamID = packet.StreamID
	frame.Type = av.MediaAudio
	frame.Audio = &av.AudioFrame{SampleRate: 48000, Channels: 1, SampleFormat: av.SampleFormatS16, Samples: len(packet.Payload.Bytes) / 2}
	if cap(frame.Planes) < 1 {
		frame.Planes = make([]av.Plane, 1)
	} else {
		frame.Planes = frame.Planes[:1]
	}
	frame.Planes[0].Buffer.Bytes = append(frame.Planes[0].Buffer.Bytes[:0], packet.Payload.Bytes...)
	frame.Planes[0].Stride = 2
	d.decodes++
	return nil
}

func (d *tapArmTestDecoder) FlushInto(context.Context, *codec.DecodeResult) error { return nil }
func (d *tapArmTestDecoder) HandleEvent(context.Context, *av.Event) error         { return nil }
func (d *tapArmTestDecoder) Close() error                                         { return nil }

func tapArmTestPacketSource(id av.StreamID, codecID av.CodecID, samples ...int16) InputSpec {
	payload := make([]byte, len(samples)*2)
	for i := range samples {
		payload[i*2] = byte(samples[i])
		payload[i*2+1] = byte(samples[i] >> 8)
	}
	return Source(string(id),
		shape.Packet(av.MediaAudio, codecID, shape.Audio(48000, codec.Mono, av.SampleFormatS16), shape.Stream(id)),
		func(_ context.Context, push SourcePush) error {
			if _, err := push.Packet(&av.Packet{StreamID: id, Type: av.MediaAudio, Payload: av.Buffer{Bytes: payload, Ownership: av.BufferImmutable}}); err != nil {
				return err
			}
			return push.EOS()
		})
}

// TestMixChainArmTapDecodesOnceAndMixes is the headline: decode a packet
// chain ONCE, tap the decoded point mid-graph, and mix it with a live frame
// source in one task. The tap is a first-class task tap (runtime branches
// attach to the decoded music), the topology holds exactly one decode node,
// and the sink receives the mixed sum.
func TestMixChainArmTapDecodesOnceAndMixes(t *testing.T) {
	ctx := context.Background()
	pcm := av.CodecID("x_pcm_mono")
	decoder := &tapArmTestDecoder{}
	rt := MustNew(WithDecoder(
		codec.Descriptor{ID: pcm, Name: "PCM mono", Type: av.MediaAudio, Capabilities: codec.Capabilities{SampleFormats: []string{av.SampleFormatS16}}},
		tapArmTestDecoderFactory{decoder: decoder},
	))

	var main [][]int16
	task, err := Mix(
		From(tapArmTestPacketSource("a", pcm, 100, 200)).Audio().Decode().Tap(FrameTap("music")),
		From(mixTestAudioSource("live", 50, -50)).Audio(),
	).To(joinTestCollectSink("out", &main)).UseRuntime(rt).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	// Exactly ONE decode node: the tapped chain decodes once, mid-graph.
	decodeNodes := 0
	for _, node := range task.Describe().Nodes {
		if strings.Contains(node.Name, "decode") {
			decodeNodes++
		}
	}
	if decodeNodes != 1 {
		t.Fatalf("decode nodes = %d, want exactly 1; nodes=%+v", decodeNodes, task.Describe().Nodes)
	}

	// The arm tap is a first-class task tap anchored on the arm's decode node.
	tap, ok := findTap(task.Taps(), "music")
	if !ok {
		t.Fatalf("taps = %+v, want music", task.Taps())
	}
	if tap.Domain != shape.DomainFrame || tap.MediaKind != av.MediaAudio || tap.Node != "mix-decode-a" {
		t.Fatalf("tap = %+v, want frame audio tap on mix-decode-a", tap)
	}

	// A runtime branch attached from the arm tap receives the DECODED music.
	var tapped [][]int16
	attachment, err := task.Attach(ctx, Branch("monitor").
		From(FrameTap("music")).
		To(joinTestCollectSink("monitor", &tapped)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = task.Detach(context.Background(), attachment) }()

	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(main) != 1 || !reflect.DeepEqual(main[0], []int16{150, 150}) {
		t.Fatalf("mixed = %v, want [[150 150]] (decoded music + live)", main)
	}
	if len(tapped) != 1 || !reflect.DeepEqual(tapped[0], []int16{100, 200}) {
		t.Fatalf("tap branch = %v, want [[100 200]] (pre-mix decoded music)", tapped)
	}
	if decoder.decodes != 1 {
		t.Fatalf("decoder ran %d times, want exactly once", decoder.decodes)
	}
}

// TestMixTapArmConvergesTappedStreamAgain: a tap-reference arm feeds the SAME tapped
// point into the join a second time, re-stamped under the tap name — no
// source re-opened, the mixed sum carries the stream twice.
func TestMixTapArmConvergesTappedStreamAgain(t *testing.T) {
	ctx := context.Background()
	var got [][]int16
	task, err := Mix(
		From(mixTestAudioSource("a", 100, 200)).Audio().Tap(FrameTap("dry")),
		FrameTap("dry"),
		From(mixTestAudioSource("live", 50, -50)).Audio(),
	).To(joinTestCollectSink("out", &got)).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !reflect.DeepEqual(got[0], []int16{250, 350}) {
		t.Fatalf("mixed = %v, want [[250 350]] (a twice + live)", got)
	}
}

// TestMixTapArmAnchorsOnNestedJoinTap: a nested join's output tap is an
// anchor like any chain tap — the outer join converges the sub-mix twice.
func TestMixTapArmAnchorsOnNestedJoinTap(t *testing.T) {
	ctx := context.Background()
	var got [][]int16
	task, err := Mix(
		Mix(
			From(mixTestAudioSource("a", 100, 200)).Audio(),
			From(mixTestAudioSource("b", 50, -50)).Audio(),
		).Tap(FrameTap("sub")),
		FrameTap("sub"),
		From(mixTestAudioSource("c", 1, 1)).Audio(),
	).To(joinTestCollectSink("out", &got)).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	// sub = a+b = [150 150]; outer = sub + sub(tap arm) + c = [301 301].
	if len(got) != 1 || !reflect.DeepEqual(got[0], []int16{301, 301}) {
		t.Fatalf("mixed = %v, want [[301 301]] (sub-mix twice + c)", got)
	}
	if _, ok := findTap(task.Taps(), "sub"); !ok {
		t.Fatalf("taps = %+v, want sub", task.Taps())
	}
}

// TestJoinDescribeEqualsBuildTapArm pins Describe() ≡ Build() for the tap-arm
// shape: the planned restamp node hangs off the tap's node and feeds the join
// alongside the declaring arm.
func TestJoinDescribeEqualsBuildTapArm(t *testing.T) {
	job := Mix(
		From(mixTestAudioSource("a", 100, 200)).Audio().Tap(FrameTap("dry")),
		FrameTap("dry"),
		From(mixTestAudioSource("live", 50, -50)).Audio(),
	).To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil })))

	planned := joinPlanGuard(t, job)
	text := specText(planned)
	for _, want := range []string{
		"a -> mix-tap-dry",   // the restamp anchors on the tap's node
		"mix-tap-dry -> mix", // and converges into the join
		"a -> mix",           // while the declaring arm keeps feeding it directly
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("planned tap-arm spec missing %q:\n%s", want, text)
		}
	}
}

// TestMixTapArmSyncByPTS: a tap arm participates in PTS alignment like any
// arm — its restamped frames carry the tapped PTS, so the doubled arm joins
// exactly the steps its source defines.
func TestMixTapArmSyncByPTS(t *testing.T) {
	ctx := context.Background()
	var got [][]int16
	var gotPTS []int64
	sink := Sink(SinkFunc("out", func(_ context.Context, m Message) error {
		if m.Kind == pipeline.MessageFrame && m.Frame != nil && m.Frame.Audio != nil {
			got = append(got, mixTestReadS16(m.Frame))
			gotPTS = append(gotPTS, m.Frame.PTS.Value)
		}
		return nil
	}))

	task, err := Mix(
		From(mixSyncTestSource("a", []int64{0, 20}, [][]int16{{10, 10}, {20, 20}})).Audio().Tap(FrameTap("dry")),
		FrameTap("dry"),
		From(mixSyncTestSource("b", []int64{20, 40}, [][]int16{{1, 1}, {2, 2}})).Audio(),
	).SyncByPTS().To(sink).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}

	// t=0: a+dry, t=20: a+dry+b, t=40: b solo.
	want := [][]int16{{20, 20}, {41, 41}, {2, 2}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mixed = %v, want %v", got, want)
	}
	if wantPTS := []int64{0, 20, 40}; !reflect.DeepEqual(gotPTS, wantPTS) {
		t.Fatalf("pts = %v, want %v", gotPTS, wantPTS)
	}
}

// TestCompositeTapArmPaintsTappedStreamTwice is the video variant: one tapped
// decoded stream painted at two canvas regions (decoded once) next to a fresh
// source.
func TestCompositeTapArmPaintsTappedStreamTwice(t *testing.T) {
	ctx := context.Background()
	var got []*av.Frame
	sink := Sink(SinkFunc("out", func(_ context.Context, m Message) error {
		if m.Kind == pipeline.MessageFrame && m.Frame != nil && m.Frame.Video != nil {
			got = append(got, cloneMixTestFrame(m.Frame))
		}
		return nil
	}))

	task, err := Composite(
		From(compositeTestVideoSource("cam", 4, 4, 100, 10, 20)).Video().Tap(FrameTap("cam.frames")).Region(0, 0),
		FrameTap("cam.frames").Region(4, 0),
		From(compositeTestVideoSource("fresh", 4, 4, 200, 30, 40)).Video().Region(0, 4),
	).To(sink).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("composited frames = %d, want 1", len(got))
	}
	out := got[0]
	if out.Video.Width != 8 || out.Video.Height != 8 {
		t.Fatalf("canvas = %dx%d, want 8x8", out.Video.Width, out.Video.Height)
	}
	if y := compositeTestYAt(out, 0, 0); y != 100 {
		t.Fatalf("Y at (0,0) = %d, want 100 (cam)", y)
	}
	if y := compositeTestYAt(out, 4, 0); y != 100 {
		t.Fatalf("Y at (4,0) = %d, want 100 (the SAME cam frames via the tap arm)", y)
	}
	if y := compositeTestYAt(out, 0, 4); y != 200 {
		t.Fatalf("Y at (0,4) = %d, want 200 (fresh source)", y)
	}
}

// TestMixTapArmUnknownTapListsDeclaredTaps: an unresolvable tap ref is
// refused before any source opens, listing the taps the join declares.
func TestMixTapArmUnknownTapListsDeclaredTaps(t *testing.T) {
	_, err := Mix(
		From(mixTestAudioSource("a", 1)).Audio().Tap(FrameTap("dry")),
		FrameTap("nope"),
	).To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		Build(context.Background())

	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "mix_tap_arm" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want mix_tap_arm wrapping ErrUnsupportedBuild", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, "tap=nope") || !strings.Contains(msg, "declared=dry") {
		t.Fatalf("err = %v, want the declared taps listed", err)
	}
}

func TestCompositeTapArmUnknownTapListsDeclaredTaps(t *testing.T) {
	_, err := Composite(
		From(compositeTestVideoSource("cam", 4, 4, 100, 10, 20)).Video().Tap(FrameTap("cam.frames")).Region(0, 0),
		FrameTap("missing").Region(4, 0),
	).To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		Build(context.Background())

	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "composite_tap_arm" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want composite_tap_arm wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "tap=missing") ||
		!strings.Contains(err.Error(), "declared=cam.frames") {
		t.Fatalf("err = %v, want composite tap candidates listed", err)
	}
}

func TestSelectTapArmUnknownTapListsDeclaredTaps(t *testing.T) {
	_, err := Select(
		From(selectTestOneShotSource("a", 1)).Audio().Tap(FrameTap("selected.frames")),
		FrameTap("missing"),
	).To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		Build(context.Background())

	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "select_tap_arm" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want select_tap_arm wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "tap=missing") ||
		!strings.Contains(err.Error(), "declared=selected.frames") {
		t.Fatalf("err = %v, want select tap candidates listed", err)
	}
}

// TestMixTapArmMustFollowDeclaringArm: a tap arm resolves strictly to an
// EARLIER arm — referencing a tap declared later is refused with the same
// candidates listing (and the ordering fix).
func TestMixTapArmMustFollowDeclaringArm(t *testing.T) {
	_, err := Mix(
		FrameTap("dry"),
		From(mixTestAudioSource("a", 1)).Audio().Tap(FrameTap("dry")),
	).To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		Build(context.Background())

	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "mix_tap_arm" {
		t.Fatalf("err = %v, want mix_tap_arm", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, "declared=dry") || !strings.Contains(msg, "before the tap arm") {
		t.Fatalf("err = %v, want the ordering suggestion with dry listed", err)
	}
}

// TestMixTapArmRejectsDomainMismatch: a typed PacketTap arm cannot attach to
// a frame-domain tap.
func TestMixTapArmRejectsDomainMismatch(t *testing.T) {
	_, err := Mix(
		From(mixTestAudioSource("a", 1)).Audio().Tap(FrameTap("dry")),
		PacketTap("dry"),
	).To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		Build(context.Background())

	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "tap_domain_mismatch" {
		t.Fatalf("err = %v, want tap_domain_mismatch", err)
	}
}

// TestMixArmRejectsUnsupportedChainOperations: arm chains support .Decode()
// and .Tap(...) — anything else used to be silently dropped and is now an
// error naming the alternatives.
func TestMixArmRejectsUnsupportedChainOperations(t *testing.T) {
	_, err := Mix(
		From(mixTestAudioSource("a", 1)).Audio().Resample(48000, 1),
		From(mixTestAudioSource("b", 1)).Audio(),
	).To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		Build(context.Background())

	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "mix_arm" {
		t.Fatalf("err = %v, want mix_arm", err)
	}
	if !strings.Contains(err.Error(), ".Decode() and .Tap(...)") {
		t.Fatalf("err = %v, want the supported arm operations named", err)
	}
}

// TestMixArmChainErrorSurfaces: a builder error recorded on an arm chain (an
// empty tap name here) fails the join compile instead of being dropped.
func TestMixArmChainErrorSurfaces(t *testing.T) {
	_, err := Mix(
		From(mixTestAudioSource("a", 1)).Audio().Tap(Tap("")),
		From(mixTestAudioSource("b", 1)).Audio(),
	).To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		Build(context.Background())

	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "tap_invalid" {
		t.Fatalf("err = %v, want tap_invalid surfaced from the arm chain", err)
	}
}

func TestJoinTapAnchorHelperContracts(t *testing.T) {
	owner := &joinPlan{name: "mix"}
	anchor := joinTapAnchor{
		owner:  owner,
		arm:    -1,
		domain: shape.DomainFrame,
		stream: av.Stream{ID: "audio", Type: av.MediaAudio},
	}
	anchors := newJoinTapAnchors([]string{"dry"})
	anchors.declare("", anchor)
	if len(anchors.entries) != 0 {
		t.Fatalf("empty tap declaration changed entries: %+v", anchors.entries)
	}
	anchors.declare("dry", anchor)
	anchors.declare("dry", joinTapAnchor{owner: owner, arm: -1, domain: shape.DomainFrame, stream: av.Stream{Type: av.MediaVideo}})
	resolved, ok := anchors.resolve("dry")
	if !ok || resolved.stream.Type != av.MediaAudio || resolved.node() != "mix" {
		t.Fatalf("resolved anchor = %+v, %v; want first audio anchor on mix", resolved, ok)
	}
	if _, ok := anchors.resolve("missing"); ok {
		t.Fatal("resolve unexpectedly found missing tap")
	}

	_, err := planJoinTapArm("mix", "mix", joinProfile{media: av.MediaAudio}, tapRef{}, anchors)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "mix_arm" || !strings.Contains(buildErr.Reason, "tap arm has no name") {
		t.Fatalf("empty tap arm err = %v", err)
	}

	_, err = planJoinTapArm("mix", "mix", joinProfile{media: av.MediaVideo}, FrameTap("dry"), anchors)
	if !errors.As(err, &buildErr) || buildErr.Code != "mix_arm" || !strings.Contains(buildErr.Reason, "carries audio, want video") {
		t.Fatalf("media mismatch err = %v", err)
	}
}

func TestJoinTapHelperCollectionContracts(t *testing.T) {
	if got := chainArmOperations(nil); got != nil {
		t.Fatalf("nil chain operations = %+v, want nil", got)
	}
	if got := chainArmOperations(&jobStreamBuilder{}); got != nil {
		t.Fatalf("empty chain operations = %+v, want nil", got)
	}

	_, err := joinChainArmTaps("mix", "arm", []operationSpec{{
		Kind: plan.OpTap,
		Tap:  tapIntent{Name: "packets", Domain: shape.DomainPacket},
	}})
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "tap_domain_mismatch" {
		t.Fatalf("packet tap collection err = %v, want tap_domain_mismatch", err)
	}

	root := Mix(
		nil,
		&jobStreamBuilder{stream: &jobStreamBuild{operations: []operationSpec{{Kind: plan.OpTap, Tap: tapIntent{Name: ""}}}}},
		From(mixTestAudioSource("a", 1)).Audio().Tap(FrameTap("dry")),
		From(mixTestAudioSource("b", 1)).Audio().Tap(FrameTap("dry")),
		Mix(
			From(mixTestAudioSource("c", 1)).Audio(),
			From(mixTestAudioSource("d", 1)).Audio(),
		).Tap(FrameTap("sub")),
	).Tap(FrameTap("root")).joinArm().join
	names := declaredJoinTapNames(root)
	if !reflect.DeepEqual(names, []string{"dry", "sub"}) {
		t.Fatalf("declared tap names = %v, want [dry sub] without root duplicate", names)
	}

	err = joinTapArmMissingError("mix", FrameTap("ghost"), nil)
	if !errors.As(err, &buildErr) || buildErr.Code != "mix_tap_arm" || !strings.Contains(err.Error(), "declared taps: none") {
		t.Fatalf("missing tap no candidates err = %v", err)
	}
}

func TestTapArmStageRestampContracts(t *testing.T) {
	stage := newTapArmStage("mix-tap-dry", "dry")
	if stage.Name() != "mix-tap-dry" {
		t.Fatalf("Name() = %q", stage.Name())
	}
	if spec := stage.DescribeNode(); spec.Name != "mix-tap-dry" || spec.Kind != pipeline.NodeStage || !strings.Contains(spec.Detail, "tap=dry") {
		t.Fatalf("DescribeNode() = %+v", spec)
	}
	emit := &tapArmStageTestEmitter{}
	ctx := context.Background()

	for _, msg := range []*pipeline.Message{
		{Kind: pipeline.MessageFrame},
		{Kind: pipeline.MessagePacket},
		{Kind: pipeline.MessageEvent},
	} {
		if err := stage.Handle(ctx, msg, emit); err != nil {
			t.Fatalf("Handle(%+v) error = %v", msg, err)
		}
	}
	if len(emit.messages) != 0 {
		t.Fatalf("nil payload messages emitted: %+v", emit.messages)
	}

	frame := &av.Frame{StreamID: "camera", Type: av.MediaVideo, Video: &av.VideoFrame{Width: 1, Height: 1}}
	if err := stage.Handle(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: frame}, emit); err != nil {
		t.Fatalf("frame error = %v", err)
	}
	if got := emit.frames[len(emit.frames)-1]; got.StreamID != "dry" || frame.StreamID != "camera" {
		t.Fatalf("restamped frame = %+v, original = %+v", got, frame)
	}

	packet := &av.Packet{StreamID: "camera", Type: av.MediaVideo}
	if err := stage.Handle(ctx, &pipeline.Message{Kind: pipeline.MessagePacket, Packet: packet}, emit); err != nil {
		t.Fatalf("packet error = %v", err)
	}
	if got := emit.packets[len(emit.packets)-1]; got.StreamID != "dry" || packet.StreamID != "camera" {
		t.Fatalf("restamped packet = %+v, original = %+v", got, packet)
	}

	selectorEvent := &av.Event{Type: av.EventStats, StreamID: "camera_b", Reason: selectorActiveReason}
	if err := stage.Handle(ctx, &pipeline.Message{Kind: pipeline.MessageEvent, Event: selectorEvent}, emit); err != nil {
		t.Fatalf("selector event error = %v", err)
	}
	if got := emit.events[len(emit.events)-1]; got.StreamID != "camera_b" || got.Reason != selectorActiveReason {
		t.Fatalf("selector event = %+v, want unchanged target", got)
	}

	ordinary := &av.Event{Type: av.EventEndOfStream, StreamID: "camera"}
	if err := stage.Handle(ctx, &pipeline.Message{Kind: pipeline.MessageEvent, Event: ordinary}, emit); err != nil {
		t.Fatalf("ordinary event error = %v", err)
	}
	if got := emit.events[len(emit.events)-1]; got.StreamID != "dry" || ordinary.StreamID != "camera" {
		t.Fatalf("ordinary event = %+v, original = %+v", got, ordinary)
	}

	custom := &pipeline.Message{Kind: pipeline.MessageKind("sideband")}
	if err := stage.Handle(ctx, custom, emit); err != nil {
		t.Fatalf("sideband error = %v", err)
	}
	if emit.messages[len(emit.messages)-1] != custom {
		t.Fatal("sideband message was not forwarded unchanged")
	}
	if err := stage.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}
}

type tapArmStageTestEmitter struct {
	messages []*pipeline.Message
	frames   []*av.Frame
	packets  []*av.Packet
	events   []*av.Event
}

func (e *tapArmStageTestEmitter) Emit(_ context.Context, msg *pipeline.Message) error {
	e.messages = append(e.messages, msg)
	switch msg.Kind {
	case pipeline.MessageFrame:
		e.frames = append(e.frames, msg.Frame)
	case pipeline.MessagePacket:
		e.packets = append(e.packets, msg.Packet)
	case pipeline.MessageEvent:
		e.events = append(e.events, msg.Event)
	}
	return nil
}
