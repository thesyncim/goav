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
	"github.com/thesyncim/goav/shape"
)

// These tests pin the tap-arm contract: a chain arm keeps its declared
// .Decode()/.Tap(...) (the tap installs on the task, anchored mid-graph), and
// a TapRef arm converges that already-flowing point again — the join-side
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
	rt := New(WithDecoder(
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

// TestMixTapArmConvergesTappedStreamAgain: a TapRef arm feeds the SAME tapped
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
