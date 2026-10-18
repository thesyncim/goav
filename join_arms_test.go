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

// These tests pin join arm chains: an arm keeps its declared .Decode() and
// .Tap(...) (the tap installs on the task, anchored mid-graph), and refuses
// every other operation instead of dropping it.

// TestMixChainArmTapDecodesOnceAndMixes is the headline: decode a packet
// chain ONCE, tap the decoded point mid-graph, and mix it with a live frame
// source in one task. The tap is a first-class task tap (runtime branches
// attach to the decoded music), the topology holds exactly one decode node,
// and the sink receives the mixed sum.
func TestMixChainArmTapDecodesOnceAndMixes(t *testing.T) {
	ctx := context.Background()
	pcm := av.CodecID("x_pcm_mono")
	decoder := &tapArmTestDecoder{}
	rt := mustNew(WithDecoder(
		codec.Descriptor{ID: pcm, Name: "PCM mono", Type: av.MediaAudio, Capabilities: codec.Capabilities{SampleFormats: []string{av.SampleFormatS16}}},
		tapArmTestDecoderFactory{decoder: decoder},
	))

	var main [][]int16
	task, err := legacyMix(
		recipeFrom(tapArmTestPacketSource("a", pcm, 100, 200)).Audio().Decode().Tap(frameTapRef("music")),
		recipeFrom(mixTestAudioSource("live", 50, -50)).Audio(),
	).To(joinTestCollectSink("out", &main)).UseRuntime(rt).BuildLive(ctx)
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
	attachment, err := legacyAttach(task, ctx, newBranch("monitor").
		From(frameTapRef("music")).
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

// TestMixArmRejectsUnsupportedChainOperations: arm chains support .Decode()
// and .Tap(...) — anything else used to be silently dropped and is now an
// error naming the alternatives.
func TestMixArmRejectsUnsupportedChainOperations(t *testing.T) {
	_, err := legacyMix(
		recipeFrom(mixTestAudioSource("a", 1)).Audio().Resample(48000, 1),
		recipeFrom(mixTestAudioSource("b", 1)).Audio(),
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
	_, err := legacyMix(
		recipeFrom(mixTestAudioSource("a", 1)).Audio().Tap(frameTapRef("")),
		recipeFrom(mixTestAudioSource("b", 1)).Audio(),
	).To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		Build(context.Background())

	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "tap_invalid" {
		t.Fatalf("err = %v, want tap_invalid surfaced from the arm chain", err)
	}
}

func TestArmRestampStageContracts(t *testing.T) {
	stage := newArmRestampStage("mix-tap-dry", "restamp, tap=dry", "", "dry")
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

	// A bound file arm passes only its stream and stream-less events.
	bound := newArmRestampStage("mix-select-a.webm", bindArmNodeDetail(av.StreamSelector{Type: av.MediaAudio}), "2", "a.webm")
	filtered := &tapArmStageTestEmitter{}
	for _, msg := range []*pipeline.Message{
		{Kind: pipeline.MessagePacket, Packet: &av.Packet{StreamID: "1"}},
		{Kind: pipeline.MessagePacket, Packet: &av.Packet{StreamID: "2"}},
		{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventStreamAdded, StreamID: "1"}},
		{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventStreamAdded, StreamID: "2", Stream: &av.Stream{ID: "2"}}},
		{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventEndOfStream}},
	} {
		if err := bound.Handle(ctx, msg, filtered); err != nil {
			t.Fatal(err)
		}
	}
	if len(filtered.packets) != 1 || filtered.packets[0].StreamID != "a.webm" {
		t.Fatalf("bound packets = %+v, want stream 2 as a.webm", filtered.packets)
	}
	if len(filtered.events) != 2 || filtered.events[0].Stream.ID != "a.webm" || filtered.events[1].StreamID != "a.webm" {
		t.Fatalf("bound events = %+v, want stream 2's and the stream-less end, as a.webm", filtered.events)
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

// TestStreamSharedByTwoJoinsDecodesOnce pins the once-per-Stream law for join
// arms: one decoded Stream feeding two joins plans one decoder, and both
// joins read it.
func TestStreamSharedByTwoJoinsDecodesOnce(t *testing.T) {
	pcm := av.CodecID("x_pcm_mono")
	decoder := &tapArmTestDecoder{}
	rt := mustNew(WithDecoder(
		codec.Descriptor{ID: pcm, Name: "PCM mono", Type: av.MediaAudio, Capabilities: codec.Capabilities{SampleFormats: []string{av.SampleFormatS16}}},
		tapArmTestDecoderFactory{decoder: decoder},
	))
	music := tapArmTestPacketSource("music", pcm, 100, 200).Audio().Decode().Tap("music.frames")
	var first, second [][]int16
	outs := []Output{
		Mix(music, mixTestAudioSource("a", 1, 1).Audio()).To(joinTestCollectSink("first", &first)),
		Mix(music, mixTestAudioSource("b", 2, 2).Audio()).To(joinTestCollectSink("second", &second)),
	}
	task, err := rt.Build(context.Background(), outs...)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	decodes := 0
	for _, node := range task.Describe().Nodes {
		if strings.Contains(node.Name, "decode") {
			decodes++
		}
	}
	if decodes != 1 {
		t.Fatalf("decode nodes = %d, want 1:\n%s", decodes, specText(task.Describe()))
	}
	taps := 0
	for _, tap := range task.Taps() {
		if tap.Name == "music.frames" {
			taps++
		}
	}
	if taps != 1 {
		t.Fatalf("taps = %+v, want music.frames once", task.Taps())
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 {
		t.Fatalf("decoder ran %d times, want once", decoder.decodes)
	}
	if !reflect.DeepEqual(first, [][]int16{{101, 201}}) || !reflect.DeepEqual(second, [][]int16{{102, 202}}) {
		t.Fatalf("mixed first=%v second=%v, want the decoded music in both", first, second)
	}
}
