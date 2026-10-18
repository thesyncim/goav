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
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/snapshot"
)

// These pin refusals and deferrals the compiler-pass contract tests covered
// before the one compiler, through the value grammar a user writes.

func discardSink(name string) Destination {
	return Sink(SinkFunc(name, func(context.Context, Message) error { return nil }))
}

// TestDecodePreflightDefersAmbiguousSelection pins that a decode of an
// ambiguous selection refuses as the ambiguity, not as a missing decoder:
// the decoder check waits for the stream the selection resolves to.
func TestDecodePreflightDefersAmbiguousSelection(t *testing.T) {
	streams := []av.Stream{
		{Index: 0, ID: "eng", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}},
		{Index: 1, ID: "spa", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}},
	}
	rt := mustNew(withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, remuxTestDemuxerFactory{demuxer: &remuxTestDemuxer{streams: streams}}),
	))
	_, err := rt.Build(context.Background(), FileInput("in.ogg", strings.NewReader("")).Audio().Decode().To(discardSink("frames")))
	assertBuildErrorCode(t, err, streamAmbiguousCode)
}

// TestPacketCopyOfALiveInputNeedsNoDecoder pins that adapter preflight skips
// a branch that never decodes: a live packet input copies on a runtime with
// no decoder for it.
func TestPacketCopyOfALiveInputNeedsNoDecoder(t *testing.T) {
	task, err := mustNew().Build(context.Background(), Input(liveVideoVP8Provider("video")).Video().To(discardSink("packets")))
	if err != nil {
		t.Fatalf("copy of a live input: %v", err)
	}
	task.Close()
}

// TestNestedJoinArmRefusesEncode pins that a nested join's arm carries no
// encoder: the outer join converges frames.
func TestNestedJoinArmRefusesEncode(t *testing.T) {
	inner := Mix(mixTestAudioSource("a", 1).Audio(), mixTestAudioSource("b", 2).Audio()).Encode(codec.Opus())
	_, err := mustNew().Describe(Mix(inner, mixTestAudioSource("c", 3).Audio()).To(discardSink("out")))
	assertBuildErrorCode(t, err, errcode.Code("mix_arm"))
	if !strings.Contains(err.Error(), "encode the outer join instead") {
		t.Fatalf("err = %v, want move-the-encoder guidance", err)
	}
}

// TestStreamRulesNeedOneInput pins that OnStream rules run on a recipe of one
// input: the rule's source is that input.
func TestStreamRulesNeedOneInput(t *testing.T) {
	a := mixTestAudioSource("a", 1)
	b := mixTestAudioSource("b", 2)
	_, err := mustNew().Describe(a.OnStream(MatchMedia(av.MediaAudio)).To(discardSink("late")), b.Audio().To(discardSink("b")))
	assertBuildErrorCode(t, err, errcode.StreamRuleInvalid)
	if !strings.Contains(err.Error(), "exactly one input") {
		t.Fatalf("err = %v, want the one-input rule", err)
	}
}

// TestOneDestinationListedTwiceIsRefused pins .To(w, w) for build-time
// streams, joins, and attached Outputs alike.
func TestOneDestinationListedTwiceIsRefused(t *testing.T) {
	w := Write("out.ogg", io.Discard, Format(av.FormatOgg))
	_, err := mustNew().Describe(mixTestAudioSource("a", 1).Audio().To(w, w))
	assertBuildErrorCode(t, err, destinationDuplicateCode)
	_, err = mustNew().Describe(Mix(mixTestAudioSource("a", 1).Audio(), mixTestAudioSource("b", 2).Audio()).To(w, w))
	assertBuildErrorCode(t, err, destinationDuplicateCode)

	task, err := mustNew().Build(context.Background(), mixTestAudioSource("live", 1).Audio().Tap("frames").To(discardSink("main")))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	sink := discardSink("twice")
	if _, err := task.Attach(context.Background(), task.Tap("frames").To(sink, sink)); err == nil {
		t.Fatal("attach .To(sink, sink) = nil, want a refusal")
	} else {
		assertBuildErrorCode(t, err, destinationDuplicateCode)
	}
}

// TestAttachMediaMismatchRefusesBeforeMutation pins that an attached audio
// step on a video tap refuses before the graph changes.
func TestAttachMediaMismatchRefusesBeforeMutation(t *testing.T) {
	ctx := context.Background()
	frame := av.Frame{StreamID: "video", Type: av.MediaVideo, Video: &av.VideoFrame{Width: 640, Height: 360, PixelFormat: av.PixelFormatYUV420P}}
	source := &runtimeTestSource{name: "source", message: pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame}}
	graph := expertGraph(mustNew())
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", &runtimeTestSink{name: "base"}).In())
	built, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer built.Close()
	built.(*task).taps = []snapshot.Tap{{
		Name: "video.frames", MediaKind: av.MediaVideo, Domain: shape.DomainFrame, Node: "source",
		Shape: shape.Spec{Domain: shape.DomainFrame, MediaKind: av.MediaVideo, StreamID: "video", Width: 640, Height: 360, PixelFormat: av.PixelFormatYUV420P},
	}}
	before := built.Describe()
	_, err = built.Attach(ctx, built.Tap("video.frames").Resample(16_000, codec.Mono).As("voice").To(discardSink("voice")))
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != operationShapeMismatchCode || buildErr.Operation != "attach runtime branch" {
		t.Fatalf("err = %v, want operation_shape_mismatch from attach", err)
	}
	if after := built.Describe(); !reflect.DeepEqual(before, after) {
		t.Fatalf("graph changed after a refused attach:\nbefore:\n%s\nafter:\n%s", specText(before), specText(after))
	}
}

// TestRuntimeBranchTapAnchorsUseStableNames pins what an attached Output
// anchors on: a tap by its name alone, a graph route by its node alone.
func TestRuntimeBranchTapAnchorsUseStableNames(t *testing.T) {
	built, err := mustNew().Build(context.Background(), mixTestAudioSource("mic", 1).Audio().Tap("audio.frames").To(discardSink("main")))
	if err != nil {
		t.Fatal(err)
	}
	defer built.Close()
	live := built.(*task)
	branches, err := live.attachBranches([]Output{
		live.Tap("audio.frames").As("levels").To(discardSink("levels")),
		live.Route(pipeline.Route{From: "select-audio"}).As("expert").To(discardSink("expert")),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(branches) != 2 {
		t.Fatalf("branches = %+v, want two", branches)
	}
	if tap := branches[0].source; tap.tap != "audio.frames" || tap.from != "" || tap.tapDomain != shape.DomainFrame {
		t.Fatalf("tap anchor = %+v, want the tap alone, frame domain", tap)
	}
	if route := branches[1].source; route.from != "select-audio" || route.tap != "" || route.policy != pipeline.RouteAll {
		t.Fatalf("route anchor = %+v, want the node alone, all of its messages", route)
	}
}
