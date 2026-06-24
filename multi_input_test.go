package goav

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/shape"
)

// TestFromMultiInputChainsShareOneDestination is north-star acceptance #23:
// two inputs, two stream chains, ONE shared Destination — one mux group
// receiving both encoded streams, no expert graph.
func TestFromMultiInputChainsShareOneDestination(t *testing.T) {
	ctx := context.Background()
	muxers := &remuxTestMuxerFactory{}
	rt := MustNew(
		withTestFormats(testFormatMuxer(av.FormatOgg, muxers)),
		WithEncoder(codec.Descriptor{ID: av.CodecVP9, Type: av.MediaVideo}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		WithEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
	)

	out := File("call.ogg", io.Discard, Format(av.FormatOgg))
	task, err := From(
		compositeTestVideoSource("camera", 4, 4, 100, 10, 20),
		mixTestAudioSource("mic", 100, 200),
	).UseRuntime(rt).
		Video().Encode(codec.VP9(codec.Bitrate(1_000_000))).To(out).
		Audio().Encode(codec.Opus(codec.Bitrate(96_000))).To(out).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	spec := task.Describe()
	muxNodes := 0
	for _, node := range spec.Nodes {
		if node.Name == "call.ogg" {
			muxNodes++
		}
	}
	if muxNodes != 1 {
		t.Fatalf("mux nodes named call.ogg = %d, want exactly 1; nodes=%+v", muxNodes, spec.Nodes)
	}
	intoMux := map[string]bool{}
	for _, edge := range spec.Edges {
		if edge.To == "call.ogg" {
			intoMux[edge.From.String()] = true
		}
	}
	if len(intoMux) != 2 || !intoMux["encode-video"] || !intoMux["encode-audio"] {
		t.Fatalf("edges into mux = %v, want encode-video and encode-audio", intoMux)
	}

	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(muxers.muxers) != 1 {
		t.Fatalf("muxers = %d, want one shared mux group", len(muxers.muxers))
	}
	muxer := muxers.muxers[0]
	if muxer.streamCount != 2 {
		t.Fatalf("mux opened with %d streams (%v), want 2", muxer.streamCount, muxer.openedStreams)
	}
	written := map[av.StreamID]bool{}
	for _, id := range muxer.writtenStreams {
		written[id] = true
	}
	if len(written) != 2 {
		t.Fatalf("mux wrote streams %v, want packets from both chains", muxer.writtenStreams)
	}
}

// TestFromMultiInputHeadlineDecodeShape runs the acceptance snippet verbatim
// in shape: packet-domain camera and mic sources, per-chain Decode+Encode,
// one shared webm destination.
func TestFromMultiInputHeadlineDecodeShape(t *testing.T) {
	ctx := context.Background()
	cameraCodec := av.CodecID("x_cam")
	micCodec := av.CodecID("x_mic")
	packetSource := func(id av.StreamID, media av.MediaType, codecID av.CodecID, opts ...shape.Option) InputSpec {
		options := append([]shape.Option{shape.Stream(id)}, opts...)
		return Source(string(id),
			shape.Packet(media, codecID, options...),
			func(_ context.Context, push SourcePush) error {
				if _, err := push.Packet(&av.Packet{StreamID: id, Type: media, Payload: av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable}}); err != nil {
					return err
				}
				return push.EOS()
			})
	}
	muxers := &remuxTestMuxerFactory{}
	rt := MustNew(
		withTestFormats(testFormatMuxer(av.FormatWebM, muxers)),
		WithDecoder(codec.Descriptor{ID: cameraCodec, Type: av.MediaVideo}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
		WithDecoder(codec.Descriptor{ID: micCodec, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
		WithEncoder(codec.Descriptor{ID: av.CodecVP9, Type: av.MediaVideo}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		WithEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
	)

	out := File("call.webm", io.Discard, Format(av.FormatWebM))
	err := From(
		packetSource("camera", av.MediaVideo, cameraCodec, shape.Video(4, 4, av.PixelFormatI420)),
		packetSource("mic", av.MediaAudio, micCodec, shape.Audio(48000, codec.Mono, av.SampleFormatS16)),
	).UseRuntime(rt).
		Video().Decode().Encode(codec.VP9(codec.Bitrate(1_000_000))).To(out).
		Audio().Decode().Encode(codec.Opus(codec.Bitrate(96_000))).To(out).
		Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(muxers.muxers) != 1 || muxers.muxers[0].streamCount != 2 {
		t.Fatalf("muxers = %+v, want one mux group opened with both streams", muxers.muxers)
	}
}

// TestFromMultiInputAmbiguousSelectionListsCandidates moved to
// goavtest_dogfood_test.go: it is now the consumer-side multi-input ambiguity
// acceptance test written against goavtest.

func TestFromMultiInputUnknownInputNameListsInputs(t *testing.T) {
	_, err := From(
		mixTestAudioSource("mic-a", 1),
		mixTestAudioSource("mic-b", 1),
	).
		Audio(InputName("microphone")).
		To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		Build(context.Background())

	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "input_unknown" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want input_unknown wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "input=mic-a") || !strings.Contains(err.Error(), "input=mic-b") {
		t.Fatalf("err = %v, want available input names listed", err)
	}
}

func TestFromMultiInputInputNameNarrowsChains(t *testing.T) {
	ctx := context.Background()
	var gotA, gotB [][]int16
	task, err := From(
		mixTestAudioSource("mic-a", 11, 22),
		mixTestAudioSource("mic-b", 33, 44),
	).
		Audio(InputName("mic-a")).To(joinTestCollectSink("sink-a", &gotA)).
		Audio(InputName("mic-b")).To(joinTestCollectSink("sink-b", &gotB)).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(gotA) != 1 || gotA[0][0] != 11 || gotA[0][1] != 22 {
		t.Fatalf("sink-a frames = %v, want mic-a samples [11 22]", gotA)
	}
	if len(gotB) != 1 || gotB[0][0] != 33 || gotB[0][1] != 44 {
		t.Fatalf("sink-b frames = %v, want mic-b samples [33 44]", gotB)
	}
}

// Single chain across two inputs: exactly one matching stream just works
// without narrowing — the audio chain binds to the only audio input.
func TestFromMultiInputUnambiguousChainJustWorks(t *testing.T) {
	ctx := context.Background()
	var got [][]int16
	task, err := From(
		compositeTestVideoSource("camera", 4, 4, 100, 10, 20),
		mixTestAudioSource("mic", 5, 6),
	).
		Audio().To(joinTestCollectSink("sink", &got)).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0][0] != 5 || got[0][1] != 6 {
		t.Fatalf("sink frames = %v, want mic samples [5 6]", got)
	}
}

func TestJoinArmsRejectMultiInputChains(t *testing.T) {
	_, err := Mix(
		From(mixTestAudioSource("a", 1), mixTestAudioSource("b", 1)).Audio(),
		From(mixTestAudioSource("c", 1)).Audio(),
	).To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		Build(context.Background())

	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "mix_arm" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want mix_arm wrapping ErrUnsupportedBuild", err)
	}
}

func TestBranchesRejectMultiInputJobs(t *testing.T) {
	_, err := From(
		mixTestAudioSource("a", 1),
		mixTestAudioSource("b", 1),
	).
		Audio(InputName("a")).
		Branches(Branch("mon").To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil })))).
		Build(context.Background())

	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "input_count_unsupported" {
		t.Fatalf("err = %v, want input_count_unsupported", err)
	}
}

// Shared-destination dedupe only merges the SAME handle; two different
// handles colliding on one label is an error.
func TestFromMultiInputRejectsConflictingDestinationHandles(t *testing.T) {
	first := File("call.ogg", io.Discard, Format(av.FormatOgg))
	second := File("call.ogg", io.Discard, Format(av.FormatOgg))
	_, err := From(
		compositeTestVideoSource("camera", 4, 4, 100, 10, 20),
		mixTestAudioSource("mic", 1),
	).
		Video().Encode(codec.VP9()).To(first).
		Audio().Encode(codec.Opus()).To(second).
		Build(context.Background())

	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_duplicate" {
		t.Fatalf("err = %v, want destination_duplicate", err)
	}
}

// The Plan intent for a multi-chain job lists every chain and dedupes the
// shared destination to one entry (one mux group).
func TestFromMultiInputPlanDedupesSharedDestination(t *testing.T) {
	out := File("call.ogg", io.Discard, Format(av.FormatOgg))
	job := From(
		compositeTestVideoSource("camera", 4, 4, 100, 10, 20),
		mixTestAudioSource("mic", 1),
	).
		Video().Encode(codec.VP9()).To(out).
		Audio().Encode(codec.Opus()).To(out)
	intent := job.plan()
	if len(intent.Inputs) != 2 || len(intent.Streams) != 2 {
		t.Fatalf("inputs=%d streams=%d, want 2 and 2", len(intent.Inputs), len(intent.Streams))
	}
	if len(intent.Destinations) != 1 || intent.Destinations[0].Name != "call.ogg" {
		t.Fatalf("destinations = %+v, want one shared call.ogg", intent.Destinations)
	}
	if intent.Streams[0].Name != "video" || intent.Streams[1].Name != "audio" {
		t.Fatalf("stream names = %q, %q", intent.Streams[0].Name, intent.Streams[1].Name)
	}
	for i := range intent.Streams {
		if len(intent.Streams[i].Destinations) != 1 || intent.Streams[i].Destinations[0] != "call.ogg" {
			t.Fatalf("stream %d destinations = %v", i, intent.Streams[i].Destinations)
		}
	}

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	muxNodes := 0
	for _, node := range spec.Nodes {
		if node.Name == "call.ogg" {
			muxNodes++
		}
	}
	if muxNodes != 1 {
		t.Fatalf("described mux nodes = %d, want one shared mux group; nodes=%+v", muxNodes, spec.Nodes)
	}
}
