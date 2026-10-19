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

// TestFromMultiInputChainsShareOneMuxDestination is north-star acceptance #23:
// two inputs, two stream chains, one explicit Mux destination group receiving
// both encoded streams, no expert graph.
func TestFromMultiInputChainsShareOneMuxDestination(t *testing.T) {
	ctx := context.Background()
	muxers := &remuxTestMuxerFactory{}
	rt := mustNew(
		withTestFormats(testFormatMuxer(av.FormatOgg, muxers)),
		WithEncoder(codec.Descriptor{ID: av.CodecVP9, Type: av.MediaVideo}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		WithEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
	)

	out := Write("call.ogg", io.Discard, Format(av.FormatOgg))
	task, err := rt.Build(ctx,
		compositeTestVideoSource("camera", 4, 4, 100, 10, 20).Video().Encode(codec.VP9(codec.Bitrate(1_000_000))).To(out),
		mixTestAudioSource("mic", 100, 200).Audio().Encode(codec.Opus(codec.Bitrate(96_000))).To(out),
	)
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
				if _, err := push.Packet(&av.Packet{StreamID: id, Type: media, Payload: av.Buffer{Bytes: []byte{1}}}); err != nil {
					return err
				}
				return push.EOS()
			})
	}
	muxers := &remuxTestMuxerFactory{}
	rt := mustNew(
		withTestFormats(testFormatMuxer(av.FormatWebM, muxers)),
		WithDecoder(codec.Descriptor{ID: cameraCodec, Type: av.MediaVideo}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
		WithDecoder(codec.Descriptor{ID: micCodec, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
		WithEncoder(codec.Descriptor{ID: av.CodecVP9, Type: av.MediaVideo}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		WithEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
	)

	out := Write("call.webm", io.Discard, Format(av.FormatWebM))
	err := rt.Run(ctx,
		packetSource("camera", av.MediaVideo, cameraCodec, shape.Video(4, 4, av.PixelFormatI420)).Video().Decode().Encode(codec.VP9(codec.Bitrate(1_000_000))).To(out),
		packetSource("mic", av.MediaAudio, micCodec, shape.Audio(48000, codec.Mono, av.SampleFormatS16)).Audio().Decode().Encode(codec.Opus(codec.Bitrate(96_000))).To(out),
	)
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

func TestFromMultiInputInputNameNarrowsChains(t *testing.T) {
	ctx := context.Background()
	var gotA, gotB [][]int16
	task, err := mustNew().Build(ctx,
		mixTestAudioSource("mic-a", 11, 22).Audio().To(joinTestCollectSink("sink-a", &gotA)),
		mixTestAudioSource("mic-b", 33, 44).Audio().To(joinTestCollectSink("sink-b", &gotB)),
	)
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
	task, err := mustNew().Build(ctx, mixTestAudioSource("mic", 5, 6).Audio().To(joinTestCollectSink("sink", &got)))
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

// TestBranchesForkOneOfSeveralInputs pins forks on a job with several inputs:
// the branched chain's select reads its own input alone.
func TestBranchesForkOneOfSeveralInputs(t *testing.T) {
	sink := func(name string) Destination {
		return Sink(SinkFunc(name, func(context.Context, Message) error { return nil }))
	}
	a := mixTestAudioSource("a", 1).Audio()
	spec, err := mustNew().Describe(
		a.As("mon").To(sink("out")), a.As("rec").To(sink("rec")),
		mixTestAudioSource("b", 1).Audio().To(sink("other")),
	)
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	for _, edge := range spec.Edges {
		if edge.To == "select-audio@a" && edge.From != "a" {
			t.Fatalf("select-audio@a reads %s, want input a alone: %+v", edge.From, spec.Edges)
		}
	}
	if !specHasNode(spec, "select-audio@a") {
		t.Fatalf("nodes = %+v, want select-audio@a", spec.Nodes)
	}
}

// Shared-destination dedupe only merges the SAME handle; two different
// handles colliding on one label is an error.
func TestFromMultiInputRejectsConflictingDestinationHandles(t *testing.T) {
	first := Write("call.ogg", io.Discard, Format(av.FormatOgg))
	second := Write("call.ogg", io.Discard, Format(av.FormatOgg))
	_, err := mustNew().Build(context.Background(),
		compositeTestVideoSource("camera", 4, 4, 100, 10, 20).Video().Encode(codec.VP9()).To(first),
		mixTestAudioSource("mic", 1).Audio().Encode(codec.Opus()).To(second),
	)

	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_duplicate" {
		t.Fatalf("err = %v, want destination_duplicate", err)
	}
}

// A multi-chain job routing one explicit destination dedupes it to one mux.
func TestFromMultiInputPlanDedupesMuxDestination(t *testing.T) {
	out := Write("call.ogg", io.Discard, Format(av.FormatOgg))
	spec, err := mustNew().Describe(
		compositeTestVideoSource("camera", 4, 4, 100, 10, 20).Video().Encode(codec.VP9()).To(out),
		mixTestAudioSource("mic", 1).Audio().Encode(codec.Opus()).To(out),
	)
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

// TestWithKeepsHandleIdentity pins that With copies keep the handle's
// identity: two configured copies of one destination still feed one mux.
func TestWithKeepsHandleIdentity(t *testing.T) {
	out := Write("call.ogg", io.Discard)
	videoOut := out.With(Format(av.FormatOgg))
	audioOut := out.With(Format(av.FormatOgg))
	spec, err := mustNew().Describe(
		compositeTestVideoSource("camera", 4, 4, 100, 10, 20).Video().Encode(codec.VP9()).To(videoOut),
		mixTestAudioSource("mic", 1).Audio().Encode(codec.Opus()).To(audioOut),
	)
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
		t.Fatalf("described mux nodes = %d, want one grouped mux; nodes=%+v", muxNodes, spec.Nodes)
	}
}

// TestOneHandleWithConflictingConfigFailsClearly: copies of one handle that
// disagree on the container cannot share a mux.
func TestOneHandleWithConflictingConfigFailsClearly(t *testing.T) {
	out := Write("call.ogg", io.Discard)
	videoOut := out.With(Format(av.FormatOgg))
	audioOut := out.With(Format(av.FormatWebM))
	_, err := mustNew().Describe(
		compositeTestVideoSource("camera", 4, 4, 100, 10, 20).Video().Encode(codec.VP9()).To(videoOut),
		mixTestAudioSource("mic", 1).Audio().Encode(codec.Opus()).To(audioOut),
	)

	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != destinationDuplicateCode {
		t.Fatalf("err = %v, want destination_duplicate", err)
	}
	if !strings.Contains(err.Error(), "same destination value") {
		t.Fatalf("err = %v, want shared-handle guidance", err)
	}
}

// TestSameHandleSharesOneMux pins destination identity: reusing one handle
// from the video and audio chains writes both into one mux — the common
// "one output file" recipe needs no grouping call.
func TestSameHandleSharesOneMux(t *testing.T) {
	out := Write("call.ogg", io.Discard, Format(av.FormatOgg))
	spec, err := mustNew().Describe(
		compositeTestVideoSource("camera", 4, 4, 100, 10, 20).Video().Encode(codec.VP9()).To(out),
		mixTestAudioSource("mic", 1).Audio().Encode(codec.Opus()).To(out),
	)
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	muxes, inbound := 0, 0
	for _, node := range spec.Nodes {
		if node.Name == "call.ogg" {
			muxes++
		}
	}
	for _, edge := range spec.Edges {
		if edge.To == "call.ogg" {
			inbound++
		}
	}
	if muxes != 1 || inbound != 2 {
		t.Fatalf("call.ogg nodes=%d inbound edges=%d, want one mux fed by both streams\n%s", muxes, inbound, specText(spec))
	}
}

// TestDistinctHandlesWithOneNameAreRefused keeps sharing explicit: two
// separately constructed destinations that happen to share a name are a
// mistake, not a mux.
func TestDistinctHandlesWithOneNameAreRefused(t *testing.T) {
	_, err := mustNew().Describe(
		compositeTestVideoSource("camera", 4, 4, 100, 10, 20).Video().Encode(codec.VP9()).To(Write("call.ogg", io.Discard, Format(av.FormatOgg))),
		mixTestAudioSource("mic", 1).Audio().Encode(codec.Opus()).To(Write("call.ogg", io.Discard, Format(av.FormatOgg))),
	)

	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != destinationDuplicateCode {
		t.Fatalf("err = %v, want destination_duplicate", err)
	}
}
