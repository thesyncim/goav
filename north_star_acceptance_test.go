package goav

import (
	"context"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
)

// northStarResampleRuntime is northStarTranscodeRuntime plus an audio resample
// filter, for acceptance checks that include a transform.
func northStarResampleRuntime() Runtime {
	streams := []av.Stream{audioOpusTestStream()}
	return New(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: &decodeTestDemuxer{streams: streams}}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
		withTestCodecs(
			testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
			testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		),
		withTestFilters(testFilterFactory(filter.Descriptor{
			Name:   filter.FactoryResample,
			Input:  av.MediaAudio,
			Output: av.MediaAudio,
		}, &transcodeTestFilterFactory{})),
	)
}

// northStarTranscodeRuntime builds a runtime that can demux/decode/encode/mux a
// single Opus audio stream, for the acceptance-suite jobs.
func northStarTranscodeRuntime() Runtime {
	streams := []av.Stream{audioOpusTestStream()}
	return New(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: &decodeTestDemuxer{streams: streams}}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
		withTestCodecs(
			testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
			testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		),
	)
}

// TestNorthStarDirectChainEqualsExplicitMainBranch is acceptance #2: a direct
// chain must lower to the same graph as the explicit Branch("main") form — the
// core "a direct chain is just an implicit Branch(\"main\")" rule.
func TestNorthStarDirectChainEqualsExplicitMainBranch(t *testing.T) {
	dest := File("archive.ogg", io.Discard)

	direct, err := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(northStarTranscodeRuntime()).
		Audio().Decode().Encode(Opus(codec.Bitrate(96_000))).To(dest).
		Describe()
	if err != nil {
		t.Fatalf("direct chain Describe(): %v", err)
	}

	branched, err := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(northStarTranscodeRuntime()).
		Audio().Decode().Branches(Branch("main").Encode(Opus(codec.Bitrate(96_000))).To(dest)).
		Describe()
	if err != nil {
		t.Fatalf("Branch(\"main\") Describe(): %v", err)
	}

	if !reflect.DeepEqual(direct, branched) {
		t.Fatalf("NORTH_STAR #2: a direct chain must lower to the same graph as Branch(\"main\").\ndirect:\n%s\nbranched:\n%s", specText(direct), specText(branched))
	}
}

// TestNorthStarDirectChainEqualsExplicitMainBranchWithTransform extends #2 to a
// chain that includes a transform: a direct chain with a resample must lower to
// the same graph as Branch("main") with the same resample — so the implicit
// branch names its private transform node by selector scope too, not "main".
func TestNorthStarDirectChainEqualsExplicitMainBranchWithTransform(t *testing.T) {
	dest := File("archive.ogg", io.Discard)

	direct, err := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(northStarResampleRuntime()).
		Audio().Decode().Resample(16_000, Mono).Encode(Opus(codec.Bitrate(96_000))).To(dest).
		Describe()
	if err != nil {
		t.Fatalf("direct chain Describe(): %v", err)
	}

	branched, err := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(northStarResampleRuntime()).
		Audio().Decode().Branches(Branch("main").Resample(16_000, Mono).Encode(Opus(codec.Bitrate(96_000))).To(dest)).
		Describe()
	if err != nil {
		t.Fatalf("Branch(\"main\") Describe(): %v", err)
	}

	if !reflect.DeepEqual(direct, branched) {
		t.Fatalf("NORTH_STAR #2 (with transform): direct chain must equal Branch(\"main\").\ndirect:\n%s\nbranched:\n%s", specText(direct), specText(branched))
	}
}

// TestNorthStarShapeGuards covers acceptance #11-15 (shape solver/validation).
func TestNorthStarShapeGuards(t *testing.T) {
	input := func() InputSpec { return FileInput("input.ogg", strings.NewReader("")) }

	// #13: a frame branch to File without Encode must fail (File needs packets).
	t.Run("frame_to_file_without_encode_fails", func(t *testing.T) {
		_, err := From(input()).UseRuntime(northStarTranscodeRuntime()).
			Audio().Decode().To(File("out.ogg", io.Discard)).
			Build(context.Background())
		if err == nil {
			t.Fatal("NORTH_STAR #13: a frame branch to File without Encode must fail (File needs packets)")
		}
	})

	// #15: decode to a frame Sink must succeed.
	t.Run("decode_to_frame_sink_succeeds", func(t *testing.T) {
		_, err := From(input()).UseRuntime(northStarTranscodeRuntime()).
			Audio().Decode().To(Sink(SinkFunc("frames", func(context.Context, Message) error { return nil }))).
			Build(context.Background())
		if err != nil {
			t.Fatalf("NORTH_STAR #15: decode to a frame Sink must succeed: %v", err)
		}
	})

	// #14: packet branch to File with Copy must succeed.
	t.Run("packet_copy_to_file_succeeds", func(t *testing.T) {
		_, err := From(input()).UseRuntime(northStarTranscodeRuntime()).
			Audio().Copy().To(File("out.ogg", io.Discard)).
			Build(context.Background())
		if err != nil {
			t.Fatalf("NORTH_STAR #14: a packet branch to File with Copy must succeed: %v", err)
		}
	})
}
