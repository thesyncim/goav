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
	"github.com/thesyncim/goav/shape"
)

// northStarResampleRuntime is northStarTranscodeRuntime plus an audio resample
// filter, for acceptance checks that include a transform.
func northStarResampleRuntime() *Runtime {
	streams := []av.Stream{audioOpusTestStream()}
	return mustNew(
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
func northStarTranscodeRuntime() *Runtime {
	streams := []av.Stream{audioOpusTestStream()}
	return mustNew(
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
	dest := Write("archive.ogg", io.Discard)

	direct, err := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(northStarTranscodeRuntime()).
		Audio().Decode().Encode(codec.Opus(codec.Bitrate(96_000))).To(dest).
		Describe()
	if err != nil {
		t.Fatalf("direct chain Describe(): %v", err)
	}

	branched, err := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(northStarTranscodeRuntime()).
		Audio().Decode().Branches(Branch("main").Encode(codec.Opus(codec.Bitrate(96_000))).To(dest)).
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
	dest := Write("archive.ogg", io.Discard)

	direct, err := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(northStarResampleRuntime()).
		Audio().Decode().Resample(16_000, codec.Mono).Encode(codec.Opus(codec.Bitrate(96_000))).To(dest).
		Describe()
	if err != nil {
		t.Fatalf("direct chain Describe(): %v", err)
	}

	branched, err := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(northStarResampleRuntime()).
		Audio().Decode().Branches(Branch("main").Resample(16_000, codec.Mono).Encode(codec.Opus(codec.Bitrate(96_000))).To(dest)).
		Describe()
	if err != nil {
		t.Fatalf("Branch(\"main\") Describe(): %v", err)
	}

	if !reflect.DeepEqual(direct, branched) {
		t.Fatalf("NORTH_STAR #2 (with transform): direct chain must equal Branch(\"main\").\ndirect:\n%s\nbranched:\n%s", specText(direct), specText(branched))
	}
}

// countingDecoderFactory counts decoder instantiations so the shared-decoder
// acceptance check can prove every consumer is fed by ONE decoder instance.
type countingDecoderFactory struct {
	inner    decodeTestDecoderFactory
	decoders int
}

func (f *countingDecoderFactory) NewDecoder(ctx context.Context, config codec.DecodeConfig) (codec.Decoder, error) {
	f.decoders++
	return f.inner.NewDecoder(ctx, config)
}

// TestNorthStarBranchesAfterDecodeShareOneDecoder is acceptance #18: two
// planned branches after one decode+tap share THE decoder. It pins both
// truths — topology (Describe shows exactly one decode node feeding both
// branch sinks) and data plane (one decoder instance is opened, it decodes
// each packet once, and every branch sink — planned or runtime-attached at
// the tap — receives all the frames).
func TestNorthStarBranchesAfterDecodeShareOneDecoder(t *testing.T) {
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{
			{StreamID: "audio", Payload: av.Buffer{Bytes: []byte{1}}},
			{StreamID: "audio", Payload: av.Buffer{Bytes: []byte{2}}},
		},
	}
	decoder := &decodeTestDecoder{}
	factory := &countingDecoderFactory{inner: decodeTestDecoderFactory{decoder: decoder}}
	rt := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		),
		withTestCodecs(testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, factory)),
	)
	sinkA := &runtimeTestSink{name: "a"}
	sinkB := &runtimeTestSink{name: "b"}
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(rt).
		Audio().Decode().Tap(FrameTap("audio.decoded")).
		Branches(
			Branch("a").To(Sink(sinkA)),
			Branch("b").To(Sink(sinkB)),
		)

	spec, err := job.Describe()
	if err != nil {
		t.Fatalf("Describe(): %v", err)
	}
	var decodeNodes []string
	for _, node := range spec.Nodes {
		if strings.HasPrefix(node.Name, "decode") {
			decodeNodes = append(decodeNodes, node.Name)
		}
	}
	if len(decodeNodes) != 1 {
		t.Fatalf("NORTH_STAR #18: decode nodes = %v, want exactly one shared decoder; spec:\n%s", decodeNodes, specText(spec))
	}
	fanout := map[string]bool{}
	for _, edge := range spec.Edges {
		if edge.From.String() == decodeNodes[0] {
			fanout[edge.To.String()] = true
		}
	}
	if !fanout["a"] || !fanout["b"] {
		t.Fatalf("NORTH_STAR #18: decode fanout = %v, want the one decode node feeding both branches; spec:\n%s", fanout, specText(spec))
	}

	task, err := job.Build(context.Background())
	if err != nil {
		t.Fatalf("Build(): %v", err)
	}
	defer task.Close()
	sinkC := &runtimeTestSink{name: "c"}
	if _, err := task.Attach(context.Background(),
		Branch("c").From(FrameTap("audio.decoded")).To(Sink(sinkC))); err != nil {
		t.Fatalf("Attach() at the decode tap: %v", err)
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatalf("Run(): %v", err)
	}
	if factory.decoders != 1 {
		t.Fatalf("NORTH_STAR #18: %d decoders opened, want the branches to share one", factory.decoders)
	}
	if decoder.decodes != len(demuxer.packets) {
		t.Fatalf("NORTH_STAR #18: %d decodes for %d packets, want each packet decoded once", decoder.decodes, len(demuxer.packets))
	}
	if sinkA.frames != 2 || sinkB.frames != 2 || sinkC.frames != 2 {
		t.Fatalf("NORTH_STAR #18: frames a=%d b=%d c=%d, want every branch to receive both decoded frames", sinkA.frames, sinkB.frames, sinkC.frames)
	}
}

// TestNorthStarFlowExposesNoDestinations is acceptance #3: a Flow is a
// reusable operation list ONLY — branches own destinations. Neither flow
// chain type may grow To or any other destination-carrying verb.
func TestNorthStarFlowExposesNoDestinations(t *testing.T) {
	forbidden := []string{"To", "Branches", "Output", "Outputs", "Destination", "Destinations", "Target", "Targets", "Path", "Paths"}
	for label, chain := range map[string]chain{
		"audio": Flow("guard").Audio(),
		"video": Flow("guard").Video(),
	} {
		chainType := reflect.TypeOf(chain)
		for _, name := range forbidden {
			if _, ok := chainType.MethodByName(name); ok {
				t.Errorf("NORTH_STAR #3: %s flow chain exposes %s; flows carry operations only, destinations belong to branches", label, name)
			}
		}
	}
}

// TestNorthStarCustomPacketSourceCopiesToFile is acceptance #34: a custom
// packet source feeds Copy into a muxed File destination end to end.
func TestNorthStarCustomPacketSourceCopiesToFile(t *testing.T) {
	muxers := &remuxTestMuxerFactory{}
	rt := mustNew(withTestFormats(testFormatMuxer(av.FormatOgg, muxers)))
	input := Source("gen",
		shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48_000, codec.Stereo, av.SampleFormatS16)),
		func(_ context.Context, push SourcePush) error {
			packet := av.Packet{Payload: av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable}}
			if _, err := push.Packet(&packet); err != nil {
				return err
			}
			return push.EOS()
		},
	)
	err := From(input).UseRuntime(rt).
		Audio().Copy().To(Write("capture.ogg", io.Discard, Format(av.FormatOgg))).
		Run(context.Background())
	if err != nil {
		t.Fatalf("NORTH_STAR #34: custom packet source Copy to File: %v", err)
	}
	if len(muxers.muxers) != 1 || muxers.muxers[0].writes != 1 || muxers.muxers[0].lastStream != "gen" {
		t.Fatalf("NORTH_STAR #34: muxers = %+v, want one File mux group with the source packet", muxers.muxers)
	}
}

// TestNorthStarCustomFrameSourceEncodesToFile is acceptance #35: a custom
// frame source feeds Encode into a muxed File destination end to end.
func TestNorthStarCustomFrameSourceEncodesToFile(t *testing.T) {
	encoder := &encodeTestEncoder{}
	muxers := &remuxTestMuxerFactory{}
	rt := mustNew(
		withTestCodecs(testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: encoder})),
		withTestFormats(testFormatMuxer(av.FormatOgg, muxers)),
	)
	input := Source("pcm",
		shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Stereo, av.SampleFormatS16)),
		func(_ context.Context, push SourcePush) error {
			frame := av.Frame{
				Type: av.MediaAudio,
				Audio: &av.AudioFrame{
					SampleRate:   48_000,
					Channels:     codec.Stereo,
					SampleFormat: av.SampleFormatS16,
					Samples:      480,
				},
			}
			if _, err := push.Frame(&frame); err != nil {
				return err
			}
			return push.EOS()
		},
	)
	err := From(input).UseRuntime(rt).
		Audio().Encode(codec.Opus(codec.Bitrate(96_000))).To(Write("capture.ogg", io.Discard, Format(av.FormatOgg))).
		Run(context.Background())
	if err != nil {
		t.Fatalf("NORTH_STAR #35: custom frame source Encode to File: %v", err)
	}
	if encoder.encodes != 1 {
		t.Fatalf("NORTH_STAR #35: encodes = %d, want the source frame encoded once", encoder.encodes)
	}
	if len(muxers.muxers) != 1 || muxers.muxers[0].writes != 1 || muxers.muxers[0].lastStream != "pcm" {
		t.Fatalf("NORTH_STAR #35: muxers = %+v, want one File mux group with the encoded packet", muxers.muxers)
	}
}

// TestNorthStarShapeGuards covers acceptance #11-15 (shape solver/validation).
func TestNorthStarShapeGuards(t *testing.T) {
	input := func() InputSpec { return FileInput("input.ogg", strings.NewReader("")) }

	// #13: a frame branch to File without Encode must fail (File needs packets).
	t.Run("frame_to_file_without_encode_fails", func(t *testing.T) {
		_, err := From(input()).UseRuntime(northStarTranscodeRuntime()).
			Audio().Decode().To(Write("out.ogg", io.Discard)).
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
			Audio().Copy().To(Write("out.ogg", io.Discard)).
			Build(context.Background())
		if err != nil {
			t.Fatalf("NORTH_STAR #14: a packet branch to File with Copy must succeed: %v", err)
		}
	})
}
