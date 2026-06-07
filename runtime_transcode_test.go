package goav

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/rtpav"
	"github.com/thesyncim/goav/transcode"
)

func TestRuntimeBuilderTranscodeBranchesFeedMuxOutputs(t *testing.T) {
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio",
			Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		}},
	}
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	decoder := &decodeTestDecoder{}
	encoderFactory := &encodeTestEncoderFactory{}
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: decoder}),
		testCodecEncoder(codec.Descriptor{ID: av.CodecPCM, Type: av.MediaAudio}, encoderFactory),
	)
	plan := transcode.Plan{
		Input: format.Input{Name: "input.ogg"},
		Branches: []transcode.Branch{
			{
				Name:     "audio-main",
				Selector: testSelectAudio(),
				Encode:   pcmEncodeConfig(),
				Labels:   []string{"archive"},
			},
			{
				Name:     "audio-low",
				Selector: testSelectAudio(),
				Encode:   pcmEncodeConfig(),
				Labels:   []string{"archive", "preview"},
			},
		},
		Outputs: []transcode.Output{
			{
				Name:     "archive.ogg",
				Format:   av.FormatOgg,
				Branches: []string{"archive"},
			},
			{
				Name:     "preview.ogg",
				Format:   av.FormatOgg,
				Branches: []string{"audio-low"},
			},
		},
	}

	builder := newTestBuilder(t, formats, codecs).
		Transcode(plan)
	planned, err := builder.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if len(planned.Nodes) != 7 || len(planned.Edges) != 7 {
		t.Fatalf("nodes=%d edges=%d", len(planned.Nodes), len(planned.Edges))
	}
	if !strings.Contains(specText(planned), "decode-audio -> encode-audio-main") ||
		!strings.Contains(specText(planned), "decode-audio -> encode-audio-low") ||
		!strings.Contains(specText(planned), "encode-audio-main -> archive.ogg") ||
		!strings.Contains(specText(planned), "encode-audio-low -> preview.ogg") {
		t.Fatalf("planned:\n%s", specText(planned))
	}

	task, err := builder.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if specText(planned) != specText(task.Describe()) || specMermaid(planned) != specMermaid(task.Describe()) {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", specText(planned), specText(task.Describe()))
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 || decoder.flushes != 1 {
		t.Fatalf("decodes=%d flushes=%d", decoder.decodes, decoder.flushes)
	}
	if len(encoderFactory.encoders) != 2 || encoderFactory.encoders[0].encodes != 1 || encoderFactory.encoders[1].encodes != 1 {
		t.Fatalf("encoders=%d first=%+v second=%+v", len(encoderFactory.encoders), encoderAt(encoderFactory.encoders, 0), encoderAt(encoderFactory.encoders, 1))
	}
	if len(encoderFactory.configs) != 2 ||
		encoderFactory.configs[0].Stream.ID != "audio-main" ||
		encoderFactory.configs[1].Stream.ID != "audio-low" {
		t.Fatalf("encode configs: %+v", encoderFactory.configs)
	}
	if len(muxers.muxers) != 2 {
		t.Fatalf("muxers = %d, want 2", len(muxers.muxers))
	}
	archive := muxers.muxers[0]
	preview := muxers.muxers[1]
	if !archive.opened || archive.writes != 2 || !streamIDsEqual(archive.openedStreams, []av.StreamID{"audio-main", "audio-low"}) ||
		!streamIDsEqual(archive.writtenStreams, []av.StreamID{"audio-main", "audio-low"}) {
		t.Fatalf("archive opened=%v writes=%d opened streams=%+v written=%+v", archive.opened, archive.writes, archive.openedStreams, archive.writtenStreams)
	}
	if !preview.opened || preview.writes != 1 || !streamIDsEqual(preview.openedStreams, []av.StreamID{"audio-low"}) ||
		!streamIDsEqual(preview.writtenStreams, []av.StreamID{"audio-low"}) {
		t.Fatalf("preview opened=%v writes=%d opened streams=%+v written=%+v", preview.opened, preview.writes, preview.openedStreams, preview.writtenStreams)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !demuxer.closed || !decoder.closed || !encoderFactory.encoders[0].closed || !encoderFactory.encoders[1].closed ||
		!archive.closed || !preview.closed {
		t.Fatalf("closed demux=%v decoder=%v encoders=%+v archive=%v preview=%v", demuxer.closed, decoder.closed, encoderFactory.encoders, archive.closed, preview.closed)
	}
}

func TestRuntimeBuilderTranscodeRunsBranchStageStep(t *testing.T) {
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio",
			Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		}},
	}
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	decoder := &decodeTestDecoder{}
	stage := &runtimeTestStage{name: "meter"}
	encoderFactory := &encodeTestEncoderFactory{}
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: decoder}),
		testCodecEncoder(codec.Descriptor{ID: av.CodecPCM, Type: av.MediaAudio}, encoderFactory),
	)
	plan := transcode.Plan{
		Input: format.Input{Name: "input.ogg"},
		Branches: []transcode.Branch{{
			Name:     "audio-main",
			Selector: testSelectAudio(),
			Steps:    []transcode.Step{{Stage: stage}},
			Encode:   pcmEncodeConfig(),
			Labels:   []string{"archive"},
		}},
		Outputs: []transcode.Output{{Name: "archive.ogg", Format: av.FormatOgg}},
	}

	builder := newTestBuilder(t, formats, codecs).Transcode(plan)
	planned, err := builder.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(specText(planned), "decode-audio -> meter") ||
		!strings.Contains(specText(planned), "meter -> encode-audio-main") {
		t.Fatalf("planned:\n%s", specText(planned))
	}
	task, err := builder.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stage.count == 0 {
		t.Fatal("custom branch stage did not receive frames")
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !stage.closed {
		t.Fatal("custom branch stage was not closed")
	}
}

func TestRuntimeBuilderTranscodeComposesAudioAndVideoIntoOneOutput(t *testing.T) {
	streams := []av.Stream{audioOpusTestStream(), videoVP8TranscodeTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{
			{
				StreamID: "audio",
				Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
			},
			{
				StreamID: "video",
				Payload:  av.Buffer{Bytes: []byte{4, 5, 6}},
			},
		},
	}
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	audioDecoder := &decodeTestDecoder{}
	videoDecoder := &decodeTestDecoder{}
	audioEncoderFactory := &encodeTestEncoderFactory{}
	videoEncoderFactory := &encodeTestEncoderFactory{}
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: audioDecoder}),
		testCodecDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, &decodeTestDecoderFactory{decoder: videoDecoder}),
		testCodecEncoder(codec.Descriptor{ID: av.CodecPCM, Type: av.MediaAudio}, audioEncoderFactory),
		testCodecEncoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, videoEncoderFactory),
	)
	plan := transcode.Plan{
		Input: format.Input{Name: "input.webm"},
		Branches: []transcode.Branch{
			{
				Name:     "a96",
				Selector: testSelectAudio(),
				Encode:   pcmEncodeConfig(),
				Labels:   []string{"web"},
			},
			{
				Name:     "v360",
				Selector: testSelectVideo(),
				Encode:   vp8EncodeConfig(),
				Labels:   []string{"web"},
			},
		},
		Outputs: []transcode.Output{{
			Name:     "web.ogg",
			Format:   av.FormatOgg,
			Branches: []string{"web"},
		}},
	}

	builder := newTestBuilder(t, formats, codecs).
		Transcode(plan)
	planned, err := builder.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(planned)
	if len(planned.Nodes) != 8 || len(planned.Edges) != 8 ||
		!strings.Contains(text, "decode-audio -> encode-a96") ||
		!strings.Contains(text, "decode-video -> encode-v360") ||
		!strings.Contains(text, "encode-a96 -> web.ogg") ||
		!strings.Contains(text, "encode-v360 -> web.ogg") ||
		strings.Contains(text, "decode-audio -> encode-v360") ||
		strings.Contains(text, "decode-video -> encode-a96") {
		t.Fatalf("planned:\n%s", text)
	}

	task, err := builder.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if specText(planned) != specText(task.Describe()) || specMermaid(planned) != specMermaid(task.Describe()) {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", specText(planned), specText(task.Describe()))
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if audioDecoder.decodes != 1 || videoDecoder.decodes != 1 {
		t.Fatalf("audio decodes=%d video decodes=%d", audioDecoder.decodes, videoDecoder.decodes)
	}
	if len(audioEncoderFactory.configs) != 1 || audioEncoderFactory.configs[0].Stream.ID != "a96" ||
		len(videoEncoderFactory.configs) != 1 || videoEncoderFactory.configs[0].Stream.ID != "v360" {
		t.Fatalf("audio configs=%+v video configs=%+v", audioEncoderFactory.configs, videoEncoderFactory.configs)
	}
	if len(muxers.muxers) != 1 {
		t.Fatalf("muxers=%d, want 1", len(muxers.muxers))
	}
	muxer := muxers.muxers[0]
	if !muxer.opened || muxer.writes != 2 ||
		!streamIDsEqual(muxer.openedStreams, []av.StreamID{"a96", "v360"}) ||
		!streamIDsEqual(muxer.writtenStreams, []av.StreamID{"a96", "v360"}) {
		t.Fatalf("opened=%v writes=%d opened streams=%+v written=%+v", muxer.opened, muxer.writes, muxer.openedStreams, muxer.writtenStreams)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !demuxer.closed || !audioDecoder.closed || !videoDecoder.closed || !muxer.closed {
		t.Fatalf("closed demux=%v audio=%v video=%v mux=%v", demuxer.closed, audioDecoder.closed, videoDecoder.closed, muxer.closed)
	}
}

func TestRuntimeBuilderRTPTranscodeBranchesDecodedStreamToOutputs(t *testing.T) {
	ctx := context.Background()
	stream := audioOpusTestStream()
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    rtpav.MIMEOpus,
			ClockRate:   48000,
			Channels:    2,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
			Payload: []byte{1, 2, 3},
		}},
		events: make(chan av.Event),
	}
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	decoder := &decodeTestDecoder{}
	encoderFactory := &encodeTestEncoderFactory{}
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: decoder}),
		testCodecEncoder(codec.Descriptor{ID: av.CodecPCM, Type: av.MediaAudio}, encoderFactory),
	)
	plan := transcode.Plan{
		Branches: []transcode.Branch{
			{
				Name:     "voice",
				Selector: testSelectAudio(),
				Encode:   pcmEncodeConfig(),
				Labels:   []string{"archive"},
			},
			{
				Name:     "preview",
				Selector: testSelectAudio(),
				Encode:   pcmEncodeConfig(),
				Labels:   []string{"archive", "preview"},
			},
		},
		Outputs: []transcode.Output{
			{
				Name:     "archive.ogg",
				Format:   av.FormatOgg,
				Branches: []string{"archive"},
			},
			{
				Name:     "preview.ogg",
				Format:   av.FormatOgg,
				Branches: []string{"preview"},
			},
		},
	}

	builder := newTestBuilder(t, formats, codecs).
		RTP(receiver,
			withRTPName("live-audio"),
			withRTPDepacketizers(rtpav.NewOpusDepacketizer(stream)),
		).
		Transcode(plan)
	planned, err := builder.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(planned)
	if !strings.Contains(text, "live-audio -> select-audio") ||
		!strings.Contains(text, "decode-audio -> encode-voice") ||
		!strings.Contains(text, "decode-audio -> encode-preview") ||
		!strings.Contains(text, "encode-voice -> archive.ogg") ||
		!strings.Contains(text, "encode-preview -> preview.ogg") {
		t.Fatalf("planned:\n%s", text)
	}

	task, err := builder.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if specText(planned) != specText(task.Describe()) || specMermaid(planned) != specMermaid(task.Describe()) {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", specText(planned), specText(task.Describe()))
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 || decoder.flushes != 1 {
		t.Fatalf("decodes=%d flushes=%d", decoder.decodes, decoder.flushes)
	}
	if len(encoderFactory.encoders) != 2 || encoderFactory.encoders[0].encodes != 1 || encoderFactory.encoders[1].encodes != 1 {
		t.Fatalf("encoders=%d first=%+v second=%+v", len(encoderFactory.encoders), encoderAt(encoderFactory.encoders, 0), encoderAt(encoderFactory.encoders, 1))
	}
	if len(muxers.muxers) != 2 {
		t.Fatalf("muxers=%d, want 2", len(muxers.muxers))
	}
	archive := muxers.muxers[0]
	preview := muxers.muxers[1]
	if !archive.opened || archive.writes != 2 || !streamIDsEqual(archive.writtenStreams, []av.StreamID{"voice", "preview"}) {
		t.Fatalf("archive opened=%v writes=%d written=%+v", archive.opened, archive.writes, archive.writtenStreams)
	}
	if !preview.opened || preview.writes != 1 || !streamIDsEqual(preview.writtenStreams, []av.StreamID{"preview"}) {
		t.Fatalf("preview opened=%v writes=%d written=%+v", preview.opened, preview.writes, preview.writtenStreams)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !receiver.closed || !decoder.closed || !archive.closed || !preview.closed {
		t.Fatalf("closed receiver=%v decoder=%v archive=%v preview=%v", receiver.closed, decoder.closed, archive.closed, preview.closed)
	}
}

func TestRuntimeBuilderBufferedTranscodeRequiresPacketCopyBound(t *testing.T) {
	builder, _, _, _, _ := newBufferedTranscodeCopyFixture(pipeline.BufferPolicy{
		Capacity: 8,
		Drop:     pipeline.DropOldest,
	})

	task, err := builder.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(context.Background()); !errors.Is(err, pipeline.ErrBufferedMessageUnsafe) {
		t.Fatalf("err = %v, want ErrBufferedMessageUnsafe", err)
	}
}

func TestRuntimeBuilderBufferedTranscodeCopiesEncodedPacketsToOutputs(t *testing.T) {
	builder, demuxer, muxers, decoder, encoderFactory := newBufferedTranscodeCopyFixture(pipeline.BufferPolicy{
		Capacity:        8,
		Drop:            pipeline.DropOldest,
		CopyPacketBytes: 8,
	})
	planned, err := builder.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(specText(planned), "decode-audio -> encode-audio-main") ||
		!strings.Contains(specText(planned), "encode-audio-low -> preview.ogg") {
		t.Fatalf("planned:\n%s", specText(planned))
	}

	task, err := builder.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if specText(planned) != specText(task.Describe()) || specMermaid(planned) != specMermaid(task.Describe()) {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", specText(planned), specText(task.Describe()))
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 || len(encoderFactory.encoders) != 2 ||
		encoderFactory.encoders[0].encodes != 1 || encoderFactory.encoders[1].encodes != 1 {
		t.Fatalf("decodes=%d encoders=%+v", decoder.decodes, encoderFactory.encoders)
	}
	if len(muxers.muxers) != 2 {
		t.Fatalf("muxers = %d, want 2", len(muxers.muxers))
	}
	archive := muxers.muxers[0]
	preview := muxers.muxers[1]
	if !streamIDsUnorderedEqual(archive.writtenStreams, []av.StreamID{"audio-main", "audio-low"}) ||
		!bytes.Equal(archive.writtenPayloads, []byte{7, 7}) {
		t.Fatalf("archive written streams=%+v payloads=%+v", archive.writtenStreams, archive.writtenPayloads)
	}
	if !streamIDsEqual(preview.writtenStreams, []av.StreamID{"audio-low"}) ||
		!bytes.Equal(preview.writtenPayloads, []byte{7}) {
		t.Fatalf("preview written streams=%+v payloads=%+v", preview.writtenStreams, preview.writtenPayloads)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !demuxer.closed || !decoder.closed || !encoderFactory.encoders[0].closed || !encoderFactory.encoders[1].closed ||
		!archive.closed || !preview.closed {
		t.Fatalf("closed demux=%v decoder=%v encoders=%+v archive=%v preview=%v", demuxer.closed, decoder.closed, encoderFactory.encoders, archive.closed, preview.closed)
	}
}

func TestRuntimeBuilderTranscodeAppliesResampleBranch(t *testing.T) {
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio",
			Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		}},
	}
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	decoder := &decodeTestDecoder{}
	encoderFactory := &encodeTestEncoderFactory{}
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: decoder}),
		testCodecEncoder(codec.Descriptor{ID: av.CodecPCM, Type: av.MediaAudio}, encoderFactory),
	)
	resampler := &transcodeTestFilter{}
	filterFactory := &transcodeTestFilterFactory{filter: resampler}
	filters := withTestFilters(testFilterFactory(filter.Descriptor{
		Name:   filter.FactoryResample,
		Input:  av.MediaAudio,
		Output: av.MediaAudio,
	}, filterFactory))
	plan := transcode.Plan{
		Input: format.Input{Name: "input.ogg"},
		Branches: []transcode.Branch{
			{
				Name:     "audio-main",
				Selector: testSelectAudio(),
				Encode:   pcmEncodeConfig(),
			},
			{
				Name:     "audio-low",
				Selector: testSelectAudio(),
				Resample: &filter.ResampleConfig{
					SampleRate: 16000,
					Channels:   1,
				},
				Encode: codec.EncodeConfig{
					Parameters: av.CodecParameters{ID: av.CodecPCM, Type: av.MediaAudio},
				},
			},
		},
		Outputs: []transcode.Output{{Name: "archive.ogg", Format: av.FormatOgg}},
	}

	builder := newTestBuilder(t, formats, codecs, filters).
		Transcode(plan)
	planned, err := builder.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if len(planned.Nodes) != 7 || len(planned.Edges) != 7 {
		t.Fatalf("nodes=%d edges=%d", len(planned.Nodes), len(planned.Edges))
	}
	if !strings.Contains(specText(planned), "decode-audio -> resample-audio-low") ||
		!strings.Contains(specText(planned), "resample-audio-low -> encode-audio-low") {
		t.Fatalf("planned:\n%s", specText(planned))
	}

	task, err := builder.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if specText(planned) != specText(task.Describe()) || specMermaid(planned) != specMermaid(task.Describe()) {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", specText(planned), specText(task.Describe()))
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if filterFactory.config.Stream.ID != "audio" || filterFactory.config.Audio == nil ||
		filterFactory.config.Audio.SampleRate != 16000 || filterFactory.config.Audio.Channels != 1 {
		t.Fatalf("filter config: %+v", filterFactory.config)
	}
	if resampler.frames != 1 || resampler.flushes != 1 {
		t.Fatalf("resampler frames=%d flushes=%d", resampler.frames, resampler.flushes)
	}
	if len(encoderFactory.encoders) != 2 || encoderFactory.encoders[0].encodes != 1 || encoderFactory.encoders[1].encodes != 1 {
		t.Fatalf("encoders=%d first=%+v second=%+v", len(encoderFactory.encoders), encoderAt(encoderFactory.encoders, 0), encoderAt(encoderFactory.encoders, 1))
	}
	if len(encoderFactory.configs) != 2 ||
		encoderFactory.configs[1].Parameters.SampleRate != 16000 ||
		encoderFactory.configs[1].Parameters.ClockRate != 16000 ||
		encoderFactory.configs[1].Parameters.Channels != 1 {
		t.Fatalf("encode configs: %+v", encoderFactory.configs)
	}
	if len(muxers.muxers) != 1 || muxers.muxers[0].writes != 2 ||
		!streamIDsEqual(muxers.muxers[0].writtenStreams, []av.StreamID{"audio-main", "audio-low"}) {
		t.Fatalf("muxers=%d first=%+v", len(muxers.muxers), muxers.muxers)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !resampler.closed {
		t.Fatal("resampler not closed")
	}
}

func newBufferedTranscodeCopyFixture(policy pipeline.BufferPolicy) (builderAPI, *decodeTestDemuxer, *remuxTestMuxerFactory, *decodeTestDecoder, *encodeTestEncoderFactory) {
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio",
			Payload: av.Buffer{
				Bytes:     []byte{1, 2, 3},
				Ownership: av.BufferImmutable,
			},
		}},
	}
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	decoder := &decodeTestDecoder{}
	encoderFactory := &encodeTestEncoderFactory{}
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: decoder}),
		testCodecEncoder(codec.Descriptor{ID: av.CodecPCM, Type: av.MediaAudio}, encoderFactory),
	)
	plan := transcode.Plan{
		Input: format.Input{Name: "input.ogg"},
		Branches: []transcode.Branch{
			{
				Name:     "audio-main",
				Selector: testSelectAudio(),
				Encode:   pcmEncodeConfig(),
				Labels:   []string{"archive"},
			},
			{
				Name:     "audio-low",
				Selector: testSelectAudio(),
				Encode:   pcmEncodeConfig(),
				Labels:   []string{"archive", "preview"},
			},
		},
		Outputs: []transcode.Output{
			{
				Name:     "archive.ogg",
				Format:   av.FormatOgg,
				Branches: []string{"archive"},
			},
			{
				Name:     "preview.ogg",
				Format:   av.FormatOgg,
				Branches: []string{"audio-low"},
			},
		},
	}
	builder := New(formats, codecs, WithBufferPolicy(policy)).(*runtime).New().
		Transcode(plan)
	return builder, demuxer, muxers, decoder, encoderFactory
}

func streamIDsUnorderedEqual(got []av.StreamID, want []av.StreamID) bool {
	if len(got) != len(want) {
		return false
	}
	seen := make(map[av.StreamID]int, len(want))
	for i := range got {
		seen[got[i]]++
	}
	for i := range want {
		if seen[want[i]] == 0 {
			return false
		}
		seen[want[i]]--
	}
	return true
}

func videoVP8TranscodeTestStream() av.Stream {
	return av.Stream{
		ID:       "video",
		Type:     av.MediaVideo,
		TimeBase: av.TimeBase{Num: 1, Den: 90000},
		Codec: av.CodecParameters{
			ID:          av.CodecVP8,
			Type:        av.MediaVideo,
			ClockRate:   90000,
			Width:       640,
			Height:      360,
			PixelFormat: av.PixelFormatYUV420P,
		},
	}
}

func vp8EncodeConfig() codec.EncodeConfig {
	return codec.EncodeConfig{
		Parameters: av.CodecParameters{
			ID:          av.CodecVP8,
			Type:        av.MediaVideo,
			ClockRate:   90000,
			Width:       640,
			Height:      360,
			PixelFormat: av.PixelFormatYUV420P,
		},
	}
}

func TestRuntimeBuilderTranscodeRequiresTransformFactory(t *testing.T) {
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
	)
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
		testCodecEncoder(codec.Descriptor{ID: av.CodecPCM, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
	)
	plan := transcode.Plan{
		Input: format.Input{Name: "input.ogg"},
		Branches: []transcode.Branch{{
			Name:     "audio-low",
			Selector: testSelectAudio(),
			Resample: &filter.ResampleConfig{
				SampleRate: 16000,
				Channels:   1,
			},
			Encode: pcmEncodeConfig(),
		}},
		Outputs: []transcode.Output{{Name: "preview.ogg"}},
	}

	_, err := newTestBuilder(t, formats, codecs).
		Transcode(plan).
		Build(context.Background())
	if err != filter.ErrNotFound {
		t.Fatalf("err = %v, want filter.ErrNotFound", err)
	}
	if !demuxer.closed {
		t.Fatal("demux source should be closed after missing filter factory")
	}
}

func TestRuntimeBuilderTranscodeRequiresMatchingOutputSelection(t *testing.T) {
	plan := transcode.Plan{
		Input: format.Input{Name: "input.ogg"},
		Branches: []transcode.Branch{{
			Name:     "audio-main",
			Selector: testSelectAudio(),
			Encode:   pcmEncodeConfig(),
		}},
		Outputs: []transcode.Output{{
			Name:     "preview.ogg",
			Branches: []string{"missing"},
		}},
	}

	_, err := newTestBuilder(t).Transcode(plan).Describe()
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "branch_destination_unmatched" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want branch_destination_unmatched wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "destination selects no branches") ||
		!strings.Contains(err.Error(), "requested: missing") ||
		!strings.Contains(err.Error(), "branch name") {
		t.Fatalf("err = %v, want unmatched output guidance", err)
	}
}

func TestRuntimeBuilderTranscodeReportsEmptyPlanParts(t *testing.T) {
	tests := []struct {
		name string
		plan transcode.Plan
		want string
	}{
		{
			name: "branches",
			plan: transcode.Plan{
				Input:   format.Input{Name: "input.ogg"},
				Outputs: []transcode.Output{{Name: "preview.ogg"}},
			},
			want: "no branches",
		},
		{
			name: "outputs",
			plan: transcode.Plan{
				Input: format.Input{Name: "input.ogg"},
				Branches: []transcode.Branch{{
					Name:     "audio-main",
					Selector: testSelectAudio(),
					Encode:   pcmEncodeConfig(),
				}},
			},
			want: "no destinations",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newTestBuilder(t).Transcode(tt.plan).Describe()
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != "branch_compose_plan_empty" || !errors.Is(err, ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want branch_compose_plan_empty wrapping ErrUnsupportedBuild", err)
			}
			if !strings.Contains(err.Error(), tt.want) ||
				!strings.Contains(err.Error(), "goav.From") {
				t.Fatalf("err = %v, want empty plan guidance", err)
			}
		})
	}
}

func TestRuntimeBuilderTranscodeReportsDuplicateBranchNames(t *testing.T) {
	plan := transcode.Plan{
		Input: format.Input{Name: "input.ogg"},
		Branches: []transcode.Branch{
			{Name: "audio-main", Selector: testSelectAudio(), Encode: pcmEncodeConfig()},
			{Name: "audio-main", Selector: testSelectAudio(), Encode: pcmEncodeConfig()},
		},
		Outputs: []transcode.Output{{Name: "preview.ogg"}},
	}

	_, err := newTestBuilder(t).Transcode(plan).Describe()
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "branch_duplicate" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want branch_duplicate wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "audio-main") ||
		!strings.Contains(err.Error(), "duplicate index: 1") ||
		!strings.Contains(err.Error(), "unique name") {
		t.Fatalf("err = %v, want duplicate branch guidance", err)
	}
}

func TestRuntimeBuilderTranscodeReportsInvalidStep(t *testing.T) {
	plan := transcode.Plan{
		Input: format.Input{Name: "input.ogg"},
		Branches: []transcode.Branch{{
			Name:     "mixed",
			Selector: testSelectAudio(),
			Steps: []transcode.Step{{
				Resize:   &filter.ResizeConfig{Width: 320, Height: 180},
				Resample: &filter.ResampleConfig{SampleRate: 16000, Channels: 1},
			}},
			Encode: pcmEncodeConfig(),
		}},
		Outputs: []transcode.Output{{Name: "preview.ogg"}},
	}

	_, err := newTestBuilder(t).Transcode(plan).Describe()
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "branch_operation_chain_unsupported" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want branch_operation_chain_unsupported wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "cannot combine resize and resample") ||
		!strings.Contains(err.Error(), "one operation per branch call") {
		t.Fatalf("err = %v, want invalid operation guidance", err)
	}
}

func TestRuntimeBuilderTranscodeReportsTransformMediaMismatch(t *testing.T) {
	streams := []av.Stream{videoVP8TranscodeTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
	)
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
	)
	plan := transcode.Plan{
		Input: format.Input{Name: "input.ogg"},
		Branches: []transcode.Branch{{
			Name:     "video-as-audio",
			Selector: testSelectVideo(),
			Steps: []transcode.Step{{
				Resample: &filter.ResampleConfig{SampleRate: 16000, Channels: 1},
			}},
			Encode: vp8EncodeConfig(),
		}},
		Outputs: []transcode.Output{{Name: "preview.ogg"}},
	}

	_, err := newTestBuilder(t, formats, codecs).Transcode(plan).Build(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "branch_transform_media_mismatch" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want branch_transform_media_mismatch wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "resample applies to audio streams") ||
		!strings.Contains(err.Error(), "stream id: video") ||
		!strings.Contains(err.Error(), "branch selector") {
		t.Fatalf("err = %v, want transform media guidance", err)
	}
	if !demuxer.closed {
		t.Fatal("demux source should be closed after transform media mismatch")
	}
}

func TestTranscodeVideoFilterResultPreallocatesI420Planes(t *testing.T) {
	stream := av.Stream{
		ID:   "video",
		Type: av.MediaVideo,
		Codec: av.CodecParameters{
			Type:        av.MediaVideo,
			Width:       640,
			Height:      360,
			PixelFormat: av.PixelFormatYUV420P,
		},
	}

	result := filterResultForStream(stream)
	if len(result.Frames) != 0 || cap(result.Frames) != 1 {
		t.Fatalf("frames len=%d cap=%d", len(result.Frames), cap(result.Frames))
	}
	frame := result.Frames[:1][0]
	if len(frame.Planes) != 3 {
		t.Fatalf("planes = %d, want 3", len(frame.Planes))
	}
	if cap(frame.Planes[0].Buffer.Bytes) != 640*360 ||
		cap(frame.Planes[1].Buffer.Bytes) != 640*360/4 ||
		cap(frame.Planes[2].Buffer.Bytes) != 640*360/4 {
		t.Fatalf("plane capacity = %d %d %d", cap(frame.Planes[0].Buffer.Bytes), cap(frame.Planes[1].Buffer.Bytes), cap(frame.Planes[2].Buffer.Bytes))
	}
}

func TestApplyResizeConfigToStreamFit(t *testing.T) {
	stream := av.Stream{
		Type: av.MediaVideo,
		Codec: av.CodecParameters{
			Type:   av.MediaVideo,
			Width:  1920,
			Height: 1080,
		},
	}

	if err := applyResizeConfigToStream(&stream, filter.ResizeConfig{Width: 1280, Height: 720, Mode: filter.ResizeFit}); err != nil {
		t.Fatal(err)
	}
	if stream.Codec.Width != 1280 || stream.Codec.Height != 720 {
		t.Fatalf("geometry = %dx%d, want 1280x720", stream.Codec.Width, stream.Codec.Height)
	}
}

func TestApplyResizeConfigToStreamReportsInvalidConfig(t *testing.T) {
	tests := []struct {
		name   string
		stream av.Stream
		config filter.ResizeConfig
		want   []string
	}{
		{
			name: "fit missing input geometry",
			stream: av.Stream{
				ID:   "video",
				Type: av.MediaVideo,
				Codec: av.CodecParameters{
					Type: av.MediaVideo,
				},
			},
			config: filter.ResizeConfig{Width: 1280, Height: 720, Mode: filter.ResizeFit},
			want:   []string{"known positive input width and height", "input width: 0", "target width: 1280"},
		},
		{
			name: "fill missing target geometry",
			stream: av.Stream{
				ID:   "video",
				Type: av.MediaVideo,
				Codec: av.CodecParameters{
					Type:   av.MediaVideo,
					Width:  640,
					Height: 360,
				},
			},
			config: filter.ResizeConfig{Height: 720, Mode: filter.ResizeFill},
			want:   []string{"resize fill requires positive target width and height", "target width: 0"},
		},
		{
			name: "unsupported mode",
			stream: av.Stream{
				ID:   "video",
				Type: av.MediaVideo,
				Codec: av.CodecParameters{
					Type:   av.MediaVideo,
					Width:  640,
					Height: 360,
				},
			},
			config: filter.ResizeConfig{Width: 640, Height: 360, Mode: filter.ResizeMode("stretch")},
			want:   []string{"unsupported resize mode", "mode: stretch", "exact, fit, fill, or passthrough"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := applyResizeConfigToStream(&tt.stream, tt.config)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != "transcode_resize_invalid" || !errors.Is(err, ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want transcode_resize_invalid wrapping ErrUnsupportedBuild", err)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want %q", err, want)
				}
			}
		})
	}
}

func encoderAt(encoders []*encodeTestEncoder, index int) *encodeTestEncoder {
	if index >= len(encoders) {
		return nil
	}
	return encoders[index]
}

type transcodeTestFilterFactory struct {
	filter *transcodeTestFilter
	config filter.Config
}

func (f *transcodeTestFilterFactory) NewFilter(_ context.Context, config filter.Config) (filter.FrameFilter, error) {
	f.config = config
	if f.filter == nil {
		f.filter = &transcodeTestFilter{}
	}
	return f.filter, nil
}

type transcodeTestFilter struct {
	frames  int
	flushes int
	closed  bool
}

func (f *transcodeTestFilter) Descriptor() filter.Descriptor {
	return filter.Descriptor{Name: filter.FactoryResample, Input: av.MediaAudio, Output: av.MediaAudio}
}

func (f *transcodeTestFilter) Open(context.Context, filter.Config) error {
	return nil
}

func (f *transcodeTestFilter) FilterInto(_ context.Context, frame *av.Frame, out *filter.Result) error {
	if frame == nil {
		return nil
	}
	if len(out.Frames) == cap(out.Frames) {
		return filter.ErrResultFull
	}
	index := len(out.Frames)
	out.Frames = out.Frames[:index+1]
	outFrame := &out.Frames[index]
	outFrame.Reset()
	*outFrame = *frame
	f.frames++
	return nil
}

func (f *transcodeTestFilter) FlushInto(context.Context, *filter.Result) error {
	f.flushes++
	return nil
}

func (f *transcodeTestFilter) HandleEvent(context.Context, *av.Event) error {
	return nil
}

func (f *transcodeTestFilter) Close() error {
	f.closed = true
	return nil
}
