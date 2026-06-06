package goav

import (
	"context"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/transcode"
)

func TestRuntimeBuilderTranscodeBranchesRenditionsToOutputs(t *testing.T) {
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio",
			Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		}},
	}
	muxers := &remuxTestMuxerFactory{}
	formats := format.NewRegistry(
		format.WithProber(remuxTestProber{streams: streams}),
		format.WithDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		format.WithMuxer(av.FormatOgg, muxers),
	)
	decoder := &decodeTestDecoder{}
	encoderFactory := &encodeTestEncoderFactory{}
	codecs := codec.NewRegistry(
		codec.WithDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: decoder}),
		codec.WithEncoder(codec.Descriptor{ID: av.CodecPCM, Type: av.MediaAudio}, encoderFactory),
	)
	plan := transcode.Plan{
		Input: format.Input{Name: "input.ogg"},
		Renditions: []transcode.Rendition{
			{
				Name:     "audio-main",
				Selector: SelectAudio(),
				Encode:   pcmEncodeConfig(),
				Labels:   []string{"archive"},
			},
			{
				Name:     "audio-low",
				Selector: SelectAudio(),
				Encode:   pcmEncodeConfig(),
				Labels:   []string{"archive", "preview"},
			},
		},
		Outputs: []transcode.Output{
			{
				Name:       "archive.ogg",
				Format:     av.FormatOgg,
				Renditions: []string{"archive"},
			},
			{
				Name:       "preview.ogg",
				Format:     av.FormatOgg,
				Renditions: []string{"audio-low"},
			},
		},
	}

	builder := New(WithFormatRegistry(formats), WithCodecRegistry(codecs)).New().
		Transcode(plan)
	planned, err := builder.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if len(planned.Nodes) != 7 || len(planned.Edges) != 7 {
		t.Fatalf("nodes=%d edges=%d", len(planned.Nodes), len(planned.Edges))
	}
	if !strings.Contains(planned.String(), "decode-audio -> encode-audio-main") ||
		!strings.Contains(planned.String(), "decode-audio -> encode-audio-low") ||
		!strings.Contains(planned.String(), "encode-audio-main -> archive.ogg") ||
		!strings.Contains(planned.String(), "encode-audio-low -> preview.ogg") {
		t.Fatalf("planned:\n%s", planned.String())
	}

	task, err := builder.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if planned.String() != task.Describe().String() || planned.Mermaid() != task.Describe().Mermaid() {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", planned.String(), task.Describe().String())
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

func TestRuntimeBuilderTranscodeRejectsUncompiledTransforms(t *testing.T) {
	plan := transcode.Plan{
		Input: format.Input{Name: "input.ogg"},
		Renditions: []transcode.Rendition{{
			Name:     "audio-low",
			Selector: SelectAudio(),
			Resample: &filter.ResampleConfig{
				SampleRate: 16000,
				Channels:   1,
			},
			Encode: pcmEncodeConfig(),
		}},
		Outputs: []transcode.Output{{Name: "preview.ogg"}},
	}

	_, err := New().New().Transcode(plan).Describe()
	if err != ErrUnsupportedBuild {
		t.Fatalf("err = %v, want ErrUnsupportedBuild", err)
	}
}

func TestRuntimeBuilderTranscodeRequiresMatchingOutputSelection(t *testing.T) {
	plan := transcode.Plan{
		Input: format.Input{Name: "input.ogg"},
		Renditions: []transcode.Rendition{{
			Name:     "audio-main",
			Selector: SelectAudio(),
			Encode:   pcmEncodeConfig(),
		}},
		Outputs: []transcode.Output{{
			Name:       "preview.ogg",
			Renditions: []string{"missing"},
		}},
	}

	_, err := New().New().Transcode(plan).Describe()
	if err != ErrUnsupportedBuild {
		t.Fatalf("err = %v, want ErrUnsupportedBuild", err)
	}
}

func encoderAt(encoders []*encodeTestEncoder, index int) *encodeTestEncoder {
	if index >= len(encoders) {
		return nil
	}
	return encoders[index]
}
