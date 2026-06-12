package goav

import (
	"context"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/pipeline"
)

func TestEncodeTargetCodecContracts(t *testing.T) {
	request := encodeRequest{config: codec.EncodeConfig{
		Stream:     av.Stream{Codec: av.CodecParameters{ID: av.CodecVP8}},
		Parameters: av.CodecParameters{ID: av.CodecVP9},
	}}
	if got := encodeTargetCodec(request); got != av.CodecVP9 {
		t.Fatalf("encodeTargetCodec with parameters = %q, want vp9", got)
	}

	request.config.Parameters.ID = ""
	if got := encodeTargetCodec(request); got != av.CodecVP8 {
		t.Fatalf("encodeTargetCodec with stream fallback = %q, want vp8", got)
	}

	request.config.Stream.Codec.ID = ""
	if got := encodeTargetCodec(request); got != "" {
		t.Fatalf("encodeTargetCodec empty = %q, want empty", got)
	}
}

func TestMediaTransformDetailContracts(t *testing.T) {
	stage := graphDetailStage{detail: "custom detail"}
	if got := mediaTransformDetail(mediaTransform{stage: stage}); got != "custom detail" {
		t.Fatalf("stage detail = %q, want custom detail", got)
	}

	video := mediaTransform{video: &filter.ResizeConfig{
		Width:       320,
		Height:      180,
		Mode:        filter.ResizeExact,
		PixelFormat: av.PixelFormatI420,
	}}
	if got := mediaTransformDetail(video); got != "resize, 320x180, exact, pixfmt=i420" {
		t.Fatalf("video detail = %q", got)
	}
	if got := mediaTransformDetail(mediaTransform{video: &filter.ResizeConfig{}}); got != "resize, exact" {
		t.Fatalf("default video detail = %q, want resize, exact", got)
	}

	audio := mediaTransform{audio: &filter.ResampleConfig{
		SampleRate:    48_000,
		Channels:      codec.Stereo,
		ChannelLayout: "stereo",
		SampleFormat:  av.SampleFormatS16,
	}}
	if got := mediaTransformDetail(audio); got != "resample, 48000 Hz, 2 ch, layout=stereo, samplefmt=s16" {
		t.Fatalf("audio detail = %q", got)
	}

	if got := mediaTransformDetail(mediaTransform{factory: "custom-filter"}); got != "custom-filter" {
		t.Fatalf("factory detail = %q, want custom-filter", got)
	}
	if got := mediaTransformDetail(mediaTransform{}); got != "filter" {
		t.Fatalf("fallback detail = %q, want filter", got)
	}
}

type graphDetailStage struct {
	detail string
}

func (s graphDetailStage) Name() string {
	return "graph-detail-stage"
}

func (s graphDetailStage) Handle(context.Context, *pipeline.Message, pipeline.Emitter) error {
	return nil
}

func (s graphDetailStage) Close() error {
	return nil
}

func (s graphDetailStage) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Detail: s.detail}
}
