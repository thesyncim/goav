package compile

import (
	"context"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/pipeline"
)

func TestEncodeTargetCodecContracts(t *testing.T) {
	request := EncodeRequest{Config: codec.EncodeConfig{
		Stream:     av.Stream{Codec: av.CodecParameters{ID: av.CodecVP8}},
		Parameters: av.CodecParameters{ID: av.CodecVP9},
	}}
	if got := EncodeTargetCodec(request); got != av.CodecVP9 {
		t.Fatalf("encodeTargetCodec with parameters = %q, want vp9", got)
	}

	request.Config.Parameters.ID = ""
	if got := EncodeTargetCodec(request); got != av.CodecVP8 {
		t.Fatalf("encodeTargetCodec with stream fallback = %q, want vp8", got)
	}

	request.Config.Stream.Codec.ID = ""
	if got := EncodeTargetCodec(request); got != "" {
		t.Fatalf("encodeTargetCodec empty = %q, want empty", got)
	}
}

func TestMediaTransformDetailContracts(t *testing.T) {
	stage := graphDetailStage{detail: "custom detail"}
	if got := MediaTransformDetail(MediaTransform{Stage: stage}); got != "custom detail" {
		t.Fatalf("stage detail = %q, want custom detail", got)
	}

	video := MediaTransform{Video: &filter.ResizeConfig{
		Width:       320,
		Height:      180,
		Mode:        filter.ResizeExact,
		PixelFormat: av.PixelFormatI420,
	}}
	if got := MediaTransformDetail(video); got != "resize, 320x180, exact, pixfmt=i420" {
		t.Fatalf("video detail = %q", got)
	}
	if got := MediaTransformDetail(MediaTransform{Video: &filter.ResizeConfig{}}); got != "resize, exact" {
		t.Fatalf("default video detail = %q, want resize, exact", got)
	}

	audio := MediaTransform{Audio: &filter.ResampleConfig{
		SampleRate:    48_000,
		Channels:      codec.Stereo,
		ChannelLayout: "stereo",
		SampleFormat:  av.SampleFormatS16,
	}}
	if got := MediaTransformDetail(audio); got != "resample, 48000 Hz, 2 ch, layout=stereo, samplefmt=s16" {
		t.Fatalf("audio detail = %q", got)
	}

	if got := MediaTransformDetail(MediaTransform{Factory: "custom-filter"}); got != "custom-filter" {
		t.Fatalf("factory detail = %q, want custom-filter", got)
	}
	if got := MediaTransformDetail(MediaTransform{}); got != "filter" {
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
