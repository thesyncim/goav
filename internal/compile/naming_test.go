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
	if got := encodeTargetCodec(request); got != av.CodecVP9 {
		t.Fatalf("encodeTargetCodec with parameters = %q, want vp9", got)
	}

	request.Config.Parameters.ID = ""
	if got := encodeTargetCodec(request); got != av.CodecVP8 {
		t.Fatalf("encodeTargetCodec with stream fallback = %q, want vp8", got)
	}

	request.Config.Stream.Codec.ID = ""
	if got := encodeTargetCodec(request); got != "" {
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

func TestRuntimeNodeNamingContracts(t *testing.T) {
	if got := EncodeNodeName(EncodeRequest{Name: "preview"}); got != "encode-preview" {
		t.Fatalf("named encode node = %q", got)
	}
	if got := EncodeNodeName(EncodeRequest{Selector: av.StreamSelector{Name: "camera"}}); got != "encode-camera" {
		t.Fatalf("selector-name encode node = %q", got)
	}
	if got := EncodeNodeName(EncodeRequest{Selector: av.StreamSelector{ID: "v0"}}); got != "encode-v0" {
		t.Fatalf("selector-id encode node = %q", got)
	}
	if got := EncodeNodeName(EncodeRequest{Selector: av.StreamSelector{Codec: av.CodecVP8}}); got != "encode-vp8" {
		t.Fatalf("selector-codec encode node = %q", got)
	}
	if got := EncodeNodeName(EncodeRequest{Selector: av.StreamSelector{Type: av.MediaVideo}}); got != "encode-video" {
		t.Fatalf("selector-type encode node = %q", got)
	}
	if got := EncodeNodeName(EncodeRequest{Selector: av.StreamSelector{UseIndex: true, Index: 2}}); got != "encode-2" {
		t.Fatalf("selector-index encode node = %q", got)
	}
	if got := EncodeNodeName(EncodeRequest{Config: codec.EncodeConfig{Parameters: av.CodecParameters{ID: av.CodecVP9}}}); got != "encode-vp9" {
		t.Fatalf("parameter-codec encode node = %q", got)
	}
	if got := EncodeNodeName(EncodeRequest{Config: codec.EncodeConfig{Stream: av.Stream{Codec: av.CodecParameters{ID: av.CodecOpus}}}}); got != "encode-opus" {
		t.Fatalf("stream-codec encode node = %q", got)
	}
	if got := EncodeNodeName(EncodeRequest{}); got != "encode" {
		t.Fatalf("fallback encode node = %q", got)
	}

	if got := DecodeNodeName(av.StreamSelector{Name: "camera"}); got != "decode-camera" {
		t.Fatalf("selector-name decode node = %q", got)
	}
	if got := DecodeNodeName(av.StreamSelector{ID: "v0"}); got != "decode-v0" {
		t.Fatalf("selector-id decode node = %q", got)
	}
	if got := DecodeNodeName(av.StreamSelector{Codec: av.CodecH264}); got != "decode-h264" {
		t.Fatalf("selector-codec decode node = %q", got)
	}
	if got := DecodeNodeName(av.StreamSelector{Type: av.MediaAudio}); got != "decode-audio" {
		t.Fatalf("selector-type decode node = %q", got)
	}
	if got := DecodeNodeName(av.StreamSelector{UseIndex: true, Index: 1}); got != "decode-1" {
		t.Fatalf("selector-index decode node = %q", got)
	}
	if got := DecodeNodeName(av.StreamSelector{}); got != "decode" {
		t.Fatalf("fallback decode node = %q", got)
	}

	if got := selectNodeName(av.StreamSelector{Name: "camera"}); got != "select-camera" {
		t.Fatalf("selector-name select node = %q", got)
	}
	if got := selectNodeName(av.StreamSelector{ID: "v0"}); got != "select-v0" {
		t.Fatalf("selector-id select node = %q", got)
	}
	if got := selectNodeName(av.StreamSelector{Codec: av.CodecVP8}); got != "select-vp8" {
		t.Fatalf("selector-codec select node = %q", got)
	}
	if got := selectNodeName(av.StreamSelector{Type: av.MediaVideo}); got != "select-video" {
		t.Fatalf("selector-type select node = %q", got)
	}
	if got := selectNodeName(av.StreamSelector{UseIndex: true, Index: 3}); got != "select-3" {
		t.Fatalf("selector-index select node = %q", got)
	}
	if got := selectNodeName(av.StreamSelector{}); got != "select" {
		t.Fatalf("fallback select node = %q", got)
	}
}

func TestWorkDestinationNameAndIDHelperContracts(t *testing.T) {
	destinations := []WorkDestination{
		{ID: "destination/archive", Name: "archive"},
		{ID: "destination/preview", Name: "preview"},
		{ID: "destination/nameless"},
	}
	ids := workDestinationIDsByName(destinations)
	if got := ids["archive"]; got != "destination/archive" {
		t.Fatalf("archive id = %q", got)
	}
	if _, ok := ids[""]; ok {
		t.Fatalf("empty destination name should not be indexed: %v", ids)
	}

	if got := workDestinationIDForName(ids, "archive"); got != "destination/archive" {
		t.Fatalf("workDestinationIDForName hit = %q", got)
	}
	if got := workDestinationIDForName(ids, "new"); got != "destination/new" {
		t.Fatalf("workDestinationIDForName miss = %q", got)
	}
	if got := workDestinationIDForName(ids, ""); got != "destination/unnamed" {
		t.Fatalf("workDestinationIDForName empty = %q", got)
	}

	if got := WorkDestinationNameByID(destinations, "destination/preview"); got != "preview" {
		t.Fatalf("workDestinationNameByID hit = %q", got)
	}
	if got := WorkDestinationNameByID(destinations, "destination/missing"); got != "destination/missing" {
		t.Fatalf("workDestinationNameByID miss = %q", got)
	}
}
