package goav

import (
	"reflect"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

func TestShapeConversionTargetFromSetContracts(t *testing.T) {
	current := shape.Spec{
		Domain:       shape.DomainFrame,
		MediaKind:    av.MediaAudio,
		StreamID:     "voice",
		Codec:        av.CodecOpus,
		Format:       av.FormatRTP,
		SampleRate:   44_100,
		Channels:     codec.Stereo,
		SampleFormat: av.SampleFormatS16,
	}
	want := shape.Spec{
		Domain:       shape.DomainFrame,
		MediaKind:    av.MediaAudio,
		StreamID:     "voice",
		Codec:        av.CodecOpus,
		Format:       av.FormatRTP,
		SampleRate:   48_000,
		Channels:     codec.Stereo,
		SampleFormat: av.SampleFormatF32,
	}
	got, ok := shapeConversionTargetFromSet(current, shape.Set{
		shape.Packet(av.MediaAudio, av.CodecOpus),
		shape.Frame(av.MediaVideo, shape.Video(640, 360, "")),
		shape.Frame(av.MediaAudio, shape.Codec(av.CodecAAC), shape.Audio(48_000, codec.Stereo, "")),
		shape.Frame(av.MediaAudio, shape.Stream("other"), shape.Audio(48_000, codec.Stereo, "")),
		shape.Frame(av.MediaAudio, shape.Format(av.FormatOgg), shape.Audio(48_000, codec.Stereo, "")),
		shape.Frame(av.MediaAudio, shape.Realtime(true), shape.Audio(48_000, codec.Stereo, "")),
		current,
		want,
	})
	if !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("conversion target = %#v, %v; want %#v, true", got, ok, want)
	}

	incomplete := shape.Frame(av.MediaAudio, shape.Audio(0, 0, av.SampleFormatS16))
	if got, ok := shapeConversionTargetFromSet(incomplete, shape.Set{
		shape.Frame(av.MediaAudio, shape.Audio(48_000, 0, av.SampleFormatF32)),
	}); ok || !mediaShapeEmpty(got) {
		t.Fatalf("incomplete conversion target = %#v, %v; want zero, false", got, ok)
	}
}

func TestSynthesizeConversionTransformContracts(t *testing.T) {
	audio, ok := synthesizeConversionTransform(
		av.MediaAudio,
		shape.Frame(av.MediaAudio, shape.Audio(44_100, codec.Stereo, av.SampleFormatS16)),
		shape.Frame(av.MediaAudio, shape.Audio(48_000, 0, av.SampleFormatF32)),
	)
	if !ok || audio.resample == nil ||
		audio.resample.SampleRate != 48_000 ||
		audio.resample.Channels != codec.Stereo ||
		audio.resample.SampleFormat != av.SampleFormatF32 {
		t.Fatalf("audio transform = %#v, %v", audio, ok)
	}

	if got, ok := synthesizeConversionTransform(
		av.MediaAudio,
		shape.Frame(av.MediaAudio, shape.Audio(0, 0, "")),
		shape.Frame(av.MediaAudio, shape.Audio(48_000, 0, "")),
	); ok || !mediaShapeEmpty(mediaShapeFromTransform(got)) {
		t.Fatalf("incomplete audio transform = %#v, %v; want zero, false", got, ok)
	}

	video, ok := synthesizeConversionTransform(
		av.MediaVideo,
		shape.Frame(av.MediaVideo, shape.Video(1920, 1080, av.PixelFormatI420)),
		shape.Frame(av.MediaVideo, shape.Video(640, 0, av.PixelFormatYUV420P)),
	)
	if !ok || video.resize == nil ||
		video.resize.Width != 640 ||
		video.resize.Height != 1080 ||
		video.resize.PixelFormat != av.PixelFormatYUV420P ||
		video.resize.Mode != filter.ResizeExact {
		t.Fatalf("video transform = %#v, %v", video, ok)
	}

	if got, ok := synthesizeConversionTransform(
		av.MediaVideo,
		shape.Frame(av.MediaVideo, shape.Video(0, 0, "")),
		shape.Frame(av.MediaVideo, shape.Video(640, 0, "")),
	); ok || !mediaShapeEmpty(mediaShapeFromTransform(got)) {
		t.Fatalf("incomplete video transform = %#v, %v; want zero, false", got, ok)
	}
	if got, ok := synthesizeConversionTransform(av.MediaData, shape.Spec{}, shape.Spec{}); ok || got != (transformSpec{}) {
		t.Fatalf("unknown media transform = %#v, %v; want zero, false", got, ok)
	}
}

func TestShapeSolverDiagnosticHelperContracts(t *testing.T) {
	labelTests := []struct {
		name      string
		operation operationSpec
		want      string
	}{
		{name: "encode id", operation: operationSpecForEncode(codec.Opus()), want: "encode-opus"},
		{name: "encode component", operation: operationSpec{Kind: plan.OpEncode, Component: "custom-encoder"}, want: "encode-custom-encoder"},
		{name: "encode fallback", operation: operationSpec{Kind: plan.OpEncode}, want: "encode-encoder"},
		{name: "decode id", operation: operationSpecForDecode(codec.VP8(), "vp8-decoder"), want: "decode-vp8"},
		{name: "decode component", operation: operationSpec{Kind: plan.OpDecode, Component: "custom-decoder"}, want: "decode-custom-decoder"},
		{name: "stage component", operation: operationSpec{Kind: plan.OpStage, Component: "meter"}, want: "stage-meter"},
		{name: "stage fallback", operation: operationSpec{Kind: plan.OpStage}, want: "stage-custom"},
		{name: "transform", operation: operationSpecForTransform(Resample(48_000, codec.Stereo)), want: "resample"},
		{name: "default component", operation: operationSpec{Kind: plan.OpTap, Component: "tap"}, want: "tap"},
		{name: "default kind", operation: operationSpec{Kind: plan.OpTap}, want: "tap"},
		{name: "default fallback", operation: operationSpec{}, want: "operation"},
	}
	for _, tt := range labelTests {
		t.Run(tt.name, func(t *testing.T) {
			if got := operationSpecLabel(tt.operation); got != tt.want {
				t.Fatalf("operationSpecLabel = %q, want %q", got, tt.want)
			}
		})
	}

	if got, want := explicitConversionSuggestion(Resample(16_000, codec.Mono), operationSpecForEncode(codec.Opus())),
		[]string{"insert .Resample(16000, 1) explicitly before encode-opus"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("resample suggestion = %#v, want %#v", got, want)
	}
	if got, want := explicitConversionSuggestion(Resize(640, 360), operationSpec{}),
		[]string{"insert .Resize(640, 360) explicitly"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("resize suggestion = %#v, want %#v", got, want)
	}
	if got := explicitConversionSuggestion(transformSpec{}, operationSpecForDecode(codec.Opus(), "opus")); got != nil {
		t.Fatalf("empty transform suggestion = %#v, want nil", got)
	}
}

func TestShapeSolverFormatHelperContracts(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{name: "unknown channels", got: formatChannels(0), want: "unknown channels"},
		{name: "channels", got: formatChannels(2), want: "2ch"},
		{name: "unknown frame size", got: formatFrameSize(0, 0), want: "unknown size"},
		{name: "partial frame size", got: formatFrameSize(0, 720), want: "0x720"},
		{name: "frame size", got: formatFrameSize(1280, 720), want: "1280x720"},
		{name: "unknown media format", got: formatMediaFormat(""), want: "unknown format"},
		{name: "media format", got: formatMediaFormat(av.SampleFormatF32), want: av.SampleFormatF32},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("got %q, want %q", tt.got, tt.want)
			}
		})
	}
	if got := firstPositiveInt(-1, 0, 44_100); got != 44_100 {
		t.Fatalf("firstPositiveInt = %d, want 44100", got)
	}
	if got := firstPositiveInt(-1, 0); got != 0 {
		t.Fatalf("firstPositiveInt without positive values = %d, want 0", got)
	}
}
