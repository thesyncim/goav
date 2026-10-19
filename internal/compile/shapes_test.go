package compile

import (
	"reflect"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/internal/recipeir"
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
	}); ok || !MediaShapeEmpty(got) {
		t.Fatalf("incomplete conversion target = %#v, %v; want zero, false", got, ok)
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

func TestSynthesizeConversionTransformContracts(t *testing.T) {
	audio, ok := synthesizeConversionTransform(
		av.MediaAudio,
		shape.Frame(av.MediaAudio, shape.Audio(44_100, codec.Stereo, av.SampleFormatS16)),
		shape.Frame(av.MediaAudio, shape.Audio(48_000, 0, av.SampleFormatF32)),
	)
	if !ok || audio.Kind != recipeir.TransformResample ||
		audio.Resample.SampleRate != 48_000 ||
		audio.Resample.Channels != codec.Stereo ||
		audio.Resample.SampleFormat != av.SampleFormatF32 {
		t.Fatalf("audio transform = %#v, %v", audio, ok)
	}

	if got, ok := synthesizeConversionTransform(
		av.MediaAudio,
		shape.Frame(av.MediaAudio, shape.Audio(0, 0, "")),
		shape.Frame(av.MediaAudio, shape.Audio(48_000, 0, "")),
	); ok || !MediaShapeEmpty(MediaShapeFromRecipeIRTransform(got)) {
		t.Fatalf("incomplete audio transform = %#v, %v; want zero, false", got, ok)
	}

	video, ok := synthesizeConversionTransform(
		av.MediaVideo,
		shape.Frame(av.MediaVideo, shape.Video(1920, 1080, av.PixelFormatI420)),
		shape.Frame(av.MediaVideo, shape.Video(640, 0, av.PixelFormatYUV420P)),
	)
	if !ok || video.Kind != recipeir.TransformResize ||
		video.Resize.Width != 640 ||
		video.Resize.Height != 1080 ||
		video.Resize.PixelFormat != av.PixelFormatYUV420P ||
		video.Resize.Mode != filter.ResizeExact {
		t.Fatalf("video transform = %#v, %v", video, ok)
	}

	if got, ok := synthesizeConversionTransform(
		av.MediaVideo,
		shape.Frame(av.MediaVideo, shape.Video(0, 0, "")),
		shape.Frame(av.MediaVideo, shape.Video(640, 0, "")),
	); ok || !MediaShapeEmpty(MediaShapeFromRecipeIRTransform(got)) {
		t.Fatalf("incomplete video transform = %#v, %v; want zero, false", got, ok)
	}
	if got, ok := synthesizeConversionTransform(av.MediaData, shape.Spec{}, shape.Spec{}); ok || got != (recipeir.Transform{}) {
		t.Fatalf("unknown media transform = %#v, %v; want zero, false", got, ok)
	}
}

func TestPreferredConversionTargetOverlaysOnlyOpenAudioFacts(t *testing.T) {
	expected := shape.Frame(
		av.MediaAudio,
		shape.Audio(48_000, 0, av.SampleFormatS16),
		shape.Stream("mic"),
		shape.Codec(av.CodecOpus),
		shape.Format(av.FormatOgg),
	)
	preference := shape.Packet(
		av.MediaAudio,
		av.CodecAAC,
		shape.Audio(44_100, 6, av.SampleFormatF32),
		shape.Stream("other"),
		shape.Format(av.FormatRTP),
		shape.Realtime(true),
	)

	want := expected
	want.Channels = 6

	if got := preferredConversionTarget(av.MediaAudio, expected, preference); !reflect.DeepEqual(got, want) {
		t.Fatalf("preferred audio target = %+v, want %+v", got, want)
	}

	expected = shape.Frame(av.MediaAudio, shape.Audio(0, 2, ""))
	want = expected
	want.SampleRate = 44_100
	want.SampleFormat = av.SampleFormatF32

	if got := preferredConversionTarget(av.MediaAudio, expected, preference); !reflect.DeepEqual(got, want) {
		t.Fatalf("preferred audio target with open rate/format = %+v, want %+v", got, want)
	}
}

func TestPreferredConversionTargetOverlaysOnlyOpenVideoFacts(t *testing.T) {
	expected := shape.Frame(
		av.MediaVideo,
		shape.Video(640, 0, ""),
		shape.Stream("camera"),
		shape.Codec(av.CodecVP8),
		shape.Format(av.FormatWebM),
	)
	preference := shape.Packet(
		av.MediaVideo,
		av.CodecH264,
		shape.Video(320, 180, av.PixelFormatI420),
		shape.Stream("other"),
		shape.Format(av.FormatRTP),
		shape.Realtime(true),
	)

	want := expected
	want.Height = 180
	want.PixelFormat = av.PixelFormatI420

	if got := preferredConversionTarget(av.MediaVideo, expected, preference); !reflect.DeepEqual(got, want) {
		t.Fatalf("preferred video target = %+v, want %+v", got, want)
	}

	expected = shape.Frame(av.MediaVideo, shape.Video(0, 480, av.PixelFormatGray8))
	want = expected
	want.Width = 320

	if got := preferredConversionTarget(av.MediaVideo, expected, preference); !reflect.DeepEqual(got, want) {
		t.Fatalf("preferred video target with open width = %+v, want %+v", got, want)
	}
}

func TestPreferredConversionTargetIgnoresUnknownMedia(t *testing.T) {
	expected := shape.New(
		shape.Stream("events"),
		shape.Format(av.FormatWebRTC),
	)
	preference := shape.New(
		shape.Audio(48_000, 2, av.SampleFormatF32),
		shape.Video(1280, 720, av.PixelFormatI420),
		shape.Stream("other"),
		shape.Codec(av.CodecOpus),
		shape.Format(av.FormatRTP),
		shape.Realtime(true),
	)

	if got := preferredConversionTarget("", expected, preference); !reflect.DeepEqual(got, expected) {
		t.Fatalf("unknown-media target = %+v, want unchanged %+v", got, expected)
	}
}

func TestExplicitEncodeSoftInputShapeContracts(t *testing.T) {
	zeroCases := []struct {
		name   string
		encode codec.Spec
	}{
		{name: "copy encode", encode: codec.Copy()},
		{name: "missing codec id", encode: codec.Codec("", av.MediaAudio)},
		{name: "audio defaults are not explicit", encode: codec.Opus()},
		{name: "video without explicit format facts", encode: codec.VP8()},
		{name: "unknown media", encode: codec.Codec("x-data", "data")},
	}
	for _, tt := range zeroCases {
		t.Run(tt.name, func(t *testing.T) {
			if got := explicitEncodeSoftInputShape(tt.encode); got != (shape.Spec{}) {
				t.Fatalf("explicitEncodeSoftInputShape() = %+v, want zero", got)
			}
		})
	}

	audio := codec.Opus(codec.SampleRate(44_100), codec.Channels(codec.Mono))
	audio.Parameters.SampleFormat = av.SampleFormatF32
	wantAudio := shape.Spec{
		Domain:       shape.DomainFrame,
		MediaKind:    av.MediaAudio,
		SampleRate:   44_100,
		Channels:     codec.Mono,
		SampleFormat: av.SampleFormatF32,
	}
	if got := explicitEncodeSoftInputShape(audio); !reflect.DeepEqual(got, wantAudio) {
		t.Fatalf("audio soft input shape = %+v, want %+v", got, wantAudio)
	}

	customAudio := codec.Codec("x-audio", av.MediaAudio, codec.SampleRate(32_000))
	wantCustomAudio := shape.Spec{
		Domain:     shape.DomainFrame,
		MediaKind:  av.MediaAudio,
		SampleRate: 32_000,
	}
	if got := explicitEncodeSoftInputShape(customAudio); !reflect.DeepEqual(got, wantCustomAudio) {
		t.Fatalf("custom audio soft input shape = %+v, want %+v", got, wantCustomAudio)
	}

	video := codec.VP8()
	video.Parameters.Width = 1280
	video.Parameters.Height = 720
	video.Parameters.PixelFormat = av.PixelFormatI420
	wantVideo := shape.Spec{
		Domain:      shape.DomainFrame,
		MediaKind:   av.MediaVideo,
		Width:       1280,
		Height:      720,
		PixelFormat: av.PixelFormatI420,
	}
	if got := explicitEncodeSoftInputShape(video); !reflect.DeepEqual(got, wantVideo) {
		t.Fatalf("video soft input shape = %+v, want %+v", got, wantVideo)
	}

	customVideo := codec.Codec("x-video", av.MediaVideo)
	customVideo.Parameters.PixelFormat = av.PixelFormatGray8
	wantCustomVideo := shape.Spec{
		Domain:      shape.DomainFrame,
		MediaKind:   av.MediaVideo,
		PixelFormat: av.PixelFormatGray8,
	}
	if got := explicitEncodeSoftInputShape(customVideo); !reflect.DeepEqual(got, wantCustomVideo) {
		t.Fatalf("custom video soft input shape = %+v, want %+v", got, wantCustomVideo)
	}
}
