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
	got, ok := ShapeConversionTargetFromSet(current, shape.Set{
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
	if got, ok := ShapeConversionTargetFromSet(incomplete, shape.Set{
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
		{name: "unknown channels", got: FormatChannels(0), want: "unknown channels"},
		{name: "channels", got: FormatChannels(2), want: "2ch"},
		{name: "unknown frame size", got: FormatFrameSize(0, 0), want: "unknown size"},
		{name: "partial frame size", got: FormatFrameSize(0, 720), want: "0x720"},
		{name: "frame size", got: FormatFrameSize(1280, 720), want: "1280x720"},
		{name: "unknown media format", got: FormatMediaFormat(""), want: "unknown format"},
		{name: "media format", got: FormatMediaFormat(av.SampleFormatF32), want: av.SampleFormatF32},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("got %q, want %q", tt.got, tt.want)
			}
		})
	}
	if got := FirstPositiveInt(-1, 0, 44_100); got != 44_100 {
		t.Fatalf("firstPositiveInt = %d, want 44100", got)
	}
	if got := FirstPositiveInt(-1, 0); got != 0 {
		t.Fatalf("firstPositiveInt without positive values = %d, want 0", got)
	}
}

func TestSynthesizeConversionTransformContracts(t *testing.T) {
	audio, ok := SynthesizeConversionTransform(
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

	if got, ok := SynthesizeConversionTransform(
		av.MediaAudio,
		shape.Frame(av.MediaAudio, shape.Audio(0, 0, "")),
		shape.Frame(av.MediaAudio, shape.Audio(48_000, 0, "")),
	); ok || !MediaShapeEmpty(MediaShapeFromRecipeIRTransform(got)) {
		t.Fatalf("incomplete audio transform = %#v, %v; want zero, false", got, ok)
	}

	video, ok := SynthesizeConversionTransform(
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

	if got, ok := SynthesizeConversionTransform(
		av.MediaVideo,
		shape.Frame(av.MediaVideo, shape.Video(0, 0, "")),
		shape.Frame(av.MediaVideo, shape.Video(640, 0, "")),
	); ok || !MediaShapeEmpty(MediaShapeFromRecipeIRTransform(got)) {
		t.Fatalf("incomplete video transform = %#v, %v; want zero, false", got, ok)
	}
	if got, ok := SynthesizeConversionTransform(av.MediaData, shape.Spec{}, shape.Spec{}); ok || got != (recipeir.Transform{}) {
		t.Fatalf("unknown media transform = %#v, %v; want zero, false", got, ok)
	}
}
