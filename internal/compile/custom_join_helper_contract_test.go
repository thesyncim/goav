package compile

import (
	"reflect"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/shape"
)

func TestApplyShapeToStreamContracts(t *testing.T) {
	stream := av.Stream{
		ID:   "joined",
		Type: av.MediaAudio,
		Codec: av.CodecParameters{
			ID:           av.CodecOpus,
			Type:         av.MediaAudio,
			SampleRate:   48_000,
			ClockRate:    48_000,
			Channels:     codec.Stereo,
			SampleFormat: av.SampleFormatS16,
		},
	}
	if got := ApplyShapeToStream(stream, shape.Spec{}); !reflect.DeepEqual(got, stream) {
		t.Fatalf("empty overlay = %+v, want unchanged %+v", got, stream)
	}

	overlay := shape.Spec{
		MediaKind:    av.MediaVideo,
		Codec:        av.CodecVP8,
		Width:        640,
		Height:       360,
		PixelFormat:  av.PixelFormatI420,
		SampleRate:   24_000,
		Channels:     codec.Mono,
		SampleFormat: av.SampleFormatF32,
	}
	got := ApplyShapeToStream(stream, overlay)
	if got.ID != "joined" ||
		got.Type != av.MediaVideo ||
		got.Codec.Type != av.MediaVideo ||
		got.Codec.ID != av.CodecVP8 ||
		got.Codec.Width != 640 ||
		got.Codec.Height != 360 ||
		got.Codec.PixelFormat != av.PixelFormatI420 ||
		got.Codec.SampleRate != 24_000 ||
		got.Codec.ClockRate != 24_000 ||
		got.Codec.Channels != codec.Mono ||
		got.Codec.SampleFormat != av.SampleFormatF32 {
		t.Fatalf("overlay stream = %+v", got)
	}
}

func TestCustomJoinArmExpectedContracts(t *testing.T) {
	if got, ok := CustomJoinArmExpected(nil); ok || got != (shape.Spec{}) {
		t.Fatalf("empty expected = %+v %v, want none", got, ok)
	}
	if got, ok := CustomJoinArmExpected(shape.Set{
		shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Stereo, "")),
		shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Stereo, "")),
	}); ok || got != (shape.Spec{}) {
		t.Fatalf("multi-shape expected = %+v %v, want none", got, ok)
	}
	if got, ok := CustomJoinArmExpected(shape.Set{shape.Packet(av.MediaAudio, av.CodecOpus)}); ok || got != (shape.Spec{}) {
		t.Fatalf("packet expected = %+v %v, want none", got, ok)
	}
	if got, ok := CustomJoinArmExpected(shape.Set{shape.Frame(av.MediaAudio)}); ok || got != (shape.Spec{}) {
		t.Fatalf("formatless frame expected = %+v %v, want none", got, ok)
	}

	audioExpected, ok := CustomJoinArmExpected(shape.Set{
		shape.Frame(
			av.MediaAudio,
			shape.Audio(48_000, codec.Stereo, av.SampleFormatF32),
			shape.Stream("ignored"),
			shape.Codec(av.CodecOpus),
			shape.Format(av.FormatOgg),
		),
	})
	wantAudio := shape.Spec{
		MediaKind:    av.MediaAudio,
		SampleRate:   48_000,
		Channels:     codec.Stereo,
		SampleFormat: av.SampleFormatF32,
	}
	if !ok || !reflect.DeepEqual(audioExpected, wantAudio) {
		t.Fatalf("audio expected = %+v %v, want %+v true", audioExpected, ok, wantAudio)
	}

	videoExpected, ok := CustomJoinArmExpected(shape.Set{
		shape.Frame(
			av.MediaVideo,
			shape.Video(1280, 720, av.PixelFormatI420),
			shape.Stream("ignored"),
			shape.Codec(av.CodecVP8),
		),
	})
	wantVideo := shape.Spec{
		MediaKind:   av.MediaVideo,
		Width:       1280,
		Height:      720,
		PixelFormat: av.PixelFormatI420,
	}
	if !ok || !reflect.DeepEqual(videoExpected, wantVideo) {
		t.Fatalf("video expected = %+v %v, want %+v true", videoExpected, ok, wantVideo)
	}
}

func TestCustomJoinShapeContractHelpers(t *testing.T) {
	audioFrame := shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Stereo, av.SampleFormatS16))
	videoFrame := shape.Frame(av.MediaVideo, shape.Video(640, 360, av.PixelFormatI420))

	if CustomJoinDecodesArms(nil) {
		t.Fatal("empty input contract decoded arms")
	}
	if CustomJoinDecodesArms(shape.Set{shape.Packet(av.MediaAudio, av.CodecOpus)}) {
		t.Fatal("packet input contract decoded arms")
	}
	if CustomJoinDecodesArms(shape.Set{audioFrame, shape.Packet(av.MediaAudio, av.CodecOpus)}) {
		t.Fatal("mixed-domain input contract decoded arms")
	}
	if !CustomJoinDecodesArms(shape.Set{audioFrame, videoFrame}) {
		t.Fatal("all frame-domain input contract did not require decoded arms")
	}

	if got := CustomJoinMedia(shape.Set{audioFrame, shape.Frame(av.MediaAudio)}); got != av.MediaAudio {
		t.Fatalf("customJoinMedia uniform audio = %s, want audio", got)
	}
	if got := CustomJoinMedia(shape.Set{shape.Frame("")}); got != "" {
		t.Fatalf("customJoinMedia missing media = %s, want empty", got)
	}
	if got := CustomJoinMedia(shape.Set{audioFrame, videoFrame}); got != "" {
		t.Fatalf("customJoinMedia mixed media = %s, want empty", got)
	}
}
