package goav

import (
	"reflect"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/shape"
)

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
