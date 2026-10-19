package goav

import (
	"reflect"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/shape"
)

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
			if got := compile.ExplicitEncodeSoftInputShape(tt.encode); got != (shape.Spec{}) {
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
	if got := compile.ExplicitEncodeSoftInputShape(audio); !reflect.DeepEqual(got, wantAudio) {
		t.Fatalf("audio soft input shape = %+v, want %+v", got, wantAudio)
	}

	customAudio := codec.Codec("x-audio", av.MediaAudio, codec.SampleRate(32_000))
	wantCustomAudio := shape.Spec{
		Domain:     shape.DomainFrame,
		MediaKind:  av.MediaAudio,
		SampleRate: 32_000,
	}
	if got := compile.ExplicitEncodeSoftInputShape(customAudio); !reflect.DeepEqual(got, wantCustomAudio) {
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
	if got := compile.ExplicitEncodeSoftInputShape(video); !reflect.DeepEqual(got, wantVideo) {
		t.Fatalf("video soft input shape = %+v, want %+v", got, wantVideo)
	}

	customVideo := codec.Codec("x-video", av.MediaVideo)
	customVideo.Parameters.PixelFormat = av.PixelFormatGray8
	wantCustomVideo := shape.Spec{
		Domain:      shape.DomainFrame,
		MediaKind:   av.MediaVideo,
		PixelFormat: av.PixelFormatGray8,
	}
	if got := compile.ExplicitEncodeSoftInputShape(customVideo); !reflect.DeepEqual(got, wantCustomVideo) {
		t.Fatalf("custom video soft input shape = %+v, want %+v", got, wantCustomVideo)
	}
}
