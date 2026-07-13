package goav

import (
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

func TestCodecAndTapNamingContracts(t *testing.T) {
	tests := []struct {
		name string
		spec codec.Spec
		want string
	}{
		{name: "auto", spec: codec.Auto(), want: "auto"},
		{name: "copy", spec: codec.Copy(), want: "copy"},
		{name: "custom", spec: codec.Codec(av.CodecID("x_custom"), av.MediaAudio), want: "x_custom"},
		{name: "zero", spec: codec.Spec{}, want: "none"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := codecIntentName(tt.spec); got != tt.want {
				t.Fatalf("codec intent name = %q, want %q", got, tt.want)
			}
		})
	}

	if got := defaultPacketTapName(av.MediaAudio, 7); got != "audio.packets" {
		t.Fatalf("audio packet tap = %q", got)
	}
	if got := defaultPacketTapName("", 0); got != "packets" {
		t.Fatalf("default packet tap = %q", got)
	}
	if got := defaultPacketTapName("", 3); got != "packets-3" {
		t.Fatalf("indexed packet tap = %q", got)
	}
}

func TestRuntimeNodeNamingContracts(t *testing.T) {
	if got := encodeNodeName(encodeRequest{name: "preview"}); got != "encode-preview" {
		t.Fatalf("named encode node = %q", got)
	}
	if got := encodeNodeName(encodeRequest{selector: av.StreamSelector{Name: "camera"}}); got != "encode-camera" {
		t.Fatalf("selector-name encode node = %q", got)
	}
	if got := encodeNodeName(encodeRequest{selector: av.StreamSelector{ID: "v0"}}); got != "encode-v0" {
		t.Fatalf("selector-id encode node = %q", got)
	}
	if got := encodeNodeName(encodeRequest{selector: av.StreamSelector{Codec: av.CodecVP8}}); got != "encode-vp8" {
		t.Fatalf("selector-codec encode node = %q", got)
	}
	if got := encodeNodeName(encodeRequest{selector: av.StreamSelector{Type: av.MediaVideo}}); got != "encode-video" {
		t.Fatalf("selector-type encode node = %q", got)
	}
	if got := encodeNodeName(encodeRequest{selector: av.StreamSelector{UseIndex: true, Index: 2}}); got != "encode-2" {
		t.Fatalf("selector-index encode node = %q", got)
	}
	if got := encodeNodeName(encodeRequest{config: codec.EncodeConfig{Parameters: av.CodecParameters{ID: av.CodecVP9}}}); got != "encode-vp9" {
		t.Fatalf("parameter-codec encode node = %q", got)
	}
	if got := encodeNodeName(encodeRequest{config: codec.EncodeConfig{Stream: av.Stream{Codec: av.CodecParameters{ID: av.CodecOpus}}}}); got != "encode-opus" {
		t.Fatalf("stream-codec encode node = %q", got)
	}
	if got := encodeNodeName(encodeRequest{}); got != "encode" {
		t.Fatalf("fallback encode node = %q", got)
	}

	if got := decodeNodeName(av.StreamSelector{Name: "camera"}); got != "decode-camera" {
		t.Fatalf("selector-name decode node = %q", got)
	}
	if got := decodeNodeName(av.StreamSelector{ID: "v0"}); got != "decode-v0" {
		t.Fatalf("selector-id decode node = %q", got)
	}
	if got := decodeNodeName(av.StreamSelector{Codec: av.CodecH264}); got != "decode-h264" {
		t.Fatalf("selector-codec decode node = %q", got)
	}
	if got := decodeNodeName(av.StreamSelector{Type: av.MediaAudio}); got != "decode-audio" {
		t.Fatalf("selector-type decode node = %q", got)
	}
	if got := decodeNodeName(av.StreamSelector{UseIndex: true, Index: 1}); got != "decode-1" {
		t.Fatalf("selector-index decode node = %q", got)
	}
	if got := decodeNodeName(av.StreamSelector{}); got != "decode" {
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

func TestFrameDurationNSContracts(t *testing.T) {
	if got := frameDurationNS(&av.Frame{Duration: av.Duration{Value: 20, Base: av.TimeBase{Num: 1, Den: 1000}}}); got != int64(20*time.Millisecond) {
		t.Fatalf("explicit frame duration = %d", got)
	}
	if got := frameDurationNS(&av.Frame{Audio: &av.AudioFrame{SampleRate: 48000, Samples: 960}}); got != int64(20*time.Millisecond) {
		t.Fatalf("audio-derived frame duration = %d", got)
	}
	if got := frameDurationNS(&av.Frame{}); got != 0 {
		t.Fatalf("empty frame duration = %d, want 0", got)
	}
}
