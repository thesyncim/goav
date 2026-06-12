package goav

import (
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

func TestBytesEqualPreservesNilDistinction(t *testing.T) {
	if !bytesEqual(nil, nil) {
		t.Fatal("nil byte slices should compare equal")
	}
	if bytesEqual(nil, []byte{}) {
		t.Fatal("nil and empty byte slices should remain distinct")
	}
	if !bytesEqual([]byte{1, 2, 3}, []byte{1, 2, 3}) {
		t.Fatal("equal byte slices did not compare equal")
	}
	if bytesEqual([]byte{1, 2, 3}, []byte{1, 2, 4}) {
		t.Fatal("different byte slices compared equal")
	}
}

func TestMergeDecodeCodecSpecContracts(t *testing.T) {
	base := codec.Codec(av.CodecID("x-base-audio"), av.MediaAudio,
		codec.Bitrate(32_000),
		codec.Profile("base-profile"),
		codec.Channels(codec.Stereo),
		codec.SampleRate(48_000),
	)
	base.Parameters.Attributes = av.Metadata{"base": "keep"}
	base.Parameters.ExtraData = av.Buffer{Bytes: []byte{0x01}, Ownership: av.BufferImmutable}

	if got := mergeDecodeCodecSpec(base, codec.CodecSpec{}); !codecSpecEqual(got, base) {
		t.Fatalf("zero override = %#v, want base %#v", got, base)
	}

	control := func(any) error { return nil }
	override := codec.CodecSpec{
		ID:   av.CodecID("x-custom-audio"),
		Type: av.MediaAudio,
		Parameters: av.CodecParameters{
			ID:            av.CodecID("x-custom-audio"),
			Type:          av.MediaAudio,
			Profile:       "decode-profile",
			Level:         "2",
			ClockRate:     16_000,
			SampleRate:    16_000,
			Channels:      codec.Mono,
			ChannelLayout: "mono",
			Width:         320,
			Height:        180,
			PixelFormat:   av.PixelFormatI420,
			SampleFormat:  av.SampleFormatF32,
			ExtraData:     av.Buffer{Bytes: []byte{0xaa, 0xbb}, Ownership: av.BufferImmutable},
			Attributes:    av.Metadata{"vendor": "external"},
		},
		Settings: codec.CodecSettings{
			Bitrate:          96_000,
			Framerate:        av.Duration{Value: 1, Base: av.TimeBase{Num: 1, Den: 30}},
			KeyframeInterval: 60,
			Profile:          "settings-profile",
			Level:            "settings-level",
			Channels:         codec.Mono,
			ChannelLayout:    "mono",
			ChannelsSet:      true,
			SampleRate:       16_000,
			SampleRateSet:    true,
			ClockRate:        16_000,
			Control:          control,
		},
	}

	merged := mergeDecodeCodecSpec(base, override)
	if merged.ID != override.ID || merged.Type != override.Type {
		t.Fatalf("merged codec identity = %s/%s, want %s/%s", merged.ID, merged.Type, override.ID, override.Type)
	}
	if !codecParametersEqual(merged.Parameters, override.Parameters) {
		t.Fatalf("merged parameters = %#v, want %#v", merged.Parameters, override.Parameters)
	}
	if merged.Settings.Bitrate != override.Settings.Bitrate ||
		merged.Settings.Framerate != override.Settings.Framerate ||
		merged.Settings.KeyframeInterval != override.Settings.KeyframeInterval ||
		merged.Settings.Profile != override.Settings.Profile ||
		merged.Settings.Level != override.Settings.Level ||
		merged.Settings.Channels != override.Settings.Channels ||
		merged.Settings.ChannelLayout != override.Settings.ChannelLayout ||
		!merged.Settings.ChannelsSet ||
		merged.Settings.SampleRate != override.Settings.SampleRate ||
		!merged.Settings.SampleRateSet ||
		merged.Settings.ClockRate != override.Settings.ClockRate ||
		merged.Settings.Control == nil {
		t.Fatalf("merged settings = %#v, want override settings", merged.Settings)
	}
}
