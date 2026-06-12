package goav

import (
	"errors"
	"reflect"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

func TestMetadataEqualContracts(t *testing.T) {
	if !metadataEqual(nil, nil) {
		t.Fatal("nil metadata should equal nil metadata")
	}
	if metadataEqual(nil, av.Metadata{}) {
		t.Fatal("nil metadata should not equal an empty non-nil map")
	}
	if !metadataEqual(av.Metadata{"profile": "main"}, av.Metadata{"profile": "main"}) {
		t.Fatal("equal metadata did not compare equal")
	}
	if metadataEqual(av.Metadata{"profile": "main"}, av.Metadata{"profile": "high"}) {
		t.Fatal("different values compared equal")
	}
	if metadataEqual(av.Metadata{"profile": "main"}, av.Metadata{"profile": "main", "level": "3.1"}) {
		t.Fatal("different lengths compared equal")
	}
}

func TestMergeCodecSettingsPreservesCustomControl(t *testing.T) {
	baseControl := func(any) error { return nil }
	base := codec.CodecSettings{
		Bitrate:          64_000,
		Framerate:        av.Duration{Value: 1, Base: av.TimeBase{Num: 1, Den: 24}},
		KeyframeInterval: 24,
		Profile:          "base",
		Level:            "1.0",
		Channels:         codec.Mono,
		SampleRate:       44_100,
		ClockRate:        44_100,
		ChannelLayout:    "mono",
		ChannelsSet:      true,
		SampleRateSet:    true,
		Control:          baseControl,
	}
	if got := mergeCodecSettings(base, codec.CodecSettings{}); !reflect.DeepEqual(codecSettingsSnapshot(got), codecSettingsSnapshot(base)) {
		t.Fatalf("zero override changed settings: got %+v want %+v", got, base)
	}

	controlErr := errors.New("native rejected")
	var native any
	override := codec.CodecSettings{
		Bitrate:          128_000,
		Framerate:        av.Duration{Value: 1001, Base: av.TimeBase{Num: 1, Den: 30000}},
		KeyframeInterval: 60,
		Profile:          "high",
		Level:            "3.1",
		Channels:         codec.Stereo,
		SampleRate:       48_000,
		ClockRate:        90_000,
		ChannelLayout:    "stereo",
		ChannelsSet:      true,
		SampleRateSet:    true,
		Control: func(value any) error {
			native = value
			return controlErr
		},
	}
	got := mergeCodecSettings(base, override)
	if got.Bitrate != 128_000 ||
		got.Framerate != override.Framerate ||
		got.KeyframeInterval != 60 ||
		got.Profile != "high" ||
		got.Level != "3.1" ||
		got.Channels != codec.Stereo ||
		got.SampleRate != 48_000 ||
		got.ClockRate != 90_000 ||
		got.ChannelLayout != "stereo" ||
		!got.ChannelsSet ||
		!got.SampleRateSet {
		t.Fatalf("merged settings = %+v", got)
	}
	if got.Control == nil {
		t.Fatal("merged settings lost custom control callback")
	}
	if err := got.Control("native-encoder"); !errors.Is(err, controlErr) || native != "native-encoder" {
		t.Fatalf("control err/native = %v/%v", err, native)
	}
}

func codecSettingsSnapshot(settings codec.CodecSettings) codec.CodecSettings {
	settings.Control = nil
	return settings
}
