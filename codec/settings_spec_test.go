package codec

import (
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
)

func TestBitrateEventRoundTripAndValidation(t *testing.T) {
	event := &av.Event{Type: av.EventBitrateChanged, Metadata: BitrateMetadata(128000)}
	if got, ok := EventBitrate(event); !ok || got != 128000 {
		t.Fatalf("EventBitrate() = %d, %v; want 128000, true", got, ok)
	}

	tests := map[string]*av.Event{
		"nil":      nil,
		"missing":  {Type: av.EventBitrateChanged},
		"garbage":  {Type: av.EventBitrateChanged, Metadata: av.Metadata{av.MetadataBitrate: "fast"}},
		"zero":     {Type: av.EventBitrateChanged, Metadata: BitrateMetadata(0)},
		"negative": {Type: av.EventBitrateChanged, Metadata: BitrateMetadata(-1)},
	}
	for name, bad := range tests {
		t.Run(name, func(t *testing.T) {
			if got, ok := EventBitrate(bad); ok {
				t.Fatalf("EventBitrate() = %d, true; want rejection", got)
			}
		})
	}
}

func TestCodecSettingsOptionsAreLastWinsAndCustomFriendly(t *testing.T) {
	var settings CodecSettings
	var native any
	controlErr := errors.New("native rejected")

	Bitrate(96000)(&settings)
	FPS(30000, 1001)(&settings)
	KeyframeInterval(60)(&settings)
	Profile("voice")(&settings)
	Level("3.1")(&settings)
	Setting("lookahead", "deep")(&settings)
	Setting("lookahead", "shallow")(&settings)
	Setting("", "ignored")(&settings)
	Channels(Stereo)(&settings)
	Channels(6)(&settings)
	SampleRate(48000)(&settings)
	ClockRate(90000)(&settings)
	Control(nil)(&settings)
	Control(func(value any) error {
		native = value
		return controlErr
	})(&settings)

	if settings.Bitrate != 96000 ||
		settings.Framerate != (av.Duration{Value: 1001, Base: av.TimeBase{Num: 1, Den: 30000}}) ||
		settings.KeyframeInterval != 60 ||
		settings.Profile != "voice" ||
		settings.Level != "3.1" {
		t.Fatalf("settings = %+v", settings)
	}
	if got := settings.Custom["lookahead"]; got != "shallow" || len(settings.Custom) != 1 {
		t.Fatalf("custom settings = %+v", settings.Custom)
	}
	if settings.Channels != 6 || !settings.ChannelsSet || settings.ChannelLayout != "" {
		t.Fatalf("channels = %d set=%v layout=%q, want custom 6 with no stale layout", settings.Channels, settings.ChannelsSet, settings.ChannelLayout)
	}
	if settings.SampleRate != 48000 || !settings.SampleRateSet || settings.ClockRate != 90000 {
		t.Fatalf("sample/clock = %d/%d set=%v", settings.SampleRate, settings.ClockRate, settings.SampleRateSet)
	}
	if settings.Control == nil {
		t.Fatal("Control option did not install callback")
	}
	if err := settings.Control("encoder"); !errors.Is(err, controlErr) || native != "encoder" {
		t.Fatalf("Control callback err/native = %v/%v", err, native)
	}

	FPS(0)(&settings)
	if settings.Framerate.Value != -1 {
		t.Fatalf("invalid FPS should mark framerate invalid, got %+v", settings.Framerate)
	}
	FPS(30, 0)(&settings)
	if settings.Framerate.Value != -1 {
		t.Fatalf("invalid FPS denominator should mark framerate invalid, got %+v", settings.Framerate)
	}
}

func TestCodecSpecsApplyTypedSettingsToParameters(t *testing.T) {
	opus := Opus(
		nil,
		Bitrate(128000),
		Channels(Mono),
		SampleRate(16000),
		ClockRate(8000),
		Profile("lowdelay"),
		Level("1"),
		KeyframeInterval(25),
		FPS(50),
	)
	if opus.ID != av.CodecOpus || opus.Type != av.MediaAudio {
		t.Fatalf("Opus id/type = %q/%q", opus.ID, opus.Type)
	}
	if opus.Parameters.ID != av.CodecOpus ||
		opus.Parameters.Type != av.MediaAudio ||
		opus.Parameters.Channels != Mono ||
		opus.Parameters.ChannelLayout != "mono" ||
		opus.Parameters.SampleRate != 16000 ||
		opus.Parameters.ClockRate != 8000 {
		t.Fatalf("Opus parameters = %+v", opus.Parameters)
	}
	if opus.Settings.Bitrate != 128000 ||
		opus.Settings.Profile != "lowdelay" ||
		opus.Settings.Level != "1" ||
		opus.Settings.KeyframeInterval != 25 ||
		opus.Settings.Framerate != (av.Duration{Value: 1, Base: av.TimeBase{Num: 1, Den: 50}}) {
		t.Fatalf("Opus settings = %+v", opus.Settings)
	}

	custom := Codec(av.CodecID("x_acme_audio"), av.MediaAudio, Channels(6), SampleRate(44100))
	if custom.Parameters.ID != "x_acme_audio" ||
		custom.Parameters.Type != av.MediaAudio ||
		custom.Parameters.Channels != 6 ||
		custom.Parameters.ChannelLayout != "" ||
		custom.Parameters.SampleRate != 44100 ||
		custom.Parameters.ClockRate != 44100 {
		t.Fatalf("custom codec parameters = %+v", custom.Parameters)
	}

	fallback := newCodecSpec(av.CodecID("x_fallback"), av.MediaData, av.CodecParameters{}, nil)
	if fallback.Parameters.ID != "x_fallback" || fallback.Parameters.Type != av.MediaData {
		t.Fatalf("fallback parameters = %+v", fallback.Parameters)
	}

	if !Auto().Auto || Auto().Copy {
		t.Fatalf("Auto() = %+v", Auto())
	}
	if !Copy().Copy || Copy().Auto {
		t.Fatalf("Copy() = %+v", Copy())
	}
}

func TestVideoCodecSpecsUseVideoDefaultsAndOptions(t *testing.T) {
	tests := []struct {
		name string
		spec Spec
		id   av.CodecID
	}{
		{name: "vp8", spec: VP8(Bitrate(1)), id: av.CodecVP8},
		{name: "vp9", spec: VP9(Bitrate(2)), id: av.CodecVP9},
		{name: "h264", spec: H264(Bitrate(3)), id: av.CodecH264},
		{name: "av1", spec: AV1(Bitrate(4)), id: av.CodecAV1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.spec.ID != tt.id ||
				tt.spec.Type != av.MediaVideo ||
				tt.spec.Parameters.ID != tt.id ||
				tt.spec.Parameters.Type != av.MediaVideo ||
				tt.spec.Parameters.ClockRate != 90000 {
				t.Fatalf("video spec = %+v", tt.spec)
			}
			if tt.spec.Settings.Bitrate == 0 {
				t.Fatalf("video option did not apply: %+v", tt.spec.Settings)
			}
		})
	}
}
