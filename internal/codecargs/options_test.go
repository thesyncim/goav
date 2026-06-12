package codecargs

import (
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

func TestParseOptionsMapsTypedAndCustomSettings(t *testing.T) {
	options, err := ParseOptions([]Arg{
		{Key: "bitrate", Value: "1.5M"},
		{Key: "fps", Value: "30000/1001"},
		{Key: "keyframe_interval", Value: "60"},
		{Key: "profile", Value: "main"},
		{Key: "level", Value: "5.1"},
		{Key: "channels", Value: "1"},
		{Key: "channel_layout", Value: "front-left"},
		{Key: "sample_rate", Value: "16000"},
		{Key: "clock_rate", Value: "90000"},
		{Key: "lookahead", Value: "deep"},
		{Key: "aq-mode", Value: "cyclic"},
	})
	if err != nil {
		t.Fatal(err)
	}
	spec := codec.Codec("x_acme", av.MediaAudio, options...)
	if spec.Settings.Bitrate != 1_500_000 ||
		spec.Settings.Framerate != (av.Duration{Value: 1001, Base: av.TimeBase{Num: 1, Den: 30000}}) ||
		spec.Settings.KeyframeInterval != 60 ||
		spec.Settings.Profile != "main" ||
		spec.Settings.Level != "5.1" ||
		spec.Parameters.Channels != 1 ||
		spec.Parameters.ChannelLayout != "front-left" ||
		spec.Parameters.SampleRate != 16000 ||
		spec.Parameters.ClockRate != 90000 ||
		spec.Settings.Custom["lookahead"] != "deep" ||
		spec.Settings.Custom["aq-mode"] != "cyclic" {
		t.Fatalf("spec = %+v", spec)
	}
}

func TestReflectedSettingsUsageIncludesTaggedCodecFields(t *testing.T) {
	usage := ArgsUsage()
	for _, fragment := range []string{
		"[bitrate=<rate>]",
		"[fps=<n|n/d>]",
		"[keyframe_interval=<frames>]",
		"[channel_layout=<layout>]",
		"[native_key=value...]",
	} {
		if !strings.Contains(usage, fragment) {
			t.Fatalf("usage %q missing %q", usage, fragment)
		}
	}
	fields := SettingsFields()
	if len(fields) == 0 {
		t.Fatal("expected reflected settings fields")
	}
}

func TestParseOptionsRejectsDuplicateAliasSpellings(t *testing.T) {
	for _, tc := range []struct {
		field      string
		suggestion string
	}{
		{field: "rate", suggestion: "use bitrate=<rate> for bitrate"},
		{field: "id", suggestion: "use codec=<id>"},
		{field: "type", suggestion: "use media=<audio|video|subtitle>"},
		{field: "bitrate_bps", suggestion: "use bitrate=<rate>"},
		{field: "framerate", suggestion: "use fps=<n|n/d>"},
		{field: "samplerate", suggestion: "use sample_rate=<hz>"},
		{field: "ch", suggestion: "use channels=<n>"},
		{field: "clockrate", suggestion: "use clock_rate=<hz>"},
		{field: "keyint", suggestion: "use keyframe_interval=<frames>"},
		{field: "gop", suggestion: "use keyframe_interval=<frames>"},
	} {
		t.Run(tc.field, func(t *testing.T) {
			_, err := ParseOptions([]Arg{{Key: tc.field, Value: "1"}})
			var optErr *Error
			if !errors.As(err, &optErr) ||
				optErr.Field != tc.field ||
				!stringSliceContains(optErr.Suggestions, tc.suggestion) {
				t.Fatalf("err = %+v, want field %q suggestion %q", err, tc.field, tc.suggestion)
			}
		})
	}
}

func TestParseOptionsRejectsDuplicateCanonicalAndCustomSettings(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []Arg
		want string
	}{
		{name: "canonical", args: []Arg{{Key: "bitrate", Value: "1k"}, {Key: "bitrate", Value: "2k"}}, want: "bitrate"},
		{name: "metadata", args: []Arg{{Key: "codec", Value: "av1"}, {Key: "codec", Value: "vp8"}}, want: "codec"},
		{name: "custom", args: []Arg{{Key: "lookahead", Value: "deep"}, {Key: "lookahead", Value: "shallow"}}, want: "lookahead"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseOptions(tc.args)
			var optErr *Error
			if !errors.As(err, &optErr) ||
				optErr.Field != tc.want ||
				!stringSliceContains(optErr.Suggestions, "keep only one "+tc.want+"=... value") {
				t.Fatalf("err = %+v, want duplicate field %q", err, tc.want)
			}
		})
	}
}

func TestParseOptionsMapKeepsExplicitClockRateOverSampleRate(t *testing.T) {
	options, err := ParseOptionsMap(map[string]string{
		"clock_rate":  "90000",
		"sample_rate": "16000",
	})
	if err != nil {
		t.Fatal(err)
	}
	spec := codec.Codec("x_audio", av.MediaAudio, options...)
	if spec.Parameters.SampleRate != 16000 || spec.Parameters.ClockRate != 90000 {
		t.Fatalf("parameters = %+v, want sample_rate 16000 clock_rate 90000", spec.Parameters)
	}
}

func TestBuildSpecUsesStandardConstructors(t *testing.T) {
	for _, tc := range []struct {
		id    av.CodecID
		media av.MediaType
	}{
		{id: av.CodecOpus, media: av.MediaAudio},
		{id: av.CodecVP8, media: av.MediaVideo},
		{id: av.CodecVP9, media: av.MediaVideo},
		{id: av.CodecH264, media: av.MediaVideo},
		{id: av.CodecAV1, media: av.MediaVideo},
		{id: "x_vendor", media: av.MediaAudio},
	} {
		spec := BuildSpec(tc.id, tc.media, codec.Bitrate(123))
		if spec.ID != tc.id || spec.Type != tc.media || spec.Settings.Bitrate != 123 {
			t.Fatalf("BuildSpec(%s) = %+v, want media %s bitrate 123", tc.id, spec, tc.media)
		}
	}
}

func stringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
