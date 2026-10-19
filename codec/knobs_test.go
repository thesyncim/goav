package codec

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
)

func knobbed() Descriptor {
	return Descriptor{
		ID:      av.CodecH264,
		Backend: BackendInfo{Name: "openh264", Kind: KindNative},
		Knobs: []Knob{
			{Name: "bitrate", Type: KnobInt, Min: 1000, Max: 100_000_000, Live: true},
			{Name: "gop", Type: KnobInt, Min: 1, Max: 1000},
			{Name: "profile", Type: KnobEnum, Values: []string{"baseline", "main", "high"}, Default: "high"},
			{Name: "rc", Type: KnobEnum, Values: []string{"cbr", "vbr", "cqp"}, Default: "vbr", Help: "rate control"},
			{Name: "realtime", Type: KnobBool},
			{Name: "denoise", Type: KnobBool},
		},
		RawKnobs: []Knob{{Name: "iMaxQp", Type: KnobInt, Min: 0, Max: 51}},
	}
}

func TestResolveKnobsMapsCommonAndScopedSettings(t *testing.T) {
	var s CodecSettings
	for _, option := range []Option{
		Bitrate(2_000_000), KeyframeInterval(60), Profile("Main"),
		Setting("rc", "cbr"),
		Setting("openh264.denoise", "on"),
		Setting("videotoolbox.allow_software", "1"),
		Setting("openh264-params", "iMaxQp=40"),
		Setting("x264-params", "whatever=1"),
	} {
		option(&s)
	}
	values, err := knobbed().ResolveKnobs(s, true)
	if err != nil {
		t.Fatalf("ResolveKnobs: %v", err)
	}
	if values.Int("bitrate") != 2_000_000 || values.Int("gop") != 60 || values.String("profile") != "main" || values.String("rc") != "cbr" {
		t.Fatalf("common settings = %+v", values)
	}
	if !values.Bool("denoise") || !values.Bool("realtime") || values.IsSet("realtime") {
		t.Fatalf("scoped/default knobs = %+v", values)
	}
	if raw := values.Raw(); !slices.Equal(raw, [][2]string{{"iMaxQp", "40"}}) {
		t.Fatalf("raw = %v", raw)
	}
	if defaults, _ := knobbed().ResolveKnobs(CodecSettings{}, false); defaults.String("rc") != "vbr" || defaults.IsSet("rc") || defaults.Bool("realtime") {
		t.Fatalf("defaults = %+v", defaults)
	}
}

func TestResolveKnobsRefusesWithTheFix(t *testing.T) {
	for name, tc := range map[string]struct {
		option Option
		want   string
	}{
		"misspelled":    {Setting("rcc", "cbr"), "takes no setting \"rcc\" (settings: bitrate, gop, profile, rc, realtime, denoise); did you mean rc?"},
		"bad enum":      {Setting("rc", "abr"), "rc=abr: want one of cbr, vbr, cqp"},
		"range":         {Bitrate(10), "bitrate=10 is outside 1000..100000000"},
		"undeclared":    {Level("4.1"), "takes no setting \"level\""},
		"raw key":       {Setting("openh264-params", "iMaxQP2=4"), "has no raw parameter \"iMaxQP2\"; did you mean iMaxQp?"},
		"raw range":     {Setting("openh264-params", "iMaxQp=99"), "iMaxQp=99 is outside 0..51"},
		"raw not a kv":  {Setting("openh264-params", "iMaxQp"), "raw parameter \"iMaxQp\" is not key=value"},
		"scoped typo":   {Setting("openh264.denoize", "1"), "did you mean denoise?"},
		"int not a num": {Setting("gop", "often"), "gop=often: want a whole number"},
	} {
		var s CodecSettings
		tc.option(&s)
		_, err := knobbed().ResolveKnobs(s, false)
		if !errors.Is(err, ErrInvalidSetting) || !strings.Contains(err.Error(), tc.want) || !strings.HasPrefix(err.Error(), "h264 openh264: ") {
			t.Errorf("%s: err = %v, want %q", name, err, tc.want)
		}
	}
}

func TestKnobStringSummarizesTheDeclaration(t *testing.T) {
	desc := knobbed()
	rc, _ := desc.Knob("RC")
	if got := rc.String(); got != "rc=cbr|vbr|cqp (default vbr): rate control" {
		t.Fatalf("rc = %q", got)
	}
	bitrate, _ := desc.Knob("bitrate")
	if got := bitrate.String(); got != "bitrate=1000..100000000 live" {
		t.Fatalf("bitrate = %q", got)
	}
}

func TestResolveKnobsWithoutSchemaPassesSettings(t *testing.T) {
	values, err := Descriptor{ID: av.CodecVP9}.ResolveKnobs(CodecSettings{Custom: av.Metadata{"cpu-used": "4"}}, false)
	if err != nil || values.String("cpu-used") != "4" {
		t.Fatalf("schemaless = %+v, %v", values, err)
	}
}

func TestSizedAccessorsParseAtTheirSize(t *testing.T) {
	v := KnobValues{values: map[string]string{"n": "70000", "big": "5000000000", "on": "true", "neg": "-3"}}
	if v.Int32("n") != 70000 || v.Uint32("n") != 70000 || v.Uint16("n") != 0 {
		t.Fatalf("70000 = %d, %d, %d; uint16 cannot hold it", v.Int32("n"), v.Uint32("n"), v.Uint16("n"))
	}
	if v.Int32("big") != 0 || v.Uint32("big") != 0 || v.Int32("on") != 1 || v.Uint16("on") != 1 || v.Uint32("neg") != 0 || v.Int32("neg") != -3 {
		t.Fatal("sized accessors read out-of-range values as something other than 0")
	}
}
