package transformargs

import (
	"errors"
	"testing"
)

func TestParseResizeCanonicalFields(t *testing.T) {
	resize, err := ParseResize([]Arg{
		{Key: "width", Value: "854"},
		{Key: "height", Value: "480"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resize.Width != 854 || resize.Height != 480 {
		t.Fatalf("resize = %+v", resize)
	}
}

func TestParseResizeRejectsDuplicateForms(t *testing.T) {
	for _, tc := range []struct {
		name       string
		args       []Arg
		field      string
		suggestion string
	}{
		{
			name:       "positional",
			args:       []Arg{{Value: "854x480"}},
			field:      "resize",
			suggestion: "use width=854 height=480",
		},
		{
			name:       "w",
			args:       []Arg{{Key: "w", Value: "854"}},
			field:      "w",
			suggestion: "use width=<px>",
		},
		{
			name:       "h",
			args:       []Arg{{Key: "h", Value: "480"}},
			field:      "h",
			suggestion: "use height=<px>",
		},
		{
			name:       "size",
			args:       []Arg{{Key: "size", Value: "854x480"}},
			field:      "size",
			suggestion: "use width=<px> height=<px>",
		},
		{
			name:       "unknown",
			args:       []Arg{{Key: "mode", Value: "fast"}},
			field:      "mode",
			suggestion: "use width=<px>",
		},
		{
			name: "duplicate",
			args: []Arg{
				{Key: "width", Value: "854"},
				{Key: "width", Value: "640"},
				{Key: "height", Value: "480"},
			},
			field:      "width",
			suggestion: "keep only one width=... value",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseResize(tc.args)
			var optErr *Error
			if !errors.As(err, &optErr) ||
				optErr.Field != tc.field ||
				!stringSliceContains(optErr.Suggestions, tc.suggestion) {
				t.Fatalf("err = %+v, want field %q suggestion %q", err, tc.field, tc.suggestion)
			}
		})
	}
}

func TestParseResampleCanonicalFields(t *testing.T) {
	resample, err := ParseResample([]Arg{
		{Key: "sample_rate", Value: "48000"},
		{Key: "channels", Value: "2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resample.SampleRate != 48000 || resample.Channels != 2 {
		t.Fatalf("resample = %+v", resample)
	}
}

func TestParseResampleRejectsDuplicateForms(t *testing.T) {
	for _, tc := range []struct {
		name       string
		args       []Arg
		field      string
		suggestion string
	}{
		{
			name:       "positional",
			args:       []Arg{{Value: "48000"}, {Value: "2"}},
			field:      "resample",
			suggestion: "use sample_rate=48000 channels=2",
		},
		{
			name:       "rate",
			args:       []Arg{{Key: "rate", Value: "48000"}},
			field:      "rate",
			suggestion: "use sample_rate=<hz>",
		},
		{
			name:       "ch",
			args:       []Arg{{Key: "ch", Value: "2"}},
			field:      "ch",
			suggestion: "use channels=<n>",
		},
		{
			name:       "unknown",
			args:       []Arg{{Key: "layout", Value: "stereo"}},
			field:      "layout",
			suggestion: "use sample_rate=<hz>",
		},
		{
			name: "duplicate",
			args: []Arg{
				{Key: "sample_rate", Value: "48000"},
				{Key: "sample_rate", Value: "44100"},
				{Key: "channels", Value: "2"},
			},
			field:      "sample_rate",
			suggestion: "keep only one sample_rate=... value",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseResample(tc.args)
			var optErr *Error
			if !errors.As(err, &optErr) ||
				optErr.Field != tc.field ||
				!stringSliceContains(optErr.Suggestions, tc.suggestion) {
				t.Fatalf("err = %+v, want field %q suggestion %q", err, tc.field, tc.suggestion)
			}
		})
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
