package fileargs

import (
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
)

func TestParseFileSinkCanonicalFields(t *testing.T) {
	sink, err := ParseFileSink([]Arg{
		{Key: "location", Value: "out.webm"},
		{Key: "format", Value: "WebM"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if sink.Location != "out.webm" || sink.Format != av.FormatWebM {
		t.Fatalf("sink = %+v", sink)
	}
}

func TestParseFileSinkRejectsDuplicateAliasesAndUnknowns(t *testing.T) {
	for _, tc := range []struct {
		name       string
		args       []Arg
		field      string
		suggestion string
	}{
		{
			name:       "positional",
			args:       []Arg{{Value: "out.webm"}},
			field:      "location",
			suggestion: "use location=out.webm",
		},
		{
			name:       "path",
			args:       []Arg{{Key: "path", Value: "out.webm"}},
			field:      "path",
			suggestion: "use location=<path>",
		},
		{
			name:       "file",
			args:       []Arg{{Key: "file", Value: "out.webm"}},
			field:      "file",
			suggestion: "use location=<path>",
		},
		{
			name:       "container",
			args:       []Arg{{Key: "container", Value: "webm"}},
			field:      "container",
			suggestion: "use format=<id>",
		},
		{
			name:       "unknown",
			args:       []Arg{{Key: "mode", Value: "fast"}},
			field:      "mode",
			suggestion: "use location=<path>",
		},
		{
			name: "duplicate",
			args: []Arg{
				{Key: "location", Value: "first.webm"},
				{Key: "location", Value: "second.webm"},
			},
			field:      "location",
			suggestion: "keep only one location=... value",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseFileSink(tc.args)
			var optErr *Error
			if !errors.As(err, &optErr) ||
				optErr.Field != tc.field ||
				!stringSliceContains(optErr.Suggestions, tc.suggestion) {
				t.Fatalf("err = %+v, want field %q suggestion %q", err, tc.field, tc.suggestion)
			}
		})
	}
}

func TestParseFileSinkMapUsesStableKeys(t *testing.T) {
	sink, err := ParseFileSinkMap(map[string]string{
		"format":   "ivf",
		"location": "out.ivf",
	})
	if err != nil {
		t.Fatal(err)
	}
	if sink.Location != "out.ivf" || sink.Format != av.FormatIVF {
		t.Fatalf("sink = %+v", sink)
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
