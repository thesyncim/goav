package sourceargs

import (
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/internal/cliargs"
)

func TestParseGeneratedVideoCanonicalFields(t *testing.T) {
	source, err := ParseGeneratedVideo("testsrc", []Arg{
		{Value: "video"},
		{Key: "name", Value: "fixture"},
		{Key: "width", Value: "1920"},
		{Key: "height", Value: "1080"},
		{Key: "fps", Value: "30000/1001"},
		{Key: "duration", Value: "100ms"},
		{Key: "realtime", Value: "false"},
		{Key: "format", Value: "yuv420p"},
		{Key: "pattern", Value: "solid"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if source.Name != "fixture" ||
		source.Width != 1920 ||
		source.Height != 1080 ||
		source.FPS != (cliargs.FPS{Num: 30000, Den: 1001}) ||
		source.Frames != 3 ||
		source.Realtime ||
		source.PixelFormat != av.PixelFormatI420 ||
		source.Pattern != "solid" {
		t.Fatalf("source = %+v", source)
	}
}

func TestParseGeneratedVideoRejectsDuplicateForms(t *testing.T) {
	for _, tc := range []struct {
		name       string
		sourceName string
		args       []Arg
		field      string
		suggestion string
	}{
		{
			name:       "source alias",
			sourceName: "videosrc",
			args:       []Arg{{Value: "video"}},
			field:      "source",
			suggestion: "use testsrc video",
		},
		{
			name:       "missing video",
			sourceName: "testsrc",
			args:       []Arg{{Key: "width", Value: "16"}},
			field:      "testsrc",
			suggestion: "use testsrc video",
		},
		{
			name:       "size",
			sourceName: "testsrc",
			args:       []Arg{{Value: "video"}, {Key: "size", Value: "1920x1080"}},
			field:      "size",
			suggestion: "use width=<px> height=<px>",
		},
		{
			name:       "framerate",
			sourceName: "testsrc",
			args:       []Arg{{Value: "video"}, {Key: "framerate", Value: "30"}},
			field:      "framerate",
			suggestion: "use fps=<n|n/d>",
		},
		{
			name:       "live",
			sourceName: "testsrc",
			args:       []Arg{{Value: "video"}, {Key: "live", Value: "true"}},
			field:      "live",
			suggestion: "use realtime=<bool>",
		},
		{
			name:       "bool shorthand",
			sourceName: "testsrc",
			args:       []Arg{{Value: "video"}, {Key: "realtime", Value: "on"}},
			field:      "realtime",
			suggestion: "use realtime=true",
		},
		{
			name:       "pix_fmt",
			sourceName: "testsrc",
			args:       []Arg{{Value: "video"}, {Key: "pix_fmt", Value: "i420"}},
			field:      "pix_fmt",
			suggestion: "use format=i420",
		},
		{
			name:       "frames duration",
			sourceName: "testsrc",
			args:       []Arg{{Value: "video"}, {Key: "frames", Value: "1"}, {Key: "duration", Value: "1s"}},
			field:      "duration",
			suggestion: "use frames=<n>",
		},
		{
			name:       "duplicate width",
			sourceName: "testsrc",
			args:       []Arg{{Value: "video"}, {Key: "width", Value: "16"}, {Key: "width", Value: "32"}},
			field:      "width",
			suggestion: "keep only one width=... value",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseGeneratedVideo(tc.sourceName, tc.args)
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
