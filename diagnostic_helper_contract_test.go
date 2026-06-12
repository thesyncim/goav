package goav

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/shape"
)

func TestRecipeDiagnosticHelperContracts(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code errcode.Code
	}{
		{
			name: "nil recipe",
			err:  nilRecipeError("compile job", "nil job"),
			code: errcode.JobInvalid,
		},
		{
			name: "runtime missing",
			err:  runtimeMissingError("compile job"),
			code: errcode.RuntimeMissing,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertBuildErrorCode(t, tt.err, tt.code)
			if !errors.Is(tt.err, ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want ErrUnsupportedBuild cause", tt.err)
			}
		})
	}

	if got := resizeModesFromStrings(nil); got != nil {
		t.Fatalf("nil resize modes = %#v, want nil", got)
	}
	modes := resizeModesFromStrings([]string{"exact", "", "fit", "fill"})
	wantModes := []filter.ResizeMode{filter.ResizeExact, filter.ResizeFit, filter.ResizeFill}
	if !reflect.DeepEqual(modes, wantModes) {
		t.Fatalf("resize modes = %#v, want %#v", modes, wantModes)
	}

	if got := joinCodecIDs(nil); got != "" {
		t.Fatalf("empty codec ids = %q, want empty", got)
	}
	if got := joinCodecIDs([]av.CodecID{av.CodecVP8, "", av.CodecOpus, av.CodecID("x_custom")}); got != "vp8,opus,x_custom" {
		t.Fatalf("codec ids = %q", got)
	}
}

func TestOutputFormatProbeErrorContract(t *testing.T) {
	output := format.Output{
		Name:     "archive",
		URI:      "file://archive.custom",
		Protocol: av.ProtocolFile,
		MIMEType: "video/custom",
	}
	err := outputFormatProbeError(output, 3, format.ErrNotFound)
	assertBuildErrorCode(t, err, errcode.OutputFormatUnknown)
	if !errors.Is(err, format.ErrNotFound) {
		t.Fatalf("err = %v, want format.ErrNotFound cause", err)
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) {
		t.Fatalf("err = %T, want *BuildError", err)
	}
	wantDetails := []string{
		"name=archive",
		"uri=file://archive.custom",
		"protocol=file",
		"mime=video/custom",
	}
	if !reflect.DeepEqual(buildErr.Details, wantDetails) {
		t.Fatalf("details = %#v, want %#v", buildErr.Details, wantDetails)
	}

	cause := errors.New("probe crashed")
	if got := outputFormatProbeError(output, 3, cause); got != cause {
		t.Fatalf("non-not-found cause = %v, want original cause", got)
	}
}

func TestCustomStreamPredicateAndPreferenceDropReasons(t *testing.T) {
	match := MatchStream(func(stream av.Stream) bool {
		return strings.HasPrefix(string(stream.ID), "external-") && stream.Codec.ID == av.CodecOpus
	})
	if !match.matches(av.Stream{ID: "external-audio", Codec: av.CodecParameters{ID: av.CodecOpus}}) {
		t.Fatal("custom stream predicate did not match")
	}
	if match.matches(av.Stream{ID: "internal-audio", Codec: av.CodecParameters{ID: av.CodecOpus}}) {
		t.Fatal("custom stream predicate matched the wrong stream")
	}
	if got := match.description(); got != "custom" {
		t.Fatalf("description = %q, want custom", got)
	}

	ambiguous := &shapeAdapterSelectionError{
		media:      av.MediaVideo,
		needed:     shape.AllowResize(),
		candidates: []string{"resize-a", "resize-b"},
		cause:      errShapeAdapterAmbiguous,
	}
	if !errors.Is(ambiguous, errShapeAdapterAmbiguous) {
		t.Fatalf("ambiguous error = %v, want errShapeAdapterAmbiguous", ambiguous)
	}
	if got := ambiguous.Error(); got != errShapeAdapterAmbiguous.Error() {
		t.Fatalf("ambiguous Error() = %q", got)
	}
	if got := preferenceDropReason(ambiguous); !strings.Contains(got, "resize-a, resize-b") {
		t.Fatalf("ambiguous preference reason = %q", got)
	}

	missing := &shapeAdapterSelectionError{
		media:  av.MediaAudio,
		needed: shape.AllowResample(),
		cause:  errShapeAdapterMissing,
	}
	if got := preferenceDropReason(missing); got != "no registered adapter can perform the preferred conversion" {
		t.Fatalf("missing preference reason = %q", got)
	}
	if got := preferenceDropReason(errors.New("plain")); got != "the preferred conversion cannot be planned" {
		t.Fatalf("plain preference reason = %q", got)
	}
}
