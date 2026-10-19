package goav

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
)

func TestRecipeDiagnosticHelperContracts(t *testing.T) {
	if got := resizeModesFromStrings(nil); got != nil {
		t.Fatalf("nil resize modes = %#v, want nil", got)
	}
	modes := resizeModesFromStrings([]string{"exact", "", "fit", "fill"})
	wantModes := []filter.ResizeMode{filter.ResizeExact, filter.ResizeFit, filter.ResizeFill}
	if !reflect.DeepEqual(modes, wantModes) {
		t.Fatalf("resize modes = %#v, want %#v", modes, wantModes)
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
	assertBuildErrorCode(t, err, outputFormatUnknownCode)
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
	if !reflect.DeepEqual(buildErr.DetailLines(), wantDetails) {
		t.Fatalf("details = %#v, want %#v", buildErr.DetailLines(), wantDetails)
	}
	if got, ok := buildErr.Detail[av.ProtocolID]("protocol"); !ok || got != av.ProtocolFile {
		t.Fatalf("protocol detail = %#v, %v; want %q, true", got, ok, av.ProtocolFile)
	}
	if got, ok := buildErr.Detail[string]("mime"); !ok || got != "video/custom" {
		t.Fatalf("mime detail = %#v, %v; want video/custom, true", got, ok)
	}
	if len(buildErr.fields) != len(wantDetails) {
		t.Fatalf("typed fields = %#v, want one field per rendered detail", buildErr.fields)
	}
	if len(buildErr.fixes) == 0 {
		t.Fatal("typed fixes are empty")
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
	if !match.Matches(av.Stream{ID: "external-audio", Codec: av.CodecParameters{ID: av.CodecOpus}}) {
		t.Fatal("custom stream predicate did not match")
	}
	if match.Matches(av.Stream{ID: "internal-audio", Codec: av.CodecParameters{ID: av.CodecOpus}}) {
		t.Fatal("custom stream predicate matched the wrong stream")
	}
	if got := match.Description(); got != "custom" {
		t.Fatalf("description = %q, want custom", got)
	}

}
