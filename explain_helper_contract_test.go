package goav

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/plan"
)

func TestWorkOperationEncodeCodecContracts(t *testing.T) {
	if got := workOperationEncodeCodec(compile.WorkOperation{
		Kind:  plan.OpEncode,
		Codec: codec.Codec(av.CodecID("x_custom"), av.MediaAudio),
	}); got != av.CodecID("x_custom") {
		t.Fatalf("work operation codec = %q, want x_custom", got)
	}
	if got := workOperationEncodeCodec(compile.WorkOperation{Kind: plan.OpEncode, Component: "x_fallback"}); got != av.CodecID("x_fallback") {
		t.Fatalf("operation component codec = %q, want x_fallback", got)
	}
	if got := workOperationEncodeCodec(compile.WorkOperation{Kind: plan.OpEncode, Component: "encoder"}); got != "" {
		t.Fatalf("generic encoder component codec = %q, want empty", got)
	}
	if got := workOperationEncodeCodec(compile.WorkOperation{Kind: plan.OpEncode}); got != "" {
		t.Fatalf("empty component codec = %q, want empty", got)
	}
}

func TestExplainDecodeCodecDeferredWarning(t *testing.T) {
	operation := compile.WorkOperation{ID: "audio/000/decode", Kind: plan.OpDecode, Component: "decoder"}
	_, warnings := appendWorkBranchOperationRequirements(nil, recipeResolved{}, compile.WorkBranch{
		Name:       "audio",
		Operations: []string{operation.ID},
	}, map[string]compile.WorkOperation{operation.ID: operation}, nil)
	if len(warnings) != 1 || warnings[0].Code != compile.DiagnosticDecodeCodecDeferred ||
		warnings[0].Node != "audio" ||
		!explainSuggestionsContain(warnings[0].Suggestions, "declare the provider codec intent") {
		t.Fatalf("warnings = %+v, want decode_codec_deferred with provider-codec guidance", warnings)
	}
}

func TestExplainRequirementsUseWorkOperationCodec(t *testing.T) {
	operation := compile.WorkOperation{
		ID:        "web/000/encode",
		Kind:      plan.OpEncode,
		Component: "encoder",
		Codec:     codec.VP9(),
	}
	requirements, warnings := appendWorkBranchOperationRequirements(nil, recipeResolved{}, compile.WorkBranch{
		Name:       "web",
		Operations: []string{operation.ID},
	}, map[string]compile.WorkOperation{operation.ID: operation}, nil)
	if len(warnings) != 0 || len(requirements) != 1 ||
		requirements[0].Kind != "encoder" ||
		requirements[0].Codec != av.CodecVP9 {
		t.Fatalf("requirements=%+v warnings=%+v, want VP9 encoder requirement from work operation", requirements, warnings)
	}
}

func TestExplainStreamsUseWorkStreamOperations(t *testing.T) {
	streams := explainStreams([]compile.WorkStream{{
		Name:   "preview",
		Select: plan.StreamSelect{Type: av.MediaVideo},
		Operations: []recipeir.Operation{
			opDecode(codec.VP8()),
			opEncode(codec.VP9()),
		},
		Destinations: []string{"webm"},
	}})
	if len(streams) != 1 {
		t.Fatalf("streams = %d, want 1", len(streams))
	}
	stream := streams[0]
	if !stream.Decode || stream.Encode.ID != av.CodecVP9 {
		t.Fatalf("stream decode=%v encode=%q, want decode and VP9 encode", stream.Decode, stream.Encode.ID)
	}
	if len(stream.Operations) != 2 || stream.Operations[0].Kind != plan.OpDecode || stream.Operations[1].Kind != plan.OpEncode {
		t.Fatalf("stream operations = %+v, want decode then encode", stream.Operations)
	}
	if len(stream.Destinations) != 1 || stream.Destinations[0] != "webm" {
		t.Fatalf("stream destinations = %+v, want webm", stream.Destinations)
	}
}

func explainSuggestionsContain(suggestions []string, fragment string) bool {
	for _, suggestion := range suggestions {
		if strings.Contains(suggestion, fragment) {
			return true
		}
	}
	return false
}

func TestExplainPreflightErrorWarning(t *testing.T) {
	var report plan.Report
	annotatePlanReportError(&report, errors.New("probe exploded"))
	if len(report.Warnings) != 1 ||
		report.Warnings[0].Code != compile.DiagnosticExplainPreflightError ||
		report.Warnings[0].Message != "probe exploded" {
		t.Fatalf("warnings = %+v, want explain_preflight_error", report.Warnings)
	}
}

func TestExplainAdapterStatusContracts(t *testing.T) {
	if got := codecFactoryStatus(nil); got != "available" {
		t.Fatalf("nil codec factory status = %q", got)
	}
	if got := codecFactoryStatus(codec.ErrUnavailable); got != "unavailable" {
		t.Fatalf("unavailable codec factory status = %q", got)
	}
	if got := codecFactoryStatus(errors.New("missing")); got != "missing" {
		t.Fatalf("missing codec factory status = %q", got)
	}

	tests := []struct {
		code errcode.Code
		want string
	}{
		{transformAdapterIncompatibleCode, "incompatible"},
		{encodeAdapterUnavailableCode, "unavailable"},
		{inputFormatUnknownCode, "unknown"},
		{encodeAdapterMissingCode, "missing"},
	}
	for _, tt := range tests {
		if got := adapterRequirementStatus(tt.code); got != tt.want {
			t.Fatalf("adapterRequirementStatus(%q) = %q, want %q", tt.code, got, tt.want)
		}
	}
}

func TestAppendMissingRequirementUpsertsByIdentity(t *testing.T) {
	requirements := []plan.Requirement{
		{Kind: "encoder", Name: "opus", RequiredBy: "audio", Status: "old"},
		{Kind: "muxer", Name: "webm", RequiredBy: "archive", Status: "missing"},
	}
	updated := appendMissingRequirement(requirements, plan.Requirement{
		Kind:       "encoder",
		Name:       "opus",
		RequiredBy: "audio",
		Status:     "new",
	})
	want := []plan.Requirement{
		{Kind: "encoder", Name: "opus", RequiredBy: "audio", Status: "new"},
		{Kind: "muxer", Name: "webm", RequiredBy: "archive", Status: "missing"},
	}
	if !reflect.DeepEqual(updated, want) {
		t.Fatalf("updated requirements = %#v, want %#v", updated, want)
	}

	appended := appendMissingRequirement(updated, plan.Requirement{
		Kind:       "encoder",
		Name:       "opus",
		RequiredBy: "preview",
		Status:     "missing",
	})
	if len(appended) != 3 || appended[2].RequiredBy != "preview" {
		t.Fatalf("appended requirements = %#v, want second owner appended", appended)
	}
}
