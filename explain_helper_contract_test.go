package goav

import (
	"errors"
	"reflect"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/plan"
)

func TestOperationEncodeCodecContracts(t *testing.T) {
	stream := streamIntent{Operations: []operationSpec{
		{Kind: plan.OpEncode, Encode: codec.Codec(av.CodecID("x_custom"), av.MediaAudio)},
	}}
	if got := operationEncodeCodec(stream, true, plan.Operation{Kind: plan.OpEncode, Component: "encoder"}); got != av.CodecID("x_custom") {
		t.Fatalf("stream encode codec = %q, want x_custom", got)
	}
	if got := operationEncodeCodec(stream, false, plan.Operation{Kind: plan.OpEncode, Component: "x_fallback"}); got != av.CodecID("x_fallback") {
		t.Fatalf("operation component codec = %q, want x_fallback", got)
	}
	if got := operationEncodeCodec(streamIntent{}, false, plan.Operation{Kind: plan.OpEncode, Component: "encoder"}); got != "" {
		t.Fatalf("generic encoder component codec = %q, want empty", got)
	}
	if got := operationEncodeCodec(streamIntent{}, false, plan.Operation{Kind: plan.OpEncode}); got != "" {
		t.Fatalf("empty component codec = %q, want empty", got)
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
		{errcode.TransformAdapterIncompatible, "incompatible"},
		{errcode.EncodeAdapterUnavailable, "unavailable"},
		{errcode.InputFormatUnknown, "unknown"},
		{errcode.EncodeAdapterMissing, "missing"},
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
