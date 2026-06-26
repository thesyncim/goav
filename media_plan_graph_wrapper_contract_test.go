package goav

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

func TestGraphPlanNotReadyContracts(t *testing.T) {
	var gp graphPlan
	if gp.ready() {
		t.Fatal("zero graphPlan ready() = true, want false")
	}

	if _, err := gp.Describe(); !buildErrorHasCodeOperation(err, errcode.RecipeGraphUnsupported, "describe graph plan") {
		t.Fatalf("Describe() error = %v, want recipe_graph_unsupported describe error", err)
	}
	if _, err := gp.Build(context.Background()); !buildErrorHasCodeOperation(err, errcode.RecipeGraphUnsupported, "build graph plan") {
		t.Fatalf("Build() error = %v, want recipe_graph_unsupported build error", err)
	}

	err := gp.lower(context.Background(), nil, nil)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != graphPlanInvalidCode ||
		!strings.Contains(buildErr.Reason, "graph plan is not ready") {
		t.Fatalf("lower() error = %v, want graph_plan_invalid not-ready error", err)
	}
}

func TestMediaPlanPacketCopyStreamLowererRefusalContracts(t *testing.T) {
	if lowerer, ok, err := mediaPlanPacketCopyStreamLowererForState(nil); lowerer != nil || ok || err != nil {
		t.Fatalf("nil state lowerer = %T, %v, %v; want nil, false, nil", lowerer, ok, err)
	}

	noJob := &recipeCompileState{jobPresent: false}
	if stream, selected, ok := mediaPlanPacketCopyStream(noJob); stream.Name != "" || selected || ok {
		t.Fatalf("mediaPlanPacketCopyStream(no job) = %+v, %v, %v; want zero, false, false", stream, selected, ok)
	}

	copyOnly := streamIntent{
		Name: "audio",
		Operations: []operationSpec{{
			Kind:   plan.OpCopy,
			Encode: codec.Copy(),
		}},
	}
	copyState := &recipeCompileState{
		jobPresent: true,
		intent:     intent{Streams: []streamIntent{copyOnly}},
	}
	stream, selected, ok := mediaPlanPacketCopyStream(copyState)
	if !ok || !selected || stream.Name != "audio" {
		t.Fatalf("mediaPlanPacketCopyStream(copy) = %+v, %v, %v; want audio, true, true", stream, selected, ok)
	}
	if lowerer, ok, err := mediaPlanPacketCopyStreamLowererForState(copyState); lowerer != nil || ok || err != nil {
		t.Fatalf("copy stream without runtime lowerer = %T, %v, %v; want nil, false, nil", lowerer, ok, err)
	}
}

func TestStreamIntentPacketCopyOnlyContracts(t *testing.T) {
	copyOp := operationSpec{Kind: plan.OpCopy, Encode: codec.Copy()}
	packetTap := operationSpec{Kind: plan.OpTap, Tap: tapIntent{Domain: shape.DomainPacket, After: plan.OpCopy}}
	annotation := operationSpec{Kind: plan.OpShape, Auto: &shape.Policy{}}

	for name, tc := range map[string]struct {
		stream streamIntent
		want   bool
	}{
		"copy": {
			stream: streamIntent{Operations: []operationSpec{copyOp}},
			want:   true,
		},
		"copy tap annotation": {
			stream: streamIntent{Operations: []operationSpec{copyOp, packetTap, annotation}},
			want:   true,
		},
		"no copy": {
			stream: streamIntent{},
		},
		"decode": {
			stream: streamIntent{Operations: []operationSpec{{Kind: plan.OpDecode}, copyOp}},
		},
		"explicit codec": {
			stream: streamIntent{Operations: []operationSpec{{Kind: plan.OpCopy, Encode: codec.CodecSpec{ID: av.CodecOpus, Copy: true}}}},
		},
		"auto codec": {
			stream: streamIntent{Operations: []operationSpec{{Kind: plan.OpCopy, Encode: codec.CodecSpec{Auto: true, Copy: true}}}},
		},
		"copy operation without copy spec": {
			stream: streamIntent{Operations: []operationSpec{{Kind: plan.OpCopy}}},
		},
		"later copy operation without copy spec": {
			stream: streamIntent{Operations: []operationSpec{copyOp, {Kind: plan.OpCopy}}},
		},
		"frame tap": {
			stream: streamIntent{Operations: []operationSpec{
				copyOp,
				{Kind: plan.OpTap, Tap: tapIntent{Domain: shape.DomainFrame, After: plan.OpCopy}},
			}},
		},
		"tap after decode": {
			stream: streamIntent{Operations: []operationSpec{
				copyOp,
				{Kind: plan.OpTap, Tap: tapIntent{Domain: shape.DomainPacket, After: plan.OpDecode}},
			}},
		},
		"non annotation shape": {
			stream: streamIntent{Operations: []operationSpec{copyOp, {Kind: plan.OpShape}}},
		},
		"transform": {
			stream: streamIntent{Operations: []operationSpec{copyOp, {Kind: plan.OpTransform}}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if got := streamIntentPacketCopyOnly(tc.stream); got != tc.want {
				t.Fatalf("streamIntentPacketCopyOnly() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMediaPlanBranchComposerLowererRefusalContracts(t *testing.T) {
	for name, state := range map[string]*recipeCompileState{
		"nil":                    nil,
		"not branch composition": {},
		"no runtime input owner": {branchCompositionPresent: true, branchInputAttachment: InputSpec{}},
	} {
		t.Run(name, func(t *testing.T) {
			lowerer, ok, err := mediaPlanBranchComposerLowerer(state)
			if lowerer != nil || ok || err != nil {
				t.Fatalf("mediaPlanBranchComposerLowerer() = %T, %v, %v; want nil, false, nil", lowerer, ok, err)
			}
		})
	}
}

func buildErrorHasCodeOperation(err error, code errcode.Code, operation string) bool {
	var buildErr *BuildError
	return errors.As(err, &buildErr) && buildErr.Code == code && buildErr.Operation == operation
}
