package goav

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

func TestPrepareBranchComposePlanEmptyContracts(t *testing.T) {
	_, _, err := prepareBranchComposePlan(branchComposePlan{})
	assertBranchComposeGraphPlanBuildError(t, err, branchComposePlanEmptyCode, "no branches")

	_, _, err = prepareBranchComposePlan(branchComposePlan{
		Branches: []branchComposeBranch{{Name: "preview"}},
	})
	assertBranchComposeGraphPlanBuildError(t, err, branchComposePlanEmptyCode, "no destinations")
}

func TestValidateBranchComposeBranchOperationContracts(t *testing.T) {
	copyRoute := branchComposeRoute{name: "copy", copy: true, sourceDomain: shape.DomainPacket}
	decodeRoute := branchComposeRoute{name: "decode", sourceDomain: shape.DomainPacket}
	encodeRoute := branchComposeRoute{
		name:         "encode",
		sourceDomain: shape.DomainPacket,
		request: encodeRequest{config: codec.EncodeConfig{
			Parameters: av.CodecParameters{ID: av.CodecVP8},
		}},
	}
	frameRoute := branchComposeRoute{name: "frame", sourceDomain: shape.DomainFrame}

	validCases := []struct {
		name       string
		route      branchComposeRoute
		operations []workOperation
	}{
		{name: "packet copy", route: copyRoute, operations: branchComposeOps(plan.OpSelect, plan.OpCopy)},
		{name: "packet decode", route: decodeRoute, operations: branchComposeOps(plan.OpSelect, plan.OpDecode)},
		{name: "packet encode", route: encodeRoute, operations: branchComposeOps(plan.OpSelect, plan.OpDecode, plan.OpEncode)},
		{name: "frame source", route: frameRoute, operations: branchComposeOps(plan.OpSelect)},
	}
	var graph mediaPlanBranchComposeGraph
	for _, tt := range validCases {
		t.Run("valid "+tt.name, func(t *testing.T) {
			if err := graph.validateBranchComposeBranchOperations(tt.route, tt.operations); err != nil {
				t.Fatalf("validateBranchComposeBranchOperations() error = %v", err)
			}
		})
	}

	sharedStage := &runtimeTestStage{name: "shared"}
	privateStage := &runtimeTestStage{name: "private"}
	errorCases := []struct {
		name       string
		route      branchComposeRoute
		operations []workOperation
		reason     string
	}{
		{name: "missing select", route: copyRoute, operations: branchComposeOps(plan.OpCopy), reason: "no select operation"},
		{name: "missing decode", route: decodeRoute, operations: branchComposeOps(plan.OpSelect), reason: "no decode operation"},
		{name: "unexpected decode", route: copyRoute, operations: branchComposeOps(plan.OpSelect, plan.OpDecode, plan.OpCopy), reason: "unexpected decode operation"},
		{name: "missing copy", route: copyRoute, operations: branchComposeOps(plan.OpSelect), reason: "no copy operation"},
		{
			name:       "shared operation count mismatch",
			route:      branchComposeRoute{name: "shared", copy: true, sourceDomain: shape.DomainPacket, sharedOperations: operationFactsFromSpecs([]operationSpec{{Kind: plan.OpStage, Stage: sharedStage}})},
			operations: branchComposeOps(plan.OpSelect, plan.OpCopy),
			reason:     "shared operations do not match",
		},
		{
			name:       "private operation count mismatch",
			route:      branchComposeRoute{name: "private", copy: true, sourceDomain: shape.DomainPacket, privateOperations: operationFactsFromSpecs([]operationSpec{{Kind: plan.OpStage, Stage: privateStage}})},
			operations: branchComposeOps(plan.OpSelect, plan.OpCopy),
			reason:     "operations do not match",
		},
		{name: "missing encode", route: encodeRoute, operations: branchComposeOps(plan.OpSelect, plan.OpDecode), reason: "no encode operation"},
		{name: "unexpected encode", route: copyRoute, operations: branchComposeOps(plan.OpSelect, plan.OpCopy, plan.OpEncode), reason: "unexpected encode operation"},
	}
	for _, tt := range errorCases {
		t.Run(tt.name, func(t *testing.T) {
			err := graph.validateBranchComposeBranchOperations(tt.route, tt.operations)
			assertBranchComposeGraphPlanBuildError(t, err, graphPlanInvalidCode, tt.reason)
		})
	}
}

func TestPrepareBranchComposeBranchOperationsContracts(t *testing.T) {
	graph := mediaPlanBranchComposeGraph{branches: []branchComposeRoute{{
		name: "encoded",
		request: encodeRequest{config: codec.EncodeConfig{
			Parameters: av.CodecParameters{ID: av.CodecVP8},
		}},
	}}}

	_, err := graph.prepareBranchComposeBranchOperations(map[string][]workOperation{
		"encoded": {{Kind: plan.OpEncode}},
	})
	assertBranchComposeGraphPlanBuildError(t, err, graphPlanInvalidCode, "encode operation has no node")

	operations, err := graph.prepareBranchComposeBranchOperations(map[string][]workOperation{
		"encoded": {{Kind: plan.OpEncode, Node: "encode-node", ShapeOut: shape.Packet(av.MediaVideo, av.CodecVP8)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	operation := operations["encoded"]
	if operation.encodeNode != "encode-node" || operation.encodeShape.Codec != av.CodecVP8 {
		t.Fatalf("branch operation = %+v, want encode-node VP8", operation)
	}
}

func TestValidateBranchComposeDestinationOperationContracts(t *testing.T) {
	sink := SinkFunc("frames", func(context.Context, Message) error { return nil })
	sinkTarget := branchComposeTargetRoute{sink: sink}
	byteTarget := branchComposeTargetRoute{target: format.Output{Name: "archive.webm"}}

	errorCases := []struct {
		name      string
		operation graphPlanDestinationOperation
		target    branchComposeTargetRoute
		reason    string
	}{
		{
			name:      "missing node",
			operation: graphPlanDestinationOperation{Name: "archive", Kind: plan.OpMux},
			target:    byteTarget,
			reason:    "destination operation has no node",
		},
		{
			name:      "mux operation for sink target",
			operation: graphPlanDestinationOperation{Name: "frames", Node: "mux-node", Kind: plan.OpMux},
			target:    sinkTarget,
			reason:    "kind does not match sink destination",
		},
		{
			name:      "sink operation for byte target",
			operation: graphPlanDestinationOperation{Name: "archive", Node: "sink-node", Kind: plan.OpSink},
			target:    byteTarget,
			reason:    "kind does not match byte destination",
		},
	}
	for _, tt := range errorCases {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBranchComposeDestinationOperation(tt.operation, tt.target)
			assertBranchComposeGraphPlanBuildError(t, err, graphPlanInvalidCode, tt.reason)
		})
	}

	validCases := []struct {
		name      string
		operation graphPlanDestinationOperation
		target    branchComposeTargetRoute
	}{
		{name: "sink", operation: graphPlanDestinationOperation{Name: "frames", Node: "sink-node", Kind: plan.OpSink}, target: sinkTarget},
		{name: "mux", operation: graphPlanDestinationOperation{Name: "archive", Node: "mux-node", Kind: plan.OpMux}, target: byteTarget},
		{name: "write", operation: graphPlanDestinationOperation{Name: "archive", Node: "write-node", Kind: plan.OpWrite}, target: byteTarget},
	}
	for _, tt := range validCases {
		t.Run("valid "+tt.name, func(t *testing.T) {
			if err := validateBranchComposeDestinationOperation(tt.operation, tt.target); err != nil {
				t.Fatalf("validateBranchComposeDestinationOperation() error = %v", err)
			}
		})
	}
}

func branchComposeOps(kinds ...plan.OperationKind) []workOperation {
	operations := make([]workOperation, 0, len(kinds))
	for _, kind := range kinds {
		operations = append(operations, workOperation{Kind: kind})
	}
	return operations
}

func assertBranchComposeGraphPlanBuildError(t *testing.T, err error, code errcode.Code, reason string) {
	t.Helper()
	var buildErr *BuildError
	if !errors.As(err, &buildErr) {
		t.Fatalf("err = %T, want *BuildError", err)
	}
	if buildErr.Code != code || !strings.Contains(buildErr.Reason, reason) {
		t.Fatalf("BuildError = %+v, want code=%s reason containing %q", buildErr, code, reason)
	}
}
