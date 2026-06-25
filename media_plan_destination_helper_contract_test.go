package goav

import (
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
)

func TestRecipeResolvedSingleStreamIntentContracts(t *testing.T) {
	single := recipeResolved{intent: intent{Streams: []streamIntent{{Name: "audio"}}}}
	stream, ok := single.singleStreamIntent()
	if !ok || stream.Name != "audio" {
		t.Fatalf("singleStreamIntent() = %+v, %v; want audio stream, true", stream, ok)
	}

	for name, resolved := range map[string]recipeResolved{
		"none": {intent: intent{}},
		"many": {intent: intent{Streams: []streamIntent{
			{Name: "audio"},
			{Name: "video"},
		}}},
	} {
		t.Run(name, func(t *testing.T) {
			stream, ok := resolved.singleStreamIntent()
			if ok || stream.Name != "" {
				t.Fatalf("singleStreamIntent() = %+v, %v; want zero stream, false", stream, ok)
			}
		})
	}
}

func TestMediaPlanPacketCopyStreamGraphConstructionContracts(t *testing.T) {
	var nilRuntime *Runtime
	if graph, ok, err := newMediaPlanPacketCopyStreamGraph(nilRuntime, []InputSpec{{}}, []destinationSpec{{name: "out"}}, streamIntent{}, false); err != nil || !ok || graph.runtime != nil {
		t.Fatalf("newMediaPlanPacketCopyStreamGraph(nil runtime) = %+v, %v, %v; want structural graph with nil runtime", graph, ok, err)
	}

	rt := MustNew()
	for name, tc := range map[string]struct {
		inputs  []InputSpec
		outputs []destinationSpec
	}{
		"no inputs":              {outputs: []destinationSpec{{name: "out"}}},
		"no outputs":             {inputs: []InputSpec{{}}},
		"unsupported multiinput": {inputs: []InputSpec{{}, {}}, outputs: []destinationSpec{{name: "out"}}},
	} {
		t.Run(name, func(t *testing.T) {
			graph, ok, err := newMediaPlanPacketCopyStreamGraph(rt, tc.inputs, tc.outputs, streamIntent{Name: "audio"}, true)
			if err != nil || ok || graph.runtime != nil {
				t.Fatalf("newMediaPlanPacketCopyStreamGraph() = %+v, %v, %v; want not handled", graph, ok, err)
			}
		})
	}
}

func TestGraphPlanDestinationOperationNodeContracts(t *testing.T) {
	target := graphPlanDestinationOperation{
		Name: "archive",
		Kind: plan.OpMux,
	}
	err := validateGraphPlanDestinationOperationNode("packet-copy", target)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != errcode.GraphPlanInvalid ||
		!strings.Contains(buildErr.Reason, "packet-copy destination operation has no node") {
		t.Fatalf("validateGraphPlanDestinationOperationNode() error = %v; want graph-plan node error", err)
	}

	target.Node = "plan-archive"
	if err := validateGraphPlanDestinationOperationNode("packet-copy", target); err != nil {
		t.Fatalf("validateGraphPlanDestinationOperationNode() error = %v", err)
	}
}

func TestGraphPlanDestinationOperationNamesComponentsFromPlannerNodes(t *testing.T) {
	sink := &runtimeTestSink{name: "component-sink"}
	stage := &runtimeTestStage{name: "component-stage"}

	if got := namedSinkForGraphPlanDestination(graphPlanDestinationOperation{}, sink); got != sink {
		t.Fatalf("unnamed sink wrapper = %T, want original sink", got)
	}
	if got := namedStageForGraphPlanDestination(graphPlanDestinationOperation{}, stage); got != stage {
		t.Fatalf("unnamed stage wrapper = %T, want original stage", got)
	}

	target := graphPlanDestinationOperation{Node: "planner-node"}
	if got := namedSinkForGraphPlanDestination(target, sink); got.Name() != "planner-node" {
		t.Fatalf("named sink Name() = %q, want planner-node", got.Name())
	}
	if got := namedStageForGraphPlanDestination(target, stage); got.Name() != "planner-node" {
		t.Fatalf("named stage Name() = %q, want planner-node", got.Name())
	}
}

func TestPipelineNodeRefsEqualContracts(t *testing.T) {
	if !pipelineNodeRefsEqual(nil, nil) {
		t.Fatal("pipelineNodeRefsEqual(nil, nil) = false, want true")
	}
	if !pipelineNodeRefsEqual([]pipeline.NodeRef{"source", "sink"}, []pipeline.NodeRef{"source", "sink"}) {
		t.Fatal("pipelineNodeRefsEqual(equal refs) = false, want true")
	}
	if pipelineNodeRefsEqual([]pipeline.NodeRef{"source"}, []pipeline.NodeRef{"source", "sink"}) {
		t.Fatal("pipelineNodeRefsEqual(length mismatch) = true, want false")
	}
	if pipelineNodeRefsEqual([]pipeline.NodeRef{"source", "left"}, []pipeline.NodeRef{"source", "right"}) {
		t.Fatal("pipelineNodeRefsEqual(value mismatch) = true, want false")
	}
}

func TestMediaPlanEncodeOutputSpecRequiresEncode(t *testing.T) {
	_, err := (mediaPlanStreamGraph{stream: streamIntent{Name: "audio"}}).encodeOutputSpec()
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != errcode.RecipeGraphUnsupported {
		t.Fatalf("encodeOutputSpec() error = %v; want recipe graph unsupported", err)
	}
}
