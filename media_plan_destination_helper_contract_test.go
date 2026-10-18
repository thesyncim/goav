package goav

import (
	"errors"
	"testing"

	"github.com/thesyncim/goav/errcode"
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

	rt := mustNew()
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

// TestComposeLowererRefusesFramesToOutputsWithoutEncoder pins the fallback
// refusal for decoded frames routed to an output that is not a sink with no
// encoder: no compose graph lowers it.
func TestComposeLowererRefusesFramesToOutputsWithoutEncoder(t *testing.T) {
	_, err := (composeLowerer{describe: "describe job", unsupported: true}).spec()
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != errcode.RecipeGraphUnsupported {
		t.Fatalf("spec() error = %v; want recipe graph unsupported", err)
	}
}
