package goav

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

const (
	graphSpecOriginMediaPlan = "media_plan"
	graphSpecOriginMigration = "migration"
)

func emitMediaPlanGraphSpecPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "emit media plan graph spec", fn: func(state *recipeCompileState) error {
		spec, ok, err := mediaPlanPacketCopySpec(state)
		if err != nil || !ok {
			return err
		}
		state.spec = spec
		state.specReady = true
		state.specOrigin = graphSpecOriginMediaPlan
		return nil
	}}
}

func mediaPlanPacketCopySpec(state *recipeCompileState) (pipeline.Spec, bool, error) {
	if state == nil || !state.jobPresent || len(state.intent.Streams) != 0 {
		return pipeline.Spec{}, false, nil
	}
	if len(state.inputAttachments) == 0 || len(state.outputAttachments) == 0 {
		return pipeline.Spec{}, false, nil
	}
	runtime, ok := state.runtime.(*runtime)
	if !ok || runtime == nil {
		return pipeline.Spec{}, false, nil
	}
	for i := range state.outputAttachments {
		if state.outputAttachments[i].sink != nil {
			return pipeline.Spec{}, false, nil
		}
	}

	spec := pipeline.Spec{Name: "goav", Realtime: runtime.realtime}
	nodes := make(map[string]plannedNode, len(state.inputAttachments)+len(state.outputAttachments))
	sourceRefs, ok, err := mediaPlanPacketCopySources(&spec, nodes, state.inputAttachments)
	if err != nil || !ok {
		return pipeline.Spec{}, ok, err
	}
	stageRefs, err := mediaPlanPacketCopyOutputs(&spec, nodes, state.outputAttachments)
	if err != nil {
		return pipeline.Spec{}, false, err
	}
	for i := range sourceRefs {
		for j := range stageRefs {
			spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
				From:   sourceRefs[i],
				To:     stageRefs[j],
				Policy: pipeline.RouteAll,
			})
		}
	}
	return spec, true, nil
}

func mediaPlanPacketCopySources(spec *pipeline.Spec, nodes map[string]plannedNode, inputs []InputSpec) ([]pipeline.NodeRef, bool, error) {
	if len(inputs) == 1 && inputs[0].rtp == nil {
		input := inputs[0].formatInput()
		name := demuxNodeName(input)
		ref := pipeline.NodeRef(name)
		if err := addPlannedNode(nodes, spec, name, pipeline.NodeSource, ref, inputNodeDetail(input)); err != nil {
			return nil, false, err
		}
		return []pipeline.NodeRef{ref}, true, nil
	}
	if !allRTPInputSpecs(inputs) {
		return nil, false, nil
	}
	refs := make([]pipeline.NodeRef, 0, len(inputs))
	for i := range inputs {
		input := inputs[i].rtpBuildInput()
		name := rtpNodeName(input, i)
		ref := pipeline.NodeRef(name)
		if err := addPlannedNode(nodes, spec, name, pipeline.NodeSource, ref, rtpInputDetail(input)); err != nil {
			return nil, false, err
		}
		refs = append(refs, ref)
	}
	return refs, true, nil
}

func mediaPlanPacketCopyOutputs(spec *pipeline.Spec, nodes map[string]plannedNode, outputs []OutputSpec) ([]pipeline.NodeRef, error) {
	refs := make([]pipeline.NodeRef, 0, len(outputs))
	for i := range outputs {
		output := outputs[i].output
		name := muxNodeName(output, i)
		ref := pipeline.NodeRef(name)
		detail := outputNodeDetailWithFormat(output, outputSpecGraphFormat(outputs[i]))
		if err := addPlannedNode(nodes, spec, name, pipeline.NodeStage, ref, detail); err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	return refs, nil
}

func allRTPInputSpecs(inputs []InputSpec) bool {
	if len(inputs) == 0 {
		return false
	}
	for i := range inputs {
		if inputs[i].rtp == nil {
			return false
		}
	}
	return true
}

func outputSpecGraphFormat(output OutputSpec) av.FormatID {
	return output.format
}
