package goav

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

const (
	graphSpecOriginMediaPlan = "media_plan"

	mediaBuildKindPacketCopy   = "packet_copy"
	mediaBuildKindSinkEndpoint = "sink_endpoint"
	mediaBuildKindEncode       = "encode"
	mediaBuildKindBranch       = "branch_composer"
)

func emitMediaPlanGraphSpecPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "emit media plan graph spec", fn: func(state *recipeCompileState) error {
		spec, kind, ok, err := mediaPlanGraphSpec(state)
		if err != nil || !ok {
			return err
		}
		state.spec = spec
		state.specReady = true
		state.specOrigin = graphSpecOriginMediaPlan
		state.mediaBuildKind = kind
		return nil
	}}
}

func mediaPlanGraphSpec(state *recipeCompileState) (pipeline.Spec, string, bool, error) {
	if spec, ok, err := mediaPlanPacketCopySpec(state); err != nil || ok {
		return spec, mediaBuildKindPacketCopy, ok, err
	}
	if spec, ok, err := mediaPlanSinkEndpointSpec(state); err != nil || ok {
		return spec, mediaBuildKindSinkEndpoint, ok, err
	}
	if spec, ok, err := mediaPlanEncodeSpec(state); err != nil || ok {
		return spec, mediaBuildKindEncode, ok, err
	}
	if spec, ok, err := mediaPlanBranchComposerSpec(state); err != nil || ok {
		return spec, mediaBuildKindBranch, ok, err
	}
	return pipeline.Spec{}, "", false, nil
}

func mediaPlanPacketCopySpec(state *recipeCompileState) (pipeline.Spec, bool, error) {
	stream, selectedStream, ok := mediaPlanPacketCopyStream(state)
	if !ok {
		return pipeline.Spec{}, false, nil
	}
	if len(state.inputAttachments) == 0 || len(state.outputAttachments) == 0 {
		return pipeline.Spec{}, false, nil
	}
	runtime, ok := state.runtime.(*runtime)
	if !ok || runtime == nil {
		return pipeline.Spec{}, false, nil
	}

	spec := pipeline.Spec{Name: "goav", Realtime: runtime.realtime}
	nodes := make(map[string]plannedNode, len(state.inputAttachments)+len(state.outputAttachments)+1)
	sourceRefs, ok, err := mediaPlanPacketCopySources(&spec, nodes, state.inputAttachments)
	if err != nil || !ok {
		return pipeline.Spec{}, ok, err
	}
	upstreamRefs := sourceRefs
	if selectedStream {
		selector := streamIntentSelector(stream)
		selectName := selectNodeName(selector)
		selectRef := pipeline.NodeRef(selectName)
		if err := addPlannedNode(nodes, &spec, selectName, pipeline.NodeStage, selectRef, selectNodeDetail(selector)); err != nil {
			return pipeline.Spec{}, false, err
		}
		for i := range sourceRefs {
			spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
				From:   sourceRefs[i],
				To:     selectRef,
				Policy: pipeline.RouteAll,
			})
		}
		upstreamRefs = []pipeline.NodeRef{selectRef}
	}
	targetRefs, err := mediaPlanPacketCopyTargets(&spec, nodes, state.outputAttachments)
	if err != nil {
		return pipeline.Spec{}, false, err
	}
	for i := range upstreamRefs {
		for j := range targetRefs {
			spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
				From:   upstreamRefs[i],
				To:     targetRefs[j],
				Policy: pipeline.RouteAll,
			})
		}
	}
	return spec, true, nil
}

func mediaPlanPacketCopyStream(state *recipeCompileState) (StreamIntent, bool, bool) {
	if state == nil || !state.jobPresent {
		return StreamIntent{}, false, false
	}
	switch len(state.intent.Streams) {
	case 0:
		return StreamIntent{}, false, true
	case 1:
		stream := state.intent.Streams[0]
		if stream.Encode.Copy && !stream.Decode && stream.Encode.ID == "" && !stream.Encode.Auto && len(state.streamSteps) == 0 {
			return stream, true, true
		}
	}
	return StreamIntent{}, false, false
}

func mediaPlanSinkEndpointSpec(state *recipeCompileState) (pipeline.Spec, bool, error) {
	if state == nil || !state.jobPresent || len(state.intent.Streams) != 1 {
		return pipeline.Spec{}, false, nil
	}
	stream := state.intent.Streams[0]
	if !mediaPlanSinkEndpointShape(stream, state.outputAttachments) {
		return pipeline.Spec{}, false, nil
	}
	plan, ok, err := newMediaPlanSingleStreamGraph(state.runtime, state.inputAttachments, state.outputAttachments, stream)
	if err != nil || !ok {
		return pipeline.Spec{}, ok, err
	}
	spec, err := plan.sinkEndpointSpec()
	return spec, err == nil, err
}

func mediaPlanSinkEndpointShape(stream StreamIntent, outputs []EndpointSpec) bool {
	return len(outputs) == 1 &&
		outputs[0].sink != nil &&
		stream.Decode &&
		len(stream.Targets) == 1 &&
		!stream.Encode.Copy
}

func mediaPlanEncodeSpec(state *recipeCompileState) (pipeline.Spec, bool, error) {
	if state == nil || !state.jobPresent || len(state.intent.Streams) != 1 {
		return pipeline.Spec{}, false, nil
	}
	stream := state.intent.Streams[0]
	if !mediaPlanEncodeShape(stream, state.outputAttachments) {
		return pipeline.Spec{}, false, nil
	}
	plan, ok, err := newMediaPlanSingleStreamGraph(state.runtime, state.inputAttachments, state.outputAttachments, stream)
	if err != nil || !ok {
		return pipeline.Spec{}, ok, err
	}
	spec, err := plan.encodeOutputSpec()
	return spec, err == nil, err
}

func mediaPlanEncodeShape(stream StreamIntent, outputs []EndpointSpec) bool {
	if !stream.Decode || !codecIntentSet(stream.Encode) || stream.Encode.Copy || len(outputs) == 0 {
		return false
	}
	for i := range outputs {
		if outputs[i].sink != nil {
			return false
		}
	}
	return len(stream.Targets) == len(outputs)
}

func mediaPlanBranchComposerSpec(state *recipeCompileState) (pipeline.Spec, bool, error) {
	if state == nil || !state.branchCompositionPresent {
		return pipeline.Spec{}, false, nil
	}
	runtime, ok := state.runtime.(*runtime)
	if !ok || runtime == nil {
		return pipeline.Spec{}, false, nil
	}
	builder := branchComposePlanBuilder(runtime, state.branchInputAttachment)
	spec := pipeline.Spec{Name: "goav", Realtime: builder.runtime.realtime}
	switch {
	case state.branchInputAttachment.rtp == nil:
		spec, err := builder.planBranchComposePlan(spec, state.plan)
		return spec, err == nil, err
	case len(builder.rtpInputs) > 0:
		spec, err := builder.planRTPBranchComposePlan(spec, state.plan)
		return spec, err == nil, err
	default:
		return pipeline.Spec{}, false, nil
	}
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

func mediaPlanPacketCopyTargets(spec *pipeline.Spec, nodes map[string]plannedNode, outputs []EndpointSpec) ([]pipeline.NodeRef, error) {
	refs := make([]pipeline.NodeRef, 0, len(outputs))
	for i := range outputs {
		if outputs[i].sink != nil {
			name := firstNonEmpty(outputs[i].sink.Name(), outputs[i].label("sink"))
			ref := pipeline.NodeRef(name)
			if err := addPlannedNode(nodes, spec, name, pipeline.NodeSink, ref, describedNodeDetail(outputs[i].sink)); err != nil {
				return nil, err
			}
			refs = append(refs, ref)
			continue
		}
		output := outputs[i].output
		name := muxNodeName(output, i)
		ref := pipeline.NodeRef(name)
		detail := outputNodeDetailWithFormat(output, endpointSpecGraphFormat(outputs[i]))
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

func endpointSpecGraphFormat(output EndpointSpec) av.FormatID {
	return output.format
}

func endpointSpecOpenFormat(output EndpointSpec) av.FormatID {
	return endpointSpecFormat(output)
}
