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
	plan, ok, err := newMediaPlanPacketCopyGraph(state.runtime, state.inputAttachments, state.outputAttachments, stream, selectedStream)
	if err != nil || !ok {
		return pipeline.Spec{}, ok, err
	}
	spec, err := plan.spec()
	return spec, err == nil, err
}

func mediaPlanPacketCopyStream(state *recipeCompileState) (StreamIntent, bool, bool) {
	if state == nil {
		return StreamIntent{}, false, false
	}
	return mediaPlanPacketCopyIntentStream(state.jobPresent, state.intent, state.streamSteps)
}

func mediaPlanPacketCopyIntentStream(jobPresent bool, intent Intent, streamSteps []jobStreamStepAttachment) (StreamIntent, bool, bool) {
	if !jobPresent {
		return StreamIntent{}, false, false
	}
	switch len(intent.Streams) {
	case 0:
		return StreamIntent{}, false, true
	case 1:
		stream := intent.Streams[0]
		if stream.Encode.Copy && !stream.Decode && stream.Encode.ID == "" && !stream.Encode.Auto && len(streamSteps) == 0 {
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
	return len(stream.Targets) == len(outputs)
}

func mediaPlanBranchComposerSpec(state *recipeCompileState) (pipeline.Spec, bool, error) {
	if state == nil || !state.branchCompositionPresent {
		return pipeline.Spec{}, false, nil
	}
	plan, ok, err := newMediaPlanBranchComposeGraph(state.runtime, state.branchInputAttachment, state.plan)
	if err != nil || !ok {
		return pipeline.Spec{}, ok, err
	}
	spec, err := plan.spec()
	return spec, err == nil, err
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
