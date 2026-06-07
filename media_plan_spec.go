package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

const (
	graphSpecOriginMediaPlan = "media_plan"
)

type mediaPlanExecutable interface {
	spec() (pipeline.Spec, error)
	runtimeRef() *runtime
	compile(context.Context, pipeline.Graph) error
}

func emitMediaPlanGraphSpecPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "emit media plan graph spec", fn: func(state *recipeCompileState) error {
		graph, ok, err := mediaPlanGraph(state)
		if err != nil || !ok {
			return err
		}
		spec, err := graph.spec()
		if err != nil {
			return err
		}
		state.spec = spec
		state.specReady = true
		state.specOrigin = graphSpecOriginMediaPlan
		state.mediaGraph = graph
		return nil
	}}
}

func mediaPlanGraph(state *recipeCompileState) (mediaPlanExecutable, bool, error) {
	if graph, ok, err := mediaPlanStreamExecutableForState(state); err != nil || ok {
		return graph, ok, err
	}
	if graph, ok, err := mediaPlanBranchComposerExecutable(state); err != nil || ok {
		return graph, ok, err
	}
	return nil, false, nil
}

func mediaPlanStreamExecutableForState(state *recipeCompileState) (mediaPlanExecutable, bool, error) {
	if graph, ok, err := mediaPlanPacketCopyStreamExecutableForState(state); err != nil || ok {
		return graph, ok, err
	}
	return mediaPlanDecodeStreamExecutableForState(state)
}

func mediaPlanPacketCopyStreamExecutableForState(state *recipeCompileState) (mediaPlanExecutable, bool, error) {
	stream, selectedStream, ok := mediaPlanPacketCopyStream(state)
	if !ok {
		return nil, false, nil
	}
	plan, ok, err := newMediaPlanPacketCopyStreamGraph(state.runtime, state.inputAttachments, state.outputAttachments, stream, selectedStream)
	if err != nil || !ok {
		return nil, ok, err
	}
	return plan, true, nil
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

func mediaPlanDecodeStreamExecutableForState(state *recipeCompileState) (mediaPlanExecutable, bool, error) {
	if state == nil || !state.jobPresent || len(state.intent.Streams) != 1 {
		return nil, false, nil
	}
	stream := state.intent.Streams[0]
	if !mediaPlanDecodeStreamShape(stream, state.outputAttachments) {
		return nil, false, nil
	}
	plan, ok, err := newMediaPlanDecodeStreamGraph(state.runtime, state.inputAttachments, state.outputAttachments, stream)
	if err != nil || !ok {
		return nil, ok, err
	}
	return plan, true, nil
}

func mediaPlanDecodeStreamShape(stream StreamIntent, outputs []destinationSpec) bool {
	return mediaPlanSinkDestinationShape(stream, outputs) || mediaPlanEncodeShape(stream, outputs)
}

func mediaPlanSinkDestinationShape(stream StreamIntent, outputs []destinationSpec) bool {
	return len(outputs) == 1 &&
		outputs[0].sink != nil &&
		stream.Decode &&
		len(stream.Targets) == 1 &&
		!stream.Encode.Copy
}

func mediaPlanEncodeShape(stream StreamIntent, outputs []destinationSpec) bool {
	if !stream.Decode || !codecIntentSet(stream.Encode) || stream.Encode.Copy || len(outputs) == 0 {
		return false
	}
	return len(stream.Targets) == len(outputs)
}

func mediaPlanBranchComposerExecutable(state *recipeCompileState) (mediaPlanExecutable, bool, error) {
	if state == nil || !state.branchCompositionPresent {
		return nil, false, nil
	}
	plan, ok, err := newMediaPlanBranchComposeGraph(state.runtime, state.branchInputAttachment, state.plan)
	if err != nil || !ok {
		return nil, ok, err
	}
	return plan, true, nil
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

func mediaPlanPacketCopyTargets(spec *pipeline.Spec, nodes map[string]plannedNode, outputs []destinationSpec) ([]pipeline.NodeRef, error) {
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
		detail := outputNodeDetailWithFormat(output, destinationGraphFormat(outputs[i]))
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

func destinationGraphFormat(output destinationSpec) av.FormatID {
	return output.format
}

func destinationOpenFormat(output destinationSpec) av.FormatID {
	return destinationSpecFormat(output)
}
