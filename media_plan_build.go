package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

func (r recipeResolved) buildMediaPlanPacketCopyTask(ctx context.Context) (Task, error) {
	runtime, ok := r.runtime.(*runtime)
	if !ok || runtime == nil {
		return nil, recipeGraphUnsupportedError("build job", r.intent)
	}
	builder := &builder{runtime: runtime}
	graph, err := builder.newGraph(ctx)
	if err != nil {
		return nil, err
	}
	if err := r.compileMediaPlanPacketCopy(ctx, builder, graph); err != nil {
		graph.Close()
		return nil, err
	}
	return newTask(graph, builder.runtime), nil
}

func (r recipeResolved) buildMediaPlanSinkEndpointTask(ctx context.Context) (Task, error) {
	builder, ok := r.builder.(*builder)
	if !ok || !builderCanBuildSinkEndpoint(builder) {
		return nil, recipeGraphUnsupportedError("build job", r.intent)
	}
	graph, err := builder.newGraph(ctx)
	if err != nil {
		return nil, err
	}
	if err := r.compileMediaPlanSinkEndpoint(ctx, builder, graph); err != nil {
		graph.Close()
		return nil, err
	}
	return newTask(graph, builder.runtime), nil
}

func (r recipeResolved) buildMediaPlanEncodeTask(ctx context.Context) (Task, error) {
	builder, ok := r.builder.(*builder)
	if !ok || !builderCanBuildEncodeOutput(builder) {
		return nil, recipeGraphUnsupportedError("build job", r.intent)
	}
	graph, err := builder.newGraph(ctx)
	if err != nil {
		return nil, err
	}
	if err := r.compileMediaPlanEncode(ctx, builder, graph); err != nil {
		graph.Close()
		return nil, err
	}
	return newTask(graph, builder.runtime), nil
}

func (r recipeResolved) buildMediaPlanBranchComposerTask(ctx context.Context) (Task, error) {
	builder, ok := r.builder.(*builder)
	if !ok || !builderCanBuildBranchComposer(builder) {
		return nil, recipeGraphUnsupportedError("build branch composition", r.intent)
	}
	graph, err := builder.newGraph(ctx)
	if err != nil {
		return nil, err
	}
	if err := r.compileMediaPlanBranchComposer(ctx, builder, graph); err != nil {
		graph.Close()
		return nil, err
	}
	return newTask(graph, builder.runtime), nil
}

func (r recipeResolved) compileMediaPlanBranchComposer(ctx context.Context, builder *builder, graph pipeline.Graph) error {
	if len(builder.rtpInputs) != 0 {
		return builder.compileRTPBranchComposePlan(ctx, graph, r.plan)
	}
	return builder.compileBranchComposePlan(ctx, graph, r.plan)
}

func (r recipeResolved) compileMediaPlanEncode(ctx context.Context, builder *builder, graph pipeline.Graph) error {
	switch {
	case len(builder.inputs) == 1 && len(builder.rtpInputs) == 0:
		return builder.compileDecodeEncodeToOutput(ctx, graph)
	case len(builder.rtpInputs) > 0 && len(builder.inputs) == 0:
		return builder.compileRTPDecodeEncodeToOutput(ctx, graph)
	default:
		return recipeGraphUnsupportedError("build job", r.intent)
	}
}

func (r recipeResolved) compileMediaPlanSinkEndpoint(ctx context.Context, builder *builder, graph pipeline.Graph) error {
	switch {
	case len(builder.inputs) == 1 && len(builder.rtpInputs) == 0 && len(builder.encodes) == 0:
		return builder.compileDecodeToSink(ctx, graph)
	case len(builder.rtpInputs) > 0 && len(builder.inputs) == 0 && len(builder.encodes) == 0:
		return builder.compileRTPDecodeToSink(ctx, graph)
	case len(builder.inputs) == 1 && len(builder.rtpInputs) == 0 && len(builder.encodes) == 1:
		return builder.compileDecodeEncodeToSink(ctx, graph)
	case len(builder.rtpInputs) > 0 && len(builder.inputs) == 0 && len(builder.encodes) == 1:
		return builder.compileRTPDecodeEncodeToSink(ctx, graph)
	default:
		return recipeGraphUnsupportedError("build job", r.intent)
	}
}

func (r recipeResolved) compileMediaPlanPacketCopy(ctx context.Context, builder *builder, graph pipeline.Graph) error {
	if len(r.inputAttachments) == 1 && r.inputAttachments[0].rtp == nil {
		return r.compileMediaPlanFileCopy(ctx, builder, graph)
	}
	if allRTPInputSpecs(r.inputAttachments) {
		return r.compileMediaPlanRTPCopy(ctx, builder, graph)
	}
	return recipeGraphUnsupportedError("build job", r.intent)
}

func (r recipeResolved) compileMediaPlanFileCopy(ctx context.Context, builder *builder, graph pipeline.Graph) error {
	demux, err := builder.openDemuxSource(ctx, r.inputAttachments[0].formatInput())
	if err != nil {
		return err
	}
	sourceRef, err := graph.AddSource(demux.source, builder.runtime.buffer)
	if err != nil {
		demux.source.Close()
		return err
	}
	return r.compileMediaPlanPacketCopyTargets(ctx, builder, graph, []pipeline.NodeRef{sourceRef}, demux.streams)
}

func (r recipeResolved) compileMediaPlanRTPCopy(ctx context.Context, builder *builder, graph pipeline.Graph) error {
	sourceRefs := make([]pipeline.NodeRef, 0, len(r.inputAttachments))
	streams := make([]av.Stream, 0, len(r.inputAttachments))
	for i := range r.inputAttachments {
		receiver, err := builder.openRTPSource(ctx, r.inputAttachments[i].rtpBuildInput(), i)
		if err != nil {
			return err
		}
		sourceRef, err := graph.AddSource(receiver.source, builder.runtime.buffer)
		if err != nil {
			receiver.source.Close()
			return err
		}
		sourceRefs = append(sourceRefs, sourceRef)
		streams = append(streams, receiver.streams...)
	}
	return r.compileMediaPlanPacketCopyTargets(ctx, builder, graph, sourceRefs, streams)
}

func (r recipeResolved) compileMediaPlanPacketCopyTargets(
	ctx context.Context,
	builder *builder,
	graph pipeline.Graph,
	sourceRefs []pipeline.NodeRef,
	streams []av.Stream,
) error {
	targetRefs := sourceRefs
	targetStreams := streams
	if stream, ok := r.selectedPacketCopyStream(); ok {
		selector := streamIntentSelector(stream)
		selected, err := selectDecodeStream(streams, selector)
		if err != nil {
			return err
		}
		selectStage := newStreamSelectStage(selectNodeName(selector), selected, selector, selectNodeDetail(selector))
		selectRef, err := graph.AddStage(selectStage, builder.runtime.buffer)
		if err != nil {
			selectStage.Close()
			return err
		}
		for i := range sourceRefs {
			if err := connectRefs(graph, sourceRefs[i], selectRef); err != nil {
				return err
			}
		}
		targetRefs = []pipeline.NodeRef{selectRef}
		targetStreams = []av.Stream{selected}
	}
	for i := range r.outputAttachments {
		output := r.outputAttachments[i]
		if output.sink != nil {
			sinkRef, err := graph.AddSink(output.sink, builder.runtime.buffer)
			if err != nil {
				return err
			}
			for j := range targetRefs {
				if err := connectRefs(graph, targetRefs[j], sinkRef); err != nil {
					return err
				}
			}
			continue
		}
		stage, err := builder.openMuxStageWithFormat(ctx, output.output, i, targetStreams, endpointSpecOpenFormat(output), endpointSpecGraphFormat(output))
		if err != nil {
			return err
		}
		stageRef, err := graph.AddStage(stage, builder.runtime.buffer)
		if err != nil {
			stage.Close()
			return err
		}
		for j := range targetRefs {
			if err := connectRefs(graph, targetRefs[j], stageRef); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r recipeResolved) selectedPacketCopyStream() (StreamIntent, bool) {
	if len(r.intent.Streams) != 1 {
		return StreamIntent{}, false
	}
	stream := r.intent.Streams[0]
	if !stream.Encode.Copy || stream.Decode || stream.Encode.ID != "" || stream.Encode.Auto {
		return StreamIntent{}, false
	}
	return stream, true
}
