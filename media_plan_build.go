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
	return &task{graph: graph}, nil
}

func (r recipeResolved) buildMediaPlanFrameSinkTask(ctx context.Context) (Task, error) {
	builder, ok := r.builder.(*builder)
	if !ok || !builderCanBuildFrameSink(builder) {
		return nil, recipeGraphUnsupportedError("build job", r.intent)
	}
	graph, err := builder.newGraph(ctx)
	if err != nil {
		return nil, err
	}
	if err := r.compileMediaPlanFrameSink(ctx, builder, graph); err != nil {
		graph.Close()
		return nil, err
	}
	return &task{graph: graph}, nil
}

func (r recipeResolved) compileMediaPlanFrameSink(ctx context.Context, builder *builder, graph pipeline.Graph) error {
	switch {
	case len(builder.inputs) == 1 && len(builder.rtpInputs) == 0:
		return builder.compileDecodeToSink(ctx, graph)
	case len(builder.rtpInputs) > 0 && len(builder.inputs) == 0:
		return builder.compileRTPDecodeToSink(ctx, graph)
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
	return r.compileMediaPlanMuxOutputs(ctx, builder, graph, []pipeline.NodeRef{sourceRef}, demux.streams)
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
	return r.compileMediaPlanMuxOutputs(ctx, builder, graph, sourceRefs, streams)
}

func (r recipeResolved) compileMediaPlanMuxOutputs(
	ctx context.Context,
	builder *builder,
	graph pipeline.Graph,
	sourceRefs []pipeline.NodeRef,
	streams []av.Stream,
) error {
	for i := range r.outputAttachments {
		output := r.outputAttachments[i]
		stage, err := builder.openMuxStageWithFormat(ctx, output.output, i, streams, outputSpecOpenFormat(output), outputSpecGraphFormat(output))
		if err != nil {
			return err
		}
		stageRef, err := graph.AddStage(stage, builder.runtime.buffer)
		if err != nil {
			stage.Close()
			return err
		}
		for j := range sourceRefs {
			if err := connectRefs(graph, sourceRefs[j], stageRef); err != nil {
				return err
			}
		}
	}
	return nil
}
