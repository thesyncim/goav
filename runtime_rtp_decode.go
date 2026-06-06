package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

type rtpDecodeToSinkGraphCompiler struct{}

func (rtpDecodeToSinkGraphCompiler) match(b *builder) bool {
	return len(b.rtpInputs) > 0 &&
		len(b.decodes) == 1 &&
		len(b.sinks) == 1 &&
		len(b.inputs) == 0 &&
		len(b.outputs) == 0 &&
		len(b.encodes) == 0 &&
		len(b.transcodes) == 0 &&
		len(b.sources) == 0 &&
		len(b.stages) == 0 &&
		len(b.links) == 0 &&
		len(b.routes) == 0
}

func (rtpDecodeToSinkGraphCompiler) describe(b *builder, spec pipeline.Spec) (pipeline.Spec, error) {
	return b.planRTPDecodeToSink(spec)
}

func (rtpDecodeToSinkGraphCompiler) build(ctx context.Context, b *builder) (Task, error) {
	return b.buildRTPDecodeToSink(ctx)
}

func (b *builder) planRTPDecodeToSink(spec pipeline.Spec) (pipeline.Spec, error) {
	if b.sinks[0] == nil {
		return pipeline.Spec{}, ErrNilSink
	}

	nodes := make(map[string]plannedNode, len(b.rtpInputs)+3+len(b.filters))
	sourceRefs := make([]pipeline.NodeRef, len(b.rtpInputs))
	for i := range b.rtpInputs {
		sourceName := rtpNodeName(b.rtpInputs[i], i)
		sourceRef := pipeline.NodeRef(sourceName)
		if err := addPlannedNode(nodes, &spec, sourceName, pipeline.NodeSource, sourceRef, rtpInputDetail(b.rtpInputs[i])); err != nil {
			return pipeline.Spec{}, err
		}
		sourceRefs[i] = sourceRef
	}

	if err := b.planDecodeFramePath(nodes, &spec, sourceRefs, b.decodes[0]); err != nil {
		return pipeline.Spec{}, err
	}
	return spec, nil
}

func (b *builder) buildRTPDecodeToSink(ctx context.Context) (Task, error) {
	graph, err := b.newGraph(ctx)
	if err != nil {
		return nil, err
	}
	if err := b.compileRTPDecodeToSink(ctx, graph); err != nil {
		graph.Close()
		return nil, err
	}
	return &task{graph: graph}, nil
}

func (b *builder) compileRTPDecodeToSink(ctx context.Context, graph pipeline.Graph) error {
	if b.sinks[0] == nil {
		return ErrNilSink
	}

	sourceRefs := make([]pipeline.NodeRef, 0, len(b.rtpInputs))
	streams := make([]av.Stream, 0, len(b.rtpInputs))
	for i := range b.rtpInputs {
		receiver, err := b.openRTPSource(ctx, b.rtpInputs[i], i)
		if err != nil {
			return err
		}
		sourceRef, err := graph.AddSource(receiver.source, b.runtime.buffer)
		if err != nil {
			receiver.source.Close()
			return err
		}
		sourceRefs = append(sourceRefs, sourceRef)
		streams = append(streams, receiver.streams...)
	}

	selector := b.decodes[0]
	stream, err := selectDecodeStream(streams, selector)
	if err != nil {
		return err
	}

	return b.compileDecodeFramePath(ctx, graph, sourceRefs, selector, stream, b.runtime.realtime)
}
