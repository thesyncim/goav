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
		len(b.filters) == 0 &&
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

	nodes := make(map[string]plannedNode, len(b.rtpInputs)+3)
	sourceRefs := make([]pipeline.NodeRef, len(b.rtpInputs))
	for i := range b.rtpInputs {
		sourceName := rtpNodeName(b.rtpInputs[i], i)
		sourceRef := pipeline.NodeRef(sourceName)
		if err := addPlannedNode(nodes, &spec, sourceName, pipeline.NodeSource, sourceRef); err != nil {
			return pipeline.Spec{}, err
		}
		sourceRefs[i] = sourceRef
	}

	selector := b.decodes[0]
	selectName := selectNodeName(selector)
	selectRef := pipeline.NodeRef(selectName)
	if err := addPlannedNode(nodes, &spec, selectName, pipeline.NodeStage, selectRef); err != nil {
		return pipeline.Spec{}, err
	}

	decodeName := decodeNodeName(selector)
	decodeRef := pipeline.NodeRef(decodeName)
	if err := addPlannedNode(nodes, &spec, decodeName, pipeline.NodeStage, decodeRef); err != nil {
		return pipeline.Spec{}, err
	}

	sinkName := b.sinks[0].Name()
	sinkRef := pipeline.NodeRef(sinkName)
	if err := addPlannedNode(nodes, &spec, sinkName, pipeline.NodeSink, sinkRef); err != nil {
		return pipeline.Spec{}, err
	}

	for i := range sourceRefs {
		spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
			From:   sourceRefs[i],
			To:     selectRef,
			Policy: pipeline.RouteAll,
		})
	}
	spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
		From:   selectRef,
		To:     decodeRef,
		Policy: pipeline.RouteAll,
	})
	spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
		From:   decodeRef,
		To:     sinkRef,
		Policy: pipeline.RouteAll,
	})
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

	selectStage := newStreamSelectStage(selectNodeName(selector), stream.ID)
	selectRef, err := graph.AddStage(selectStage, b.runtime.buffer)
	if err != nil {
		selectStage.Close()
		return err
	}
	stage, err := b.newDecodeStage(ctx, selector, stream, b.runtime.realtime)
	if err != nil {
		return err
	}
	stageRef, err := graph.AddStage(stage, b.runtime.buffer)
	if err != nil {
		stage.Close()
		return err
	}
	sinkRef, err := graph.AddSink(b.sinks[0], b.runtime.buffer)
	if err != nil {
		return err
	}

	for i := range sourceRefs {
		if err := graph.Link(pipeline.Link{From: sourceRefs[i], To: selectRef}); err != nil {
			return err
		}
	}
	if err := graph.Link(pipeline.Link{From: selectRef, To: stageRef}); err != nil {
		return err
	}
	return graph.Link(pipeline.Link{From: stageRef, To: sinkRef})
}
