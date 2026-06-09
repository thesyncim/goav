package goav

import (
	"context"
	"strconv"

	"github.com/thesyncim/goav/pipeline"
)

type builderCompiler interface {
	match(*builder) bool
	describe(*builder, pipeline.Spec) (pipeline.Spec, error)
	build(context.Context, *builder) (Task, error)
}

var builderCompilers = [...]builderCompiler{
	emptyGraphCompiler{},
	explicitGraphCompiler{},
	remuxGraphCompiler{},
	decodeEncodeToOutputGraphCompiler{},
	decodeToSinkGraphCompiler{},
}

func (b *builder) selectCompiler() (builderCompiler, error) {
	for i := range builderCompilers {
		if builderCompilers[i].match(b) {
			return builderCompilers[i], nil
		}
	}
	return nil, unsupportedRuntimeGraphError(b)
}

func unsupportedRuntimeGraphError(b *builder) error {
	details := []string{
		"inputs: " + strconv.Itoa(len(b.inputs)),
		"outputs: " + strconv.Itoa(len(b.outputs)),
		"decodes: " + strconv.Itoa(len(b.decodes)),
		"encodes: " + strconv.Itoa(len(b.encodes)),
		"filters: " + strconv.Itoa(len(b.filters)),
		"sources: " + strconv.Itoa(len(b.sources)),
		"stages: " + strconv.Itoa(len(b.stages)),
		"sinks: " + strconv.Itoa(len(b.sinks)),
		"routes: " + strconv.Itoa(len(b.routes)),
	}
	return &BuildError{
		Code:      "runtime_graph_unsupported",
		Operation: "build runtime graph",
		Reason:    "no compiler matches this builder shape",
		Details:   details,
		Suggestions: []string{
			"use From(...).Copy().To(...) for packet-preserving record and remux jobs",
			"use From(...).Audio()/Video().Decode().To(Sink(...)) for frame output",
			"avoid mixing explicit Source/Stage/Sink graph nodes with high-level runtime builder requests",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func (b *builder) newGraph(_ context.Context) (pipeline.Graph, error) {
	return pipeline.NewGraph(pipeline.GraphConfig{
		Name:          "goav",
		Realtime:      b.runtime.realtime,
		Buffer:        b.runtime.buffer,
		EventCapacity: b.runtime.eventCapacity,
	})
}

type emptyGraphCompiler struct{}

func (emptyGraphCompiler) match(b *builder) bool {
	return !b.hasHighLevelRequests() && !b.hasExplicitGraph()
}

func (emptyGraphCompiler) describe(_ *builder, spec pipeline.Spec) (pipeline.Spec, error) {
	return spec, nil
}

func (emptyGraphCompiler) build(ctx context.Context, b *builder) (Task, error) {
	graph, err := b.newGraph(ctx)
	if err != nil {
		return nil, err
	}
	return newTask(graph, b.runtime, b.destinationTxs...), nil
}

type explicitGraphCompiler struct{}

func (explicitGraphCompiler) match(b *builder) bool {
	return !b.hasHighLevelRequests() && b.hasExplicitGraph()
}

func (explicitGraphCompiler) describe(b *builder, spec pipeline.Spec) (pipeline.Spec, error) {
	return b.planExplicitGraph(spec)
}

func (explicitGraphCompiler) build(ctx context.Context, b *builder) (Task, error) {
	graph, err := b.newGraph(ctx)
	if err != nil {
		return nil, err
	}
	if err := b.compileExplicitGraph(graph); err != nil {
		graph.Close()
		return nil, err
	}
	return newTask(graph, b.runtime, b.destinationTxs...), nil
}

type remuxGraphCompiler struct{}

func (remuxGraphCompiler) match(b *builder) bool {
	return !b.hasExplicitGraph() && b.canBuildRemux()
}

func (remuxGraphCompiler) describe(b *builder, spec pipeline.Spec) (pipeline.Spec, error) {
	return b.planRemux(spec)
}

func (remuxGraphCompiler) build(ctx context.Context, b *builder) (Task, error) {
	return b.buildRemux(ctx)
}
