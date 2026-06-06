package goav

import (
	"context"

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
	transcodeGraphCompiler{},
	decodeEncodeToOutputGraphCompiler{},
	decodeToSinkGraphCompiler{},
	rtpDecodeEncodeToOutputGraphCompiler{},
	rtpDecodeToSinkGraphCompiler{},
	rtpRecordGraphCompiler{},
}

func (b *builder) selectCompiler() (builderCompiler, error) {
	for i := range builderCompilers {
		if builderCompilers[i].match(b) {
			return builderCompilers[i], nil
		}
	}
	return nil, ErrUnsupportedBuild
}

func (b *builder) newGraph(_ context.Context) (pipeline.Graph, error) {
	return pipeline.NewGraph(pipeline.GraphConfig{
		Name:     "goav",
		Realtime: b.runtime.realtime,
		Buffer:   b.runtime.buffer,
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
	return &task{graph: graph}, nil
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
	return &task{graph: graph}, nil
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
