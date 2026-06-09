package goav

import (
	"context"
	"errors"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

// ErrExpertRuntimeRequired reports attempts to use expert graph wiring with a
// runtime that was not created by goav.New or goav.Default.
var ErrExpertRuntimeRequired = errors.New("goav: expert graph requires goav runtime")

// ExpertTools exposes advanced APIs that are intentionally kept off Runtime.
type ExpertTools struct {
	runtime Runtime
}

// Expert returns the explicit advanced entry point for manual graph wiring.
func Expert(runtime Runtime) ExpertTools {
	return ExpertTools{runtime: runtime}
}

// Graph creates a handle-based graph builder for advanced/manual compositions.
func (e ExpertTools) Graph() GraphBuilder {
	standard, ok := e.runtime.(*runtime)
	if !ok || standard == nil {
		return &graphBuilder{err: ErrExpertRuntimeRequired}
	}
	return &graphBuilder{builder: standard.New()}
}

type GraphNode struct {
	name string
}

type GraphOutlet struct {
	name   string
	policy pipeline.RoutePolicy
	label  string
}

type GraphInlet struct {
	name string
}

func (n GraphNode) Name() string {
	return n.name
}

func (n GraphNode) branchSource() branchSourceBinding {
	return branchSourceBinding{from: n.name, policy: pipeline.RouteAll}
}

func (n GraphNode) In() GraphInlet {
	return GraphInlet(n)
}

func (n GraphNode) Out() GraphOutlet {
	return GraphOutlet{name: n.name, policy: pipeline.RouteAll}
}

func (n GraphNode) Stream(stream av.StreamID) GraphOutlet {
	return GraphOutlet{name: n.name, policy: pipeline.RouteByStream, label: string(stream)}
}

func (n GraphNode) Event(event av.EventType) GraphOutlet {
	return GraphOutlet{name: n.name, policy: pipeline.RouteByEvent, label: string(event)}
}

func (o GraphOutlet) branchSource() branchSourceBinding {
	policy := o.policy
	if policy == "" {
		policy = pipeline.RouteAll
	}
	return branchSourceBinding{from: o.name, policy: policy, label: o.label}
}

type graphBuilder struct {
	builder builderAPI
	err     error
}

func (g *graphBuilder) Source(name string, source pipeline.Source) GraphNode {
	if source == nil {
		g.setErr(ErrNilSource)
		return GraphNode{name: name}
	}
	node := namedSource{name: firstNonEmpty(name, source.Name()), source: source}
	g.builder = g.builder.Source(node)
	return GraphNode{name: node.Name()}
}

func (g *graphBuilder) Stage(name string, stage pipeline.Stage) GraphNode {
	if stage == nil {
		g.setErr(ErrNilStage)
		return GraphNode{name: name}
	}
	node := namedStage{name: firstNonEmpty(name, stage.Name()), stage: stage}
	g.builder = g.builder.Stage(node)
	return GraphNode{name: node.Name()}
}

func (g *graphBuilder) Sink(name string, sink pipeline.Sink) GraphNode {
	if sink == nil {
		g.setErr(ErrNilSink)
		return GraphNode{name: name}
	}
	node := namedSink{name: firstNonEmpty(name, sink.Name()), sink: sink}
	g.builder = g.builder.Sink(node)
	return GraphNode{name: node.Name()}
}

func (g *graphBuilder) Connect(from GraphOutlet, to ...GraphInlet) GraphBuilder {
	if len(to) == 0 {
		g.setErr(pipeline.ErrInvalidLink)
		return g
	}
	if from.name == "" {
		g.setErr(pipeline.ErrUnknownNode)
		return g
	}
	destinations := make([]string, len(to))
	for i := range to {
		if to[i].name == "" {
			g.setErr(pipeline.ErrUnknownNode)
			return g
		}
		destinations[i] = to[i].name
	}
	policy := from.policy
	if policy == "" {
		policy = pipeline.RouteAll
	}
	g.builder = g.builder.Routes(pipeline.Route{
		From:   from.name,
		To:     destinations,
		Policy: policy,
		Label:  from.label,
	})
	return g
}

func (g *graphBuilder) Describe() (pipeline.Spec, error) {
	if g.err != nil {
		return pipeline.Spec{}, g.err
	}
	return g.builder.Describe()
}

func (g *graphBuilder) Build(ctx context.Context) (Task, error) {
	if g.err != nil {
		return nil, g.err
	}
	return g.builder.Build(ctx)
}

func (g *graphBuilder) setErr(err error) {
	if g.err == nil {
		g.err = err
	}
}

type namedSource struct {
	name   string
	source pipeline.Source
}

func (s namedSource) Name() string {
	return s.name
}

func (s namedSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	return s.source.Start(ctx, emitter)
}

func (s namedSource) Close() error {
	return s.source.Close()
}

func (s namedSource) DescribeNode() pipeline.NodeSpec {
	return describeNamedNode(s.name, pipeline.NodeSource, s.source)
}

type namedStage struct {
	name  string
	stage pipeline.Stage
}

func (s namedStage) Name() string {
	return s.name
}

func (s namedStage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	return s.stage.Handle(ctx, msg, emitter)
}

func (s namedStage) Close() error {
	return s.stage.Close()
}

func (s namedStage) DescribeNode() pipeline.NodeSpec {
	return describeNamedNode(s.name, pipeline.NodeStage, s.stage)
}

type namedSink struct {
	name string
	sink pipeline.Sink
}

func (s namedSink) Name() string {
	return s.name
}

func (s namedSink) Handle(ctx context.Context, msg *pipeline.Message) error {
	return s.sink.Handle(ctx, msg)
}

func (s namedSink) Close() error {
	return s.sink.Close()
}

func (s namedSink) DescribeNode() pipeline.NodeSpec {
	return describeNamedNode(s.name, pipeline.NodeSink, s.sink)
}

func describeNamedNode(name string, kind pipeline.NodeKind, node any) pipeline.NodeSpec {
	spec := pipeline.NodeSpec{Name: name, Kind: kind}
	describer, ok := node.(pipeline.NodeDescriber)
	if !ok {
		return spec
	}
	described := describer.DescribeNode()
	spec.Detail = described.Detail
	return spec
}
