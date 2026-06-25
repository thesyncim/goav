// Package expert is the handle-based graph layer deliberately kept off the
// goav grammar: Graph(runtime) wires caller-provided pipeline Sources,
// Stages, and Sinks into a goav Task through explicit routes — for
// compositions the From/To grammar cannot express. Graph handles also anchor
// runtime branches: pass a GraphNode or GraphOutlet to goav.Branch(...).From.
// Most applications should start with goav.From and never need this package.
package expert

import (
	"context"
	"errors"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

// ErrRuntimeRequired reports attempts to use expert graph wiring without a
// runtime created by goav.New, goav.MustNew, bundle.New, or bundle.MustNew.
var ErrRuntimeRequired = errors.New("expert: graph requires goav runtime")

// GraphBuilder is the handle-based expert graph builder: named nodes wired by
// explicit Connect routes, described and built like any goav task.
type GraphBuilder interface {
	// Source adds a source node under the given name (the source's own name
	// when empty) and returns its handle.
	Source(string, pipeline.Source) GraphNode
	// Stage adds a stage node and returns its handle.
	Stage(string, pipeline.Stage) GraphNode
	// Sink adds a sink node and returns its handle.
	Sink(string, pipeline.Sink) GraphNode
	// Connect routes one outlet to the given inlets.
	Connect(GraphOutlet, ...GraphInlet) GraphBuilder
	// Describe reports the wired graph spec, or the first wiring error.
	Describe() (pipeline.Spec, error)
	// Build compiles the wired graph into a runnable goav live task.
	Build(context.Context) (goav.LiveTask, error)
}

// Graph opens the handle-based graph builder on a goav runtime.
func Graph(runtime *goav.Runtime) GraphBuilder {
	if runtime == nil {
		return &graphBuilder{err: ErrRuntimeRequired}
	}
	if core, ok := runtime.ExpertGraph().(graphCore); ok {
		return &graphBuilder{core: core}
	}
	return &graphBuilder{err: ErrRuntimeRequired}
}

// graphCore is the structural bridge a goav runtime provides behind its
// ExpertGraph capability: string-named node registration plus Describe and
// Build. Root exposes it without importing this package; this package
// asserts it without naming root internals.
type graphCore interface {
	AddSource(name string, source pipeline.Source) string
	AddStage(name string, stage pipeline.Stage) string
	AddSink(name string, sink pipeline.Sink) string
	AddRoute(from string, policy pipeline.RoutePolicy, label string, to ...string)
	Describe() (pipeline.Spec, error)
	Build(ctx context.Context) (goav.LiveTask, error)
}

// GraphNode is an expert-graph handle to one added source, stage, or sink;
// its outlets and inlets wire Connect routes, and the handle itself anchors
// runtime branches through goav.Branch(...).From.
type GraphNode struct {
	name string
}

// GraphOutlet selects which messages leave a node on a route: everything
// (Out), one stream (Stream), or one event type (Event).
type GraphOutlet struct {
	name   string
	policy pipeline.RoutePolicy
	label  string
}

// GraphInlet is the receiving end of a Connect route.
type GraphInlet struct {
	name string
}

// Name returns the node's graph-unique name.
func (n GraphNode) Name() string {
	return n.name
}

// Route reports the route the handle anchors for goav.Branch(...).From:
// every message the node emits.
func (n GraphNode) Route() pipeline.Route {
	return pipeline.Route{From: n.name, Policy: pipeline.RouteAll}
}

// In returns the node's inlet for Connect.
func (n GraphNode) In() GraphInlet {
	return GraphInlet(n)
}

// Out returns an outlet routing every message the node emits.
func (n GraphNode) Out() GraphOutlet {
	return GraphOutlet{name: n.name, policy: pipeline.RouteAll}
}

// Stream returns an outlet routing only the given stream's messages.
func (n GraphNode) Stream(stream av.StreamID) GraphOutlet {
	return GraphOutlet{name: n.name, policy: pipeline.RouteByStream, label: string(stream)}
}

// Event returns an outlet routing only the given event type.
func (n GraphNode) Event(event av.EventType) GraphOutlet {
	return GraphOutlet{name: n.name, policy: pipeline.RouteByEvent, label: string(event)}
}

// Name returns the name of the node the outlet routes from.
func (o GraphOutlet) Name() string {
	return o.name
}

// Route reports the route the outlet anchors for goav.Branch(...).From: the
// node plus the outlet's stream or event narrowing.
func (o GraphOutlet) Route() pipeline.Route {
	policy := o.policy
	if policy == "" {
		policy = pipeline.RouteAll
	}
	return pipeline.Route{From: o.name, Policy: policy, Label: o.label}
}

type graphBuilder struct {
	core graphCore
	err  error
}

func (g *graphBuilder) Source(name string, source pipeline.Source) GraphNode {
	if g.err != nil {
		return GraphNode{name: name}
	}
	return GraphNode{name: g.core.AddSource(name, source)}
}

func (g *graphBuilder) Stage(name string, stage pipeline.Stage) GraphNode {
	if g.err != nil {
		return GraphNode{name: name}
	}
	return GraphNode{name: g.core.AddStage(name, stage)}
}

func (g *graphBuilder) Sink(name string, sink pipeline.Sink) GraphNode {
	if g.err != nil {
		return GraphNode{name: name}
	}
	return GraphNode{name: g.core.AddSink(name, sink)}
}

func (g *graphBuilder) Connect(from GraphOutlet, to ...GraphInlet) GraphBuilder {
	if g.err != nil {
		return g
	}
	destinations := make([]string, len(to))
	for i := range to {
		destinations[i] = to[i].name
	}
	g.core.AddRoute(from.name, from.policy, from.label, destinations...)
	return g
}

func (g *graphBuilder) Describe() (pipeline.Spec, error) {
	if g.err != nil {
		return pipeline.Spec{}, g.err
	}
	return g.core.Describe()
}

func (g *graphBuilder) Build(ctx context.Context) (goav.LiveTask, error) {
	if g.err != nil {
		return nil, g.err
	}
	return g.core.Build(ctx)
}
