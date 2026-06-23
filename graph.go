// The internal handle-based graph layer behind the expert package: a fluent
// builder over caller-provided pipeline nodes plus the string-based bridge
// methods the expert package reaches through (*runtime).ExpertGraph.

package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

// graphNode is the internal handle to one added source, stage, or sink; its
// outlets and inlets wire Connect routes. The expert package exposes the
// public twin (expert.GraphNode).
type graphNode struct {
	name string
}

// graphOutlet selects which messages leave a node on a route: everything
// (Out), one stream (Stream), or one event type (Event).
type graphOutlet struct {
	name   string
	policy pipeline.RoutePolicy
	label  string
}

// graphInlet is the receiving end of a Connect route.
type graphInlet struct {
	name string
}

// Name returns the node's graph-unique name.
func (n graphNode) Name() string {
	return n.name
}

func (n graphNode) branchSource() branchSourceBinding {
	return branchSourceBinding{from: n.name, policy: pipeline.RouteAll}
}

// In returns the node's inlet for Connect.
func (n graphNode) In() graphInlet {
	return graphInlet(n)
}

// Out returns an outlet routing every message the node emits.
func (n graphNode) Out() graphOutlet {
	return graphOutlet{name: n.name, policy: pipeline.RouteAll}
}

// Stream returns an outlet routing only the given stream's messages.
func (n graphNode) Stream(stream av.StreamID) graphOutlet {
	return graphOutlet{name: n.name, policy: pipeline.RouteByStream, label: string(stream)}
}

// Event returns an outlet routing only the given event type.
func (n graphNode) Event(event av.EventType) graphOutlet {
	return graphOutlet{name: n.name, policy: pipeline.RouteByEvent, label: string(event)}
}

func (o graphOutlet) branchSource() branchSourceBinding {
	policy := o.policy
	if policy == "" {
		policy = pipeline.RouteAll
	}
	return branchSourceBinding{from: o.name, policy: policy, label: o.label}
}

// Name returns the name of the node the outlet routes from.
func (o graphOutlet) Name() string {
	return o.name
}

// newExpertGraph returns the fluent handle builder over a goav runtime; the
// expert package reaches it through the (*runtime).ExpertGraph bridge, which
// guarantees the concrete runtime type.
func newExpertGraph(r *runtime) *graphBuilder {
	return &graphBuilder{builder: r.New()}
}

// ExpertGraph is the structural bridge behind expert.Graph: it returns the
// graph core the expert package asserts against its own leaf-typed interface,
// keeping expert types out of the root package. The any return is what makes
// the assertion possible without an import in either direction.
func (r *runtime) ExpertGraph() any {
	return newExpertGraph(r)
}

type graphBuilder struct {
	builder *builder
	err     error
}

func (g *graphBuilder) Source(name string, source pipeline.Source) graphNode {
	if source == nil {
		g.setErr(ErrNilSource)
		return graphNode{name: name}
	}
	node := namedSource{name: firstNonEmpty(name, source.Name()), source: source}
	g.builder = g.builder.Source(node)
	return graphNode{name: node.Name()}
}

func (g *graphBuilder) Stage(name string, stage pipeline.Stage) graphNode {
	if stage == nil {
		g.setErr(ErrNilStage)
		return graphNode{name: name}
	}
	node := namedStage{name: firstNonEmpty(name, stage.Name()), stage: stage}
	g.builder = g.builder.Stage(node)
	return graphNode{name: node.Name()}
}

func (g *graphBuilder) Sink(name string, sink pipeline.Sink) graphNode {
	if sink == nil {
		g.setErr(ErrNilSink)
		return graphNode{name: name}
	}
	node := namedSink{name: firstNonEmpty(name, sink.Name()), sink: sink}
	g.builder = g.builder.Sink(node)
	return graphNode{name: node.Name()}
}

func (g *graphBuilder) Connect(from graphOutlet, to ...graphInlet) *graphBuilder {
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

// AddSource registers a source under the resolved node name (the given name,
// or the source's own when empty) and reports it — the string-based bridge
// behind expert.GraphBuilder.Source. A nil source latches ErrNilSource.
func (g *graphBuilder) AddSource(name string, source pipeline.Source) string {
	return g.Source(name, source).name
}

// AddStage registers a stage under the resolved node name and reports it —
// the bridge behind expert.GraphBuilder.Stage. A nil stage latches
// ErrNilStage.
func (g *graphBuilder) AddStage(name string, stage pipeline.Stage) string {
	return g.Stage(name, stage).name
}

// AddSink registers a sink under the resolved node name and reports it — the
// bridge behind expert.GraphBuilder.Sink. A nil sink latches ErrNilSink.
func (g *graphBuilder) AddSink(name string, sink pipeline.Sink) string {
	return g.Sink(name, sink).name
}

// AddRoute connects one outlet (node, policy, label) to the named inlets —
// the bridge behind expert.GraphBuilder.Connect, with the same validation.
func (g *graphBuilder) AddRoute(from string, policy pipeline.RoutePolicy, label string, to ...string) {
	inlets := make([]graphInlet, len(to))
	for i := range to {
		inlets[i] = graphInlet{name: to[i]}
	}
	g.Connect(graphOutlet{name: from, policy: policy, label: label}, inlets...)
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

func (s namedSource) DroppedMessages() uint64 {
	return droppedMessagesFrom(s.source)
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

func (s namedStage) DroppedMessages() uint64 {
	return droppedMessagesFrom(s.stage)
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

func (s namedSink) DroppedMessages() uint64 {
	return droppedMessagesFrom(s.sink)
}

func droppedMessagesFrom(node any) uint64 {
	reporter, ok := node.(pipeline.DropReporter)
	if !ok {
		return 0
	}
	return reporter.DroppedMessages()
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
