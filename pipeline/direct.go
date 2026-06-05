package pipeline

import (
	"context"
	"errors"

	"github.com/thesyncim/goav/av"
)

var (
	ErrBackpressure             = errors.New("pipeline: backpressure")
	ErrBufferedEdgesUnsupported = errors.New("pipeline: buffered edges unsupported by direct graph")
	ErrClosed                   = errors.New("pipeline: closed")
	ErrInvalidLink              = errors.New("pipeline: invalid link")
	ErrNilMessage               = errors.New("pipeline: nil message")
	ErrNodeExists               = errors.New("pipeline: node exists")
	ErrUnsupportedRoute         = errors.New("pipeline: unsupported route")
	ErrUnknownNode              = errors.New("pipeline: unknown node")
)

type nodeKind uint8

const (
	nodeSource nodeKind = iota + 1
	nodeStage
	nodeSink
)

type directNode struct {
	name    string
	kind    nodeKind
	source  Source
	stage   Stage
	sink    Sink
	routes  []directRoute
	emitter directEmitter
}

type directRoute struct {
	to     []int
	policy RoutePolicy
	label  string
}

type directEmitter struct {
	graph *DirectGraph
	from  int
}

func (e *directEmitter) Emit(ctx context.Context, msg *Message) error {
	return e.graph.emit(ctx, e.from, msg)
}

type DirectFactory struct{}

func NewDirectFactory() DirectFactory {
	return DirectFactory{}
}

func (DirectFactory) NewGraph(_ context.Context, config GraphConfig) (Graph, error) {
	return NewDirectGraph(config)
}

type DirectGraph struct {
	config  GraphConfig
	index   map[string]int
	nodes   []directNode
	sources []int
	events  chan av.Event
	closed  bool
}

func NewDirectGraph(config GraphConfig) (*DirectGraph, error) {
	if !config.Buffer.IsDirect() {
		return nil, ErrBufferedEdgesUnsupported
	}
	eventCapacity := config.Buffer.Capacity
	if eventCapacity < 1 {
		eventCapacity = 1
	}
	return &DirectGraph{
		config: config,
		index:  make(map[string]int),
		events: make(chan av.Event, eventCapacity),
	}, nil
}

func (g *DirectGraph) AddSource(source Source, policy BufferPolicy) (PadRef, error) {
	if !policy.IsDirect() {
		return PadRef{}, ErrBufferedEdgesUnsupported
	}
	index, err := g.addNode(directNode{name: source.Name(), kind: nodeSource, source: source})
	if err != nil {
		return PadRef{}, err
	}
	g.sources = append(g.sources, index)
	return PadRef{Node: g.nodes[index].name, Pad: "out"}, nil
}

func (g *DirectGraph) AddStage(stage Stage, policy BufferPolicy) (PadRef, error) {
	if !policy.IsDirect() {
		return PadRef{}, ErrBufferedEdgesUnsupported
	}
	index, err := g.addNode(directNode{name: stage.Name(), kind: nodeStage, stage: stage})
	if err != nil {
		return PadRef{}, err
	}
	return PadRef{Node: g.nodes[index].name, Pad: "inout"}, nil
}

func (g *DirectGraph) AddSink(sink Sink, policy BufferPolicy) (PadRef, error) {
	if !policy.IsDirect() {
		return PadRef{}, ErrBufferedEdgesUnsupported
	}
	index, err := g.addNode(directNode{name: sink.Name(), kind: nodeSink, sink: sink})
	if err != nil {
		return PadRef{}, err
	}
	return PadRef{Node: g.nodes[index].name, Pad: "in"}, nil
}

func (g *DirectGraph) Link(link Link) error {
	return g.Route(Route{
		From:   link.From,
		To:     []PadRef{link.To},
		Policy: RouteAll,
	})
}

func (g *DirectGraph) Route(route Route) error {
	if route.Policy == RouteByLabel {
		return ErrUnsupportedRoute
	}
	from, ok := g.index[route.From.Node]
	if !ok {
		return ErrUnknownNode
	}
	if g.nodes[from].kind == nodeSink {
		return ErrInvalidLink
	}

	targets := make([]int, 0, len(route.To))
	for i := range route.To {
		to, ok := g.index[route.To[i].Node]
		if !ok {
			return ErrUnknownNode
		}
		if g.nodes[to].kind == nodeSource {
			return ErrInvalidLink
		}
		targets = append(targets, to)
	}
	g.nodes[from].routes = append(g.nodes[from].routes, directRoute{
		to:     targets,
		policy: route.Policy,
		label:  route.Label,
	})
	return nil
}

func (g *DirectGraph) Run(ctx context.Context) error {
	if g.closed {
		return ErrClosed
	}
	for i := range g.sources {
		node := &g.nodes[g.sources[i]]
		if err := node.source.Start(ctx, &node.emitter); err != nil {
			return err
		}
	}
	return nil
}

func (g *DirectGraph) Spec() Spec {
	spec := Spec{
		Name:     g.config.Name,
		Realtime: g.config.Realtime,
		Nodes:    make([]NodeSpec, len(g.nodes)),
	}
	for i := range g.nodes {
		node := &g.nodes[i]
		spec.Nodes[i] = NodeSpec{Name: node.name, Kind: directSpecKind(node.kind)}
		for j := range node.routes {
			route := &node.routes[j]
			for k := range route.to {
				to := &g.nodes[route.to[k]]
				spec.Edges = append(spec.Edges, EdgeSpec{
					From:   PadRef{Node: node.name, Pad: directOutputPad(node.kind)},
					To:     PadRef{Node: to.name, Pad: directInputPad(to.kind)},
					Policy: route.policy,
					Label:  route.label,
				})
			}
		}
	}
	return spec
}

func (g *DirectGraph) Events() <-chan av.Event {
	return g.events
}

func (g *DirectGraph) Close() error {
	if g.closed {
		return nil
	}
	g.closed = true
	var first error
	for i := range g.nodes {
		node := &g.nodes[i]
		var err error
		switch node.kind {
		case nodeSource:
			err = node.source.Close()
		case nodeStage:
			err = node.stage.Close()
		case nodeSink:
			err = node.sink.Close()
		}
		if first == nil && err != nil {
			first = err
		}
	}
	close(g.events)
	return first
}

func (g *DirectGraph) addNode(node directNode) (int, error) {
	if node.name == "" {
		return 0, ErrUnknownNode
	}
	if _, ok := g.index[node.name]; ok {
		return 0, ErrNodeExists
	}
	index := len(g.nodes)
	node.emitter = directEmitter{graph: g, from: index}
	g.index[node.name] = index
	g.nodes = append(g.nodes, node)
	return index, nil
}

func (g *DirectGraph) emit(ctx context.Context, from int, msg *Message) error {
	if g.closed {
		return ErrClosed
	}
	if msg == nil {
		return ErrNilMessage
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := g.publishEvent(msg); err != nil {
		return err
	}

	routes := g.nodes[from].routes
	for i := range routes {
		route := &routes[i]
		if !route.matches(msg) {
			continue
		}
		for j := range route.to {
			if err := g.deliver(ctx, route.to[j], msg); err != nil {
				return err
			}
		}
	}
	return nil
}

func (g *DirectGraph) deliver(ctx context.Context, to int, msg *Message) error {
	node := &g.nodes[to]
	switch node.kind {
	case nodeStage:
		return node.stage.Handle(ctx, msg, &node.emitter)
	case nodeSink:
		return node.sink.Handle(ctx, msg)
	default:
		return ErrInvalidLink
	}
}

func (g *DirectGraph) publishEvent(msg *Message) error {
	if msg.Kind != MessageEvent || msg.Event == nil {
		return nil
	}
	select {
	case g.events <- *msg.Event:
		return nil
	default:
		return ErrBackpressure
	}
}

func (r *directRoute) matches(msg *Message) bool {
	switch r.policy {
	case "", RouteAll:
		return true
	case RouteByStream:
		return r.matchesStream(msg)
	case RouteByEvent:
		return r.matchesEvent(msg)
	default:
		return false
	}
}

func (r *directRoute) matchesStream(msg *Message) bool {
	if r.label == "" {
		return true
	}
	switch msg.Kind {
	case MessagePacket:
		return msg.Packet != nil && string(msg.Packet.StreamID) == r.label
	case MessageFrame:
		return msg.Frame != nil && string(msg.Frame.StreamID) == r.label
	case MessageEvent:
		return msg.Event != nil && string(msg.Event.StreamID) == r.label
	default:
		return false
	}
}

func (r *directRoute) matchesEvent(msg *Message) bool {
	if msg.Kind != MessageEvent || msg.Event == nil {
		return false
	}
	return r.label == "" || string(msg.Event.Type) == r.label
}

func directSpecKind(kind nodeKind) NodeKind {
	switch kind {
	case nodeSource:
		return NodeSource
	case nodeStage:
		return NodeStage
	case nodeSink:
		return NodeSink
	default:
		return ""
	}
}

func directOutputPad(kind nodeKind) string {
	switch kind {
	case nodeSource:
		return "out"
	case nodeStage:
		return "inout"
	default:
		return ""
	}
}

func directInputPad(kind nodeKind) string {
	switch kind {
	case nodeStage:
		return "inout"
	case nodeSink:
		return "in"
	default:
		return ""
	}
}
