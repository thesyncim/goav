package pipeline

import (
	"context"
	"errors"

	"github.com/thesyncim/goav/av"
)

var (
	ErrBackpressure             = errors.New("pipeline: backpressure")
	ErrBufferedMessageUnsafe    = errors.New("pipeline: buffered message unsafe")
	ErrBufferedEdgesUnsupported = errors.New("pipeline: buffered edges unsupported by direct graph")
	ErrClosed                   = errors.New("pipeline: closed")
	ErrInvalidLink              = errors.New("pipeline: invalid link")
	ErrMessageTooLarge          = errors.New("pipeline: message too large")
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
	graph *directRunner
	from  int
}

func (e *directEmitter) Emit(ctx context.Context, msg *Message) error {
	return e.graph.emit(ctx, e.from, msg)
}

// NewGraph creates the single public pipeline graph. Direct execution is used
// for direct buffer policy; bounded buffered execution is selected otherwise.
func NewGraph(config GraphConfig) (Graph, error) {
	if !config.Buffer.IsDirect() {
		return newBufferedRunner(config)
	}
	return newDirectRunner(config)
}

type directRunner struct {
	config  GraphConfig
	index   map[string]int
	nodes   []directNode
	sources []int
	events  chan av.Event
	closed  bool
}

func newDirectRunner(config GraphConfig) (*directRunner, error) {
	if !config.Buffer.IsDirect() {
		return nil, ErrBufferedEdgesUnsupported
	}
	eventCapacity := config.EventCapacity
	if eventCapacity < 1 {
		eventCapacity = 16
	}
	return &directRunner{
		config: config,
		index:  make(map[string]int),
		events: make(chan av.Event, eventCapacity),
	}, nil
}

func (g *directRunner) AddSource(source Source, policy BufferPolicy) (NodeRef, error) {
	if !policy.IsDirect() {
		return "", ErrBufferedEdgesUnsupported
	}
	index, err := g.addNode(directNode{name: source.Name(), kind: nodeSource, source: source})
	if err != nil {
		return "", err
	}
	g.sources = append(g.sources, index)
	return NodeRef(g.nodes[index].name), nil
}

func (g *directRunner) AddStage(stage Stage, policy BufferPolicy) (NodeRef, error) {
	if !policy.IsDirect() {
		return "", ErrBufferedEdgesUnsupported
	}
	index, err := g.addNode(directNode{name: stage.Name(), kind: nodeStage, stage: stage})
	if err != nil {
		return "", err
	}
	return NodeRef(g.nodes[index].name), nil
}

func (g *directRunner) AddSink(sink Sink, policy BufferPolicy) (NodeRef, error) {
	if !policy.IsDirect() {
		return "", ErrBufferedEdgesUnsupported
	}
	index, err := g.addNode(directNode{name: sink.Name(), kind: nodeSink, sink: sink})
	if err != nil {
		return "", err
	}
	return NodeRef(g.nodes[index].name), nil
}

func (g *directRunner) Connect(route Route) error {
	if len(route.To) == 0 {
		return ErrInvalidLink
	}
	policy, err := normalizeRoutePolicy(route.Policy)
	if err != nil {
		return err
	}
	from, ok := g.index[route.From]
	if !ok {
		return ErrUnknownNode
	}
	if g.nodes[from].kind == nodeSink {
		return ErrInvalidLink
	}

	targets := make([]int, 0, len(route.To))
	for i := range route.To {
		to, ok := g.index[route.To[i]]
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
		policy: policy,
		label:  route.Label,
	})
	return nil
}

func normalizeRoutePolicy(policy RoutePolicy) (RoutePolicy, error) {
	switch policy {
	case "", RouteAll:
		return RouteAll, nil
	case RouteByStream, RouteByEvent:
		return policy, nil
	default:
		return "", ErrUnsupportedRoute
	}
}

func (g *directRunner) Run(ctx context.Context) error {
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

func (g *directRunner) Spec() Spec {
	spec := Spec{
		Name:     g.config.Name,
		Realtime: g.config.Realtime,
		Nodes:    make([]NodeSpec, len(g.nodes)),
	}
	for i := range g.nodes {
		node := &g.nodes[i]
		spec.Nodes[i] = directNodeSpec(node)
		for j := range node.routes {
			route := &node.routes[j]
			for k := range route.to {
				to := &g.nodes[route.to[k]]
				spec.Edges = append(spec.Edges, EdgeSpec{
					From:   NodeRef(node.name),
					To:     NodeRef(to.name),
					Policy: route.policy,
					Label:  route.label,
				})
			}
		}
	}
	return spec
}

func (g *directRunner) Events() <-chan av.Event {
	return g.events
}

func (g *directRunner) Close() error {
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

func (g *directRunner) addNode(node directNode) (int, error) {
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

func (g *directRunner) emit(ctx context.Context, from int, msg *Message) error {
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

func (g *directRunner) deliver(ctx context.Context, to int, msg *Message) error {
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

func (g *directRunner) publishEvent(msg *Message) error {
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

func directNodeSpec(node *directNode) NodeSpec {
	spec := NodeSpec{Name: node.name, Kind: directSpecKind(node.kind)}
	describer := directNodeDescriber(node)
	if describer == nil {
		return spec
	}
	described := describer.DescribeNode()
	spec.Detail = described.Detail
	return spec
}

func directNodeDescriber(node *directNode) NodeDescriber {
	switch node.kind {
	case nodeSource:
		if describer, ok := node.source.(NodeDescriber); ok {
			return describer
		}
	case nodeStage:
		if describer, ok := node.stage.(NodeDescriber); ok {
			return describer
		}
	case nodeSink:
		if describer, ok := node.sink.(NodeDescriber); ok {
			return describer
		}
	}
	return nil
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
