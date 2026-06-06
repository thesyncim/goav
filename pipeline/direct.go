package pipeline

import (
	"context"
	"errors"
	"sync"

	"github.com/thesyncim/goav/av"
)

var (
	ErrBackpressure             = errors.New("pipeline: backpressure")
	ErrBufferedMessageUnsafe    = errors.New("pipeline: buffered message unsafe")
	ErrBufferedEdgesUnsupported = errors.New("pipeline: buffered edges unsupported by direct graph")
	ErrClosed                   = errors.New("pipeline: closed")
	ErrDynamicGraphUnsupported  = errors.New("pipeline: dynamic graph unsupported")
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
	active  bool
	source  Source
	stage   Stage
	sink    Sink
	routes  []directRoute
	emitter *directEmitter
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
	mu      sync.RWMutex
	statsMu sync.Mutex
	index   map[string]int
	nodes   []directNode
	sources []int
	events  chan av.Event
	stats   GraphStats
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
	g.mu.Lock()
	defer g.mu.Unlock()
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
	g.mu.Lock()
	defer g.mu.Unlock()
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
	g.mu.Lock()
	defer g.mu.Unlock()
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
	g.mu.Lock()
	defer g.mu.Unlock()
	from, ok := g.index[route.From]
	if !ok || !g.nodes[from].active {
		return ErrUnknownNode
	}
	if g.nodes[from].kind == nodeSink {
		return ErrInvalidLink
	}

	targets := make([]int, 0, len(route.To))
	for i := range route.To {
		to, ok := g.index[route.To[i]]
		if !ok || !g.nodes[to].active {
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

func (g *directRunner) Disconnect(route Route) error {
	if len(route.To) == 0 {
		return ErrInvalidLink
	}
	policy, err := normalizeRoutePolicy(route.Policy)
	if err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	from, ok := g.index[route.From]
	if !ok || !g.nodes[from].active {
		return ErrUnknownNode
	}
	targets := make(map[int]struct{}, len(route.To))
	for i := range route.To {
		to, ok := g.index[route.To[i]]
		if !ok {
			return ErrUnknownNode
		}
		targets[to] = struct{}{}
	}
	removed := false
	routes := g.nodes[from].routes[:0]
	for i := range g.nodes[from].routes {
		existing := g.nodes[from].routes[i]
		if existing.policy != policy || existing.label != route.Label {
			routes = append(routes, existing)
			continue
		}
		to := existing.to[:0]
		for j := range existing.to {
			if _, ok := targets[existing.to[j]]; ok {
				removed = true
				continue
			}
			to = append(to, existing.to[j])
		}
		if len(to) == 0 {
			continue
		}
		existing.to = to
		routes = append(routes, existing)
	}
	g.nodes[from].routes = routes
	if !removed {
		return ErrInvalidLink
	}
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
	g.mu.RLock()
	if g.closed {
		g.mu.RUnlock()
		return ErrClosed
	}
	var sourceStack [8]int
	sources := sourceStack[:0]
	for i := range g.sources {
		if !g.nodes[g.sources[i]].active {
			continue
		}
		if len(sources) < cap(sources) {
			sources = append(sources, g.sources[i])
			continue
		}
		sources = append(sources, g.sources[i])
	}
	g.mu.RUnlock()
	for i := range sources {
		node, err := g.nodeSnapshot(sources[i])
		if err != nil {
			return err
		}
		if err := node.source.Start(ctx, node.emitter); err != nil {
			return err
		}
	}
	return nil
}

func (g *directRunner) Spec() Spec {
	g.mu.RLock()
	defer g.mu.RUnlock()
	spec := Spec{
		Name:     g.config.Name,
		Realtime: g.config.Realtime,
	}
	for i := range g.nodes {
		node := &g.nodes[i]
		if !node.active {
			continue
		}
		spec.Nodes = append(spec.Nodes, directNodeSpec(node))
		for j := range node.routes {
			route := &node.routes[j]
			for k := range route.to {
				to := &g.nodes[route.to[k]]
				if !to.active {
					continue
				}
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

func (g *directRunner) Stats() GraphStats {
	g.statsMu.Lock()
	defer g.statsMu.Unlock()
	return cloneGraphStats(g.stats)
}

func (g *directRunner) Close() error {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return nil
	}
	g.closed = true
	nodes := make([]directNode, 0, len(g.nodes))
	for i := range g.nodes {
		node := &g.nodes[i]
		if !node.active {
			continue
		}
		node.active = false
		nodes = append(nodes, *node)
	}
	g.mu.Unlock()

	var first error
	for i := range nodes {
		err := closeDirectNode(&nodes[i])
		if first == nil && err != nil {
			first = err
		}
	}
	close(g.events)
	return first
}

func (g *directRunner) Remove(ref NodeRef) error {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return ErrClosed
	}
	index, ok := g.index[ref.String()]
	if !ok || !g.nodes[index].active {
		g.mu.Unlock()
		return ErrUnknownNode
	}
	if g.nodes[index].kind == nodeSource {
		g.mu.Unlock()
		return ErrInvalidLink
	}
	node := &g.nodes[index]
	node.active = false
	node.routes = nil
	delete(g.index, ref.String())
	for i := range g.nodes {
		if !g.nodes[i].active {
			continue
		}
		for j := range g.nodes[i].routes {
			route := &g.nodes[i].routes[j]
			to := route.to[:0]
			for k := range route.to {
				if route.to[k] == index {
					continue
				}
				to = append(to, route.to[k])
			}
			route.to = to
		}
		routes := g.nodes[i].routes[:0]
		for j := range g.nodes[i].routes {
			if len(g.nodes[i].routes[j].to) == 0 {
				continue
			}
			routes = append(routes, g.nodes[i].routes[j])
		}
		g.nodes[i].routes = routes
	}
	closeNode := *node
	g.mu.Unlock()
	return closeDirectNode(&closeNode)
}

func (g *directRunner) addNode(node directNode) (int, error) {
	if node.name == "" {
		return 0, ErrUnknownNode
	}
	if _, ok := g.index[node.name]; ok {
		return 0, ErrNodeExists
	}
	index := len(g.nodes)
	node.active = true
	node.emitter = &directEmitter{graph: g, from: index}
	g.index[node.name] = index
	g.nodes = append(g.nodes, node)
	return index, nil
}

func (g *directRunner) emit(ctx context.Context, from int, msg *Message) error {
	if msg == nil {
		return ErrNilMessage
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	g.mu.RLock()
	if g.closed {
		g.mu.RUnlock()
		return ErrClosed
	}
	if from < 0 || from >= len(g.nodes) {
		g.mu.RUnlock()
		return ErrUnknownNode
	}
	if !g.nodes[from].active {
		g.mu.RUnlock()
		return nil
	}
	g.observeMessage(msg)
	if err := g.publishEvent(msg); err != nil {
		g.mu.RUnlock()
		return err
	}

	var targetStack [8]int
	targets := targetStack[:0]
	routes := g.nodes[from].routes
	for i := range routes {
		route := &routes[i]
		if !route.matches(msg) {
			continue
		}
		for j := range route.to {
			if len(targets) < cap(targets) {
				targets = append(targets, route.to[j])
				continue
			}
			targets = append(targets, route.to[j])
		}
	}
	g.mu.RUnlock()
	for i := range targets {
		if err := g.deliver(ctx, targets[i], msg); err != nil {
			return err
		}
	}
	return nil
}

func (g *directRunner) deliver(ctx context.Context, to int, msg *Message) error {
	node, err := g.nodeSnapshot(to)
	if err != nil {
		if errors.Is(err, ErrUnknownNode) {
			return nil
		}
		return err
	}
	g.observeDelivered()
	switch node.kind {
	case nodeStage:
		return node.stage.Handle(ctx, msg, node.emitter)
	case nodeSink:
		return node.sink.Handle(ctx, msg)
	default:
		return ErrInvalidLink
	}
}

func (g *directRunner) nodeSnapshot(index int) (directNode, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.closed {
		return directNode{}, ErrClosed
	}
	if index < 0 || index >= len(g.nodes) {
		return directNode{}, ErrUnknownNode
	}
	if !g.nodes[index].active {
		return directNode{}, ErrUnknownNode
	}
	return g.nodes[index], nil
}

func closeDirectNode(node *directNode) error {
	switch node.kind {
	case nodeSource:
		return node.source.Close()
	case nodeStage:
		return node.stage.Close()
	case nodeSink:
		return node.sink.Close()
	default:
		return nil
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

func (g *directRunner) observeMessage(msg *Message) {
	g.statsMu.Lock()
	defer g.statsMu.Unlock()
	g.stats.observeMessage(msg)
}

func (g *directRunner) observeDelivered() {
	g.statsMu.Lock()
	defer g.statsMu.Unlock()
	g.stats.Delivered++
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
