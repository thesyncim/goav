package pipeline

import (
	"context"
	"sync"

	"github.com/thesyncim/goav/av"
)

const bufferedMaxFramePlanes = 8

type bufferedNode struct {
	name       string
	kind       nodeKind
	source     Source
	stage      Stage
	sink       Sink
	policy     BufferPolicy
	drop       dropController
	queue      chan bufferedMessage
	routes     []directRoute
	emitter    bufferedEmitter
	queueMutex sync.Mutex
}

type bufferedEmitter struct {
	graph *BufferedGraph
	from  int
}

func (e *bufferedEmitter) Emit(ctx context.Context, msg *Message) error {
	return e.graph.emit(ctx, e.from, msg)
}

type bufferedMessage struct {
	message Message
	packet  av.Packet
	frame   av.Frame
	event   av.Event
	planes  [bufferedMaxFramePlanes]av.Plane
}

type BufferedGraph struct {
	config  GraphConfig
	index   map[string]int
	nodes   []bufferedNode
	sources []int
	events  chan av.Event
	pending sync.WaitGroup
	closed  bool
}

func NewBufferedGraph(config GraphConfig) (*BufferedGraph, error) {
	if config.Buffer.IsDirect() {
		return nil, ErrBufferedEdgesUnsupported
	}
	config.Buffer = normalizeBufferedPolicy(config.Buffer)
	eventCapacity := config.EventCapacity
	if eventCapacity < 1 {
		eventCapacity = 16
	}
	return &BufferedGraph{
		config: config,
		index:  make(map[string]int),
		events: make(chan av.Event, eventCapacity),
	}, nil
}

func normalizeBufferedPolicy(policy BufferPolicy) BufferPolicy {
	if policy.Drop == "" {
		policy.Drop = DropNever
	}
	if policy.Capacity < 1 {
		policy.Capacity = 1
	}
	return policy
}

func (g *BufferedGraph) AddSource(source Source, policy BufferPolicy) (NodeRef, error) {
	index, err := g.addNode(bufferedNode{name: source.Name(), kind: nodeSource, source: source, policy: g.nodePolicy(policy)})
	if err != nil {
		return "", err
	}
	g.sources = append(g.sources, index)
	return NodeRef(g.nodes[index].name), nil
}

func (g *BufferedGraph) AddStage(stage Stage, policy BufferPolicy) (NodeRef, error) {
	index, err := g.addNode(bufferedNode{name: stage.Name(), kind: nodeStage, stage: stage, policy: g.nodePolicy(policy)})
	if err != nil {
		return "", err
	}
	return NodeRef(g.nodes[index].name), nil
}

func (g *BufferedGraph) AddSink(sink Sink, policy BufferPolicy) (NodeRef, error) {
	index, err := g.addNode(bufferedNode{name: sink.Name(), kind: nodeSink, sink: sink, policy: g.nodePolicy(policy)})
	if err != nil {
		return "", err
	}
	return NodeRef(g.nodes[index].name), nil
}

func (g *BufferedGraph) Connect(connection Connection) error {
	if len(connection.To) == 0 {
		return ErrInvalidLink
	}
	to := make([]NodeRef, len(connection.To))
	for i := range connection.To {
		to[i] = NodeRef(connection.To[i])
	}
	return g.Route(Route{
		From:   NodeRef(connection.From),
		To:     to,
		Policy: connection.Policy,
		Label:  connection.Label,
	})
}

func (g *BufferedGraph) Link(link Link) error {
	return g.Connect(Connection{
		From:   link.From.String(),
		To:     []string{link.To.String()},
		Policy: RouteAll,
	})
}

func (g *BufferedGraph) Route(route Route) error {
	if route.Policy == RouteByLabel {
		return ErrUnsupportedRoute
	}
	from, ok := g.index[route.From.String()]
	if !ok {
		return ErrUnknownNode
	}
	if g.nodes[from].kind == nodeSink {
		return ErrInvalidLink
	}

	targets := make([]int, 0, len(route.To))
	for i := range route.To {
		to, ok := g.index[route.To[i].String()]
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

func (g *BufferedGraph) Run(ctx context.Context) error {
	if g.closed {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	g.openQueues()
	errs := make(chan error, len(g.nodes)+len(g.sources)+1)
	report := func(err error) {
		if err == nil {
			return
		}
		select {
		case errs <- err:
		default:
		}
	}

	var workers sync.WaitGroup
	for i := range g.nodes {
		if g.nodes[i].kind == nodeSource {
			continue
		}
		index := i
		workers.Add(1)
		go func() {
			defer workers.Done()
			report(g.runNode(ctx, index))
		}()
	}

	var sources sync.WaitGroup
	for i := range g.sources {
		index := g.sources[i]
		sources.Add(1)
		go func() {
			defer sources.Done()
			report(g.nodes[index].source.Start(ctx, &g.nodes[index].emitter))
		}()
	}

	sources.Wait()
	g.pending.Wait()
	g.closeQueues()
	workers.Wait()

	select {
	case err := <-errs:
		return err
	default:
		return nil
	}
}

func (g *BufferedGraph) Spec() Spec {
	spec := Spec{
		Name:     g.config.Name,
		Realtime: g.config.Realtime,
		Nodes:    make([]NodeSpec, len(g.nodes)),
	}
	for i := range g.nodes {
		node := &g.nodes[i]
		spec.Nodes[i] = bufferedNodeSpec(node)
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

func (g *BufferedGraph) Events() <-chan av.Event {
	return g.events
}

func (g *BufferedGraph) Close() error {
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

func (g *BufferedGraph) addNode(node bufferedNode) (int, error) {
	if node.name == "" {
		return 0, ErrUnknownNode
	}
	if _, ok := g.index[node.name]; ok {
		return 0, ErrNodeExists
	}
	index := len(g.nodes)
	node.policy = normalizeBufferedPolicy(node.policy)
	node.drop = newDropController(node.policy)
	node.emitter = bufferedEmitter{graph: g, from: index}
	g.index[node.name] = index
	g.nodes = append(g.nodes, node)
	return index, nil
}

func (g *BufferedGraph) nodePolicy(policy BufferPolicy) BufferPolicy {
	if policy.IsDirect() {
		return g.config.Buffer
	}
	return normalizeBufferedPolicy(policy)
}

func (g *BufferedGraph) openQueues() {
	for i := range g.nodes {
		node := &g.nodes[i]
		if node.kind == nodeSource {
			continue
		}
		node.queue = make(chan bufferedMessage, node.policy.Capacity)
		node.drop = newDropController(node.policy)
	}
}

func (g *BufferedGraph) closeQueues() {
	for i := range g.nodes {
		node := &g.nodes[i]
		if node.kind == nodeSource || node.queue == nil {
			continue
		}
		close(node.queue)
	}
}

func (g *BufferedGraph) emit(ctx context.Context, from int, msg *Message) error {
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
			if err := g.enqueue(ctx, route.to[j], msg); err != nil {
				return err
			}
		}
	}
	return nil
}

func (g *BufferedGraph) enqueue(ctx context.Context, to int, msg *Message) error {
	node := &g.nodes[to]
	var queued bufferedMessage
	if err := queued.bind(msg); err != nil {
		return err
	}

	node.queueMutex.Lock()
	defer node.queueMutex.Unlock()

	action := node.drop.decide(len(node.queue), msg)
	switch action {
	case bufferAdmit:
		g.pending.Add(1)
		if err := enqueueMessage(ctx, node.queue, queued); err != nil {
			g.pending.Done()
			return err
		}
		return nil
	case bufferDropIncoming:
		return nil
	case bufferDropOldest:
		select {
		case <-node.queue:
			g.pending.Done()
		default:
		}
		g.pending.Add(1)
		if err := enqueueMessage(ctx, node.queue, queued); err != nil {
			g.pending.Done()
			return err
		}
		return nil
	default:
		return ErrBackpressure
	}
}

func enqueueMessage(ctx context.Context, queue chan bufferedMessage, msg bufferedMessage) error {
	select {
	case queue <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return ErrBackpressure
	}
}

func (g *BufferedGraph) runNode(ctx context.Context, index int) error {
	node := &g.nodes[index]
	var first error
	for msg := range node.queue {
		if first == nil {
			if err := g.deliver(ctx, index, &msg.message); err != nil {
				first = err
			}
		}
		g.pending.Done()
	}
	return first
}

func (g *BufferedGraph) deliver(ctx context.Context, to int, msg *Message) error {
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

func (g *BufferedGraph) publishEvent(msg *Message) error {
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

func (m *bufferedMessage) bind(src *Message) error {
	m.Reset()
	if src == nil {
		return ErrNilMessage
	}
	m.message.Kind = src.Kind
	switch src.Kind {
	case MessagePacket:
		if src.Packet == nil {
			return nil
		}
		if !bufferSafe(src.Packet.Payload) {
			return ErrBufferedMessageUnsafe
		}
		m.packet = *src.Packet
		m.message.Packet = &m.packet
	case MessageFrame:
		if src.Frame == nil {
			return nil
		}
		if len(src.Frame.Planes) > len(m.planes) {
			return ErrMessageTooLarge
		}
		for i := range src.Frame.Planes {
			if !bufferSafe(src.Frame.Planes[i].Buffer) {
				return ErrBufferedMessageUnsafe
			}
		}
		m.frame = *src.Frame
		copy(m.planes[:], src.Frame.Planes)
		m.frame.Planes = m.planes[:len(src.Frame.Planes)]
		m.message.Frame = &m.frame
	case MessageEvent:
		if src.Event == nil {
			return nil
		}
		m.event = *src.Event
		m.message.Event = &m.event
	default:
		return nil
	}
	return nil
}

func (m *bufferedMessage) Reset() {
	m.message.Reset()
	m.packet.Reset()
	m.frame.Reset()
	m.event.Reset()
	for i := range m.planes {
		m.planes[i].Reset()
	}
}

func bufferSafe(buffer av.Buffer) bool {
	if len(buffer.Bytes) == 0 {
		return true
	}
	return buffer.Ownership == av.BufferImmutable
}

func bufferedNodeSpec(node *bufferedNode) NodeSpec {
	spec := NodeSpec{Name: node.name, Kind: directSpecKind(node.kind)}
	describer := bufferedNodeDescriber(node)
	if describer == nil {
		return spec
	}
	described := describer.DescribeNode()
	spec.Detail = described.Detail
	return spec
}

func bufferedNodeDescriber(node *bufferedNode) NodeDescriber {
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
