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
	queue      chan *bufferedMessage
	free       chan *bufferedMessage
	slots      []bufferedMessage
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
	message       Message
	packet        av.Packet
	frame         av.Frame
	event         av.Event
	planes        [bufferedMaxFramePlanes]av.Plane
	packetBacking []byte
	frameBacking  []byte
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

func (g *BufferedGraph) Connect(route Route) error {
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
		slotCount := node.policy.Capacity + 1
		node.queue = make(chan *bufferedMessage, node.policy.Capacity)
		node.free = make(chan *bufferedMessage, slotCount)
		node.slots = make([]bufferedMessage, slotCount)
		for i := range node.slots {
			node.slots[i].init(node.policy)
			node.free <- &node.slots[i]
		}
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
	if err := validateBufferedMessage(msg, node.policy); err != nil {
		return err
	}

	node.queueMutex.Lock()
	defer node.queueMutex.Unlock()

	action := node.drop.decide(len(node.queue), msg)
	switch action {
	case bufferAdmit:
		return g.enqueueBound(ctx, node, msg)
	case bufferDropIncoming:
		return nil
	case bufferDropOldest:
		select {
		case dropped := <-node.queue:
			node.releaseSlot(dropped)
			g.pending.Done()
		default:
		}
		return g.enqueueBound(ctx, node, msg)
	default:
		return ErrBackpressure
	}
}

func (g *BufferedGraph) enqueueBound(ctx context.Context, node *bufferedNode, msg *Message) error {
	slot, err := node.acquireSlot()
	if err != nil {
		return err
	}
	if err := slot.bind(msg, node.policy); err != nil {
		node.releaseSlot(slot)
		return err
	}
	g.pending.Add(1)
	if err := enqueueMessage(ctx, node.queue, slot); err != nil {
		g.pending.Done()
		node.releaseSlot(slot)
		return err
	}
	return nil
}

func enqueueMessage(ctx context.Context, queue chan *bufferedMessage, msg *bufferedMessage) error {
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
		node.releaseSlot(msg)
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

func (n *bufferedNode) acquireSlot() (*bufferedMessage, error) {
	select {
	case slot := <-n.free:
		return slot, nil
	default:
		return nil, ErrBackpressure
	}
}

func (n *bufferedNode) releaseSlot(slot *bufferedMessage) {
	if slot == nil {
		return
	}
	slot.Reset()
	n.free <- slot
}

func (m *bufferedMessage) init(policy BufferPolicy) {
	if policy.CopyPacketBytes > 0 && cap(m.packetBacking) < policy.CopyPacketBytes {
		m.packetBacking = make([]byte, policy.CopyPacketBytes)
	}
	if policy.CopyFrameBytes > 0 && cap(m.frameBacking) < policy.CopyFrameBytes {
		m.frameBacking = make([]byte, policy.CopyFrameBytes)
	}
	m.Reset()
}

func (m *bufferedMessage) bind(src *Message, policy BufferPolicy) error {
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
		packetBacking := m.packetBacking
		m.packet = *src.Packet
		if !bufferSafe(src.Packet.Payload) {
			if policy.CopyPacketBytes <= 0 {
				return ErrBufferedMessageUnsafe
			}
			if len(src.Packet.Payload.Bytes) > cap(packetBacking) {
				return ErrMessageTooLarge
			}
			m.packet.Payload.Bytes = packetBacking[:len(src.Packet.Payload.Bytes)]
			copy(m.packet.Payload.Bytes, src.Packet.Payload.Bytes)
			m.packet.Payload.Ownership = av.BufferBorrowed
			m.packet.Payload.Owner = nil
		}
		m.message.Packet = &m.packet
	case MessageFrame:
		if src.Frame == nil {
			return nil
		}
		if len(src.Frame.Planes) > len(m.planes) {
			return ErrMessageTooLarge
		}
		frameBacking := m.frameBacking
		m.frame = *src.Frame
		offset := 0
		for i := range src.Frame.Planes {
			plane := src.Frame.Planes[i]
			if !bufferSafe(plane.Buffer) {
				if policy.CopyFrameBytes <= 0 {
					return ErrBufferedMessageUnsafe
				}
				if len(plane.Buffer.Bytes) > cap(frameBacking)-offset {
					return ErrMessageTooLarge
				}
				dst := frameBacking[offset : offset+len(plane.Buffer.Bytes)]
				copy(dst, plane.Buffer.Bytes)
				plane.Buffer.Bytes = dst
				plane.Buffer.Ownership = av.BufferBorrowed
				plane.Buffer.Owner = nil
				offset += len(dst)
			}
			m.planes[i] = plane
		}
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
	frameBacking := m.frameBacking
	m.packet.Reset()
	m.frame.Reset()
	m.event.Reset()
	for i := range m.planes {
		m.planes[i].Reset()
	}
	m.packet.Payload.Bytes = m.packetBacking[:0]
	m.frameBacking = frameBacking[:0]
}

func bufferSafe(buffer av.Buffer) bool {
	if len(buffer.Bytes) == 0 {
		return true
	}
	return buffer.Ownership == av.BufferImmutable
}

func validateBufferedMessage(msg *Message, policy BufferPolicy) error {
	if msg == nil {
		return ErrNilMessage
	}
	switch msg.Kind {
	case MessagePacket:
		if msg.Packet == nil {
			return nil
		}
		if bufferSafe(msg.Packet.Payload) {
			return nil
		}
		if policy.CopyPacketBytes <= 0 {
			return ErrBufferedMessageUnsafe
		}
		if len(msg.Packet.Payload.Bytes) > policy.CopyPacketBytes {
			return ErrMessageTooLarge
		}
	case MessageFrame:
		if msg.Frame == nil {
			return nil
		}
		if len(msg.Frame.Planes) > bufferedMaxFramePlanes {
			return ErrMessageTooLarge
		}
		needed := 0
		for i := range msg.Frame.Planes {
			buffer := msg.Frame.Planes[i].Buffer
			if bufferSafe(buffer) {
				continue
			}
			if policy.CopyFrameBytes <= 0 {
				return ErrBufferedMessageUnsafe
			}
			needed += len(buffer.Bytes)
			if needed > policy.CopyFrameBytes {
				return ErrMessageTooLarge
			}
		}
	}
	return nil
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
