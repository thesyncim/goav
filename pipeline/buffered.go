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
	active     bool
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
	done       chan struct{}
}

type bufferedEmitter struct {
	graph *bufferedRunner
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

type bufferedRunner struct {
	config      GraphConfig
	mu          sync.RWMutex
	index       map[string]int
	nodes       []bufferedNode
	statsByNode []NodeStats
	sources     []int
	events      chan av.Event
	pending     sync.WaitGroup
	workers     sync.WaitGroup
	statsMu     sync.Mutex
	stats       GraphStats
	runCtx      context.Context
	runErrs     chan<- error
	running     bool
	draining    bool
	closed      bool
}

type bufferedSourceRun struct {
	source  Source
	emitter bufferedEmitter
}

func newBufferedRunner(config GraphConfig) (*bufferedRunner, error) {
	if config.Buffer.IsDirect() {
		return nil, ErrBufferedEdgesUnsupported
	}
	config.Buffer = normalizeBufferedPolicy(config.Buffer)
	eventCapacity := config.EventCapacity
	if eventCapacity < 1 {
		eventCapacity = 16
	}
	return &bufferedRunner{
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

func (g *bufferedRunner) AddSource(source Source, policy BufferPolicy) (NodeRef, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return "", ErrClosed
	}
	if g.running {
		return "", ErrDynamicGraphUnsupported
	}
	index, err := g.addNode(bufferedNode{name: source.Name(), kind: nodeSource, source: source, policy: g.nodePolicy(policy)})
	if err != nil {
		return "", err
	}
	g.sources = append(g.sources, index)
	return NodeRef(g.nodes[index].name), nil
}

func (g *bufferedRunner) AddStage(stage Stage, policy BufferPolicy) (NodeRef, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return "", ErrClosed
	}
	if g.running && g.draining {
		return "", ErrDynamicGraphUnsupported
	}
	index, err := g.addNode(bufferedNode{name: stage.Name(), kind: nodeStage, stage: stage, policy: g.nodePolicy(policy)})
	if err != nil {
		return "", err
	}
	if g.running {
		g.openQueue(&g.nodes[index])
		g.startWorkerLocked(index)
	}
	return NodeRef(g.nodes[index].name), nil
}

func (g *bufferedRunner) AddSink(sink Sink, policy BufferPolicy) (NodeRef, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return "", ErrClosed
	}
	if g.running && g.draining {
		return "", ErrDynamicGraphUnsupported
	}
	index, err := g.addNode(bufferedNode{name: sink.Name(), kind: nodeSink, sink: sink, policy: g.nodePolicy(policy)})
	if err != nil {
		return "", err
	}
	if g.running {
		g.openQueue(&g.nodes[index])
		g.startWorkerLocked(index)
	}
	return NodeRef(g.nodes[index].name), nil
}

func (g *bufferedRunner) Connect(route Route) error {
	if len(route.To) == 0 {
		return ErrInvalidLink
	}
	policy, err := normalizeRoutePolicy(route.Policy)
	if err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.running && g.draining {
		return ErrDynamicGraphUnsupported
	}
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

func (g *bufferedRunner) Disconnect(route Route) error {
	if len(route.To) == 0 {
		return ErrInvalidLink
	}
	policy, err := normalizeRoutePolicy(route.Policy)
	if err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.running && g.draining {
		return ErrDynamicGraphUnsupported
	}
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

func (g *bufferedRunner) Run(ctx context.Context) error {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		g.mu.Unlock()
		return err
	}

	errs := make(chan error, len(g.nodes)+len(g.sources)+16)
	g.openQueuesLocked()
	g.running = true
	g.draining = false
	g.runCtx = ctx
	g.runErrs = errs
	g.workers = sync.WaitGroup{}
	for i := range g.nodes {
		if !g.nodes[i].active || g.nodes[i].kind == nodeSource {
			continue
		}
		g.startWorkerLocked(i)
	}
	sourceRuns := make([]bufferedSourceRun, 0, len(g.sources))
	for i := range g.sources {
		index := g.sources[i]
		if !g.nodes[index].active {
			continue
		}
		sourceRuns = append(sourceRuns, bufferedSourceRun{
			source:  g.nodes[index].source,
			emitter: g.nodes[index].emitter,
		})
	}
	g.mu.Unlock()

	var sources sync.WaitGroup
	for i := range sourceRuns {
		source := sourceRuns[i].source
		emitter := sourceRuns[i].emitter
		sources.Add(1)
		go func() {
			defer sources.Done()
			reportBufferedError(errs, source.Start(ctx, &emitter))
		}()
	}

	sources.Wait()
	g.pending.Wait()
	g.mu.Lock()
	g.draining = true
	g.closeQueuesLocked()
	g.mu.Unlock()
	g.workers.Wait()
	g.mu.Lock()
	g.running = false
	g.draining = false
	g.runCtx = nil
	g.runErrs = nil
	g.mu.Unlock()

	select {
	case err := <-errs:
		return err
	default:
		return nil
	}
}

func (g *bufferedRunner) Spec() Spec {
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
		spec.Nodes = append(spec.Nodes, bufferedNodeSpec(node))
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

func (g *bufferedRunner) Events() <-chan av.Event {
	return g.events
}

func (g *bufferedRunner) Stats() GraphStats {
	g.mu.RLock()
	defer g.mu.RUnlock()
	names := make([]string, len(g.nodes))
	for i := range g.nodes {
		names[i] = g.nodes[i].name
	}

	g.statsMu.Lock()
	defer g.statsMu.Unlock()
	return cloneGraphStatsWithNodes(g.stats, names, g.statsByNode)
}

func (g *bufferedRunner) Close() error {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return nil
	}
	g.closed = true
	running := g.running
	nodes := make([]bufferedNode, 0, len(g.nodes))
	dones := make([]chan struct{}, 0, len(g.nodes))
	for i := range g.nodes {
		node := &g.nodes[i]
		if !node.active {
			continue
		}
		if running && node.kind != nodeSource && node.queue != nil {
			close(node.queue)
			if node.done != nil {
				dones = append(dones, node.done)
			}
		}
		node.active = false
		nodes = append(nodes, *node)
	}
	g.mu.Unlock()

	for i := range dones {
		<-dones[i]
	}
	var first error
	for i := range nodes {
		err := closeBufferedNode(&nodes[i])
		if first == nil && err != nil {
			first = err
		}
	}
	close(g.events)
	return first
}

func (g *bufferedRunner) Remove(ref NodeRef) error {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return ErrClosed
	}
	running := g.running
	if running && g.draining {
		g.mu.Unlock()
		return ErrDynamicGraphUnsupported
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
	if running && node.queue != nil {
		close(node.queue)
	}
	done := node.done
	g.mu.Unlock()
	if running && done != nil {
		<-done
	}
	return closeBufferedNode(&closeNode)
}

func (g *bufferedRunner) addNode(node bufferedNode) (int, error) {
	if node.name == "" {
		return 0, ErrUnknownNode
	}
	if _, ok := g.index[node.name]; ok {
		return 0, ErrNodeExists
	}
	index := len(g.nodes)
	node.active = true
	node.policy = normalizeBufferedPolicy(node.policy)
	node.drop = newDropController(node.policy)
	node.emitter = bufferedEmitter{graph: g, from: index}
	g.index[node.name] = index
	g.nodes = append(g.nodes, node)
	g.statsByNode = append(g.statsByNode, NodeStats{})
	return index, nil
}

func (g *bufferedRunner) nodePolicy(policy BufferPolicy) BufferPolicy {
	if policy.IsDirect() {
		return g.config.Buffer
	}
	return normalizeBufferedPolicy(policy)
}

func (g *bufferedRunner) openQueuesLocked() {
	for i := range g.nodes {
		node := &g.nodes[i]
		if !node.active || node.kind == nodeSource {
			continue
		}
		g.openQueue(node)
	}
}

func (g *bufferedRunner) openQueue(node *bufferedNode) {
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

func (g *bufferedRunner) startWorkerLocked(index int) {
	queue := g.nodes[index].queue
	done := make(chan struct{})
	g.nodes[index].done = done
	ctx := g.runCtx
	errs := g.runErrs
	g.workers.Add(1)
	go func() {
		defer g.workers.Done()
		defer close(done)
		reportBufferedError(errs, g.runNode(ctx, index, queue))
	}()
}

func reportBufferedError(errs chan<- error, err error) {
	if err == nil {
		return
	}
	select {
	case errs <- err:
	default:
	}
}

func (g *bufferedRunner) closeQueuesLocked() {
	for i := range g.nodes {
		node := &g.nodes[i]
		if !node.active || node.kind == nodeSource || node.queue == nil {
			continue
		}
		close(node.queue)
	}
}

func (g *bufferedRunner) emit(ctx context.Context, from int, msg *Message) error {
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
	fromNode := &g.nodes[from]
	if !fromNode.active {
		g.mu.RUnlock()
		return nil
	}
	g.observeMessage(from, msg)
	if err := g.publishEvent(msg); err != nil {
		g.mu.RUnlock()
		return err
	}

	routes := fromNode.routes
	for i := range routes {
		route := &routes[i]
		if !route.matches(msg) {
			continue
		}
		for j := range route.to {
			to := route.to[j]
			if to < 0 || to >= len(g.nodes) || !g.nodes[to].active {
				continue
			}
			if err := g.enqueue(ctx, to, msg); err != nil {
				g.mu.RUnlock()
				return err
			}
		}
	}
	g.mu.RUnlock()
	return nil
}

func (g *bufferedRunner) enqueue(ctx context.Context, to int, msg *Message) error {
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
		g.observeDrop(to, node.policy.Drop)
		return nil
	case bufferDropOldest:
		select {
		case dropped := <-node.queue:
			node.releaseSlot(dropped)
			g.pending.Done()
			g.observeDrop(to, node.policy.Drop)
		default:
		}
		return g.enqueueBound(ctx, node, msg)
	default:
		return ErrBackpressure
	}
}

func (g *bufferedRunner) enqueueBound(ctx context.Context, node *bufferedNode, msg *Message) error {
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

func (g *bufferedRunner) runNode(ctx context.Context, index int, queue <-chan *bufferedMessage) error {
	var first error
	for msg := range queue {
		if first == nil {
			if err := g.deliver(ctx, index, &msg.message); err != nil {
				first = err
			}
		}
		g.releaseSlot(index, msg)
		g.pending.Done()
	}
	return first
}

func (g *bufferedRunner) deliver(ctx context.Context, to int, msg *Message) error {
	g.mu.RLock()
	if g.closed {
		g.mu.RUnlock()
		return ErrClosed
	}
	if to < 0 || to >= len(g.nodes) {
		g.mu.RUnlock()
		return ErrUnknownNode
	}
	if !g.nodes[to].active {
		g.mu.RUnlock()
		return nil
	}
	kind := g.nodes[to].kind
	stage := g.nodes[to].stage
	sink := g.nodes[to].sink
	emitter := g.nodes[to].emitter
	g.mu.RUnlock()
	g.observeDelivered(to, msg)
	switch kind {
	case nodeStage:
		return stage.Handle(ctx, msg, &emitter)
	case nodeSink:
		return sink.Handle(ctx, msg)
	default:
		return ErrInvalidLink
	}
}

func (g *bufferedRunner) releaseSlot(index int, slot *bufferedMessage) {
	if slot == nil {
		return
	}
	g.mu.RLock()
	var free chan *bufferedMessage
	if index >= 0 && index < len(g.nodes) {
		free = g.nodes[index].free
	}
	g.mu.RUnlock()
	if free == nil {
		slot.Reset()
		return
	}
	slot.Reset()
	free <- slot
}

func (g *bufferedRunner) observeMessage(node int, msg *Message) {
	g.statsMu.Lock()
	defer g.statsMu.Unlock()
	g.stats.observeMessage(msg)
	if node >= 0 && node < len(g.statsByNode) {
		g.statsByNode[node].observeOut(msg)
	}
}

func (g *bufferedRunner) observeDrop(node int, policy DropPolicy) {
	g.statsMu.Lock()
	defer g.statsMu.Unlock()
	g.stats.observeDrop(policy)
	if node >= 0 && node < len(g.statsByNode) {
		g.statsByNode[node].observeDrop(policy)
	}
}

func (g *bufferedRunner) observeDelivered(node int, msg *Message) {
	g.statsMu.Lock()
	defer g.statsMu.Unlock()
	g.stats.Delivered++
	if node >= 0 && node < len(g.statsByNode) {
		g.statsByNode[node].observeIn(msg)
	}
}

func (g *bufferedRunner) publishEvent(msg *Message) error {
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

func closeBufferedNode(node *bufferedNode) error {
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
