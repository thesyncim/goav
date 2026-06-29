package pipeline

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/goav/av"
)

// Graph and flow-control sentinels, matched with errors.Is.
var (
	// ErrBackpressure reports a strict (non-dropping) queue that is full: the
	// message was not delivered and a pacing producer should slow down.
	ErrBackpressure = errors.New("pipeline: backpressure")
	// ErrBufferedMessageUnsafe reports a mutable payload a buffered edge
	// refused to share or could not copy within its byte bounds.
	ErrBufferedMessageUnsafe = errors.New("pipeline: buffered message unsafe")
	// ErrBufferedEdgesUnsupported reports a buffered route on a graph
	// implementation that only executes direct edges.
	ErrBufferedEdgesUnsupported = errors.New("pipeline: buffered edges unsupported by direct graph")
	// ErrClosed reports an emit or control into a graph that has shut down.
	ErrClosed = errors.New("pipeline: closed")
	// ErrDynamicGraphUnsupported reports a live mutation (connect, disconnect,
	// remove while running) on a graph that cannot apply it.
	ErrDynamicGraphUnsupported = errors.New("pipeline: dynamic graph unsupported")
	// ErrInvalidLink reports a route whose endpoints cannot be wired (a sink
	// as a route source, a source as a target).
	ErrInvalidLink = errors.New("pipeline: invalid link")
	// ErrMessageTooLarge reports a payload exceeding the configured copy
	// bounds of a buffered edge.
	ErrMessageTooLarge = errors.New("pipeline: message too large")
	// ErrNilMessage reports a nil message handed to Emit.
	ErrNilMessage = errors.New("pipeline: nil message")
	// ErrNodeExists reports an added node whose name is already taken.
	ErrNodeExists = errors.New("pipeline: node exists")
	// ErrUnsupportedRoute reports a route policy the graph cannot apply.
	ErrUnsupportedRoute = errors.New("pipeline: unsupported route")
	// ErrUnknownNode reports a route or control naming a node the graph does
	// not contain.
	ErrUnknownNode = errors.New("pipeline: unknown node")
)

type nodeKind uint8

const (
	nodeSource nodeKind = iota + 1
	nodeStage
	nodeSink
)

type directNode struct {
	name     string
	kind     nodeKind
	active   bool
	source   Source
	stage    Stage
	sink     Sink
	routes   []directRoute
	emitter  *directEmitter
	counters *nodeCounters
	paused   *atomic.Bool
	handleMu *sync.Mutex
	// gate counts in-flight deliveries to this node and carries the closing
	// tombstone bit, so a live Remove can wait out deliveries that loaded an
	// older routing snapshot before it closes the node. One atomic add/sub per
	// delivery — the same budget class as the per-node counters.
	gate *atomic.Int64
}

// nodeClosingBit marks a node's delivery gate as closing: deliveries that
// arrive after the bit is set shed themselves, and Remove waits until every
// delivery that arrived before it has drained.
const nodeClosingBit = int64(1) << 62

type directRoute struct {
	to     []int
	policy RoutePolicy
	label  string
}

// directTopo is an immutable routing snapshot read lock-free on the hot path via
// directRunner.topo (atomic.Pointer). It is rebuilt under g.mu on every topology
// mutation and atomically swapped, so emit/deliver take no lock, copy no node,
// and build no per-message destination slice. A published directTopo (and its
// route slices) is never mutated, so concurrent readers stay race-free.
type directTopo struct {
	nodes []directTopoNode
}

type directTopoNode struct {
	active   bool
	kind     nodeKind
	stage    Stage
	sink     Sink
	emitter  *directEmitter
	counters *nodeCounters
	paused   *atomic.Bool
	handleMu *sync.Mutex
	gate     *atomic.Int64
	routes   []directRoute
}

type directEmitter struct {
	graph *directRunner
	from  int
}

type directSourceRun struct {
	source  Source
	emitter *directEmitter
}

func (e *directEmitter) Emit(ctx context.Context, msg *Message) error {
	return e.graph.emit(ctx, e.from, msg)
}

// EmitDelivery implements the optional DeliveryEmitter capability: Emit plus a
// per-message report of delivered vs deliberately-shed targets.
func (e *directEmitter) EmitDelivery(ctx context.Context, msg *Message) (Delivery, error) {
	return e.graph.emitDelivery(ctx, e.from, msg)
}

// NewGraph creates the single public pipeline graph. Direct execution is used
// for direct buffer policy; bounded buffered execution is selected otherwise.
func NewGraph(config GraphConfig) (Graph, error) {
	config.CloseWaitTimeout = normalizeCloseWaitTimeout(config.CloseWaitTimeout)
	if !config.Buffer.IsDirect() {
		return newBufferedRunner(config)
	}
	return newDirectRunner(config)
}

type directRunner struct {
	config       GraphConfig
	mu           sync.RWMutex
	index        map[string]int
	nodes        []directNode
	sources      []int
	topo         atomic.Pointer[directTopo]
	events       chan av.Event
	eventsMu     sync.Mutex
	eventsClosed bool
	cold         coldStats
	closed       bool
}

// rebuildTopoLocked publishes a fresh immutable routing snapshot from the current
// node set. Caller must hold g.mu (write). Route target slices are deep-copied so
// in-place edits by Disconnect/Remove never touch an already-published snapshot.
func (g *directRunner) rebuildTopoLocked() {
	if g.closed {
		g.topo.Store(nil)
		return
	}
	topo := &directTopo{nodes: make([]directTopoNode, len(g.nodes))}
	for i := range g.nodes {
		n := &g.nodes[i]
		tn := &topo.nodes[i]
		tn.active = n.active
		tn.kind = n.kind
		tn.stage = n.stage
		tn.sink = n.sink
		tn.emitter = n.emitter
		tn.counters = n.counters
		tn.paused = n.paused
		tn.handleMu = n.handleMu
		tn.gate = n.gate
		if len(n.routes) == 0 {
			continue
		}
		tn.routes = make([]directRoute, len(n.routes))
		for j := range n.routes {
			tn.routes[j] = n.routes[j]
			if len(n.routes[j].to) > 0 {
				to := make([]int, len(n.routes[j].to))
				copy(to, n.routes[j].to)
				tn.routes[j].to = to
			}
		}
	}
	g.topo.Store(topo)
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
	if g.closed {
		return "", ErrClosed
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
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return "", ErrClosed
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
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return "", ErrClosed
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
	g.mu.Lock()
	defer g.mu.Unlock()
	from, ok := g.index[route.From]
	if !ok || !g.nodes[from].active {
		return ErrUnknownNode
	}
	if g.nodes[from].kind == nodeSink {
		return ErrInvalidLink
	}

	destinations := make([]int, 0, len(route.To))
	for i := range route.To {
		to, ok := g.index[route.To[i]]
		if !ok || !g.nodes[to].active {
			return ErrUnknownNode
		}
		if g.nodes[to].kind == nodeSource {
			return ErrInvalidLink
		}
		destinations = append(destinations, to)
	}
	g.nodes[from].routes = append(g.nodes[from].routes, directRoute{
		to:     destinations,
		policy: policy,
		label:  route.Label,
	})
	g.rebuildTopoLocked()
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
	destinations := make(map[int]struct{}, len(route.To))
	for i := range route.To {
		to, ok := g.index[route.To[i]]
		if !ok {
			return ErrUnknownNode
		}
		destinations[to] = struct{}{}
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
			if _, ok := destinations[existing.to[j]]; ok {
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
	g.rebuildTopoLocked()
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
	var sourceStack [8]directSourceRun
	sources := sourceStack[:0]
	for i := range g.sources {
		index := g.sources[i]
		if !g.nodes[index].active {
			continue
		}
		if len(sources) < cap(sources) {
			sources = append(sources, directSourceRun{
				source:  g.nodes[index].source,
				emitter: g.nodes[index].emitter,
			})
			continue
		}
		sources = append(sources, directSourceRun{
			source:  g.nodes[index].source,
			emitter: g.nodes[index].emitter,
		})
	}
	g.mu.RUnlock()

	switch len(sources) {
	case 0:
		return nil
	case 1:
		run := sources[0]
		return run.source.Start(ctx, run.emitter)
	}

	// Run every source under a shared cancelable context so the first fatal
	// error cancels its siblings instead of leaving them running until they end
	// on their own. The triggering error is captured once and returned; siblings
	// that observe the cancellation and return context.Canceled do not overwrite
	// it. We still wait for every source goroutine before returning so no source
	// outlives Run.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		once     sync.Once
		firstErr error
		runs     sync.WaitGroup
	)
	for i := range sources {
		run := sources[i]
		runs.Add(1)
		go func() {
			defer runs.Done()
			if err := run.source.Start(runCtx, run.emitter); err != nil {
				once.Do(func() {
					firstErr = err
					cancel()
				})
			}
		}()
	}
	runs.Wait()
	return firstErr
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
	g.mu.RLock()
	defer g.mu.RUnlock()
	names := make([]string, len(g.nodes))
	nodes := make([]*nodeCounters, len(g.nodes))
	reported := make([]uint64, len(g.nodes))
	for i := range g.nodes {
		names[i] = g.nodes[i].name
		nodes[i] = g.nodes[i].counters
		reported[i] = reportedDrops(g.nodes[i].source, g.nodes[i].stage, g.nodes[i].sink)
	}
	return snapshotStats(&g.cold, names, nodes, reported)
}

// SetNodePaused pauses or resumes delivery to a single node. The lock-free
// hot path reads the flag from the routing snapshot, so pausing one branch leaves
// the source and sibling branches untouched.
func (g *directRunner) SetNodePaused(ref NodeRef, paused bool) error {
	g.mu.RLock()
	defer g.mu.RUnlock()
	index, ok := g.index[ref.String()]
	if !ok || index < 0 || index >= len(g.nodes) || !g.nodes[index].active || g.nodes[index].paused == nil {
		return ErrUnknownNode
	}
	g.nodes[index].paused.Store(paused)
	return nil
}

func (g *directRunner) Close() error {
	return g.CloseContext(context.Background())
}

func (g *directRunner) CloseContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return nil
	}
	g.closed = true
	g.topo.Store(nil)
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

	// New emits stop at the nil routing snapshot, but a delivery already in
	// flight loaded the previous one: drain every node's gate before closing,
	// so Close never runs under a concurrent Handle — the same contract a
	// live Remove keeps.
	var first error
	stuck := make(map[string]struct{})
	for i := range nodes {
		if err := waitNodeGateDrainedContext(ctx, "close", nodes[i].name, nodes[i].gate, g.config.CloseWaitTimeout); err != nil {
			if first == nil {
				first = err
			}
			stuck[nodes[i].name] = struct{}{}
		}
	}
	for i := range nodes {
		if _, ok := stuck[nodes[i].name]; ok {
			continue
		}
		err := closeDirectNode(&nodes[i])
		if first == nil && err != nil {
			first = err
		}
	}
	// Close events under eventsMu so a concurrent lock-free emit cannot send on
	// the channel after it is closed.
	g.eventsMu.Lock()
	g.eventsClosed = true
	close(g.events)
	g.eventsMu.Unlock()
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
	g.rebuildTopoLocked()
	closeNode := *node
	g.mu.Unlock()
	if err := waitNodeGateDrained("remove", closeNode.name, closeNode.gate, g.config.CloseWaitTimeout); err != nil {
		return err
	}
	return closeDirectNode(&closeNode)
}

// waitNodeGateDrained tombstones a removed node's delivery gate and waits for
// every in-flight delivery to finish, so the node is never closed under a
// concurrent Handle. New deliveries (from emits holding an older routing
// snapshot) see the closing bit and shed themselves. This is the cold removal
// path; in-flight deliveries are bounded by one downstream chain.
func waitNodeGateDrained(operation string, node string, gate *atomic.Int64, timeout time.Duration) error {
	return waitNodeGateDrainedContext(context.Background(), operation, node, gate, timeout)
}

func waitNodeGateDrainedContext(ctx context.Context, operation string, node string, gate *atomic.Int64, timeout time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if gate == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	gate.Add(nodeClosingBit)
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	for spins := 0; gate.Load() != nodeClosingBit; spins++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			pending := gate.Load() &^ nodeClosingBit
			return &CloseWaitError{Operation: operation, Node: node, Pending: pending, Timeout: timeout}
		}
		if spins < 100 {
			runtime.Gosched()
			continue
		}
		time.Sleep(100 * time.Microsecond)
	}
	return nil
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
	node.counters = &nodeCounters{}
	node.paused = &atomic.Bool{}
	node.handleMu = &sync.Mutex{}
	node.gate = &atomic.Int64{}
	g.index[node.name] = index
	g.nodes = append(g.nodes, node)
	g.rebuildTopoLocked()
	return index, nil
}

func (g *directRunner) emit(ctx context.Context, from int, msg *Message) error {
	_, err := g.emitDelivery(ctx, from, msg)
	return err
}

func (g *directRunner) emitDelivery(ctx context.Context, from int, msg *Message) (Delivery, error) {
	var delivery Delivery
	if msg == nil {
		return delivery, ErrNilMessage
	}
	if err := ctx.Err(); err != nil {
		return delivery, err
	}
	// Lock-free hot path: read an immutable routing snapshot, deliver inline. No
	// mutex, no per-node copy, and no per-message destination slice (so wide
	// fanout stays allocation-free).
	topo := g.topo.Load()
	if topo == nil {
		return delivery, ErrClosed
	}
	if from < 0 || from >= len(topo.nodes) {
		return delivery, ErrUnknownNode
	}
	fromNode := &topo.nodes[from]
	if !fromNode.active {
		return delivery, nil
	}
	observeOut(fromNode.counters, msg, &g.cold)
	g.publishEvent(msg, fromNode.counters)
	for i := range fromNode.routes {
		route := &fromNode.routes[i]
		if !route.matches(msg) {
			continue
		}
		for j := range route.to {
			to := route.to[j]
			if to < 0 || to >= len(topo.nodes) {
				continue
			}
			dst := &topo.nodes[to]
			if !dst.active {
				continue
			}
			delivered, err := g.deliverTopo(ctx, dst, msg)
			if err != nil {
				return delivery, err
			}
			if delivered {
				delivery.Delivered++
			} else {
				delivery.Shed++
			}
		}
	}
	return delivery, nil
}

func (g *directRunner) deliverTopo(ctx context.Context, dst *directTopoNode, msg *Message) (bool, error) {
	if dst.paused != nil && dst.paused.Load() {
		// Branch paused: skip this node; the source and siblings keep flowing.
		return false, nil
	}
	if dst.gate != nil {
		// Register this delivery before checking the tombstone: Remove sets
		// the closing bit and then waits for the gate to drain, so a delivery
		// that registered first is waited out and one that arrives after the
		// bit sheds itself — Close never runs under an in-flight Handle.
		if dst.gate.Add(1)&nodeClosingBit != 0 {
			dst.gate.Add(-1)
			return false, nil
		}
		defer dst.gate.Add(-1)
	}
	if dst.handleMu != nil {
		dst.handleMu.Lock()
		defer dst.handleMu.Unlock()
	}
	observeIn(dst.counters, msg, &g.cold)
	switch dst.kind {
	case nodeStage:
		if err := dst.stage.Handle(ctx, msg, dst.emitter); err != nil {
			return false, err
		}
		return true, nil
	case nodeSink:
		if err := dst.sink.Handle(ctx, msg); err != nil {
			return false, err
		}
		return true, nil
	default:
		return false, ErrInvalidLink
	}
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

func (g *directRunner) publishEvent(msg *Message, counters *nodeCounters) {
	if msg.Kind != MessageEvent || msg.Event == nil {
		return
	}
	g.eventsMu.Lock()
	defer g.eventsMu.Unlock()
	if g.eventsClosed {
		return
	}
	select {
	case g.events <- *msg.Event:
	default:
		observeDrop(counters, DropObserver, &g.cold)
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
