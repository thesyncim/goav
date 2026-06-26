package pipeline

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

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
	counters   *nodeCounters
	// queuedBytes tracks the total payload bytes of in-flight queued messages,
	// maintained only when policy.MaxBytes > 0: the producer adds before queuing
	// (under queueMutex), the lock-free worker subtracts in releaseSlot.
	queuedBytes atomic.Int64
	// removed is set (once) when the node is torn down by Remove, so the
	// lock-free worker can shed in-flight messages without taking g.mu.
	removed atomic.Bool
	// queueClosed guards the queue channel against the lock-free producer: it is
	// set under queueMutex before the queue is closed, so enqueue (which holds
	// queueMutex but no g.mu) never sends on a closed channel.
	queueClosed bool
	// paused, when set, makes the lock-free producer skip this node so a single
	// branch can be paused without touching the source or its siblings.
	paused atomic.Bool
}

// bufferedTopo is an immutable producer-side routing snapshot read lock-free by
// emit via bufferedRunner.topo (atomic.Pointer), rebuilt under g.mu on every
// topology/queue change. It holds stable *bufferedNode pointers (their
// queue/free/policy/counters are set under g.mu before the rebuild that
// publishes them) plus a deep copy of each node's active flag and routes, so a
// published snapshot is never mutated.
type bufferedTopo struct {
	nodes []bufferedTopoNode
}

type bufferedTopoNode struct {
	node   *bufferedNode
	active bool
	routes []directRoute
}

type bufferedEmitter struct {
	graph *bufferedRunner
	from  int
}

func (e *bufferedEmitter) Emit(ctx context.Context, msg *Message) error {
	return e.graph.emit(ctx, e.from, msg)
}

// EmitDelivery implements the optional DeliveryEmitter capability: Emit plus a
// per-message report of delivered vs deliberately-shed targets.
func (e *bufferedEmitter) EmitDelivery(ctx context.Context, msg *Message) (Delivery, error) {
	return e.graph.emitDelivery(ctx, e.from, msg)
}

type bufferedMessage struct {
	message       Message
	packet        av.Packet
	frame         av.Frame
	event         av.Event
	audio         av.AudioFrame
	video         av.VideoFrame
	planes        [bufferedMaxFramePlanes]av.Plane
	packetBacking []byte
	frameBacking  []byte
	// enqueuedAt stamps when the message entered the queue, set only when the
	// node's MaxLatency is non-zero, so the worker can shed stale messages.
	enqueuedAt time.Time
	// bytes is the payload size counted against the node's MaxBytes budget,
	// recorded when the slot is queued and subtracted from queuedBytes on release.
	// Nonzero only when MaxBytes > 0, so releaseSlot's subtract is a no-op by default.
	bytes int64
}

type bufferedRunner struct {
	config   GraphConfig
	mu       sync.RWMutex
	index    map[string]int
	nodes    []*bufferedNode
	sources  []int
	events   chan av.Event
	pending  sync.WaitGroup
	workers  sync.WaitGroup
	cold     coldStats
	runCtx   context.Context
	runErrs  chan<- error
	running  bool
	draining bool
	closed   bool
	// closing is the lock-free mirror of closed, set under g.mu at Close so the
	// lock-free worker path sheds messages once the graph is closing.
	closing atomic.Bool
	// topo is the immutable producer-side routing snapshot; emit reads it without
	// g.mu. nil means closed.
	topo atomic.Pointer[bufferedTopo]
}

// rebuildTopoLocked publishes a fresh producer-side routing snapshot. Caller must
// hold g.mu (write). Routes are deep-copied so in-place edits by Disconnect/Remove
// never touch an already-published snapshot.
func (g *bufferedRunner) rebuildTopoLocked() {
	if g.closed {
		g.topo.Store(nil)
		return
	}
	topo := &bufferedTopo{nodes: make([]bufferedTopoNode, len(g.nodes))}
	for i := range g.nodes {
		n := g.nodes[i]
		tn := &topo.nodes[i]
		tn.node = n
		tn.active = n.active
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
	index, err := g.addNode(&bufferedNode{name: source.Name(), kind: nodeSource, source: source, policy: g.nodePolicy(policy)})
	if err != nil {
		return "", err
	}
	g.sources = append(g.sources, index)
	g.rebuildTopoLocked()
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
	index, err := g.addNode(&bufferedNode{name: stage.Name(), kind: nodeStage, stage: stage, policy: g.nodePolicy(policy)})
	if err != nil {
		return "", err
	}
	if g.running {
		g.openQueue(g.nodes[index])
		g.startWorkerLocked(index)
	}
	g.rebuildTopoLocked()
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
	index, err := g.addNode(&bufferedNode{name: sink.Name(), kind: nodeSink, sink: sink, policy: g.nodePolicy(policy)})
	if err != nil {
		return "", err
	}
	if g.running {
		g.openQueue(g.nodes[index])
		g.startWorkerLocked(index)
	}
	g.rebuildTopoLocked()
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
	g.rebuildTopoLocked() // publish the snapshot with queues now open
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
	reportBufferedError(errs, g.closeQueuesLocked())
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
		node := g.nodes[i]
		if !node.active {
			continue
		}
		spec.Nodes = append(spec.Nodes, bufferedNodeSpec(node))
		for j := range node.routes {
			route := &node.routes[j]
			for k := range route.to {
				to := g.nodes[route.to[k]]
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

// SetNodePaused pauses or resumes delivery to a single node. While paused the
// lock-free producer skips it, so the source and sibling branches are unaffected.
func (g *bufferedRunner) SetNodePaused(ref NodeRef, paused bool) error {
	g.mu.RLock()
	defer g.mu.RUnlock()
	index, ok := g.index[ref.String()]
	if !ok || index < 0 || index >= len(g.nodes) || !g.nodes[index].active {
		return ErrUnknownNode
	}
	g.nodes[index].paused.Store(paused)
	return nil
}

func (g *bufferedRunner) Stats() GraphStats {
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

func (g *bufferedRunner) Close() error {
	return g.CloseContext(context.Background())
}

func (g *bufferedRunner) CloseContext(ctx context.Context) error {
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
	g.closing.Store(true)
	running := g.running
	var first error
	stuck := make(map[*bufferedNode]struct{})
	nodes := make([]*bufferedNode, 0, len(g.nodes))
	for i := range g.nodes {
		node := g.nodes[i]
		if !node.active {
			continue
		}
		if running && node.kind != nodeSource && node.queue != nil {
			if err := g.closeNodeQueueContext(ctx, "close", node); err != nil {
				if first == nil {
					first = err
				}
				stuck[node] = struct{}{}
				go forceCloseNodeQueue(node)
			}
		}
		node.active = false
		nodes = append(nodes, node)
	}
	g.rebuildTopoLocked() // g.closed is set, so this publishes nil
	g.mu.Unlock()

	for i := range nodes {
		node := nodes[i]
		if _, ok := stuck[node]; ok {
			continue
		}
		if running && node.kind != nodeSource {
			if err := waitCloseDoneContext(ctx, "close", node.name, node.done, g.config.CloseWaitTimeout); err != nil {
				if first == nil {
					first = err
				}
				stuck[node] = struct{}{}
			}
		}
	}
	for i := range nodes {
		if _, ok := stuck[nodes[i]]; ok {
			continue
		}
		err := closeBufferedNode(nodes[i])
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
	node := g.nodes[index]
	node.active = false
	node.removed.Store(true)
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
	closeNode := node
	if running && node.queue != nil {
		if err := g.closeNodeQueue("remove", node); err != nil {
			g.rebuildTopoLocked()
			g.mu.Unlock()
			go forceCloseNodeQueue(node)
			return err
		}
	}
	done := node.done
	g.rebuildTopoLocked()
	g.mu.Unlock()
	if running && done != nil {
		if err := waitCloseDone("remove", closeNode.name, done, g.config.CloseWaitTimeout); err != nil {
			return err
		}
	}
	return closeBufferedNode(closeNode)
}

// closeNodeQueue closes a node's queue exactly once, serialized with the
// lock-free producer via queueMutex so enqueue never sends on a closed channel.
func (g *bufferedRunner) closeNodeQueue(operation string, node *bufferedNode) error {
	return g.closeNodeQueueContext(context.Background(), operation, node)
}

func (g *bufferedRunner) closeNodeQueueContext(ctx context.Context, operation string, node *bufferedNode) error {
	if err := lockNodeQueueForCloseContext(ctx, operation, node, g.config.CloseWaitTimeout); err != nil {
		return err
	}
	defer node.queueMutex.Unlock()
	if node.queueClosed || node.queue == nil {
		return nil
	}
	node.queueClosed = true
	close(node.queue)
	return nil
}

func lockNodeQueueForCloseContext(ctx context.Context, operation string, node *bufferedNode, timeout time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if timeout <= 0 {
		for !node.queueMutex.TryLock() {
			if err := ctx.Err(); err != nil {
				return err
			}
			time.Sleep(100 * time.Microsecond)
		}
		return nil
	}
	deadline := time.Now().Add(timeout)
	for !node.queueMutex.TryLock() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if time.Now().After(deadline) {
			return &CloseWaitError{Operation: operation, Node: node.name, Pending: 1, Timeout: timeout}
		}
		time.Sleep(100 * time.Microsecond)
	}
	return nil
}

func forceCloseNodeQueue(node *bufferedNode) {
	node.queueMutex.Lock()
	defer node.queueMutex.Unlock()
	if node.queueClosed || node.queue == nil {
		return
	}
	node.queueClosed = true
	close(node.queue)
}

func (g *bufferedRunner) addNode(node *bufferedNode) (int, error) {
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
	node.counters = &nodeCounters{}
	g.index[node.name] = index
	g.nodes = append(g.nodes, node)
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
		node := g.nodes[i]
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
	node.queueClosed = false
}

func (g *bufferedRunner) startWorkerLocked(index int) {
	node := g.nodes[index]
	queue := node.queue
	done := make(chan struct{})
	node.done = done
	ctx := g.runCtx
	errs := g.runErrs
	g.workers.Add(1)
	go func() {
		defer g.workers.Done()
		defer close(done)
		reportBufferedError(errs, g.runNode(ctx, node, queue))
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

func (g *bufferedRunner) closeQueuesLocked() error {
	var first error
	for i := range g.nodes {
		node := g.nodes[i]
		if !node.active || node.kind == nodeSource || node.queue == nil {
			continue
		}
		if err := g.closeNodeQueue("drain", node); first == nil && err != nil {
			first = err
		}
	}
	return first
}

func (g *bufferedRunner) emit(ctx context.Context, from int, msg *Message) error {
	_, err := g.emitDelivery(ctx, from, msg)
	return err
}

// emitDelivery is emit with per-message delivery accounting: it additionally
// reports how many matching targets queued the message and how many shed it,
// powering the optional DeliveryEmitter capability.
func (g *bufferedRunner) emitDelivery(ctx context.Context, from int, msg *Message) (Delivery, error) {
	var delivery Delivery
	if msg == nil {
		return delivery, ErrNilMessage
	}
	if err := ctx.Err(); err != nil {
		return delivery, err
	}
	// Lock-free producer: read the routing snapshot and enqueue without g.mu, so a
	// (possibly blocking) send holds no topology lock and cannot deadlock a
	// concurrent mutation or a re-emitting stage.
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
	observeOut(fromNode.node.counters, msg, &g.cold)
	g.publishEvent(msg, fromNode.node.counters)

	// A real fanout (more than one matching target) isolates a failing target so
	// it cannot abort siblings or the source; a simple one-target chain keeps the
	// strict behavior (e.g. DropNever propagates ErrBackpressure to tear down).
	fanout := countTargets(topo, fromNode, msg) > 1

	for i := range fromNode.routes {
		route := &fromNode.routes[i]
		if !route.matches(msg) {
			continue
		}
		for j := range route.to {
			to := route.to[j]
			if to < 0 || to >= len(topo.nodes) || !topo.nodes[to].active {
				continue
			}
			target := topo.nodes[to].node
			delivered, err := g.enqueue(ctx, target, msg)
			if err != nil {
				// Independent fanout backpressure: a full target on a real fanout
				// drops for itself and keeps delivering to its siblings, so one slow
				// subscriber cannot tear down the source. Everything else —
				// cancellation, config/validation errors, and simple-chain
				// backpressure (the strict DropNever contract) — still propagates.
				if fanout && errors.Is(err, ErrBackpressure) {
					observeDrop(target.counters, target.policy.Drop, &g.cold)
					delivery.Shed++
				} else {
					return delivery, err
				}
			} else if delivered {
				delivery.Delivered++
			} else {
				delivery.Shed++
			}
		}
	}
	return delivery, nil
}

// countTargets counts the matching, active delivery targets for a message; it
// stops at two since only "one vs many" matters (simple chain vs fanout).
func countTargets(topo *bufferedTopo, fromNode *bufferedTopoNode, msg *Message) int {
	count := 0
	for i := range fromNode.routes {
		route := &fromNode.routes[i]
		if !route.matches(msg) {
			continue
		}
		for j := range route.to {
			to := route.to[j]
			if to >= 0 && to < len(topo.nodes) && topo.nodes[to].active {
				count++
				if count > 1 {
					return count
				}
			}
		}
	}
	return count
}

// enqueue queues msg on node and reports whether THIS message was delivered:
// (false, nil) is a deliberate shed (paused branch, torn-down queue, byte
// budget, dropping policy) — counted where applicable, silent to the producer
// on the plain Emit path, surfaced through Delivery on the EmitDelivery path.
func (g *bufferedRunner) enqueue(ctx context.Context, node *bufferedNode, msg *Message) (bool, error) {
	if node.paused.Load() {
		// Branch paused: skip this node without touching the source or siblings.
		return false, nil
	}
	if err := validateBufferedMessage(msg, node.policy); err != nil {
		return false, err
	}

	node.queueMutex.Lock()
	defer node.queueMutex.Unlock()

	if node.queueClosed {
		// Queue torn down (Remove/Close) after the snapshot was read; shed.
		return false, nil
	}

	if node.policy.MaxBytes > 0 && node.queuedBytes.Load()+messageBytes(msg) > node.policy.MaxBytes {
		// Byte budget exhausted: shed rather than queue past the bound.
		observeDrop(node.counters, DropOverflow, &g.cold)
		return false, nil
	}

	action := node.drop.decide(len(node.queue), msg)
	switch action {
	case bufferAdmit:
		if err := g.enqueueBound(ctx, node, msg); err != nil {
			return false, err
		}
		return true, nil
	case bufferBlock:
		if err := g.enqueueBlocking(ctx, node, msg); err != nil {
			return false, err
		}
		return true, nil
	case bufferDropIncoming:
		observeDrop(node.counters, node.policy.Drop, &g.cold)
		return false, nil
	case bufferDropOldest:
		select {
		case dropped := <-node.queue:
			node.releaseSlot(dropped)
			g.pending.Done()
			observeDrop(node.counters, node.policy.Drop, &g.cold)
		default:
		}
		if err := g.enqueueBound(ctx, node, msg); err != nil {
			return false, err
		}
		return true, nil
	default:
		return false, ErrBackpressure
	}
}

// enqueueBlocking implements true backpressure for DropBlock: it waits for a free
// slot and for queue space, so a full queue paces the producer instead of
// erroring. It holds node.queueMutex but no g.mu (FC-1), and the lock-free worker
// drains the queue, so the wait always makes progress; ctx cancellation is the
// only escape.
func (g *bufferedRunner) enqueueBlocking(ctx context.Context, node *bufferedNode, msg *Message) error {
	var slot *bufferedMessage
	select {
	case slot = <-node.free:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := slot.bind(msg, node.policy); err != nil {
		node.releaseSlot(slot)
		return err
	}
	g.pending.Add(1)
	node.accountQueuedBytes(slot, msg)
	select {
	case node.queue <- slot:
		return nil
	case <-ctx.Done():
		g.pending.Done()
		node.releaseSlot(slot)
		return ctx.Err()
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
	node.accountQueuedBytes(slot, msg)
	if err := enqueueMessage(ctx, node.queue, slot); err != nil {
		g.pending.Done()
		node.releaseSlot(slot)
		return err
	}
	return nil
}

// accountQueuedBytes records a slot's payload size against the node's MaxBytes
// budget before it enters the queue (so the lock-free worker's releaseSlot
// subtract is correctly paired). A no-op unless MaxBytes > 0.
func (n *bufferedNode) accountQueuedBytes(slot *bufferedMessage, msg *Message) {
	if n.policy.MaxBytes <= 0 {
		return
	}
	slot.bytes = messageBytes(msg)
	n.queuedBytes.Add(slot.bytes)
}

// messageBytes is the payload size of a message counted against MaxBytes: packet
// payload length plus frame plane lengths. Cold metadata is not counted.
func messageBytes(msg *Message) int64 {
	if msg == nil {
		return 0
	}
	var n int64
	if msg.Packet != nil {
		n += int64(len(msg.Packet.Payload.Bytes))
	}
	if msg.Frame != nil {
		for i := range msg.Frame.Planes {
			n += int64(len(msg.Frame.Planes[i].Buffer.Bytes))
		}
	}
	return n
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

func (g *bufferedRunner) runNode(ctx context.Context, node *bufferedNode, queue <-chan *bufferedMessage) error {
	var first error
	for msg := range queue {
		switch {
		case first != nil:
			// A prior delivery failed; drain without delivering.
		case g.staleByLatency(node, msg):
			observeDrop(node.counters, DropStale, &g.cold)
		default:
			if err := g.deliver(ctx, node, &msg.message); err != nil {
				first = err
			}
		}
		node.releaseSlot(msg)
		g.pending.Done()
	}
	return first
}

// staleByLatency reports whether a queued message has waited longer than the
// node's MaxLatency and should be shed instead of delivered late (realtime).
func (g *bufferedRunner) staleByLatency(node *bufferedNode, msg *bufferedMessage) bool {
	return node.policy.MaxLatency > 0 &&
		!msg.enqueuedAt.IsZero() &&
		time.Since(msg.enqueuedAt) > node.policy.MaxLatency
}

// deliver runs lock-free: the worker owns a stable *bufferedNode whose
// kind/stage/sink/emitter/counters/free are immutable after the queue opens, so
// no g.mu is taken per delivered message. Teardown is observed via two atomics:
// g.closing (graph Close) sheds with ErrClosed; node.removed (Remove) sheds
// silently — matching the prior locked behavior exactly.
func (g *bufferedRunner) deliver(ctx context.Context, node *bufferedNode, msg *Message) error {
	if g.closing.Load() {
		return ErrClosed
	}
	if node.removed.Load() {
		return nil
	}
	observeIn(node.counters, msg, &g.cold)
	switch node.kind {
	case nodeStage:
		return node.stage.Handle(ctx, msg, &node.emitter)
	case nodeSink:
		return node.sink.Handle(ctx, msg)
	default:
		return ErrInvalidLink
	}
}

func (g *bufferedRunner) publishEvent(msg *Message, counters *nodeCounters) {
	if msg.Kind != MessageEvent || msg.Event == nil {
		return
	}
	select {
	case g.events <- *msg.Event:
	default:
		observeDrop(counters, DropObserver, &g.cold)
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
	if slot.bytes != 0 {
		n.queuedBytes.Add(-slot.bytes)
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
	if policy.MaxLatency > 0 {
		m.enqueuedAt = time.Now()
	}
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
		if !bufferSafe(src.Packet.Payload, policy) {
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
		if src.Frame.Audio != nil {
			m.audio = *src.Frame.Audio
			m.frame.Audio = &m.audio
		}
		if src.Frame.Video != nil {
			m.video = *src.Frame.Video
			m.frame.Video = &m.video
		}
		offset := 0
		for i := range src.Frame.Planes {
			plane := src.Frame.Planes[i]
			if !bufferSafe(plane.Buffer, policy) {
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
	m.audio = av.AudioFrame{}
	m.video = av.VideoFrame{}
	for i := range m.planes {
		m.planes[i].Reset()
	}
	m.packet.Payload.Bytes = m.packetBacking[:0]
	m.frameBacking = frameBacking[:0]
	m.enqueuedAt = time.Time{}
	m.bytes = 0
}

// bufferSafe reports whether a payload may be queued by reference without a
// graph-owned copy: empty payloads always, and av.BufferImmutable payloads
// unless the policy demands defensive copies of everything (CopyAlways).
// Everything else is mutable from the graph's point of view and must be copied
// into the target node's slot backing — or refused — before queueing, so a
// consumer that mutates its delivered bytes can never reach bytes the producer
// or a sibling branch still sees.
func bufferSafe(buffer av.Buffer, policy BufferPolicy) bool {
	if len(buffer.Bytes) == 0 {
		return true
	}
	if policy.CopyAlways {
		return false
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
		if bufferSafe(msg.Packet.Payload, policy) {
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
			if bufferSafe(buffer, policy) {
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
