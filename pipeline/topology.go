package pipeline

import (
	"cmp"
	"context"
	"errors"
	"sync"
	"sync/atomic"

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

// NewGraph creates the single public pipeline graph. Direct execution is used
// for direct buffer policy; bounded buffered execution is selected otherwise.
func NewGraph(config GraphConfig) (Graph, error) {
	config.CloseWaitTimeout = normalizeCloseWaitTimeout(config.CloseWaitTimeout)
	if !config.Buffer.IsDirect() {
		return newBufferedRunner(config)
	}
	return newDirectRunner(config)
}

type nodeKind uint8

const (
	nodeSource nodeKind = iota + 1
	nodeStage
	nodeSink
)

// nodeBase is the part of a node both runners share: identity, the node
// itself, its routes, and the per-node state the topology reads. A runner's
// node type embeds it and adds how messages reach the node.
type nodeBase struct {
	name   string
	kind   nodeKind
	source Source
	stage  Stage
	admit  Admitter // stage's Admitter capability, resolved once at add
	sink   Sink
	// active and routes change under topology.mu only; the data plane reads
	// the copies in the routing snapshot.
	active   bool
	routes   []nodeRoute
	counters *nodeCounters
	// paused, when set, makes the producer skip this node, so a single branch
	// can be paused without touching the source or its siblings.
	paused atomic.Bool
	// sawMedia is set (once) when a sink node receives its first packet or
	// frame; the readiness signal counts sinks out through it. The delivery
	// path pays one atomic load per media message, nothing more.
	sawMedia atomic.Bool
}

// graphNode is a runner's node type: a pointer to a struct embedding nodeBase.
type graphNode interface {
	base() *nodeBase
}

type nodeRoute struct {
	to     []int
	policy RoutePolicy
	label  string
}

// snapshot is an immutable routing snapshot read lock-free on the hot path
// via topology.snap (atomic.Pointer). It is rebuilt under topology.mu on every
// topology mutation and atomically swapped, so emit takes no lock and builds
// no per-message destination slice. It holds stable node pointers plus a copy
// of each node's active flag and routes; a published snapshot (and its route
// slices) is never mutated, so concurrent readers stay race-free.
type snapshot[P graphNode] struct {
	nodes []snapshotNode[P]
}

type snapshotNode[P graphNode] struct {
	node   P
	active bool
	routes []nodeRoute
}

// strategy is what a runner adds to the shared topology: how a message
// reaches a node. The topology calls it on cold paths only.
type strategy[P graphNode] interface {
	// newNode makes an empty node of the runner's type for policy, before
	// topology.mu is taken; the topology fills in its nodeBase.
	newNode(policy BufferPolicy) (P, error)
	// mutable refuses a topology change the runner cannot apply now; source
	// reports whether the change adds a source. Called under topology.mu.
	mutable(source bool) error
	// attachLocked wires a just-added node at index into the runner, before
	// the snapshot that publishes it. Called under topology.mu.
	attachLocked(n P, index int)
	// stopLocked starts tearing n's delivery down as Close begins, and
	// reports whether Close must wait n out (awaitStopped). An error leaves n
	// stuck: it is reported and never closed. Called under topology.mu.
	stopLocked(ctx context.Context, n P) (await bool, err error)
	// awaitStopped waits, outside topology.mu, until no delivery into n is
	// in flight, so Close never runs under a concurrent Handle.
	awaitStopped(ctx context.Context, n P) error
}

// topology is the graph both runners share: the nodes, their routes and the
// lock-free routing snapshot built from them, spec, stats, readiness, the
// observer event stream, the run's error contract, and shutdown. A runner
// embeds it and supplies the delivery strategy: the direct runner delivers
// inline on the producer's goroutine, the buffered runner through per-node
// queues and workers.
type topology[P graphNode] struct {
	config   GraphConfig
	strategy strategy[P]
	// fail records the run's first error (run_failure.go): a node failure is
	// what Run returns even when the source swallows the Emit error.
	fail runFailure
	// ready is the once-per-run av.EventTaskReady countdown (readiness.go).
	ready   readiness
	mu      sync.RWMutex
	index   map[string]int
	nodes   []P
	sources []int
	closed  bool
	// snap is the routing snapshot the data plane reads without mu; nil once
	// the graph is closed.
	snap   atomic.Pointer[snapshot[P]]
	events chan av.Event
	// eventsMu guards the event channel close against the lock-free
	// publishers, so a concurrent emit cannot send on a closed channel. The
	// data plane takes it only for an observer send.
	eventsMu     sync.Mutex
	eventsClosed bool
	// cold stays last: a runner's per-message counters follow it, and its
	// size keeps them off the cache lines the data plane reads above.
	cold coldStats
}

func (t *topology[P]) init(config GraphConfig, s strategy[P]) {
	eventCapacity := config.EventCapacity
	if eventCapacity < 1 {
		eventCapacity = defaultEventCapacity
	}
	t.config = config
	t.strategy = s
	t.index = make(map[string]int)
	t.events = make(chan av.Event, eventCapacity)
}

// rebuildLocked publishes a fresh immutable routing snapshot from the current
// node set. Caller must hold t.mu (write). Route target slices are
// deep-copied so in-place edits by Disconnect/Remove never touch an
// already-published snapshot.
func (t *topology[P]) rebuildLocked() {
	if t.closed {
		t.snap.Store(nil)
		return
	}
	snap := &snapshot[P]{nodes: make([]snapshotNode[P], len(t.nodes))}
	for i := range t.nodes {
		n := t.nodes[i].base()
		sn := &snap.nodes[i]
		sn.node = t.nodes[i]
		sn.active = n.active
		if len(n.routes) == 0 {
			continue
		}
		sn.routes = make([]nodeRoute, len(n.routes))
		for j := range n.routes {
			sn.routes[j] = n.routes[j]
			if len(n.routes[j].to) > 0 {
				sn.routes[j].to = append([]int(nil), n.routes[j].to...)
			}
		}
	}
	t.snap.Store(snap)
}

func (t *topology[P]) AddSource(source Source, policy BufferPolicy) (NodeRef, error) {
	n, err := t.strategy.newNode(policy)
	if err != nil {
		return "", err
	}
	b := n.base()
	b.name, b.kind, b.source = source.Name(), nodeSource, source
	return t.add(n)
}

func (t *topology[P]) AddStage(stage Stage, policy BufferPolicy) (NodeRef, error) {
	n, err := t.strategy.newNode(policy)
	if err != nil {
		return "", err
	}
	b := n.base()
	b.name, b.kind, b.stage = stage.Name(), nodeStage, stage
	if admit, ok := stage.(Admitter); ok {
		b.admit = admit
	}
	return t.add(n)
}

func (t *topology[P]) AddSink(sink Sink, policy BufferPolicy) (NodeRef, error) {
	n, err := t.strategy.newNode(policy)
	if err != nil {
		return "", err
	}
	b := n.base()
	b.name, b.kind, b.sink = sink.Name(), nodeSink, sink
	return t.add(n)
}

func (t *topology[P]) add(n P) (NodeRef, error) {
	b := n.base()
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return "", ErrClosed
	}
	if err := t.strategy.mutable(b.kind == nodeSource); err != nil {
		return "", err
	}
	if b.name == "" {
		return "", ErrUnknownNode
	}
	if _, ok := t.index[b.name]; ok {
		return "", ErrNodeExists
	}
	index := len(t.nodes)
	b.active = true
	b.counters = &nodeCounters{}
	t.index[b.name] = index
	t.nodes = append(t.nodes, n)
	if b.kind == nodeSource {
		t.sources = append(t.sources, index)
	}
	t.strategy.attachLocked(n, index)
	t.rebuildLocked()
	return NodeRef(b.name), nil
}

func (t *topology[P]) Connect(route Route) error {
	if len(route.To) == 0 {
		return ErrInvalidLink
	}
	policy, err := normalizeRoutePolicy(route.Policy)
	if err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.strategy.mutable(false); err != nil {
		return err
	}
	from, ok := t.index[route.From]
	if !ok || !t.nodes[from].base().active {
		return ErrUnknownNode
	}
	if t.nodes[from].base().kind == nodeSink {
		return ErrInvalidLink
	}

	destinations := make([]int, 0, len(route.To))
	for i := range route.To {
		to, ok := t.index[route.To[i]]
		if !ok || !t.nodes[to].base().active {
			return ErrUnknownNode
		}
		if t.nodes[to].base().kind == nodeSource {
			return ErrInvalidLink
		}
		destinations = append(destinations, to)
	}
	n := t.nodes[from].base()
	n.routes = append(n.routes, nodeRoute{
		to:     destinations,
		policy: policy,
		label:  route.Label,
	})
	t.rebuildLocked()
	return nil
}

func (t *topology[P]) Disconnect(route Route) error {
	if len(route.To) == 0 {
		return ErrInvalidLink
	}
	policy, err := normalizeRoutePolicy(route.Policy)
	if err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.strategy.mutable(false); err != nil {
		return err
	}
	from, ok := t.index[route.From]
	if !ok || !t.nodes[from].base().active {
		return ErrUnknownNode
	}
	destinations := make(map[int]struct{}, len(route.To))
	for i := range route.To {
		to, ok := t.index[route.To[i]]
		if !ok {
			return ErrUnknownNode
		}
		destinations[to] = struct{}{}
	}
	n := t.nodes[from].base()
	removed := false
	routes := n.routes[:0]
	for i := range n.routes {
		existing := n.routes[i]
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
	n.routes = routes
	if !removed {
		return ErrInvalidLink
	}
	t.rebuildLocked()
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

// detachLocked is the shared front half of Remove: it validates ref, takes
// the node out of the index and drops every route into it. The runner then
// retires the node its own way (and publishes the snapshot). Caller holds
// t.mu (write).
func (t *topology[P]) detachLocked(ref NodeRef) (P, error) {
	var zero P
	if t.closed {
		return zero, ErrClosed
	}
	if err := t.strategy.mutable(false); err != nil {
		return zero, err
	}
	index, ok := t.index[ref.String()]
	if !ok || !t.nodes[index].base().active {
		return zero, ErrUnknownNode
	}
	n := t.nodes[index]
	if n.base().kind == nodeSource {
		return zero, ErrInvalidLink
	}
	delete(t.index, ref.String())
	for i := range t.nodes {
		b := t.nodes[i].base()
		if !b.active {
			continue
		}
		routes := b.routes[:0]
		for j := range b.routes {
			route := b.routes[j]
			to := route.to[:0]
			for k := range route.to {
				if route.to[k] != index {
					to = append(to, route.to[k])
				}
			}
			if len(to) == 0 {
				continue
			}
			route.to = to
			routes = append(routes, route)
		}
		b.routes = routes
	}
	return n, nil
}

func (t *topology[P]) Spec() Spec {
	t.mu.RLock()
	defer t.mu.RUnlock()
	spec := Spec{
		Name:     t.config.Name,
		Realtime: t.config.Realtime,
	}
	for i := range t.nodes {
		node := t.nodes[i].base()
		if !node.active {
			continue
		}
		spec.Nodes = append(spec.Nodes, node.spec())
		for j := range node.routes {
			route := &node.routes[j]
			for k := range route.to {
				to := t.nodes[route.to[k]].base()
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

func (t *topology[P]) Events() <-chan av.Event {
	return t.events
}

func (t *topology[P]) Stats() GraphStats {
	t.mu.RLock()
	defer t.mu.RUnlock()
	names := make([]string, len(t.nodes))
	nodes := make([]*nodeCounters, len(t.nodes))
	reported := make([]uint64, len(t.nodes))
	for i := range t.nodes {
		n := t.nodes[i].base()
		names[i] = n.name
		nodes[i] = n.counters
		reported[i] = reportedDrops(n.source, n.stage, n.sink)
	}
	return snapshotStats(&t.cold, names, nodes, reported)
}

// SetNodePaused pauses or resumes delivery to a single node. The lock-free
// producer reads the flag on every delivery, so pausing one branch leaves
// the source and sibling branches untouched.
func (t *topology[P]) SetNodePaused(ref NodeRef, paused bool) error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	index, ok := t.index[ref.String()]
	if !ok || index < 0 || index >= len(t.nodes) || !t.nodes[index].base().active {
		return ErrUnknownNode
	}
	t.nodes[index].base().paused.Store(paused)
	return nil
}

func (t *topology[P]) Close() error {
	return t.CloseContext(context.Background())
}

// CloseContext shuts the graph down. New emits stop at the nil routing
// snapshot; the strategy tears each node's delivery down and waits out what
// is in flight, so Close never runs under a concurrent Handle (the contract
// a live Remove keeps). A node whose wait gets stuck is reported and left
// open. The observer stream closes last.
func (t *topology[P]) CloseContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.snap.Store(nil)
	type closing struct {
		node         P
		await, stuck bool
	}
	var first error
	nodes := make([]closing, 0, len(t.nodes))
	for i := range t.nodes {
		n := closing{node: t.nodes[i]}
		b := n.node.base()
		if !b.active {
			continue
		}
		var err error
		if n.await, err = t.strategy.stopLocked(ctx, n.node); err != nil {
			first = cmp.Or(first, err)
			n.stuck = true
		}
		b.active = false
		nodes = append(nodes, n)
	}
	t.mu.Unlock()

	for i := range nodes {
		n := &nodes[i]
		if n.stuck || !n.await {
			continue
		}
		if err := t.strategy.awaitStopped(ctx, n.node); err != nil {
			first = cmp.Or(first, err)
			n.stuck = true
		}
	}
	for i := range nodes {
		if nodes[i].stuck {
			continue
		}
		if err := nodes[i].node.base().close(); err != nil {
			first = cmp.Or(first, err)
		}
	}
	// Close events under eventsMu so a concurrent lock-free emit cannot send
	// on the channel after it is closed.
	t.eventsMu.Lock()
	t.eventsClosed = true
	close(t.events)
	t.eventsMu.Unlock()
	return first
}

// armReadinessLocked snapshots the readiness set at Run start: the active
// sinks that have not yet seen media. Caller must hold t.mu.
func (t *topology[P]) armReadinessLocked() {
	var sinks, unfed int64
	for i := range t.nodes {
		node := t.nodes[i].base()
		if !node.active || node.kind != nodeSink {
			continue
		}
		sinks++
		if !node.sawMedia.Load() {
			unfed++
		}
	}
	if t.ready.arm(sinks, unfed) {
		t.publishReady()
	}
}

// noteSinkMedia counts a sink out of the readiness set (first media delivery,
// or removal before any media); the last count publishes readiness.
func (t *topology[P]) noteSinkMedia(sawMedia *atomic.Bool) {
	if t.ready.note(sawMedia) {
		t.publishReady()
	}
}

// publishReady reports av.EventTaskReady on the graph's observer stream, at
// most once. Cold path: it runs once per graph, never per message.
func (t *topology[P]) publishReady() {
	if !t.ready.claim() {
		return
	}
	t.observe(taskReadyEvent(), nil)
}

// publishEvent mirrors an emitted event onto the observer stream.
func (t *topology[P]) publishEvent(msg *Message, counters *nodeCounters) {
	if msg.Kind != MessageEvent || msg.Event == nil {
		return
	}
	t.observe(*msg.Event, counters)
}

// observe sends event to the observer stream without blocking; a full
// stream counts a DropObserver against counters (nil: graph-level only).
func (t *topology[P]) observe(event av.Event, counters *nodeCounters) {
	t.eventsMu.Lock()
	defer t.eventsMu.Unlock()
	if t.eventsClosed {
		return
	}
	select {
	case t.events <- event:
	default:
		observeDrop(counters, DropObserver, &t.cold)
	}
}

func (n *nodeBase) close() error {
	switch n.kind {
	case nodeSource:
		return n.source.Close()
	case nodeStage:
		return n.stage.Close()
	case nodeSink:
		return n.sink.Close()
	default:
		return nil
	}
}

func (n *nodeBase) spec() NodeSpec {
	spec := NodeSpec{Name: n.name, Kind: n.kind.spec()}
	if describer := n.describer(); describer != nil {
		spec.Detail = describer.DescribeNode().Detail
	}
	return spec
}

func (n *nodeBase) describer() NodeDescriber {
	var node any
	switch n.kind {
	case nodeSource:
		node = n.source
	case nodeStage:
		node = n.stage
	case nodeSink:
		node = n.sink
	}
	describer, _ := node.(NodeDescriber)
	return describer
}

func (k nodeKind) spec() NodeKind {
	switch k {
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

func (r *nodeRoute) matches(msg *Message) bool {
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

func (r *nodeRoute) matchesStream(msg *Message) bool {
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

func (r *nodeRoute) matchesEvent(msg *Message) bool {
	if msg.Kind != MessageEvent || msg.Event == nil {
		return false
	}
	return r.label == "" || string(msg.Event.Type) == r.label
}
