package pipeline

import (
	"cmp"
	"context"
	"errors"
	"fmt"
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
	// Retirement removes routing before finalization. A timeout only stops
	// the caller's wait; the graph continues owning the node until Close.
	retiring  bool // guarded by topology.mu
	deferred  bool // a caller stopped waiting; retain the eventual outcome
	finalized bool
	reported  bool // deferred close error recorded, guarded by topology.mu
	closeOnce sync.Once
	reapOnce  sync.Once
	closeErr  error
	name      string
	id        uint64
	// index is the node's position in topology.nodes, fixed for its life; a
	// retired node's index is reused once nothing can deliver into it.
	index  int
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
	policy   BufferPolicy
	domain   atomic.Pointer[FaultDomain]
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
	// reports whether Close must wait n out (awaitStopped). An interrupted
	// stop is retried by eventual cleanup. Called under topology.mu.
	stopLocked(ctx context.Context, n P) (await bool, err error)
	// awaitStopped waits, outside topology.mu, until no delivery into n is
	// in flight, so Close never runs under a concurrent Handle.
	awaitStopped(ctx context.Context, n P, timeout time.Duration) error
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
	// reusable lists retired node indices nothing can deliver into anymore:
	// the next add takes one instead of growing nodes, so a task that attaches
	// and detaches forever keeps a bounded node table. Emitters compare their
	// own node with the snapshot's, so a retired node's emitter never speaks
	// for the index's new occupant.
	reusable []int
	// retired keeps the graph totals of nodes whose index was reused.
	retired retiredCounts
	closed  bool
	// deferredErr survives node reclamation and observer shutdown. Ordinary
	// synchronous close errors still belong to their initiating call only.
	deferredErr  error // guarded by mu
	cleanupErr   error // all real finalizer failures, never a caller's wait timeout
	domains      []*FaultDomain
	domainParent context.Context
	nextNodeID   uint64
	// snap is the routing snapshot the data plane reads without mu; nil once
	// the graph is closed.
	snap   atomic.Pointer[snapshot[P]]
	events chan av.Event
	// sending counts the observer sends in flight and carries eventsClosed,
	// so Close never closes the channel under a send and a send after Close
	// is skipped, with no lock: a send never blocks, so Close waits it out in
	// moments.
	sending atomic.Int64
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
	b.policy = policy
	b.name, b.kind, b.source = source.Name(), nodeSource, source
	return t.add(n)
}

func (t *topology[P]) AddStage(stage Stage, policy BufferPolicy) (NodeRef, error) {
	n, err := t.strategy.newNode(policy)
	if err != nil {
		return "", err
	}
	b := n.base()
	b.policy = policy
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
	b.policy = policy
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
	if last := len(t.reusable) - 1; last >= 0 {
		index = t.reusable[last]
		t.reusable = t.reusable[:last]
		t.retired.fold(t.nodes[index].base(), &t.cold)
		t.nodes[index] = n
	} else {
		t.nodes = append(t.nodes, n)
	}
	b.index = index
	t.nextNodeID++
	b.id = t.nextNodeID
	b.active = true
	b.counters = &nodeCounters{}
	t.index[b.name] = index
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
		src, dst := t.nodes[from].base(), t.nodes[to].base()
		sd, dd := src.domain.Load(), dst.domain.Load()
		if sd != nil && sd != dd || (sd != nil || dd != nil) && (src.admit != nil || dst.admit != nil) {
			return ErrUnsafeFaultDomain
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
	n.base().retiring = true
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

// finalStats reads a removed node's counters, as Stats would have listed
// them, once nothing delivers into it anymore.
func (t *topology[P]) finalStats(n *nodeBase) NodeStats {
	stats := snapshotStats(&t.cold, []string{n.name}, []*nodeCounters{n.counters}, []uint64{reportedDrops(n.source, n.stage, n.sink)})
	return stats.Nodes[n.name]
}

// reclaimLocked makes a removed node's index reusable once nothing can
// deliver into it: it is out of the index, routeless and inactive, and its
// worker or in-flight deliveries are done. Caller holds t.mu (write).
func (t *topology[P]) reclaimLocked(n P) {
	b := n.base()
	if b.active || b.index < 0 || b.index >= len(t.nodes) || t.nodes[b.index].base() != b {
		return
	}
	t.reusable = append(t.reusable, b.index)
	b.index = -1
}

// finalize is shared by removal, shutdown, and eventual cleanup. Reuse is
// allowed only after both delivery and resource finalization have finished.
func (t *topology[P]) finalize(n P) error {
	b := n.base()
	b.closeOnce.Do(func() {
		b.closeErr = b.close()
		t.mu.Lock()
		b.active = false
		b.routes = nil
		b.finalized = true
		var outcome error
		if b.closeErr != nil {
			outcome = &FinalizationError{ID: b.id, Node: NodeRef(b.name), Cause: b.closeErr}
			t.cleanupErr = errors.Join(t.cleanupErr, outcome)
		}
		if d := b.domain.Load(); d != nil && d.finalized(outcome) {
			for i, domain := range t.domains {
				if domain == d {
					t.domains = append(t.domains[:i], t.domains[i+1:]...)
					break
				}
			}
		}
		t.rememberDeferredLocked(b)
		t.reclaimLocked(n)
		t.mu.Unlock()
	})
	return b.closeErr
}

// CleanupError is the durable outcome of resource finalization. A caller
// timing out while waiting for a handler is not a finalization failure.
func (t *topology[P]) CleanupError() error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.cleanupErr
}

func (t *topology[P]) reap(n P) {
	n.base().reapOnce.Do(func() {
		t.mu.Lock()
		n.base().deferred = true
		// Another Close may already have finalized this node after our wait
		// expired. Recording under the same lock covers that race as well.
		t.rememberDeferredLocked(n.base())
		t.mu.Unlock()
		go func() {
			_ = t.strategy.awaitStopped(context.Background(), n, -1)
			_ = t.finalize(n)
		}()
	})
}

// rememberDeferredLocked retains the failure before PendingCleanup reaches
// zero or the slot can be reused. Events are supplementary and may be closed.
func (t *topology[P]) rememberDeferredLocked(b *nodeBase) {
	if !b.deferred || !b.finalized || b.reported || b.closeErr == nil {
		return
	}
	b.reported = true
	err := fmt.Errorf("pipeline: deferred cleanup of %q: %w", b.name, b.closeErr)
	t.deferredErr = errors.Join(t.deferredErr, err)
	t.observe(av.Event{Type: av.EventCleanupError, Cause: err, Reason: b.name, Metadata: av.Metadata{"node": b.name}}, nil)
}

func (t *topology[P]) Spec() Spec {
	t.mu.RLock()
	defer t.mu.RUnlock()
	spec := Spec{
		Name:      t.config.Name,
		Realtime:  t.config.Realtime,
		Execution: DescribeExecution(t.config),
	}
	spec.Execution.Admission = t.executionAdmission()
	for i := range t.nodes {
		node := t.nodes[i].base()
		if !node.active {
			continue
		}
		specNode := node.spec()
		specNode.Execution = DescribeNodeExecution(specNode.Kind, t.config, node.policy)
		spec.Nodes = append(spec.Nodes, specNode)
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
	// Like Spec, Nodes covers the live nodes; a removed node still counts in
	// the graph totals (an unnamed entry), and so do the nodes whose index was
	// reused (the retired entry).
	names := make([]string, len(t.nodes)+1)
	nodes := make([]*nodeCounters, len(t.nodes)+1)
	reported := make([]uint64, len(t.nodes)+1)
	for i := range t.nodes {
		n := t.nodes[i].base()
		if n.active {
			names[i] = n.name
		}
		nodes[i] = n.counters
		reported[i] = reportedDrops(n.source, n.stage, n.sink)
	}
	nodes[len(t.nodes)] = &t.retired.counters
	reported[len(t.nodes)] = t.retired.reported
	stats := snapshotStats(&t.cold, names, nodes, reported)
	for _, n := range t.nodes {
		if b := n.base(); b.retiring && b.index >= 0 {
			stats.PendingCleanup++
		}
	}
	return stats
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
// a live Remove keeps). A node whose wait times out remains owned and is
// finalized once its work finishes. Later calls also wait for pending cleanup
// and report retained deferred finalization errors, including after events close.
func (t *topology[P]) CloseContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	t.mu.Lock()
	firstClose := !t.closed
	t.closed = true
	t.snap.Store(nil)
	type closing struct {
		node         P
		await, stuck bool
	}
	first := ctx.Err()
	nodes := make([]closing, 0, len(t.nodes))
	for i := range t.nodes {
		n := closing{node: t.nodes[i]}
		b := n.node.base()
		if b.index < 0 {
			continue
		}
		var err error
		if n.await, err = t.strategy.stopLocked(ctx, n.node); err != nil {
			first = cmp.Or(first, err)
			n.stuck = true
		}
		b.active = false
		b.retiring = true
		nodes = append(nodes, n)
	}
	t.mu.Unlock()

	for i := range nodes {
		n := &nodes[i]
		if n.stuck || !n.await {
			continue
		}
		if err := t.strategy.awaitStopped(ctx, n.node, t.config.CloseWaitTimeout); err != nil {
			first = cmp.Or(first, err)
			n.stuck = true
		}
	}
	for i := range nodes {
		if nodes[i].stuck {
			// Even a canceled stop must stop delivery before the reaper waits.
			t.mu.Lock()
			_, _ = t.strategy.stopLocked(context.Background(), nodes[i].node)
			t.mu.Unlock()
			t.reap(nodes[i].node)
			continue
		}
		if err := t.finalize(nodes[i].node); err != nil {
			first = cmp.Or(first, err)
		}
	}
	// Close events once no send is in flight, so a concurrent emit can never
	// send on the closed channel.
	if firstClose {
		t.sending.Add(eventsClosed)
		for t.sending.Load() != eventsClosed {
			runtime.Gosched()
		}
		close(t.events)
	}
	t.mu.RLock()
	deferred := t.deferredErr
	t.mu.RUnlock()
	return errors.Join(first, deferred)
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
	if t.sending.Add(1)&eventsClosed != 0 {
		t.sending.Add(-1)
		return
	}
	select {
	case t.events <- event:
	default:
		observeDrop(counters, DropObserver, &t.cold)
	}
	t.sending.Add(-1)
}

// eventsClosed marks the observer stream closed in topology.sending.
const eventsClosed = int64(1) << 62

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
