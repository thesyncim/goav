package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/goav/av"
)

// Graph and flow-control sentinels, matched with errors.Is.
var (
	// ErrAlreadyRunning reports a concurrent Run call on a graph that is
	// already executing or draining.
	ErrAlreadyRunning = errors.New("pipeline: already running")
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

type nodePhase uint8

const (
	nodeActive    nodePhase = iota + 1 // live in index, Spec/Stats, and routing snapshot
	nodeDraining                       // detached from index/Spec/Stats, still emitting its queued tail
	nodeRetired                        // routeless and inactive in snapshot, awaiting finalization
	nodeFinalized                      // closed and reclaimed
)

// nodeBase is the part of a node both runners share: identity, the node
// itself, its routes, and the per-node state the topology reads. A runner's
// node type embeds it and adds how messages reach the node.
type nodeBase struct {
	// phase tracks the node's lifecycle under topology.mu. Retirement removes
	// routing before finalization; a timeout only stops the caller's wait, and
	// the graph continues owning the node until Close.
	phase    nodePhase // guarded by topology.mu
	deferred bool      // a caller stopped waiting; retain the eventual outcome
	reported bool      // deferred close error recorded, guarded by topology.mu
	// sinkCounted is set by armReadinessLocked for unfed sinks present at Run
	// start, so dynamically added sinks do not decrement readiness for them.
	sinkCounted bool
	closeOnce   sync.Once
	reapOnce    sync.Once
	closeErr    error
	name        string
	id          uint64
	// index is the node's position in topology.nodes, fixed for its life; a
	// retired node's index is reused once nothing can deliver into it.
	index  int
	kind   nodeKind
	source Source
	stage  Stage
	admit  Admitter // stage's Admitter capability, resolved once at add
	sink   Sink
	// routes changes under topology.mu only; the data plane reads the copies in
	// the routing snapshot.
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

func (b *nodeBase) isActive() bool   { return b.phase == nodeActive }
func (b *nodeBase) isRoutable() bool { return b.phase == nodeActive || b.phase == nodeDraining }
func (b *nodeBase) isRetiring() bool { return b.phase == nodeDraining || b.phase == nodeRetired }

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
		sn.active = n.isRoutable()
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
	b.phase = nodeActive
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
	if t.closed {
		return ErrClosed
	}
	if err := t.strategy.mutable(false); err != nil {
		return err
	}
	from, ok := t.index[route.From]
	if !ok || !t.nodes[from].base().isActive() {
		return ErrUnknownNode
	}
	if t.nodes[from].base().kind == nodeSink {
		return ErrInvalidLink
	}

	destinations := make([]int, 0, len(route.To))
	for i := range route.To {
		to, ok := t.index[route.To[i]]
		if !ok || !t.nodes[to].base().isActive() {
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
	if t.closed {
		return ErrClosed
	}
	if err := t.strategy.mutable(false); err != nil {
		return err
	}
	from, ok := t.index[route.From]
	if !ok || !t.nodes[from].base().isActive() {
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
	if !ok || !t.nodes[index].base().isActive() {
		return zero, ErrUnknownNode
	}
	n := t.nodes[index]
	if n.base().kind == nodeSource {
		return zero, ErrInvalidLink
	}
	delete(t.index, ref.String())
	n.base().phase = nodeDraining
	for i := range t.nodes {
		b := t.nodes[i].base()
		if !b.isRoutable() {
			continue
		}
		pruneNodeRoutes(b, func(to int) bool { return to == index })
	}
	return n, nil
}

func pruneNodeRoutes(b *nodeBase, drop func(int) bool) {
	routes := b.routes[:0]
	for j := range b.routes {
		route := b.routes[j]
		to := route.to[:0]
		for k := range route.to {
			if !drop(route.to[k]) {
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
	if b.isRoutable() || b.index < 0 || b.index >= len(t.nodes) || t.nodes[b.index].base() != b {
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
		b.phase = nodeFinalized
		b.routes = nil
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
	if !b.deferred || b.phase != nodeFinalized || b.reported || b.closeErr == nil {
		return
	}
	b.reported = true
	err := fmt.Errorf("pipeline: deferred cleanup of %q: %w", b.name, b.closeErr)
	t.deferredErr = errors.Join(t.deferredErr, err)
	t.observe(av.Event{Type: av.EventCleanupError, Cause: err, Reason: b.name, Metadata: av.Metadata{"node": b.name}}, nil)
}
