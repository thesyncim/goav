package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// directRunner is the inline delivery strategy over the shared topology: an
// emit runs every target's Handle on the producer's goroutine, serialized per
// node by ownership of its gate (handoff.go), with no lock.
type directRunner struct {
	topology[*directNode]
	// order is the running time order (time_order.go), set for an ordered
	// run so a delivery into a join charges its source.
	order   atomic.Pointer[timeOrder]
	running atomic.Bool
}

type directNode struct {
	nodeBase
	emitter directEmitter
	// drained receives a non-blocking signal when the last in-flight delivery
	// exits after nodeClosingBit is set, waking Remove/Close without polling.
	drained chan struct{}
	// The pad keeps what every delivery writes (the gate) off the cache lines
	// of the fields it only reads, so producers on several cores do not bounce
	// those lines between them.
	_ [cacheLine]byte
	// gate counts the deliveries registered on this node (its owner and the
	// waiters queued behind it) and carries the closing tombstone bit, so a
	// live Remove can wait out deliveries that loaded an older routing
	// snapshot before it closes the node. The increment that takes it from 0
	// to 1 makes its producer the owner (handoff.go).
	gate atomic.Int64
	// waiters queues the deliveries that arrived while the node was owned.
	waiters waiterQueue
}

func (n *directNode) notifyDrained() {
	if n.drained == nil {
		return
	}
	select {
	case n.drained <- struct{}{}:
	default:
	}
}

func (n *directNode) base() *nodeBase { return &n.nodeBase }

// nodeClosingBit marks a node's delivery gate as closing: deliveries that
// arrive after the bit is set shed themselves, and Remove waits until every
// delivery that arrived before it has drained.
const nodeClosingBit = int64(1) << 62

type directEmitter struct {
	graph *directRunner
	from  int
	// self is the emitting node; once its index is reused, the snapshot holds
	// another node there and this emitter sheds.
	self *directNode
	// waiter is this emitter's record for queuing on an owned node, made
	// once so a contended delivery allocates nothing.
	waiter *directWaiter
}

type directSourceRun struct {
	source  Source
	emitter *directEmitter
}

func (r directSourceRun) of() Source { return r.source }

func (e *directEmitter) Emit(ctx context.Context, msg *Message) error {
	_, err := e.graph.emitDelivery(ctx, e, msg)
	return err
}

// EmitDelivery implements the optional DeliveryEmitter capability: Emit plus a
// per-message report of delivered vs deliberately-shed targets.
func (e *directEmitter) EmitDelivery(ctx context.Context, msg *Message) (Delivery, error) {
	return e.graph.emitDelivery(ctx, e, msg)
}

func newDirectRunner(config GraphConfig) (*directRunner, error) {
	if !config.Buffer.IsDirect() {
		return nil, ErrBufferedEdgesUnsupported
	}
	g := &directRunner{}
	g.init(config, g)
	return g, nil
}

func (g *directRunner) newNode(policy BufferPolicy) (*directNode, error) {
	if !policy.IsDirect() {
		return nil, ErrBufferedEdgesUnsupported
	}
	n := &directNode{drained: make(chan struct{}, 1)}
	n.waiters.init()
	return n, nil
}

// mutable accepts every change: the direct runner applies a mutation by
// swapping the routing snapshot, running or not.
func (g *directRunner) mutable(bool) error { return nil }

func (g *directRunner) attachLocked(n *directNode, index int) {
	n.emitter = directEmitter{graph: g, from: index, self: n, waiter: newDirectWaiter()}
}

func (g *directRunner) stopLocked(context.Context, *directNode) (bool, error) {
	return true, nil
}

// awaitStopped drains the node's gate: a delivery already in flight loaded
// the previous snapshot.
func (g *directRunner) awaitStopped(ctx context.Context, n *directNode, timeout time.Duration) error {
	return waitNodeGateDrainedContext(ctx, "close", n.name, &n.gate, n.drained, timeout)
}

func (g *directRunner) Run(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return ErrClosed
	}
	if !g.running.CompareAndSwap(false, true) {
		g.mu.Unlock()
		return ErrAlreadyRunning
	}
	g.armReadinessLocked()
	var sourceStack [8]directSourceRun
	sources := sourceStack[:0]
	for _, index := range g.sources {
		node := g.nodes[index]
		if !node.isActive() {
			continue
		}
		sources = append(sources, directSourceRun{source: node.source, emitter: &node.emitter})
	}
	g.mu.Unlock()
	defer g.running.Store(false)

	switch len(sources) {
	case 0:
		return nil
	case 1:
		// One source sees any node failure synchronously from Emit, so there
		// is no sibling to cancel and the run needs no context of its own
		// (keeping Run allocation-free); the record still makes the node's
		// error the result when the source swallows it.
		g.fail.reset(nil)
		g.startFaultDomains(ctx)
		run := sources[0]
		g.fail.report(run.source.Start(ctx, run.emitter))
		return g.fail.err()
	}

	// Run every source under a shared cancelable context so the first fatal
	// error — a source's, or a node's Handle — cancels its siblings instead of
	// leaving them running until they end on their own. Siblings that observe
	// the cancellation and return context.Canceled do not overwrite it. We
	// still wait for every source goroutine before returning so no source
	// outlives Run.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Make the done channel now: a context makes it on the first Done, under
	// its own mutex, and that first call must not be on the message path.
	_ = runCtx.Done()
	g.fail.reset(cancel)
	g.startFaultDomains(runCtx)
	// Offline file sources interleave by time instead of racing (time_order.go);
	// the rest run freely, as every source of a realtime graph does.
	ordered := timeOrderedSources(g.config.Realtime, false, g.snap.Load(), sources, directSourceRun.of)
	var order *timeOrder
	if ordered != nil {
		order = newTimeOrder(ordered, nil)
		g.order.Store(order)
		defer g.order.Store(nil)
	}
	var runs sync.WaitGroup
	for i := range sources {
		run := sources[i]
		if ordered == nil || !ordered[i] {
			runs.Go(func() {
				g.fail.report(run.source.Start(runCtx, run.emitter))
			})
			continue
		}
		emitter := &timeOrderedEmitter{emitter: run.emitter, order: order, index: i}
		runs.Go(func() {
			defer order.leave(i)
			order.enter(runCtx, i)
			g.fail.report(run.source.Start(runCtx, emitter))
		})
	}
	runs.Wait()
	return g.fail.err()
}

func (g *directRunner) Remove(ref NodeRef) error {
	_, err := g.RemoveNode(ref)
	return err
}

// RemoveNode is Remove that also returns the node's final counters, read
// once its in-flight deliveries finished (the NodeRemover capability).
func (g *directRunner) RemoveNode(ref NodeRef) (NodeStats, error) {
	return g.RemoveNodeContext(context.Background(), ref)
}

// RemoveNodeContext is the optional context-aware removal capability. Once
// removed, a node stays graph-owned until its queued work and finalizer finish.
func (g *directRunner) RemoveNodeContext(ctx context.Context, ref NodeRef) (NodeStats, error) {
	node, err := g.remove(ctx, ref)
	if node == nil {
		return NodeStats{}, err
	}
	return g.finalStats(&node.nodeBase), err
}

func (g *directRunner) remove(ctx context.Context, ref NodeRef) (*directNode, error) {
	g.mu.Lock()
	node, err := g.detachLocked(ref)
	if err != nil {
		g.mu.Unlock()
		return nil, err
	}
	node.phase = nodeRetired
	node.routes = nil
	if node.kind == nodeSink {
		// A sink leaving before its first media message stops gating readiness.
		g.noteSinkMedia(&node.nodeBase)
	}
	g.rebuildLocked()
	g.mu.Unlock()
	if err := waitNodeGateDrainedContext(ctx, "remove", node.name, &node.gate, node.drained, g.config.CloseWaitTimeout); err != nil {
		g.reap(node)
		return node, err
	}
	return node, g.finalize(node)
}

// waitNodeGateDrainedContext tombstones a removed node's delivery gate and waits
// for every in-flight delivery to finish, so the node is never closed under a
// concurrent Handle. New deliveries (from emits holding an older routing
// snapshot) see the closing bit and shed themselves; the last exiting delivery
// signals drained so this wait parks cleanly without polling.
func waitNodeGateDrainedContext(ctx context.Context, operation string, node string, gate *atomic.Int64, drained <-chan struct{}, timeout time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if gate == nil {
		return nil
	}
	gate.Or(nodeClosingBit)
	if err := ctx.Err(); err != nil {
		return err
	}
	if gate.Load() == nodeClosingBit {
		return nil
	}
	var timer *time.Timer
	var timerCh <-chan time.Time
	if timeout > 0 {
		timer = time.NewTimer(timeout)
		defer timer.Stop()
		timerCh = timer.C
	}
	for gate.Load() != nodeClosingBit {
		select {
		case <-drained:
		case <-ctx.Done():
			return ctx.Err()
		case <-timerCh:
			pending := gate.Load() &^ nodeClosingBit
			return &CloseWaitError{Operation: operation, Node: node, Pending: pending, Timeout: timeout}
		}
	}
	return nil
}

func (g *directRunner) emitDelivery(ctx context.Context, e *directEmitter, msg *Message) (Delivery, error) {
	var delivery Delivery
	if e.self.faulted() {
		return Delivery{Shed: 1}, nil
	}
	if msg == nil {
		return delivery, ErrNilMessage
	}
	if err := ctx.Err(); err != nil {
		return delivery, err
	}
	if g.fail.failed.Load() {
		// The run already failed: refuse further media with its error, so a
		// source that swallows or retries its first Emit error stops anyway.
		return delivery, g.fail.firstErr()
	}
	// Lock-free hot path: read an immutable routing snapshot, deliver inline. No
	// mutex, no per-node copy, and no per-message destination slice (so wide
	// fanout stays allocation-free).
	snap := g.snap.Load()
	if snap == nil {
		return delivery, ErrClosed
	}
	from := e.from
	if from < 0 || from >= len(snap.nodes) {
		return delivery, ErrUnknownNode
	}
	fromNode := &snap.nodes[from]
	if !fromNode.active || fromNode.node != e.self {
		return delivery, nil
	}
	observeOut(fromNode.node.counters, msg, &g.cold)
	g.publishEvent(msg, fromNode.node.counters)
	for i := range fromNode.routes {
		route := &fromNode.routes[i]
		if !route.matches(msg) {
			continue
		}
		for j := range route.to {
			to := route.to[j]
			if to < 0 || to >= len(snap.nodes) {
				continue
			}
			dst := &snap.nodes[to]
			if !dst.active {
				continue
			}
			delivered, err := g.deliver(ctx, e, dst.node, msg)
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
