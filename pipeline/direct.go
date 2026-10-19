package pipeline

import (
	"context"
	"runtime"
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
	// run so a delivery into a converging stage can park its source.
	order atomic.Pointer[timeOrder]
}

type directNode struct {
	nodeBase
	emitter directEmitter
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
	n := &directNode{}
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
func (g *directRunner) awaitStopped(ctx context.Context, n *directNode) error {
	return waitNodeGateDrainedContext(ctx, "close", n.name, &n.gate, g.config.CloseWaitTimeout)
}

func (g *directRunner) Run(ctx context.Context) error {
	g.mu.RLock()
	if g.closed {
		g.mu.RUnlock()
		return ErrClosed
	}
	g.armReadinessLocked()
	var sourceStack [8]directSourceRun
	sources := sourceStack[:0]
	for _, index := range g.sources {
		node := g.nodes[index]
		if !node.active {
			continue
		}
		sources = append(sources, directSourceRun{source: node.source, emitter: &node.emitter})
	}
	g.mu.RUnlock()

	switch len(sources) {
	case 0:
		return nil
	case 1:
		// One source sees any node failure synchronously from Emit, so there
		// is no sibling to cancel and the run needs no context of its own
		// (keeping Run allocation-free); the record still makes the node's
		// error the result when the source swallows it.
		g.fail.reset(nil)
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
		turnCtx := order.context(runCtx, i)
		runs.Go(func() {
			defer order.leave(i)
			order.enter(turnCtx, i)
			g.fail.report(run.source.Start(turnCtx, emitter))
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
	node, err := g.remove(ref)
	if node == nil {
		return NodeStats{}, err
	}
	return g.finalStats(&node.nodeBase), err
}

func (g *directRunner) remove(ref NodeRef) (*directNode, error) {
	g.mu.Lock()
	node, err := g.detachLocked(ref)
	if err != nil {
		g.mu.Unlock()
		return nil, err
	}
	node.active = false
	node.routes = nil
	if node.kind == nodeSink {
		// A sink leaving before its first media message stops gating readiness.
		g.noteSinkMedia(&node.sawMedia)
	}
	g.rebuildLocked()
	g.mu.Unlock()
	if err := waitNodeGateDrained("remove", node.name, &node.gate, g.config.CloseWaitTimeout); err != nil {
		return node, err
	}
	// No delivery is in flight and none can start: the index is free.
	g.mu.Lock()
	g.reclaimLocked(node)
	g.mu.Unlock()
	return node, node.close()
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

func (g *directRunner) emitDelivery(ctx context.Context, e *directEmitter, msg *Message) (Delivery, error) {
	var delivery Delivery
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
