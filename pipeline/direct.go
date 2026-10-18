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
// node by its handle lock.
type directRunner struct {
	topology[*directNode]
}

type directNode struct {
	nodeBase
	emitter directEmitter
	// The pad keeps what every delivery writes (handleMu, gate) off the cache
	// lines of the fields it only reads, so producers on several cores do not
	// bounce those lines between them.
	_        [128]byte
	handleMu sync.Mutex
	// gate counts in-flight deliveries to this node and carries the closing
	// tombstone bit, so a live Remove can wait out deliveries that loaded an
	// older routing snapshot before it closes the node. One atomic add/sub per
	// delivery — the same budget class as the per-node counters.
	gate atomic.Int64
}

func (n *directNode) base() *nodeBase { return &n.nodeBase }

// nodeClosingBit marks a node's delivery gate as closing: deliveries that
// arrive after the bit is set shed themselves, and Remove waits until every
// delivery that arrived before it has drained.
const nodeClosingBit = int64(1) << 62

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
	return &directNode{}, nil
}

// mutable accepts every change: the direct runner applies a mutation by
// swapping the routing snapshot, running or not.
func (g *directRunner) mutable(bool) error { return nil }

func (g *directRunner) attachLocked(n *directNode, index int) {
	n.emitter = directEmitter{graph: g, from: index}
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
	g.fail.reset(cancel)
	// Offline file sources interleave by time instead of racing (time_order.go);
	// the rest run freely, as every source of a realtime graph does.
	ordered := timeOrderedSources(g.config.Realtime, g.snap.Load(), sources)
	var order *timeOrder
	if ordered != nil {
		order = newTimeOrder(ordered)
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
	g.mu.Lock()
	node, err := g.detachLocked(ref)
	if err != nil {
		g.mu.Unlock()
		return err
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
		return err
	}
	return node.close()
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
	if from < 0 || from >= len(snap.nodes) {
		return delivery, ErrUnknownNode
	}
	fromNode := &snap.nodes[from]
	if !fromNode.active {
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
			delivered, err := g.deliver(ctx, dst.node, msg)
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

func (g *directRunner) deliver(ctx context.Context, dst *directNode, msg *Message) (bool, error) {
	if dst.paused.Load() {
		// Branch paused: skip this node; the source and siblings keep flowing.
		return false, nil
	}
	if dst.admit != nil {
		// Admission runs on the producer's goroutine before the delivery
		// registers or takes the node's handle lock, so a converging stage can
		// hold one upstream back without holding anything its siblings need.
		if err := dst.admit.Admit(ctx, msg); err != nil {
			return false, err
		}
	}
	// Register this delivery before checking the tombstone: Remove sets the
	// closing bit and then waits for the gate to drain, so a delivery that
	// registered first is waited out and one that arrives after the bit sheds
	// itself — Close never runs under an in-flight Handle.
	if dst.gate.Add(1)&nodeClosingBit != 0 {
		dst.gate.Add(-1)
		return false, nil
	}
	defer dst.gate.Add(-1)
	dst.handleMu.Lock()
	defer dst.handleMu.Unlock()
	observeIn(dst.counters, msg, &g.cold)
	switch dst.kind {
	case nodeStage:
		if err := dst.stage.Handle(ctx, msg, &dst.emitter); err != nil {
			g.fail.report(err)
			return false, err
		}
		return true, nil
	case nodeSink:
		if (msg.Kind == MessagePacket || msg.Kind == MessageFrame) && !dst.sawMedia.Load() {
			g.noteSinkMedia(&dst.sawMedia)
		}
		if err := dst.sink.Handle(ctx, msg); err != nil {
			g.fail.report(err)
			return false, err
		}
		return true, nil
	default:
		return false, ErrInvalidLink
	}
}
