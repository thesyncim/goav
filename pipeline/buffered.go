package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/goav/av"
)

type runPhase uint8

const (
	runIdle     runPhase = iota // not executing; topology freely mutable
	runActive                   // sources and workers executing; stages/sinks mutable
	runDraining                 // sources ended, queues closing; topology and controls refused
)

// bufferedRunner is the queued delivery strategy over the shared topology:
// an emit binds the message into a slot of each target's bounded queue, and
// one serial worker per node drains it into Handle.
type bufferedRunner struct {
	topology[*bufferedNode]
	pending inflight // queued, not-yet-handled messages, for the drain
	// order is the running time order (time_order.go), set for an ordered
	// run so a delivery into a join charges its source.
	order   atomic.Pointer[timeOrder]
	workers sync.WaitGroup
	runCtx  context.Context
	// phase tracks the run lifecycle under topology.mu:
	// runIdle -> runActive -> runDraining -> runIdle.
	phase runPhase
}

// bufferedNode is a node with its own bounded queue and serial worker. The
// queue is a lock-free ring (ring.go) of slots from the node's preallocated
// pool, so a producer enqueues with two CAS operations (take a free slot,
// claim a queue position) and the worker dequeues with two, and nothing on
// the message path takes a lock. Producers park only on a full DropBlock
// queue (or a flush rewriting it), each on its own wake channel; the worker
// parks only on an empty queue, on the wake channel.
type bufferedNode struct {
	nodeBase
	policy  BufferPolicy
	drop    dropController
	slots   []bufferedMessage
	emitter bufferedEmitter
	done    chan struct{}
	// opened says the queue has been made, by Run or by an attach to a
	// running graph. Written and read under topology.mu.
	opened bool
	// removed is set (once) when Remove retires the node: after its queue
	// drained, or when the drain could not start, so a worker still running
	// sheds without taking g.mu.
	removed atomic.Bool
	// qosLast rate-limits MaxLatency-shed QoS reports to one per node per
	// qosReportWindow (0 = never reported). Touched only on the shed path.
	qosLast atomic.Int64
	// wake parks the worker on an empty queue; a producer that queues while
	// it sleeps sends the token. Capacity 1, made once in attachLocked, so it
	// is never replaced under a running worker.
	wake chan struct{}
	// queue holds the slots waiting for the worker; free holds the slots
	// not in use. Both are reset when the queue opens. A retired node keeps
	// them, closed and empty, until its index is reused and it is collected:
	// a producer still holding an old snapshot may meet them, and sheds.
	queue ring[bufferedMessage]
	free  ring[bufferedMessage]
	// sleeping is set by the worker before it parks on wake, so a producer
	// pays one load per message and a channel send only to an idle worker.
	sleeping atomic.Bool
	// slotLine holds the DropBlock producers waiting for a slot, spaceLine
	// the ones holding a slot and waiting for a queue position (or any
	// producer waiting out a flush); both first come, first served.
	slotLine  parkLine
	spaceLine parkLine
	// waiting counts the producers announced for the slot line (high 32
	// bits) and the space line (low 32 bits) in one word, so a producer, a
	// release, or a take that finds nobody waiting pays one load and the
	// first come, first served machinery costs nothing.
	waiting atomic.Int64
	// queuedBytes is the payload bytes of queued messages, maintained only
	// when policy.MaxBytes > 0: a producer reserves before queuing (one CAS
	// that also checks the budget), releaseSlot subtracts.
	queuedBytes atomic.Int64
}

func (n *bufferedNode) base() *nodeBase { return &n.nodeBase }

func (n *bufferedNode) sourceOf() Source { return n.source }

type bufferedEmitter struct {
	graph *bufferedRunner
	from  int
	// self is the emitting node; once its index is reused, the snapshot holds
	// another node there and this emitter sheds.
	self *bufferedNode
	// park is this emitter's record for parking on a full DropBlock queue,
	// made once so a blocked emit allocates nothing.
	park *parkedProducer
}

func (e *bufferedEmitter) Emit(ctx context.Context, msg *Message) error {
	_, err := e.graph.emitDelivery(ctx, e, msg)
	return err
}

// EmitDelivery implements the optional DeliveryEmitter capability: Emit plus a
// per-message report of delivered vs deliberately-shed targets.
func (e *bufferedEmitter) EmitDelivery(ctx context.Context, msg *Message) (Delivery, error) {
	return e.graph.emitDelivery(ctx, e, msg)
}

// bufferedMessage is one queue slot: a header copy of the queued message that
// holds its own references to the payload (bind).
type bufferedMessage struct {
	message Message
	packet  av.Packet
	frame   av.Frame
	event   av.Event
	stream  av.Stream
	audio   av.AudioFrame
	video   av.VideoFrame
	planes  []av.Plane
	// enqueuedAt stamps when the message entered the queue, set only when the
	// node's MaxLatency is non-zero, so the worker can shed stale messages.
	enqueuedAt time.Time
	// bytes is the payload size counted against the node's MaxBytes budget,
	// recorded when the slot is queued and subtracted from queuedBytes on release.
	// Nonzero only when MaxBytes > 0, so releaseSlot's subtract is a no-op by default.
	bytes int64
}

func newBufferedRunner(config GraphConfig) (*bufferedRunner, error) {
	if config.Buffer.IsDirect() {
		return nil, ErrBufferedEdgesUnsupported
	}
	config.Buffer = normalizeBufferedPolicy(config.Buffer)
	g := &bufferedRunner{}
	g.init(config, g)
	g.pending.idle = make(chan struct{}, 1)
	return g, nil
}

func (g *bufferedRunner) newNode(policy BufferPolicy) (*bufferedNode, error) {
	n := &bufferedNode{policy: g.nodePolicy(policy)}
	// Closed until the queue opens: an emit before Run sheds.
	n.queue.reset(0)
	n.free.reset(0)
	return n, nil
}

// mutable refuses a new source once Run started (sources start only there),
// and every change while the run drains.
func (g *bufferedRunner) mutable(source bool) error {
	if g.phase == runDraining || (g.phase == runActive && source) {
		return ErrDynamicGraphUnsupported
	}
	return nil
}

// attachLocked gives a new node its delivery state; a stage or sink added to
// a running graph gets its queue and worker at once.
func (g *bufferedRunner) attachLocked(n *bufferedNode, index int) {
	n.drop.reset(n.policy)
	n.wake = make(chan struct{}, 1)
	n.emitter = bufferedEmitter{graph: g, from: index, self: n, park: newParkedProducer()}
	if g.phase == runActive && n.kind != nodeSource {
		g.openQueue(n)
		g.startWorkerLocked(n)
	}
}

// stopLocked closes a running node's queue; its worker sheds what is left
// (the routing snapshot is nil) and exits.
func (g *bufferedRunner) stopLocked(ctx context.Context, n *bufferedNode) (bool, error) {
	n.removed.Store(true)
	if g.phase == runIdle || n.kind == nodeSource {
		return false, nil
	}
	if err := g.closeNodeQueueContext(ctx, n); err != nil {
		return false, err
	}
	return true, nil
}

// awaitStopped waits for the node's worker to exit.
func (g *bufferedRunner) awaitStopped(ctx context.Context, n *bufferedNode, timeout time.Duration) error {
	return waitCloseDoneContext(ctx, "close", n.name, n.done, timeout)
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

func (g *bufferedRunner) Run(ctx context.Context) error {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return ErrClosed
	}
	if g.phase != runIdle {
		g.mu.Unlock()
		return ErrAlreadyRunning
	}
	if err := ctx.Err(); err != nil {
		g.mu.Unlock()
		return err
	}

	// The first error anywhere — a source, a node's Handle, teardown — cancels
	// runCtx, so live sources stop at their next Emit and blocked producers
	// wake, instead of the failure waiting for the sources to end on their own.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Make the done channel now: a context makes it on the first Done, under
	// its own mutex, and that first call must not be on the message path.
	_ = runCtx.Done()
	g.fail.reset(cancel)
	g.openQueuesLocked()
	g.armReadinessLocked()
	g.phase = runActive
	g.runCtx = runCtx
	g.domainParent = runCtx
	for _, d := range g.domains {
		d.setContext(runCtx)
	}
	g.pending.n.Store(0) // no worker runs yet; queues were just reopened
	g.workers = sync.WaitGroup{}
	for _, node := range g.nodes {
		if !node.isActive() || node.kind == nodeSource {
			continue
		}
		g.startWorkerLocked(node)
	}
	sources := make([]*bufferedNode, 0, len(g.sources))
	for _, index := range g.sources {
		if node := g.nodes[index]; node.isActive() {
			sources = append(sources, node)
		}
	}
	g.rebuildLocked() // publish the snapshot with queues now open
	g.mu.Unlock()

	// Offline file sources interleave by time (time_order.go), each packet's
	// work finishing before the next packet enters.
	var order *timeOrder
	if ordered := timeOrderedSources(g.config.Realtime, true, g.snap.Load(), sources, (*bufferedNode).sourceOf); ordered != nil {
		order = newTimeOrder(ordered, func() { g.pending.wait(runCtx.Done()) })
		g.order.Store(order)
		defer g.order.Store(nil)
	}
	var runs sync.WaitGroup
	for i, node := range sources {
		if order == nil {
			runs.Go(func() {
				g.fail.report(node.source.Start(runCtx, &node.emitter))
			})
			continue
		}
		emitter := &timeOrderedEmitter{emitter: &node.emitter, order: order, index: i}
		runs.Go(func() {
			defer order.leave(i)
			order.enter(runCtx, i)
			g.fail.report(node.source.Start(runCtx, emitter))
		})
	}

	runs.Wait()
	// A clean end drains: queued messages (and whatever stages re-emit from
	// them) are handled before the queues close. A failure or cancellation
	// skips the wait; workers shed what is left once the queues close.
	g.pending.wait(runCtx.Done())
	g.mu.Lock()
	g.phase = runDraining
	g.closeQueuesLocked()
	g.mu.Unlock()
	g.workers.Wait()
	g.mu.Lock()
	g.phase = runIdle
	g.runCtx = nil
	for _, node := range g.nodes {
		node.opened = false
	}
	g.mu.Unlock()
	return g.fail.err()
}

func (g *bufferedRunner) Remove(ref NodeRef) error {
	_, err := g.RemoveNode(ref)
	return err
}

// RemoveNode is Remove that also returns the node's final counters, read
// once its queued tail was delivered (the NodeRemover capability).
func (g *bufferedRunner) RemoveNode(ref NodeRef) (NodeStats, error) {
	return g.RemoveNodeContext(context.Background(), ref)
}

// RemoveNodeContext is the optional context-aware removal capability. Once
// removed, a node stays graph-owned until its queued work and finalizer finish.
func (g *bufferedRunner) RemoveNodeContext(ctx context.Context, ref NodeRef) (NodeStats, error) {
	node, err := g.remove(ctx, ref)
	if node == nil {
		return NodeStats{}, err
	}
	return g.finalStats(&node.nodeBase), err
}

func (g *bufferedRunner) remove(ctx context.Context, ref NodeRef) (*bufferedNode, error) {
	g.mu.Lock()
	running := g.phase == runActive
	node, err := g.detachLocked(ref)
	if err != nil {
		g.mu.Unlock()
		return nil, err
	}
	if node.kind == nodeSink && running {
		// A sink leaving before its first media message stops gating readiness.
		g.noteSinkMedia(&node.nodeBase)
	}
	if !running || !node.opened {
		g.retireLocked(node)
		g.mu.Unlock()
		return node, g.finalize(node)
	}
	// Nothing new reaches the node now; what it already queued is delivered
	// before it closes. Its own routes stay up while it drains, so a stage
	// hands its tail downstream instead of losing it. The drain is bounded by
	// the queue's capacity and the close-wait budget.
	g.closeNodeQueue(node)
	done := node.done
	g.rebuildLocked()
	g.mu.Unlock()
	err = waitCloseDoneContext(ctx, "remove", node.name, done, g.config.CloseWaitTimeout)
	g.mu.Lock()
	g.retireLocked(node)
	g.mu.Unlock()
	if err != nil {
		g.reap(node)
		return node, err
	}
	return node, g.finalize(node)
}

// retireLocked takes a removed node out of the graph: inactive, routeless,
// and shedding whatever its worker still holds. Finalization makes its index
// reusable only after its worker and Close finish. Caller holds g.mu.
func (g *bufferedRunner) retireLocked(node *bufferedNode) {
	node.phase = nodeRetired
	node.removed.Store(true)
	node.routes = nil
	g.rebuildLocked()
}

// closeNodeQueue closes a node's queue to producers, exactly once. It is one
// atomic step on the ring's tail (waiting out a flush in progress), so it
// never waits on a producer: a push that claimed its position first still
// lands and is delivered, every later one sheds. Then it wakes the worker,
// which drains what is queued and exits, and every parked producer, which
// sheds.
func (g *bufferedRunner) closeNodeQueue(node *bufferedNode) {
	if !node.opened {
		return
	}
	if node.queue.close() {
		node.wakeWorker()
		node.wakeAllWaiters()
	}
}

func (g *bufferedRunner) closeNodeQueueContext(ctx context.Context, node *bufferedNode) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	g.closeNodeQueue(node)
	return nil
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
		if !node.isActive() || node.kind == nodeSource {
			continue
		}
		g.openQueue(node)
	}
}

// openQueue makes a node's queue: Capacity positions and Capacity+1 slots,
// one more than the queue holds so the worker can deliver one while the
// queue is full. Cold: no producer or worker runs on the node.
func (g *bufferedRunner) openQueue(node *bufferedNode) {
	slotCount := node.policy.Capacity + 1
	node.slots = make([]bufferedMessage, slotCount)
	planes := make([]av.Plane, slotCount*slotPlanes)
	node.free.reset(slotCount)
	for i := range node.slots {
		node.slots[i].planes = planes[i*slotPlanes : i*slotPlanes : (i+1)*slotPlanes]
		node.free.push(&node.slots[i], false)
	}
	node.drop.reset(node.policy)
	node.queuedBytes.Store(0)
	node.sleeping.Store(false)
	node.queue.reset(node.policy.Capacity)
	node.opened = true
}

func (g *bufferedRunner) startWorkerLocked(node *bufferedNode) {
	done := make(chan struct{})
	node.done = done
	ctx := g.runCtx
	g.workers.Go(func() {
		defer close(done)
		g.runNode(ctx, node)
	})
}

func (g *bufferedRunner) closeQueuesLocked() {
	for i := range g.nodes {
		node := g.nodes[i]
		if !node.isRoutable() || node.kind == nodeSource {
			continue
		}
		g.closeNodeQueue(node)
	}
}
