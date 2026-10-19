package pipeline

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/goav/av"
)

// bufferedRunner is the queued delivery strategy over the shared topology:
// an emit binds the message into a slot of each target's bounded queue, and
// one serial worker per node drains it into Handle.
type bufferedRunner struct {
	topology[*bufferedNode]
	pending inflight // queued, not-yet-handled messages, for the drain
	workers sync.WaitGroup
	runCtx  context.Context
	running bool
	// draining is set once the sources ended and the queues are closing; the
	// topology refuses changes until Run returns.
	draining bool
}

type bufferedNode struct {
	nodeBase
	policy     BufferPolicy
	drop       dropController
	queue      chan *bufferedMessage
	free       chan *bufferedMessage
	slots      []bufferedMessage
	emitter    bufferedEmitter
	queueMutex sync.Mutex
	done       chan struct{}
	// queuedBytes tracks the total payload bytes of in-flight queued messages,
	// maintained only when policy.MaxBytes > 0: the producer adds before queuing
	// (under queueMutex), the lock-free worker subtracts in releaseSlot.
	queuedBytes atomic.Int64
	// removed is set (once) when Remove retires the node: after its queue
	// drained, or when the drain could not start, so a worker still running
	// sheds without taking g.mu.
	removed atomic.Bool
	// queueClosed guards the queue channel against the lock-free producer: it is
	// set under queueMutex before the queue is closed, so enqueue (which holds
	// queueMutex but no g.mu) never sends on a closed channel.
	queueClosed bool
	// qosLast rate-limits MaxLatency-shed QoS reports to one per node per
	// qosReportWindow (0 = never reported). Touched only on the shed path.
	qosLast atomic.Int64
	// space wakes a DropBlock producer parked in enqueueBlocking: the worker
	// signals it on every take and slot return, teardown on queue close.
	// Capacity 1, made once in attachLocked, so it is never replaced under a
	// running worker.
	space chan struct{}
}

func (n *bufferedNode) base() *nodeBase { return &n.nodeBase }

type bufferedEmitter struct {
	graph *bufferedRunner
	from  int
	// self is the emitting node; once its index is reused, the snapshot holds
	// another node there and this emitter sheds.
	self *bufferedNode
}

func (e *bufferedEmitter) Emit(ctx context.Context, msg *Message) error {
	_, err := e.graph.emitDelivery(ctx, e.from, e.self, msg)
	return err
}

// EmitDelivery implements the optional DeliveryEmitter capability: Emit plus a
// per-message report of delivered vs deliberately-shed targets.
func (e *bufferedEmitter) EmitDelivery(ctx context.Context, msg *Message) (Delivery, error) {
	return e.graph.emitDelivery(ctx, e.from, e.self, msg)
}

// bufferedMessage is one queue slot: a header copy of the queued message that
// holds its own references to the payload (bind).
type bufferedMessage struct {
	message Message
	packet  av.Packet
	frame   av.Frame
	event   av.Event
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
	return &bufferedNode{policy: g.nodePolicy(policy)}, nil
}

// mutable refuses a new source once Run started (sources start only there),
// and every change while the run drains.
func (g *bufferedRunner) mutable(source bool) error {
	if g.running && (source || g.draining) {
		return ErrDynamicGraphUnsupported
	}
	return nil
}

// attachLocked gives a new node its delivery state; a stage or sink added to
// a running graph gets its queue and worker at once.
func (g *bufferedRunner) attachLocked(n *bufferedNode, index int) {
	n.drop = newDropController(n.policy)
	n.space = make(chan struct{}, 1)
	n.emitter = bufferedEmitter{graph: g, from: index, self: n}
	if g.running && n.kind != nodeSource {
		g.openQueue(n)
		g.startWorkerLocked(n)
	}
}

// stopLocked closes a running node's queue; its worker sheds what is left
// (the routing snapshot is nil) and exits.
func (g *bufferedRunner) stopLocked(ctx context.Context, n *bufferedNode) (bool, error) {
	if !g.running || n.kind == nodeSource {
		return false, nil
	}
	if n.queue != nil {
		if err := g.closeNodeQueueContext(ctx, "close", n); err != nil {
			go forceCloseNodeQueue(n)
			return false, err
		}
	}
	return true, nil
}

// awaitStopped waits for the node's worker to exit.
func (g *bufferedRunner) awaitStopped(ctx context.Context, n *bufferedNode) error {
	return waitCloseDoneContext(ctx, "close", n.name, n.done, g.config.CloseWaitTimeout)
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
	if err := ctx.Err(); err != nil {
		g.mu.Unlock()
		return err
	}

	// The first error anywhere — a source, a node's Handle, teardown — cancels
	// runCtx, so live sources stop at their next Emit and blocked producers
	// wake, instead of the failure waiting for the sources to end on their own.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	g.fail.reset(cancel)
	g.openQueuesLocked()
	g.armReadinessLocked()
	g.running = true
	g.draining = false
	g.runCtx = runCtx
	g.pending.n.Store(0) // no worker runs yet; queues were just reopened
	g.workers = sync.WaitGroup{}
	for _, node := range g.nodes {
		if !node.active || node.kind == nodeSource {
			continue
		}
		g.startWorkerLocked(node)
	}
	sources := make([]*bufferedNode, 0, len(g.sources))
	for _, index := range g.sources {
		if node := g.nodes[index]; node.active {
			sources = append(sources, node)
		}
	}
	g.rebuildLocked() // publish the snapshot with queues now open
	g.mu.Unlock()

	var runs sync.WaitGroup
	for _, node := range sources {
		runs.Go(func() {
			g.fail.report(node.source.Start(runCtx, &node.emitter))
		})
	}

	runs.Wait()
	// A clean end drains: queued messages (and whatever stages re-emit from
	// them) are handled before the queues close. A failure or cancellation
	// skips the wait; workers shed what is left once the queues close.
	g.pending.wait(runCtx.Done())
	g.mu.Lock()
	g.draining = true
	g.fail.report(g.closeQueuesLocked())
	g.mu.Unlock()
	g.workers.Wait()
	g.mu.Lock()
	g.running = false
	g.draining = false
	g.runCtx = nil
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
	node, err := g.remove(ref)
	if node == nil {
		return NodeStats{}, err
	}
	return g.finalStats(&node.nodeBase), err
}

func (g *bufferedRunner) remove(ref NodeRef) (*bufferedNode, error) {
	g.mu.Lock()
	running := g.running
	node, err := g.detachLocked(ref)
	if err != nil {
		g.mu.Unlock()
		return nil, err
	}
	if node.kind == nodeSink && running {
		// A sink leaving before its first media message stops gating readiness.
		g.noteSinkMedia(&node.sawMedia)
	}
	if !running || node.queue == nil {
		g.retireLocked(node, true)
		g.mu.Unlock()
		return node, node.close()
	}
	// Nothing new reaches the node now; what it already queued is delivered
	// before it closes. Its own routes stay up while it drains, so a stage
	// hands its tail downstream instead of losing it. The drain is bounded by
	// the queue's capacity and the close-wait budget.
	if err := g.closeNodeQueue("remove", node); err != nil {
		// The worker may still run: the node keeps its queue and index.
		g.retireLocked(node, false)
		g.mu.Unlock()
		go forceCloseNodeQueue(node)
		return node, err
	}
	done := node.done
	g.rebuildLocked()
	g.mu.Unlock()
	err = waitCloseDone("remove", node.name, done, g.config.CloseWaitTimeout)
	g.mu.Lock()
	if !node.active {
		// Close took the graph down mid-drain and closed the node itself.
		g.mu.Unlock()
		return node, err
	}
	// A drain that timed out leaves the worker running: keep its queue.
	g.retireLocked(node, err == nil)
	g.mu.Unlock()
	if err != nil {
		return node, err
	}
	return node, node.close()
}

// retireLocked takes a removed node out of the graph: inactive, routeless,
// and shedding whatever its worker still holds. Once its worker is done
// (stopped), the node gives up its queue and slots and its index becomes
// reusable, so a removed node holds no memory. Caller holds g.mu.
func (g *bufferedRunner) retireLocked(node *bufferedNode, stopped bool) {
	node.active = false
	node.removed.Store(true)
	node.routes = nil
	if stopped {
		node.queueMutex.Lock()
		node.queueClosed = true
		node.queue, node.free, node.slots = nil, nil, nil
		node.queueMutex.Unlock()
		g.reclaimLocked(node)
	}
	g.rebuildLocked()
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
	node.signalSpace()
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
	node.signalSpace()
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
	planes := make([]av.Plane, slotCount*slotPlanes)
	for i := range node.slots {
		node.slots[i].planes = planes[i*slotPlanes : i*slotPlanes : (i+1)*slotPlanes]
		node.free <- &node.slots[i]
	}
	node.drop = newDropController(node.policy)
	node.queueClosed = false
}

func (g *bufferedRunner) startWorkerLocked(node *bufferedNode) {
	queue := node.queue
	done := make(chan struct{})
	node.done = done
	ctx := g.runCtx
	g.workers.Go(func() {
		defer close(done)
		g.runNode(ctx, node, queue)
	})
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

// emitDelivery is emit with per-message delivery accounting: it additionally
// reports how many matching targets queued the message and how many shed it,
// powering the optional DeliveryEmitter capability.
func (g *bufferedRunner) emitDelivery(ctx context.Context, from int, self *bufferedNode, msg *Message) (Delivery, error) {
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
	snap := g.snap.Load()
	if snap == nil {
		return delivery, ErrClosed
	}
	if from < 0 || from >= len(snap.nodes) {
		return delivery, ErrUnknownNode
	}
	fromNode := &snap.nodes[from]
	if !fromNode.active || fromNode.node != self {
		return delivery, nil
	}
	observeOut(fromNode.node.counters, msg, &g.cold)
	g.publishEvent(msg, fromNode.node.counters)

	// A real fanout (more than one matching target) isolates a failing target so
	// it cannot abort siblings or the source; a simple one-target chain keeps the
	// strict behavior (e.g. DropNever propagates ErrBackpressure to tear down).
	// Every target queues the same payload by reference.
	fanout := countTargets(snap, fromNode, msg) > 1

	for i := range fromNode.routes {
		route := &fromNode.routes[i]
		if !route.matches(msg) {
			continue
		}
		for j := range route.to {
			to := route.to[j]
			if to < 0 || to >= len(snap.nodes) || !snap.nodes[to].active {
				continue
			}
			target := snap.nodes[to].node
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
func countTargets(snap *snapshot[*bufferedNode], fromNode *snapshotNode[*bufferedNode], msg *Message) int {
	count := 0
	for i := range fromNode.routes {
		route := &fromNode.routes[i]
		if !route.matches(msg) {
			continue
		}
		for j := range route.to {
			to := route.to[j]
			if to >= 0 && to < len(snap.nodes) && snap.nodes[to].active {
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
	if node.admit != nil {
		// Admission runs on the producer's goroutine before it takes the queue
		// lock or a slot, so holding one upstream back blocks nothing else.
		if err := node.admit.Admit(ctx, msg); err != nil {
			return false, err
		}
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
		return g.enqueueBlocking(ctx, node, msg)
	case bufferDropIncoming:
		observeDrop(node.counters, node.policy.Drop, &g.cold)
		return false, nil
	case bufferDropOldest:
		select {
		case dropped := <-node.queue:
			node.releaseSlot(dropped)
			g.pending.done()
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

// enqueueBlocking implements true backpressure for DropBlock: it paces the
// producer until the node has both queue space and a free slot, then admits the
// message atomically, reporting false when the queue was torn down instead
// (a shed). The slot is acquired only when the send is about to happen — never
// held across a wait — so a paced producer cannot starve the node's free pool
// for other producers (which would make their admit path observe space yet no
// slot). The admit (slot bind + queue send) runs under node.queueMutex; only
// the lock-free worker drains the queue, so a space checked under the mutex
// cannot vanish before the send, and the send stays serialized with
// close(node.queue). The wait between attempts releases the mutex, so a
// concurrent Close/Remove (which needs the mutex to tear the queue down) is
// never stuck behind a paced producer. The caller holds node.queueMutex on
// entry and this returns with it held.
//
// The wait polls nothing and allocates nothing: it parks on node.space (see
// bufferedNode) or ctx. A producer leaving the wait passes the signal on, so
// several parked producers never lose a wakeup to the one-token channel.
func (g *bufferedRunner) enqueueBlocking(ctx context.Context, node *bufferedNode, msg *Message) (bool, error) {
	for {
		if node.queueClosed {
			// Queue torn down (Remove/Close): shed.
			node.signalSpace()
			return false, nil
		}
		if len(node.queue) < cap(node.queue) {
			select {
			case slot := <-node.free:
				if err := slot.bind(msg, node.policy); err != nil {
					node.releaseSlot(slot)
					return false, err
				}
				g.pending.add()
				node.accountQueuedBytes(slot, msg)
				node.queue <- slot
				if len(node.free) > 0 && len(node.queue) < cap(node.queue) {
					node.signalSpace()
				}
				return true, nil
			default:
				// Free pool momentarily empty though the queue has space; wait.
			}
		}
		if err := ctx.Err(); err != nil {
			node.signalSpace()
			return false, err
		}
		node.queueMutex.Unlock()
		select {
		case <-ctx.Done():
			node.queueMutex.Lock()
			node.signalSpace()
			return false, ctx.Err()
		case <-node.space:
			node.queueMutex.Lock()
		}
	}
}

// signalSpace wakes one producer parked in enqueueBlocking. It never blocks:
// the one-token channel already holding a token means a wakeup is pending.
func (n *bufferedNode) signalSpace() {
	select {
	case n.space <- struct{}{}:
	default:
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
	g.pending.add()
	node.accountQueuedBytes(slot, msg)
	if err := enqueueMessage(ctx, node.queue, slot); err != nil {
		g.pending.done()
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

// runNode is a node's serial worker. A delivery error is reported the moment
// it happens — which cancels the run — and from then on every worker sheds
// what is still queued instead of delivering it, so a failed task stops
// promptly rather than finishing its backlog.
func (g *bufferedRunner) runNode(ctx context.Context, node *bufferedNode, queue <-chan *bufferedMessage) {
	for msg := range queue {
		// Taking a message frees queue space a parked DropBlock producer may be
		// waiting on, even while this slot stays held through delivery.
		node.signalSpace()
		switch {
		case g.fail.failed.Load():
			// The run failed; shed without delivering.
		case g.staleByLatency(node, msg):
			observeDrop(node.counters, DropStale, &g.cold)
			g.reportStaleQoS(node, msg)
		default:
			g.fail.report(g.deliver(ctx, node, &msg.message))
		}
		node.releaseSlot(msg)
		g.pending.done()
	}
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
// the routing snapshot going nil (graph Close) sheds with ErrClosed; node.removed (a node Remove
// retired before its queue drained) sheds silently.
func (g *bufferedRunner) deliver(ctx context.Context, node *bufferedNode, msg *Message) error {
	if g.snap.Load() == nil {
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
		if (msg.Kind == MessagePacket || msg.Kind == MessageFrame) && !node.sawMedia.Load() {
			g.noteSinkMedia(&node.sawMedia)
		}
		return node.sink.Handle(ctx, msg)
	default:
		return ErrInvalidLink
	}
}

// qosReportWindow rate-limits MaxLatency-shed QoS reports to one per node per
// window, so a persistently stale queue cannot storm the observer stream. The
// window runs on the process monotonic clock (time.Since a fixed origin, like
// staleByLatency's measurement); 0 in qosLast means "never reported", and the
// origin bias below keeps a stored stamp from ever being 0.
const qosReportWindow = time.Second

var qosWindowOrigin = time.Now()

// reportStaleQoS publishes one rate-limited av.EventQoS for a message shed
// under BufferPolicy.MaxLatency: lateness is how long past the budget it
// waited, dropped is always true. Runs only on the shed path — never per
// delivered message — with one atomic load when suppressed and the cold
// observer send when claiming.
func (g *bufferedRunner) reportStaleQoS(node *bufferedNode, msg *bufferedMessage) {
	now := time.Since(qosWindowOrigin)
	last := node.qosLast.Load()
	if last != 0 && now-time.Duration(last) < qosReportWindow {
		return
	}
	stamp := int64(now)
	if stamp == 0 {
		stamp = 1
	}
	if !node.qosLast.CompareAndSwap(last, stamp) {
		return
	}
	event := av.Event{
		Type:     av.EventQoS,
		StreamID: qosStreamID(&msg.message),
		Reason:   node.name,
		Metadata: av.QoSMetadata(time.Since(msg.enqueuedAt)-node.policy.MaxLatency, true),
	}
	g.observe(event, node.counters)
}

func qosStreamID(msg *Message) av.StreamID {
	switch {
	case msg == nil:
		return ""
	case msg.Packet != nil:
		return msg.Packet.StreamID
	case msg.Frame != nil:
		return msg.Frame.StreamID
	case msg.Event != nil:
		return msg.Event.StreamID
	default:
		return ""
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

// releaseSlot drops the references a slot holds and returns it to the free
// list.
func (n *bufferedNode) releaseSlot(slot *bufferedMessage) {
	if slot == nil {
		return
	}
	if slot.bytes != 0 {
		n.queuedBytes.Add(-slot.bytes)
		slot.bytes = 0
	}
	slot.release()
	n.free <- slot
	n.signalSpace()
}

// slotPlanes is the plane capacity each slot reserves up front, enough for
// planar video and audio; a frame with more planes grows its slot once.
const slotPlanes = 4

// bind queues src by reference: the slot copies the message header (packet,
// frame with its planes and Video or Audio header, or event) and takes a
// reference to every payload buffer, so the node reads the same bytes as
// every sibling branch and the producer may release its own reference as
// soon as Emit returns. Nothing is copied but the header.
func (m *bufferedMessage) bind(src *Message, policy BufferPolicy) error {
	if src == nil {
		return ErrNilMessage
	}
	if policy.MaxLatency > 0 {
		m.enqueuedAt = time.Now()
	}
	m.message.Kind = src.Kind
	switch src.Kind {
	case MessagePacket:
		if src.Packet == nil {
			return nil
		}
		m.packet = *src.Packet
		m.packet.Payload.Retain()
		m.message.Packet = &m.packet
	case MessageFrame:
		if src.Frame == nil {
			return nil
		}
		m.frame = *src.Frame
		if src.Frame.Audio != nil {
			m.audio = *src.Frame.Audio
			m.frame.Audio = &m.audio
		}
		if src.Frame.Video != nil {
			m.video = *src.Frame.Video
			m.frame.Video = &m.video
		}
		m.planes = append(m.planes[:0], src.Frame.Planes...)
		for i := range m.planes {
			m.planes[i].Buffer.Retain()
		}
		m.frame.Planes = m.planes
		m.message.Frame = &m.frame
	case MessageEvent:
		if src.Event == nil {
			return nil
		}
		m.event = *src.Event
		m.message.Event = &m.event
	}
	return nil
}

// release drops the payload references the slot holds and clears it for the
// next message, keeping its plane capacity.
func (m *bufferedMessage) release() {
	m.packet.Payload.Release()
	for i := range m.planes {
		m.planes[i].Buffer.Release()
	}
	planes := m.planes[:0]
	*m = bufferedMessage{planes: planes}
}

// inflight counts queued, not-yet-handled messages. It replaces a WaitGroup
// because an increment from zero may race the drain wait (Inject from another
// goroutine, or a stage emitting from a goroutine of its own), which the
// WaitGroup contract forbids. The drain wait polls nothing: the decrement that
// reaches zero leaves a token in idle, and the waiter rechecks the count.
type inflight struct {
	n    atomic.Int64
	idle chan struct{} // capacity 1
}

func (f *inflight) add() { f.n.Add(1) }

func (f *inflight) done() {
	if f.n.Add(-1) == 0 {
		select {
		case f.idle <- struct{}{}:
		default:
		}
	}
}

// wait blocks until the count is zero or stop is closed, reporting whether
// it drained.
func (f *inflight) wait(stop <-chan struct{}) bool {
	for f.n.Load() != 0 {
		select {
		case <-f.idle:
		case <-stop:
			return false
		}
	}
	return true
}
