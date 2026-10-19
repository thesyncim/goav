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
	// waiters counts parked producers, so the worker and slot releases pay
	// one load per message and signal only when it is nonzero.
	waiters atomic.Int32
	// parked holds the parked producers, first come, first woken.
	parked parkQueue
	// queuedBytes is the payload bytes of queued messages, maintained only
	// when policy.MaxBytes > 0: a producer reserves before queuing (one CAS
	// that also checks the budget), releaseSlot subtracts.
	queuedBytes atomic.Int64
}

func (n *bufferedNode) base() *nodeBase { return &n.nodeBase }

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
	n.parked.init()
	return n, nil
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
	n.drop.reset(n.policy)
	n.wake = make(chan struct{}, 1)
	n.emitter = bufferedEmitter{graph: g, from: index, self: n, park: newParkedProducer()}
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
	if err := g.closeNodeQueueContext(ctx, n); err != nil {
		return false, err
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
	// Make the done channel now: a context makes it on the first Done, under
	// its own mutex, and that first call must not be on the message path.
	_ = runCtx.Done()
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
	g.closeQueuesLocked()
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
	if !running || !node.opened {
		g.retireLocked(node, true)
		g.mu.Unlock()
		return node, node.close()
	}
	// Nothing new reaches the node now; what it already queued is delivered
	// before it closes. Its own routes stay up while it drains, so a stage
	// hands its tail downstream instead of losing it. The drain is bounded by
	// the queue's capacity and the close-wait budget.
	g.closeNodeQueue(node)
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
	// A drain that timed out leaves the worker running: keep its index.
	g.retireLocked(node, err == nil)
	g.mu.Unlock()
	if err != nil {
		return node, err
	}
	return node, node.close()
}

// retireLocked takes a removed node out of the graph: inactive, routeless,
// and shedding whatever its worker still holds. Once its worker is done
// (stopped), its index becomes reusable; the node, closed rings and slots
// included, goes to the collector once the index is taken. Caller holds g.mu.
func (g *bufferedRunner) retireLocked(node *bufferedNode, stopped bool) {
	node.active = false
	node.removed.Store(true)
	node.routes = nil
	if stopped {
		g.reclaimLocked(node)
	}
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
		if !node.active || node.kind == nodeSource {
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
	node.parked.credits.Store(0) // a close left a wake for everyone
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
		if !node.active || node.kind == nodeSource {
			continue
		}
		g.closeNodeQueue(node)
	}
}

func (g *bufferedRunner) emitDelivery(ctx context.Context, e *bufferedEmitter, msg *Message) (Delivery, error) {
	from := e.from
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
	if !fromNode.active || fromNode.node != e.self {
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
			delivered, err := g.enqueue(ctx, target, msg, e.park)
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
//
// It takes no lock. Each pass reads the queue's tail once and lets the drop
// policy decide on what it saw; admitting takes a free slot, binds the
// message into it (header and references, outside any critical section),
// and claims a queue position with one CAS. When a concurrent producer or a
// close got there first, the CAS fails and the policy decides again on the
// new state, keeping the bound slot. A DropBlock producer parks (await) and
// never holds a slot while it waits, so it cannot starve the node's pool.
// park is the producer's own parking record, nil for Inject.
func (g *bufferedRunner) enqueue(ctx context.Context, node *bufferedNode, msg *Message, park *parkedProducer) (bool, error) {
	if node.paused.Load() {
		// Branch paused: skip this node without touching the source or siblings.
		return false, nil
	}
	if node.admit != nil {
		// Admission runs on the producer's goroutine before it takes a slot,
		// so holding one upstream back blocks nothing else.
		if err := node.admit.Admit(ctx, msg); err != nil {
			return false, err
		}
	}
	q := &node.queue
	if q.closed() {
		// Queue torn down (Remove/Close) after the snapshot was read; shed.
		return false, nil
	}
	// bytes is the payload counted against MaxBytes; held is how much of it
	// this enqueue has counted in queuedBytes. The budget is checked once, on
	// entry, as one CAS that also counts it; a producer gives it back before it
	// parks, so it never sheds others while it waits, and counts it again,
	// without a second check, when the message is admitted.
	var bytes, held int64
	if node.policy.MaxBytes > 0 {
		bytes = messageBytes(msg)
		if !node.reserveBytes(bytes) {
			// Byte budget exhausted: shed rather than queue past the bound.
			observeDrop(node.counters, DropOverflow, &g.cold)
			return false, nil
		}
		held = bytes
	}
	var slot *bufferedMessage // bound to msg, not yet queued
	parked := false
	for {
		t := q.tail.Load()
		if t&ringClosed != 0 {
			g.abandon(node, slot, held, parked)
			return false, nil
		}
		if t&ringLocked != 0 {
			// A flush is rewriting the queue: wait it out, holding no slot.
			slot = node.giveBack(slot)
			parked = true
			held = node.unhold(held)
			if err := node.await(ctx, awaitUnlock, park); err != nil {
				g.abandon(node, nil, held, parked)
				return false, err
			}
			continue
		}
		switch node.drop.decide(q.fullAt(t), msg) {
		case bufferAdmit:
			if !parked && node.policy.Drop == DropBlock && node.waiters.Load() > 0 {
				// Producers are parked here: queue behind them rather than take
				// the room one of them is woken for, so a producer that keeps
				// running cannot starve the parked ones. Wake one for this room
				// first, then park until a wakeup reaches this producer.
				slot = node.giveBack(slot)
				parked = true
				held = node.unhold(held)
				if err := node.await(ctx, awaitTurn, park); err != nil {
					g.abandon(node, nil, held, parked)
					return false, err
				}
				continue
			}
			if slot == nil {
				if slot = node.free.popWait(); slot == nil {
					// Every slot is queued, in delivery, or bound by another
					// producer that is about to queue it: the shortage passes
					// as the worker drains, so a blocking producer waits for
					// a slot and any other policy reports backpressure.
					if node.policy.Drop != DropBlock {
						g.abandon(node, nil, held, parked)
						return false, ErrBackpressure
					}
					parked = true
					held = node.unhold(held)
					if err := node.await(ctx, awaitSlot, park); err != nil {
						g.abandon(node, nil, held, parked)
						return false, err
					}
					continue
				}
				if err := slot.bind(msg, node.policy); err != nil {
					g.abandon(node, slot, held, parked)
					return false, err
				}
			}
			if held != bytes {
				node.queuedBytes.Add(bytes - held)
				held = bytes
			}
			slot.bytes = bytes
			g.pending.add()
			if q.push(slot, false) == ringOK {
				node.wakeWorker()
				if parked {
					node.notifyWaiters() // pass the wakeup on
				}
				return true, nil
			}
			slot.bytes = 0
			g.pending.done()
			// Full, locked, or closed since the tail was read: decide again.
		case bufferBlock:
			slot = node.giveBack(slot)
			parked = true
			held = node.unhold(held)
			if err := node.await(ctx, awaitSpace, park); err != nil {
				g.abandon(node, nil, held, parked)
				return false, err
			}
		case bufferDropIncoming:
			g.abandon(node, slot, held, parked)
			observeDrop(node.counters, node.policy.Drop, &g.cold)
			return false, nil
		case bufferDropOldest:
			if dropped := q.pop(); dropped != nil {
				node.releaseSlot(dropped)
				g.pending.done()
				observeDrop(node.counters, node.policy.Drop, &g.cold)
			} else {
				// The worker took the oldest first, or its push is still
				// publishing: look again.
				runtime.Gosched()
			}
		default:
			g.abandon(node, slot, held, parked)
			return false, ErrBackpressure
		}
	}
}

// abandon undoes an enqueue that did not queue its message: the bound slot
// goes back to the pool, the counted bytes are returned, and a producer that
// parked passes the wakeup on, so a token it consumed is not lost to the
// other parked producers.
func (g *bufferedRunner) abandon(node *bufferedNode, slot *bufferedMessage, held int64, parked bool) {
	if slot != nil {
		node.releaseSlot(slot)
	}
	node.unhold(held)
	if parked {
		node.notifyWaiters()
	}
}

// unhold returns the bytes an enqueue counted against MaxBytes before it
// parks or gives up, and reports what it still holds: nothing.
func (n *bufferedNode) unhold(held int64) int64 {
	if held != 0 {
		n.queuedBytes.Add(-held)
	}
	return 0
}

// giveBack returns a bound, unqueued slot to the pool before a wait.
func (n *bufferedNode) giveBack(slot *bufferedMessage) *bufferedMessage {
	if slot != nil {
		n.releaseSlot(slot)
	}
	return nil
}

// reserveBytes claims b bytes of the MaxBytes budget, or reports false when
// they do not fit. One CAS checks and claims, so concurrent producers never
// queue past the bound.
func (n *bufferedNode) reserveBytes(b int64) bool {
	for {
		queued := n.queuedBytes.Load()
		if queued+b > n.policy.MaxBytes {
			return false
		}
		if n.queuedBytes.CompareAndSwap(queued, queued+b) {
			return true
		}
	}
}

// awaitCond is what a parked producer waits for.
type awaitCond uint8

const (
	awaitSpace  awaitCond = iota // a queue position (full DropBlock queue)
	awaitSlot                    // a free slot
	awaitUnlock                  // the end of a flush
	awaitTurn                    // a wakeup of its own (queued behind parked producers)
)

// await parks a producer until cond may hold, ctx ends, or the queue closes.
// It polls nothing and allocates nothing: the producer announces itself in
// waiters, joins the node's park queue on its own wake channel, and sleeps.
// Whatever can satisfy cond (the worker taking a message, a slot returning,
// a flush ending, the queue closing) changes the queue state first and then
// reads waiters, so either the producer's check (made after announcing)
// sees the change and asks for a wakeup itself, or the signaler sees the
// producer and asks for one; the queue delivers it to the oldest parked
// producer (both sides are sequentially consistent atomics). The caller
// re-runs its policy decision after every wakeup, so a spurious one only
// costs a recheck. cached is the producer's own record, nil for Inject.
func (n *bufferedNode) await(ctx context.Context, cond awaitCond, cached *parkedProducer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p := takeParked(cached)
	n.waiters.Add(1)
	// A producer queuing behind others (awaitTurn) saw room; a wakeup for it
	// goes to whoever parked first.
	n.parked.park(p, cond == awaitTurn || n.ready(cond))
	select {
	case <-p.wake:
		n.waiters.Add(-1)
		putParked(cached, p)
		return nil
	case <-ctx.Done():
		n.waiters.Add(-1)
		if !n.parked.cancel(p) {
			putParked(cached, p)
		}
		return ctx.Err()
	}
}

func (n *bufferedNode) ready(cond awaitCond) bool {
	t := n.queue.tail.Load()
	if t&ringClosed != 0 {
		return true
	}
	switch cond {
	case awaitSpace:
		return t&ringLocked != 0 || !n.queue.fullAt(t)
	case awaitSlot:
		return n.free.ready()
	case awaitTurn:
		return false
	default:
		return t&ringLocked == 0
	}
}

// notifyWaiters wakes the oldest parked producer when one can proceed: the
// queue has a free position and the pool a free slot (an admit needs both),
// or the queue closed or locked. Called on every queue take and slot return,
// it costs one load when nobody waits. Whichever of the take and the return
// completes the pair sees the other's change (each publishes its own and
// then reads the other's), so the last one wakes.
func (n *bufferedNode) notifyWaiters() {
	if n.waiters.Load() > 0 && n.hasRoom() {
		n.parked.wake(1)
	}
}

func (n *bufferedNode) hasRoom() bool {
	t := n.queue.tail.Load()
	return t&(ringClosed|ringLocked) != 0 || (!n.queue.fullAt(t) && n.free.ready())
}

// wakeAllWaiters wakes every parked producer, and every one that parks
// later, because the queue closed: each sheds.
func (n *bufferedNode) wakeAllWaiters() {
	n.parked.wake(1 << 40)
}

// wakeParked wakes the producers parked now, because a flush ended and each
// must look again.
func (n *bufferedNode) wakeParked() {
	n.parked.wake(int64(n.waiters.Load()) + 1)
}

// wakeWorker hands the token to a worker parked on an empty queue. The
// producer's push is published before it reads sleeping, and the worker sets
// sleeping before it looks at the queue a last time, so one of them always
// sees the other.
func (n *bufferedNode) wakeWorker() {
	if n.sleeping.Load() && n.sleeping.CompareAndSwap(true, false) {
		select {
		case n.wake <- struct{}{}:
		default:
		}
	}
}

// idle parks the worker until a producer queues something or the queue
// closes, and reports false once the queue is closed and fully drained. A
// position claimed before the close is still delivered: the worker waits for
// its producer to publish it.
func (n *bufferedNode) idle() bool {
	q := &n.queue
	n.sleeping.Store(true)
	if q.ready() {
		n.sleeping.Store(false)
		return true
	}
	if q.closed() && q.drained() {
		n.sleeping.Store(false)
		return false
	}
	<-n.wake
	return true
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

// runNode is a node's serial worker. A delivery error is reported the moment
// it happens — which cancels the run — and from then on every worker sheds
// what is still queued instead of delivering it, so a failed task stops
// promptly rather than finishing its backlog.
func (g *bufferedRunner) runNode(ctx context.Context, node *bufferedNode) {
	for {
		msg := node.queue.pop()
		if msg == nil {
			if !node.idle() {
				return
			}
			continue
		}
		// Taking a message frees a queue position a parked DropBlock producer
		// may be waiting on, even while this slot stays held through delivery.
		node.notifyWaiters()
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

// releaseSlot drops the references a slot holds and returns it to the free
// list, waking a producer parked for one.
func (n *bufferedNode) releaseSlot(slot *bufferedMessage) {
	if slot == nil {
		return
	}
	if slot.bytes != 0 {
		n.queuedBytes.Add(-slot.bytes)
		slot.bytes = 0
	}
	slot.release()
	n.free.pushWait(slot)
	n.notifyWaiters()
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
