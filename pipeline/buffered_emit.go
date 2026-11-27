package pipeline

import (
	"context"
	"errors"
	"runtime"
)

func (g *bufferedRunner) emitDelivery(ctx context.Context, e *bufferedEmitter, msg *Message) (Delivery, error) {
	from := e.from
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
	fanoutKnown := false
	fanout := false

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
				if errors.Is(err, ErrBackpressure) {
					if !fanoutKnown {
						fanout = countTargets(snap, fromNode, msg) > 1
						fanoutKnown = true
					}
					if fanout {
						observeDrop(target.counters, target.policy.Drop.Reason(), &g.cold)
						delivery.Shed++
						continue
					}
				}
				return delivery, err
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
// It takes no lock. Each pass reads the queue's tail once; admitting takes a
// free slot, binds the message into it (header and references, outside any
// critical section), and claims a queue position with one CAS. When a
// concurrent producer or a close got there first, the CAS fails and the pass
// starts over on the new state, keeping the bound slot. A DropBlock producer
// waits instead of shedding (see enqueueBlocking); the other policies decide
// on what the pass saw. park is the producer's own parking record, nil for
// Inject.
func (g *bufferedRunner) enqueue(ctx context.Context, node *bufferedNode, msg *Message, park *parkedProducer) (bool, error) {
	if node.paused.Load() || node.faulted() {
		// Branch paused: skip this node without touching the source or siblings.
		return false, nil
	}
	if node.admit != nil {
		// Admission runs on the producer's goroutine before it takes a slot,
		// so holding one upstream back blocks nothing else.
		// An ordered run bounds a join's arms at the turn instead.
		if order := g.order.Load(); order == nil || !order.admit(node.admit, msg) {
			if err := node.admit.Admit(ctx, msg); err != nil {
				return false, err
			}
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
	// waits, so it never sheds others while it waits, and counts it again,
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
	if node.policy.Drop == DropBlock {
		return g.enqueueBlocking(ctx, node, msg, park, bytes, held)
	}
	var slot *bufferedMessage // bound to msg, not yet queued
	for {
		t := q.tail.Load()
		if t&ringClosed != 0 {
			g.abandon(node, slot, held)
			return false, nil
		}
		if t&ringLocked != 0 {
			// A flush is rewriting the queue: wait it out.
			held = node.unhold(held)
			if err := node.waitSpace(ctx, park, awaitUnlock); err != nil {
				g.abandon(node, slot, held)
				return false, err
			}
			continue
		}
		switch node.drop.decide(q.fullAt(t), msg) {
		case bufferAdmit:
			if slot == nil {
				if slot = node.free.popWait(); slot == nil {
					// Every slot is queued, in delivery, or bound by another
					// producer: report backpressure.
					g.abandon(node, nil, held)
					return false, ErrBackpressure
				}
				if err := slot.bind(msg, node.policy); err != nil {
					g.abandon(node, slot, held)
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
				return true, nil
			}
			slot.bytes = 0
			g.pending.done()
			// Full, locked, or closed since the tail was read: decide again.
		case bufferDropIncoming:
			g.abandon(node, slot, held)
			observeDrop(node.counters, node.policy.Drop.Reason(), &g.cold)
			return false, nil
		case bufferDropOldest:
			if dropped := q.pop(); dropped != nil {
				node.releaseSlot(dropped)
				g.pending.done()
				observeDrop(node.counters, node.policy.Drop.Reason(), &g.cold)
			} else {
				// The worker took the oldest first, or its push is still
				// publishing: look again.
				runtime.Gosched()
			}
		default:
			g.abandon(node, slot, held)
			return false, ErrBackpressure
		}
	}
}

// enqueueBlocking is enqueue for DropBlock: true backpressure, first come,
// first served. A producer needs a slot and then a queue position, and
// waits for either rather than shed:
//
//   - A slot comes from the free list, unless producers already wait for
//     one: then it queues behind them, and a released slot is handed to the
//     oldest of them directly (pumpSlots), so no producer that keeps running
//     can take slots ahead of parked ones. A producer that waited for one
//     takes from the free list whenever it gets to look.
//   - Holding a bound slot, it waits for a position when the queue is full;
//     the worker's next take wakes the oldest such producer. With Capacity+1
//     slots and Capacity positions, a full queue while a producer holds a
//     slot only lasts until that take.
//
// Waiting polls nothing and allocates nothing (park.go).
func (g *bufferedRunner) enqueueBlocking(ctx context.Context, node *bufferedNode, msg *Message, park *parkedProducer, bytes, held int64) (bool, error) {
	q := &node.queue
	var slot *bufferedMessage // bound to msg, not yet queued
	waited := false           // has waited for a slot: may take one from the free list
	lined := false            // has waited in the space line: may push
	for {
		t := q.tail.Load()
		if t&ringClosed != 0 {
			g.abandon(node, slot, held)
			return false, nil
		}
		if t&ringLocked != 0 {
			held = node.unhold(held)
			if err := node.waitSpace(ctx, park, awaitUnlock); err != nil {
				g.abandon(node, slot, held)
				return false, err
			}
			continue
		}
		waiting := node.waiting.Load() // the one load a pass with nobody waiting pays
		if slot == nil {
			if waited || slotWaiters(waiting) == 0 {
				slot = node.free.popWait()
			}
			if slot == nil {
				held = node.unhold(held)
				s, err := node.waitSlot(ctx, park)
				waited = true
				if err != nil {
					g.abandon(node, nil, held)
					return false, err
				}
				if s == nil {
					continue // closed: the next pass sheds
				}
				slot = s
				waiting = node.waiting.Load()
			}
			if err := slot.bind(msg, node.policy); err != nil {
				g.abandon(node, slot, held)
				return false, err
			}
		}
		if !lined && spaceWaiters(waiting) > 0 {
			// Producers holding slots wait for a position: queue behind them
			// rather than take the next one ahead of them.
			lined = true
			held = node.unhold(held)
			if err := node.waitSpace(ctx, park, awaitTurn); err != nil {
				g.abandon(node, slot, held)
				return false, err
			}
			continue
		}
		if held != bytes {
			node.queuedBytes.Add(bytes - held)
			held = bytes
		}
		slot.bytes = bytes
		g.pending.add()
		switch q.push(slot, false) {
		case ringOK:
			node.wakeWorker()
			return true, nil
		case ringFull:
			slot.bytes = 0
			g.pending.done()
			lined = true
			held = node.unhold(held)
			if err := node.waitSpace(ctx, park, awaitSpace); err != nil {
				g.abandon(node, slot, held)
				return false, err
			}
		default:
			// Locked or closed since the tail was read: look again.
			slot.bytes = 0
			g.pending.done()
		}
	}
}

// abandon undoes an enqueue that did not queue its message: the bound slot
// goes back to the pool (or to a producer waiting for one), and the counted
// bytes are returned.
func (g *bufferedRunner) abandon(node *bufferedNode, slot *bufferedMessage, held int64) {
	if slot != nil {
		node.releaseSlot(slot)
	}
	node.unhold(held)
}

// unhold returns the bytes an enqueue counted against MaxBytes before it
// waits or gives up, and reports what it still holds: nothing.
func (n *bufferedNode) unhold(held int64) int64 {
	if held != 0 {
		n.queuedBytes.Add(-held)
	}
	return 0
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

// awaitCond is what a producer in the space line waits for.
type awaitCond uint8

const (
	awaitSpace  awaitCond = iota // a queue position (full DropBlock queue)
	awaitTurn                    // a position, behind the producers already waiting
	awaitUnlock                  // the end of a flush
)

func (n *bufferedNode) ready(cond awaitCond) bool {
	t := n.queue.tail.Load()
	if t&ringClosed != 0 {
		return true
	}
	if cond == awaitSpace || cond == awaitTurn {
		return t&ringLocked != 0 || !n.queue.fullAt(t)
	}
	return t&ringLocked == 0
}

// waitSlot queues the producer in the slot line and returns the slot a
// release hands it, oldest first, or nil once the queue closed. The
// producer announces itself, joins the line, and then pumps (pumpSlots), so
// a slot freed before it joined still reaches the oldest in line; a close
// that came before it joined is caught by the closed check after joining.
func (n *bufferedNode) waitSlot(ctx context.Context, cached *parkedProducer) (*bufferedMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p := takeParked(cached)
	n.waiting.Add(slotWaiter)
	n.slotLine.join(p)
	n.pumpSlots()
	if n.queue.closed() {
		n.wakeAllWaiters()
	}
	select {
	case <-p.wake:
	case <-ctx.Done():
		n.waiting.Add(-slotWaiter)
		if n.slotLine.cancel(p) {
			return nil, ctx.Err() // p stays in line until skipped: not reused
		}
		if s := p.handed(); s != nil {
			n.releaseSlot(s) // to the next in line
		}
		putParked(cached, p)
		return nil, ctx.Err()
	}
	n.waiting.Add(-slotWaiter)
	s := p.handed()
	putParked(cached, p)
	return s, nil
}

// pumpSlots hands free slots to the producers in the slot line, oldest
// first. Any number of goroutines may pump at once: each pairs one free slot
// with one producer from the line, both taken from lock-free rings. A pump
// that finds the line empty puts its slot back and looks once more, because
// a producer may have joined (and pumped, finding no slot) meanwhile; the
// put-back and the join are each published before the other side's look.
func (n *bufferedNode) pumpSlots() {
	for slotWaiters(n.waiting.Load()) > 0 {
		s := n.free.pop()
		if s == nil {
			return
		}
		p := n.slotLine.next()
		if p == nil {
			n.free.pushWait(s)
			if n.slotLine.ready() {
				continue
			}
			return
		}
		p.hand(s)
	}
}

// waitSpace queues the producer in the space line until cond may hold: the
// worker's next take wakes the oldest in line (a full queue), a flush's end
// or a close wakes everyone. It checks cond after announcing itself and
// again after joining, waking the oldest in line itself when cond holds by
// then, so a change that came meanwhile is never lost.
func (n *bufferedNode) waitSpace(ctx context.Context, cached *parkedProducer, cond awaitCond) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if spaceWaiters(n.waiting.Add(spaceWaiter)) == 1 && cond != awaitTurn && n.ready(cond) {
		// Alone in the line and the change already came: no need to wait.
		n.waiting.Add(-spaceWaiter)
		return nil
	}
	defer n.waiting.Add(-spaceWaiter)
	p := takeParked(cached)
	n.spaceLine.join(p)
	if n.ready(cond) {
		n.spaceLine.wakeOne()
	}
	select {
	case <-p.wake:
		putParked(cached, p)
		return nil
	case <-ctx.Done():
		if n.spaceLine.cancel(p) {
			return ctx.Err() // p stays in line until skipped: not reused
		}
		putParked(cached, p)
		return ctx.Err()
	}
}

// wakeAllWaiters wakes every producer in either line: the queue closed, so
// each sheds. One that joins later sees the close after joining.
func (n *bufferedNode) wakeAllWaiters() {
	n.slotLine.wakeAll()
	n.spaceLine.wakeAll()
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
