package pipeline

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
)

// A direct node is serialized without a lock, by ownership. Its gate counts
// the deliveries registered on it: the producer whose CAS takes the count
// from 0 to 1 owns the node and runs Handle on its own goroutine; a CAS from
// 1 back to 0 releases it, the only two atomics a lone producer pays. A
// producer that finds the node busy registers with an increment and queues
// a waiter record on the node's lock-free list, spins briefly on that
// record, then parks on it. It never spins on the node itself: producers
// that walk the same nodes in lockstep and spin to take each one stay
// bunched up and collide at every node, while parking spreads them apart,
// as a mutex does.
//
// When the owner finishes and others are queued, it hands the node to the
// first waiter that is still spinning (one CAS, no wakeup), or serves the
// ones that parked: it runs each one's message with that producer's context
// and hands back the result, and wakes them only after it let go of the
// node. A parked waiter is never handed the node, or the node would stay
// owned by a sleeping goroutine while everyone arriving piled up behind it.
// Serving is bounded: after directServeBudget messages the owner passes the
// node on regardless, so no producer serves others forever.
//
// Each message is still handled exactly once, one at a time, and before its
// producer's Emit returns: a waiter does not return until its message was
// handled. Remove and Close keep their contract through the same gate: they
// set the closing bit and wait for the count to drain, which includes every
// queued waiter, and a delivery whose increment sees the bit sheds itself.

// directServeBudget is how many queued messages an owner handles for other
// producers before it passes the node on.
const directServeBudget = 64

// waiterSpins is how many times a waiter checks its own record before it
// parks: long enough to be handed a node that frees at once, short enough
// that a long Handle costs the waiters no CPU.
const waiterSpins = 32

// registrantSpins is how many times an owner looks for a registered
// producer's record, mid-push, before it yields to let that producer run.
const registrantSpins = 64

const (
	waiterPending uint32 = iota // queued, not yet handled
	waiterParked                // queued, and its producer sleeps on wake
	waiterDone                  // handled; delivered and err hold the result
	waiterOwner                 // not handled: the node was handed to it
)

// directWaiter is one producer's delivery queued on a node another producer
// owns. The producer fills it, queues it, and waits; the owner handles the
// message and completes it.
type directWaiter struct {
	next  atomic.Pointer[directWaiter] // the node queue's link
	state atomic.Uint32
	inUse atomic.Bool   // an emitter's cached record is taken
	wake  chan struct{} // capacity 1: the owner's token for a parked waiter

	ctx       context.Context
	msg       *Message
	delivered bool
	err       error
}

func newDirectWaiter() *directWaiter {
	return &directWaiter{wake: make(chan struct{}, 1)}
}

// directWaiters backs the rare case of one emitter delivering from several
// goroutines at once; an emitter's own cached record covers the rest.
var directWaiters = sync.Pool{New: func() any { return newDirectWaiter() }}

// takeWaiter returns a free waiter record for a delivery from e.
func (e *directEmitter) takeWaiter() *directWaiter {
	if w := e.waiter; w != nil && w.inUse.CompareAndSwap(false, true) {
		return w
	}
	return directWaiters.Get().(*directWaiter)
}

func (e *directEmitter) putWaiter(w *directWaiter) {
	w.ctx, w.msg, w.err, w.delivered = nil, nil, nil, false
	if w == e.waiter {
		w.inUse.Store(false)
		return
	}
	directWaiters.Put(w)
}

// wait blocks until the owner completed w, and reports how.
func (w *directWaiter) wait() uint32 {
	for range waiterSpins {
		if s := w.state.Load(); s != waiterPending {
			return s
		}
	}
	if w.state.CompareAndSwap(waiterPending, waiterParked) {
		<-w.wake
	}
	return w.state.Load()
}

// complete finishes w as done or owner. After the swap the owner does not
// touch w again unless its producer is parked, in which case that producer
// is blocked on wake until this send.
func (w *directWaiter) complete(state uint32) {
	if w.state.Swap(state) == waiterParked {
		w.wake <- struct{}{}
	}
}

func (w *directWaiter) link() *atomic.Pointer[directWaiter] { return &w.next }

// waiterQueue is a node's queue of waiters. Only the node's current owner
// pops, and ownership passes through the gate's atomics.
type waiterQueue = mpscQueue[directWaiter, *directWaiter]

// deliver hands msg to dst once and reports whether Handle ran. The lone
// producer path is one CAS, Handle, and one CAS.
func (g *directRunner) deliver(ctx context.Context, from *directEmitter, dst *directNode, msg *Message) (bool, error) {
	if dst.paused.Load() {
		// Branch paused: skip this node; the source and siblings keep flowing.
		return false, nil
	}
	if dst.admit != nil {
		// Admission runs on the producer's goroutine before the delivery
		// registers, so a converging stage can hold one upstream back without
		// holding anything its siblings need.
		if err := dst.admit.Admit(ctx, msg); err != nil {
			return false, err
		}
	}
	// A free node is taken with one CAS from 0 to 1, which can only succeed
	// before Remove sets the closing bit. The CAS comes first, not a load: on
	// a line another core wrote last, a load and then a CAS cost two
	// coherence trips.
	if dst.gate.CompareAndSwap(0, 1) {
		return g.own(ctx, dst, msg)
	}
	// Busy: register, then check the tombstone. Remove sets the closing bit
	// and then waits for the count to drain, so a delivery that registered
	// first is waited out and one that arrives after the bit sheds itself —
	// Close never runs under an in-flight Handle. The waiter record is ready
	// before the increment, so an owner that sees the registration finds the
	// record queued moments later.
	w := from.takeWaiter()
	w.ctx, w.msg = ctx, msg
	w.state.Store(waiterPending)
	v := dst.gate.Add(1)
	if v&nodeClosingBit != 0 {
		dst.gate.Add(-1)
		from.putWaiter(w)
		return false, nil
	}
	if v == 1 {
		// The node freed meanwhile: this producer owns it.
		from.putWaiter(w)
		return g.own(ctx, dst, msg)
	}
	return g.queue(ctx, from, dst, msg, w)
}

// own runs msg as dst's owner and then releases the node, serving whoever
// queued meanwhile. The release is deferred, as an unlock would be, so a
// panic in this producer's own Handle still passes the node on. A panic in
// a Handle the owner runs for a queued waiter leaves that waiter parked;
// goav recovers no panics, so either way the process ends.
func (g *directRunner) own(ctx context.Context, dst *directNode, msg *Message) (bool, error) {
	defer g.release(dst)
	return g.handle(ctx, dst, msg)
}

// queue parks msg, carried by w, on dst's waiter queue until the owner
// handled it, or handed it the node.
func (g *directRunner) queue(ctx context.Context, from *directEmitter, dst *directNode, msg *Message, w *directWaiter) (bool, error) {
	dst.waiters.push(w)
	if w.wait() == waiterOwner {
		from.putWaiter(w)
		return g.own(ctx, dst, msg)
	}
	delivered, err := w.delivered, w.err
	from.putWaiter(w)
	return delivered, err
}

// release gives up dst. Alone (count 1), one CAS does it; otherwise the
// owner hands the node to a queued waiter that is still spinning, or serves
// the parked ones in order, waking them once the node is released, so their
// wakeups never hold the node up. A count above one with an empty queue is
// a producer between its increment and its push, or a shedding one between
// its increment and decrement: both finish in a few instructions, so the
// owner looks again.
func (g *directRunner) release(dst *directNode) {
	if dst.gate.CompareAndSwap(1, 0) {
		return
	}
	var woken [directServeBudget]*directWaiter
	served := 0
	defer wakeServed(woken[:], &served)
	for spins := 0; ; {
		v := dst.gate.Load()
		if v&^nodeClosingBit == 1 {
			if dst.gate.CompareAndSwap(v, v-1) {
				return
			}
			continue
		}
		w := dst.waiters.pop()
		if w == nil {
			if spins++; spins > registrantSpins {
				runtime.Gosched()
			}
			continue
		}
		spins = 0
		if w.state.CompareAndSwap(waiterPending, waiterOwner) {
			// Still spinning: it takes the node at once, on its own goroutine.
			dst.gate.Add(-1) // this owner's own registration
			return
		}
		if served == directServeBudget {
			dst.gate.Add(-1) // this owner's own registration
			w.complete(waiterOwner)
			return
		}
		w.delivered, w.err = g.handle(w.ctx, dst, w.msg)
		dst.gate.Add(-1) // w's registration
		// w is parked (the CAS above failed, and a parked waiter never
		// unparks on its own): it stays blocked until woken, so its wakeup
		// can wait until the node is released.
		w.state.Store(waiterDone)
		woken[served] = w
		served++
	}
}

// wakeServed wakes the parked waiters a release served, once the node is
// released.
func wakeServed(woken []*directWaiter, served *int) {
	for _, w := range woken[:*served] {
		w.wake <- struct{}{}
	}
}

// handle runs one message through dst. The caller owns dst.
func (g *directRunner) handle(ctx context.Context, dst *directNode, msg *Message) (bool, error) {
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
