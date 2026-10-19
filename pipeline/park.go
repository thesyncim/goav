package pipeline

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// parkedProducer is a producer parked on a buffered node, with a wake
// channel of its own: a waker wakes exactly that producer, so a busy worker
// never contends on a channel lock with other producers parking and
// unparking. Each emitter caches one (made once at attach), so parking
// allocates nothing.
type parkedProducer struct {
	next  atomic.Pointer[parkedProducer] // the park queue's link
	state atomic.Uint32
	wake  chan struct{} // capacity 1: one token per park
	inUse atomic.Bool   // an emitter's cached record is taken
}

const (
	parkIdle      uint32 = iota // not queued
	parkWaiting                 // queued, its producer waits on wake
	parkWoken                   // popped and woken: its token is sent
	parkCancelled               // its producer left on ctx; skip it
)

func (p *parkedProducer) link() *atomic.Pointer[parkedProducer] { return &p.next }

func newParkedProducer() *parkedProducer {
	return &parkedProducer{wake: make(chan struct{}, 1)}
}

// parkedProducers backs a producer with no emitter of its own (Inject) and
// one emitter parking from several goroutines at once.
var parkedProducers = sync.Pool{New: func() any { return newParkedProducer() }}

func takeParked(cached *parkedProducer) *parkedProducer {
	if cached != nil && cached.inUse.CompareAndSwap(false, true) {
		return cached
	}
	return parkedProducers.Get().(*parkedProducer)
}

func putParked(cached, p *parkedProducer) {
	if p == cached {
		p.inUse.Store(false)
		return
	}
	parkedProducers.Put(p)
}

// parkQueue is a node's parked producers, in the order they parked. Waking
// is first come, first served, so no producer waits out the others: one
// parked behind k others is woken after at most k wakeups.
//
// Any number of goroutines may ask for a wakeup at once (the worker, a
// releasing producer, a close), but the queue has one consumer at a time.
// Asking adds a credit and then tries to take the popping turn with a CAS;
// a goroutine that loses never waits, since the holder sees its credit
// before it lets go: after giving the turn up, the holder looks once more
// and takes it back while credits and parked producers both remain. A
// credit nobody can use yet stays until a producer parks, and that parker
// takes the turn itself, so a wakeup that comes just before a producer parks
// is not lost.
type parkQueue struct {
	queue   mpscQueue[parkedProducer, *parkedProducer]
	credits atomic.Int64
	popping atomic.Bool
}

func (q *parkQueue) init() { q.queue.init() }

// park queues p and delivers any wakeup already waiting; with room, it asks
// for one so the oldest parked producer, perhaps p itself, takes the room.
func (q *parkQueue) park(p *parkedProducer, room bool) {
	p.state.Store(parkWaiting)
	q.queue.push(p)
	if room {
		q.credits.Add(1)
	}
	q.deliver()
}

// wake asks for n parked producers to be woken, oldest first.
func (q *parkQueue) wake(n int64) {
	q.credits.Add(n)
	q.deliver()
}

// cancel takes p out after its producer gave up waiting, and reports whether
// p must be abandoned: still queued, it is skipped by the next pop and must
// not be reused before then. When a waker got there first, cancel takes the
// token it sent, and p is free again.
func (q *parkQueue) cancel(p *parkedProducer) (abandoned bool) {
	if p.state.CompareAndSwap(parkWaiting, parkCancelled) {
		return true
	}
	<-p.wake
	return false
}

func (q *parkQueue) deliver() {
	for q.popping.CompareAndSwap(false, true) {
		for q.credits.Load() > 0 {
			p := q.queue.pop()
			if p == nil {
				break
			}
			if !p.state.CompareAndSwap(parkWaiting, parkWoken) {
				continue // cancelled: its producer left
			}
			q.credits.Add(-1)
			p.wake <- struct{}{}
		}
		q.popping.Store(false)
		if q.credits.Load() <= 0 || q.queue.empty() {
			return
		}
		// A producer parked (or is linking itself in) while this goroutine
		// held the turn: take it back. A push between its swap and its link
		// lasts a few instructions; yield so it can finish.
		runtime.Gosched()
	}
}
