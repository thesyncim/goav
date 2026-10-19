package pipeline

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// parkedProducer is a producer waiting in one of a buffered node's lines,
// with a wake channel of its own, so a waker wakes exactly that producer and
// a busy worker never contends on a channel lock with producers parking and
// unparking. A slot handed to a producer waiting for one rides along in
// slot. Each emitter caches one (made once at attach), so waiting allocates
// nothing.
type parkedProducer struct {
	state atomic.Uint32
	wake  chan struct{}    // capacity 1: one token per wait
	slot  *bufferedMessage // handed over before the token, read after it
	inUse atomic.Bool      // an emitter's cached record is taken
}

const (
	parkIdle      uint32 = iota // not in a line
	parkWaiting                 // in a line, its producer waits on wake
	parkWoken                   // taken out of the line: its token is sent
	parkCancelled               // its producer left on ctx; skip it
)

func newParkedProducer() *parkedProducer {
	return &parkedProducer{wake: make(chan struct{}, 1)}
}

// parkedProducers backs a producer with no emitter of its own (Inject), one
// emitter waiting from several goroutines at once, and an emitter whose own
// record was left in a line by a cancelled wait.
var parkedProducers = sync.Pool{New: func() any { return newParkedProducer() }}

func takeParked(cached *parkedProducer) *parkedProducer {
	if cached != nil && cached.inUse.CompareAndSwap(false, true) {
		return cached
	}
	return parkedProducers.Get().(*parkedProducer)
}

func putParked(cached, p *parkedProducer) {
	p.state.Store(parkIdle)
	if p == cached {
		p.inUse.Store(false)
		return
	}
	parkedProducers.Put(p)
}

// hand gives p a slot and wakes it.
func (p *parkedProducer) hand(s *bufferedMessage) {
	p.slot = s
	p.wake <- struct{}{}
}

// handed takes the slot a waker handed p, nil when it was woken without one.
func (p *parkedProducer) handed() *bufferedMessage {
	s := p.slot
	p.slot = nil
	return s
}

// parkLineSize is how many producers one line holds; more wait for room in
// it, yielding, which takes that many producers parked on one node at once.
const parkLineSize = 256

// parkLine is a first come, first served line of waiting producers: a
// lock-free MPMC ring of their records (ring.go), made the first time
// anyone waits. Producers join at the back; any number of wakers take from
// the front at once, each record exactly once, so a producer that waits
// behind k others is woken after at most k wakeups of its line. A producer
// that gives up on ctx marks its record cancelled and leaves it in place;
// wakers skip it.
type parkLine struct {
	line atomic.Pointer[ring[parkedProducer]]
}

func (l *parkLine) ring() *ring[parkedProducer] {
	if r := l.line.Load(); r != nil {
		return r
	}
	r := new(ring[parkedProducer])
	r.reset(parkLineSize)
	if l.line.CompareAndSwap(nil, r) {
		return r
	}
	return l.line.Load()
}

// ready reports whether a record, waiting or cancelled, is at the front.
func (l *parkLine) ready() bool {
	r := l.line.Load()
	return r != nil && r.ready()
}

func (l *parkLine) join(p *parkedProducer) {
	p.state.Store(parkWaiting)
	r := l.ring()
	for r.push(p, false) != ringOK {
		runtime.Gosched()
	}
}

// next takes the oldest waiting producer out of the line, or nil.
func (l *parkLine) next() *parkedProducer {
	r := l.line.Load()
	if r == nil {
		return nil
	}
	for {
		p := r.pop()
		if p == nil {
			return nil
		}
		if p.state.CompareAndSwap(parkWaiting, parkWoken) {
			return p
		}
	}
}

// wakeOne wakes the oldest producer in the line.
func (l *parkLine) wakeOne() {
	if p := l.next(); p != nil {
		p.wake <- struct{}{}
	}
}

// wakeAll wakes every producer in the line.
func (l *parkLine) wakeAll() {
	for p := l.next(); p != nil; p = l.next() {
		p.wake <- struct{}{}
	}
}

// cancel takes p out after its producer gave up waiting, and reports whether
// p was still in line: it then stays there, skipped by the next waker, and
// must not be reused. Otherwise a waker took it first, and cancel takes the
// token (and any slot) it sent.
func (l *parkLine) cancel(p *parkedProducer) (abandoned bool) {
	if p.state.CompareAndSwap(parkWaiting, parkCancelled) {
		return true
	}
	<-p.wake
	return false
}

// The two halves of bufferedNode.waiting.
const (
	slotWaiter  = int64(1) << 32
	spaceWaiter = int64(1)
)

func slotWaiters(v int64) int64  { return v >> 32 }
func spaceWaiters(v int64) int64 { return v & (slotWaiter - 1) }
