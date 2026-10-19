package pipeline

import (
	"sync"
	"sync/atomic"
)

// parkedProducer is a producer parked on a full buffered queue, with a wake
// channel of its own: a signaler wakes exactly that producer, so a busy
// worker never contends on a channel lock with other producers parking and
// unparking. Each emitter caches one (made once at attach), so parking
// allocates nothing.
type parkedProducer struct {
	wake  chan struct{} // capacity 1: at most one token is ever in flight
	inUse atomic.Bool   // an emitter's cached record is taken
}

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

// parkSlots is how many producers one node parks on their own channels; any
// more share the node's space channel.
const parkSlots = 16

// parkingLot is a node's registry of parked producers. A producer registers
// with one CAS into a free slot; a signaler takes a producer out with one
// CAS and then sends its token, so a token is sent only to a producer that
// is registered and owed one. The scan starts where the last one stopped,
// so no slot is favored.
type parkingLot struct {
	slots  [parkSlots]atomic.Pointer[parkedProducer]
	cursor atomic.Uint32
}

// register puts p in a free slot and returns its index, or -1 when every
// slot is taken.
func (l *parkingLot) register(p *parkedProducer) int {
	for i := range l.slots {
		if l.slots[i].Load() == nil && l.slots[i].CompareAndSwap(nil, p) {
			return i
		}
	}
	return -1
}

// leave takes p back out of slot i. If a signaler took it first, its token
// is on the way: leave receives it, so the channel is empty for the next
// park.
func (l *parkingLot) leave(i int, p *parkedProducer) {
	if i >= 0 && !l.slots[i].CompareAndSwap(p, nil) {
		<-p.wake
	}
}

// wakeOne wakes one registered producer and reports whether there was one.
func (l *parkingLot) wakeOne() bool {
	start := l.cursor.Add(1)
	for k := range uint32(parkSlots) {
		i := (start + k) % parkSlots
		if p := l.slots[i].Load(); p != nil && l.slots[i].CompareAndSwap(p, nil) {
			p.wake <- struct{}{}
			return true
		}
	}
	return false
}

// wakeAll wakes every registered producer (the queue closed or a flush
// ended).
func (l *parkingLot) wakeAll() {
	for i := range l.slots {
		if p := l.slots[i].Load(); p != nil && l.slots[i].CompareAndSwap(p, nil) {
			p.wake <- struct{}{}
		}
	}
}
