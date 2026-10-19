package pipeline

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// TestRingCapacityIsExact pins the bound the queue policy relies on: a ring
// of n cells holds exactly n values for every n, including one (where
// Vyukov's original numbering would read a full cell as free), and pops
// them in order.
func TestRingCapacityIsExact(t *testing.T) {
	for _, n := range []int{1, 2, 3, 4, 5, 7, 8, 64, 100} {
		var r ring[int]
		r.reset(n)
		values := make([]int, n+1)
		for lap := range 3 {
			for i := range n {
				values[i] = lap*1000 + i
				if got := r.push(&values[i], false); got != ringOK {
					t.Fatalf("n=%d lap %d push %d = %v, want ok", n, lap, i, got)
				}
			}
			if !r.fullAt(r.tail.Load()) {
				t.Fatalf("n=%d lap %d: not full after %d pushes", n, lap, n)
			}
			if got := r.push(&values[n], false); got != ringFull {
				t.Fatalf("n=%d lap %d push past capacity = %v, want full", n, lap, got)
			}
			for i := range n {
				v := r.pop()
				if v == nil || *v != lap*1000+i {
					t.Fatalf("n=%d lap %d pop %d = %v, want %d", n, lap, i, v, lap*1000+i)
				}
			}
			if v := r.pop(); v != nil {
				t.Fatalf("n=%d lap %d pop from empty = %d", n, lap, *v)
			}
			if !r.drained() {
				t.Fatalf("n=%d lap %d: not drained", n, lap)
			}
		}
	}
}

// TestRingCloseAndLock pins the tail flags: a closed ring refuses pushes but
// keeps what it holds; a locked ring refuses pushes from everyone but the
// holder; close waits out a lock holder.
func TestRingCloseAndLock(t *testing.T) {
	var r ring[int]
	r.reset(2)
	a, b, c := 1, 2, 3
	if r.push(&a, false) != ringOK {
		t.Fatal("push a")
	}
	if !r.lock() {
		t.Fatal("lock refused on an open ring")
	}
	if got := r.push(&b, false); got != ringIsLocked {
		t.Fatalf("push while locked = %v, want locked", got)
	}
	if got := r.push(&b, true); got != ringOK {
		t.Fatalf("holder push while locked = %v, want ok", got)
	}
	closed := make(chan bool)
	go func() { closed <- r.close() }()
	select {
	case <-closed:
		t.Fatal("close did not wait for the lock holder")
	default:
	}
	r.unlock()
	if !<-closed {
		t.Fatal("close reported the ring already closed")
	}
	if got := r.push(&c, false); got != ringIsClosed {
		t.Fatalf("push after close = %v, want closed", got)
	}
	if r.close() {
		t.Fatal("second close reported closing")
	}
	if r.lock() {
		t.Fatal("lock succeeded on a closed ring")
	}
	if v := r.pop(); v == nil || *v != 1 {
		t.Fatalf("pop after close = %v, want 1", v)
	}
	if v := r.pop(); v == nil || *v != 2 {
		t.Fatalf("pop after close = %v, want 2", v)
	}
	if !r.drained() || !r.closed() {
		t.Fatal("closed ring not drained after popping everything")
	}
	var zero ring[int]
	zero.reset(0)
	if zero.push(&a, false) != ringIsClosed || zero.pop() != nil || zero.popWait() != nil {
		t.Fatal("a zero-size ring must be closed and empty")
	}
}

type ringItem struct {
	producer int
	seq      int
}

// TestRingConcurrentExactlyOnce runs producers and consumers on one small
// ring at once: every value comes out exactly once, and with one consumer
// each producer's values come out in the order it pushed them.
func TestRingConcurrentExactlyOnce(t *testing.T) {
	const producers, perProducer = 8, 20_000
	for _, tc := range []struct {
		name      string
		size      int
		consumers int
	}{
		{"one-cell/1-consumer", 1, 1},
		{"pow2/1-consumer", 8, 1},
		{"odd/1-consumer", 5, 1},
		{"pow2/4-consumers", 8, 4},
		{"odd/4-consumers", 3, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var r ring[ringItem]
			r.reset(tc.size)
			items := make([][]ringItem, producers)
			for p := range items {
				items[p] = make([]ringItem, perProducer)
				for s := range items[p] {
					items[p][s] = ringItem{producer: p, seq: s}
				}
			}
			seen := make([]atomic.Int32, producers*perProducer)
			var popped atomic.Int64
			var outOfOrder atomic.Int64
			var wg sync.WaitGroup
			for c := 0; c < tc.consumers; c++ {
				wg.Go(func() {
					last := make([]int, producers)
					for i := range last {
						last[i] = -1
					}
					for popped.Load() < producers*perProducer {
						v := r.pop()
						if v == nil {
							runtime.Gosched()
							continue
						}
						seen[v.producer*perProducer+v.seq].Add(1)
						if v.seq <= last[v.producer] {
							outOfOrder.Add(1)
						}
						last[v.producer] = v.seq
						popped.Add(1)
					}
				})
			}
			for p := range producers {
				wg.Go(func() {
					for s := range items[p] {
						for r.push(&items[p][s], false) != ringOK {
							runtime.Gosched()
						}
					}
				})
			}
			wg.Wait()
			for i := range seen {
				if n := seen[i].Load(); n != 1 {
					t.Fatalf("producer %d seq %d popped %d times, want once", i/perProducer, i%perProducer, n)
				}
			}
			if tc.consumers == 1 && outOfOrder.Load() != 0 {
				t.Fatalf("%d values out of producer order with one consumer", outOfOrder.Load())
			}
		})
	}
}

// TestWaiterQueueConcurrentExactlyOnce pins the direct runner's waiter queue:
// many producers push records while one consumer pops; every record comes
// out exactly once and in each producer's order, and records are reusable as
// soon as they are popped.
func TestWaiterQueueConcurrentExactlyOnce(t *testing.T) {
	const producers, perProducer = 8, 20_000
	msgs := make([]Message, producers*perProducer)
	index := make(map[*Message]int, len(msgs))
	for i := range msgs {
		index[&msgs[i]] = i
	}
	var q waiterQueue
	q.init()
	var popped atomic.Int64
	next := make([]int, producers)
	var wg sync.WaitGroup
	wg.Go(func() {
		for popped.Load() < producers*perProducer {
			w := q.pop()
			if w == nil {
				runtime.Gosched()
				continue
			}
			i := index[w.msg]
			p, s := i/perProducer, i%perProducer
			if s != next[p] {
				t.Errorf("producer %d: popped record %d, want %d", p, s, next[p])
			}
			next[p] = s + 1
			popped.Add(1)
			w.complete(waiterDone)
		}
	})
	for p := range producers {
		wg.Go(func() {
			w := newDirectWaiter()
			for s := range perProducer {
				w.msg = &msgs[p*perProducer+s]
				w.state.Store(waiterPending)
				q.push(w)
				if got := w.wait(); got != waiterDone {
					t.Errorf("producer %d record %d completed as %d", p, s, got)
				}
			}
		})
	}
	wg.Wait()
	for p, n := range next {
		if n != perProducer {
			t.Fatalf("producer %d: %d records popped, want %d", p, n, perProducer)
		}
	}
}
