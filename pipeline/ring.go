package pipeline

import (
	"runtime"
	"sync/atomic"
)

// cacheLine pads write-hot words apart. 128 bytes covers the 128-byte lines
// of Apple silicon and the adjacent-line prefetch of x86.
const cacheLine = 128

// The tail word of a ring carries two flags above the position: closed (no
// push will ever succeed again) and locked (pushes wait while the holder
// rewrites the queue). Both are set and cleared with one atomic operation,
// and a push claims its position with a CAS on the whole word, so a flag set
// between a producer's load and its CAS makes the CAS fail.
const (
	ringClosed = uint64(1) << 63
	ringLocked = uint64(1) << 62
	ringPos    = ringLocked - 1
)

type ringStatus uint8

const (
	ringOK ringStatus = iota
	ringFull
	ringIsClosed
	ringIsLocked
)

type ringCell[T any] struct {
	// seq says whose turn the cell is: seq == 2*pos means free for the push
	// at pos, seq == 2*pos+1 means it holds the value pushed at pos, ready for
	// the pop at pos, which hands the cell to the next lap with 2*(pos+size).
	// Doubling keeps the two states apart even for a one-cell ring, where
	// Vyukov's pos/pos+1 numbering would read a full cell as free.
	seq atomic.Uint64
	// val is written by the push that claimed the cell before it publishes
	// seq, and read by the pop that claimed it after it observed seq, so the
	// seq store/load pair orders it.
	val *T
}

// ring is a bounded, lock-free, multi-producer multi-consumer FIFO of
// pointers: Dmitry Vyukov's sequence-numbered cell ring. A push claims the
// next position with one CAS on the tail and publishes it with one store to
// its cell; a pop claims the oldest with one CAS on the head and hands the
// cell back with one store. Producers and consumers touch the same cache
// line only at the cell they meet on, never a shared lock or a count.
//
// The buffered runner keeps two per node: the queue (its worker pops, a
// DropOldest producer pops to shed, a flush pops to drain) and the free list
// of the node's preallocated slots, so a steady queue moves messages without
// allocating. The ring's size is exact, not rounded, so the queue bound is
// the configured capacity.
type ring[T any] struct {
	_     [cacheLine]byte
	tail  atomic.Uint64
	_     [cacheLine - 8]byte
	head  atomic.Uint64
	_     [cacheLine - 8]byte
	cells []ringCell[T]
	size  uint64
	mask  uint64 // size-1, used when pow2
	pow2  bool   // size is a power of two: index by mask, not %
	_     [cacheLine]byte
}

// reset empties the ring and sizes it to hold n values; n < 1 leaves it
// closed. It runs on cold paths with no push or pop in flight.
func (r *ring[T]) reset(n int) {
	if n < 1 {
		r.cells, r.size, r.mask, r.pow2 = nil, 0, 0, false
		r.head.Store(0)
		r.tail.Store(ringClosed)
		return
	}
	r.cells = make([]ringCell[T], n)
	for i := range r.cells {
		r.cells[i].seq.Store(2 * uint64(i))
	}
	r.size = uint64(n)
	r.mask = r.size - 1
	r.pow2 = n&(n-1) == 0
	r.head.Store(0)
	r.tail.Store(0)
}

func (r *ring[T]) cell(pos uint64) *ringCell[T] {
	if r.pow2 {
		return &r.cells[pos&r.mask]
	}
	return &r.cells[pos%r.size]
}

// push appends v. It reports ringFull when the oldest value has not been
// popped yet, ringIsClosed after close, and ringIsLocked while the ring is
// locked, unless owner says the caller holds that lock.
func (r *ring[T]) push(v *T, owner bool) ringStatus {
	for {
		t := r.tail.Load()
		if t&ringClosed != 0 {
			return ringIsClosed
		}
		if t&ringLocked != 0 && !owner {
			return ringIsLocked
		}
		pos := t & ringPos
		c := r.cell(pos)
		seq := c.seq.Load()
		switch {
		case seq == 2*pos:
			if r.tail.CompareAndSwap(t, t+1) {
				c.val = v
				c.seq.Store(2*pos + 1)
				return ringOK
			}
		case int64(seq-2*pos) < 0:
			if r.tail.Load() == t {
				return ringFull
			}
		}
		// Another push claimed pos first, or a flag changed: reload.
	}
}

// pop removes the oldest value, or returns nil when the ring is empty or its
// oldest position is claimed but not yet published.
func (r *ring[T]) pop() *T {
	if r.size == 0 {
		return nil
	}
	for {
		h := r.head.Load()
		c := r.cell(h)
		seq := c.seq.Load()
		switch {
		case seq == 2*h+1:
			if r.head.CompareAndSwap(h, h+1) {
				v := c.val
				c.val = nil
				c.seq.Store(2 * (h + r.size))
				return v
			}
		case int64(seq-(2*h+1)) < 0:
			return nil
		}
		// Another pop took h first: reload.
	}
}

// popWait pops like pop but waits out a push that claimed the oldest position
// and has not published it yet, so it reports nil only when nothing is
// claimed: a free list read as empty while a slot is mid-return would be a
// spurious backpressure.
func (r *ring[T]) popWait() *T {
	for {
		if v := r.pop(); v != nil {
			return v
		}
		if r.size == 0 || r.drained() {
			return nil
		}
		runtime.Gosched()
	}
}

// pushWait pushes into a ring that cannot be full for long: the free list,
// which has a cell for every slot, can only look full for the instant a pop
// of the cell it needs is between its claim and its handback.
func (r *ring[T]) pushWait(v *T) {
	for r.push(v, false) == ringFull {
		runtime.Gosched()
	}
}

// fullAt reports whether a push at tail word t would find the ring full.
func (r *ring[T]) fullAt(t uint64) bool {
	if r.size == 0 {
		return true
	}
	pos := t & ringPos
	return int64(r.cell(pos).seq.Load()-2*pos) < 0
}

// ready reports whether a pop would find a published value.
func (r *ring[T]) ready() bool {
	if r.size == 0 {
		return false
	}
	h := r.head.Load()
	return r.cell(h).seq.Load() == 2*h+1
}

// drained reports whether every claimed position has been popped.
func (r *ring[T]) drained() bool {
	t := r.tail.Load() & ringPos
	return r.head.Load() == t
}

func (r *ring[T]) closed() bool { return r.tail.Load()&ringClosed != 0 }

// close makes every later push report ringIsClosed; it reports whether this
// call closed it. Values already pushed stay poppable. It waits out a lock
// holder, so a flush never finds its queue closed halfway through.
func (r *ring[T]) close() bool {
	for {
		t := r.tail.Load()
		if t&ringClosed != 0 {
			return false
		}
		if t&ringLocked != 0 {
			runtime.Gosched()
			continue
		}
		if r.tail.CompareAndSwap(t, t|ringClosed) {
			return true
		}
	}
}

// lock makes later pushes report ringIsLocked until unlock, so the holder
// can drain and refill the ring as one step. It waits out another holder
// (locking is a cold path) and reports false once the ring is closed.
func (r *ring[T]) lock() bool {
	for {
		t := r.tail.Load()
		if t&ringClosed != 0 {
			return false
		}
		if t&ringLocked == 0 && r.tail.CompareAndSwap(t, t|ringLocked) {
			return true
		}
		runtime.Gosched()
	}
}

func (r *ring[T]) unlock() { r.tail.And(^ringLocked) }
