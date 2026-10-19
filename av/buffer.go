package av

import (
	"encoding/json"
	"maps"
	"math/bits"
	"slices"
	"sync"
	"sync/atomic"
)

// Buffer is one byte payload: Bytes, plus the reference count behind them
// when they came from a pool.
//
// Payloads follow the rule GStreamer and libav use: bytes published on a
// graph are immutable. A Buffer without an Owner is ordinary memory its
// producer never writes again once published, so anyone may keep it. A
// Buffer with an Owner is pooled (NewBuffer) and shared by reference: a
// message lends it to each handler for the call, Retain keeps it past that
// call, Release returns the reference, and MakeWritable copies it before a
// write when anyone else holds it. Every branch of a fanout reads the same
// bytes; none is copied per edge.
type Buffer struct {
	Bytes []byte
	Owner BufferOwner
}

// BufferOwner is the reference count behind a pooled Buffer: Retain adds a
// reference, Release drops one and recycles the bytes once the last is gone,
// and Refs reports how many are live. Methods are safe for concurrent use.
// NewBuffer's pool is one implementation; an adapter with memory of its own
// (a decoder's reference pictures, a mapped file) can supply another.
type BufferOwner interface {
	Retain()
	Release()
	Refs() int
}

// NewBuffer returns an n-byte pooled Buffer holding one reference, which the
// caller owns: write it, publish it, and Release it when done. Its bytes are
// not zeroed. The pool is process-wide and backed by the garbage collector,
// so a reference that is never released costs a reuse, never a leak.
func NewBuffer(n int) Buffer {
	if n <= 0 {
		return Buffer{}
	}
	block := takeBlock(n)
	return Buffer{Bytes: block.data[:n:n], Owner: block}
}

// Retain adds a reference for a copy of b the caller keeps past the call that
// lent it. Bytes without an Owner need none: Retain is a no-op for them.
func (b Buffer) Retain() {
	if b.Owner != nil {
		b.Owner.Retain()
	}
}

// Release drops the caller's reference and clears b. Releasing bytes without
// an Owner just clears b.
func (b *Buffer) Release() {
	if b.Owner != nil {
		b.Owner.Release()
	}
	*b = Buffer{}
}

// MakeWritable readies b for writing: unless b holds the only reference to
// pooled bytes, it copies them into a new pooled Buffer and releases b's old
// reference, so the write reaches no one else. b must be a reference the
// caller holds (its own Retain, NewBuffer, or Frame.Ref), never bytes a
// message lent it.
func (b *Buffer) MakeWritable() {
	if b.Owner != nil && b.Owner.Refs() == 1 {
		return
	}
	if len(b.Bytes) == 0 {
		b.Release()
		return
	}
	next := NewBuffer(len(b.Bytes))
	copy(next.Bytes, b.Bytes)
	b.Release()
	*b = next
}

// Reset clears b for reuse without releasing it. Bytes without an Owner keep
// their backing array as scratch capacity; pooled bytes are dropped, because
// once released the pool may hand them to someone else.
func (b *Buffer) Reset() {
	if b.Owner != nil {
		*b = Buffer{}
		return
	}
	b.Bytes = b.Bytes[:0]
}

// MarshalJSON writes the bytes, base64-encoded like a []byte; the owner is a
// runtime fact and stays out of the JSON.
func (b Buffer) MarshalJSON() ([]byte, error) { return json.Marshal(b.Bytes) }

// UnmarshalJSON reads bytes MarshalJSON wrote into ordinary memory.
func (b *Buffer) UnmarshalJSON(data []byte) error {
	var bytes []byte
	if err := json.Unmarshal(data, &bytes); err != nil {
		return err
	}
	*b = Buffer{Bytes: bytes}
	return nil
}

// The pool: power-of-two size classes from 64 B to 128 MiB, one sync.Pool
// each. Larger buffers are allocated exactly and left to the garbage
// collector when released.
const (
	minBlockShift = 6
	maxBlockShift = 27
)

var blockPools [maxBlockShift - minBlockShift + 1]sync.Pool

// block is one pooled allocation and the reference count over it.
type block struct {
	refs  atomic.Int32
	class int8 // index into blockPools, or -1 for an unpooled size
	data  []byte
}

func takeBlock(n int) *block {
	class := blockClass(n)
	var b *block
	if class < 0 {
		b = &block{class: -1, data: make([]byte, n)}
	} else if pooled, _ := blockPools[class].Get().(*block); pooled != nil {
		b = pooled
	} else {
		b = &block{class: int8(class), data: make([]byte, 1<<(class+minBlockShift))}
	}
	b.refs.Store(1)
	return b
}

func blockClass(n int) int {
	shift := max(bits.Len(uint(n-1)), minBlockShift)
	if shift > maxBlockShift {
		return -1
	}
	return shift - minBlockShift
}

// Retain adds a reference. Retaining a buffer whose last reference is gone is
// a use after release and panics.
func (b *block) Retain() {
	if b.refs.Add(1) <= 1 {
		b.refs.Add(-1)
		panic("av: Retain of a released Buffer")
	}
}

// Release drops a reference; the last one returns the bytes to the pool.
// Releasing more often than retained panics.
func (b *block) Release() {
	switch refs := b.refs.Add(-1); {
	case refs > 0:
	case refs == 0:
		recycle(b)
	default:
		panic("av: Buffer released more often than retained")
	}
}

// Refs reports the live references.
func (b *block) Refs() int { return int(b.refs.Load()) }

// putBlock returns a released block to its size class.
func putBlock(b *block) {
	if b.class >= 0 {
		blockPools[b.class].Put(b)
	}
}

// Ref returns a new packet header holding its own reference to p's payload,
// for keeping p past the call that lent it; the Metadata map is copied.
// Release it when done.
func (p *Packet) Ref() *Packet {
	ref := *p
	ref.Payload.Retain()
	ref.Metadata = maps.Clone(p.Metadata)
	return &ref
}

// Release drops the reference p holds to its payload and clears it.
func (p *Packet) Release() { p.Payload.Release() }

// Ref returns a new frame header holding its own references to f's payload,
// for keeping f past the call that lent it: the planes, the Video or Audio
// header, and the Metadata map are copied, and each plane buffer is retained;
// the bytes themselves are shared, not copied. Release it when done.
func (f *Frame) Ref() *Frame {
	ref := *f
	ref.Planes = slices.Clone(f.Planes)
	for i := range ref.Planes {
		ref.Planes[i].Buffer.Retain()
	}
	if f.Video != nil {
		video := *f.Video
		ref.Video = &video
	}
	if f.Audio != nil {
		audio := *f.Audio
		ref.Audio = &audio
	}
	ref.Metadata = maps.Clone(f.Metadata)
	return &ref
}

// Release drops the references f holds to its planes and clears their
// buffers. Each plane holds its own reference, even when planes share one
// allocation.
func (f *Frame) Release() {
	for i := range f.Planes {
		f.Planes[i].Buffer.Release()
	}
}

// MakeWritable readies every plane for writing (see Buffer.MakeWritable). f
// must hold its references: a header from Ref, or one its producer filled.
func (f *Frame) MakeWritable() {
	for i := range f.Planes {
		f.Planes[i].Buffer.MakeWritable()
	}
}
