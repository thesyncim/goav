// Package flow holds the branch buffer and flow-control vocabulary used to
// describe how a goav branch queues, drops, or backpressures messages. It keeps
// the goav root grammar clean while letting callers reach for buffer policies as
// flow.Blocking, flow.DropOldest, and friends.
package flow

import (
	"fmt"
	"time"

	"github.com/thesyncim/goav/pipeline"
)

// BranchBufferMode names the queueing discipline a branch buffer applies.
type BranchBufferMode string

const (
	// BufferBlocking preserves every message: a full queue paces the producer.
	BufferBlocking BranchBufferMode = "blocking"
	// BufferDropOldest sheds the oldest queued message to admit the new one.
	BufferDropOldest BranchBufferMode = "drop_oldest"
	// BufferDropNewest sheds the incoming message when the queue is full.
	BufferDropNewest BranchBufferMode = "drop_newest"
	// BufferLatest keeps only the most recent message.
	BufferLatest BranchBufferMode = "latest"
	// BufferUnbounded queues without limit; memory is the only bound.
	BufferUnbounded BranchBufferMode = "unbounded"
)

// CopyMode declares when a branch buffer copies a queued message's payload
// bytes into branch-owned backing before queueing. This is the ownership
// contract for buffered fanout: every buffered branch binds messages into its
// own preallocated slots, so owned mutable payloads are copied into branch-local
// backing — a consumer that mutates those delivered bytes can never corrupt the
// producer's bytes or a sibling branch's view. Borrowed packet fanout may copy
// once into graph-owned backing and share refcounted read-only views across
// subscribers. Payloads that are shared by reference instead of copied must
// never be written by any consumer.
//
// Copying is bounded, never allocated per message: BufferCopyBounds sizes the
// per-slot backing, and a payload that needs a copy but cannot get one (bounds
// unset or too small) is refused with pipeline.ErrBufferedMessageUnsafe or
// pipeline.ErrMessageTooLarge rather than silently shared.
type CopyMode string

const (
	// CopyIfMutable (the default) copies every payload not declared
	// av.BufferImmutable and shares immutable payloads by reference. Mutable
	// owned or undeclared mutable payloads are either copied into branch-owned
	// backing or refused. Borrowed packet fanout may share one graph-owned copy
	// across read-only subscriber slots. A branch that mutates its delivered
	// owned frame or packet cannot corrupt a sibling.
	CopyIfMutable CopyMode = "if_mutable"
	// CopyAlways copies every payload, including ones declared
	// av.BufferImmutable: defensive isolation for producers whose immutable
	// declaration is not trusted. BufferCopyBounds must cover every payload;
	// without bounds nothing can be copied, so every non-empty payload is
	// refused with pipeline.ErrBufferedMessageUnsafe.
	CopyAlways CopyMode = "always"
	// CopyNever shares every payload by reference and never copies. It is
	// safe-only: combining it with BufferCopyBounds is a build error, and only
	// payloads declared av.BufferImmutable are admitted — a mutable payload is
	// refused with pipeline.ErrBufferedMessageUnsafe at delivery time. By
	// choosing CopyNever the caller declares that every consumer on the branch
	// treats shared payloads as read-only; that declaration itself is not
	// enforceable at runtime.
	CopyNever CopyMode = "never"
)

// BranchBuffer describes how a branch queues, copies, drops, or backpressures
// messages. Build one with Blocking, DropOldest, DropNewest, Latest, or
// Unbounded, then tune it with BranchBufferOption values.
type BranchBuffer struct {
	Mode            BranchBufferMode
	Capacity        int
	MaxBytes        int64
	MaxDelay        time.Duration
	CopyMode        CopyMode
	CopyPacketBytes int
	CopyFrameBytes  int
}

// BranchBufferOption tunes a BranchBuffer produced by one of the constructors.
type BranchBufferOption func(*BranchBuffer)

// Blocking applies backpressure to the producer when the branch queue is full.
func Blocking(capacity int, options ...BranchBufferOption) BranchBuffer {
	return newBranchBuffer(BufferBlocking, capacity, options...)
}

// DropOldest sheds the queued head to admit a newer message when full.
func DropOldest(capacity int, options ...BranchBufferOption) BranchBuffer {
	return newBranchBuffer(BufferDropOldest, capacity, options...)
}

// DropNewest refuses the incoming message and keeps the queue when full.
func DropNewest(capacity int, options ...BranchBufferOption) BranchBuffer {
	return newBranchBuffer(BufferDropNewest, capacity, options...)
}

// Latest keeps only the most recent message (capacity one, drop oldest).
func Latest(options ...BranchBufferOption) BranchBuffer {
	return newBranchBuffer(BufferLatest, 1, options...)
}

// Unbounded queues without a capacity limit.
func Unbounded(options ...BranchBufferOption) BranchBuffer {
	return newBranchBuffer(BufferUnbounded, 0, options...)
}

// BufferMaxBytes caps the total bytes a branch buffer may hold.
func BufferMaxBytes(maxBytes int64) BranchBufferOption {
	return func(buffer *BranchBuffer) {
		if buffer != nil {
			buffer.MaxBytes = maxBytes
		}
	}
}

// BufferMaxDelay caps how stale a queued message may be before it is shed.
func BufferMaxDelay(maxDelay time.Duration) BranchBufferOption {
	return func(buffer *BranchBuffer) {
		if buffer != nil {
			buffer.MaxDelay = maxDelay
		}
	}
}

// BufferCopyMode sets when the branch buffer copies messages before queueing.
// The default is CopyIfMutable; see CopyMode for the ownership contract each
// mode guarantees.
func BufferCopyMode(mode CopyMode) BranchBufferOption {
	return func(buffer *BranchBuffer) {
		if buffer != nil {
			buffer.CopyMode = mode
		}
	}
}

// BufferCopyBounds bounds the per-message copy sizes for packets and frames:
// each branch slot preallocates packetBytes of packet backing and frameBytes
// of frame-plane backing, so copies never allocate on the hot path. A payload
// that needs a copy (see CopyMode) but exceeds its bound is refused with
// pipeline.ErrMessageTooLarge; with a zero bound it is refused with
// pipeline.ErrBufferedMessageUnsafe. Bounds cannot be combined with CopyNever.
func BufferCopyBounds(packetBytes int, frameBytes int) BranchBufferOption {
	return func(buffer *BranchBuffer) {
		if buffer == nil {
			return
		}
		buffer.CopyPacketBytes = packetBytes
		buffer.CopyFrameBytes = frameBytes
	}
}

func newBranchBuffer(mode BranchBufferMode, capacity int, options ...BranchBufferOption) BranchBuffer {
	buffer := BranchBuffer{
		Mode:     mode,
		Capacity: capacity,
		CopyMode: CopyIfMutable,
	}
	for i := range options {
		if options[i] != nil {
			options[i](&buffer)
		}
	}
	return buffer
}

// PipelinePolicy lowers a BranchBuffer to the pipeline.BufferPolicy the runtime
// installs on the branch queue.
func (b BranchBuffer) PipelinePolicy() pipeline.BufferPolicy {
	if b.Mode == BufferLatest {
		b.Capacity = 1
	}
	policy := pipeline.BufferPolicy{
		Capacity:        b.Capacity,
		MaxLatency:      b.MaxDelay,
		MaxBytes:        b.MaxBytes,
		CopyPacketBytes: b.CopyPacketBytes,
		CopyFrameBytes:  b.CopyFrameBytes,
		CopyAlways:      b.CopyMode == CopyAlways,
	}
	switch b.Mode {
	case BufferBlocking:
		policy.Drop = pipeline.DropBlock
	case BufferDropOldest, BufferLatest:
		policy.Drop = pipeline.DropOldest
	case BufferDropNewest:
		policy.Drop = pipeline.DropNewest
	default:
		policy.Drop = pipeline.DropNever
	}
	return policy
}

// String renders the buffer declaration (mode plus capacity) for plans and
// diagnostics.
func (b BranchBuffer) String() string {
	mode := b.Mode
	if mode == "" {
		mode = BufferBlocking
	}
	return fmt.Sprintf("%s(capacity=%d)", mode, b.Capacity)
}

// DropReason keys GraphStats.DropReasons / NodeStats.DropReasons: why the runtime
// shed a message. Use the DropReason* constants to read those maps without
// importing the pipeline package.
type DropReason = pipeline.DropPolicy

const (
	// DropReasonOldest: a DropOldest branch dropped the queued head to admit a newer message.
	DropReasonOldest DropReason = pipeline.DropOldest
	// DropReasonNewest: a DropNewest branch refused the incoming message and kept the queue.
	DropReasonNewest DropReason = pipeline.DropNewest
	// DropReasonStale: a message older than the branch BufferMaxDelay was shed.
	DropReasonStale DropReason = pipeline.DropStale
	// DropReasonOverflow: a message that would exceed the branch BufferMaxBytes budget was shed.
	DropReasonOverflow DropReason = pipeline.DropOverflow
	// DropReasonUntilSync: messages dropped until the next keyframe/sync point.
	DropReasonUntilSync DropReason = pipeline.DropUntilSync
	// DropReasonNonKeyVideo: a non-keyframe video message was dropped.
	DropReasonNonKeyVideo DropReason = pipeline.DropNonKeyVideo
)
