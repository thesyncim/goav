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
	BufferBlocking   BranchBufferMode = "blocking"
	BufferDropOldest BranchBufferMode = "drop_oldest"
	BufferDropNewest BranchBufferMode = "drop_newest"
	BufferLatest     BranchBufferMode = "latest"
	BufferUnbounded  BranchBufferMode = "unbounded"
)

// CopyMode controls whether a branch buffer copies messages before queueing.
type CopyMode string

const (
	CopyIfMutable CopyMode = "if_mutable"
	CopyAlways    CopyMode = "always"
	CopyNever     CopyMode = "never"
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
func BufferCopyMode(mode CopyMode) BranchBufferOption {
	return func(buffer *BranchBuffer) {
		if buffer != nil {
			buffer.CopyMode = mode
		}
	}
}

// BufferCopyBounds bounds the per-message copy sizes for packets and frames.
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

func (b BranchBuffer) String() string {
	mode := b.Mode
	if mode == "" {
		mode = BufferBlocking
	}
	return fmt.Sprintf("%s(capacity=%d)", mode, b.Capacity)
}

// DropReason keys TaskStats.DropReasons / NodeStats.DropReasons: why the runtime
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
