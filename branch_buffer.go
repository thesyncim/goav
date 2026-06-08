package goav

import (
	"fmt"
	"time"

	"github.com/thesyncim/goav/pipeline"
)

type BranchBufferMode string

const (
	BufferBlocking   BranchBufferMode = "blocking"
	BufferDropOldest BranchBufferMode = "drop_oldest"
	BufferDropNewest BranchBufferMode = "drop_newest"
	BufferLatest     BranchBufferMode = "latest"
	BufferUnbounded  BranchBufferMode = "unbounded"
)

type CopyMode string

const (
	CopyIfMutable CopyMode = "if_mutable"
	CopyAlways    CopyMode = "always"
	CopyNever     CopyMode = "never"
)

type BranchBuffer struct {
	Mode            BranchBufferMode
	Capacity        int
	MaxBytes        int64
	MaxDelay        time.Duration
	CopyMode        CopyMode
	CopyPacketBytes int
	CopyFrameBytes  int
	err             error
}

type BranchBufferOption func(*BranchBuffer)

func Blocking(capacity int, options ...BranchBufferOption) BranchBuffer {
	return newBranchBuffer(BufferBlocking, capacity, options...)
}

func DropOldest(capacity int, options ...BranchBufferOption) BranchBuffer {
	return newBranchBuffer(BufferDropOldest, capacity, options...)
}

func DropNewest(capacity int, options ...BranchBufferOption) BranchBuffer {
	return newBranchBuffer(BufferDropNewest, capacity, options...)
}

func Latest(options ...BranchBufferOption) BranchBuffer {
	return newBranchBuffer(BufferLatest, 1, options...)
}

func Unbounded(options ...BranchBufferOption) BranchBuffer {
	return newBranchBuffer(BufferUnbounded, 0, options...)
}

func BufferMaxBytes(maxBytes int64) BranchBufferOption {
	return func(buffer *BranchBuffer) {
		if buffer != nil {
			buffer.MaxBytes = maxBytes
		}
	}
}

func BufferMaxDelay(maxDelay time.Duration) BranchBufferOption {
	return func(buffer *BranchBuffer) {
		if buffer != nil {
			buffer.MaxDelay = maxDelay
		}
	}
}

func BufferCopyMode(mode CopyMode) BranchBufferOption {
	return func(buffer *BranchBuffer) {
		if buffer != nil {
			buffer.CopyMode = mode
		}
	}
}

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

func (b BranchBuffer) validate(operation string, node string) error {
	if b.err != nil {
		return b.err
	}
	switch b.Mode {
	case "", BufferBlocking, BufferDropOldest, BufferDropNewest, BufferLatest:
	case BufferUnbounded:
		return &BuildError{
			Code:      "branch_buffer_unsupported",
			Operation: operation,
			Node:      firstNonEmpty(node, "branch"),
			Reason:    "unbounded branch buffers are not supported by the runtime yet",
			Suggestions: []string{
				"use goav.Blocking(capacity) for intentional backpressure",
				"use goav.DropOldest(capacity), goav.DropNewest(capacity), or goav.Latest() for realtime branches",
			},
			Cause: ErrUnsupportedBuild,
		}
	default:
		return branchBufferInvalidError(operation, node, "unknown branch buffer mode "+string(b.Mode))
	}
	if b.Mode == BufferLatest {
		b.Capacity = 1
	}
	if b.Capacity < 1 {
		return branchBufferInvalidError(operation, node, "branch buffer capacity must be positive")
	}
	if b.MaxBytes < 0 {
		return branchBufferInvalidError(operation, node, "branch buffer byte limit must be non-negative")
	}
	if b.MaxDelay < 0 {
		return branchBufferInvalidError(operation, node, "branch buffer delay limit must be non-negative")
	}
	if b.CopyPacketBytes < 0 || b.CopyFrameBytes < 0 {
		return branchBufferInvalidError(operation, node, "branch buffer copy bounds must be non-negative")
	}
	switch b.CopyMode {
	case "", CopyIfMutable, CopyAlways, CopyNever:
	default:
		return branchBufferInvalidError(operation, node, "unknown branch buffer copy mode "+string(b.CopyMode))
	}
	if b.CopyMode == CopyNever && (b.CopyPacketBytes > 0 || b.CopyFrameBytes > 0) {
		return branchBufferInvalidError(operation, node, "copy bounds cannot be set when copy mode is never")
	}
	return nil
}

func branchBufferInvalidError(operation string, node string, reason string) error {
	return &BuildError{
		Code:      "branch_buffer_invalid",
		Operation: operation,
		Node:      firstNonEmpty(node, "branch"),
		Reason:    reason,
		Suggestions: []string{
			"use goav.Blocking(capacity) when slow branches should apply backpressure",
			"use goav.DropOldest(capacity), goav.DropNewest(capacity), or goav.Latest() for realtime diagnostics and previews",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func (b BranchBuffer) pipelinePolicy() pipeline.BufferPolicy {
	if b.Mode == BufferLatest {
		b.Capacity = 1
	}
	policy := pipeline.BufferPolicy{
		Capacity:        b.Capacity,
		TargetLatency:   b.MaxDelay,
		MaxLatency:      b.MaxDelay,
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
