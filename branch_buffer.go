package goav

import (
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/flow"
)

// validateBranchBuffer rejects branch buffers the runtime cannot install, in
// terms of the goav BuildError vocabulary. The flow package owns the buffer
// data and lowering; goav owns the build-time diagnostics that reference its
// own error types.
func validateBranchBuffer(b flow.BranchBuffer, operation string, node string) error {
	switch b.Mode {
	case "", flow.BufferBlocking, flow.BufferDropOldest, flow.BufferDropNewest, flow.BufferLatest:
	case flow.BufferUnbounded:
		return &BuildError{
			Family:    errcode.FamilyForCode(errcode.BranchBufferUnsupported),
			Code:      errcode.BranchBufferUnsupported,
			Operation: operation,
			Node:      firstNonEmpty(node, "branch"),
			Reason:    "unbounded branch buffers are not supported by the runtime yet",
			Suggestions: []string{
				"use flow.Blocking(capacity) for intentional backpressure",
				"use flow.DropOldest(capacity), flow.DropNewest(capacity), or flow.Latest() for realtime branches",
			},
			Cause: ErrUnsupportedBuild,
		}
	default:
		return branchBufferInvalidError(operation, node, "unknown branch buffer mode "+string(b.Mode))
	}
	capacity := b.Capacity
	if b.Mode == flow.BufferLatest {
		capacity = 1
	}
	if capacity < 1 {
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
	case "", flow.CopyIfMutable, flow.CopyAlways, flow.CopyNever:
	default:
		return branchBufferInvalidError(operation, node, "unknown branch buffer copy mode "+string(b.CopyMode))
	}
	if b.CopyMode == flow.CopyNever && (b.CopyPacketBytes > 0 || b.CopyFrameBytes > 0) {
		return branchBufferInvalidError(operation, node, "copy bounds cannot be set when copy mode is never")
	}
	return nil
}

func branchBufferInvalidError(operation string, node string, reason string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.BranchBufferInvalid),
		Code:      errcode.BranchBufferInvalid,
		Operation: operation,
		Node:      firstNonEmpty(node, "branch"),
		Reason:    reason,
		Suggestions: []string{
			"use flow.Blocking(capacity) when slow branches should apply backpressure",
			"use flow.DropOldest(capacity), flow.DropNewest(capacity), or flow.Latest() for realtime diagnostics and previews",
		},
		Cause: ErrUnsupportedBuild,
	}
}
