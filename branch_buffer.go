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
			Phase:     phaseBuild,
			Family:    errcode.FamilyForCode(branchBufferUnsupportedCode),
			Code:      branchBufferUnsupportedCode,
			Operation: operation,
			Node:      firstNonEmpty(node, "branch"),
			Reason:    "unbounded branch buffers are not supported by the runtime yet",
			fixes: buildErrorFixes([]string{
				"use flow.Blocking(capacity) for intentional backpressure",
				"use flow.DropOldest(capacity), flow.DropNewest(capacity), or flow.Latest() for realtime branches",
			}),
			cause: errUnsupportedBuild,
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
	return nil
}

func branchBufferInvalidError(operation string, node string, reason string) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(branchBufferInvalidCode),
		Code:      branchBufferInvalidCode,
		Operation: operation,
		Node:      firstNonEmpty(node, "branch"),
		Reason:    reason,
		fixes: buildErrorFixes([]string{
			"use flow.Blocking(capacity) when slow branches should apply backpressure",
			"use flow.DropOldest(capacity), flow.DropNewest(capacity), or flow.Latest() for realtime diagnostics and previews",
		}),
		cause: errUnsupportedBuild,
	}
}

// promotedGraphBufferCapacity is the trunk queue depth of a graph that runs
// buffered only because a branch declared .Buffer(...) or its encoders run in
// parallel: deep enough to keep each branch's worker busy, shallow enough to
// bound the latency the queue adds.
const promotedGraphBufferCapacity = 4
