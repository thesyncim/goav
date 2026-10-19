package compile

import (
	"cmp"

	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/internal/builderr"
)

const RealtimeRecipeBufferCapacity = 32

// ValidateBranchBuffer rejects branch buffers the runtime cannot install, in
// terms of the goav BuildError vocabulary. The flow package owns the buffer
// data and lowering; goav owns the build-time diagnostics that reference its
// own error types.
func ValidateBranchBuffer(b flow.BranchBuffer, operation string, node string) error {
	switch b.Mode {
	case "", flow.BufferBlocking, flow.BufferDropOldest, flow.BufferDropNewest, flow.BufferLatest:
	case flow.BufferUnbounded:
		return &builderr.Refusal{
			Phase:     builderr.PhaseBuild,
			Family:    errcode.FamilyForCode(builderr.BranchBufferUnsupportedCode),
			Code:      builderr.BranchBufferUnsupportedCode,
			Operation: operation,
			Node:      cmp.Or(node, "branch"),
			Reason:    "unbounded branch buffers are not supported by the runtime yet",
			Fixes: builderr.Fixes([]string{
				"use flow.Blocking(capacity) for intentional backpressure",
				"use flow.DropOldest(capacity), flow.DropNewest(capacity), or flow.Latest() for realtime branches",
			}),
			Cause: builderr.ErrUnsupportedBuild,
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
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.BranchBufferInvalidCode),
		Code:      builderr.BranchBufferInvalidCode,
		Operation: operation,
		Node:      cmp.Or(node, "branch"),
		Reason:    reason,
		Fixes: builderr.Fixes([]string{
			"use flow.Blocking(capacity) when slow branches should apply backpressure",
			"use flow.DropOldest(capacity), flow.DropNewest(capacity), or flow.Latest() for realtime diagnostics and previews",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}
