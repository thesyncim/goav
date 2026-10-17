package goav

import (
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/pipeline"
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
// buffered only because a branch declared .Buffer(...): deep enough to keep
// each branch's worker busy, small enough that the per-slot copy backing
// stays modest.
const promotedGraphBufferCapacity = 4

// branchComposeBufferPolicy lowers a planned branch's declared buffer to the
// node policy its exclusively owned nodes run behind. Copy bounds the caller
// left unset are sized from the branch's own shapes, as Attach does, so a
// buffered branch carries mutable decoded frames without a redundant
// flow.BufferCopyBounds; CopyNever keeps zero bounds, which is its contract.
// An undeclared buffer stays direct and inherits the graph's policy.
func branchComposeBufferPolicy(buffer flow.BranchBuffer, operations []workOperation) (pipeline.BufferPolicy, error) {
	policy := buffer.PipelinePolicy()
	if policy.IsDirect() || buffer.CopyMode == flow.CopyNever {
		return policy, nil
	}
	return bufferPolicyWithShapeBudgetsForOperations(policy, operations)
}

// branchComposeOutputBuffer is the policy of an output node: the feeding
// branch's queue when one branch owns the output, the graph default when
// several branches share it.
func branchComposeOutputBuffer(graphDefault pipeline.BufferPolicy, matches []int, branches []branchComposeRoute, plan map[string]graphPlanBranchComposeBranchOperation) pipeline.BufferPolicy {
	if len(matches) != 1 {
		return graphDefault
	}
	if buffer := plan[branches[matches[0]].name].buffer; !buffer.IsDirect() {
		return buffer
	}
	return graphDefault
}

// branchComposeDeclaresBuffer reports whether any planned branch declared a
// queue, which makes the whole graph run buffered.
func branchComposeDeclaresBuffer(branches []branchComposeRoute) bool {
	for i := range branches {
		if !branches[i].branch.Buffer.PipelinePolicy().IsDirect() {
			return true
		}
	}
	return false
}
