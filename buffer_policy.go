package goav

import "github.com/thesyncim/goav/pipeline"

const realtimeRecipeBufferCapacity = 32

func realtimeRecipeBufferPolicy(runtimeBuffer pipeline.BufferPolicy) pipeline.BufferPolicy {
	if !runtimeBuffer.IsDirect() {
		return runtimeBuffer
	}
	return pipeline.BufferPolicy{
		Capacity: realtimeRecipeBufferCapacity,
		Drop:     pipeline.DropBlock,
	}
}

// mergeBufferPolicy picks the queue a destination shared by several branches
// runs behind: the first buffered policy among them.
func mergeBufferPolicy(base pipeline.BufferPolicy, next pipeline.BufferPolicy) pipeline.BufferPolicy {
	if base.IsDirect() && !next.IsDirect() {
		return next
	}
	return base
}
