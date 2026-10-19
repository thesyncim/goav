package goav

import (
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/pipeline"
)

func realtimeRecipeBufferPolicy(runtimeBuffer pipeline.BufferPolicy) pipeline.BufferPolicy {
	if !runtimeBuffer.IsDirect() {
		return runtimeBuffer
	}
	return pipeline.BufferPolicy{
		Capacity: compile.RealtimeRecipeBufferCapacity,
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
