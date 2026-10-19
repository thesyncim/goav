package goav

import (
	"testing"

	"github.com/thesyncim/goav/pipeline"
)

func TestJoinStagePreallocDepthContracts(t *testing.T) {
	if got := joinStagePreallocDepth(nil); got != 1 {
		t.Fatalf("nil runtime prealloc depth = %d, want 1", got)
	}
	if got := joinStagePreallocDepth(mustNew()); got != 1 {
		t.Fatalf("direct runtime prealloc depth = %d, want 1", got)
	}
	if got := joinStagePreallocDepth(mustNew(WithBufferPolicy(pipeline.BufferPolicy{Drop: pipeline.DropOldest}))); got != 1 {
		t.Fatalf("zero-capacity buffered prealloc depth = %d, want 1", got)
	}
	if got := joinStagePreallocDepth(mustNew(WithBufferPolicy(pipeline.BufferPolicy{Capacity: 128, Drop: pipeline.DropOldest}))); got != joinStagePreallocDepthLimit {
		t.Fatalf("large buffered prealloc depth = %d, want cap %d", got, joinStagePreallocDepthLimit)
	}
}
