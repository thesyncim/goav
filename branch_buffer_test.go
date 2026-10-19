package goav

import (
	"reflect"
	"testing"
	"time"

	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/pipeline"
)

func TestBranchBufferConstructorsLowerToPipelinePolicy(t *testing.T) {
	tests := []struct {
		name   string
		buffer flow.BranchBuffer
		want   pipeline.BufferPolicy
	}{
		{
			name:   "blocking",
			buffer: flow.Blocking(8),
			want:   pipeline.BufferPolicy{Capacity: 8, Drop: pipeline.DropBlock},
		},
		{
			name:   "drop oldest",
			buffer: flow.DropOldest(3),
			want:   pipeline.BufferPolicy{Capacity: 3, Drop: pipeline.DropOldest},
		},
		{
			name:   "drop newest",
			buffer: flow.DropNewest(5),
			want:   pipeline.BufferPolicy{Capacity: 5, Drop: pipeline.DropNewest},
		},
		{
			name:   "latest",
			buffer: flow.Latest(),
			want:   pipeline.BufferPolicy{Capacity: 1, Drop: pipeline.DropOldest},
		},
		{
			name:   "delay",
			buffer: flow.Blocking(2, flow.BufferMaxDelay(25*time.Millisecond)),
			want:   pipeline.BufferPolicy{Capacity: 2, Drop: pipeline.DropBlock, MaxLatency: 25 * time.Millisecond},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := publicError(compile.ValidateBranchBuffer(tt.buffer, "build branch", "preview")); err != nil {
				t.Fatal(err)
			}
			if got := tt.buffer.PipelinePolicy(); got != tt.want {
				t.Fatalf("PipelinePolicy() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestStreamBufferIsNormalStreamAPI(t *testing.T) {
	method, ok := reflect.TypeFor[Stream]().MethodByName("Buffer")
	if !ok {
		t.Fatal("Stream should expose Buffer")
	}
	got := method.Type.In(1)
	if got != reflect.TypeFor[flow.BranchBuffer]() {
		t.Fatalf("Stream.Buffer argument = %s, want flow.BranchBuffer", got)
	}
	if got == reflect.TypeFor[pipeline.BufferPolicy]() {
		t.Fatalf("Stream.Buffer must not expose pipeline.BufferPolicy")
	}
}

// TestFrontDoorDropReasonsReadableWithoutPipeline proves a consumer can classify
// dropped messages from pipeline.GraphStats/pipeline.NodeStats using the public DropReason* vocabulary
// — note this test body imports no pipeline symbols to read the counters.
func TestFrontDoorDropReasonsReadableWithoutPipeline(t *testing.T) {
	var stats pipeline.GraphStats
	stats.DropReasons = map[flow.DropReason]uint64{flow.DropReasonStale: 3, flow.DropReasonOverflow: 1}
	if stats.DropReasons[flow.DropReasonStale] != 3 || stats.DropReasons[flow.DropReasonOverflow] != 1 {
		t.Fatalf("task drop reasons not readable via public constants: %+v", stats.DropReasons)
	}
	var node pipeline.NodeStats
	node.DropReasons = map[flow.DropReason]uint64{flow.DropReasonNonKeyVideo: 2, flow.DropReasonOldest: 5}
	if node.DropReasons[flow.DropReasonNonKeyVideo] != 2 || node.DropReasons[flow.DropReasonOldest] != 5 {
		t.Fatalf("node drop reasons not readable via public constants: %+v", node.DropReasons)
	}
}
