package goav

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/flow"
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
			name:   "copy bounds and delay",
			buffer: flow.Blocking(2, flow.BufferCopyBounds(64, 1024), flow.BufferMaxDelay(25*time.Millisecond)),
			want: pipeline.BufferPolicy{
				Capacity:        2,
				Drop:            pipeline.DropBlock,
				MaxLatency:      25 * time.Millisecond,
				CopyPacketBytes: 64,
				CopyFrameBytes:  1024,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateBranchBuffer(tt.buffer, "build branch", "preview"); err != nil {
				t.Fatal(err)
			}
			if got := tt.buffer.PipelinePolicy(); got != tt.want {
				t.Fatalf("PipelinePolicy() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestBranchBufferRejectsInvalidPolicies(t *testing.T) {
	tests := []struct {
		name   string
		buffer flow.BranchBuffer
		code   errcode.Code
	}{
		{name: "zero capacity", buffer: flow.DropOldest(0), code: "branch_buffer_invalid"},
		{name: "negative copy bounds", buffer: flow.Blocking(1, flow.BufferCopyBounds(-1, 0)), code: "branch_buffer_invalid"},
		{name: "copy never with bounds", buffer: flow.Blocking(1, flow.BufferCopyMode(flow.CopyNever), flow.BufferCopyBounds(1, 0)), code: "branch_buffer_invalid"},
		{name: "unbounded unsupported", buffer: flow.Unbounded(), code: "branch_buffer_unsupported"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBranchBuffer(tt.buffer, "build branch", "preview")
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want %s wrapping ErrUnsupportedBuild", err, tt.code)
			}
		})
	}
}

func TestBranchBufferIsNormalBranchAPI(t *testing.T) {
	method, ok := reflect.TypeOf(Branch("preview")).MethodByName("Buffer")
	if !ok {
		t.Fatal("Branch should expose Buffer")
	}
	got := method.Type.In(1)
	if got != reflect.TypeOf(flow.BranchBuffer{}) {
		t.Fatalf("Branch.Buffer argument = %s, want flow.BranchBuffer", got)
	}
	if got == reflect.TypeOf(pipeline.BufferPolicy{}) {
		t.Fatalf("Branch.Buffer must not expose pipeline.BufferPolicy")
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
