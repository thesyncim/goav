package goav

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/thesyncim/goav/pipeline"
)

func TestBranchBufferConstructorsLowerToPipelinePolicy(t *testing.T) {
	tests := []struct {
		name   string
		buffer BranchBuffer
		want   pipeline.BufferPolicy
	}{
		{
			name:   "blocking",
			buffer: Blocking(8),
			want:   pipeline.BufferPolicy{Capacity: 8, Drop: pipeline.DropBlock},
		},
		{
			name:   "drop oldest",
			buffer: DropOldest(3),
			want:   pipeline.BufferPolicy{Capacity: 3, Drop: pipeline.DropOldest},
		},
		{
			name:   "drop newest",
			buffer: DropNewest(5),
			want:   pipeline.BufferPolicy{Capacity: 5, Drop: pipeline.DropNewest},
		},
		{
			name:   "latest",
			buffer: Latest(),
			want:   pipeline.BufferPolicy{Capacity: 1, Drop: pipeline.DropOldest},
		},
		{
			name:   "copy bounds and delay",
			buffer: Blocking(2, BufferCopyBounds(64, 1024), BufferMaxDelay(25*time.Millisecond)),
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
			if err := tt.buffer.validate("build branch", "preview"); err != nil {
				t.Fatal(err)
			}
			if got := tt.buffer.pipelinePolicy(); got != tt.want {
				t.Fatalf("pipelinePolicy() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestBranchBufferRejectsInvalidPolicies(t *testing.T) {
	tests := []struct {
		name   string
		buffer BranchBuffer
		code   string
	}{
		{name: "zero capacity", buffer: DropOldest(0), code: "branch_buffer_invalid"},
		{name: "negative copy bounds", buffer: Blocking(1, BufferCopyBounds(-1, 0)), code: "branch_buffer_invalid"},
		{name: "copy never with bounds", buffer: Blocking(1, BufferCopyMode(CopyNever), BufferCopyBounds(1, 0)), code: "branch_buffer_invalid"},
		{name: "unbounded unsupported", buffer: Unbounded(), code: "branch_buffer_unsupported"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.buffer.validate("build branch", "preview")
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
	if got != reflect.TypeOf(BranchBuffer{}) {
		t.Fatalf("Branch.Buffer argument = %s, want goav.BranchBuffer", got)
	}
	if got == reflect.TypeOf(pipeline.BufferPolicy{}) {
		t.Fatalf("Branch.Buffer must not expose pipeline.BufferPolicy")
	}
}

// TestFrontDoorDropReasonsReadableWithoutPipeline proves a consumer can classify
// dropped messages from TaskStats/NodeStats using the public DropReason* vocabulary
// — note this test body imports no pipeline symbols to read the counters.
func TestFrontDoorDropReasonsReadableWithoutPipeline(t *testing.T) {
	var stats TaskStats
	stats.DropReasons = map[DropReason]uint64{DropReasonStale: 3, DropReasonOverflow: 1}
	if stats.DropReasons[DropReasonStale] != 3 || stats.DropReasons[DropReasonOverflow] != 1 {
		t.Fatalf("task drop reasons not readable via public constants: %+v", stats.DropReasons)
	}
	var node NodeStats
	node.DropReasons = map[DropReason]uint64{DropReasonNonKeyVideo: 2, DropReasonOldest: 5}
	if node.DropReasons[DropReasonNonKeyVideo] != 2 || node.DropReasons[DropReasonOldest] != 5 {
		t.Fatalf("node drop reasons not readable via public constants: %+v", node.DropReasons)
	}
}
