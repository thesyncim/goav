package flow_test

import (
	"testing"

	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/pipeline"
)

// TestBranchBufferCopyModeDefaultsToCopyIfMutable pins the default ownership
// contract: every constructor produces a CopyIfMutable buffer unless the
// caller opts into another mode.
func TestBranchBufferCopyModeDefaultsToCopyIfMutable(t *testing.T) {
	buffers := map[string]flow.BranchBuffer{
		"blocking":    flow.Blocking(4),
		"drop oldest": flow.DropOldest(4),
		"drop newest": flow.DropNewest(4),
		"latest":      flow.Latest(),
		"unbounded":   flow.Unbounded(),
	}
	for name, buffer := range buffers {
		if buffer.CopyMode != flow.CopyIfMutable {
			t.Fatalf("%s CopyMode = %q, want CopyIfMutable", name, buffer.CopyMode)
		}
	}
}

// TestBranchBufferCopyModeLowersToPipelinePolicy proves CopyMode actually
// reaches the runtime: CopyAlways lowers to the pipeline's defensive-copy
// policy, while CopyIfMutable and CopyNever rely on the pipeline's intrinsic
// behavior (copy or refuse mutable payloads, bounded by the copy bounds).
func TestBranchBufferCopyModeLowersToPipelinePolicy(t *testing.T) {
	tests := []struct {
		name   string
		buffer flow.BranchBuffer
		want   pipeline.BufferPolicy
	}{
		{
			name:   "default copy if mutable",
			buffer: flow.Blocking(2, flow.BufferCopyBounds(64, 1024)),
			want: pipeline.BufferPolicy{
				Capacity:        2,
				Drop:            pipeline.DropBlock,
				CopyPacketBytes: 64,
				CopyFrameBytes:  1024,
			},
		},
		{
			name: "copy always",
			buffer: flow.Blocking(2,
				flow.BufferCopyMode(flow.CopyAlways),
				flow.BufferCopyBounds(64, 1024),
			),
			want: pipeline.BufferPolicy{
				Capacity:        2,
				Drop:            pipeline.DropBlock,
				CopyPacketBytes: 64,
				CopyFrameBytes:  1024,
				CopyAlways:      true,
			},
		},
		{
			name:   "copy never",
			buffer: flow.Latest(flow.BufferCopyMode(flow.CopyNever)),
			want:   pipeline.BufferPolicy{Capacity: 1, Drop: pipeline.DropOldest},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.buffer.PipelinePolicy(); got != tt.want {
				t.Fatalf("PipelinePolicy() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
