package flow_test

import (
	"testing"
	"time"

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

func TestBranchBufferOptionsApplyLimitsAndIgnoreNil(t *testing.T) {
	delay := 250 * time.Millisecond
	buffer := flow.DropOldest(3,
		flow.BufferMaxBytes(4096),
		flow.BufferMaxDelay(delay),
		flow.BufferCopyMode(flow.CopyAlways),
		flow.BufferCopyBounds(128, 2048),
	)

	if buffer.MaxBytes != 4096 {
		t.Fatalf("MaxBytes = %d, want 4096", buffer.MaxBytes)
	}
	if buffer.MaxDelay != delay {
		t.Fatalf("MaxDelay = %s, want %s", buffer.MaxDelay, delay)
	}
	if buffer.CopyMode != flow.CopyAlways {
		t.Fatalf("CopyMode = %q, want CopyAlways", buffer.CopyMode)
	}
	if buffer.CopyPacketBytes != 128 || buffer.CopyFrameBytes != 2048 {
		t.Fatalf("copy bounds = (%d, %d), want (128, 2048)", buffer.CopyPacketBytes, buffer.CopyFrameBytes)
	}

	flow.BufferMaxBytes(1)(nil)
	flow.BufferMaxDelay(time.Second)(nil)
	flow.BufferCopyMode(flow.CopyNever)(nil)
	flow.BufferCopyBounds(1, 2)(nil)
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

func TestBranchBufferPipelinePolicyDropModes(t *testing.T) {
	delay := 500 * time.Millisecond
	tests := []struct {
		name   string
		buffer flow.BranchBuffer
		want   pipeline.BufferPolicy
	}{
		{
			name:   "drop oldest",
			buffer: flow.DropOldest(4),
			want:   pipeline.BufferPolicy{Capacity: 4, Drop: pipeline.DropOldest},
		},
		{
			name: "drop newest with budgets",
			buffer: flow.DropNewest(5,
				flow.BufferMaxBytes(2048),
				flow.BufferMaxDelay(delay),
			),
			want: pipeline.BufferPolicy{
				Capacity:   5,
				Drop:       pipeline.DropNewest,
				MaxBytes:   2048,
				MaxLatency: delay,
			},
		},
		{
			name:   "unbounded",
			buffer: flow.Unbounded(),
			want:   pipeline.BufferPolicy{Drop: pipeline.DropNever},
		},
		{
			name:   "unknown mode",
			buffer: flow.BranchBuffer{Mode: "vendor_custom", Capacity: 7},
			want:   pipeline.BufferPolicy{Capacity: 7, Drop: pipeline.DropNever},
		},
		{
			name:   "latest normalizes capacity",
			buffer: flow.BranchBuffer{Mode: flow.BufferLatest, Capacity: 99},
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

func TestBranchBufferString(t *testing.T) {
	tests := []struct {
		name   string
		buffer flow.BranchBuffer
		want   string
	}{
		{name: "zero defaults to blocking", buffer: flow.BranchBuffer{}, want: "blocking(capacity=0)"},
		{name: "configured mode", buffer: flow.DropNewest(8), want: "drop_newest(capacity=8)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.buffer.String(); got != tt.want {
				t.Fatalf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}
