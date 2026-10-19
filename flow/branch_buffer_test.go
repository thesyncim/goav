package flow_test

import (
	"testing"
	"time"

	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/pipeline"
)

func TestBranchBufferOptionsApplyLimitsAndIgnoreNil(t *testing.T) {
	delay := 250 * time.Millisecond
	buffer := flow.DropOldest(3,
		flow.BufferMaxBytes(4096),
		flow.BufferMaxDelay(delay),
	)

	if buffer.MaxBytes != 4096 {
		t.Fatalf("MaxBytes = %d, want 4096", buffer.MaxBytes)
	}
	if buffer.MaxDelay != delay {
		t.Fatalf("MaxDelay = %s, want %s", buffer.MaxDelay, delay)
	}

	flow.BufferMaxBytes(1)(nil)
	flow.BufferMaxDelay(time.Second)(nil)
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
