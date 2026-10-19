package compile

import (
	"errors"
	"testing"

	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/internal/builderr"
)

func TestBranchBufferRejectsInvalidPolicies(t *testing.T) {
	tests := []struct {
		name   string
		buffer flow.BranchBuffer
		code   errcode.Code
	}{
		{name: "zero capacity", buffer: flow.DropOldest(0), code: "branch_buffer_invalid"},
		{name: "negative byte limit", buffer: flow.Blocking(1, flow.BufferMaxBytes(-1)), code: "branch_buffer_invalid"},
		{name: "unbounded unsupported", buffer: flow.Unbounded(), code: "branch_buffer_unsupported"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateBranchBuffer(tt.buffer, "build branch", "preview")
			var buildErr *builderr.Refusal
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, builderr.ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want %s with matching BuildError code", err, tt.code)
			}
		})
	}
}
