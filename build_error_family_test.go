package goav

import (
	"errors"
	"testing"

	"github.com/thesyncim/goav/errcode"
)

func TestBuildErrorFamilyReachesReturnedErrors(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		code   errcode.Code
		family errcode.Family
	}{
		{
			name:   "destination duplicate",
			err:    duplicateDestinationHandleError("build recipe", "archive.webm"),
			code:   destinationDuplicateCode,
			family: errcode.FamilyDestination,
		},
		{
			name:   "nil detach attachment",
			err:    nilAttachmentDetachError(),
			code:   runtimeBranchInvalidCode,
			family: errcode.FamilyRuntimeBranch,
		},
	}
	for _, tt := range tests {
		var buildErr *BuildError
		if !errors.As(tt.err, &buildErr) {
			t.Fatalf("%s: err = %T, want *BuildError", tt.name, tt.err)
		}
		if buildErr.Code != tt.code || buildErr.Family != tt.family {
			t.Fatalf("%s: code/family = %q/%q, want %q/%q", tt.name, buildErr.Code, buildErr.Family, tt.code, tt.family)
		}
	}
}
