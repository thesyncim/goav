package goav

import (
	"errors"
	"testing"

	"github.com/thesyncim/goav/errcode"
)

func TestBuildErrorFamilyForCodePinned(t *testing.T) {
	tests := []struct {
		code   errcode.Code
		family errcode.Family
	}{
		{errcode.InputInvalid, errcode.FamilyInput},
		{errcode.StreamAmbiguous, errcode.FamilyStream},
		{flowDecodeDuplicateCode, errcode.FamilyFlow},
		{errcode.TransformAdapterMissing, errcode.FamilyTransform},
		{errcode.ShapeRequirementUnmet, errcode.FamilyShape},
		{errcode.DecodeAdapterMissing, errcode.FamilyCodec},
		{errcode.EncodeMissing, errcode.FamilyEncode},
		{errcode.DestinationDuplicate, errcode.FamilyDestination},
		{branchInvalidCode, errcode.FamilyBranch},
		{bufferPayloadUnsafeCode, errcode.FamilyBuffer},
		{errcode.TapInvalid, errcode.FamilyTap},
		{errcode.Code("mix_inputs"), errcode.FamilyJoin},
		{runtimeBranchInvalidCode, errcode.FamilyRuntimeBranch},
		{errcode.StreamRuleInvalid, errcode.FamilyStreamRule},
		{compilerPassFailedCode, errcode.FamilyCompiler},
		{errcode.Code(diagnosticShapeConversionInserted), errcode.Family("diagnostic")},
		{errcode.Code("crossfade_inputs"), errcode.FamilyJoin},
		{errcode.Code("example_vendor_failure"), errcode.FamilyExternal},
	}
	for _, tt := range tests {
		if got := errcode.FamilyForCode(tt.code); got != tt.family {
			t.Fatalf("FamilyForCode(%q) = %q, want %q", tt.code, got, tt.family)
		}
	}
}

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
			code:   errcode.DestinationDuplicate,
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
