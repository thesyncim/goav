package compile

import (
	"testing"

	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/builderr"
)

func TestBuildErrorFamilyForCodePinned(t *testing.T) {
	tests := []struct {
		code   errcode.Code
		family errcode.Family
	}{
		{builderr.InputInvalidCode, errcode.FamilyInput},
		{builderr.StreamAmbiguousCode, errcode.FamilyStream},
		{builderr.TransformAdapterMissingCode, errcode.FamilyTransform},
		{builderr.ShapeRequirementUnmetCode, errcode.FamilyShape},
		{builderr.DecodeAdapterMissingCode, errcode.FamilyCodec},
		{builderr.EncodeMissingCode, errcode.FamilyEncode},
		{builderr.DestinationDuplicateCode, errcode.FamilyDestination},
		{builderr.DecodeConfigConflictCode, errcode.FamilyBranch},
		{builderr.BranchBufferInvalidCode, errcode.FamilyBuffer},
		{errcode.TapInvalid, errcode.FamilyTap},
		{errcode.Code("mix_inputs"), errcode.FamilyJoin},
		{builderr.RuntimeBranchInvalidCode, errcode.FamilyRuntimeBranch},
		{errcode.StreamRuleInvalid, errcode.FamilyStreamRule},
		{builderr.CompilerPassFailedCode, errcode.FamilyCompiler},
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
