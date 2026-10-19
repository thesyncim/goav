package compile

import (
	"errors"
	"testing"

	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/builderr"
)

// TestCompilerPassErrorContracts pins how a pass's error leaves the
// compiler: an error that is not a refusal, or a refusal that carries a
// diagnostic, passes through; a refusal with none is named after its pass.
func TestCompilerPassErrorContracts(t *testing.T) {
	plain := errors.New("plain")
	if got := CompilerPassError("compile", "lower", plain); got != plain {
		t.Fatalf("plain compiler error = %v, want original", got)
	}
	diagnostic := &builderr.Refusal{
		Family: errcode.FamilyForCode(builderr.StreamMissingCode),
		Code:   builderr.StreamMissingCode,
		Reason: "has diagnostic",
	}
	if got := CompilerPassError("compile", "lower", diagnostic); got != diagnostic {
		t.Fatalf("diagnostic compiler error = %v, want original", got)
	}
	empty := &builderr.Refusal{}
	var got *builderr.Refusal
	if !errors.As(CompilerPassError("compile recipe", "lower", empty), &got) {
		t.Fatal("wrapped error is not a refusal")
	}
	if got.Code != builderr.CompilerPassFailedCode ||
		got.Operation != "compile recipe" ||
		got.Reason != "recipe compiler pass failed without a diagnostic" ||
		len(got.Details) != 1 ||
		got.Details[0].String() != "pass=lower" ||
		!errors.Is(got, empty) {
		t.Fatalf("wrapped compiler pass error = %+v", got)
	}
	if !errors.As(CompilerPassError("compile recipe", "lower", &builderr.Refusal{Operation: "build branch"}), &got) || got.Operation != "build branch" {
		t.Fatalf("wrapped operation = %+v", got)
	}
}
