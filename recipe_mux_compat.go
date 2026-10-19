package goav

import (
	"github.com/thesyncim/goav/internal/compile"
)

func resolvedMuxCompatibilityIssues(resolved recipeResolved) []compile.MuxCompatibilityIssue {
	return compile.MuxCompatibilityIssues(resolved.workIR(), resolved.inputProbes, resolved.runtime.compileEnv())
}
