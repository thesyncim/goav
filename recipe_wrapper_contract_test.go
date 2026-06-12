package goav

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav/errcode"
)

func TestRecipeResolvedUnsupportedContracts(t *testing.T) {
	var resolved recipeResolved
	if _, err := resolved.Describe(); !buildErrorHasCodeOperation(err, errcode.RecipeGraphUnsupported, "describe recipe") {
		t.Fatalf("Describe() error = %v, want recipe_graph_unsupported describe error", err)
	}
	if _, err := resolved.Build(context.Background()); !buildErrorHasCodeOperation(err, errcode.RecipeGraphUnsupported, "build recipe") {
		t.Fatalf("Build() error = %v, want recipe_graph_unsupported build error", err)
	}
	if work := resolved.workIR(); work.Name != "" || len(work.Operations) != 0 || len(work.Branches) != 0 {
		t.Fatalf("workIR() = %+v, want empty work plan", work)
	}
}

func TestBranchCompositionNilComposePlanContract(t *testing.T) {
	var job *branchCompositionJob
	plan, err := job.composePlan()
	if err != nil {
		t.Fatal(err)
	}
	if branchComposePlanReady(plan) {
		t.Fatalf("nil composePlan() = %+v, want not ready", plan)
	}
}

func TestCompileJobBranchRecipeWithOptionsRequiresOneInput(t *testing.T) {
	_, err := compileJobBranchRecipeWithOptions(&Job{runtime: New()}, recipeCompileOptions{})
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != errcode.InputCountUnsupported ||
		buildErr.Operation != "build branches" || buildErr.Node != "branches" {
		t.Fatalf("compileJobBranchRecipeWithOptions() error = %v, want branch input-count error", err)
	}
}

func TestStateMuxCompatibilityIssuesRequireGraphPlanSpec(t *testing.T) {
	for name, state := range map[string]*recipeCompileState{
		"nil":          nil,
		"not ready":    {},
		"wrong origin": {specReady: true, specOrigin: "planned"},
	} {
		t.Run(name, func(t *testing.T) {
			if issues := stateMuxCompatibilityIssues(state); len(issues) != 0 {
				t.Fatalf("stateMuxCompatibilityIssues() = %+v, want none", issues)
			}
		})
	}
}
