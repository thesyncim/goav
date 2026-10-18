package goav

import (
	"context"
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
