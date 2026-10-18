package goav

import (
	"context"

	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/shape"
)

func CopyOperationContractForTest() shape.Contract {
	return operationSpecForCopy(codec.Copy())
}

// ResizeSpecForTest and ResampleSpecForTest expose the transform specs the
// stream methods build, for shape-contract tests.
var (
	ResizeSpecForTest   = resizeSpec
	ResampleSpecForTest = resampleSpec
)

func TransformOperationContractForTest(transform transformSpec) shape.Contract {
	return operationSpecForTransform(transform)
}

// expertGraph opens the internal fluent graph builder the expert package
// wraps — in-package tests cannot import expert (cycle), so they pin the
// graph semantics through this seam.
func expertGraph(rt *Runtime) *graphBuilder {
	return newExpertGraph(rt)
}

// legacyAttach attaches builder branches to a built task, for the tests that
// pin the branch attach path the value grammar lowers onto.
func legacyAttach(live LiveTask, ctx context.Context, specs ...branchRecipe) (Attachment, error) {
	return live.(*task).attachBranches(ctx, specs...)
}
