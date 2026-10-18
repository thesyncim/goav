package goav

import (
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/shape"
)

// operationContract is the shape.Contract of one planned operation, for the
// shape-contract tests.
type operationContract recipeir.Operation

func (o operationContract) InputShapes() shape.Set {
	return recipeIROperationInputShapes(recipeir.Operation(o))
}

func (o operationContract) OutputShapes(input shape.Spec) shape.Set {
	return recipeIROperationOutputShapes(input, recipeir.Operation(o))
}

// CopyOperationContractForTest, ResizeContractForTest, and
// ResampleContractForTest expose the contracts of the operations .Copy(),
// .Resize(...), and .Resample(...) plan.
func CopyOperationContractForTest() shape.Contract { return operationContract(opCopy()) }

func ResizeContractForTest(width, height int) shape.Contract {
	return operationContract(opTransform(resizeSpec(width, height)))
}

func ResampleContractForTest(sampleRate, channels int) shape.Contract {
	return operationContract(opTransform(resampleSpec(sampleRate, channels)))
}

// expertGraph opens the internal fluent graph builder the expert package
// wraps — in-package tests cannot import expert (cycle), so they pin the
// graph semantics through this seam.
func expertGraph(rt *Runtime) *graphBuilder {
	return newExpertGraph(rt)
}
