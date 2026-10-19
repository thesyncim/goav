package goav

import (
	"context"

	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// AttachPlannedForTest attaches outs to a running task as Attach does and
// returns, beside the attachment, the graph patch the attach planned before
// it touched the graph: the attach twin of Describe.
func AttachPlannedForTest(ctx context.Context, live LiveTask, outs ...Output) (pipeline.Spec, Attachment, error) {
	t := live.(*task)
	branches, err := t.attachBranches(outs)
	if err != nil {
		return pipeline.Spec{}, nil, err
	}
	input, err := runtimeAttachInputFromBranchSpecs(branches)
	if err != nil {
		return pipeline.Spec{}, nil, err
	}
	return t.attachPlanned(ctx, input)
}

// RebranchPlannedForTest plans and attaches the replacements a Rebranch with
// args attaches, leaving the replaced branch in place.
func RebranchPlannedForTest(ctx context.Context, live LiveTask, args ...RebranchArg) (pipeline.Spec, Attachment, error) {
	t := live.(*task)
	args, err := t.rebranchArgs(args)
	if err != nil {
		return pipeline.Spec{}, nil, err
	}
	input, err := runtimeRebranchInputFromArgs(ctx, args)
	if err != nil {
		return pipeline.Spec{}, nil, err
	}
	return t.attachPlanned(ctx, input.attach)
}

func (t *task) attachPlanned(ctx context.Context, input runtimeAttachInput) (pipeline.Spec, Attachment, error) {
	t.attachMu.Lock()
	defer t.attachMu.Unlock()
	p, err := t.planAttach(ctx, input)
	if err != nil {
		return pipeline.Spec{}, nil, err
	}
	planned, err := p.spec()
	if err != nil {
		p.close()
		return pipeline.Spec{}, nil, err
	}
	attachment, err := p.apply()
	if err != nil {
		return pipeline.Spec{}, nil, err
	}
	t.trackAttachmentLocked(attachment)
	t.addAttachmentTapsLocked(attachment.taps)
	return planned, attachment, nil
}

// operationContract is the shape.Contract of one planned operation, for the
// shape-contract tests.
type operationContract recipeir.Operation

func (o operationContract) InputShapes() shape.Set {
	return compile.RecipeIROperationInputShapes(recipeir.Operation(o))
}

func (o operationContract) OutputShapes(input shape.Spec) shape.Set {
	return compile.RecipeIROperationOutputShapes(input, recipeir.Operation(o))
}

// CopyOperationContractForTest, ResizeContractForTest, and
// ResampleContractForTest expose the contracts of the operations .Copy(),
// .Resize(...), and .Resample(...) plan.
func CopyOperationContractForTest() shape.Contract { return operationContract(compile.OpCopy()) }

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
