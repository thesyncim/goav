package goav

import (
	"context"

	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// JobPlanForTest exposes the unexported intent normalization to the external
// API tests. The production surface does not export Job.Plan: the intent it
// returns is the compiler's input, not a caller-facing report (Explain is).
func JobPlanForTest(j *recipeJob) intent {
	return j.plan()
}

// StreamHasDecodeForTest / StreamEncodeForTest / StreamTapsForTest expose the
// operation-list accessors production code reads stream intents through, so
// the external API tests assert on the same single source of truth —
// streamIntent keeps no parallel decode/encode/tap fields.
func StreamHasDecodeForTest(stream streamIntent) bool {
	return chainHasDecode(stream.Operations)
}

func StreamEncodeForTest(stream streamIntent) codec.Spec {
	return chainEncodeSpec(stream.Operations)
}

func StreamTapsForTest(stream streamIntent) []tapIntent {
	return operationSpecTaps(stream.Operations, stream.Select.Type)
}

func OperationSpecKindsForTest(operations any) []plan.OperationKind {
	ops, ok := operations.([]operationSpec)
	if !ok {
		return nil
	}
	kinds := make([]plan.OperationKind, 0, len(ops))
	for i := range ops {
		kinds = append(kinds, ops[i].Kind)
	}
	return kinds
}

type TransformViewForTest struct {
	Resize   *filter.ResizeConfig
	Resample *filter.ResampleConfig
}

func TransformViewForTestFrom(spec transformSpec) TransformViewForTest {
	var out TransformViewForTest
	if spec.resize != nil {
		resize := *spec.resize
		out.Resize = &resize
	}
	if spec.resample != nil {
		resample := *spec.resample
		out.Resample = &resample
	}
	return out
}

func TransformOperationsForTest(operations any) []TransformViewForTest {
	ops, ok := operations.([]operationSpec)
	if !ok {
		return nil
	}
	transforms := make([]TransformViewForTest, 0)
	for i := range ops {
		if ops[i].Kind == plan.OpTransform {
			transforms = append(transforms, TransformViewForTestFrom(ops[i].Transform))
		}
	}
	return transforms
}

func CopyOperationContractForTest() shape.Contract {
	return operationSpecForCopy(codec.Copy())
}

// ResizeSpecForTest and ResampleSpecForTest expose the transform specs the
// chain methods build, for shape-contract tests.
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

// The builder grammar the value grammar compiles onto, under its former
// exported names, for the external tests that pin its refusals and plans.
type (
	Job             = recipeJob
	Chain           = streamChain
	BranchBuilder   = branchBuilder
	BranchSpec      = branchRecipe
	FlowBuilder     = flowBuilder
	JoinArm         = recipeArm
	MixStream       = mixStream
	CompositeStream = compositeStream
	SelectStream    = legacySelectStream
	JoinStream      = joinStreamBuilder
)

var (
	From            = recipeFrom
	Branch          = newBranch
	Flow            = newFlow
	LegacyMix       = legacyMix
	LegacyComposite = legacyComposite
	LegacySelect    = legacySelect
	LegacyJoin      = legacyJoin
	FrameTap        = frameTapRef
	PacketTap       = packetTapRef
	InputName       = selectInputName
	LegacyAttach    = legacyAttach
)
