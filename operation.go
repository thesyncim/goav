package goav

import (
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// The constructors of the one operation record. Each Stream method appends
// one of these; the planner reads them, and inserts its own (a packet copy, a
// shape conversion) through the same constructors.

func opDecode(spec codec.Spec) recipeir.Operation {
	return recipeir.Operation{Kind: plan.OpDecode, Decode: compile.CloneCodecSpec(spec)}
}

func opEncode(spec codec.Spec) recipeir.Operation {
	return recipeir.Operation{Kind: plan.OpEncode, Component: string(spec.ID), Encode: compile.CloneCodecSpec(spec)}
}

func opStage(stage pipeline.Stage) recipeir.Operation {
	name := ""
	if stage != nil {
		name = stage.Name()
	}
	return recipeir.Operation{Kind: plan.OpStage, Component: name, Stage: stage}
}

func opSync(policy flow.SyncPolicy) recipeir.Operation {
	return opStage(newSyncGate(policy))
}

func opPlayout(policy flow.PlayoutPolicy) recipeir.Operation {
	return opStage(newPlayoutGate(policy))
}

func opShape(spec shape.Spec) recipeir.Operation {
	return recipeir.Operation{Kind: plan.OpShape, Component: "shape", Shape: spec}
}

// opAuto is the policy-carrying operation .Auto(...) appends: a shape
// annotation with no shape facts (it never changes the media) whose policy
// opts the stream into shape solving.
func opAuto(policies []shape.Policy) recipeir.Operation {
	var policy shape.Policy
	for i := range policies {
		policy = policy.Union(policies[i])
	}
	return recipeir.Operation{Kind: plan.OpShape, Component: "auto", Auto: &policy}
}

// opRequire is the assertion .Require(...) appends: the shape walk fails the
// build when the stream does not satisfy it at this point.
func opRequire(spec shape.Spec) recipeir.Operation {
	return recipeir.Operation{Kind: plan.OpShape, Component: "require", Require: &spec}
}

// opPrefer is the preference .Prefer(...) appends: it biases the shape
// solver's otherwise-open choices.
func opPrefer(spec shape.Spec) recipeir.Operation {
	return recipeir.Operation{Kind: plan.OpShape, Component: "prefer", Prefer: &spec}
}

// opTransform is a resize or resample: the transform and the filter factory
// that performs it.
func opTransform(transform recipeir.Transform) recipeir.Operation {
	return recipeir.Operation{Kind: plan.OpTransform, Component: compile.RecipeIRTransformFactoryName(transform), Transform: transform}
}

func recipeIROperationTapIsTerminalPacket(operation recipeir.Operation) bool {
	if operation.Kind != plan.OpTap {
		return false
	}
	return operation.Tap.Domain == shape.DomainPacket &&
		(operation.Tap.After == plan.OpEncode || operation.Tap.After == plan.OpCopy)
}

// cloneOperations copies operations for a branch planned more than once —
// a stream rule's branch per discovered stream: each copy runs its own sync
// and playout gates, which share their policy's scheduler.
func cloneOperations(operations []recipeir.Operation) []recipeir.Operation {
	if len(operations) == 0 {
		return nil
	}
	out := append([]recipeir.Operation(nil), operations...)
	for i := range out {
		switch gate := out[i].Stage.(type) {
		case *syncGate:
			out[i].Stage = newSyncGate(gate.policy)
		case *playoutGate:
			out[i].Stage = newPlayoutGate(gate.policy)
		}
	}
	return out
}
