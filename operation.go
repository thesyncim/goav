package goav

import (
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// The constructors of the one operation record. Each Stream method appends
// one of these; the planner reads them, and inserts its own (a packet copy, a
// shape conversion) through the same constructors.

func opDecode(spec codec.Spec) recipeir.Operation {
	return recipeir.Operation{Kind: plan.OpDecode, Decode: cloneCodecSpec(spec)}
}

func opCopy() recipeir.Operation {
	return recipeir.Operation{Kind: plan.OpCopy, Component: "packet-copy", Detail: "explicit packet copy", Encode: codec.Copy()}
}

func opEncode(spec codec.Spec) recipeir.Operation {
	if spec.Copy {
		operation := opCopy()
		operation.Encode = cloneCodecSpec(spec)
		return operation
	}
	return recipeir.Operation{Kind: plan.OpEncode, Component: string(spec.ID), Encode: cloneCodecSpec(spec)}
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
	return recipeir.Operation{Kind: plan.OpTransform, Component: recipeIRTransformFactoryName(transform), Transform: transform}
}

// operationsAutoPolicy unions a stream's .Auto(...) policies; the second
// result reports whether any is present — an empty .Auto() activates solving
// while allowing nothing, so refusals name the exact policy to add.
func operationsAutoPolicy(operations []recipeir.Operation) (shape.Policy, bool) {
	var policy shape.Policy
	active := false
	for i := range operations {
		if operations[i].Auto == nil {
			continue
		}
		active = true
		policy = policy.Union(*operations[i].Auto)
	}
	return policy, active
}

// operationsPreference merges a stream's .Prefer(...) specs (later facts
// win) and reports whether any is present.
func operationsPreference(operations []recipeir.Operation) (shape.Spec, bool) {
	var preference shape.Spec
	active := false
	for i := range operations {
		if operations[i].Prefer == nil {
			continue
		}
		active = true
		preference = shape.Merge(preference, *operations[i].Prefer)
	}
	return preference, active
}

func operationsHaveDecode(operations []recipeir.Operation) bool {
	return recipeIROperationsContainKind(operations, plan.OpDecode)
}

// operationsDecodeCodec is a stream's decode codec options.
func operationsDecodeCodec(operations []recipeir.Operation) codec.Spec {
	for i := range operations {
		if operations[i].Kind == plan.OpDecode {
			return operations[i].Decode
		}
	}
	return codec.Spec{}
}

// operationsEncodeSpec is a stream's encoder (or packet copy).
func operationsEncodeSpec(operations []recipeir.Operation) codec.Spec {
	for i := range operations {
		if operations[i].Kind == plan.OpEncode || operations[i].Kind == plan.OpCopy {
			return operations[i].Encode
		}
	}
	return codec.Spec{}
}

// operationsHaveStage reports a custom stage among the operations.
func operationsHaveStage(operations []recipeir.Operation) bool {
	for i := range operations {
		if operations[i].Kind == plan.OpStage {
			switch operations[i].Stage.(type) {
			case *syncGate, *playoutGate:
				continue
			}
			return true
		}
	}
	return false
}

// tapDomainForAfter infers a domain-less tap's media domain from the
// operation it follows: packets after select/copy/encode, frames otherwise.
func tapDomainForAfter(after plan.OperationKind) shape.MediaDomain {
	switch after {
	case plan.OpSelect, plan.OpCopy, plan.OpEncode:
		return shape.DomainPacket
	default:
		return shape.DomainFrame
	}
}

func recipeIROperationTapIsTerminalPacket(operation recipeir.Operation) bool {
	if operation.Kind != plan.OpTap {
		return false
	}
	return operation.Tap.Domain == shape.DomainPacket &&
		(operation.Tap.After == plan.OpEncode || operation.Tap.After == plan.OpCopy)
}

func recipeIRTransformFactoryName(transform recipeir.Transform) string {
	switch transform.Kind {
	case recipeir.TransformResize:
		return filter.FactoryResize
	case recipeir.TransformResample:
		return filter.FactoryResample
	default:
		return ""
	}
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
