package compile

import (
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

func OpCopy() recipeir.Operation {
	return recipeir.Operation{Kind: plan.OpCopy, Component: "packet-copy", Detail: "explicit packet copy", Encode: codec.Copy()}
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

func OperationsHaveDecode(operations []recipeir.Operation) bool {
	return recipeIROperationsContainKind(operations, plan.OpDecode)
}

// operationsEncodeSpec is a stream's encoder (or packet copy).
func OperationsEncodeSpec(operations []recipeir.Operation) codec.Spec {
	for i := range operations {
		if operations[i].Kind == plan.OpEncode || operations[i].Kind == plan.OpCopy {
			return operations[i].Encode
		}
	}
	return codec.Spec{}
}

func RecipeIRTransformFactoryName(transform recipeir.Transform) string {
	switch transform.Kind {
	case recipeir.TransformResize:
		return filter.FactoryResize
	case recipeir.TransformResample:
		return filter.FactoryResample
	default:
		return ""
	}
}
