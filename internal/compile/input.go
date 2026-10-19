package compile

import (
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/shape"
)

// recipeIRInputSourceShape is the stream shape a provider or custom source
// declares, normalized.
func recipeIRInputSourceShape(input recipeir.Input) (shape.Spec, bool) {
	switch input.Kind {
	case recipeir.InputKindCustomSource, recipeir.InputKindProvider:
	default:
		return shape.Spec{}, false
	}
	if input.SourceShape.Domain == "" {
		return shape.Spec{}, false
	}
	return NormalizeCustomSourceShape(input.Name, input.SourceShape), true
}
