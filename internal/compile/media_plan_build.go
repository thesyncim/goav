package compile

import (
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/shape"
)

// mediaShapeFromMediaTransform is the frame shape a lowered transform
// produces.
func mediaShapeFromMediaTransform(transform MediaTransform) shape.Spec {
	switch {
	case transform.Video != nil:
		return MediaShapeFromRecipeIRTransform(recipeir.Transform{Kind: recipeir.TransformResize, Resize: *transform.Video})
	case transform.Audio != nil:
		return MediaShapeFromRecipeIRTransform(recipeir.Transform{Kind: recipeir.TransformResample, Resample: *transform.Audio})
	default:
		return shape.Spec{}
	}
}
