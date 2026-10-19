// Built-in transforms: Resize and Resample specs and their build-time validation.

package goav

import (
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/shape"
)

type resizeOption func(*filter.ResizeConfig)

type audioOption func(*filter.ResampleConfig)

// resizeSpec declares a video geometry conversion to width x height (exact mode
// unless an option changes it), performed by the runtime's registered resize
// filter. Chains reach it through .Resize(...).
func resizeSpec(width int, height int, options ...resizeOption) recipeir.Transform {
	config := filter.ResizeConfig{Width: width, Height: height, Mode: filter.ResizeExact}
	for i := range options {
		if options[i] != nil {
			options[i](&config)
		}
	}
	return recipeir.Transform{Kind: recipeir.TransformResize, Resize: config}
}

// resampleSpec declares an audio conversion to the given sample rate and
// channel count, performed by the runtime's registered resample filter.
// Chains reach it through .Resample(...).
func resampleSpec(sampleRate int, channels int, options ...audioOption) recipeir.Transform {
	config := filter.ResampleConfig{SampleRate: sampleRate, Channels: channels}
	for i := range options {
		if options[i] != nil {
			options[i](&config)
		}
	}
	return recipeir.Transform{Kind: recipeir.TransformResample, Resample: config}
}

func validateTransformSpec(operation string, node string, spec recipeir.Transform) error {
	switch {
	case spec.Kind == recipeir.TransformResize:
		if spec.Resize.Width > 0 && spec.Resize.Height > 0 {
			return nil
		}
		return &BuildError{
			Phase:     phaseBuild,
			Family:    errcode.FamilyForCode(transformInvalidCode),
			Code:      transformInvalidCode,
			Operation: operation,
			Node:      node,
			Reason:    "resize requires positive width and height",
			fields:    errDetails(errDetail("width", fmt.Sprintf("%d", spec.Resize.Width)), errDetail("height", fmt.Sprintf("%d", spec.Resize.Height))),
			fixes: buildErrorFixes([]string{
				"call .Resize(width, height) with positive dimensions",
				"remove .Resize(...) when no video scaling is needed",
			}),
			cause: errUnsupportedBuild,
		}
	case spec.Kind == recipeir.TransformResample:
		if spec.Resample.SampleRate > 0 && spec.Resample.Channels > 0 {
			return nil
		}
		return &BuildError{
			Phase:     phaseBuild,
			Family:    errcode.FamilyForCode(transformInvalidCode),
			Code:      transformInvalidCode,
			Operation: operation,
			Node:      node,
			Reason:    "resample requires positive sample rate and channels",
			fields:    errDetails(errDetail("sample_rate", fmt.Sprintf("%d", spec.Resample.SampleRate)), errDetail("channels", fmt.Sprintf("%d", spec.Resample.Channels))),
			fixes: buildErrorFixes([]string{
				"call .Resample(sampleRate, channels) with positive values",
				"remove .Resample(...) when no audio conversion is needed",
			}),
			cause: errUnsupportedBuild,
		}
	default:
		return publicError(compile.EmptyTransformSpecError(operation, node))
	}
}

func transformMediaError(stream string, transform string, expected av.MediaType, actual av.MediaType) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(transformMediaMismatchCode),
		Code:      transformMediaMismatchCode,
		Operation: "build stream",
		Node:      stream,
		Reason:    transform + " applies to " + string(expected) + " streams",
		fields:    errDetails(errDetail("expected_shape", shape.Frame(expected).String()), errDetail("actual_shape", shape.Frame(actual).String())),
		fixes: buildErrorFixes([]string{
			"use .Video().Resize(...) for video scaling",
			"use .Audio().Resample(...) for audio sample-rate or channel conversion",
		}),
		cause: errUnsupportedBuild,
	}
}
