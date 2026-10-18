// Built-in transforms: Resize and Resample specs and their build-time validation.

package goav

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
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

func validateRecipeIRTransformAdapters(operation string, rt *Runtime, streams []recipeir.Stream) error {
	if rt == nil {
		return nil
	}
	for i := range streams {
		stream := streams[i]
		streamIntent := stream
		transforms := recipeIRStreamTransforms(stream)
		for j := range transforms {
			name := recipeIRTransformFactoryName(transforms[j])
			if name == "" {
				continue
			}
			if _, err := rt.filters.Factory(name); err != nil {
				return recipeTransformAdapterError(operation, streamIntent, name, err)
			}
			desc, err := rt.filters.Descriptor(name)
			if err == nil {
				if err := validateRecipeIRTransformAdapterDescriptor(operation, streamIntent, transforms[j], name, desc); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func validateRecipeIRTransformAdapterDescriptor(operation string, stream recipeir.Stream, spec recipeir.Transform, name string, desc filter.Descriptor) error {
	expectedInput, expectedOutput := transformAdapterExpectedMedia(name)
	if expectedInput != "" && desc.Input != "" && desc.Input != expectedInput {
		return transformAdapterIncompatibleError(operation, stream, name, desc, expectedInput, expectedOutput)
	}
	if expectedOutput != "" && desc.Output != "" && desc.Output != expectedOutput {
		return transformAdapterIncompatibleError(operation, stream, name, desc, expectedInput, expectedOutput)
	}
	switch spec.Kind {
	case recipeir.TransformResize:
		if mode := resizeModeWithDefault(spec.Resize.Mode); mode != "" && len(desc.ResizeModes) != 0 && !resizeModeAllowed(desc.ResizeModes, mode) {
			return transformAdapterCapabilityError(operation, stream, name, "resize_mode", string(mode), resizeModesToStrings(desc.ResizeModes))
		}
		if format := spec.Resize.PixelFormat; format != "" && len(desc.PixelFormats) != 0 && !stringAllowed(desc.PixelFormats, format) {
			return transformAdapterCapabilityError(operation, stream, name, "pixel_format", format, desc.PixelFormats)
		}
	case recipeir.TransformResample:
		if format := spec.Resample.SampleFormat; format != "" && len(desc.SampleFormats) != 0 && !stringAllowed(desc.SampleFormats, format) {
			return transformAdapterCapabilityError(operation, stream, name, "sample_format", format, desc.SampleFormats)
		}
	}
	return nil
}

func resizeModeWithDefault(mode filter.ResizeMode) filter.ResizeMode {
	if mode == "" {
		return filter.ResizeExact
	}
	return mode
}

func resizeModeAllowed(allowed []filter.ResizeMode, mode filter.ResizeMode) bool {
	return slices.Contains(allowed, mode)
}

func stringAllowed(allowed []string, value string) bool {
	return slices.Contains(allowed, value)
}

func resizeModesToStrings(modes []filter.ResizeMode) []string {
	out := make([]string, 0, len(modes))
	for i := range modes {
		if modes[i] != "" {
			out = append(out, string(modes[i]))
		}
	}
	return out
}

func transformAdapterExpectedMedia(name string) (av.MediaType, av.MediaType) {
	switch name {
	case filter.FactoryResize:
		return av.MediaVideo, av.MediaVideo
	case filter.FactoryResample:
		return av.MediaAudio, av.MediaAudio
	default:
		return "", ""
	}
}

func transformAdapterIncompatibleError(operation string, stream recipeir.Stream, name string, desc filter.Descriptor, expectedInput av.MediaType, expectedOutput av.MediaType) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(transformAdapterIncompatibleCode),
		Code:      transformAdapterIncompatibleCode,
		Operation: operation,
		Node:      recipeIRStreamName(stream),
		Reason:    name + " filter adapter declares incompatible media",
		fields:    errDetails(errDetail("transform", name), errDetail("expected_input", string(expectedInput)), errDetail("expected_output", string(expectedOutput)), errDetail("actual_input", string(desc.Input)), errDetail("actual_output", string(desc.Output))),
		fixes: buildErrorFixes([]string{
			"register a " + name + " filter adapter whose descriptor declares " + string(expectedInput) + " input and " + string(expectedOutput) + " output",
			"use .Video().Resize(...) with video resize adapters and .Audio().Resample(...) with audio resample adapters",
			"fix the adapter descriptor if the implementation already supports this transform",
		}),
		cause: errUnsupportedBuild,
	}
}

func transformAdapterCapabilityError(operation string, stream recipeir.Stream, name string, field string, requested string, supported []string) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(transformAdapterIncompatibleCode),
		Code:      transformAdapterIncompatibleCode,
		Operation: operation,
		Node:      recipeIRStreamName(stream),
		Reason:    name + " filter adapter does not support the requested " + strings.ReplaceAll(field, "_", " "),
		fields:    errDetails(errDetail("transform", name), errDetail("field", field), errDetail("requested", requested), errDetail("supported", strings.Join(supported, ","))),
		fixes: buildErrorFixes([]string{
			"choose one of the supported " + strings.ReplaceAll(field, "_", " ") + " values",
			"register a " + name + " filter adapter whose descriptor supports this transform config",
			"fix the adapter descriptor if the implementation already supports this config",
		}),
		cause: errUnsupportedBuild,
	}
}

func recipeTransformAdapterError(operation string, stream recipeir.Stream, name string, cause error) error {
	if !errors.Is(cause, filter.ErrNotFound) {
		return cause
	}
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(transformAdapterMissingCode),
		Code:      transformAdapterMissingCode,
		Operation: operation,
		Node:      recipeIRStreamName(stream),
		Reason:    "no " + name + " filter adapter is registered",
		fields:    errDetails(errDetail("transform", name)),
		fixes: buildErrorFixes([]string{
			"register a filter adapter that provides " + name,
			"import github.com/thesyncim/goav/bundle and build with bundle.MustNew(...), or goav.New(bundle.Filters(), ...) for the bundled resize and resample adapters",
			"remove ." + transformMethodName(name) + "(...) when that conversion is not needed",
		}),
		cause: cause,
	}
}

func transformMethodName(name string) string {
	switch name {
	case filter.FactoryResize:
		return "Resize"
	case filter.FactoryResample:
		return "Resample"
	default:
		return "Do"
	}
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
		return emptyTransformSpecError(operation, node)
	}
}

func emptyTransformSpecError(operation string, node string) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(transformInvalidCode),
		Code:      transformInvalidCode,
		Operation: operation,
		Node:      node,
		Reason:    "empty stream transform",
		fixes: buildErrorFixes([]string{
			"call .Resize(width, height) for video streams",
			"call .Resample(sampleRate, channels) for audio streams",
		}),
		cause: errUnsupportedBuild,
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

// transformResize is a resize transform's config for the lowering, nil for
// any other transform.
func transformResize(transform recipeir.Transform) *filter.ResizeConfig {
	if transform.Kind != recipeir.TransformResize {
		return nil
	}
	config := transform.Resize
	return &config
}

// transformResample is a resample transform's config for the lowering, nil
// for any other transform.
func transformResample(transform recipeir.Transform) *filter.ResampleConfig {
	if transform.Kind != recipeir.TransformResample {
		return nil
	}
	config := transform.Resample
	return &config
}
