package compile

import (
	"errors"
	"slices"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/internal/builderr"
	"github.com/thesyncim/goav/internal/recipeir"
)

func validateRecipeIRTransformAdapters(operation string, env Env, streams []recipeir.Stream) error {
	if !env.Present {
		return nil
	}
	for i := range streams {
		stream := streams[i]
		transforms := recipeIRStreamTransforms(stream)
		for j := range transforms {
			name := RecipeIRTransformFactoryName(transforms[j])
			if name == "" {
				continue
			}
			if _, err := env.Filters.Factory(name); err != nil {
				return RecipeTransformAdapterError(operation, stream, name, err)
			}
			desc, err := env.Filters.Descriptor(name)
			if err == nil {
				if err := ValidateRecipeIRTransformAdapterDescriptor(operation, stream, transforms[j], name, desc); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func ValidateRecipeIRTransformAdapterDescriptor(operation string, stream recipeir.Stream, spec recipeir.Transform, name string, desc filter.Descriptor) error {
	expectedInput, expectedOutput := TransformAdapterExpectedMedia(name)
	if expectedInput != "" && desc.Input != "" && desc.Input != expectedInput {
		return transformAdapterIncompatibleError(operation, stream, name, desc, expectedInput, expectedOutput)
	}
	if expectedOutput != "" && desc.Output != "" && desc.Output != expectedOutput {
		return transformAdapterIncompatibleError(operation, stream, name, desc, expectedInput, expectedOutput)
	}
	switch spec.Kind {
	case recipeir.TransformResize:
		if mode := ResizeModeWithDefault(spec.Resize.Mode); mode != "" && len(desc.ResizeModes) != 0 && !resizeModeAllowed(desc.ResizeModes, mode) {
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

func ResizeModeWithDefault(mode filter.ResizeMode) filter.ResizeMode {
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

func TransformAdapterExpectedMedia(name string) (av.MediaType, av.MediaType) {
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
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.TransformAdapterIncompatibleCode),
		Code:      builderr.TransformAdapterIncompatibleCode,
		Operation: operation,
		Node:      recipeIRStreamName(stream),
		Reason:    name + " filter adapter declares incompatible media",
		Details:   builderr.Details(builderr.KV("transform", name), builderr.KV("expected_input", string(expectedInput)), builderr.KV("expected_output", string(expectedOutput)), builderr.KV("actual_input", string(desc.Input)), builderr.KV("actual_output", string(desc.Output))),
		Fixes: builderr.Fixes([]string{
			"register a " + name + " filter adapter whose descriptor declares " + string(expectedInput) + " input and " + string(expectedOutput) + " output",
			"use .Video().Resize(...) with video resize adapters and .Audio().Resample(...) with audio resample adapters",
			"fix the adapter descriptor if the implementation already supports this transform",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

func transformAdapterCapabilityError(operation string, stream recipeir.Stream, name string, field string, requested string, supported []string) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.TransformAdapterIncompatibleCode),
		Code:      builderr.TransformAdapterIncompatibleCode,
		Operation: operation,
		Node:      recipeIRStreamName(stream),
		Reason:    name + " filter adapter does not support the requested " + strings.ReplaceAll(field, "_", " "),
		Details:   builderr.Details(builderr.KV("transform", name), builderr.KV("field", field), builderr.KV("requested", requested), builderr.KV("supported", strings.Join(supported, ","))),
		Fixes: builderr.Fixes([]string{
			"choose one of the supported " + strings.ReplaceAll(field, "_", " ") + " values",
			"register a " + name + " filter adapter whose descriptor supports this transform config",
			"fix the adapter descriptor if the implementation already supports this config",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

func RecipeTransformAdapterError(operation string, stream recipeir.Stream, name string, cause error) error {
	if !errors.Is(cause, filter.ErrNotFound) {
		return cause
	}
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.TransformAdapterMissingCode),
		Code:      builderr.TransformAdapterMissingCode,
		Operation: operation,
		Node:      recipeIRStreamName(stream),
		Reason:    "no " + name + " filter adapter is registered",
		Details:   builderr.Details(builderr.KV("transform", name)),
		Fixes: builderr.Fixes([]string{
			"register a filter adapter that provides " + name,
			"import github.com/thesyncim/goav/bundle and build with bundle.MustNew(...), or goav.New(bundle.Filters(), ...) for the bundled resize and resample adapters",
			"remove ." + TransformMethodName(name) + "(...) when that conversion is not needed",
		}),
		Cause: cause,
	}
}

func TransformMethodName(name string) string {
	switch name {
	case filter.FactoryResize:
		return "Resize"
	case filter.FactoryResample:
		return "Resample"
	default:
		return "Do"
	}
}

func EmptyTransformSpecError(operation string, node string) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.TransformInvalidCode),
		Code:      builderr.TransformInvalidCode,
		Operation: operation,
		Node:      node,
		Reason:    "empty stream transform",
		Fixes: builderr.Fixes([]string{
			"call .Resize(width, height) for video streams",
			"call .Resample(sampleRate, channels) for audio streams",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

// transformResize is a resize transform's config for the lowering, nil for
// any other transform.
func TransformResize(transform recipeir.Transform) *filter.ResizeConfig {
	if transform.Kind != recipeir.TransformResize {
		return nil
	}
	config := transform.Resize
	return &config
}

// transformResample is a resample transform's config for the lowering, nil
// for any other transform.
func TransformResample(transform recipeir.Transform) *filter.ResampleConfig {
	if transform.Kind != recipeir.TransformResample {
		return nil
	}
	config := transform.Resample
	return &config
}
