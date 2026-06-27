// Built-in transforms: Resize and Resample specs and their build-time validation.

package goav

import (
	"errors"
	"fmt"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/shape"
)

type resizeOption func(*filter.ResizeConfig)

type audioOption func(*filter.ResampleConfig)

// transformSpec is one declared frame transform. Chains create these through
// the Resize/Resample methods; the spec exists as an opaque value so flows can
// describe transforms without exposing partially valid pointer-union state.
type transformSpec struct {
	resize   *filter.ResizeConfig
	resample *filter.ResampleConfig
}

// Resize declares a video geometry conversion to width x height (exact mode
// unless an option changes it), performed by the runtime's registered resize
// filter.
func Resize(width int, height int, options ...resizeOption) transformSpec {
	config := filter.ResizeConfig{Width: width, Height: height, Mode: filter.ResizeExact}
	for i := range options {
		if options[i] != nil {
			options[i](&config)
		}
	}
	return transformSpec{resize: &config}
}

// Resample declares an audio conversion to the given sample rate and channel
// count, performed by the runtime's registered resample filter.
func Resample(sampleRate int, channels int, options ...audioOption) transformSpec {
	config := filter.ResampleConfig{SampleRate: sampleRate, Channels: channels}
	for i := range options {
		if options[i] != nil {
			options[i](&config)
		}
	}
	return transformSpec{resample: &config}
}

func validateRecipeIRTransformAdapters(operation string, rt *Runtime, streams []recipeir.Stream) error {
	if rt == nil {
		return nil
	}
	for i := range streams {
		stream := streams[i]
		streamIntent := streamIntentFromRecipeIR(stream)
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

func transformFactoryName(spec transformSpec) string {
	switch {
	case spec.resize != nil:
		return filter.FactoryResize
	case spec.resample != nil:
		return filter.FactoryResample
	default:
		return ""
	}
}

func validateTransformAdapterDescriptor(operation string, stream streamIntent, spec transformSpec, name string, desc filter.Descriptor) error {
	expectedInput, expectedOutput := transformAdapterExpectedMedia(name)
	if expectedInput != "" && desc.Input != "" && desc.Input != expectedInput {
		return transformAdapterIncompatibleError(operation, stream, name, desc, expectedInput, expectedOutput)
	}
	if expectedOutput != "" && desc.Output != "" && desc.Output != expectedOutput {
		return transformAdapterIncompatibleError(operation, stream, name, desc, expectedInput, expectedOutput)
	}
	if spec.resize != nil {
		if mode := resizeModeWithDefault(spec.resize.Mode); mode != "" && len(desc.ResizeModes) != 0 && !resizeModeAllowed(desc.ResizeModes, mode) {
			return transformAdapterCapabilityError(operation, stream, name, "resize_mode", string(mode), resizeModesToStrings(desc.ResizeModes))
		}
		if format := spec.resize.PixelFormat; format != "" && len(desc.PixelFormats) != 0 && !stringAllowed(desc.PixelFormats, format) {
			return transformAdapterCapabilityError(operation, stream, name, "pixel_format", format, desc.PixelFormats)
		}
	}
	if spec.resample != nil {
		if format := spec.resample.SampleFormat; format != "" && len(desc.SampleFormats) != 0 && !stringAllowed(desc.SampleFormats, format) {
			return transformAdapterCapabilityError(operation, stream, name, "sample_format", format, desc.SampleFormats)
		}
	}
	return nil
}

func validateRecipeIRTransformAdapterDescriptor(operation string, stream streamIntent, spec recipeir.Transform, name string, desc filter.Descriptor) error {
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
	for i := range allowed {
		if allowed[i] == mode {
			return true
		}
	}
	return false
}

func stringAllowed(allowed []string, value string) bool {
	for i := range allowed {
		if allowed[i] == value {
			return true
		}
	}
	return false
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

func transformAdapterIncompatibleError(operation string, stream streamIntent, name string, desc filter.Descriptor, expectedInput av.MediaType, expectedOutput av.MediaType) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(transformAdapterIncompatibleCode),
		Code:      transformAdapterIncompatibleCode,
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    name + " filter adapter declares incompatible media",
		fields: buildErrorFields([]string{
			"transform=" + name,
			"expected_input=" + string(expectedInput),
			"expected_output=" + string(expectedOutput),
			"actual_input=" + string(desc.Input),
			"actual_output=" + string(desc.Output),
		}),
		fixes: buildErrorFixes([]string{
			"register a " + name + " filter adapter whose descriptor declares " + string(expectedInput) + " input and " + string(expectedOutput) + " output",
			"use .Video().Resize(...) with video resize adapters and .Audio().Resample(...) with audio resample adapters",
			"fix the adapter descriptor if the implementation already supports this transform",
		}),
		cause: errUnsupportedBuild,
	}
}

func transformAdapterCapabilityError(operation string, stream streamIntent, name string, field string, requested string, supported []string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(transformAdapterIncompatibleCode),
		Code:      transformAdapterIncompatibleCode,
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    name + " filter adapter does not support the requested " + strings.ReplaceAll(field, "_", " "),
		fields: buildErrorFields([]string{
			"transform=" + name,
			"field=" + field,
			"requested=" + requested,
			"supported=" + strings.Join(supported, ","),
		}),
		fixes: buildErrorFixes([]string{
			"choose one of the supported " + strings.ReplaceAll(field, "_", " ") + " values",
			"register a " + name + " filter adapter whose descriptor supports this transform config",
			"fix the adapter descriptor if the implementation already supports this config",
		}),
		cause: errUnsupportedBuild,
	}
}

func recipeTransformAdapterError(operation string, stream streamIntent, name string, cause error) error {
	if !errors.Is(cause, filter.ErrNotFound) {
		return cause
	}
	return &BuildError{
		Family:    errcode.FamilyForCode(transformAdapterMissingCode),
		Code:      transformAdapterMissingCode,
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    "no " + name + " filter adapter is registered",
		fields: buildErrorFields([]string{
			"transform=" + name,
		}),
		fixes: buildErrorFixes([]string{
			"register a filter adapter that provides " + name,
			"import github.com/thesyncim/goav/bundle and build with bundle.MustNewFilters(...) for bundled resize and resample adapters",
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

func streamTransform(streamName string, selector av.StreamSelector, spec transformSpec, index int) (mediaTransform, error) {
	base := firstNonEmpty(streamName, string(selector.ID), string(selector.Type), "stream")
	suffix := ""
	if index > 0 {
		suffix = "-" + fmt.Sprint(index+1)
	}
	if err := validateTransformSpec("build stream", base, spec); err != nil {
		return mediaTransform{}, err
	}
	switch {
	case spec.resize != nil:
		if selector.Type == av.MediaAudio {
			return mediaTransform{}, transformMediaError(base, "resize", av.MediaVideo, selector.Type)
		}
		resize := *spec.resize
		return mediaTransform{
			name:    "resize-" + base + suffix,
			factory: filter.FactoryResize,
			video:   &resize,
		}, nil
	case spec.resample != nil:
		if selector.Type == av.MediaVideo {
			return mediaTransform{}, transformMediaError(base, "resample", av.MediaAudio, selector.Type)
		}
		resample := *spec.resample
		return mediaTransform{
			name:    "resample-" + base + suffix,
			factory: filter.FactoryResample,
			audio:   &resample,
		}, nil
	default:
		return mediaTransform{}, emptyTransformSpecError("build stream", base)
	}
}

func validateTransformSpec(operation string, node string, spec transformSpec) error {
	switch {
	case spec.resize != nil && spec.resample != nil:
		return combinedTransformSpecError(operation, node)
	case spec.resize != nil:
		if spec.resize.Width > 0 && spec.resize.Height > 0 {
			return nil
		}
		return &BuildError{
			Family:    errcode.FamilyForCode(transformInvalidCode),
			Code:      transformInvalidCode,
			Operation: operation,
			Node:      node,
			Reason:    "resize requires positive width and height",
			fields: buildErrorFields([]string{
				fmt.Sprintf("width=%d", spec.resize.Width),
				fmt.Sprintf("height=%d", spec.resize.Height),
			}),
			fixes: buildErrorFixes([]string{
				"call .Resize(width, height) with positive dimensions",
				"remove .Resize(...) when no video scaling is needed",
			}),
			cause: errUnsupportedBuild,
		}
	case spec.resample != nil:
		if spec.resample.SampleRate > 0 && spec.resample.Channels > 0 {
			return nil
		}
		return &BuildError{
			Family:    errcode.FamilyForCode(transformInvalidCode),
			Code:      transformInvalidCode,
			Operation: operation,
			Node:      node,
			Reason:    "resample requires positive sample rate and channels",
			fields: buildErrorFields([]string{
				fmt.Sprintf("sample_rate=%d", spec.resample.SampleRate),
				fmt.Sprintf("channels=%d", spec.resample.Channels),
			}),
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

func combinedTransformSpecError(operation string, node string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(transformInvalidCode),
		Code:      transformInvalidCode,
		Operation: operation,
		Node:      node,
		Reason:    "one transform cannot be both resize and resample",
		fixes:     buildErrorFixes([]string{"declare two separate steps instead: .Resize(width, height).Resample(rate, channels)"}),
		cause:     errUnsupportedBuild,
	}
}

func emptyTransformSpecError(operation string, node string) error {
	return &BuildError{
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
		Family:    errcode.FamilyForCode(transformMediaMismatchCode),
		Code:      transformMediaMismatchCode,
		Operation: "build stream",
		Node:      stream,
		Reason:    transform + " applies to " + string(expected) + " streams",
		fields: buildErrorFields([]string{
			"expected_shape=" + shape.Frame(expected).String(),
			"actual_shape=" + shape.Frame(actual).String(),
		}),
		fixes: buildErrorFixes([]string{
			"use .Video().Resize(...) for video scaling",
			"use .Audio().Resample(...) for audio sample-rate or channel conversion",
		}),
		cause: errUnsupportedBuild,
	}
}
