// Recipe inputs: InputSpec constructors, validation, and input format adapter checks.

package goav

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/provider"
	"github.com/thesyncim/goav/shape"
)

// InputSpec is one declared job input: a file-like reader, custom source, or
// source provider, plus the optional name and MIME facts the planner uses
// before opening it. Construct one with FileInput, Source, or
// Input; configure it with options (Name, MIME, Metadata) and decorate the
// opened source with WrapSource. Packet/frame codec facts come from a custom
// Source or provider SourceShape declaration. The zero value is intentionally
// not a valid input; construct input specs with FileInput, Source, or Input.
type InputSpec struct {
	origin   inputSpecOrigin
	input    format.Input
	provider provider.Source
	source   *sourceInputSpec
	codec    codec.CodecSpec
	name     string
	realtime bool
	// wraps decorate the opened pipeline source (WrapSource), applied in
	// declaration order after the input opens through the one source seam.
	wraps []func(pipeline.Source) pipeline.Source
	err   error
}

type inputSpecOrigin uint8

const (
	inputSpecOriginZero inputSpecOrigin = iota
	inputSpecOriginConstructed
)

func inputSpecHandle(spec InputSpec) InputSpec {
	spec.origin = inputSpecOriginConstructed
	return spec
}

// WrapSource returns a copy of the input whose opened source is decorated by
// wrap — the decoration seam for inputs. Every input kind (file, URI, source
// provider, custom Source) opens through one internal seam into a running
// pipeline.Source; wrap intercepts that value right after it opens, so an
// external decorator can observe or transform the message stream of any
// built-in input without reimplementing it: delegate Name/Close, wrap Start,
// and interpose on the pipeline.Emitter it is started with. Several wraps
// compose in declaration order — the last one added is outermost (closest to
// the graph). Planning is untouched: the input keeps its identity, probes,
// declared shape, and codec intent, so stream selection and Describe() see
// the original input.
//
// Node identity is pinned: after wrapping, the source's Name() and described
// node detail are forced back to the opened source's values, so Describe() ≡
// Build() holds no matter what the decorator reports — decorators decorate
// behavior, not identity. Optional node capabilities (pipeline.
// ControllableSource, DropReporter, ...) are visible only when the decorator
// implements them by delegation; a decorator that hides ControllableSource
// detaches the input from source controls such as Seek.
//
// Destinations need no analog: every destination constructor takes a
// caller-held value (an io.Writer for Write, a provider.Destination for
// Custom, a pipeline.Sink for Sink) that can be wrapped before it is passed.
func WrapSource(spec InputSpec, wrap func(pipeline.Source) pipeline.Source) InputSpec {
	if wrap == nil {
		return spec
	}
	wraps := make([]func(pipeline.Source) pipeline.Source, 0, len(spec.wraps)+1)
	wraps = append(wraps, spec.wraps...)
	spec.wraps = append(wraps, wrap)
	return spec
}

// inputOptionValue configures an input value (FileInput, Source, Input, or
// InputSpec.With). The direction-agnostic media options (Name, MIME,
// Metadata) satisfy it. It is sealed — only goav option constructors implement
// it.
type inputOptionValue interface {
	applyInput(*InputSpec)
}

// With returns a copy of the input with the options applied — the same option
// vocabulary the constructors take, for layering config onto an
// already-constructed value (renaming a generated test input, say).
func (s InputSpec) With(opts ...inputOptionValue) InputSpec {
	return applyInputOptions(s, opts)
}

func applyInputOptions(spec InputSpec, opts []inputOptionValue) InputSpec {
	for i := range opts {
		if opts[i] != nil {
			opts[i].applyInput(&spec)
		}
	}
	return spec
}

// FileInput declares a file-like input read from reader; name carries the
// extension format probing uses (a .ivf name selects the IVF demuxer).
func FileInput(name string, reader io.Reader, opts ...inputOptionValue) InputSpec {
	spec := InputSpec{
		input: format.Input{
			Name:     name,
			Protocol: av.ProtocolFile,
			Reader:   reader,
		},
		name: name,
	}
	if reader == nil {
		spec.err = errNilReader
	}
	return applyInputOptions(inputSpecHandle(spec), opts)
}

func (s InputSpec) formatInput() format.Input {
	input := s.input
	input.Realtime = input.Realtime || s.realtime
	return input
}

func (s InputSpec) validate() error {
	if s.err != nil {
		fixes := []string{
			"check the input constructor arguments",
			"pass a non-nil provider to goav.Input(provider)",
		}
		if errors.Is(s.err, errNilReader) {
			fixes = []string{
				"pass a non-nil io.Reader to goav.FileInput(name, reader)",
				"use goav.Input(provider) for realtime receive through a source provider",
				"use goav.Source(name, shape, fn) for application-pushed media",
			}
		}
		return &BuildError{
			Phase:     phaseBuild,
			Family:    errcode.FamilyForCode(inputInvalidCode),
			Code:      inputInvalidCode,
			Operation: "build input",
			Node:      firstNonEmpty(s.name, s.input.Name, s.input.URI, "input"),
			Reason:    s.err.Error(),
			fixes:     buildErrorFixes(fixes),
			cause:     s.err,
		}
	}
	if s.origin != inputSpecOriginConstructed {
		return &BuildError{
			Phase:     phaseBuild,
			Family:    errcode.FamilyForCode(inputInvalidCode),
			Code:      inputInvalidCode,
			Operation: "build input",
			Node:      firstNonEmpty(s.name, s.input.Name, s.input.URI, "input"),
			Reason:    "empty input spec",
			fixes: buildErrorFixes([]string{
				"use goav.FileInput(name, reader) for file-like input",
				"use goav.Source(name, shape, fn) for application-pushed packets",
				"use goav.Input(provider) for realtime receive through a source provider",
			}),
			cause: errUnsupportedBuild,
		}
	}
	if err := s.validateCustomSource(); err != nil {
		return err
	}
	return s.validatePlainInput()
}

func (s InputSpec) validateCustomSource() error {
	if s.source == nil {
		return nil
	}
	node := firstNonEmpty(s.name, s.input.Name, "source")
	if s.source.fn == nil {
		return &BuildError{
			Phase:     phaseBuild,
			Family:    errcode.FamilyForCode(sourceCallbackMissingCode),
			Code:      sourceCallbackMissingCode,
			Operation: "build input",
			Node:      node,
			Reason:    "custom source has no push callback",
			fixes: buildErrorFixes([]string{
				"pass a non-nil callback to goav.Source(name, shape, fn)",
				"use goav.FileInput or goav.Input(provider) for built-in source adapters",
			}),
			cause: errNilSource,
		}
	}
	spec := normalizeCustomSourceShape(node, s.source.shape)
	if spec.Domain != shape.DomainPacket && spec.Domain != shape.DomainFrame && spec.Domain != shape.DomainEvent {
		return &BuildError{
			Phase:     phaseBuild,
			Family:    errcode.FamilyForCode(sourceShapeUnsupportedCode),
			Code:      sourceShapeUnsupportedCode,
			Operation: "build input",
			Node:      node,
			Reason:    "custom recipe sources currently produce packet-domain, frame-domain, or event-domain media",
			fields:    errDetails(errDetail("actual_shape", spec.String())),
			fixes: buildErrorFixes([]string{
				"declare the source with shape.Packet(media, codec, ...)",
				"declare raw generated media with shape.Frame(media, ...)",
				"declare diagnostic or lifecycle sources with shape.Event(...)",
				"use goav.Sink(...) after decode or transform when observing frame-domain media",
			}),
			cause: errUnsupportedBuild,
		}
	}
	if spec.Domain != shape.DomainEvent && spec.MediaKind == "" {
		return &BuildError{
			Phase:     phaseBuild,
			Family:    errcode.FamilyForCode(sourceShapeInvalidCode),
			Code:      sourceShapeInvalidCode,
			Operation: "build input",
			Node:      node,
			Reason:    "custom source shape needs a media kind",
			fixes: buildErrorFixes([]string{
				"use shape.Packet(av.MediaAudio, codec) or shape.Packet(av.MediaVideo, codec)",
				"add shape.Media(...) when constructing a custom shape",
			}),
			cause: errUnsupportedBuild,
		}
	}
	return nil
}

func (s InputSpec) validatePlainInput() error {
	if s.provider != nil {
		return nil
	}
	if s.source != nil {
		return nil
	}
	if s.input.Name != "" || s.input.URI != "" || s.input.Protocol != "" || s.input.MIMEType != "" || s.input.Reader != nil || s.input.ReaderAt != nil {
		return nil
	}
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(inputInvalidCode),
		Code:      inputInvalidCode,
		Operation: "build input",
		Node:      "input",
		Reason:    "empty input spec",
		fixes: buildErrorFixes([]string{
			"use goav.FileInput(name, reader) for file-like input",
			"use goav.Source(name, shape, fn) for application-pushed packets",
			"use goav.Input(provider) for realtime receive through a source provider",
		}),
		cause: errUnsupportedBuild,
	}
}

// providerName returns the provider's node name capability ("" without one) so
// recipe naming, narrowing, and duplicate detection see provider-named inputs.
func (s InputSpec) providerName() string {
	if s.provider == nil {
		return ""
	}
	if named, ok := s.provider.(sourceProviderNamer); ok {
		return named.Name()
	}
	return ""
}

func (s InputSpec) intent() inputIntent {
	return inputIntent{
		Name:     firstNonEmpty(s.name, s.input.Name, s.providerName()),
		URI:      s.input.URI,
		Protocol: s.input.Protocol,
		MIMEType: s.input.MIMEType,
		Codec:    cloneCodecSpec(s.codec),
		Realtime: s.input.Realtime || (s.source != nil && s.source.shape.Realtime),
	}
}

func (s InputSpec) inputName(fallback string) string {
	return firstNonEmpty(s.name, s.input.Name, s.input.URI, s.providerName(), fallback)
}

func validateJobInputs(inputs []InputSpec) error {
	for i := range inputs {
		if err := inputs[i].validate(); err != nil {
			return err
		}
	}
	if len(inputs) <= 1 {
		return nil
	}
	for i := range inputs {
		if inputs[i].provider != nil || inputs[i].source != nil {
			continue
		}
		return &BuildError{
			Phase:     phaseBuild,
			Family:    errcode.FamilyForCode(multiInputUnsupportedCode),
			Code:      multiInputUnsupportedCode,
			Operation: "build job",
			Node:      firstNonEmpty(inputs[i].name, inputs[i].input.Name, inputs[i].input.URI, fmt.Sprintf("input-%d", i)),
			Reason:    "multiple recipe inputs currently require realtime source providers or custom sources",
			fixes: buildErrorFixes([]string{
				"use goav.From(goav.Input(...)).And(goav.Input(...)) for repeated live inputs",
				"build an explicit graph when combining multiple file or protocol sources",
			}),
			cause: errUnsupportedBuild,
		}
	}
	if err := validateRealtimeInputNames(inputs); err != nil {
		return err
	}
	return nil
}

func validateRealtimeInputNames(inputs []InputSpec) error {
	seen := make(map[string]int, len(inputs))
	for i := range inputs {
		name := inputs[i].inputName("")
		if name == "" {
			continue
		}
		if firstIndex, ok := seen[name]; ok {
			return duplicateInputNameError(name, firstIndex, i)
		}
		seen[name] = i
	}
	return nil
}

func duplicateInputNameError(name string, firstIndex int, secondIndex int) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(inputDuplicateCode),
		Code:      inputDuplicateCode,
		Operation: "build job",
		Node:      name,
		Reason:    fmt.Sprintf("realtime input name %q is defined more than once", name),
		fields:    errDetails(errNote(fmt.Sprintf("first input index: %d", firstIndex)), errNote(fmt.Sprintf("second input index: %d", secondIndex))),
		fixes: buildErrorFixes([]string{
			"give each repeated realtime input a distinct goav.Name(...) option",
			"use stable names such as \"audio\" and \"video\" for separate live streams",
		}),
		cause: errUnsupportedBuild,
	}
}

func validateInputFormatAdapters(ctx context.Context, rt *Runtime, inputs []InputSpec) ([]format.ProbeResult, error) {
	if rt == nil {
		return nil, nil
	}
	probes := make([]format.ProbeResult, len(inputs))
	for i := range inputs {
		if inputs[i].provider != nil {
			continue
		}
		if inputs[i].source != nil {
			probes[i] = customSourceProbeResult(inputs[i])
			continue
		}
		input := inputs[i].input
		result, err := rt.formats.Probe(ctx, inputProbeRequest(input))
		if err != nil {
			return nil, inputFormatProbeError(input, err)
		}
		probes[i] = result
		if _, err := rt.formats.DemuxerFactory(result.Format); err != nil {
			return nil, inputDemuxerMissingError(input, result.Format, err)
		}
	}
	return probes, nil
}
