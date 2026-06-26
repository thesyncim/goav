// Recipe inputs: InputSpec constructors, validation, and input format adapter checks.

package goav

import (
	"context"
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

// InputSpec is one declared job input: a file, URI, custom source, or source
// provider, plus the optional name and MIME facts the planner uses
// before opening it. Construct one with FileInput, URIInput, Source, or
// Input; configure it with options (Name, MIME, Metadata) and decorate the
// opened source with WrapSource. Packet/frame codec facts come from a custom
// Source or provider SourceShape declaration. The zero value is intentionally
// not a valid input; construct input specs with FileInput, URIInput, Source,
// or Input.
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

// inputOptionValue configures an input value (FileInput, URIInput, Source,
// Input, or InputSpec.With). The direction-agnostic media options (Name, MIME,
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
	return applyInputOptions(inputSpecHandle(InputSpec{
		input: format.Input{
			Name:     name,
			Protocol: av.ProtocolFile,
			Reader:   reader,
		},
		name: name,
	}), opts)
}

// URIInput declares an input opened by a registered format adapter from a
// URI.
func URIInput(uri string, opts ...inputOptionValue) InputSpec {
	return applyInputOptions(inputSpecHandle(InputSpec{
		input: format.Input{
			Name: uri,
			URI:  uri,
		},
		name: uri,
	}), opts)
}

func (s InputSpec) formatInput() format.Input {
	input := s.input
	input.Realtime = input.Realtime || s.realtime
	return input
}

func (s InputSpec) validate() error {
	if s.err != nil {
		return &BuildError{
			Family:    errcode.FamilyForCode(errcode.InputInvalid),
			Code:      errcode.InputInvalid,
			Operation: "build input",
			Node:      firstNonEmpty(s.name, s.input.Name, s.input.URI, "input"),
			Reason:    s.err.Error(),
			Fixes: buildErrorFixes([]string{
				"check the input constructor arguments",
				"pass a non-nil provider to goav.Input(provider)",
			}),
			Cause: s.err,
		}
	}
	if s.origin != inputSpecOriginConstructed {
		return &BuildError{
			Family:    errcode.FamilyForCode(errcode.InputInvalid),
			Code:      errcode.InputInvalid,
			Operation: "build input",
			Node:      firstNonEmpty(s.name, s.input.Name, s.input.URI, "input"),
			Reason:    "empty input spec",
			Fixes: buildErrorFixes([]string{
				"use goav.FileInput(name, reader) for file-like input",
				"use goav.URIInput(uri) for URI-backed input",
				"use goav.Source(name, shape, fn) for application-pushed packets",
				"use goav.Input(provider) for realtime receive through a source provider",
			}),
			Cause: ErrUnsupportedBuild,
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
			Family:    errcode.FamilyForCode(errcode.SourceCallbackMissing),
			Code:      errcode.SourceCallbackMissing,
			Operation: "build input",
			Node:      node,
			Reason:    "custom source has no push callback",
			Fixes: buildErrorFixes([]string{
				"pass a non-nil callback to goav.Source(name, shape, fn)",
				"use goav.FileInput or goav.Input(provider) for built-in source adapters",
			}),
			Cause: ErrNilSource,
		}
	}
	spec := normalizeCustomSourceShape(node, s.source.shape)
	if spec.Domain != shape.DomainPacket && spec.Domain != shape.DomainFrame && spec.Domain != shape.DomainEvent {
		return &BuildError{
			Family:    errcode.FamilyForCode(errcode.SourceShapeUnsupported),
			Code:      errcode.SourceShapeUnsupported,
			Operation: "build input",
			Node:      node,
			Reason:    "custom recipe sources currently produce packet-domain, frame-domain, or event-domain media",
			Fields: buildErrorFields([]string{
				"actual_shape=" + spec.String(),
			}),
			Fixes: buildErrorFixes([]string{
				"declare the source with shape.Packet(media, codec, ...)",
				"declare raw generated media with shape.Frame(media, ...)",
				"declare diagnostic or lifecycle sources with shape.Event(...)",
				"use goav.Sink(...) after decode or transform when observing frame-domain media",
			}),
			Cause: ErrUnsupportedBuild,
		}
	}
	if spec.Domain != shape.DomainEvent && spec.MediaKind == "" {
		return &BuildError{
			Family:    errcode.FamilyForCode(errcode.SourceShapeInvalid),
			Code:      errcode.SourceShapeInvalid,
			Operation: "build input",
			Node:      node,
			Reason:    "custom source shape needs a media kind",
			Fixes: buildErrorFixes([]string{
				"use shape.Packet(av.MediaAudio, codec) or shape.Packet(av.MediaVideo, codec)",
				"add shape.Media(...) when constructing a custom shape",
			}),
			Cause: ErrUnsupportedBuild,
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
		Family:    errcode.FamilyForCode(errcode.InputInvalid),
		Code:      errcode.InputInvalid,
		Operation: "build input",
		Node:      "input",
		Reason:    "empty input spec",
		Fixes: buildErrorFixes([]string{
			"use goav.FileInput(name, reader) for file-like input",
			"use goav.URIInput(uri) for URI-backed input",
			"use goav.Source(name, shape, fn) for application-pushed packets",
			"use goav.Input(provider) for realtime receive through a source provider",
		}),
		Cause: ErrUnsupportedBuild,
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
			Family:    errcode.FamilyForCode(errcode.MultiInputUnsupported),
			Code:      errcode.MultiInputUnsupported,
			Operation: "build job",
			Node:      firstNonEmpty(inputs[i].name, inputs[i].input.Name, inputs[i].input.URI, fmt.Sprintf("input-%d", i)),
			Reason:    "multiple recipe inputs currently require realtime source providers or custom sources",
			Fixes: buildErrorFixes([]string{
				"use goav.From(goav.Input(...)).And(goav.Input(...)) for repeated live inputs",
				"build an explicit graph when combining multiple file or protocol sources",
			}),
			Cause: ErrUnsupportedBuild,
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
		Family:    errcode.FamilyForCode(errcode.InputDuplicate),
		Code:      errcode.InputDuplicate,
		Operation: "build job",
		Node:      name,
		Reason:    fmt.Sprintf("realtime input name %q is defined more than once", name),
		Fields: buildErrorFields([]string{
			fmt.Sprintf("first input index: %d", firstIndex),
			fmt.Sprintf("second input index: %d", secondIndex),
		}),
		Fixes: buildErrorFixes([]string{
			"give each repeated realtime input a distinct goav.Name(...) option",
			"use stable names such as \"audio\" and \"video\" for separate live streams",
		}),
		Cause: ErrUnsupportedBuild,
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
