// Recipe inputs: InputSpec constructors, validation, and input format adapter checks.

package goav

import (
	"context"
	"fmt"
	"io"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/codes"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/shape"
)

// InputSpec is one declared job input: a file, URI, custom source, or source
// provider, plus the optional name, MIME, and codec facts the planner uses
// before opening it. Construct one with FileInput, URIInput, Source, or
// Input; configure it with options (Name, MIME, Metadata, Codec).
type InputSpec struct {
	input    format.Input
	provider SourceProvider
	source   *sourceInputSpec
	codec    codec.CodecSpec
	name     string
	realtime bool
	err      error
}

// InputOption configures an input value (FileInput, URIInput, Source, Input,
// or InputSpec.With): Codec declares the stream's codec, and the
// direction-agnostic MediaOptions (Name, MIME, Metadata) satisfy it too. It
// is sealed — only goav option constructors implement it.
type InputOption interface {
	applyInput(*InputSpec)
}

// inputOption is the concrete input-only option (Codec).
type inputOption func(*InputSpec)

func (o inputOption) applyInput(spec *InputSpec) {
	if spec != nil && o != nil {
		o(spec)
	}
}

// Codec declares the input's codec when probing cannot discover it — typical
// for live receives where the transport negotiated the codec out of band. It
// is input-only: destinations carry container facts (Format, MIME), never a
// stream codec.
func Codec(spec codec.CodecSpec) InputOption {
	return inputOption(func(input *InputSpec) {
		input.codec = cloneCodecSpec(spec)
	})
}

// With returns a copy of the input with the options applied — the same option
// vocabulary the constructors take, for layering config onto an
// already-constructed value (renaming a generated test input, say).
func (s InputSpec) With(opts ...InputOption) InputSpec {
	return applyInputOptions(s, opts)
}

func applyInputOptions(spec InputSpec, opts []InputOption) InputSpec {
	for i := range opts {
		if opts[i] != nil {
			opts[i].applyInput(&spec)
		}
	}
	return spec
}

// FileInput declares a file-like input read from reader; name carries the
// extension format probing uses (a .ivf name selects the IVF demuxer).
func FileInput(name string, reader io.Reader, opts ...InputOption) InputSpec {
	return applyInputOptions(InputSpec{
		input: format.Input{
			Name:     name,
			Protocol: av.ProtocolFile,
			Reader:   reader,
		},
		name: name,
	}, opts)
}

// URIInput declares an input opened by a registered format adapter from a
// URI.
func URIInput(uri string, opts ...InputOption) InputSpec {
	return applyInputOptions(InputSpec{
		input: format.Input{
			Name: uri,
			URI:  uri,
		},
		name: uri,
	}, opts)
}

func (s InputSpec) formatInput() format.Input {
	input := s.input
	input.Realtime = input.Realtime || s.realtime
	return input
}

func (s InputSpec) validate() error {
	if s.err != nil {
		return &BuildError{
			Code:      codes.InputInvalid,
			Operation: "build input",
			Node:      firstNonEmpty(s.name, s.input.Name, s.input.URI, "input"),
			Reason:    s.err.Error(),
			Suggestions: []string{
				"check the input constructor arguments",
				"pass a non-nil provider to goav.Input(provider)",
			},
			Cause: s.err,
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
			Code:      codes.SourceCallbackMissing,
			Operation: "build input",
			Node:      node,
			Reason:    "custom source has no push callback",
			Suggestions: []string{
				"pass a non-nil callback to goav.Source(name, shape, fn)",
				"use goav.FileInput or goav.Input(provider) for built-in source adapters",
			},
			Cause: ErrNilSource,
		}
	}
	spec := normalizeCustomSourceShape(node, s.source.shape)
	if spec.Domain != shape.DomainPacket && spec.Domain != shape.DomainFrame && spec.Domain != shape.DomainEvent {
		return &BuildError{
			Code:      codes.SourceShapeUnsupported,
			Operation: "build input",
			Node:      node,
			Reason:    "custom recipe sources currently produce packet-domain, frame-domain, or event-domain media",
			Details: []string{
				"actual_shape=" + spec.String(),
			},
			Suggestions: []string{
				"declare the source with shape.Packet(media, codec, ...)",
				"declare raw generated media with shape.Frame(media, ...)",
				"declare diagnostic or lifecycle sources with shape.Event(...)",
				"use goav.Sink(...) after decode or transform when observing frame-domain media",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
	if spec.Domain != shape.DomainEvent && spec.MediaKind == "" {
		return &BuildError{
			Code:      codes.SourceShapeInvalid,
			Operation: "build input",
			Node:      node,
			Reason:    "custom source shape needs a media kind",
			Suggestions: []string{
				"use shape.Packet(av.MediaAudio, codec) or shape.Packet(av.MediaVideo, codec)",
				"add shape.Media(...) when constructing a custom shape",
			},
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
		Code:      codes.InputInvalid,
		Operation: "build input",
		Node:      "input",
		Reason:    "empty input spec",
		Suggestions: []string{
			"use goav.FileInput(name, reader) for file-like input",
			"use goav.URIInput(uri) for URI-backed input",
			"use goav.Source(name, shape, fn) for application-pushed packets",
			"use goav.Input(provider) for realtime receive through a source provider",
		},
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
			Code:      codes.MultiInputUnsupported,
			Operation: "build job",
			Node:      firstNonEmpty(inputs[i].name, inputs[i].input.Name, inputs[i].input.URI, fmt.Sprintf("input-%d", i)),
			Reason:    "multiple recipe inputs currently require realtime source providers or custom sources",
			Suggestions: []string{
				"use goav.From(goav.Input(...)).And(goav.Input(...)) for repeated live inputs",
				"build an explicit graph when combining multiple file or protocol sources",
			},
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
		Code:      codes.InputDuplicate,
		Operation: "build job",
		Node:      name,
		Reason:    fmt.Sprintf("realtime input name %q is defined more than once", name),
		Details: []string{
			fmt.Sprintf("first input index: %d", firstIndex),
			fmt.Sprintf("second input index: %d", secondIndex),
		},
		Suggestions: []string{
			"give each repeated realtime input a distinct goav.Name(...) option",
			"use stable names such as \"audio\" and \"video\" for separate live streams",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func validateInputFormatAdapters(ctx context.Context, rt Runtime, inputs []InputSpec) ([]format.ProbeResult, error) {
	standard, ok := rt.(*runtime)
	if !ok || standard == nil {
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
		result, err := standard.formats.Probe(ctx, inputProbeRequest(input))
		if err != nil {
			return nil, inputFormatProbeError(input, err)
		}
		probes[i] = result
		if _, err := standard.formats.DemuxerFactory(result.Format); err != nil {
			return nil, inputDemuxerMissingError(input, result.Format, err)
		}
	}
	return probes, nil
}
