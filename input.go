// Recipe inputs: InputSpec constructors, validation, and input format adapter checks.

package goav

import (
	"context"
	"fmt"
	"io"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/shape"
)

type InputSpec struct {
	input    format.Input
	provider SourceProvider
	source   *sourceInputSpec
	codec    codec.CodecSpec
	name     string
	realtime bool
	err      error
}

func FileInput(name string, reader io.Reader) InputSpec {
	return InputSpec{
		input: format.Input{
			Name:     name,
			Protocol: av.ProtocolFile,
			Reader:   reader,
		},
		name: name,
	}
}

func URI(uri string) InputSpec {
	return InputSpec{
		input: format.Input{
			Name: uri,
			URI:  uri,
		},
		name: uri,
	}
}

func (s InputSpec) Name(name string) InputSpec {
	s.name = name
	s.input.Name = name
	return s
}

func (s InputSpec) MIME(mimeType string) InputSpec {
	s.input.MIMEType = mimeType
	return s
}

func (s InputSpec) Codec(codec codec.CodecSpec) InputSpec {
	s.codec = cloneCodecSpec(codec)
	return s
}

func (s InputSpec) formatInput() format.Input {
	input := s.input
	input.Realtime = input.Realtime || s.realtime
	return input
}

func (s InputSpec) validate() error {
	if s.err != nil {
		return &BuildError{
			Code:      CodeInputInvalid,
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
			Code:      CodeSourceCallbackMissing,
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
			Code:      CodeSourceShapeUnsupported,
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
			Code:      CodeSourceShapeInvalid,
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
		Code:      CodeInputInvalid,
		Operation: "build input",
		Node:      "input",
		Reason:    "empty input spec",
		Suggestions: []string{
			"use goav.FileInput(name, reader) for file-like input",
			"use goav.URI(uri) for URI-backed input",
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
			Code:      CodeMultiInputUnsupported,
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
		Code:      CodeInputDuplicate,
		Operation: "build job",
		Node:      name,
		Reason:    fmt.Sprintf("realtime input name %q is defined more than once", name),
		Details: []string{
			fmt.Sprintf("first input index: %d", firstIndex),
			fmt.Sprintf("second input index: %d", secondIndex),
		},
		Suggestions: []string{
			"give each repeated realtime input a distinct .Name(...)",
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
