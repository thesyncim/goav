// Recipe destinations: Destination constructors, destinationSpec machinery, naming, and validation.

package goav

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/provider"
)

// destinationSpec describes a concrete file, URI, writer, or sink destination.
type destinationSpec struct {
	id             uint64
	group          string
	output         format.Output
	sink           pipeline.Sink
	custom         provider.Destination
	format         av.FormatID
	resolvedFormat av.FormatID
	name           string
	err            error
}

func destinationHandle(spec destinationSpec) Destination {
	return Destination{spec: spec}
}

// applyDestinationOptions is the destination twin of applyInputOptions: one
// option-application path shared by every constructor and Destination.With.
func applyDestinationOptions(spec destinationSpec, opts []DestinationOption) destinationSpec {
	for i := range opts {
		if opts[i] != nil {
			opts[i].applyDestination(&spec)
		}
	}
	return spec
}

func applyMuxOptions(spec destinationSpec, opts []MuxOption) destinationSpec {
	for i := range opts {
		if opts[i] != nil {
			opts[i].applyMux(&spec)
		}
	}
	return spec
}

// File creates a writer-backed destination from an already-open writer. When
// the writer also implements io.Closer it is closed exactly once when the
// destination finalizes (run end, drained detach, or failure); a plain writer
// is left open for the caller.
func File(name string, writer io.Writer, opts ...DestinationOption) Destination {
	return Destination{spec: applyDestinationOptions(fileDestination(name, writer), opts)}
}

func fileDestination(name string, writer io.Writer) destinationSpec {
	return destinationSpec{
		id: destinationSpecSeq.Add(1),
		output: format.Output{
			Name:     name,
			Protocol: av.ProtocolFile,
			Writer:   writer,
		},
		name: name,
	}
}

// URI creates a URI destination opened by a registered format adapter.
func URI(uri string, opts ...DestinationOption) Destination {
	return Destination{spec: applyDestinationOptions(uriDestination(uri), opts)}
}

func uriDestination(uri string) destinationSpec {
	return destinationSpec{
		id: destinationSpecSeq.Add(1),
		output: format.Output{
			Name: uri,
			URI:  uri,
		},
		name: uri,
	}
}

// Sink creates a media-message destination for decoded frames or packets.
// Like every destination constructor it accepts options; Name overrides the
// label outputs group and dedupe by (byte-stream options such as Format and
// MIME state nothing about a sink).
func Sink(sink pipeline.Sink, opts ...DestinationOption) Destination {
	return Destination{spec: applyDestinationOptions(sinkDestination(sink), opts)}
}

func sinkDestination(sink pipeline.Sink) destinationSpec {
	name := ""
	if sink != nil {
		name = sink.Name()
	}
	if sink == nil {
		return destinationSpec{id: destinationSpecSeq.Add(1), err: ErrNilSink}
	}
	return destinationSpec{id: destinationSpecSeq.Add(1), sink: sink, name: name}
}

// Custom wraps a reusable provider.Destination as a destination value: the
// provider owns naming, contract, and opening, while the returned Destination
// stays the stable routing handle branches share.
func Custom(name string, dest provider.Destination, opts ...DestinationOption) Destination {
	return Destination{spec: applyDestinationOptions(customDestination(name, dest), opts)}
}

func customDestination(name string, dest provider.Destination) destinationSpec {
	if dest == nil {
		return destinationSpec{id: destinationSpecSeq.Add(1), err: ErrNilWriter}
	}
	contract := dest.Contract()
	spec := destinationSpec{
		id:     destinationSpecSeq.Add(1),
		custom: dest,
		output: format.Output{
			Name:     name,
			URI:      name,
			Protocol: contract.Protocol,
			Realtime: contract.Realtime,
		},
		name: firstNonEmpty(name, dest.Name()),
	}
	if spec.output.Name == "" {
		spec.output.Name = spec.name
		spec.output.URI = spec.name
	}
	if len(contract.Formats) != 0 {
		spec.format = contract.Formats[0]
	}
	if len(contract.MIMETypes) != 0 {
		spec.output.MIMEType = contract.MIMETypes[0]
	}
	return spec
}

// Writer creates a byte destination opened on demand: the callback runs after
// goav has selected the format and streams, so the writer sees the final
// destination metadata (provider.Info). The writer closes exactly once. When
// the returned writer also implements provider.TransactionalWriter, Commit
// runs after a successful run or drained detach and Abort runs on failure —
// the seam for object-store uploads with an explicit commit boundary.
func Writer(name string, open provider.OpenFunc, opts ...DestinationOption) Destination {
	spec := destinationSpec{
		id:     destinationSpecSeq.Add(1),
		custom: writerDestination{name: name, open: open},
		output: format.Output{
			Name: name,
			URI:  name,
		},
		name: name,
	}
	if open == nil {
		spec.custom = nil
		spec.err = ErrNilWriter
	}
	return Destination{spec: applyDestinationOptions(spec, opts)}
}

type writerDestination struct {
	name     string
	open     provider.OpenFunc
	contract provider.Contract
}

func (d writerDestination) Name() string {
	return d.name
}

func (d writerDestination) Contract() provider.Contract {
	contract := d.contract
	contract.ByteStream = true
	return contract
}

func (d writerDestination) Open(ctx context.Context, info provider.Info) (provider.Writer, error) {
	if d.open == nil {
		return nil, ErrNilWriter
	}
	writer, err := d.open(ctx, info)
	if err != nil {
		return nil, err
	}
	if writer == nil {
		return nil, ErrNilWriter
	}
	return writer, nil
}

type nopDestinationWriter struct {
	io.Writer
}

func (w nopDestinationWriter) Close() error {
	return nil
}

// MediaOption is a direction-agnostic option accepted by both input and
// destination constructors: Name, MIME, and Metadata state the same fact
// about either end of a recipe, so goav.FileInput(name, r, goav.MIME(...))
// and goav.File(name, w, goav.MIME(...)) share one vocabulary. It is sealed —
// only goav option constructors implement it.
type MediaOption interface {
	InputOption
	DestinationOption
}

// mediaOption is the concrete direction-agnostic option: one apply function
// per direction, each acting on that direction's spec.
type mediaOption struct {
	input       func(*InputSpec)
	destination func(*destinationSpec)
}

func (o mediaOption) applyInput(spec *InputSpec) {
	if spec != nil && o.input != nil {
		o.input(spec)
	}
}

func (o mediaOption) applyDestination(spec *destinationSpec) {
	if spec != nil && o.destination != nil {
		o.destination(spec)
	}
}

// destinationOption is the concrete destination-only option (Format).
type destinationOption func(*destinationSpec)

func (o destinationOption) applyDestination(spec *destinationSpec) {
	if spec != nil && o != nil {
		o(spec)
	}
}

type muxOption func(*destinationSpec)

func (o muxOption) applyMux(spec *destinationSpec) {
	if spec != nil && o != nil {
		o(spec)
	}
}

// Name overrides the value's label. On an input it is the name selector
// narrowing (InputName), errors, and Explain use; on a destination it is the
// label outputs group and dedupe by.
func Name(name string) MediaOption {
	return mediaOption{
		input: func(spec *InputSpec) {
			spec.name = name
			spec.input.Name = name
		},
		destination: func(spec *destinationSpec) {
			*spec = spec.withName(name)
		},
	}
}

// MIME sets the value's MIME type, which drives format probing when the name
// carries no extension: input MIME selects the demuxer, destination MIME
// selects the output format and is reported to destination openers.
func MIME(mimeType string) MediaOption {
	return mediaOption{
		input: func(spec *InputSpec) {
			spec.input.MIMEType = mimeType
		},
		destination: func(spec *destinationSpec) {
			*spec = spec.withMIME(mimeType)
		},
	}
}

// Metadata attaches caller metadata to the value: input metadata rides
// format.Input to the opening adapter, destination metadata reaches openers
// on provider.Info (an uploader can store it as object metadata).
func Metadata(metadata av.Metadata) MediaOption {
	return mediaOption{
		input: func(spec *InputSpec) {
			spec.input.Metadata = cloneMetadata(metadata)
		},
		destination: func(spec *destinationSpec) {
			spec.output.Metadata = cloneMetadata(metadata)
		},
	}
}

// Format pins the destination's container format explicitly instead of
// probing it from the name or MIME type. It is destination-only: input
// containers are always probed from the name, MIME type, or leading bytes.
func Format(format av.FormatID) DestinationOption {
	return destinationOption(func(spec *destinationSpec) {
		*spec = spec.withFormat(format)
	})
}

// destinationGroup marks destinations as the same logical mux/sink group for
// Mux. It stays internal so the public grouping model has one constructor.
func destinationGroup(name string) DestinationOption {
	return destinationOption(func(spec *destinationSpec) {
		if name == "" {
			if spec.err == nil {
				spec.err = fmt.Errorf("destination group name is empty")
			}
			return
		}
		spec.group = name
	})
}

// Mux marks destination as a first-class logical mux/sink group. Independently
// built destinations wrapped with the same non-empty group name share one
// planned output when their destination settings are compatible. MuxOption is
// reserved for explicit group-level policies; callers usually pass no options.
func Mux(name string, destination Destination, opts ...MuxOption) Destination {
	spec := applyDestinationOptions(cloneDestinationSpec(destination.spec), []DestinationOption{destinationGroup(name)})
	return Destination{spec: applyMuxOptions(spec, opts)}
}

// With returns a copy of the destination with the options applied — the same
// option vocabulary the constructors take, for layering config onto an
// already-constructed value. The copy keeps the original's routing identity.
func (d Destination) With(opts ...DestinationOption) Destination {
	return Destination{spec: applyDestinationOptions(cloneDestinationSpec(d.spec), opts)}
}

func (s destinationSpec) withName(name string) destinationSpec {
	s.name = name
	if s.sink == nil {
		s.output.Name = name
	}
	return s
}

func (s destinationSpec) withMIME(mimeType string) destinationSpec {
	s.output.MIMEType = mimeType
	return s
}

func (s destinationSpec) withFormat(format av.FormatID) destinationSpec {
	s.format = format
	return s
}

func (s destinationSpec) withResolvedFormat(format av.FormatID) destinationSpec {
	s.resolvedFormat = format
	return s
}

func (s destinationSpec) Name() string {
	return s.name
}

func (s destinationSpec) Contract() provider.Contract {
	contract := provider.Contract{
		ByteStream: s.sink == nil,
		Protocol:   s.output.Protocol,
		Realtime:   s.output.Realtime,
	}
	if s.format != "" {
		contract.Formats = append(contract.Formats, s.format)
	}
	if s.resolvedFormat != "" && s.resolvedFormat != s.format {
		contract.Formats = append(contract.Formats, s.resolvedFormat)
	}
	if s.output.MIMEType != "" {
		contract.MIMETypes = append(contract.MIMETypes, s.output.MIMEType)
	}
	if s.custom != nil {
		customContract := s.custom.Contract()
		if contract.Protocol == "" {
			contract.Protocol = customContract.Protocol
		}
		contract.Seekable = customContract.Seekable
		contract.Realtime = contract.Realtime || customContract.Realtime
		if len(contract.Formats) == 0 {
			contract.Formats = append(contract.Formats, customContract.Formats...)
		}
		if len(contract.MIMETypes) == 0 {
			contract.MIMETypes = append(contract.MIMETypes, customContract.MIMETypes...)
		}
	}
	return contract
}

func (s destinationSpec) Open(ctx context.Context, info provider.Info) (provider.Writer, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.custom != nil {
		return s.custom.Open(ctx, info)
	}
	if s.output.Writer != nil {
		if closer, ok := s.output.Writer.(io.WriteCloser); ok {
			return closer, nil
		}
		return nopDestinationWriter{Writer: s.output.Writer}, nil
	}
	return nil, destinationInvalidError("open destination", firstNonEmpty(info.Name, s.name, "destination"), "destination does not provide a writer")
}

func (s destinationSpec) validate(operation string, fallback string) error {
	node := s.label(fallback)
	if s.err != nil {
		suggestions := []string{
			"pass a non-nil sink to goav.Sink(...)",
			"use goav.File(...) or goav.URI(...) for muxed output",
		}
		if errors.Is(s.err, ErrNilWriter) {
			suggestions = []string{
				"pass a non-nil writer callback to goav.Writer(...)",
				"use goav.File(...) or goav.URI(...) for muxed output",
			}
		}
		return &BuildError{
			Family:    errcode.FamilyForCode(errcode.OutputInvalid),
			Code:      errcode.OutputInvalid,
			Operation: operation,
			Node:      node,
			Reason:    s.err.Error(),
			Fixes:     buildErrorFixes(suggestions),
			Cause:     s.err,
		}
	}
	if s.sink != nil {
		if err := validateSinkComponent(s.sink); err != nil {
			return &BuildError{
				Family:    errcode.FamilyForCode(errcode.OutputInvalid),
				Code:      errcode.OutputInvalid,
				Operation: operation,
				Node:      node,
				Reason:    err.Error(),
				Fixes: buildErrorFixes([]string{
					"pass a non-nil sink callback to component.SinkFunc(...)",
					"pass a non-nil sink to goav.Sink(...)",
					"use goav.File(...) or goav.URI(...) for muxed output",
				}),
				Cause: err,
			}
		}
		return nil
	}
	if s.output.Name == "" && s.output.URI == "" && s.output.Protocol == "" && s.output.MIMEType == "" && s.output.Writer == nil && s.custom == nil && s.format == "" {
		return &BuildError{
			Family:    errcode.FamilyForCode(errcode.OutputInvalid),
			Code:      errcode.OutputInvalid,
			Operation: operation,
			Node:      node,
			Reason:    "empty destination",
			Fixes: buildErrorFixes([]string{
				"use goav.File(name, writer) for muxed output",
				"use goav.Sink(sink) for decoded frames or packets",
			}),
			Cause: ErrUnsupportedBuild,
		}
	}
	if s.output.Protocol == av.ProtocolFile && s.output.Writer == nil && s.custom == nil {
		return &BuildError{
			Family:    errcode.FamilyForCode(errcode.OutputWriterMissing),
			Code:      errcode.OutputWriterMissing,
			Operation: operation,
			Node:      node,
			Reason:    "file output has no writer",
			Fixes: buildErrorFixes([]string{
				"pass a non-nil io.Writer to goav.File(name, writer)",
				"use goav.URI(uri) when the output is opened by an adapter",
			}),
			Cause: ErrUnsupportedBuild,
		}
	}
	if s.output.Protocol == av.ProtocolFile && s.output.Writer != nil && s.output.Name == "" && s.output.URI == "" && s.output.MIMEType == "" && s.format == "" {
		return &BuildError{
			Family:    errcode.FamilyForCode(errcode.OutputFormatMissing),
			Code:      errcode.OutputFormatMissing,
			Operation: operation,
			Node:      node,
			Reason:    "writer-backed file output has no name, URI, MIME type, or explicit format",
			Fixes: buildErrorFixes([]string{
				"give goav.File(name, writer) a name with a container extension",
				"pass goav.Format(...) to goav.File(...) when the writer has no filename",
			}),
			Cause: ErrUnsupportedBuild,
		}
	}
	if s.output.URI == "" && s.output.Protocol != av.ProtocolFile && s.output.Writer == nil && s.custom == nil {
		return &BuildError{
			Family:    errcode.FamilyForCode(errcode.OutputDestinationMissing),
			Code:      errcode.OutputDestinationMissing,
			Operation: operation,
			Node:      node,
			Reason:    "output has no URI, writer, or sink",
			Fixes: buildErrorFixes([]string{
				"use goav.File(name, writer) for writer-backed output",
				"use goav.URI(uri) for URI-backed output",
			}),
			Cause: ErrUnsupportedBuild,
		}
	}
	return nil
}

func (s destinationSpec) label(fallback string) string {
	return firstNonEmpty(s.name, s.output.Name, s.output.URI, fallback)
}

func (s destinationSpec) intent() destinationIntent {
	return destinationIntent{
		Name:     s.label("output"),
		URI:      s.output.URI,
		Protocol: s.output.Protocol,
		MIMEType: s.output.MIMEType,
		Format:   s.format,
	}
}

func (s destinationSpec) intentWithName(name string) destinationIntent {
	intent := s.intent()
	intent.Name = firstNonEmpty(name, intent.Name)
	return intent
}

func validateDestinationSpecs(operation string, outputs []destinationSpec, destinationNames ...string) error {
	seen := make(map[string]bool, len(outputs))
	for i := range outputs {
		fallback := fmt.Sprintf("output-%d", i)
		if err := outputs[i].validate(operation, fallback); err != nil {
			return err
		}
		name := jobOutputDestinationName(outputs, destinationNames, i)
		destinationNamed := i < len(destinationNames) && destinationNames[i] != ""
		if previousTargetNamed, ok := seen[name]; ok {
			if destinationNamed || previousTargetNamed {
				return duplicateDestinationHandleError(operation, name)
			}
			return duplicateOutputError(operation, name)
		}
		seen[name] = destinationNamed
	}
	return nil
}

func validateOutputFormatAdapters(ctx context.Context, rt *Runtime, outputs []destinationSpec, destinationNames ...string) ([]destinationSpec, error) {
	resolved := append([]destinationSpec(nil), outputs...)
	if rt == nil {
		return resolved, nil
	}
	for i := range resolved {
		if resolved[i].sink != nil {
			continue
		}
		formatID := resolved[i].format
		if formatID == "" {
			result, err := rt.formats.Probe(ctx, outputProbeRequest(resolved[i].output))
			if err != nil {
				return nil, destinationFormatProbeError(destinationNodeName(resolved[i].output, i, destinationNames), resolved[i].output, err)
			}
			formatID = result.Format
			resolved[i] = resolved[i].withResolvedFormat(formatID)
		}
		if _, err := rt.formats.MuxerFactory(formatID); err != nil {
			return nil, destinationMuxerMissingError(destinationNodeName(resolved[i].output, i, destinationNames), resolved[i].output, formatID, err)
		}
	}
	return resolved, nil
}

func destinationNodeName(output format.Output, index int, destinationNames []string) string {
	if index >= 0 && index < len(destinationNames) && destinationNames[index] != "" {
		return destinationNames[index]
	}
	return muxNodeName(output, index)
}

func duplicateOutputError(operation string, name string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.OutputDuplicate),
		Code:      errcode.OutputDuplicate,
		Operation: operation,
		Node:      name,
		Reason:    fmt.Sprintf("output name %q is defined more than once", name),
		Fixes: buildErrorFixes([]string{
			"use a unique output name for each output in the recipe",
			"remove repeated outputs when one output should receive the stream once",
			"pass goav.Name(...) to outputs or choose distinct sink names when labels should differ",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func duplicateDestinationHandleError(operation string, name string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.DestinationDuplicate),
		Code:      errcode.DestinationDuplicate,
		Operation: operation,
		Node:      name,
		Reason:    fmt.Sprintf("destination %q is attached more than once", name),
		Fixes: buildErrorFixes([]string{
			"list each destination value once in .To(...)",
			"use distinct destination names when writing to separate destinations",
			"wrap grouped destinations with goav.Mux(name, destination)",
			"reuse one destination value only as compatibility sugar for local recipes",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func destinationNamesWithOverrides(outputs []destinationSpec, destinationNames []string) []string {
	names := make([]string, 0, len(outputs))
	for i := range outputs {
		names = append(names, jobOutputDestinationName(outputs, destinationNames, i))
	}
	return names
}

func jobOutputDestinationName(outputs []destinationSpec, destinationNames []string, index int) string {
	if index >= 0 && index < len(destinationNames) && destinationNames[index] != "" {
		return destinationNames[index]
	}
	return outputs[index].label(fmt.Sprintf("output-%d", index))
}

func firstNonEmpty(values ...string) string {
	for i := range values {
		if values[i] != "" {
			return values[i]
		}
	}
	return ""
}
