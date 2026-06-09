package goav

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

var destinationRefSeq atomic.Uint64
var destinationSpecSeq atomic.Uint64

// Destination is an opaque handle for a file, URI, object writer, media sink,
// or shared mux/sink group. Built-in constructors and Custom return destination
// values with goav-owned routing identity.
type Destination struct {
	spec destinationSpec
}

// DestinationProvider opens a custom destination behind a goav-owned
// Destination handle.
type DestinationProvider interface {
	Name() string
	Contract() DestinationContract
	Open(context.Context, DestinationInfo) (DestinationWriter, error)
}

// DestinationWriter is the byte writer returned by custom byte destinations.
type DestinationWriter interface {
	io.Writer
	Close() error
}

// TransactionalDestinationWriter may be returned by destinations that need an
// explicit upload commit or abort boundary.
type TransactionalDestinationWriter interface {
	DestinationWriter
	Commit(context.Context) error
	Abort(context.Context) error
}

type DestinationContract struct {
	ByteStream bool
	Seekable   bool
	Realtime   bool
	Protocol   av.ProtocolID
	Formats    []av.FormatID
	MIMETypes  []string
}

// DestinationInfo describes the resolved output context passed to custom
// destination open functions.
type DestinationInfo struct {
	Name     string
	Format   av.FormatID
	MIMEType string
	Streams  []av.Stream
	Metadata av.Metadata
	Realtime bool
}

type DestinationOption func(*destinationSpec)

type WriterOpenFunc func(context.Context, DestinationInfo) (io.WriteCloser, error)

type ObjectOpenFunc func(context.Context, DestinationInfo) (TransactionalDestinationWriter, error)

type destinationBinding struct {
	dest      destinationSpec
	hasDirect bool
}

type destinationRef struct {
	name string
	dest destinationSpec
	id   uint64
	err  error
}

func newDirectDestinationRef(name string, dest destinationSpec) destinationRef {
	if name == "" {
		return destinationRef{dest: dest, err: destinationNameMissingError(dest)}
	}
	id := dest.id
	if id == 0 {
		id = destinationRefSeq.Add(1)
	}
	return destinationRef{
		name: name,
		dest: dest.withName(firstNonEmpty(dest.name, name)),
		id:   id,
	}
}

func destinationBindingFromDestination(dest Destination) (destinationBinding, error) {
	direct, err := destinationSpecFromDestination(dest)
	if err != nil {
		return destinationBinding{}, err
	}
	return destinationBinding{dest: direct, hasDirect: true}, nil
}

func destinationSpecFromDestination(dest Destination) (destinationSpec, error) {
	if destinationSpecEmpty(dest.spec) {
		return destinationSpec{}, fmt.Errorf("destination is empty")
	}
	return cloneDestinationSpec(dest.spec), nil
}

func destinationSpecEmpty(dest destinationSpec) bool {
	return dest.sink == nil &&
		dest.custom == nil &&
		dest.output.Name == "" &&
		dest.output.URI == "" &&
		dest.output.Protocol == "" &&
		dest.output.MIMEType == "" &&
		dest.output.Writer == nil &&
		dest.format == "" &&
		dest.resolvedFormat == "" &&
		dest.name == "" &&
		dest.err == nil
}

type BranchSpec struct {
	name             string
	media            av.MediaType
	operations       []OperationSpec
	destinations     []destinationRef
	destinationNames []string

	from         string
	tap          string
	tapDomain    MediaDomain
	policy       pipeline.RoutePolicy
	label        string
	branchBuffer BranchBuffer
	buffer       pipeline.BufferPolicy

	err error
}

type branchBuilder struct {
	spec BranchSpec
}

type branchSourceBinding struct {
	from      string
	tap       string
	tapDomain MediaDomain
	policy    pipeline.RoutePolicy
	label     string
}

type branchSource interface {
	branchSource() branchSourceBinding
}

func Branch(name string) *branchBuilder {
	return &branchBuilder{spec: BranchSpec{name: name}}
}

func (b *branchBuilder) From(source branchSource) *branchBuilder {
	if b == nil {
		return b
	}
	if source == nil {
		b.setErr(branchSourceInvalidError(firstNonEmpty(b.spec.name, "branch")))
		return b
	}
	binding := source.branchSource()
	b.spec.from = binding.from
	b.spec.tap = binding.tap
	b.spec.tapDomain = binding.tapDomain
	b.spec.policy = binding.policy
	b.spec.label = binding.label
	return b
}

func (b *branchBuilder) Stream(stream av.StreamID) *branchBuilder {
	if b == nil {
		return b
	}
	b.spec.policy = pipeline.RouteByStream
	b.spec.label = string(stream)
	return b
}

func (b *branchBuilder) Event(event av.EventType) *branchBuilder {
	if b == nil {
		return b
	}
	b.spec.policy = pipeline.RouteByEvent
	b.spec.label = string(event)
	return b
}

func (b *branchBuilder) Buffer(buffer BranchBuffer) *branchBuilder {
	if b == nil {
		return b
	}
	if err := buffer.validate("build branch", firstNonEmpty(b.spec.name, "branch")); err != nil {
		b.setErr(err)
		return b
	}
	b.spec.branchBuffer = buffer
	b.spec.buffer = buffer.pipelinePolicy()
	return b
}

func (b *branchBuilder) Decode(options ...CodecOption) *branchBuilder {
	if b == nil {
		return b
	}
	if codecIntentSet(chainEncodeSpec(b.spec.operations)) {
		b.setErr(chainStepAfterEncodeError("build branch", firstNonEmpty(b.spec.name, "branch"), "decode", chainEncodeSpec(b.spec.operations)))
		return b
	}
	if chainHasDecode(b.spec.operations) {
		b.setErr(duplicateBranchDecodeError(firstNonEmpty(b.spec.name, "branch")))
		return b
	}
	if branchOperationSpecsContainStep(b.spec.operations) {
		b.setErr(branchDecodeOrderError(firstNonEmpty(b.spec.name, "branch")))
		return b
	}
	decodeCodec := mergeDecodeCodecSpec(CodecSpec{}, codecSpecFromOptions(options...))
	b.spec.operations = append(b.spec.operations, operationSpecForDecode(decodeCodec, string(decodeCodec.ID)))
	return b
}

func (b *branchBuilder) Apply(flow Chain) *branchBuilder {
	if b == nil {
		return b
	}
	spec, err := chainSpecFrom(flow)
	if err != nil {
		b.setErr(err)
		return b
	}
	specSteps := chainStepsFromChainOperations(spec.operations)
	if codecIntentSet(chainEncodeSpec(b.spec.operations)) && (chainHasDecode(spec.operations) || len(specSteps) != 0 || codecIntentSet(chainEncodeSpec(spec.operations))) {
		b.setErr(chainStepAfterEncodeError("build branch", firstNonEmpty(b.spec.name, "branch"), "flow", chainEncodeSpec(b.spec.operations)))
		return b
	}
	if spec.media != "" {
		if b.spec.media == "" {
			b.spec.media = spec.media
		} else if err := validateChainMedia("build branch", firstNonEmpty(b.spec.name, "branch"), b.spec.media, spec); err != nil {
			b.setErr(err)
			return b
		}
	}
	if chainHasDecode(spec.operations) {
		if chainHasDecode(b.spec.operations) {
			b.setErr(duplicateBranchDecodeError(firstNonEmpty(b.spec.name, "branch")))
			return b
		}
		if branchOperationSpecsContainStep(b.spec.operations) {
			b.setErr(branchDecodeOrderError(firstNonEmpty(b.spec.name, "branch")))
			return b
		}
	}
	if chainEncodeSpec(spec.operations).Copy && (chainHasDecode(b.spec.operations) || branchOperationSpecsContainStep(b.spec.operations)) {
		b.setErr(flowCopyDomainError("build branch", firstNonEmpty(spec.name, b.spec.name, "flow")))
		return b
	}
	b.spec.operations = append(b.spec.operations, cloneOperationSpecs(spec.operations)...)
	return b
}

func (b *branchBuilder) Do(stages ...pipeline.Stage) *branchBuilder {
	if b == nil {
		return b
	}
	for i := range stages {
		if codecIntentSet(chainEncodeSpec(b.spec.operations)) {
			b.setErr(chainStepAfterEncodeError("build branch", firstNonEmpty(b.spec.name, "branch"), "custom stage", chainEncodeSpec(b.spec.operations)))
			return b
		}
		if stages[i] == nil {
			b.setErr(streamStageMissingError(streamIntent{Name: firstNonEmpty(b.spec.name, "branch")}))
			return b
		}
		b.spec.operations = append(b.spec.operations, operationSpecForStage(stages[i]))
	}
	return b
}

func (b *branchBuilder) Shape(shape MediaShape) *branchBuilder {
	if b == nil {
		return b
	}
	if codecIntentSet(chainEncodeSpec(b.spec.operations)) {
		b.setErr(chainStepAfterEncodeError("build branch", firstNonEmpty(b.spec.name, "branch"), "shape", chainEncodeSpec(b.spec.operations)))
		return b
	}
	b.spec.operations = append(b.spec.operations, operationSpecForShape(shape))
	return b
}

func (b *branchBuilder) Resize(width int, height int, options ...resizeOption) *branchBuilder {
	if b == nil {
		return b
	}
	if codecIntentSet(chainEncodeSpec(b.spec.operations)) {
		b.setErr(chainStepAfterEncodeError("build branch", firstNonEmpty(b.spec.name, "branch"), "resize", chainEncodeSpec(b.spec.operations)))
		return b
	}
	transform := Resize(width, height, options...)
	b.spec.operations = append(b.spec.operations, operationSpecForTransform(transform))
	return b
}

func (b *branchBuilder) Resample(sampleRate int, channels int, options ...audioOption) *branchBuilder {
	if b == nil {
		return b
	}
	if codecIntentSet(chainEncodeSpec(b.spec.operations)) {
		b.setErr(chainStepAfterEncodeError("build branch", firstNonEmpty(b.spec.name, "branch"), "resample", chainEncodeSpec(b.spec.operations)))
		return b
	}
	transform := Resample(sampleRate, channels, options...)
	b.spec.operations = append(b.spec.operations, operationSpecForTransform(transform))
	return b
}

func (b *branchBuilder) Tap(tap TapRef) *branchBuilder {
	if b == nil {
		return b
	}
	if tap.name == "" {
		b.setErr(&BuildError{
			Code:      "tap_invalid",
			Operation: "build branch",
			Node:      firstNonEmpty(b.spec.name, "branch"),
			Reason:    "tap name is empty",
			Suggestions: []string{
				"call .Tap(goav.FrameTap(\"video.720p.frames\")) or another stable tap ref",
				"omit .Tap(...) when no runtime branch should attach at that point",
			},
			Cause: ErrUnsupportedBuild,
		})
		return b
	}
	if codecIntentSet(chainEncodeSpec(b.spec.operations)) {
		if err := validateTapDomain("build branch", firstNonEmpty(b.spec.name, "branch"), tap, DomainPacket); err != nil {
			b.setErr(err)
			return b
		}
		b.spec.operations = append(b.spec.operations, operationSpecForTap(tap, b.spec.media, operationSpecAfter(b.spec.operations, OpEncode)))
		return b
	}
	b.spec.operations = append(b.spec.operations, operationSpecForTap(tap, b.spec.media, operationSpecAfter(b.spec.operations, initialStepAfter(chainHasDecode(b.spec.operations)))))
	return b
}

func (b *branchBuilder) Encode(codec CodecSpec) *branchBuilder {
	if b == nil {
		return b
	}
	if codecIntentSet(chainEncodeSpec(b.spec.operations)) {
		b.setErr(duplicateStreamEncodeError("build branch", firstNonEmpty(b.spec.name, "branch"), chainEncodeSpec(b.spec.operations), codec))
		return b
	}
	if codec.Copy && chainHasDecode(b.spec.operations) {
		b.setErr(branchDecodeCopyError(firstNonEmpty(b.spec.name, "branch")))
		return b
	}
	b.spec.operations = append(b.spec.operations, operationSpecForEncode(cloneCodecSpec(codec)))
	return b
}

func (b *branchBuilder) Copy() *branchBuilder {
	return b.Encode(Copy())
}

func (b *branchBuilder) To(destinations ...Destination) BranchSpec {
	if b == nil {
		return BranchSpec{err: nilBranchError()}
	}
	spec := b.snapshot()
	if len(destinations) == 0 {
		spec.err = branchDestinationMissingError(spec.name)
		return spec
	}
	for i := range destinations {
		destination := destinations[i]
		binding, err := destinationBindingFromDestination(destination)
		if err != nil {
			spec.err = branchDestinationInvalidError(spec.name, err.Error())
			return spec
		}
		if err := appendDestination(&spec, binding, i); err != nil {
			spec.err = err
			return spec
		}
	}
	return spec
}

func (b *branchBuilder) snapshot() BranchSpec {
	spec := b.spec
	spec.operations = cloneOperationSpecs(spec.operations)
	spec.destinations = cloneDestinationRefs(spec.destinations)
	spec.destinationNames = append([]string(nil), spec.destinationNames...)
	return spec
}

func (b *branchBuilder) setErr(err error) {
	if b.spec.err == nil {
		b.spec.err = err
	}
}

func appendDestination(spec *BranchSpec, destination destinationBinding, index int) error {
	switch {
	case destination.hasDirect:
		destination := destination.dest
		destinationName := destination.label(fmt.Sprintf("%s-%d", firstNonEmpty(spec.name, "branch"), index+1))
		ref := newDirectDestinationRef(destinationName, destination)
		if ref.err != nil {
			return ref.err
		}
		spec.destinations = append(spec.destinations, ref)
		spec.destinationNames = append(spec.destinationNames, ref.name)
		return nil
	default:
		return branchDestinationInvalidError(spec.name, "unsupported branch destination")
	}
}

func (b *jobStreamBuilder) Branches(branches ...BranchSpec) *Job {
	stream := b.current()
	job := b.job
	if len(branches) == 0 {
		job.setErr(branchMissingError(jobStreamName(stream)))
		return job
	}
	if len(job.inputs) != 1 {
		job.setErr(branchInputCountError(jobStreamName(stream), len(job.inputs)))
		return job
	}
	if len(job.outputs) != 0 || len(stream.outputs) != 0 {
		job.setErr(branchOutputScopeError(jobStreamName(stream)))
		return job
	}

	streamSteps := jobStreamChainSteps(stream)
	parentPacket := stream.encode.Copy && !stream.decode && len(streamSteps) == 0
	if stream.encode.Copy && !parentPacket {
		job.setErr(branchCopyParentOperationError(jobStreamName(stream)))
		return job
	}
	if codecIntentSet(stream.encode) && !stream.encode.Copy {
		job.setErr(branchEncodeParentOperationError(jobStreamName(stream), stream.encode))
		return job
	}

	for i := range branches {
		if err := validateBranchSpec(stream.selector.Type, parentPacket, i, branches[i]); err != nil {
			job.setErr(err)
			return job
		}
		if err := job.addBranchDestinations(branches[i].destinations...); err != nil {
			job.setErr(err)
			return job
		}
		encode := cloneCodecSpec(chainEncodeSpec(branches[i].operations))
		if parentPacket && !chainHasDecode(branches[i].operations) && !codecIntentSet(encode) {
			encode = Copy()
		}
		decode := !parentPacket || chainHasDecode(branches[i].operations)
		_, from, err := plannedBranchAnchor(stream, branches[i], parentPacket)
		if err != nil {
			job.setErr(err)
			return job
		}
		sharedOps := plannedBranchSharedOperationSpecs(stream, branches[i], parentPacket)
		privateOps := plannedBranchPrivateOperationSpecs(stream, branches[i], parentPacket)
		operations := append(cloneOperationSpecs(sharedOps), cloneOperationSpecs(privateOps)...)
		job.branchStreams = append(job.branchStreams, streamBuild{
			name:             branches[i].name,
			selector:         stream.selector,
			from:             from,
			decode:           decode,
			decodeCodec:      mergeDecodeCodecSpec(stream.decodeCodec, chainDecodeCodec(branches[i].operations)),
			operations:       operations,
			sharedOps:        sharedOps,
			privateOps:       privateOps,
			encode:           encode,
			destinationNames: append([]string(nil), branches[i].destinationNames...),
		})
	}
	return job
}

func validateBranchSpec(selected av.MediaType, parentPacket bool, index int, spec BranchSpec) error {
	if spec.err != nil {
		return spec.err
	}
	if spec.from != "" {
		return plannedBranchNodeSourceError(spec.name, spec.from)
	}
	if err := validateChainMedia("build branches", firstNonEmpty(spec.name, "branch"), selected, chainSpec{name: spec.name, media: spec.media}); err != nil {
		return err
	}
	if spec.name == "" {
		return branchIntentNameMissingError(index, streamIntent{Select: StreamSelect{Type: selected}})
	}
	if len(spec.destinationNames) == 0 {
		return branchIntentDestinationMissingError(streamIntent{Name: spec.name, Select: StreamSelect{Type: selected}})
	}
	stream := streamIntent{Name: spec.name, Select: StreamSelect{Type: selected}}
	if chainHasDecode(spec.operations) && !parentPacket {
		return branchDecodeDomainError(stream.Name)
	}
	if chainEncodeSpec(spec.operations).Copy {
		if chainHasDecode(spec.operations) {
			return branchDecodeCopyError(stream.Name)
		}
		if !parentPacket {
			return branchCopyUnsupportedError(stream)
		}
	} else if parentPacket && codecIntentSet(chainEncodeSpec(spec.operations)) && !chainHasDecode(spec.operations) {
		return branchPacketEncodeUnsupportedError(stream, chainEncodeSpec(spec.operations))
	}
	if parentPacket && !chainHasDecode(spec.operations) {
		transforms := transformSpecsFromOperationSpecs(spec.operations)
		for i := range transforms {
			if err := validateTransformSpec("build branches", spec.name, transforms[i]); err != nil {
				return err
			}
		}
		if len(transforms) > 0 {
			return branchPacketTransformUnsupportedError(stream)
		}
	}
	if err := validateBranchStepTapDomains(spec, parentPacket); err != nil {
		return err
	}
	effectiveEncode := chainEncodeSpec(spec.operations)
	if parentPacket && !chainHasDecode(spec.operations) && !codecIntentSet(effectiveEncode) {
		effectiveEncode = Copy()
	}
	if !codecIntentSet(effectiveEncode) && !branchDestinationsAllSinkDestinations(spec.destinations) {
		return branchEncodeMissingError(stream)
	}
	seen := make(map[string]int, len(spec.destinationNames))
	for i, destinationName := range spec.destinationNames {
		if destinationName == "" {
			return branchDestinationNameEmptyError(streamBuild{name: spec.name, selector: av.StreamSelector{Type: selected}}, i)
		}
		if firstIndex, ok := seen[destinationName]; ok {
			return duplicateBranchDestinationError(
				streamIntent{Name: spec.name, Select: StreamSelect{Type: selected}, Destinations: spec.destinationNames},
				destinationName,
				firstIndex,
				i,
			)
		}
		seen[destinationName] = i
	}
	return nil
}

func validateBranchStepTapDomains(spec BranchSpec, parentPacket bool) error {
	domain := DomainFrame
	if parentPacket && !chainHasDecode(spec.operations) {
		domain = DomainPacket
	}
	steps := branchSpecChainSteps(spec)
	for i := range steps {
		step := steps[i]
		if step.tap == "" {
			continue
		}
		if err := validateTapDomain("build branches", firstNonEmpty(spec.name, "branch"), TapRef{name: step.tap, domain: step.tapDomain}, domain); err != nil {
			return err
		}
	}
	return nil
}

func branchSpecChainSteps(spec BranchSpec) []chainStep {
	return branchChainStepsFromOperationSpecs(spec.operations)
}

func branchOperationSpecsContainStep(operations []OperationSpec) bool {
	for i := range operations {
		switch operations[i].Kind {
		case OpStage, OpShape, OpTransform:
			return true
		case OpTap:
			if !operationSpecTapIsTerminalPacket(operations[i]) {
				return true
			}
		}
	}
	return false
}

func branchChainStepsFromOperationSpecs(operations []OperationSpec) []chainStep {
	if len(operations) == 0 {
		return nil
	}
	steps := make([]chainStep, 0, len(operations))
	for i := range operations {
		operation := operations[i]
		switch operation.Kind {
		case OpStage:
			if operation.Stage != nil {
				steps = append(steps, chainStep{stage: operation.Stage})
			}
		case OpShape:
			if !mediaShapeEmpty(operation.Shape) {
				steps = append(steps, chainStep{shape: operation.Shape})
			}
		case OpTransform:
			if operation.Transform.Resize != nil || operation.Transform.Resample != nil {
				steps = append(steps, chainStep{transform: cloneTransformSpec(operation.Transform)})
			}
		case OpTap:
			if operation.Tap.Name != "" && !operationSpecTapIsTerminalPacket(operation) {
				steps = append(steps, chainStep{tap: operation.Tap.Name, tapDomain: operation.Tap.Domain})
			}
		}
	}
	return steps
}

func operationSpecTapIsTerminalPacket(operation OperationSpec) bool {
	if operation.Kind != OpTap {
		return false
	}
	return operation.Tap.Domain == DomainPacket &&
		(operation.Tap.After == OpEncode || operation.Tap.After == OpCopy)
}

func plannedBranchAnchor(stream *jobStreamBuild, spec BranchSpec, parentPacket bool) ([]chainStep, TapRef, error) {
	streamSteps := jobStreamChainSteps(stream)
	if spec.tap == "" {
		if parentPacket {
			return nil, lastStreamTapRef(stream), nil
		}
		return cloneChainSteps(streamSteps), lastStreamTapRef(stream), nil
	}
	if stream == nil {
		return nil, TapRef{}, plannedBranchTapMissingError("", spec.name, spec.tap)
	}
	if parentPacket {
		if tapIsPacketAnchor(stream, spec.tap) {
			from := TapRef{name: spec.tap, domain: spec.tapDomain}
			if err := validateTapDomain("build branches", firstNonEmpty(spec.name, "branch"), from, DomainPacket); err != nil {
				return nil, TapRef{}, err
			}
			return nil, tapWithDomain(from, DomainPacket), nil
		}
		return nil, TapRef{}, plannedBranchTapMissingError(jobStreamName(stream), spec.name, spec.tap)
	}
	if stream.decode && spec.tap == defaultDecodedTapName(stream.selector.Type) {
		from := TapRef{name: spec.tap, domain: spec.tapDomain}
		if err := validateTapDomain("build branches", firstNonEmpty(spec.name, "branch"), from, DomainFrame); err != nil {
			return nil, TapRef{}, err
		}
		return nil, tapWithDomain(from, DomainFrame), nil
	}
	if steps, ok := chainStepsThroughTap(streamSteps, spec.tap); ok {
		from := TapRef{name: spec.tap, domain: spec.tapDomain}
		if err := validateTapDomain("build branches", firstNonEmpty(spec.name, "branch"), from, DomainFrame); err != nil {
			return nil, TapRef{}, err
		}
		return steps, tapWithDomain(from, DomainFrame), nil
	}
	if tapIsPostEncodeAnchor(stream, spec.tap) {
		from := TapRef{name: spec.tap, domain: spec.tapDomain}
		if err := validateTapDomain("build branches", firstNonEmpty(spec.name, "branch"), from, DomainPacket); err != nil {
			return nil, TapRef{}, err
		}
		return nil, TapRef{}, plannedBranchPostEncodeTapError(spec.name, spec.tap)
	}
	return nil, TapRef{}, plannedBranchTapMissingError(jobStreamName(stream), spec.name, spec.tap)
}

func tapIsPacketAnchor(stream *jobStreamBuild, tap string) bool {
	if tap == "" || stream == nil {
		return false
	}
	if tap == defaultPacketTapName(stream.selector.Type, 0) {
		return true
	}
	return tapIsPostEncodeAnchor(stream, tap)
}

func tapIsPostEncodeAnchor(stream *jobStreamBuild, tap string) bool {
	if tap == "" || stream == nil {
		return false
	}
	for i := range stream.operations {
		if stream.operations[i].Tap.Name == tap && operationSpecTapIsTerminalPacket(stream.operations[i]) {
			return true
		}
	}
	return false
}

func chainStepsThroughTap(steps []chainStep, tap string) ([]chainStep, bool) {
	for i := range steps {
		if steps[i].tap == tap {
			return cloneChainSteps(steps[:i+1]), true
		}
	}
	return nil, false
}

func branchCopyParentOperationError(node string) error {
	return &BuildError{
		Code:      "copy_branch_source_invalid",
		Operation: "build branches",
		Node:      node,
		Reason:    "packet-copy branches must start from a packet-domain stream point",
		Suggestions: []string{
			"call .Copy().Branches(...) before frame operations when the branches should preserve packets",
			"use .Decode().Branches(...) when branches need resize, resample, custom frame stages, or encode",
			"attach runtime packet-copy branches from a packet Tap when the branch should start later",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchEncodeParentOperationError(node string, encode CodecSpec) error {
	return &BuildError{
		Code:      "encode_branch_source_invalid",
		Operation: "build branches",
		Node:      node,
		Reason:    "stream encoders are terminal for planned branches",
		Details: []string{
			"encoder: " + codecIntentName(encode),
		},
		Suggestions: []string{
			"move .Branches(...) before the stream encoder",
			"put .Encode(goav.Opus(...)), .Encode(goav.VP8(...)), or .Encode(goav.VP9(...)) on each goav.Branch(...) that writes a destination",
			"attach post-encode packet branches at runtime with Task.Attach(ctx, goav.Branch(name).From(goav.PacketTap(name))...)",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func plannedBranchNodeSourceError(name string, source string) error {
	return &BuildError{
		Code:      "branch_source_invalid",
		Operation: "build branches",
		Node:      firstNonEmpty(name, "branch"),
		Reason:    "planned branches do not anchor from graph handles",
		Details: []string{
			"source: " + source,
		},
		Suggestions: []string{
			"use .From(goav.FrameTap(name)) or .From(goav.PacketTap(name)) to branch from a stable tap",
			"omit .From(...) to branch from the current stream point",
			"use Task.Attach(ctx, goav.Branch(name).From(graphNode)...) for expert runtime graph attachment",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func plannedBranchTapMissingError(stream string, branch string, tap string) error {
	return &BuildError{
		Code:      "branch_tap_missing",
		Operation: "build branches",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "branch tap is not declared on the parent stream",
		Details: []string{
			"stream: " + firstNonEmpty(stream, "stream"),
			"tap: " + tap,
		},
		Suggestions: []string{
			"add .Tap(goav.FrameTap(\"" + tap + "\")) before .Branches(...) on the selected stream",
			"use .From(goav.FrameTap(\"audio.decoded\")) or .From(goav.FrameTap(\"video.decoded\")) after .Decode() when branching from decoded frames",
			"omit .From(...) to branch from the current stream point",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func plannedBranchPostEncodeTapError(branch string, tap string) error {
	return &BuildError{
		Code:      "branch_tap_domain_unsupported",
		Operation: "build branches",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "post-encode taps are runtime attachment anchors for planned branches",
		Details: []string{
			"tap: " + tap,
		},
		Suggestions: []string{
			"attach this branch at runtime with Task.Attach(ctx, goav.Branch(name).From(goav.PacketTap(\"" + tap + "\"))...)",
			"move .Branches(...) before the encoder when the split should be planned",
			"use .Copy().Branches(...) for packet-preserving planned branches",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func duplicateBranchDecodeError(node string) error {
	return &BuildError{
		Code:      "branch_decode_duplicate",
		Operation: "build branch",
		Node:      node,
		Reason:    "branch already decodes its input packets",
		Suggestions: []string{
			"call .Decode() once before frame operations",
			"remove the second .Decode() call",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchDecodeOrderError(node string) error {
	return &BuildError{
		Code:      "branch_decode_order_invalid",
		Operation: "build branch",
		Node:      node,
		Reason:    "decode must be the first branch operation",
		Suggestions: []string{
			"write goav.Branch(name).Decode().Resample(...).To(target)",
			"start from a frame tap when the branch should skip decode",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchDecodeDomainError(node string) error {
	return &BuildError{
		Code:      "branch_decode_domain_mismatch",
		Operation: "build branches",
		Node:      node,
		Reason:    "branch decoding requires a packet-domain stream point",
		Suggestions: []string{
			"omit .Decode() when the branch already starts after stream decode",
			"use .Copy().Branches(goav.Branch(name).Decode()...) when a packet-preserving split later needs frames",
			"attach runtime decode branches from packet taps",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchDecodeCopyError(node string) error {
	return &BuildError{
		Code:      "branch_decode_copy_invalid",
		Operation: "build branch",
		Node:      node,
		Reason:    "a branch cannot decode packets and then copy the original packet payload",
		Suggestions: []string{
			"use .Copy() for packet-preserving branches",
			"use .Decode().To(goav.Sink(...)) for decoded frames",
			"use .Decode().Encode(codec).To(destination) for re-encoded packets",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchPacketEncodeUnsupportedError(stream streamIntent, encode CodecSpec) error {
	return &BuildError{
		Code:      "packet_branch_encode_unsupported",
		Operation: "build branches",
		Node:      branchIntentName(stream),
		Reason:    "packet-domain planned branches cannot encode without decoding first",
		Details: []string{
			"encoder: " + codecIntentName(encode),
		},
		Suggestions: []string{
			"use .Decode().Branches(goav.Branch(name).Encode(goav.Opus(...)).To(destination)) for encoded variants",
			"use .Copy().Branches(goav.Branch(name).To(destination)) for packet-preserving variants",
			"attach a runtime branch from a frame Tap when late encoding is needed",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchPacketTransformUnsupportedError(stream streamIntent) error {
	return &BuildError{
		Code:      "packet_branch_transform_unsupported",
		Operation: "build branches",
		Node:      branchIntentName(stream),
		Reason:    "packet-domain planned branches cannot resize or resample without decoding first",
		Suggestions: []string{
			"use .Decode().Branches(...) when branch variants need frame transforms",
			"use .Copy().Branches(...) only for packet-preserving branches",
			"attach a runtime branch from a frame Tap when late transforms are needed",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchDestinationsAllSinkDestinations(destinations []destinationRef) bool {
	if len(destinations) == 0 {
		return false
	}
	for i := range destinations {
		if destinations[i].dest.sink == nil {
			return false
		}
	}
	return true
}

func cloneDestinationRefs(destinations []destinationRef) []destinationRef {
	if len(destinations) == 0 {
		return nil
	}
	out := make([]destinationRef, 0, len(destinations))
	for i := range destinations {
		out = append(out, cloneDestinationRef(destinations[i]))
	}
	return out
}

func cloneDestinationRef(ref destinationRef) destinationRef {
	ref.dest = cloneDestinationSpec(ref.dest)
	return ref
}

func cloneDestinationSpec(dest destinationSpec) destinationSpec {
	return dest
}

func branchMissingError(node string) error {
	return &BuildError{
		Code:      "branch_missing",
		Operation: "build branches",
		Node:      node,
		Reason:    "Branches requires at least one encoded branch",
		Suggestions: []string{
			"pass branches with goav.Branch(name).Encode(goav.VP9(...)).To(goav.File(name, writer))",
			"reuse the same destination value from multiple branches when they should share one mux group",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func nilBranchError() error {
	return &BuildError{
		Code:      "branch_invalid",
		Operation: "build branch",
		Reason:    "branch is nil",
		Suggestions: []string{
			"build branches with goav.Branch(name)",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchDestinationMissingError(name string) error {
	return &BuildError{
		Code:      "destination_missing",
		Operation: "build branch",
		Node:      firstNonEmpty(name, "branch"),
		Reason:    "branch has no destination",
		Suggestions: []string{
			"finish the branch with .To(goav.File(\"web.ivf\", writer)) or .To(goav.Sink(sink))",
			"reuse the same destination value when several branches should share one mux or sink group",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchDestinationInvalidError(name string, reason string) error {
	return destinationInvalidError("build branch", firstNonEmpty(name, "branch"), reason)
}

func streamDestinationInvalidError(name string, reason string) error {
	return destinationInvalidError("build stream", firstNonEmpty(name, "stream"), reason)
}

func jobDestinationInvalidError(name string, reason string) error {
	return destinationInvalidError("build job", firstNonEmpty(name, "job"), reason)
}

func destinationInvalidError(operation string, node string, reason string) error {
	return &BuildError{
		Code:      "destination_invalid",
		Operation: operation,
		Node:      node,
		Reason:    reason,
		Suggestions: []string{
			"reuse one goav.File(...), goav.URIOut(...), or goav.Sink(...) value for mux/sink groups",
			"use distinct destination values for independent outputs",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func destinationNameMissingError(dest destinationSpec) error {
	return &BuildError{
		Code:      "destination_invalid",
		Operation: "build destination",
		Node:      dest.label("destination"),
		Reason:    "destination name is empty",
		Suggestions: []string{
			"pass a named destination such as goav.File(\"web.ivf\", writer)",
			"pass goav.Sink(goav.SinkFunc(name, fn)) for sink destinations",
		},
		Cause: ErrUnsupportedBuild,
	}
}
