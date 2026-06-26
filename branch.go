package goav

import (
	"fmt"
	"sync/atomic"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

var destinationRefSeq atomic.Uint64
var destinationSpecSeq atomic.Uint64

func destinationShareKey(dest destinationSpec, id uint64) string {
	if dest.group != "" {
		return "group:" + dest.group
	}
	return ""
}

// Destination is an opaque handle for a file, URI, writer, media sink, or
// shared mux/sink group. Built-in constructors and Custom return destination
// values with goav-owned routing identity; Mux can make that grouping identity
// explicit across separately constructed values.
type Destination struct {
	spec destinationSpec
}

// DestinationOption configures a destination value (Write, URI, Writer,
// Custom, or Destination.With): Format pins the container, and the
// direction-agnostic MediaOptions (Name, MIME, Metadata) satisfy it too. It is
// sealed — only goav option constructors implement it.
type DestinationOption interface {
	applyDestination(*destinationSpec)
}

type destinationRef struct {
	name string
	dest destinationSpec
	id   uint64
}

// newDirectDestinationRef names a destination handle for branch routing.
// Callers derive name through destinationSpec.label with a non-empty
// fallback, so a ref always carries a routing name.
func newDirectDestinationRef(name string, dest destinationSpec) destinationRef {
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

// branchDestinationNames derives a branch's destination names from its
// destination handles, so BranchSpec keeps no parallel name list — the
// destinationRef handles are the single source of truth.
func branchDestinationNames(destinations []destinationRef) []string {
	if len(destinations) == 0 {
		return nil
	}
	names := make([]string, len(destinations))
	for i := range destinations {
		names[i] = destinations[i].name
	}
	return names
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

// BranchSpec is one finished branch declaration: the operations and
// destinations a Branch(...) builder accumulated, ready for .Branches(...) on
// a stream chain or Mutable.Attach at runtime. Values are immutable snapshots —
// reusing a builder cannot mutate a spec already passed along. The zero value
// is intentionally not a valid branch; construct branch specs with Branch(name).
type BranchSpec struct {
	origin       branchSpecOrigin
	name         string
	media        av.MediaType
	operations   []operationSpec
	destinations []destinationRef

	source       branchSourceBinding
	branchBuffer flow.BranchBuffer

	removeDisposition    oldBranchDisposition
	hasRemoveDisposition bool
	err                  error
}

type branchSpecOrigin uint8

const (
	branchSpecOriginZero branchSpecOrigin = iota
	branchSpecOriginBranch
	branchSpecOriginOnRemove
)

type branchBuilder struct {
	spec BranchSpec
}

type branchSourceBinding struct {
	from      string
	tap       string
	tapDomain shape.MediaDomain
	policy    pipeline.RoutePolicy
	label     string
	// stream pins a discovered-stream anchor: the branch hangs off the source
	// node `from` routed by stream id, and the attach planner derives the
	// anchor shape from these stream facts instead of a tap. streamDomain is
	// the source's media domain (packet unless the source produces frames).
	stream       *av.Stream
	streamDomain shape.MediaDomain
}

// branchSource is the anchor a branch hangs from: a TapRef names a stable
// tap, an InputStream names one stream from a recipe input, and an expert graph
// handle (expert.GraphNode, expert.GraphOutlet) names a graph node through its
// Route capability. The bound is structural — From validates the anchor and
// refuses values that are neither.
type branchSource interface {
	// Name reports the anchor's name: the tap, source node, or graph node name.
	Name() string
}

// graphRouteAnchor is the capability expert graph handles expose: the route
// (node, policy, label) a branch anchored on them reads from. Asserted
// structurally so the root never imports the expert package.
type graphRouteAnchor interface {
	Route() pipeline.Route
}

// tapAnchor is the sealed capability TapRef and the internal graph handles
// implement directly.
type tapAnchor interface {
	branchSource() branchSourceBinding
}

// Branch starts a named downstream branch: operations chain onto it exactly
// like a stream chain (.Decode, .Resize, .Encode, .Do, ...), .From(tap)
// anchors it at an earlier point, and .To(destinations...) finishes it into a
// BranchSpec. Names must be unique within one Branches or Attach call.
func Branch(name string) *branchBuilder {
	return &branchBuilder{spec: BranchSpec{origin: branchSpecOriginBranch, name: name}}
}

func (b *branchBuilder) From(source branchSource) *branchBuilder {
	if b == nil {
		return b
	}
	switch anchor := source.(type) {
	case tapAnchor:
		b.spec.source = anchor.branchSource()
	case graphRouteAnchor:
		route := anchor.Route()
		policy := route.Policy
		if policy == "" {
			policy = pipeline.RouteAll
		}
		b.spec.source = branchSourceBinding{from: route.From, policy: policy, label: route.Label}
	default:
		b.setErr(branchSourceInvalidError(firstNonEmpty(b.spec.name, "branch")))
	}
	return b
}

func (b *branchBuilder) Stream(stream av.StreamID) *branchBuilder {
	if b == nil {
		return b
	}
	b.spec.source.policy = pipeline.RouteByStream
	b.spec.source.label = string(stream)
	return b
}

func (b *branchBuilder) Event(event av.EventType) *branchBuilder {
	if b == nil {
		return b
	}
	b.spec.source.policy = pipeline.RouteByEvent
	b.spec.source.label = string(event)
	return b
}

func (b *branchBuilder) Buffer(buffer flow.BranchBuffer) *branchBuilder {
	if b == nil {
		return b
	}
	if err := validateBranchBuffer(buffer, "build branch", firstNonEmpty(b.spec.name, "branch")); err != nil {
		b.setErr(err)
		return b
	}
	b.spec.branchBuffer = buffer
	return b
}

func (b *branchBuilder) Decode(options ...codec.Option) *branchBuilder {
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
	decodeCodec := mergeDecodeCodecSpec(codec.CodecSpec{}, codecSpecFromOptions(options...))
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
		if err := validateStageComponent(stages[i]); err != nil {
			b.setErr(streamStageMissingError(streamIntent{Name: firstNonEmpty(b.spec.name, "branch")}))
			return b
		}
		b.spec.operations = append(b.spec.operations, operationSpecForStage(stages[i]))
	}
	return b
}

// Sync places this branch on a shared media timeline. Reuse one flow.SyncPolicy
// value across live-room branches when recording or preview paths should align.
func (b *branchBuilder) Sync(policy flow.SyncPolicy) *branchBuilder {
	if b == nil {
		return b
	}
	b.spec.operations = append(b.spec.operations, operationSpecForSync(policy))
	return b
}

// Auto opts the branch into shape solving with the given conversion policies —
// the branch-side twin of the stream chain's .Auto(...): needed conversions an
// active policy allows are inserted from the runtime's filter registry as real
// planned operations; everything else is refused with the exact policy to add.
func (b *branchBuilder) Auto(policies ...shape.Policy) *branchBuilder {
	if b == nil {
		return b
	}
	b.spec.operations = append(b.spec.operations, operationSpecForAutoPolicy(policies))
	return b
}

// Require asserts a hard shape constraint at this point of the branch — the
// branch-side twin of the stream chain's .Require(...): the stream MUST
// satisfy the given spec here, or the build fails with the actual and required
// shapes and the exact fix. It lowers to no runtime node.
func (b *branchBuilder) Require(spec shape.Spec) *branchBuilder {
	if b == nil {
		return b
	}
	b.spec.operations = append(b.spec.operations, operationSpecForRequire(spec))
	return b
}

// Prefer biases the shape solver's otherwise-open choices on this branch — the
// branch-side twin of the stream chain's .Prefer(...). Soft by definition: a
// preference that cannot be honored is dropped with an Explain diagnostic,
// never an error. It lowers to no runtime node.
func (b *branchBuilder) Prefer(spec shape.Spec) *branchBuilder {
	if b == nil {
		return b
	}
	b.spec.operations = append(b.spec.operations, operationSpecForPreference(spec))
	return b
}

func (b *branchBuilder) Shape(shape shape.Spec) *branchBuilder {
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
			Family:    errcode.FamilyForCode(errcode.TapInvalid),
			Code:      errcode.TapInvalid,
			Operation: "build branch",
			Node:      firstNonEmpty(b.spec.name, "branch"),
			Reason:    "tap name is empty",
			Fixes: buildErrorFixes([]string{
				"call .Tap(goav.FrameTap(\"video.720p.frames\")) or another stable tap ref",
				"omit .Tap(...) when no runtime branch should attach at that point",
			}),
			Cause: ErrUnsupportedBuild,
		})
		return b
	}
	if codecIntentSet(chainEncodeSpec(b.spec.operations)) {
		if err := validateTapDomain("build branch", firstNonEmpty(b.spec.name, "branch"), tap, shape.DomainPacket); err != nil {
			b.setErr(err)
			return b
		}
		b.spec.operations = append(b.spec.operations, operationSpecForTap(tap, b.spec.media, operationSpecAfter(b.spec.operations, plan.OpEncode)))
		return b
	}
	b.spec.operations = append(b.spec.operations, operationSpecForTap(tap, b.spec.media, operationSpecAfter(b.spec.operations, initialStepAfter(chainHasDecode(b.spec.operations)))))
	return b
}

func (b *branchBuilder) Encode(codec codec.CodecSpec) *branchBuilder {
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
	return b.Encode(codec.Copy())
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
		direct, err := destinationSpecFromDestination(destinations[i])
		if err != nil {
			spec.err = branchDestinationInvalidError(spec.name, err.Error())
			return spec
		}
		appendDestination(&spec, direct, i)
	}
	return spec
}

func (b *branchBuilder) snapshot() BranchSpec {
	spec := b.spec
	spec.operations = cloneOperationSpecs(spec.operations)
	spec.destinations = cloneDestinationRefs(spec.destinations)
	return spec
}

func (b *branchBuilder) setErr(err error) {
	if b.spec.err == nil {
		b.spec.err = err
	}
}

// appendDestination resolves the destination's routing name — its declared
// name, URI, or a branch-indexed fallback, never empty — and appends the ref.
func appendDestination(spec *BranchSpec, destination destinationSpec, index int) {
	destinationName := destination.label(fmt.Sprintf("%s-%d", firstNonEmpty(spec.name, "branch"), index+1))
	spec.destinations = append(spec.destinations, newDirectDestinationRef(destinationName, destination))
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
	parentPacket := chainEncodeSpec(stream.operations).Copy && !chainHasDecode(stream.operations) && len(streamSteps) == 0
	if chainEncodeSpec(stream.operations).Copy && !parentPacket {
		job.setErr(branchCopyParentOperationError(jobStreamName(stream)))
		return job
	}
	if codecIntentSet(chainEncodeSpec(stream.operations)) && !chainEncodeSpec(stream.operations).Copy {
		job.setErr(branchEncodeParentOperationError(jobStreamName(stream), chainEncodeSpec(stream.operations)))
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
		decode := !parentPacket || chainHasDecode(branches[i].operations)
		_, from, err := plannedBranchAnchor(stream, branches[i], parentPacket)
		if err != nil {
			job.setErr(err)
			return job
		}
		sharedOps := plannedBranchSharedOperationSpecs(stream, branches[i], parentPacket)
		privateOps := plannedBranchPrivateOperationSpecs(stream, branches[i], parentPacket)
		operations := append(cloneOperationSpecs(sharedOps), cloneOperationSpecs(privateOps)...)
		// encode is derived from operations (chainEncodeSpec) at every reader —
		// plannedBranchPrivateOperationSpecs already injects the plan.OpCopy for the
		// parentPacket passthrough case, so the op list is the single source.
		job.branchStreams = append(job.branchStreams, streamBuild{
			name:             branches[i].name,
			selector:         stream.selector,
			from:             from,
			decode:           decode,
			decodeCodec:      mergeDecodeCodecSpec(chainDecodeCodec(stream.operations), chainDecodeCodec(branches[i].operations)),
			operations:       operations,
			sharedOps:        sharedOps,
			privateOps:       privateOps,
			destinationNames: append([]string(nil), branchDestinationNames(branches[i].destinations)...),
		})
	}
	return job
}

func validateBranchSpec(selected av.MediaType, parentPacket bool, index int, spec BranchSpec) error {
	if spec.err != nil {
		return spec.err
	}
	if spec.origin != branchSpecOriginBranch {
		return branchSpecOriginError(index, selected)
	}
	if spec.source.from != "" {
		return plannedBranchNodeSourceError(spec.name, spec.source.from)
	}
	if err := validateChainMedia("build branches", firstNonEmpty(spec.name, "branch"), selected, chainSpec{name: spec.name, media: spec.media}); err != nil {
		return err
	}
	if spec.name == "" {
		return branchIntentNameMissingError(index, streamIntent{Select: plan.StreamSelect{Type: selected}})
	}
	if len(spec.destinations) == 0 {
		return branchIntentDestinationMissingError(streamIntent{Name: spec.name, Select: plan.StreamSelect{Type: selected}})
	}
	stream := streamIntent{Name: spec.name, Select: plan.StreamSelect{Type: selected}}
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
		effectiveEncode = codec.Copy()
	}
	if !codecIntentSet(effectiveEncode) && !branchDestinationsAllSinkDestinations(spec.destinations) {
		return branchEncodeMissingError(stream)
	}
	seen := make(map[string]int, len(spec.destinations))
	for i, destinationName := range branchDestinationNames(spec.destinations) {
		if destinationName == "" {
			return branchDestinationNameEmptyError(streamBuild{name: spec.name, selector: av.StreamSelector{Type: selected}}, i)
		}
		if firstIndex, ok := seen[destinationName]; ok {
			return duplicateBranchDestinationError(
				streamIntent{Name: spec.name, Select: plan.StreamSelect{Type: selected}, Destinations: branchDestinationNames(spec.destinations)},
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
	domain := shape.DomainFrame
	if parentPacket && !chainHasDecode(spec.operations) {
		domain = shape.DomainPacket
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

func branchOperationSpecsContainStep(operations []operationSpec) bool {
	for i := range operations {
		switch operations[i].Kind {
		case plan.OpStage, plan.OpTransform:
			return true
		case plan.OpShape:
			// Empty shape annotations (the .Auto(...) policy carrier) lower to
			// nothing and never constrain operation order.
			if !mediaShapeEmpty(operations[i].Shape) {
				return true
			}
		case plan.OpTap:
			if !operationSpecTapIsTerminalPacket(operations[i]) {
				return true
			}
		}
	}
	return false
}

func branchChainStepsFromOperationSpecs(operations []operationSpec) []chainStep {
	if len(operations) == 0 {
		return nil
	}
	steps := make([]chainStep, 0, len(operations))
	for i := range operations {
		operation := operations[i]
		switch operation.Kind {
		case plan.OpStage:
			if operation.Stage != nil {
				steps = append(steps, chainStep{stage: operation.Stage})
			}
		case plan.OpShape:
			if !mediaShapeEmpty(operation.Shape) {
				steps = append(steps, chainStep{shape: operation.Shape})
			}
		case plan.OpTransform:
			if operation.Transform.resize != nil || operation.Transform.resample != nil {
				steps = append(steps, chainStep{transform: cloneTransformSpec(operation.Transform)})
			}
		case plan.OpTap:
			if operation.Tap.Name != "" && !operationSpecTapIsTerminalPacket(operation) {
				steps = append(steps, chainStep{tap: operation.Tap.Name, tapDomain: operation.Tap.Domain})
			}
		}
	}
	return steps
}

func operationSpecTapIsTerminalPacket(operation operationSpec) bool {
	if operation.Kind != plan.OpTap {
		return false
	}
	return operation.Tap.Domain == shape.DomainPacket &&
		(operation.Tap.After == plan.OpEncode || operation.Tap.After == plan.OpCopy)
}

func plannedBranchAnchor(stream *jobStreamBuild, spec BranchSpec, parentPacket bool) ([]chainStep, TapRef, error) {
	streamSteps := jobStreamChainSteps(stream)
	if spec.source.tap == "" {
		if parentPacket {
			return nil, lastStreamTapRef(stream), nil
		}
		return cloneChainSteps(streamSteps), lastStreamTapRef(stream), nil
	}
	if stream == nil {
		return nil, TapRef{}, plannedBranchTapMissingError("", spec.name, spec.source.tap)
	}
	if parentPacket {
		if tapIsPacketAnchor(stream, spec.source.tap) {
			from := TapRef{name: spec.source.tap, domain: spec.source.tapDomain}
			if err := validateTapDomain("build branches", firstNonEmpty(spec.name, "branch"), from, shape.DomainPacket); err != nil {
				return nil, TapRef{}, err
			}
			return nil, tapWithDomain(from, shape.DomainPacket), nil
		}
		return nil, TapRef{}, plannedBranchTapMissingError(jobStreamName(stream), spec.name, spec.source.tap)
	}
	if chainHasDecode(stream.operations) && spec.source.tap == defaultDecodedTapName(stream.selector.Type) {
		from := TapRef{name: spec.source.tap, domain: spec.source.tapDomain}
		if err := validateTapDomain("build branches", firstNonEmpty(spec.name, "branch"), from, shape.DomainFrame); err != nil {
			return nil, TapRef{}, err
		}
		return nil, tapWithDomain(from, shape.DomainFrame), nil
	}
	if steps, ok := chainStepsThroughTap(streamSteps, spec.source.tap); ok {
		from := TapRef{name: spec.source.tap, domain: spec.source.tapDomain}
		if err := validateTapDomain("build branches", firstNonEmpty(spec.name, "branch"), from, shape.DomainFrame); err != nil {
			return nil, TapRef{}, err
		}
		return steps, tapWithDomain(from, shape.DomainFrame), nil
	}
	// Post-encode taps cannot occur here: Branches already refused encoding
	// parents, and a .Copy() parent reaches the packet-anchor path above.
	return nil, TapRef{}, plannedBranchTapMissingError(jobStreamName(stream), spec.name, spec.source.tap)
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
		Family:    errcode.FamilyForCode(errcode.CopyBranchSourceInvalid),
		Code:      errcode.CopyBranchSourceInvalid,
		Operation: "build branches",
		Node:      node,
		Reason:    "packet-copy branches must start from a packet-domain stream point",
		Fixes: buildErrorFixes([]string{
			"call .Copy().Branches(...) before frame operations when the branches should preserve packets",
			"use .Decode().Branches(...) when branches need resize, resample, custom frame stages, or encode",
			"attach runtime packet-copy branches from a packet Tap when the branch should start later",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func branchEncodeParentOperationError(node string, encode codec.CodecSpec) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.EncodeBranchSourceInvalid),
		Code:      errcode.EncodeBranchSourceInvalid,
		Operation: "build branches",
		Node:      node,
		Reason:    "stream encoders are terminal for planned branches",
		Fields: buildErrorFields([]string{
			"encoder: " + codecIntentName(encode),
		}),
		Fixes: buildErrorFixes([]string{
			"move .Branches(...) before the stream encoder",
			"put .Encode(codec.Opus(...)), .Encode(codec.VP8(...)), or .Encode(codec.VP9(...)) on each goav.Branch(...) that writes a destination",
			"attach post-encode packet branches at runtime with Mutable.Attach(ctx, goav.Branch(name).From(goav.PacketTap(name))...)",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func plannedBranchNodeSourceError(name string, source string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.BranchSourceInvalid),
		Code:      errcode.BranchSourceInvalid,
		Operation: "build branches",
		Node:      firstNonEmpty(name, "branch"),
		Reason:    "planned branches do not anchor from graph handles",
		Fields: buildErrorFields([]string{
			"source: " + source,
		}),
		Fixes: buildErrorFixes([]string{
			"use .From(goav.FrameTap(name)) or .From(goav.PacketTap(name)) to branch from a stable tap",
			"omit .From(...) to branch from the current stream point",
			"use Mutable.Attach(ctx, goav.Branch(name).From(graphNode)...) for expert runtime graph attachment",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func plannedBranchTapMissingError(stream string, branch string, tap string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.BranchTapMissing),
		Code:      errcode.BranchTapMissing,
		Operation: "build branches",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "branch tap is not declared on the parent stream",
		Fields: buildErrorFields([]string{
			"stream: " + firstNonEmpty(stream, "stream"),
			"tap: " + tap,
		}),
		Fixes: buildErrorFixes([]string{
			"add .Tap(goav.FrameTap(\"" + tap + "\")) before .Branches(...) on the selected stream",
			"use .From(goav.FrameTap(\"audio.decoded\")) or .From(goav.FrameTap(\"video.decoded\")) after .Decode() when branching from decoded frames",
			"omit .From(...) to branch from the current stream point",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func duplicateBranchDecodeError(node string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.BranchDecodeDuplicate),
		Code:      errcode.BranchDecodeDuplicate,
		Operation: "build branch",
		Node:      node,
		Reason:    "branch already decodes its input packets",
		Fixes: buildErrorFixes([]string{
			"call .Decode() once before frame operations",
			"remove the second .Decode() call",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func branchDecodeOrderError(node string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.BranchDecodeOrderInvalid),
		Code:      errcode.BranchDecodeOrderInvalid,
		Operation: "build branch",
		Node:      node,
		Reason:    "decode must be the first branch operation",
		Fixes: buildErrorFixes([]string{
			"write goav.Branch(name).Decode().Resample(...).To(target)",
			"start from a frame tap when the branch should skip decode",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func branchDecodeDomainError(node string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.BranchDecodeDomainMismatch),
		Code:      errcode.BranchDecodeDomainMismatch,
		Operation: "build branches",
		Node:      node,
		Reason:    "branch decoding requires a packet-domain stream point",
		Fixes: buildErrorFixes([]string{
			"omit .Decode() when the branch already starts after stream decode",
			"use .Copy().Branches(goav.Branch(name).Decode()...) when a packet-preserving split later needs frames",
			"attach runtime decode branches from packet taps",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func branchDecodeCopyError(node string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.BranchDecodeCopyInvalid),
		Code:      errcode.BranchDecodeCopyInvalid,
		Operation: "build branch",
		Node:      node,
		Reason:    "a branch cannot decode packets and then copy the original packet payload",
		Fixes: buildErrorFixes([]string{
			"use .Copy() for packet-preserving branches",
			"use .Decode().To(goav.Sink(...)) for decoded frames",
			"use .Decode().Encode(codec).To(destination) for re-encoded packets",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func branchPacketEncodeUnsupportedError(stream streamIntent, encode codec.CodecSpec) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.PacketBranchEncodeUnsupported),
		Code:      errcode.PacketBranchEncodeUnsupported,
		Operation: "build branches",
		Node:      branchIntentName(stream),
		Reason:    "packet-domain planned branches cannot encode without decoding first",
		Fields: buildErrorFields([]string{
			"encoder: " + codecIntentName(encode),
		}),
		Fixes: buildErrorFixes([]string{
			"use .Decode().Branches(goav.Branch(name).Encode(codec.Opus(...)).To(destination)) for encoded variants",
			"use .Copy().Branches(goav.Branch(name).To(destination)) for packet-preserving variants",
			"attach a runtime branch from a frame Tap when late encoding is needed",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func branchPacketTransformUnsupportedError(stream streamIntent) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.PacketBranchTransformUnsupported),
		Code:      errcode.PacketBranchTransformUnsupported,
		Operation: "build branches",
		Node:      branchIntentName(stream),
		Reason:    "packet-domain planned branches cannot resize or resample without decoding first",
		Fixes: buildErrorFixes([]string{
			"use .Decode().Branches(...) when branch variants need frame transforms",
			"use .Copy().Branches(...) only for packet-preserving branches",
			"attach a runtime branch from a frame Tap when late transforms are needed",
		}),
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
		Family:    errcode.FamilyForCode(errcode.BranchMissing),
		Code:      errcode.BranchMissing,
		Operation: "build branches",
		Node:      node,
		Reason:    "Branches requires at least one encoded branch",
		Fixes: buildErrorFixes([]string{
			"pass branches with goav.Branch(name).Encode(codec.VP9(...)).To(goav.Write(name, writer))",
			"pass goav.Mux(name, destination) when branches should share one mux group",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func nilBranchError() error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.BranchInvalid),
		Code:      errcode.BranchInvalid,
		Operation: "build branch",
		Reason:    "branch is nil",
		Fixes: buildErrorFixes([]string{
			"build branches with goav.Branch(name)",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func branchSpecOriginError(index int, selected av.MediaType) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.BranchInvalid),
		Code:      errcode.BranchInvalid,
		Operation: "build branches",
		Node:      fmt.Sprintf("branch-%d", index),
		Reason:    "branch spec was not constructed with goav.Branch(name)",
		Fields: buildErrorFields([]string{
			"media=" + string(selected),
		}),
		Fixes: buildErrorFixes([]string{
			"construct branches with goav.Branch(name).To(destination)",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func branchDestinationMissingError(name string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.DestinationMissing),
		Code:      errcode.DestinationMissing,
		Operation: "build branch",
		Node:      firstNonEmpty(name, "branch"),
		Reason:    "branch has no destination",
		Fixes: buildErrorFixes([]string{
			"finish the branch with .To(goav.Write(\"web.ivf\", writer)) or .To(goav.Sink(sink))",
			"pass goav.Mux(name, destination) when several branches should share one mux or sink group",
		}),
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
		Family:    errcode.FamilyForCode(errcode.DestinationInvalid),
		Code:      errcode.DestinationInvalid,
		Operation: operation,
		Node:      node,
		Reason:    reason,
		Fixes: buildErrorFixes([]string{
			"use goav.Write(...), goav.URI(...), or goav.Sink(...) for a real destination",
			"wrap shared outputs with goav.Mux(name, destination)",
			"use distinct destination values for independent outputs",
		}),
		Cause: ErrUnsupportedBuild,
	}
}
