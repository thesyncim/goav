// The normalized build intent: intent types, operationSpec construction, and intent derivation from job streams.

package goav

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// intent is the normalized build plan derived from a Job before graph
// compilation: inputs, stream chains with their ordered operations, and
// deduplicated destinations. It is a read-only projection used by planning
// and tests; applications normally read plan.Report from Explain instead.
type intent struct {
	Name         string
	Inputs       []inputIntent
	Streams      []streamIntent
	Destinations []destinationIntent
	Policies     policyIntent
	Copy         bool
}

type inputIntent struct {
	Name     string
	URI      string
	Protocol av.ProtocolID
	MIMEType string
	Codec    codec.CodecSpec
	Realtime bool
}

// streamIntent is one stream chain of the normalized build intent. Operations
// is the single source of truth for the chain's work: decode facts live on the
// plan.OpDecode operation, encode/copy facts on the terminal plan.OpEncode/
// plan.OpCopy operation, and taps on plan.OpTap operations — read them through
// chainHasDecode, chainDecodeCodec, chainEncodeSpec, and operationSpecTaps
// instead of keeping parallel fields.
//
// Destinations is not derived from Operations: it is the stream→destination
// routing, naming the intent.Destinations entries this chain feeds. The flow is
// one-directional — builders write the labels here, and the plan derives its
// destination/<label> handle IDs from them (planBranchDestinations).
type streamIntent struct {
	Name         string
	Select       plan.StreamSelect
	From         TapRef
	Operations   []operationSpec
	CodecChange  CodecChangePolicy
	Destinations []string
}

// operationSpec is one chain operation in normalized form — the single
// representation every chain (stream, branch, flow, join arm) lowers to.
// Kind says which fields apply: Decode/Encode carry codec specs, Transform a
// resize/resample, Stage a custom stage, Tap an attach point, and Shape the
// annotation/solver fields (Shape, Auto, Require, Prefer).
type operationSpec struct {
	Kind      plan.OperationKind
	Component string
	Detail    string
	Stage     pipeline.Stage
	Shape     shape.Spec
	Transform TransformSpec
	Tap       tapIntent
	Decode    codec.CodecSpec
	Encode    codec.CodecSpec
	Shared    bool
	// Auto carries the chain's shape-solving policy: .Auto(policies...) appends
	// one plan.OpShape operation with Auto set, and the solver unions every Auto
	// operation on the chain. Nil means the operation carries no policy.
	Auto *shape.Policy
	// Require carries a hard shape assertion: .Require(spec) appends one
	// plan.OpShape operation with Require set, and the shape walk fails the
	// build when the propagated shape does not satisfy it at this point. Nil
	// means the operation asserts nothing.
	Require *shape.Spec
	// Prefer carries the chain's soft solver preference: .Prefer(spec) appends
	// one plan.OpShape operation with Prefer set, and the solver merges every
	// preference on the chain to bias otherwise-open conversion choices. Nil
	// means the operation carries no preference.
	Prefer *shape.Spec
}

type destinationIntent struct {
	Name     string
	URI      string
	Protocol av.ProtocolID
	MIMEType string
	Format   av.FormatID
}

type policyIntent struct {
	Realtime bool
}

// CodecChangePolicy says how a decoding chain reacts when a live source
// renegotiates its codec mid-stream: whether a compatible decoder is rebound
// in place, whether a keyframe is requested after the switch, whether media
// is dropped until a sync point, and whether a different codec fails the
// chain instead of rebinding.
type CodecChangePolicy struct {
	RebindCompatible     bool
	RequestKeyframe      bool
	DropUntilSync        bool
	FailOnDifferentCodec bool
}

// RealtimeCodecChangePolicy is the live-receive default: rebind compatible
// decoders, request a keyframe, drop until sync, and fail on a genuinely
// different codec.
func RealtimeCodecChangePolicy() CodecChangePolicy {
	return CodecChangePolicy{
		RebindCompatible:     true,
		RequestKeyframe:      true,
		DropUntilSync:        true,
		FailOnDifferentCodec: true,
	}
}

func operationSpecForDecode(codec codec.CodecSpec, component string) operationSpec {
	return operationSpec{Kind: plan.OpDecode, Component: component, Decode: cloneCodecSpec(codec)}
}

func operationSpecForCopy(codec codec.CodecSpec) operationSpec {
	return operationSpec{Kind: plan.OpCopy, Component: "packet-copy", Detail: "explicit packet copy", Encode: cloneCodecSpec(codec)}
}

func operationSpecForEncode(codec codec.CodecSpec) operationSpec {
	if codec.Copy {
		return operationSpecForCopy(codec)
	}
	return operationSpec{Kind: plan.OpEncode, Component: string(codec.ID), Encode: cloneCodecSpec(codec)}
}

func operationSpecForStage(stage pipeline.Stage) operationSpec {
	name := ""
	if stage != nil {
		name = stage.Name()
	}
	return operationSpec{Kind: plan.OpStage, Component: name, Stage: stage}
}

func operationSpecCodec(operation operationSpec) codec.CodecSpec {
	switch operation.Kind {
	case plan.OpDecode:
		return cloneCodecSpec(operation.Decode)
	case plan.OpEncode, plan.OpCopy:
		return cloneCodecSpec(operation.Encode)
	default:
		return codec.CodecSpec{}
	}
}

func operationSpecForShape(shape shape.Spec) operationSpec {
	return operationSpec{Kind: plan.OpShape, Component: "shape", Shape: shape}
}

// operationSpecForAutoPolicy is the policy-carrying operation .Auto(...)
// appends: an plan.OpShape annotation with no shape facts (it never changes the
// media) whose Auto field opts the chain into shape solving.
func operationSpecForAutoPolicy(policies []shape.Policy) operationSpec {
	var policy shape.Policy
	for i := range policies {
		policy = policy.Union(policies[i])
	}
	return operationSpec{Kind: plan.OpShape, Component: "auto", Auto: &policy}
}

// operationSpecForRequire is the assertion-carrying operation .Require(...)
// appends: an plan.OpShape annotation with no shape facts of its own (it never
// changes the media and lowers to no runtime node) whose Require field the
// shape walk enforces as a hard constraint at this chain position.
func operationSpecForRequire(spec shape.Spec) operationSpec {
	return operationSpec{Kind: plan.OpShape, Component: "require", Require: &spec}
}

// operationSpecForPreference is the preference-carrying operation .Prefer(...)
// appends: an plan.OpShape annotation with no shape facts (it never changes
// the media and lowers to no runtime node) whose Prefer field biases the shape
// solver's otherwise-open choices.
func operationSpecForPreference(spec shape.Spec) operationSpec {
	return operationSpec{Kind: plan.OpShape, Component: "prefer", Prefer: &spec}
}

// chainAutoPolicy unions the chain's .Auto(...) policies. The second result
// reports whether any policy operation is present — an empty .Auto() activates
// solving while allowing nothing, so refusals name the exact policy to add.
func chainAutoPolicy(operations []operationSpec) (shape.Policy, bool) {
	var policy shape.Policy
	active := false
	for i := range operations {
		if operations[i].Auto == nil {
			continue
		}
		active = true
		policy = policy.Union(*operations[i].Auto)
	}
	return policy, active
}

// operationSpecIsAnnotation reports whether the operation is a pure annotation
// carrier — an .Auto(...) policy, a .Require(...) assertion, or a .Prefer(...)
// preference — that lowers to no runtime node and does no work.
func operationSpecIsAnnotation(operation operationSpec) bool {
	return operation.Kind == plan.OpShape && mediaShapeEmpty(operation.Shape) &&
		(operation.Auto != nil || operation.Require != nil || operation.Prefer != nil)
}

func operationSpecForTransform(transform TransformSpec) operationSpec {
	return operationSpec{
		Kind:      plan.OpTransform,
		Component: transformFactoryName(transform),
		Transform: cloneTransformSpec(transform),
	}
}

func operationSpecForTap(tap TapRef, media av.MediaType, after plan.OperationKind) operationSpec {
	domain := tap.domain
	if domain == "" {
		domain = tapDomainForAfter(after)
	}
	intent := tapIntent{Name: tap.name, MediaKind: media, Domain: domain, After: after}
	return operationSpec{Kind: plan.OpTap, Component: tap.name, Tap: intent}
}

// tapDomainForAfter infers a domain-less tap's media domain from the operation it
// follows: packets after select/copy/encode, frames otherwise.
func tapDomainForAfter(after plan.OperationKind) shape.MediaDomain {
	switch after {
	case plan.OpSelect, plan.OpCopy, plan.OpEncode:
		return shape.DomainPacket
	default:
		return shape.DomainFrame
	}
}

func operationSpecAfter(operations []operationSpec, fallback plan.OperationKind) plan.OperationKind {
	after := fallback
	for i := range operations {
		switch operations[i].Kind {
		case plan.OpTap:
			continue
		default:
			after = operations[i].Kind
		}
	}
	return after
}

func operationSpecsContainKind(operations []operationSpec, kind plan.OperationKind) bool {
	for i := range operations {
		if operations[i].Kind == kind {
			return true
		}
	}
	return false
}

func operationSpecsContainChainStep(operations []operationSpec) bool {
	for i := range operations {
		switch operations[i].Kind {
		case plan.OpStage, plan.OpTransform:
			return true
		case plan.OpShape:
			// Shape annotations with facts are steps; empty annotations (the
			// .Auto(...) policy carrier) lower to nothing and constrain nothing.
			if !mediaShapeEmpty(operations[i].Shape) {
				return true
			}
		case plan.OpTap:
			if operations[i].Tap.Domain != shape.DomainPacket {
				return true
			}
		}
	}
	return false
}

func jobStreamHasDecodeOperation(stream *jobStreamBuild) bool {
	if stream == nil {
		return false
	}
	return operationSpecsContainKind(stream.operations, plan.OpDecode)
}

func ensureJobStreamDecodeOperation(stream *jobStreamBuild) {
	if stream == nil {
		return
	}
	if jobStreamHasDecodeOperation(stream) {
		return
	}
	// Reached only when no decode op exists yet (so no codec options were given);
	// an explicit Decode(opts) always appends its own plan.OpDecode.
	operation := operationSpecForDecode(codec.CodecSpec{}, string(stream.selector.Codec))
	stream.operations = append([]operationSpec{operation}, stream.operations...)
}

func jobIntentStream(intent intent) (streamIntent, bool) {
	if len(intent.Streams) == 0 {
		return streamIntent{}, false
	}
	return intent.Streams[0], true
}

func jobStreamIntent(stream *jobStreamBuild) streamIntent {
	if stream == nil {
		return streamIntent{}
	}
	selected := streamSelectFromAV(stream.selector)
	selected.Input = stream.input
	return streamIntent{
		Name:         stream.name,
		Select:       selected,
		Operations:   jobOperationSpecs(stream),
		CodecChange:  stream.codecChange,
		Destinations: destinationNamesWithOverrides(stream.outputs, stream.outputNames),
	}
}

func branchStreamIntent(stream streamBuild) streamIntent {
	return streamIntent{
		Name:         stream.name,
		Select:       streamSelectFromAV(stream.selector),
		From:         stream.from,
		Operations:   streamBuildOperationSpecs(stream),
		Destinations: append([]string(nil), stream.destinationNames...),
	}
}

func jobOperationSpecs(stream *jobStreamBuild) []operationSpec {
	if stream == nil {
		return nil
	}
	// The operation list is authoritative: every builder method (Decode, Copy,
	// Encode, transforms, taps, and the implicit decode-for-sink in To) appends
	// its operation, so there is nothing to reconstruct from decode/encode flags.
	return cloneOperationSpecs(stream.operations)
}

func streamBuildOperationSpecs(stream streamBuild) []operationSpec {
	if len(stream.sharedOps) != 0 || len(stream.privateOps) != 0 {
		return streamBuildSplitOperationSpecs(stream)
	}
	if len(stream.operations) != 0 {
		return cloneOperationSpecs(stream.operations)
	}
	operations := make([]operationSpec, 0, 2)
	if stream.decode {
		operations = append(operations, operationSpec{
			Kind:      plan.OpDecode,
			Component: string(stream.selector.Codec),
			Decode:    cloneCodecSpec(stream.decodeCodec),
			Shared:    stream.from.Domain() == shape.DomainFrame,
		})
	}
	// The encode op is carried by stream.operations (or sharedOps/privateOps in
	// the split path) — plannedBranchPrivateOperationSpecs injects the copy for
	// the parentPacket passthrough — so there is no encode to reconstruct here.
	return operations
}

func streamBuildSplitOperationSpecs(stream streamBuild) []operationSpec {
	operations := make([]operationSpec, 0, len(stream.sharedOps)+len(stream.privateOps)+2)
	operations = append(operations, cloneOperationSpecs(stream.sharedOps)...)
	operations = append(operations, cloneOperationSpecs(stream.privateOps)...)
	if stream.decode && !operationSpecsContainKind(operations, plan.OpDecode) {
		operation := operationSpecForDecode(stream.decodeCodec, string(stream.selector.Codec))
		operation.Shared = stream.from.Domain() == shape.DomainFrame && len(stream.sharedOps) != 0
		operations = append([]operationSpec{operation}, operations...)
	}
	// The encode op (plan.OpEncode/plan.OpCopy) is always already in sharedOps/privateOps,
	// so there is nothing to re-add.
	return operations
}

func plannedBranchSharedOperationSpecs(stream *jobStreamBuild, spec BranchSpec, parentPacket bool) []operationSpec {
	if stream == nil || parentPacket {
		return nil
	}
	parentOperations := jobOperationSpecs(stream)
	if len(parentOperations) == 0 {
		return nil
	}
	if spec.source.tap == "" {
		return sharedOperationSpecs(parentOperations)
	}
	if prefix, ok := operationSpecsThroughTap(parentOperations, spec.source.tap); ok {
		return sharedOperationSpecs(prefix)
	}
	if chainHasDecode(parentOperations) && spec.source.tap == defaultDecodedTapName(stream.selector.Type) {
		if prefix, ok := operationSpecsThroughKind(parentOperations, plan.OpDecode); ok {
			return sharedOperationSpecs(prefix)
		}
		return sharedOperationSpecs([]operationSpec{operationSpecForDecode(chainDecodeCodec(parentOperations), string(stream.selector.Codec))})
	}
	return nil
}

func plannedBranchPrivateOperationSpecs(stream *jobStreamBuild, spec BranchSpec, parentPacket bool) []operationSpec {
	if !parentPacket || chainHasDecode(spec.operations) || codecIntentSet(chainEncodeSpec(spec.operations)) {
		return cloneOperationSpecs(spec.operations)
	}
	if stream == nil {
		return cloneOperationSpecs(spec.operations)
	}
	if len(spec.operations) != 0 {
		if operationSpecsContainKind(spec.operations, plan.OpCopy) {
			return cloneOperationSpecs(spec.operations)
		}
		if prefix, ok := operationSpecsThroughKind(stream.operations, plan.OpCopy); ok {
			out := cloneOperationSpecs(prefix)
			out = append(out, cloneOperationSpecs(spec.operations)...)
			return out
		}
		out := []operationSpec{operationSpecForCopy(codec.Copy())}
		out = append(out, cloneOperationSpecs(spec.operations)...)
		return out
	}
	if spec.source.tap != "" {
		if prefix, ok := operationSpecsThroughTap(stream.operations, spec.source.tap); ok {
			return prefix
		}
	}
	return cloneOperationSpecs(stream.operations)
}

func sharedOperationSpecs(operations []operationSpec) []operationSpec {
	if len(operations) == 0 {
		return nil
	}
	out := cloneOperationSpecs(operations)
	for i := range out {
		out[i].Shared = true
	}
	return out
}

func operationSpecsThroughKind(operations []operationSpec, kind plan.OperationKind) ([]operationSpec, bool) {
	for i := range operations {
		if operations[i].Kind == kind {
			return cloneOperationSpecs(operations[:i+1]), true
		}
	}
	return nil, false
}

func operationSpecsThroughTap(operations []operationSpec, tap string) ([]operationSpec, bool) {
	if tap == "" {
		return nil, false
	}
	for i := range operations {
		operation := operations[i]
		if operation.Kind != plan.OpTap {
			continue
		}
		if operation.Component == tap || operation.Tap.Name == tap {
			return cloneOperationSpecs(operations[:i+1]), true
		}
	}
	return nil, false
}

// operationSpecTaps derives a stream's exported taps from its plan.OpTap operations —
// the single source of truth — instead of the parallel chain-step-tap and
// post-encode-tap projections. Each plan.OpTap already carries the resolved tap
// (name/domain/media/after) from operationSpecForTap.
func operationSpecTaps(operations []operationSpec, media av.MediaType) []tapIntent {
	taps := make([]tapIntent, 0, len(operations))
	for i := range operations {
		if operations[i].Kind != plan.OpTap {
			continue
		}
		tap := operations[i].Tap
		if tap.MediaKind == "" {
			tap.MediaKind = media // backfill from the resolved stream selector
		}
		taps = append(taps, tap)
	}
	return taps
}

func initialStepAfter(decode bool) plan.OperationKind {
	if decode {
		return plan.OpDecode
	}
	return ""
}

func jobStreamOutputs(stream *jobStreamBuild) []destinationSpec {
	if stream == nil || len(stream.outputs) == 0 {
		return nil
	}
	return append([]destinationSpec(nil), stream.outputs...)
}

func jobStreamOutputNames(stream *jobStreamBuild) []string {
	if stream == nil || len(stream.outputNames) == 0 {
		return nil
	}
	return append([]string(nil), stream.outputNames...)
}

func streamStageMissingError(stream streamIntent) error {
	return &BuildError{
		Code:      errcode.StageMissing,
		Operation: "build stream",
		Node:      jobStreamIntentName(stream),
		Reason:    "custom stream stage is nil",
		Suggestions: []string{
			"pass a non-nil stage to .Do(stage)",
			"use goav.FrameFunc, goav.PacketFunc, or goav.EventFunc for small hooks",
			"remove .Do(...) when no custom processing is needed",
		},
		Cause: ErrNilStage,
	}
}

func validateJobStreamOutputKinds(operation string, stream streamIntent, outputs []destinationSpec) error {
	encode := chainEncodeSpec(stream.Operations)
	if outputsContainSinkDestination(outputs) && outputsContainMuxDestination(outputs) && !codecIntentSet(encode) {
		return mixedStreamOutputError(operation, stream)
	}
	if encode.ID == "" && !encode.Copy && outputsContainMuxDestination(outputs) {
		return streamEncodeMissingError(operation, stream)
	}
	return nil
}

func mixedStreamOutputError(operation string, stream streamIntent) error {
	return &BuildError{
		Code:      errcode.OutputKindMixed,
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    "stream recipes cannot mix sinks and muxed outputs",
		Suggestions: []string{
			"use .To(goav.Sink(...)) for decoded frames",
			"call .Encode(codec.Opus(...)), .Encode(codec.VP8(...)), or .Encode(codec.VP9(...)) before .To(goav.File(...)) for encoded output",
			"use .Branches(...) when one stream needs separate decoded and encoded branches",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func streamEncodeMissingError(operation string, stream streamIntent) error {
	return &BuildError{
		Code:      errcode.EncodeMissing,
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    "decoded frames cannot be written to a muxed output without an encoder",
		Details: []string{
			"expected_shape=" + shape.New(shape.Domain(shape.DomainPacket), shape.Media(stream.Select.Type)).String(),
			"actual_shape=" + shape.Frame(stream.Select.Type).String(),
		},
		Suggestions: []string{
			"call .Encode(codec.Opus(...)), .Encode(codec.VP8(...)), or .Encode(codec.VP9(...)) before .To(goav.File(...))",
			"send decoded frames to goav.Sink(...)",
			"use .Copy().To(output) if you want to copy packets without decoding",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func jobStreamIntentName(stream streamIntent) string {
	return firstNonEmpty(stream.Name, string(stream.Select.ID), string(stream.Select.Type), "stream")
}

func jobStreamName(stream *jobStreamBuild) string {
	if stream == nil {
		return "stream"
	}
	return firstNonEmpty(stream.name, string(stream.selector.ID), string(stream.selector.Type), "stream")
}

func duplicateJobStreamError(existing *jobStreamBuild, next *jobStreamBuild) error {
	return &BuildError{
		Code:      errcode.StreamDuplicate,
		Operation: "build job",
		Node:      jobStreamName(next),
		Reason:    "ordinary stream recipes select one audio or video stream",
		Details: []string{
			"first stream: " + jobStreamName(existing),
			"second stream: " + jobStreamName(next),
		},
		Suggestions: []string{
			"keep one .Audio(...) or .Video(...) chain on goav.From(...)",
			"use goav.From(input).Video().Decode().Branches(...) for multiple branches from one stream",
			"use the expert graph API for custom multi-stream routing",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func streamIntentHasOperation(stream streamIntent) bool {
	for i := range stream.Operations {
		// Annotation carriers (.Auto/.Require/.Prefer) opt into solving or
		// assert shapes but do no work; a chain holding only annotations still
		// has no operation. Decode, copy, and encode are ordinary operations
		// (plan.OpDecode/plan.OpCopy/plan.OpEncode) on the same list.
		if !operationSpecIsAnnotation(stream.Operations[i]) {
			return true
		}
	}
	return false
}

func jobStreamChainSteps(stream *jobStreamBuild) []chainStep {
	if stream == nil {
		return nil
	}
	return chainStepsFromChainOperations(stream.operations)
}

func cloneChainSteps(steps []chainStep) []chainStep {
	if len(steps) == 0 {
		return nil
	}
	out := make([]chainStep, 0, len(steps))
	for i := range steps {
		step := steps[i]
		step.transform = cloneTransformSpec(step.transform)
		out = append(out, step)
	}
	return out
}

func outputsContainMuxDestination(outputs []destinationSpec) bool {
	for i := range outputs {
		if outputs[i].sink == nil {
			return true
		}
	}
	return false
}

func outputsContainSinkDestination(outputs []destinationSpec) bool {
	for i := range outputs {
		if outputs[i].sink != nil {
			return true
		}
	}
	return false
}
