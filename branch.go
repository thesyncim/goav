package goav

import (
	"fmt"
	"sync/atomic"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

var targetSpecSeq atomic.Uint64

// Destination is accepted by To. Use Target for named mux/sink groups, or pass
// FileOutput, URIOutput, or Sink directly for one-off destinations.
type Destination interface {
	// Name overrides the destination name used for diagnostics and graph nodes.
	Name(string) Destination
	// MIME sets the MIME type used for format detection.
	MIME(string) Destination
	// Format sets the container format explicitly.
	Format(av.FormatID) Destination
	destination() destinationBinding
}

type destinationBinding struct {
	target    targetSpec
	dest      destinationSpec
	hasTarget bool
	hasDirect bool
}

type targetSpec struct {
	name string
	dest destinationSpec
	id   uint64
	err  error
}

// Target binds a stable target name to a concrete destination.
func Target(name string, dest Destination) Destination {
	if dest == nil {
		return targetSpec{err: destinationInvalidError("build target", firstNonEmpty(name, "target"), "target destination is nil")}
	}
	binding := dest.destination()
	if !binding.hasDirect {
		return targetSpec{err: destinationInvalidError("build target", firstNonEmpty(name, "target"), "target destination must be a file, URI, or sink destination")}
	}
	return newTargetSpec(name, binding.dest)
}

func newTargetSpec(name string, dest destinationSpec) targetSpec {
	if name == "" {
		return targetSpec{dest: dest, err: targetNameMissingError(dest)}
	}
	return targetSpec{
		name: name,
		dest: dest.withName(firstNonEmpty(dest.name, name)),
		id:   targetSpecSeq.Add(1),
	}
}

func (t targetSpec) destination() destinationBinding {
	return destinationBinding{target: t, hasTarget: true}
}

func (t targetSpec) Name(name string) Destination {
	t.name = name
	t.dest = t.dest.withName(firstNonEmpty(t.dest.name, name))
	return t
}

func (t targetSpec) MIME(mimeType string) Destination {
	t.dest = t.dest.withMIME(mimeType)
	return t
}

func (t targetSpec) Format(format av.FormatID) Destination {
	t.dest = t.dest.withFormat(format)
	return t
}

func (s destinationSpec) destination() destinationBinding {
	return destinationBinding{dest: s, hasDirect: true}
}

type BranchSpec struct {
	name           string
	media          av.MediaType
	decode         bool
	steps          []chainStep
	postEncodeTaps []string
	transforms     []TransformSpec
	encode         CodecSpec
	targets        []targetSpec
	labels         []string

	from      string
	tap       string
	tapDomain MediaDomain
	policy    pipeline.RoutePolicy
	label     string
	buffer    pipeline.BufferPolicy

	err error
}

type branchBuilder struct {
	spec BranchSpec
}

func Branch(name string) *branchBuilder {
	return &branchBuilder{spec: BranchSpec{name: name}}
}

func (b *branchBuilder) From(source any) *branchBuilder {
	if b == nil {
		return b
	}
	switch source := source.(type) {
	case TapRef:
		return b.fromTypedTap(source)
	case string:
		b.spec.from = source
		b.spec.tap = ""
		b.spec.tapDomain = ""
	default:
		b.setErr(branchSourceInvalidError(firstNonEmpty(b.spec.name, "branch")))
	}
	return b
}

func (b *branchBuilder) fromTypedTap(tap TapRef) *branchBuilder {
	if b == nil {
		return b
	}
	b.spec.tap = tap.name
	b.spec.tapDomain = tap.domain
	b.spec.from = ""
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

func (b *branchBuilder) Buffer(policy pipeline.BufferPolicy) *branchBuilder {
	if b == nil {
		return b
	}
	b.spec.buffer = policy
	return b
}

func (b *branchBuilder) Decode() *branchBuilder {
	if b == nil {
		return b
	}
	if codecIntentSet(b.spec.encode) {
		b.setErr(chainStepAfterEncodeError("build branch", firstNonEmpty(b.spec.name, "branch"), "decode", b.spec.encode))
		return b
	}
	if b.spec.decode {
		b.setErr(duplicateBranchDecodeError(firstNonEmpty(b.spec.name, "branch")))
		return b
	}
	if len(b.spec.steps) != 0 {
		b.setErr(branchDecodeOrderError(firstNonEmpty(b.spec.name, "branch")))
		return b
	}
	b.spec.decode = true
	return b
}

func (b *branchBuilder) Apply(flow Chain) *branchBuilder {
	if b == nil {
		return b
	}
	spec, err := flowSpecFrom(flow)
	if err != nil {
		b.setErr(err)
		return b
	}
	if codecIntentSet(b.spec.encode) && (spec.decode || len(spec.steps) != 0 || codecIntentSet(spec.encode)) {
		b.setErr(chainStepAfterEncodeError("build branch", firstNonEmpty(b.spec.name, "branch"), "flow", b.spec.encode))
		return b
	}
	if spec.media != "" {
		if b.spec.media == "" {
			b.spec.media = spec.media
		} else if err := validateFlowMedia("build branch", firstNonEmpty(b.spec.name, "branch"), b.spec.media, spec); err != nil {
			b.setErr(err)
			return b
		}
	}
	if spec.decode {
		if b.spec.decode {
			b.setErr(duplicateBranchDecodeError(firstNonEmpty(b.spec.name, "branch")))
			return b
		}
		if len(b.spec.steps) != 0 {
			b.setErr(branchDecodeOrderError(firstNonEmpty(b.spec.name, "branch")))
			return b
		}
		b.spec.decode = true
	}
	b.spec.steps = append(b.spec.steps, cloneChainSteps(spec.steps)...)
	b.spec.transforms = append(b.spec.transforms, cloneTransformSpecs(spec.transforms)...)
	if codecIntentSet(spec.encode) {
		if spec.encode.Copy && (b.spec.decode || len(b.spec.steps) != 0) {
			b.setErr(flowCopyDomainError("build branch", firstNonEmpty(spec.name, b.spec.name, "flow")))
			return b
		}
		b.Encode(spec.encode)
	}
	b.spec.postEncodeTaps = append(b.spec.postEncodeTaps, spec.postEncodeTaps...)
	return b
}

func (b *branchBuilder) Do(stages ...pipeline.Stage) *branchBuilder {
	if b == nil {
		return b
	}
	for i := range stages {
		if codecIntentSet(b.spec.encode) {
			b.setErr(chainStepAfterEncodeError("build branch", firstNonEmpty(b.spec.name, "branch"), "custom stage", b.spec.encode))
			return b
		}
		if stages[i] == nil {
			b.setErr(streamStageMissingError(StreamIntent{Name: firstNonEmpty(b.spec.name, "branch")}))
			return b
		}
		b.spec.steps = append(b.spec.steps, chainStep{stage: stages[i]})
	}
	return b
}

func (b *branchBuilder) Resize(width int, height int, options ...resizeOption) *branchBuilder {
	if b == nil {
		return b
	}
	if codecIntentSet(b.spec.encode) {
		b.setErr(chainStepAfterEncodeError("build branch", firstNonEmpty(b.spec.name, "branch"), "resize", b.spec.encode))
		return b
	}
	transform := Resize(width, height, options...)
	b.spec.steps = append(b.spec.steps, chainStep{transform: transform})
	b.spec.transforms = append(b.spec.transforms, transform)
	return b
}

func (b *branchBuilder) Resample(sampleRate int, channels int, options ...audioOption) *branchBuilder {
	if b == nil {
		return b
	}
	if codecIntentSet(b.spec.encode) {
		b.setErr(chainStepAfterEncodeError("build branch", firstNonEmpty(b.spec.name, "branch"), "resample", b.spec.encode))
		return b
	}
	transform := Resample(sampleRate, channels, options...)
	b.spec.steps = append(b.spec.steps, chainStep{transform: transform})
	b.spec.transforms = append(b.spec.transforms, transform)
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
	if codecIntentSet(b.spec.encode) {
		if err := validateTapDomain("build branch", firstNonEmpty(b.spec.name, "branch"), tap, DomainPacket); err != nil {
			b.setErr(err)
			return b
		}
		b.spec.postEncodeTaps = append(b.spec.postEncodeTaps, tap.name)
		return b
	}
	b.spec.steps = append(b.spec.steps, chainStep{tap: tap.name, tapDomain: tap.domain})
	return b
}

func (b *branchBuilder) Encode(codec CodecSpec) *branchBuilder {
	if b == nil {
		return b
	}
	if codecIntentSet(b.spec.encode) {
		b.setErr(duplicateStreamEncodeError("build branch", firstNonEmpty(b.spec.name, "branch"), b.spec.encode, codec))
		return b
	}
	if codec.Copy && b.spec.decode {
		b.setErr(branchDecodeCopyError(firstNonEmpty(b.spec.name, "branch")))
		return b
	}
	b.spec.encode = codec
	return b
}

func (b *branchBuilder) Copy() *branchBuilder {
	return b.Encode(Copy())
}

func (b *branchBuilder) Opus(bitrate int, options ...codecOption) *branchBuilder {
	return b.Encode(Opus(append([]codecOption{Bitrate(bitrate)}, options...)...))
}

func (b *branchBuilder) VP8(bitrate int, options ...codecOption) *branchBuilder {
	return b.Encode(VP8(append([]codecOption{Bitrate(bitrate)}, options...)...))
}

func (b *branchBuilder) VP9(bitrate int, options ...codecOption) *branchBuilder {
	return b.Encode(VP9(append([]codecOption{Bitrate(bitrate)}, options...)...))
}

func (b *branchBuilder) To(destinations ...Destination) BranchSpec {
	if b == nil {
		return BranchSpec{err: nilBranchError()}
	}
	spec := b.snapshot()
	if len(destinations) == 0 {
		spec.err = branchTargetMissingError(spec.name)
		return spec
	}
	for i := range destinations {
		destination := destinations[i]
		if destination == nil {
			spec.err = branchDestinationInvalidError(spec.name, "branch destination is nil")
			return spec
		}
		if err := appendDestination(&spec, destination.destination(), i); err != nil {
			spec.err = err
			return spec
		}
	}
	return spec
}

func (b *branchBuilder) snapshot() BranchSpec {
	spec := b.spec
	spec.steps = cloneChainSteps(spec.steps)
	spec.postEncodeTaps = append([]string(nil), spec.postEncodeTaps...)
	spec.transforms = cloneTransformSpecs(spec.transforms)
	spec.targets = cloneTargetSpecs(spec.targets)
	spec.labels = append([]string(nil), spec.labels...)
	return spec
}

func (b *branchBuilder) setErr(err error) {
	if b.spec.err == nil {
		b.spec.err = err
	}
}

func appendDestination(spec *BranchSpec, destination destinationBinding, index int) error {
	switch {
	case destination.hasTarget:
		target := cloneTargetSpec(destination.target)
		if target.err != nil {
			return target.err
		}
		if target.name == "" {
			return targetNameMissingError(target.dest)
		}
		target.dest = target.dest.withName(firstNonEmpty(target.dest.name, target.name))
		spec.targets = append(spec.targets, target)
		spec.labels = append(spec.labels, target.name)
		return nil
	case destination.hasDirect:
		destination := destination.dest
		name := destination.label(fmt.Sprintf("%s-%d", firstNonEmpty(spec.name, "branch"), index+1))
		target := newTargetSpec(name, destination)
		if target.err != nil {
			return target.err
		}
		spec.targets = append(spec.targets, target)
		spec.labels = append(spec.labels, target.name)
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

	parentPacket := stream.encode.Copy && !stream.decode && len(stream.steps) == 0
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
		if err := job.addBranchTargets(branches[i].targets...); err != nil {
			job.setErr(err)
			return job
		}
		encode := branches[i].encode
		if parentPacket && !branches[i].decode && !codecIntentSet(encode) {
			encode = Copy()
		}
		decode := !parentPacket || branches[i].decode
		sharedSteps, from, err := plannedBranchAnchor(stream, branches[i], parentPacket)
		if err != nil {
			job.setErr(err)
			return job
		}
		job.branchStreams = append(job.branchStreams, streamBuild{
			name:        branches[i].name,
			selector:    stream.selector,
			from:        from,
			decode:      decode,
			sharedSteps: sharedSteps,
			steps:       cloneChainSteps(branches[i].steps),
			postEncodeTaps: append(
				append([]string(nil), stream.postEncodeTaps...),
				branches[i].postEncodeTaps...,
			),
			transforms: appendTransformSpecs(transformSpecsFromChainSteps(sharedSteps), branches[i].transforms),
			encode:     encode,
			labels:     append([]string(nil), branches[i].labels...),
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
	if err := validateFlowMedia("build branches", firstNonEmpty(spec.name, "branch"), selected, streamFlowSpec{name: spec.name, media: spec.media}); err != nil {
		return err
	}
	if spec.name == "" {
		return branchIntentNameMissingError(index, StreamIntent{Select: StreamSelect{Type: selected}})
	}
	if len(spec.labels) == 0 {
		return branchIntentTargetMissingError(StreamIntent{Name: spec.name, Select: StreamSelect{Type: selected}})
	}
	stream := StreamIntent{Name: spec.name, Select: StreamSelect{Type: selected}}
	if spec.decode && !parentPacket {
		return branchDecodeDomainError(stream.Name)
	}
	if spec.encode.Copy {
		if spec.decode {
			return branchDecodeCopyError(stream.Name)
		}
		if !parentPacket {
			return branchCopyUnsupportedError(stream)
		}
	} else if parentPacket && codecIntentSet(spec.encode) && !spec.decode {
		return branchPacketEncodeUnsupportedError(stream, spec.encode)
	}
	if parentPacket && !spec.decode {
		for i := range spec.transforms {
			if err := validateTransformSpec("build branches", spec.name, spec.transforms[i]); err != nil {
				return err
			}
			return branchPacketTransformUnsupportedError(stream)
		}
	}
	if err := validateBranchStepTapDomains(spec, parentPacket); err != nil {
		return err
	}
	effectiveEncode := spec.encode
	if parentPacket && !spec.decode && !codecIntentSet(effectiveEncode) {
		effectiveEncode = Copy()
	}
	if !codecIntentSet(effectiveEncode) && !branchTargetsAllSinkDestinations(spec.targets) {
		return branchEncodeMissingError(stream)
	}
	seen := make(map[string]int, len(spec.labels))
	for i, label := range spec.labels {
		if label == "" {
			return transcodeEmptyOutputLabelError(streamBuild{name: spec.name, selector: av.StreamSelector{Type: selected}}, i)
		}
		if firstIndex, ok := seen[label]; ok {
			return duplicateBranchTargetRefError(
				StreamIntent{Name: spec.name, Select: StreamSelect{Type: selected}, Targets: spec.labels},
				label,
				firstIndex,
				i,
			)
		}
		seen[label] = i
	}
	return nil
}

func validateBranchStepTapDomains(spec BranchSpec, parentPacket bool) error {
	domain := DomainFrame
	if parentPacket && !spec.decode {
		domain = DomainPacket
	}
	for i := range spec.steps {
		step := spec.steps[i]
		if step.tap == "" {
			continue
		}
		if err := validateTapDomain("build branches", firstNonEmpty(spec.name, "branch"), TapRef{name: step.tap, domain: step.tapDomain}, domain); err != nil {
			return err
		}
	}
	return nil
}

func plannedBranchAnchor(stream *jobStreamBuild, spec BranchSpec, parentPacket bool) ([]chainStep, TapRef, error) {
	if spec.tap == "" {
		if parentPacket {
			return nil, lastStreamTapRef(stream), nil
		}
		return cloneChainSteps(stream.steps), lastStreamTapRef(stream), nil
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
	if steps, ok := chainStepsThroughTap(stream.steps, spec.tap); ok {
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
	for i := range stream.postEncodeTaps {
		if stream.postEncodeTaps[i] == tap {
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

func transformSpecsFromChainSteps(steps []chainStep) []TransformSpec {
	if len(steps) == 0 {
		return nil
	}
	transforms := make([]TransformSpec, 0, len(steps))
	for i := range steps {
		if steps[i].transform.Resize != nil || steps[i].transform.Resample != nil {
			transforms = append(transforms, cloneTransformSpec(steps[i].transform))
		}
	}
	return transforms
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
			"put .Opus(...), .VP8(...), or .VP9(...) on each goav.Branch(...) that writes a target",
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
		Reason:    "planned branches do not anchor from graph node names",
		Details: []string{
			"source: " + source,
		},
		Suggestions: []string{
			"use .From(goav.FrameTap(name)) or .From(goav.PacketTap(name)) to branch from a stable tap",
			"omit .From(...) to branch from the current stream point",
			"use Task.Attach(ctx, goav.Branch(name).From(node)...) for expert runtime graph attachment",
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
			"use .Decode().Encode(codec).To(target) for re-encoded packets",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchPacketEncodeUnsupportedError(stream StreamIntent, encode CodecSpec) error {
	return &BuildError{
		Code:      "packet_branch_encode_unsupported",
		Operation: "build branches",
		Node:      branchIntentName(stream),
		Reason:    "packet-domain planned branches cannot encode without decoding first",
		Details: []string{
			"encoder: " + codecIntentName(encode),
		},
		Suggestions: []string{
			"use .Decode().Branches(goav.Branch(name).Opus(...).To(target)) for encoded variants",
			"use .Copy().Branches(goav.Branch(name).To(target)) for packet-preserving variants",
			"attach a runtime branch from a frame Tap when late encoding is needed",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchPacketTransformUnsupportedError(stream StreamIntent) error {
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

func branchTargetsAllSinkDestinations(targets []targetSpec) bool {
	if len(targets) == 0 {
		return false
	}
	for i := range targets {
		if targets[i].dest.sink == nil {
			return false
		}
	}
	return true
}

func cloneTargetSpecs(targets []targetSpec) []targetSpec {
	if len(targets) == 0 {
		return nil
	}
	out := make([]targetSpec, 0, len(targets))
	for i := range targets {
		out = append(out, cloneTargetSpec(targets[i]))
	}
	return out
}

func cloneTargetSpec(target targetSpec) targetSpec {
	target.dest = cloneDestinationSpec(target.dest)
	return target
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
			"pass branches with goav.Branch(name).VP9(...).To(goav.Target(name, destination))",
			"reuse the same target value from multiple branches when they should share one mux group",
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

func branchTargetMissingError(name string) error {
	return &BuildError{
		Code:      "target_missing",
		Operation: "build branch",
		Node:      firstNonEmpty(name, "branch"),
		Reason:    "branch has no target",
		Suggestions: []string{
			"finish the branch with .To(goav.Target(\"web\", goav.FileOutput(...)))",
			"pass a file, URI, or sink destination directly when no shared target is needed",
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
		Code:      "target_invalid",
		Operation: operation,
		Node:      node,
		Reason:    reason,
		Suggestions: []string{
			"use goav.Target(name, destination) for named mux/sink groups",
			"use goav.FileOutput(...), goav.URIOutput(...), or goav.Sink(...) for one-off destinations",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func targetNameMissingError(dest destinationSpec) error {
	return &BuildError{
		Code:      "target_invalid",
		Operation: "build target",
		Node:      dest.label("target"),
		Reason:    "target name is empty",
		Suggestions: []string{
			"call goav.Target(\"web\", goav.FileOutput(...)) with a stable target name",
			"pass goav.FileOutput(...) directly to .To(...) when a separate target name is not needed",
		},
		Cause: ErrUnsupportedBuild,
	}
}
