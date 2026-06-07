package goav

import (
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

// BranchDestination is a typed destination for a branch. Use Target for named
// mux/sink groups, or pass an endpoint such as FileOutput, URIOutput, or
// SinkEndpoint directly.
type BranchDestination interface {
	branchDestination() branchDestination
}

type branchDestination struct {
	target      TargetSpec
	endpoint    EndpointSpec
	hasTarget   bool
	hasEndpoint bool
}

// TargetSpec names a logical destination. Several branches can feed the same
// target so the runtime can mux or group them as one output.
type TargetSpec struct {
	name     string
	endpoint EndpointSpec
	err      error
}

// Target binds a stable target name to a concrete endpoint.
func Target(name string, endpoint EndpointSpec) TargetSpec {
	if name == "" {
		return TargetSpec{endpoint: endpoint, err: targetNameMissingError(endpoint)}
	}
	return TargetSpec{name: name, endpoint: endpoint.Name(firstNonEmpty(endpoint.name, name))}
}

func (t TargetSpec) branchDestination() branchDestination {
	return branchDestination{target: t, hasTarget: true}
}

func (s EndpointSpec) branchDestination() branchDestination {
	return branchDestination{endpoint: s, hasEndpoint: true}
}

type BranchSpec struct {
	name           string
	media          av.MediaType
	decode         bool
	steps          []jobStreamStep
	postEncodeTaps []string
	transforms     []TransformSpec
	encode         CodecSpec
	targets        []TargetSpec
	labels         []string

	from   string
	tap    string
	policy pipeline.RoutePolicy
	label  string
	buffer pipeline.BufferPolicy

	err error
}

type BranchBuilder struct {
	spec BranchSpec
}

func Branch(name string) *BranchBuilder {
	return &BranchBuilder{spec: BranchSpec{name: name}}
}

func (b *BranchBuilder) From(node string) *BranchBuilder {
	if b == nil {
		return b
	}
	b.spec.from = node
	b.spec.tap = ""
	return b
}

func (b *BranchBuilder) FromTap(name string) *BranchBuilder {
	if b == nil {
		return b
	}
	b.spec.tap = name
	b.spec.from = ""
	return b
}

func (b *BranchBuilder) Stream(stream av.StreamID) *BranchBuilder {
	if b == nil {
		return b
	}
	b.spec.policy = pipeline.RouteByStream
	b.spec.label = string(stream)
	return b
}

func (b *BranchBuilder) Event(event av.EventType) *BranchBuilder {
	if b == nil {
		return b
	}
	b.spec.policy = pipeline.RouteByEvent
	b.spec.label = string(event)
	return b
}

func (b *BranchBuilder) Buffer(policy pipeline.BufferPolicy) *BranchBuilder {
	if b == nil {
		return b
	}
	b.spec.buffer = policy
	return b
}

func (b *BranchBuilder) Decode() *BranchBuilder {
	if b == nil {
		return b
	}
	if codecIntentSet(b.spec.encode) {
		b.setErr(streamStepAfterEncodeError("build branch", firstNonEmpty(b.spec.name, "branch"), "decode", b.spec.encode))
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

func (b *BranchBuilder) Apply(flow Flow) *BranchBuilder {
	if b == nil {
		return b
	}
	spec, err := flowSpecFrom(flow)
	if err != nil {
		b.setErr(err)
		return b
	}
	if codecIntentSet(b.spec.encode) && (spec.decode || len(spec.steps) != 0 || codecIntentSet(spec.encode)) {
		b.setErr(streamStepAfterEncodeError("build branch", firstNonEmpty(b.spec.name, "branch"), "flow", b.spec.encode))
		return b
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
	b.spec.steps = append(b.spec.steps, cloneJobStreamSteps(spec.steps)...)
	b.spec.transforms = append(b.spec.transforms, cloneTransformSpecs(spec.transforms)...)
	if codecIntentSet(spec.encode) {
		return b.Encode(spec.encode)
	}
	return b
}

func (b *BranchBuilder) Do(stages ...pipeline.Stage) *BranchBuilder {
	if b == nil {
		return b
	}
	for i := range stages {
		if codecIntentSet(b.spec.encode) {
			b.setErr(streamStepAfterEncodeError("build branch", firstNonEmpty(b.spec.name, "branch"), "custom stage", b.spec.encode))
			return b
		}
		if stages[i] == nil {
			b.setErr(streamStageMissingError(StreamIntent{Name: firstNonEmpty(b.spec.name, "branch")}))
			return b
		}
		b.spec.steps = append(b.spec.steps, jobStreamStep{stage: stages[i]})
	}
	return b
}

func (b *BranchBuilder) Resize(width int, height int, options ...resizeOption) *BranchBuilder {
	if b == nil {
		return b
	}
	if codecIntentSet(b.spec.encode) {
		b.setErr(streamStepAfterEncodeError("build branch", firstNonEmpty(b.spec.name, "branch"), "resize", b.spec.encode))
		return b
	}
	transform := Resize(width, height, options...)
	b.spec.steps = append(b.spec.steps, jobStreamStep{transform: transform})
	b.spec.transforms = append(b.spec.transforms, transform)
	return b
}

func (b *BranchBuilder) Resample(sampleRate int, channels int, options ...audioOption) *BranchBuilder {
	if b == nil {
		return b
	}
	if codecIntentSet(b.spec.encode) {
		b.setErr(streamStepAfterEncodeError("build branch", firstNonEmpty(b.spec.name, "branch"), "resample", b.spec.encode))
		return b
	}
	transform := Resample(sampleRate, channels, options...)
	b.spec.steps = append(b.spec.steps, jobStreamStep{transform: transform})
	b.spec.transforms = append(b.spec.transforms, transform)
	return b
}

func (b *BranchBuilder) Tap(name string) *BranchBuilder {
	if b == nil {
		return b
	}
	if name == "" {
		b.setErr(&BuildError{
			Code:      "tap_invalid",
			Operation: "build branch",
			Node:      firstNonEmpty(b.spec.name, "branch"),
			Reason:    "tap name is empty",
			Suggestions: []string{
				"call .Tap(\"video.720p.frames\") or another stable tap name",
				"omit .Tap(...) when no runtime branch should attach at that point",
			},
			Cause: ErrUnsupportedBuild,
		})
		return b
	}
	if codecIntentSet(b.spec.encode) {
		b.spec.postEncodeTaps = append(b.spec.postEncodeTaps, name)
		return b
	}
	b.spec.steps = append(b.spec.steps, jobStreamStep{tap: name})
	return b
}

func (b *BranchBuilder) Encode(codec CodecSpec) *BranchBuilder {
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

func (b *BranchBuilder) Copy() *BranchBuilder {
	return b.Encode(Copy())
}

func (b *BranchBuilder) Opus(bitrate int, options ...codecOption) *BranchBuilder {
	return b.Encode(Opus(append([]codecOption{Bitrate(bitrate)}, options...)...))
}

func (b *BranchBuilder) VP8(bitrate int, options ...codecOption) *BranchBuilder {
	return b.Encode(VP8(append([]codecOption{Bitrate(bitrate)}, options...)...))
}

func (b *BranchBuilder) VP9(bitrate int, options ...codecOption) *BranchBuilder {
	return b.Encode(VP9(append([]codecOption{Bitrate(bitrate)}, options...)...))
}

func (b *BranchBuilder) To(destinations ...BranchDestination) BranchSpec {
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
		if err := appendBranchDestination(&spec, destination.branchDestination(), i); err != nil {
			spec.err = err
			return spec
		}
	}
	return spec
}

func (b *BranchBuilder) snapshot() BranchSpec {
	spec := b.spec
	spec.steps = cloneJobStreamSteps(spec.steps)
	spec.postEncodeTaps = append([]string(nil), spec.postEncodeTaps...)
	spec.transforms = cloneTransformSpecs(spec.transforms)
	spec.targets = cloneTargetSpecs(spec.targets)
	spec.labels = append([]string(nil), spec.labels...)
	return spec
}

func (b *BranchBuilder) setErr(err error) {
	if b.spec.err == nil {
		b.spec.err = err
	}
}

func appendBranchDestination(spec *BranchSpec, destination branchDestination, index int) error {
	switch {
	case destination.hasTarget:
		target := cloneTargetSpec(destination.target)
		if target.err != nil {
			return target.err
		}
		if target.name == "" {
			return targetNameMissingError(target.endpoint)
		}
		target.endpoint = target.endpoint.Name(firstNonEmpty(target.endpoint.name, target.name))
		spec.targets = append(spec.targets, target)
		spec.labels = append(spec.labels, target.name)
		return nil
	case destination.hasEndpoint:
		endpoint := destination.endpoint
		name := endpoint.label(fmt.Sprintf("%s-%d", firstNonEmpty(spec.name, "branch"), index+1))
		target := Target(name, endpoint)
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

func (b *JobStreamBuilder) Branches(branches ...BranchSpec) *Job {
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
		sharedSteps := cloneJobStreamSteps(stream.steps)
		if parentPacket {
			sharedSteps = nil
		}
		job.branchStreams = append(job.branchStreams, streamBuild{
			name:        branches[i].name,
			selector:    stream.selector,
			fromTap:     lastStreamTap(stream),
			decode:      decode,
			sharedSteps: sharedSteps,
			steps:       cloneJobStreamSteps(branches[i].steps),
			postEncodeTaps: append(
				append([]string(nil), stream.postEncodeTaps...),
				branches[i].postEncodeTaps...,
			),
			transforms: appendTransformSpecs(stream.transformSpecs(), branches[i].transforms),
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
	effectiveEncode := spec.encode
	if parentPacket && !spec.decode && !codecIntentSet(effectiveEncode) {
		effectiveEncode = Copy()
	}
	if !codecIntentSet(effectiveEncode) && !branchTargetsAllSinkEndpoints(spec.targets) {
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
			"use .Decode().To(goav.SinkEndpoint(...)) for decoded frames",
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

func branchTargetsAllSinkEndpoints(targets []TargetSpec) bool {
	if len(targets) == 0 {
		return false
	}
	for i := range targets {
		if targets[i].endpoint.sink == nil {
			return false
		}
	}
	return true
}

func cloneTargetSpecs(targets []TargetSpec) []TargetSpec {
	if len(targets) == 0 {
		return nil
	}
	out := make([]TargetSpec, 0, len(targets))
	for i := range targets {
		out = append(out, cloneTargetSpec(targets[i]))
	}
	return out
}

func cloneTargetSpec(target TargetSpec) TargetSpec {
	target.endpoint = cloneEndpointSpec(target.endpoint)
	return target
}

func cloneEndpointSpec(endpoint EndpointSpec) EndpointSpec {
	return endpoint
}

func branchMissingError(node string) error {
	return &BuildError{
		Code:      "branch_missing",
		Operation: "build branches",
		Node:      node,
		Reason:    "Branches requires at least one encoded branch",
		Suggestions: []string{
			"pass branches with goav.Branch(name).VP9(...).To(goav.Target(name, endpoint))",
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
			"pass an endpoint directly when no shared target is needed, such as .To(goav.FileOutput(...))",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchDestinationInvalidError(name string, reason string) error {
	return &BuildError{
		Code:      "target_invalid",
		Operation: "build branch",
		Node:      firstNonEmpty(name, "branch"),
		Reason:    reason,
		Suggestions: []string{
			"use goav.Target(name, endpoint) for named mux/sink groups",
			"use goav.FileOutput(...), goav.URIOutput(...), or goav.SinkEndpoint(...) as endpoints",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func targetNameMissingError(endpoint EndpointSpec) error {
	return &BuildError{
		Code:      "target_invalid",
		Operation: "build target",
		Node:      endpoint.label("target"),
		Reason:    "target name is empty",
		Suggestions: []string{
			"call goav.Target(\"web\", goav.FileOutput(...)) with a stable target name",
			"pass goav.FileOutput(...) directly to .To(...) when a separate target name is not needed",
		},
		Cause: ErrUnsupportedBuild,
	}
}
