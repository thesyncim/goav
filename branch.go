package goav

import (
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

// BranchDestination is a typed destination for a branch. Use Target for named
// mux/sink groups, or pass an endpoint such as FileOutput, URIOutput, or
// FrameSink directly.
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
	name       string
	media      av.MediaType
	steps      []jobStreamStep
	transforms []TransformSpec
	taps       []string
	encode     CodecSpec
	targets    []TargetSpec
	labels     []string

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

func (b *BranchBuilder) Apply(flow Flow) *BranchBuilder {
	if b == nil {
		return b
	}
	spec, err := flowSpecFrom(flow)
	if err != nil {
		b.setErr(err)
		return b
	}
	if codecIntentSet(b.spec.encode) && (len(spec.transforms) != 0 || codecIntentSet(spec.encode)) {
		b.setErr(streamStepAfterEncodeError("build branch", firstNonEmpty(b.spec.name, "branch"), "flow", b.spec.encode))
		return b
	}
	b.spec.steps = append(b.spec.steps, streamStepsFromTransforms(spec.transforms)...)
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
	b.spec.steps = append(b.spec.steps, jobStreamStep{tap: name})
	b.spec.taps = append(b.spec.taps, name)
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
	spec.transforms = cloneTransformSpecs(spec.transforms)
	spec.taps = append([]string(nil), spec.taps...)
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

	for i := range branches {
		if err := validateBranchSpec(stream.selector.Type, i, branches[i]); err != nil {
			job.setErr(err)
			return job
		}
		if err := job.addBranchTargets(branches[i].targets...); err != nil {
			job.setErr(err)
			return job
		}
		job.branchStreams = append(job.branchStreams, streamBuild{
			name:       branches[i].name,
			selector:   stream.selector,
			fromTap:    lastStreamTap(stream),
			decode:     true,
			steps:      appendBranchSteps(stream.steps, branches[i].steps),
			transforms: appendTransformSpecs(stream.transformSpecs(), branches[i].transforms),
			taps:       append([]string(nil), branches[i].taps...),
			encode:     branches[i].encode,
			labels:     append([]string(nil), branches[i].labels...),
		})
	}
	return job
}

func validateBranchSpec(selected av.MediaType, index int, spec BranchSpec) error {
	if spec.err != nil {
		return spec.err
	}
	if err := validateFlowMedia("build branches", firstNonEmpty(spec.name, "branch"), selected, streamFlowSpec{name: spec.name, media: spec.media}); err != nil {
		return err
	}
	if spec.name == "" {
		return transcodeIntentBranchNameMissingError(index, StreamIntent{Select: StreamSelect{Type: selected}})
	}
	if !codecIntentSet(spec.encode) {
		return transcodeEncodeMissingError(StreamIntent{Name: spec.name, Select: StreamSelect{Type: selected}})
	}
	if len(spec.labels) == 0 {
		return transcodeBranchOutputMissingError(StreamIntent{Name: spec.name, Select: StreamSelect{Type: selected}})
	}
	seen := make(map[string]int, len(spec.labels))
	for i, label := range spec.labels {
		if label == "" {
			return transcodeEmptyOutputLabelError(streamBuild{name: spec.name, selector: av.StreamSelector{Type: selected}}, i)
		}
		if firstIndex, ok := seen[label]; ok {
			return transcodeDuplicateBranchOutputError(
				StreamIntent{Name: spec.name, Select: StreamSelect{Type: selected}, RouteTo: spec.labels},
				label,
				firstIndex,
				i,
			)
		}
		seen[label] = i
	}
	return nil
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
			"use goav.FileOutput(...), goav.URIOutput(...), or goav.FrameSink(...) as endpoints",
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
