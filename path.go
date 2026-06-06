package goav

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

type PathSpec struct {
	name       string
	media      av.MediaType
	steps      []jobStreamStep
	transforms []TransformSpec
	taps       []string
	encode     CodecSpec
	labels     []string
	err        error
}

type PathBuilder struct {
	spec PathSpec
}

func Path(name string) *PathBuilder {
	return &PathBuilder{spec: PathSpec{name: name}}
}

func (b *PathBuilder) Apply(flow Flow) *PathBuilder {
	if b == nil {
		return b
	}
	spec, err := flowSpecFrom(flow)
	if err != nil {
		b.setErr(err)
		return b
	}
	if codecIntentSet(b.spec.encode) && (len(spec.transforms) != 0 || codecIntentSet(spec.encode)) {
		b.setErr(streamStepAfterEncodeError("build path", firstNonEmpty(b.spec.name, "path"), "flow", b.spec.encode))
		return b
	}
	b.spec.steps = append(b.spec.steps, streamStepsFromTransforms(spec.transforms)...)
	b.spec.transforms = append(b.spec.transforms, cloneTransformSpecs(spec.transforms)...)
	if codecIntentSet(spec.encode) {
		return b.Encode(spec.encode)
	}
	return b
}

func (b *PathBuilder) Do(stage pipeline.Stage) *PathBuilder {
	if b == nil {
		return b
	}
	if codecIntentSet(b.spec.encode) {
		b.setErr(streamStepAfterEncodeError("build path", firstNonEmpty(b.spec.name, "path"), "custom stage", b.spec.encode))
		return b
	}
	if stage == nil {
		b.setErr(streamStageMissingError(StreamIntent{Name: firstNonEmpty(b.spec.name, "path")}))
		return b
	}
	b.spec.steps = append(b.spec.steps, jobStreamStep{stage: stage})
	return b
}

func (b *PathBuilder) Resize(width int, height int, options ...resizeOption) *PathBuilder {
	if b == nil {
		return b
	}
	if codecIntentSet(b.spec.encode) {
		b.setErr(streamStepAfterEncodeError("build path", firstNonEmpty(b.spec.name, "path"), "resize", b.spec.encode))
		return b
	}
	transform := Resize(width, height, options...)
	b.spec.steps = append(b.spec.steps, jobStreamStep{transform: transform})
	b.spec.transforms = append(b.spec.transforms, transform)
	return b
}

func (b *PathBuilder) Resample(sampleRate int, channels int, options ...audioOption) *PathBuilder {
	if b == nil {
		return b
	}
	if codecIntentSet(b.spec.encode) {
		b.setErr(streamStepAfterEncodeError("build path", firstNonEmpty(b.spec.name, "path"), "resample", b.spec.encode))
		return b
	}
	transform := Resample(sampleRate, channels, options...)
	b.spec.steps = append(b.spec.steps, jobStreamStep{transform: transform})
	b.spec.transforms = append(b.spec.transforms, transform)
	return b
}

func (b *PathBuilder) Tap(name string) *PathBuilder {
	if b == nil {
		return b
	}
	if name == "" {
		b.setErr(&BuildError{
			Code:      "tap_invalid",
			Operation: "build path",
			Node:      firstNonEmpty(b.spec.name, "path"),
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

func (b *PathBuilder) Encode(codec CodecSpec) *PathBuilder {
	if b == nil {
		return b
	}
	if codecIntentSet(b.spec.encode) {
		b.setErr(duplicateStreamEncodeError("build path", firstNonEmpty(b.spec.name, "path"), b.spec.encode, codec))
		return b
	}
	b.spec.encode = codec
	return b
}

func (b *PathBuilder) Opus(bitrate int, options ...codecOption) *PathBuilder {
	return b.Encode(Opus(append([]codecOption{Bitrate(bitrate)}, options...)...))
}

func (b *PathBuilder) VP8(bitrate int, options ...codecOption) *PathBuilder {
	return b.Encode(VP8(append([]codecOption{Bitrate(bitrate)}, options...)...))
}

func (b *PathBuilder) VP9(bitrate int, options ...codecOption) *PathBuilder {
	return b.Encode(VP9(append([]codecOption{Bitrate(bitrate)}, options...)...))
}

func (b *PathBuilder) To(labels ...string) PathSpec {
	if b == nil {
		return PathSpec{err: nilPathError()}
	}
	spec := b.snapshot()
	spec.labels = append([]string(nil), labels...)
	return spec
}

func (b *PathBuilder) snapshot() PathSpec {
	spec := b.spec
	spec.steps = cloneJobStreamSteps(spec.steps)
	spec.transforms = cloneTransformSpecs(spec.transforms)
	spec.taps = append([]string(nil), spec.taps...)
	spec.labels = append([]string(nil), spec.labels...)
	return spec
}

func (b *PathBuilder) setErr(err error) {
	if b.spec.err == nil {
		b.spec.err = err
	}
}

func (b *JobStreamBuilder) Paths(paths ...PathSpec) *Job {
	stream := b.current()
	job := b.job
	if len(paths) == 0 {
		job.setErr(pathMissingError(jobStreamName(stream)))
		return job
	}
	if len(job.inputs) != 1 {
		job.setErr(pathInputCountError(jobStreamName(stream), len(job.inputs)))
		return job
	}
	if len(job.outputs) != 0 || len(stream.outputs) != 0 {
		job.setErr(pathOutputScopeError(jobStreamName(stream)))
		return job
	}

	for i := range paths {
		if err := validatePathSpec(stream.selector.Type, i, paths[i]); err != nil {
			job.setErr(err)
			return job
		}
		job.pathStreams = append(job.pathStreams, streamBuild{
			name:       paths[i].name,
			selector:   stream.selector,
			fromTap:    lastStreamTap(stream),
			decode:     true,
			steps:      appendPathSteps(stream.steps, paths[i].steps),
			transforms: appendTransformSpecs(stream.transformSpecs(), paths[i].transforms),
			taps:       append([]string(nil), paths[i].taps...),
			encode:     paths[i].encode,
			labels:     append([]string(nil), paths[i].labels...),
		})
	}
	return job
}

func validatePathSpec(selected av.MediaType, index int, spec PathSpec) error {
	if spec.err != nil {
		return spec.err
	}
	if err := validateFlowMedia("build paths", firstNonEmpty(spec.name, "path"), selected, streamFlowSpec{name: spec.name, media: spec.media}); err != nil {
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

func pathMissingError(node string) error {
	return &BuildError{
		Code:      "path_missing",
		Operation: "build paths",
		Node:      node,
		Reason:    "Paths requires at least one encoded path",
		Suggestions: []string{
			"pass paths with goav.Path(name).VP9(...).To(label)",
			"define shared output labels once with .Outputs(goav.Output(\"label\", goav.FileOutput(...)))",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func nilPathError() error {
	return &BuildError{
		Code:      "path_invalid",
		Operation: "build path",
		Reason:    "path is nil",
		Suggestions: []string{
			"build paths with goav.Path(name)",
		},
		Cause: ErrUnsupportedBuild,
	}
}
