package goav

import (
	"fmt"

	"github.com/thesyncim/goav/av"
)

// Flow is a reusable stream-local recipe fragment.
//
// Build flows with AudioFlow or VideoFlow, then apply them to one stream chain
// or route them as named paths with JobStreamBuilder.Paths.
type Flow interface {
	Name() string
	isFlow()
}

type streamFlowSpec struct {
	name       string
	media      av.MediaType
	transforms []TransformSpec
	encode     CodecSpec
	err        error
}

type flowBuilder struct {
	spec streamFlowSpec
}

type flowSnapshotter interface {
	flowSpec() streamFlowSpec
}

// AudioFlow creates a reusable audio stream fragment.
func AudioFlow(name string) *AudioFlowBuilder {
	return &AudioFlowBuilder{flowBuilder{spec: streamFlowSpec{name: name, media: av.MediaAudio}}}
}

// VideoFlow creates a reusable video stream fragment.
func VideoFlow(name string) *VideoFlowBuilder {
	return &VideoFlowBuilder{flowBuilder{spec: streamFlowSpec{name: name, media: av.MediaVideo}}}
}

type AudioFlowBuilder struct {
	flowBuilder
}

type VideoFlowBuilder struct {
	flowBuilder
}

func (b *AudioFlowBuilder) Name() string {
	if b == nil {
		return ""
	}
	return b.flowBuilder.name()
}

func (b *VideoFlowBuilder) Name() string {
	if b == nil {
		return ""
	}
	return b.flowBuilder.name()
}

func (b *AudioFlowBuilder) isFlow() {}

func (b *VideoFlowBuilder) isFlow() {}

func (b *AudioFlowBuilder) Resample(sampleRate int, channels int, options ...audioOption) *AudioFlowBuilder {
	if b == nil {
		return b
	}
	b.flowBuilder.transform(Resample(sampleRate, channels, options...))
	return b
}

func (b *AudioFlowBuilder) Encode(codec CodecSpec) *AudioFlowBuilder {
	if b == nil {
		return b
	}
	b.flowBuilder.encode(codec)
	return b
}

func (b *AudioFlowBuilder) Opus(bitrate int, options ...codecOption) *AudioFlowBuilder {
	return b.Encode(Opus(append([]codecOption{Bitrate(bitrate)}, options...)...))
}

func (b *AudioFlowBuilder) OpusVoice() *AudioFlowBuilder {
	return b.Encode(OpusVoice())
}

func (b *AudioFlowBuilder) OpusMusic() *AudioFlowBuilder {
	return b.Encode(OpusMusic())
}

func (b *AudioFlowBuilder) To(labels ...string) PathSpec {
	if b == nil {
		return pathFromFlow(streamFlowSpec{err: nilFlowError()}, labels...)
	}
	return b.flowBuilder.to(labels...)
}

func (b *AudioFlowBuilder) flowSpec() streamFlowSpec {
	if b == nil {
		return streamFlowSpec{err: nilFlowError()}
	}
	return b.flowBuilder.snapshot()
}

func (b *VideoFlowBuilder) Resize(width int, height int, options ...resizeOption) *VideoFlowBuilder {
	if b == nil {
		return b
	}
	b.flowBuilder.transform(Resize(width, height, options...))
	return b
}

func (b *VideoFlowBuilder) Encode(codec CodecSpec) *VideoFlowBuilder {
	if b == nil {
		return b
	}
	b.flowBuilder.encode(codec)
	return b
}

func (b *VideoFlowBuilder) VP8(bitrate int, options ...codecOption) *VideoFlowBuilder {
	return b.Encode(VP8(append([]codecOption{Bitrate(bitrate)}, options...)...))
}

func (b *VideoFlowBuilder) VP9(bitrate int, options ...codecOption) *VideoFlowBuilder {
	return b.Encode(VP9(append([]codecOption{Bitrate(bitrate)}, options...)...))
}

func (b *VideoFlowBuilder) To(labels ...string) PathSpec {
	if b == nil {
		return pathFromFlow(streamFlowSpec{err: nilFlowError()}, labels...)
	}
	return b.flowBuilder.to(labels...)
}

func (b *VideoFlowBuilder) flowSpec() streamFlowSpec {
	if b == nil {
		return streamFlowSpec{err: nilFlowError()}
	}
	return b.flowBuilder.snapshot()
}

func (b *flowBuilder) name() string {
	if b == nil {
		return ""
	}
	return b.spec.name
}

func (b *flowBuilder) transform(spec TransformSpec) {
	if b == nil {
		return
	}
	if codecIntentSet(b.spec.encode) {
		b.setErr(streamStepAfterEncodeError("build flow", firstNonEmpty(b.spec.name, "flow"), flowTransformStepName(spec), b.spec.encode))
		return
	}
	b.spec.transforms = append(b.spec.transforms, cloneTransformSpec(spec))
}

func (b *flowBuilder) encode(codec CodecSpec) {
	if b == nil {
		return
	}
	if codecIntentSet(b.spec.encode) {
		b.setErr(duplicateFlowEncodeError(b.spec.name, b.spec.encode, codec))
		return
	}
	b.spec.encode = codec
}

func (b *flowBuilder) to(labels ...string) PathSpec {
	return pathFromFlow(b.snapshot(), labels...)
}

func (b *flowBuilder) snapshot() streamFlowSpec {
	if b == nil {
		return streamFlowSpec{err: nilFlowError()}
	}
	spec := b.spec
	spec.transforms = cloneTransformSpecs(spec.transforms)
	return spec
}

func (b *flowBuilder) setErr(err error) {
	if b.spec.err == nil {
		b.spec.err = err
	}
}

func flowSpecFrom(flow Flow) (streamFlowSpec, error) {
	if flow == nil {
		return streamFlowSpec{}, nilFlowError()
	}
	snapshotter, ok := flow.(flowSnapshotter)
	if !ok {
		return streamFlowSpec{}, nilFlowError()
	}
	spec := snapshotter.flowSpec()
	if spec.err != nil {
		return spec, spec.err
	}
	return spec, nil
}

func cloneTransformSpecs(specs []TransformSpec) []TransformSpec {
	if len(specs) == 0 {
		return nil
	}
	out := make([]TransformSpec, 0, len(specs))
	for i := range specs {
		out = append(out, cloneTransformSpec(specs[i]))
	}
	return out
}

func cloneTransformSpec(spec TransformSpec) TransformSpec {
	var out TransformSpec
	if spec.Resize != nil {
		resize := *spec.Resize
		out.Resize = &resize
	}
	if spec.Resample != nil {
		resample := *spec.Resample
		out.Resample = &resample
	}
	return out
}

func pathFromFlow(spec streamFlowSpec, labels ...string) PathSpec {
	return PathSpec{
		name:       spec.name,
		media:      spec.media,
		steps:      streamStepsFromTransforms(spec.transforms),
		transforms: cloneTransformSpecs(spec.transforms),
		encode:     spec.encode,
		labels:     append([]string(nil), labels...),
		err:        spec.err,
	}
}

func flowTransformStepName(spec TransformSpec) string {
	switch {
	case spec.Resize != nil:
		return "resize"
	case spec.Resample != nil:
		return "resample"
	default:
		return "transform"
	}
}

func duplicateFlowEncodeError(name string, first CodecSpec, second CodecSpec) error {
	return duplicateStreamEncodeError("build flow", firstNonEmpty(name, "flow"), first, second)
}

func nilFlowError() error {
	return &BuildError{
		Code:      "flow_invalid",
		Operation: "build flow",
		Reason:    "flow is nil",
		Suggestions: []string{
			"build flows with goav.AudioFlow(name) or goav.VideoFlow(name)",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func validateFlowMedia(operation string, node string, selected av.MediaType, spec streamFlowSpec) error {
	if selected == "" || spec.media == "" || selected == spec.media {
		return nil
	}
	return &BuildError{
		Code:      "flow_media_mismatch",
		Operation: operation,
		Node:      firstNonEmpty(spec.name, node, "flow"),
		Reason:    string(spec.media) + " flow cannot be applied to " + string(selected) + " stream",
		Suggestions: []string{
			"use goav.AudioFlow(...) with .Audio()",
			"use goav.VideoFlow(...) with .Video()",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func pathInputCountError(node string, count int) error {
	return &BuildError{
		Code:      "input_count_unsupported",
		Operation: "build paths",
		Node:      node,
		Reason:    "paths currently compose from one input",
		Details: []string{
			fmt.Sprintf("inputs=%d", count),
		},
		Suggestions: []string{
			"start paths from goav.From(input).Audio() or goav.From(input).Video() with one input",
			"use the expert graph API when combining several sources manually",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func pathOutputScopeError(node string) error {
	return &BuildError{
		Code:      "output_scope_mixed",
		Operation: "build paths",
		Node:      node,
		Reason:    "path outputs are declared as labeled output groups",
		Suggestions: []string{
			"route paths with .To(\"label\")",
			"declare output groups once with .Outputs(goav.Output(\"label\", goav.FileOutput(...)))",
		},
		Cause: ErrUnsupportedBuild,
	}
}
