package goav

import (
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

// Flow is a reusable stream-local recipe fragment.
//
// Build flows with AudioFlow or VideoFlow, then apply them to one stream chain
// or to a Branch. A flow is only an operation sequence; branches own targets.
type Flow interface {
	Name() string
	isFlow()
}

type streamFlowSpec struct {
	name           string
	media          av.MediaType
	decode         bool
	steps          []jobStreamStep
	postEncodeTaps []string
	transforms     []TransformSpec
	encode         CodecSpec
	err            error
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

func (b *AudioFlowBuilder) Decode() *AudioFlowBuilder {
	if b == nil {
		return b
	}
	b.flowBuilder.decode()
	return b
}

func (b *AudioFlowBuilder) Resample(sampleRate int, channels int, options ...audioOption) *AudioFlowBuilder {
	if b == nil {
		return b
	}
	b.flowBuilder.transform(Resample(sampleRate, channels, options...))
	return b
}

func (b *AudioFlowBuilder) Do(stage pipeline.Stage) *AudioFlowBuilder {
	if b == nil {
		return b
	}
	b.flowBuilder.stage(stage)
	return b
}

func (b *AudioFlowBuilder) Tap(name string) *AudioFlowBuilder {
	if b == nil {
		return b
	}
	b.flowBuilder.tap(name)
	return b
}

func (b *AudioFlowBuilder) Encode(codec CodecSpec) *AudioFlowBuilder {
	if b == nil {
		return b
	}
	b.flowBuilder.encode(codec)
	return b
}

func (b *AudioFlowBuilder) Copy() *AudioFlowBuilder {
	return b.Encode(Copy())
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

func (b *AudioFlowBuilder) flowSpec() streamFlowSpec {
	if b == nil {
		return streamFlowSpec{err: nilFlowError()}
	}
	return b.flowBuilder.snapshot()
}

func (b *VideoFlowBuilder) Decode() *VideoFlowBuilder {
	if b == nil {
		return b
	}
	b.flowBuilder.decode()
	return b
}

func (b *VideoFlowBuilder) Resize(width int, height int, options ...resizeOption) *VideoFlowBuilder {
	if b == nil {
		return b
	}
	b.flowBuilder.transform(Resize(width, height, options...))
	return b
}

func (b *VideoFlowBuilder) Do(stage pipeline.Stage) *VideoFlowBuilder {
	if b == nil {
		return b
	}
	b.flowBuilder.stage(stage)
	return b
}

func (b *VideoFlowBuilder) Tap(name string) *VideoFlowBuilder {
	if b == nil {
		return b
	}
	b.flowBuilder.tap(name)
	return b
}

func (b *VideoFlowBuilder) Encode(codec CodecSpec) *VideoFlowBuilder {
	if b == nil {
		return b
	}
	b.flowBuilder.encode(codec)
	return b
}

func (b *VideoFlowBuilder) Copy() *VideoFlowBuilder {
	return b.Encode(Copy())
}

func (b *VideoFlowBuilder) VP8(bitrate int, options ...codecOption) *VideoFlowBuilder {
	return b.Encode(VP8(append([]codecOption{Bitrate(bitrate)}, options...)...))
}

func (b *VideoFlowBuilder) VP9(bitrate int, options ...codecOption) *VideoFlowBuilder {
	return b.Encode(VP9(append([]codecOption{Bitrate(bitrate)}, options...)...))
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

func (b *flowBuilder) decode() {
	if b == nil {
		return
	}
	if codecIntentSet(b.spec.encode) {
		b.setErr(streamStepAfterEncodeError("build flow", firstNonEmpty(b.spec.name, "flow"), "decode", b.spec.encode))
		return
	}
	if b.spec.decode {
		b.setErr(duplicateFlowDecodeError(firstNonEmpty(b.spec.name, "flow")))
		return
	}
	if len(b.spec.steps) != 0 {
		b.setErr(flowDecodeOrderError(firstNonEmpty(b.spec.name, "flow")))
		return
	}
	b.spec.decode = true
}

func (b *flowBuilder) transform(spec TransformSpec) {
	if b == nil {
		return
	}
	if codecIntentSet(b.spec.encode) {
		b.setErr(streamStepAfterEncodeError("build flow", firstNonEmpty(b.spec.name, "flow"), flowTransformStepName(spec), b.spec.encode))
		return
	}
	transform := cloneTransformSpec(spec)
	b.spec.steps = append(b.spec.steps, jobStreamStep{transform: transform})
	b.spec.transforms = append(b.spec.transforms, cloneTransformSpec(spec))
}

func (b *flowBuilder) stage(stage pipeline.Stage) {
	if b == nil {
		return
	}
	if codecIntentSet(b.spec.encode) {
		b.setErr(streamStepAfterEncodeError("build flow", firstNonEmpty(b.spec.name, "flow"), "custom stage", b.spec.encode))
		return
	}
	if stage == nil {
		b.setErr(streamStageMissingError(StreamIntent{Name: firstNonEmpty(b.spec.name, "flow")}))
		return
	}
	b.spec.steps = append(b.spec.steps, jobStreamStep{stage: stage})
}

func (b *flowBuilder) tap(name string) {
	if b == nil {
		return
	}
	if name == "" {
		b.setErr(&BuildError{
			Code:      "tap_invalid",
			Operation: "build flow",
			Node:      firstNonEmpty(b.spec.name, "flow"),
			Reason:    "tap name is empty",
			Suggestions: []string{
				"call .Tap(\"audio.voice.frames\") or another stable tap name",
				"omit .Tap(...) when no runtime branch should attach at that point",
			},
			Cause: ErrUnsupportedBuild,
		})
		return
	}
	if codecIntentSet(b.spec.encode) {
		b.spec.postEncodeTaps = append(b.spec.postEncodeTaps, name)
		return
	}
	b.spec.steps = append(b.spec.steps, jobStreamStep{tap: name})
}

func (b *flowBuilder) encode(codec CodecSpec) {
	if b == nil {
		return
	}
	if codecIntentSet(b.spec.encode) {
		b.setErr(duplicateFlowEncodeError(b.spec.name, b.spec.encode, codec))
		return
	}
	if codec.Copy && (b.spec.decode || len(b.spec.steps) != 0) {
		b.setErr(flowCopyDomainError("build flow", firstNonEmpty(b.spec.name, "flow")))
		return
	}
	b.spec.encode = codec
}

func (b *flowBuilder) snapshot() streamFlowSpec {
	if b == nil {
		return streamFlowSpec{err: nilFlowError()}
	}
	spec := b.spec
	spec.steps = cloneJobStreamSteps(spec.steps)
	spec.postEncodeTaps = append([]string(nil), spec.postEncodeTaps...)
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

func duplicateFlowDecodeError(node string) error {
	return &BuildError{
		Code:      "flow_decode_duplicate",
		Operation: "build flow",
		Node:      node,
		Reason:    "flow already decodes its input packets",
		Suggestions: []string{
			"call .Decode() once at the start of the flow",
			"remove the second .Decode() call",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func flowDecodeOrderError(node string) error {
	return &BuildError{
		Code:      "flow_decode_order_invalid",
		Operation: "build flow",
		Node:      node,
		Reason:    "decode must be the first flow operation",
		Suggestions: []string{
			"write goav.AudioFlow(name).Decode().Resample(...)",
			"omit .Decode() when the flow is only applied after stream decode",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func flowDecodeDomainError(operation string, node string) error {
	return &BuildError{
		Code:      "flow_decode_domain_mismatch",
		Operation: operation,
		Node:      firstNonEmpty(node, "flow"),
		Reason:    "flow decoding requires a packet-domain stream point",
		Suggestions: []string{
			"omit .Decode() when applying the flow after stream decode",
			"use the flow from a packet branch or packet tap when it should own decode",
			"split packet-preserving streams with .Copy().Branches(...) before applying the flow",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func flowCopyDomainError(operation string, node string) error {
	return &BuildError{
		Code:      "flow_copy_domain_mismatch",
		Operation: operation,
		Node:      firstNonEmpty(node, "flow"),
		Reason:    "flow copying requires a packet-domain stream point",
		Suggestions: []string{
			"start packet-preserving reusable work with goav.AudioFlow(name).Copy() or goav.VideoFlow(name).Copy()",
			"declare packet taps after copy with .Copy().Tap(name)",
			"use .Decode().Resample(...).Opus(...) when the flow should transform frames",
		},
		Cause: ErrUnsupportedBuild,
	}
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

func branchInputCountError(node string, count int) error {
	return &BuildError{
		Code:      "input_count_unsupported",
		Operation: "build branches",
		Node:      node,
		Reason:    "branches currently compose from one input",
		Details: []string{
			fmt.Sprintf("inputs=%d", count),
		},
		Suggestions: []string{
			"start branches from goav.From(input).Audio() or goav.From(input).Video() with one input",
			"use the expert graph API when combining several sources manually",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchOutputScopeError(node string) error {
	return &BuildError{
		Code:      "output_scope_mixed",
		Operation: "build branches",
		Node:      node,
		Reason:    "branch targets are declared inside Branch(...).To(...)",
		Suggestions: []string{
			"route branches with .Branches(goav.Branch(name).To(goav.Target(name, endpoint)))",
			"use stream .To(endpoint) only for one ordinary stream output",
		},
		Cause: ErrUnsupportedBuild,
	}
}
