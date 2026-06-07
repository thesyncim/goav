package goav

import (
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

// Chain is a reusable stream-local recipe fragment.
//
// Build chains with Flow(name).Audio() or Flow(name).Video(), then apply them
// to one stream chain or to a Branch. A chain is only an operation sequence;
// branches own destinations.
type Chain interface {
	Name() string
	InputShapes() ShapeSet
	OutputShapes(MediaShape) ShapeSet
	Taps() []TapRef
	isChain()
}

type chainSpec struct {
	name           string
	media          av.MediaType
	decode         bool
	decodeCodec    CodecSpec
	operations     []OperationSpec
	postEncodeTaps []string
	encode         CodecSpec
	err            error
}

type chainBuilder struct {
	spec chainSpec
}

type chainSnapshotter interface {
	chainSpec() chainSpec
}

type flowRoot struct {
	name string
}

// Flow starts a reusable operation sequence.
func Flow(name string) *flowRoot {
	return &flowRoot{name: name}
}

func (b *flowRoot) Audio() *audioChain {
	if b == nil {
		return newAudioChain("")
	}
	return newAudioChain(b.name)
}

func (b *flowRoot) Video() *videoChain {
	if b == nil {
		return newVideoChain("")
	}
	return newVideoChain(b.name)
}

func newAudioChain(name string) *audioChain {
	return &audioChain{chainBuilder{spec: chainSpec{name: name, media: av.MediaAudio}}}
}

func newVideoChain(name string) *videoChain {
	return &videoChain{chainBuilder{spec: chainSpec{name: name, media: av.MediaVideo}}}
}

type audioChain struct {
	chainBuilder
}

type videoChain struct {
	chainBuilder
}

func (b *audioChain) Name() string {
	if b == nil {
		return ""
	}
	return b.chainBuilder.name()
}

func (b *videoChain) Name() string {
	if b == nil {
		return ""
	}
	return b.chainBuilder.name()
}

func (b *audioChain) isChain() {}

func (b *videoChain) isChain() {}

func (b *audioChain) InputShapes() ShapeSet {
	if b == nil {
		return nil
	}
	return b.chainBuilder.inputShapes()
}

func (b *videoChain) InputShapes() ShapeSet {
	if b == nil {
		return nil
	}
	return b.chainBuilder.inputShapes()
}

func (b *audioChain) OutputShapes(input MediaShape) ShapeSet {
	if b == nil {
		return nil
	}
	return b.chainBuilder.outputShapes(input)
}

func (b *videoChain) OutputShapes(input MediaShape) ShapeSet {
	if b == nil {
		return nil
	}
	return b.chainBuilder.outputShapes(input)
}

func (b *audioChain) Taps() []TapRef {
	if b == nil {
		return nil
	}
	return b.chainBuilder.taps()
}

func (b *videoChain) Taps() []TapRef {
	if b == nil {
		return nil
	}
	return b.chainBuilder.taps()
}

func (b *audioChain) Decode(options ...CodecOption) *audioChain {
	if b == nil {
		return b
	}
	b.chainBuilder.decode(options...)
	return b
}

func (b *audioChain) Resample(sampleRate int, channels int, options ...audioOption) *audioChain {
	if b == nil {
		return b
	}
	b.chainBuilder.transform(Resample(sampleRate, channels, options...))
	return b
}

func (b *audioChain) Do(stage pipeline.Stage) *audioChain {
	if b == nil {
		return b
	}
	b.chainBuilder.stage(stage)
	return b
}

func (b *audioChain) Shape(shape MediaShape) *audioChain {
	if b == nil {
		return b
	}
	b.chainBuilder.shape(shape)
	return b
}

func (b *audioChain) Tap(tap TapRef) *audioChain {
	if b == nil {
		return b
	}
	b.chainBuilder.tap(tap)
	return b
}

func (b *audioChain) Encode(codec CodecSpec) *audioChain {
	if b == nil {
		return b
	}
	b.chainBuilder.encode(codec)
	return b
}

func (b *audioChain) Copy() *audioChain {
	return b.Encode(Copy())
}

func (b *audioChain) chainSpec() chainSpec {
	if b == nil {
		return chainSpec{err: nilFlowError()}
	}
	return b.chainBuilder.snapshot()
}

func (b *videoChain) Decode(options ...CodecOption) *videoChain {
	if b == nil {
		return b
	}
	b.chainBuilder.decode(options...)
	return b
}

func (b *videoChain) Resize(width int, height int, options ...resizeOption) *videoChain {
	if b == nil {
		return b
	}
	b.chainBuilder.transform(Resize(width, height, options...))
	return b
}

func (b *videoChain) Do(stage pipeline.Stage) *videoChain {
	if b == nil {
		return b
	}
	b.chainBuilder.stage(stage)
	return b
}

func (b *videoChain) Shape(shape MediaShape) *videoChain {
	if b == nil {
		return b
	}
	b.chainBuilder.shape(shape)
	return b
}

func (b *videoChain) Tap(tap TapRef) *videoChain {
	if b == nil {
		return b
	}
	b.chainBuilder.tap(tap)
	return b
}

func (b *videoChain) Encode(codec CodecSpec) *videoChain {
	if b == nil {
		return b
	}
	b.chainBuilder.encode(codec)
	return b
}

func (b *videoChain) Copy() *videoChain {
	return b.Encode(Copy())
}

func (b *videoChain) chainSpec() chainSpec {
	if b == nil {
		return chainSpec{err: nilFlowError()}
	}
	return b.chainBuilder.snapshot()
}

func (b *chainBuilder) name() string {
	if b == nil {
		return ""
	}
	return b.spec.name
}

func (b *chainBuilder) inputShapes() ShapeSet {
	if b == nil {
		return nil
	}
	switch {
	case b.spec.decode:
		return ShapeSet{PacketShape(b.spec.media, b.spec.decodeCodec.ID)}
	case b.spec.encode.Copy:
		return ShapeSet{PacketShape(b.spec.media, "")}
	default:
		return ShapeSet{FrameShape(b.spec.media)}
	}
}

func (b *chainBuilder) outputShapes(input MediaShape) ShapeSet {
	if b == nil {
		return nil
	}
	shape := input
	if shape.MediaKind == "" {
		shape.MediaKind = b.spec.media
	}
	if shape.Domain == "" {
		switch {
		case b.spec.decode || b.spec.encode.Copy:
			shape.Domain = DomainPacket
		default:
			shape.Domain = DomainFrame
		}
	}
	for i := range b.spec.operations {
		shapes := b.spec.operations[i].OutputShapes(shape)
		if len(shapes) != 0 {
			shape = shapes[0]
		}
	}
	return ShapeSet{shape}
}

func (b *chainBuilder) taps() []TapRef {
	if b == nil {
		return nil
	}
	out := make([]TapRef, 0, len(b.spec.operations))
	for i := range b.spec.operations {
		operation := b.spec.operations[i]
		if operation.Kind != OpTap || operation.Tap.Name == "" {
			continue
		}
		out = append(out, TapRef{name: operation.Tap.Name, domain: operation.Tap.Domain})
	}
	return out
}

func (b *chainBuilder) decode(options ...CodecOption) {
	if b == nil {
		return
	}
	if codecIntentSet(b.spec.encode) {
		b.setErr(chainStepAfterEncodeError("build flow", firstNonEmpty(b.spec.name, "flow"), "decode", b.spec.encode))
		return
	}
	if b.spec.decode {
		b.setErr(duplicateFlowDecodeError(firstNonEmpty(b.spec.name, "flow")))
		return
	}
	if operationSpecsContainChainStep(b.spec.operations) {
		b.setErr(flowDecodeOrderError(firstNonEmpty(b.spec.name, "flow")))
		return
	}
	b.spec.decode = true
	b.spec.decodeCodec = mergeDecodeCodecSpec(b.spec.decodeCodec, codecSpecFromOptions(options...))
	b.spec.operations = append(b.spec.operations, operationSpecForDecode(b.spec.decodeCodec, string(b.spec.decodeCodec.ID)))
}

func (b *chainBuilder) transform(spec TransformSpec) {
	if b == nil {
		return
	}
	if codecIntentSet(b.spec.encode) {
		b.setErr(chainStepAfterEncodeError("build flow", firstNonEmpty(b.spec.name, "flow"), chainTransformStepName(spec), b.spec.encode))
		return
	}
	transform := cloneTransformSpec(spec)
	b.spec.operations = append(b.spec.operations, operationSpecForTransform(transform))
}

func (b *chainBuilder) stage(stage pipeline.Stage) {
	if b == nil {
		return
	}
	if codecIntentSet(b.spec.encode) {
		b.setErr(chainStepAfterEncodeError("build flow", firstNonEmpty(b.spec.name, "flow"), "custom stage", b.spec.encode))
		return
	}
	if stage == nil {
		b.setErr(streamStageMissingError(StreamIntent{Name: firstNonEmpty(b.spec.name, "flow")}))
		return
	}
	b.spec.operations = append(b.spec.operations, operationSpecForStage(stage))
}

func (b *chainBuilder) shape(shape MediaShape) {
	if b == nil {
		return
	}
	if codecIntentSet(b.spec.encode) {
		b.setErr(chainStepAfterEncodeError("build flow", firstNonEmpty(b.spec.name, "flow"), "shape", b.spec.encode))
		return
	}
	b.spec.operations = append(b.spec.operations, operationSpecForShape(shape))
}

func (b *chainBuilder) tap(tap TapRef) {
	if b == nil {
		return
	}
	if tap.name == "" {
		b.setErr(&BuildError{
			Code:      "tap_invalid",
			Operation: "build flow",
			Node:      firstNonEmpty(b.spec.name, "flow"),
			Reason:    "tap name is empty",
			Suggestions: []string{
				"call .Tap(goav.FrameTap(\"audio.voice.frames\")) or another stable tap ref",
				"omit .Tap(...) when no runtime branch should attach at that point",
			},
			Cause: ErrUnsupportedBuild,
		})
		return
	}
	if codecIntentSet(b.spec.encode) {
		if err := validateTapDomain("build flow", firstNonEmpty(b.spec.name, "flow"), tap, DomainPacket); err != nil {
			b.setErr(err)
			return
		}
		b.spec.postEncodeTaps = append(b.spec.postEncodeTaps, tap.name)
		b.spec.operations = append(b.spec.operations, operationSpecForTap(tap, b.spec.media, operationSpecAfter(b.spec.operations, OpEncode)))
		return
	}
	if err := validateTapDomain("build flow", firstNonEmpty(b.spec.name, "flow"), tap, DomainFrame); err != nil {
		b.setErr(err)
		return
	}
	b.spec.operations = append(b.spec.operations, operationSpecForTap(tap, b.spec.media, operationSpecAfter(b.spec.operations, initialStepAfter(b.spec.decode))))
}

func (b *chainBuilder) encode(codec CodecSpec) {
	if b == nil {
		return
	}
	if codecIntentSet(b.spec.encode) {
		b.setErr(duplicateFlowEncodeError(b.spec.name, b.spec.encode, codec))
		return
	}
	if codec.Copy && (b.spec.decode || operationSpecsContainChainStep(b.spec.operations)) {
		b.setErr(flowCopyDomainError("build flow", firstNonEmpty(b.spec.name, "flow")))
		return
	}
	b.spec.encode = cloneCodecSpec(codec)
	b.spec.operations = append(b.spec.operations, operationSpecForEncode(b.spec.encode))
}

func (b *chainBuilder) snapshot() chainSpec {
	if b == nil {
		return chainSpec{err: nilFlowError()}
	}
	spec := b.spec
	spec.decodeCodec = cloneCodecSpec(spec.decodeCodec)
	spec.encode = cloneCodecSpec(spec.encode)
	spec.operations = cloneOperationSpecs(spec.operations)
	spec.postEncodeTaps = append([]string(nil), spec.postEncodeTaps...)
	return spec
}

func (b *chainBuilder) setErr(err error) {
	if b.spec.err == nil {
		b.spec.err = err
	}
}

func chainSpecFrom(flow Chain) (chainSpec, error) {
	if flow == nil {
		return chainSpec{}, nilFlowError()
	}
	snapshotter, ok := flow.(chainSnapshotter)
	if !ok {
		return chainSpec{}, nilFlowError()
	}
	spec := snapshotter.chainSpec()
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

func chainTransformStepName(spec TransformSpec) string {
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
			"write goav.Flow(name).Audio().Decode().Resample(...)",
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
			"start packet-preserving reusable work with goav.Flow(name).Audio().Copy() or goav.Flow(name).Video().Copy()",
			"declare packet taps after copy with .Copy().Tap(goav.PacketTap(name))",
			"use .Decode().Resample(...).Encode(goav.Opus(...)) when the flow should transform frames",
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
			"build flows with goav.Flow(name).Audio() or goav.Flow(name).Video()",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func validateChainMedia(operation string, node string, selected av.MediaType, spec chainSpec) error {
	if selected == "" || spec.media == "" || selected == spec.media {
		return nil
	}
	return &BuildError{
		Code:      "flow_media_mismatch",
		Operation: operation,
		Node:      firstNonEmpty(spec.name, node, "flow"),
		Reason:    string(spec.media) + " flow cannot be applied to " + string(selected) + " stream",
		Suggestions: []string{
			"use goav.Flow(name).Audio() with .Audio()",
			"use goav.Flow(name).Video() with .Video()",
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
		Reason:    "branch destinations are declared inside Branch(...).To(...)",
		Suggestions: []string{
			"route branches with .Branches(goav.Branch(name).To(goav.File(name, writer)))",
			"use stream .To(goav.File(...)) or .To(goav.Sink(...)) only for one ordinary stream destination",
		},
		Cause: ErrUnsupportedBuild,
	}
}
