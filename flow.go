package goav

import (
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// chain is a reusable stream-local recipe fragment.
//
// Build chains with Flow(name).Audio() or Flow(name).Video(), then apply them
// to one stream chain or to a Branch. A chain is only an operation sequence;
// branches own destinations.
type chain interface {
	Name() string
	InputShapes() shape.Set
	OutputShapes(shape.Spec) shape.Set
	Taps() []tapRef
	isChain()
}

type chainSpec struct {
	name       string
	media      av.MediaType
	operations []operationSpec
	err        error
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

func (b *audioChain) InputShapes() shape.Set {
	if b == nil {
		return nil
	}
	return b.chainBuilder.inputShapes()
}

func (b *videoChain) InputShapes() shape.Set {
	if b == nil {
		return nil
	}
	return b.chainBuilder.inputShapes()
}

func (b *audioChain) OutputShapes(input shape.Spec) shape.Set {
	if b == nil {
		return nil
	}
	return b.chainBuilder.outputShapes(input)
}

func (b *videoChain) OutputShapes(input shape.Spec) shape.Set {
	if b == nil {
		return nil
	}
	return b.chainBuilder.outputShapes(input)
}

func (b *audioChain) Taps() []tapRef {
	if b == nil {
		return nil
	}
	return b.chainBuilder.taps()
}

func (b *videoChain) Taps() []tapRef {
	if b == nil {
		return nil
	}
	return b.chainBuilder.taps()
}

func (b *audioChain) Decode(options ...codec.Option) *audioChain {
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

func (b *audioChain) Do(stages ...pipeline.Stage) *audioChain {
	if b == nil {
		return b
	}
	for i := range stages {
		b.chainBuilder.stage(stages[i])
	}
	return b
}

func (b *audioChain) Shape(shape shape.Spec) *audioChain {
	if b == nil {
		return b
	}
	b.chainBuilder.shape(shape)
	return b
}

// Auto opts chains the flow is applied to into shape solving with the given
// conversion policies — the flow-side twin of the stream chain's .Auto(...):
// needed conversions an active policy allows are inserted from the runtime's
// filter registry as real planned operations; everything else is refused with
// the exact policy to add.
func (b *audioChain) Auto(policies ...shape.Policy) *audioChain {
	if b == nil {
		return b
	}
	b.chainBuilder.auto(policies)
	return b
}

// Require asserts a hard shape constraint at this point of the flow: wherever
// the flow is applied, the stream MUST satisfy the given spec here or the
// build fails with the actual and required shapes and the exact fix.
func (b *audioChain) Require(spec shape.Spec) *audioChain {
	if b == nil {
		return b
	}
	b.chainBuilder.require(spec)
	return b
}

// Prefer biases the shape solver's otherwise-open choices on chains the flow
// is applied to. Soft by definition: a preference that cannot be honored is
// dropped with an Explain diagnostic, never an error.
func (b *audioChain) Prefer(spec shape.Spec) *audioChain {
	if b == nil {
		return b
	}
	b.chainBuilder.prefer(spec)
	return b
}

// Apply splices another flow's operations into this flow at the declaration
// position — the flow-side twin of a chain's .Apply(flow): flows compose
// exactly like chains apply flows. The applied flow must carry the same media
// kind, and its decode/encode terminals obey the same ordering rules as
// directly declared operations. Apply copies the other flow's operation list
// by value at call time, so cycles are unrepresentable — a flow applied to
// itself only splices its operations as declared so far.
func (b *audioChain) Apply(flow chain) *audioChain {
	if b == nil {
		return b
	}
	b.chainBuilder.apply(flow)
	return b
}

func (b *audioChain) Tap(tap tapRef) *audioChain {
	if b == nil {
		return b
	}
	b.chainBuilder.tap(tap)
	return b
}

func (b *audioChain) Encode(codec codec.CodecSpec) *audioChain {
	if b == nil {
		return b
	}
	b.chainBuilder.encode(codec)
	return b
}

func (b *audioChain) Copy() *audioChain {
	return b.Encode(codec.Copy())
}

func (b *audioChain) chainSpec() chainSpec {
	if b == nil {
		return chainSpec{err: nilFlowError()}
	}
	return b.chainBuilder.snapshot()
}

func (b *videoChain) Decode(options ...codec.Option) *videoChain {
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

func (b *videoChain) Do(stages ...pipeline.Stage) *videoChain {
	if b == nil {
		return b
	}
	for i := range stages {
		b.chainBuilder.stage(stages[i])
	}
	return b
}

func (b *videoChain) Shape(shape shape.Spec) *videoChain {
	if b == nil {
		return b
	}
	b.chainBuilder.shape(shape)
	return b
}

// Auto opts chains the flow is applied to into shape solving with the given
// conversion policies — the flow-side twin of the stream chain's .Auto(...):
// needed conversions an active policy allows are inserted from the runtime's
// filter registry as real planned operations; everything else is refused with
// the exact policy to add.
func (b *videoChain) Auto(policies ...shape.Policy) *videoChain {
	if b == nil {
		return b
	}
	b.chainBuilder.auto(policies)
	return b
}

// Require asserts a hard shape constraint at this point of the flow: wherever
// the flow is applied, the stream MUST satisfy the given spec here or the
// build fails with the actual and required shapes and the exact fix.
func (b *videoChain) Require(spec shape.Spec) *videoChain {
	if b == nil {
		return b
	}
	b.chainBuilder.require(spec)
	return b
}

// Prefer biases the shape solver's otherwise-open choices on chains the flow
// is applied to. Soft by definition: a preference that cannot be honored is
// dropped with an Explain diagnostic, never an error.
func (b *videoChain) Prefer(spec shape.Spec) *videoChain {
	if b == nil {
		return b
	}
	b.chainBuilder.prefer(spec)
	return b
}

// Apply splices another flow's operations into this flow at the declaration
// position — the flow-side twin of a chain's .Apply(flow): flows compose
// exactly like chains apply flows. The applied flow must carry the same media
// kind, and its decode/encode terminals obey the same ordering rules as
// directly declared operations. Apply copies the other flow's operation list
// by value at call time, so cycles are unrepresentable — a flow applied to
// itself only splices its operations as declared so far.
func (b *videoChain) Apply(flow chain) *videoChain {
	if b == nil {
		return b
	}
	b.chainBuilder.apply(flow)
	return b
}

func (b *videoChain) Tap(tap tapRef) *videoChain {
	if b == nil {
		return b
	}
	b.chainBuilder.tap(tap)
	return b
}

func (b *videoChain) Encode(codec codec.CodecSpec) *videoChain {
	if b == nil {
		return b
	}
	b.chainBuilder.encode(codec)
	return b
}

func (b *videoChain) Copy() *videoChain {
	return b.Encode(codec.Copy())
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

func (b *chainBuilder) inputShapes() shape.Set {
	if b == nil {
		return nil
	}
	switch {
	case chainHasDecode(b.spec.operations):
		return shape.Set{shape.Packet(b.spec.media, chainDecodeCodec(b.spec.operations).ID)}
	case chainEncodeSpec(b.spec.operations).Copy:
		return shape.Set{shape.Packet(b.spec.media, "")}
	default:
		return shape.Set{shape.Frame(b.spec.media)}
	}
}

func (b *chainBuilder) outputShapes(input shape.Spec) shape.Set {
	if b == nil {
		return nil
	}
	spec := input
	if spec.MediaKind == "" {
		spec.MediaKind = b.spec.media
	}
	if spec.Domain == "" {
		switch {
		case chainHasDecode(b.spec.operations) || chainEncodeSpec(b.spec.operations).Copy:
			spec.Domain = shape.DomainPacket
		default:
			spec.Domain = shape.DomainFrame
		}
	}
	for i := range b.spec.operations {
		shapes := b.spec.operations[i].OutputShapes(spec)
		if len(shapes) != 0 {
			spec = shapes[0]
		}
	}
	return shape.Set{spec}
}

func (b *chainBuilder) taps() []tapRef {
	if b == nil {
		return nil
	}
	out := make([]tapRef, 0, len(b.spec.operations))
	for i := range b.spec.operations {
		operation := b.spec.operations[i]
		if operation.Kind != plan.OpTap || operation.Tap.Name == "" {
			continue
		}
		out = append(out, tapRef{name: operation.Tap.Name, domain: operation.Tap.Domain})
	}
	return out
}

func (b *chainBuilder) decode(options ...codec.Option) {
	if b == nil {
		return
	}
	if codecIntentSet(chainEncodeSpec(b.spec.operations)) {
		b.setErr(chainStepAfterEncodeError("build flow", firstNonEmpty(b.spec.name, "flow"), "decode", chainEncodeSpec(b.spec.operations)))
		return
	}
	if chainHasDecode(b.spec.operations) {
		b.setErr(duplicateFlowDecodeError(firstNonEmpty(b.spec.name, "flow")))
		return
	}
	if operationSpecsContainChainStep(b.spec.operations) {
		b.setErr(flowDecodeOrderError(firstNonEmpty(b.spec.name, "flow")))
		return
	}
	decodeCodec := mergeDecodeCodecSpec(codec.CodecSpec{}, codecSpecFromOptions(options...))
	b.spec.operations = append(b.spec.operations, operationSpecForDecode(decodeCodec, string(decodeCodec.ID)))
}

func (b *chainBuilder) transform(spec transformSpec) {
	if b == nil {
		return
	}
	if codecIntentSet(chainEncodeSpec(b.spec.operations)) {
		b.setErr(chainStepAfterEncodeError("build flow", firstNonEmpty(b.spec.name, "flow"), chainTransformStepName(spec), chainEncodeSpec(b.spec.operations)))
		return
	}
	transform := cloneTransformSpec(spec)
	b.spec.operations = append(b.spec.operations, operationSpecForTransform(transform))
}

func (b *chainBuilder) stage(stage pipeline.Stage) {
	if b == nil {
		return
	}
	if codecIntentSet(chainEncodeSpec(b.spec.operations)) {
		b.setErr(chainStepAfterEncodeError("build flow", firstNonEmpty(b.spec.name, "flow"), "custom stage", chainEncodeSpec(b.spec.operations)))
		return
	}
	if err := validateStageComponent(stage); err != nil {
		b.setErr(streamStageMissingError(streamIntent{Name: firstNonEmpty(b.spec.name, "flow")}))
		return
	}
	b.spec.operations = append(b.spec.operations, operationSpecForStage(stage))
}

func (b *chainBuilder) shape(shape shape.Spec) {
	if b == nil {
		return
	}
	if codecIntentSet(chainEncodeSpec(b.spec.operations)) {
		b.setErr(chainStepAfterEncodeError("build flow", firstNonEmpty(b.spec.name, "flow"), "shape", chainEncodeSpec(b.spec.operations)))
		return
	}
	b.spec.operations = append(b.spec.operations, operationSpecForShape(shape))
}

// auto, require, and prefer append annotation carriers; they lower to no
// runtime node and solve/assert/bias only, so — unlike .Shape(...) — they are
// valid after encode (a post-encode .Require asserts the packet-domain output
// shape).
func (b *chainBuilder) auto(policies []shape.Policy) {
	if b == nil {
		return
	}
	b.spec.operations = append(b.spec.operations, operationSpecForAutoPolicy(policies))
}

func (b *chainBuilder) require(spec shape.Spec) {
	if b == nil {
		return
	}
	b.spec.operations = append(b.spec.operations, operationSpecForRequire(spec))
}

func (b *chainBuilder) prefer(spec shape.Spec) {
	if b == nil {
		return
	}
	b.spec.operations = append(b.spec.operations, operationSpecForPreference(spec))
}

// apply splices another flow's snapshotted operations onto this builder with
// the same ordering rules declaration-side operations obey: nothing splices
// after a terminal encoder, an applied decode must be the flow's first
// operation, and an applied packet copy refuses frame-domain prefixes. The
// applied operations are cloned values, so later mutation of either flow
// cannot reach the other (and self-application cannot recurse).
func (b *chainBuilder) apply(flow chain) {
	if b == nil {
		return
	}
	spec, err := chainSpecFrom(flow)
	if err != nil {
		b.setErr(err)
		return
	}
	node := firstNonEmpty(b.spec.name, "flow")
	if err := validateChainMedia("build flow", node, b.spec.media, spec); err != nil {
		b.setErr(err)
		return
	}
	specSteps := chainStepsFromChainOperations(spec.operations)
	if codecIntentSet(chainEncodeSpec(b.spec.operations)) && (chainHasDecode(spec.operations) || len(specSteps) != 0 || codecIntentSet(chainEncodeSpec(spec.operations))) {
		b.setErr(chainStepAfterEncodeError("build flow", node, "flow", chainEncodeSpec(b.spec.operations)))
		return
	}
	if chainHasDecode(spec.operations) {
		if chainHasDecode(b.spec.operations) {
			b.setErr(duplicateFlowDecodeError(node))
			return
		}
		if operationSpecsContainChainStep(b.spec.operations) {
			b.setErr(flowDecodeOrderError(node))
			return
		}
	}
	if chainEncodeSpec(spec.operations).Copy && (chainHasDecode(b.spec.operations) || operationSpecsContainChainStep(b.spec.operations)) {
		b.setErr(flowCopyDomainError("build flow", firstNonEmpty(spec.name, node)))
		return
	}
	b.spec.operations = append(b.spec.operations, cloneOperationSpecs(spec.operations)...)
}

func (b *chainBuilder) tap(tap tapRef) {
	if b == nil {
		return
	}
	if tap.name == "" {
		b.setErr(&BuildError{
			Family:    errcode.FamilyForCode(errcode.TapInvalid),
			Code:      errcode.TapInvalid,
			Operation: "build flow",
			Node:      firstNonEmpty(b.spec.name, "flow"),
			Reason:    "tap name is empty",
			fixes: buildErrorFixes([]string{
				"call .Tap(goav.FrameTap(\"audio.voice.frames\")) or another stable tap ref",
				"omit .Tap(...) when no runtime branch should attach at that point",
			}),
			cause: errUnsupportedBuild,
		})
		return
	}
	if codecIntentSet(chainEncodeSpec(b.spec.operations)) {
		if err := validateTapDomain("build flow", firstNonEmpty(b.spec.name, "flow"), tap, shape.DomainPacket); err != nil {
			b.setErr(err)
			return
		}
		b.spec.operations = append(b.spec.operations, operationSpecForTap(tap, b.spec.media, operationSpecAfter(b.spec.operations, plan.OpEncode)))
		return
	}
	if err := validateTapDomain("build flow", firstNonEmpty(b.spec.name, "flow"), tap, shape.DomainFrame); err != nil {
		b.setErr(err)
		return
	}
	b.spec.operations = append(b.spec.operations, operationSpecForTap(tap, b.spec.media, operationSpecAfter(b.spec.operations, initialStepAfter(chainHasDecode(b.spec.operations)))))
}

func (b *chainBuilder) encode(codec codec.CodecSpec) {
	if b == nil {
		return
	}
	if codecIntentSet(chainEncodeSpec(b.spec.operations)) {
		b.setErr(duplicateFlowEncodeError(b.spec.name, chainEncodeSpec(b.spec.operations), codec))
		return
	}
	if codec.Copy && (chainHasDecode(b.spec.operations) || operationSpecsContainChainStep(b.spec.operations)) {
		b.setErr(flowCopyDomainError("build flow", firstNonEmpty(b.spec.name, "flow")))
		return
	}
	b.spec.operations = append(b.spec.operations, operationSpecForEncode(cloneCodecSpec(codec)))
}

func (b *chainBuilder) snapshot() chainSpec {
	if b == nil {
		return chainSpec{err: nilFlowError()}
	}
	spec := b.spec
	spec.operations = cloneOperationSpecs(spec.operations)
	return spec
}

func (b *chainBuilder) setErr(err error) {
	if b.spec.err == nil {
		b.spec.err = err
	}
}

func chainSpecFrom(flow chain) (chainSpec, error) {
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

// chainHasDecode / chainDecodeCodec / chainEncodeSpec derive a chain's decode and
// encode facts from its operation list, so chainSpec keeps no parallel decode/
// encode state — the operations are the single source of truth (one operation
// list). The plan.OpDecode operation carries the decode codec; the terminal
// plan.OpEncode/plan.OpCopy carries the encode codec.
func chainHasDecode(operations []operationSpec) bool {
	return operationSpecsContainKind(operations, plan.OpDecode)
}

func chainDecodeCodec(operations []operationSpec) codec.CodecSpec {
	for i := range operations {
		if operations[i].Kind == plan.OpDecode {
			return operations[i].Decode
		}
	}
	return codec.CodecSpec{}
}

func chainEncodeSpec(operations []operationSpec) codec.CodecSpec {
	for i := range operations {
		if operations[i].Kind == plan.OpEncode || operations[i].Kind == plan.OpCopy {
			return operations[i].Encode
		}
	}
	return codec.CodecSpec{}
}

func cloneTransformSpec(spec transformSpec) transformSpec {
	var out transformSpec
	if spec.resize != nil {
		resize := *spec.resize
		out.resize = &resize
	}
	if spec.resample != nil {
		resample := *spec.resample
		out.resample = &resample
	}
	return out
}

func chainTransformStepName(spec transformSpec) string {
	switch {
	case spec.resize != nil:
		return "resize"
	case spec.resample != nil:
		return "resample"
	default:
		return "transform"
	}
}

func duplicateFlowEncodeError(name string, first codec.CodecSpec, second codec.CodecSpec) error {
	return duplicateStreamEncodeError("build flow", firstNonEmpty(name, "flow"), first, second)
}

func duplicateFlowDecodeError(node string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(flowDecodeDuplicateCode),
		Code:      flowDecodeDuplicateCode,
		Operation: "build flow",
		Node:      node,
		Reason:    "flow already decodes its input packets",
		fixes: buildErrorFixes([]string{
			"call .Decode() once at the start of the flow",
			"remove the second .Decode() call",
		}),
		cause: errUnsupportedBuild,
	}
}

func flowDecodeOrderError(node string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(flowDecodeOrderInvalidCode),
		Code:      flowDecodeOrderInvalidCode,
		Operation: "build flow",
		Node:      node,
		Reason:    "decode must be the first flow operation",
		fixes: buildErrorFixes([]string{
			"write goav.Flow(name).Audio().Decode().Resample(...)",
			"omit .Decode() when the flow is only applied after stream decode",
		}),
		cause: errUnsupportedBuild,
	}
}

func flowDecodeDomainError(operation string, node string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(flowDecodeDomainMismatchCode),
		Code:      flowDecodeDomainMismatchCode,
		Operation: operation,
		Node:      firstNonEmpty(node, "flow"),
		Reason:    "flow decoding requires a packet-domain stream point",
		fixes: buildErrorFixes([]string{
			"omit .Decode() when applying the flow after stream decode",
			"use the flow from a packet branch or packet tap when it should own decode",
			"split packet-preserving streams with .Copy().Branches(...) before applying the flow",
		}),
		cause: errUnsupportedBuild,
	}
}

func flowCopyDomainError(operation string, node string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(flowCopyDomainMismatchCode),
		Code:      flowCopyDomainMismatchCode,
		Operation: operation,
		Node:      firstNonEmpty(node, "flow"),
		Reason:    "flow copying requires a packet-domain stream point",
		fixes: buildErrorFixes([]string{
			"start packet-preserving reusable work with goav.Flow(name).Audio().Copy() or goav.Flow(name).Video().Copy()",
			"declare packet taps after copy with .Copy().Tap(goav.PacketTap(name))",
			"use .Decode().Resample(...).Encode(codec.Opus(...)) when the flow should transform frames",
		}),
		cause: errUnsupportedBuild,
	}
}

func nilFlowError() error {
	return &BuildError{
		Family:    errcode.FamilyForCode(flowInvalidCode),
		Code:      flowInvalidCode,
		Operation: "build flow",
		Reason:    "flow is nil",
		fixes: buildErrorFixes([]string{
			"build flows with goav.Flow(name).Audio() or goav.Flow(name).Video()",
		}),
		cause: errUnsupportedBuild,
	}
}

func validateChainMedia(operation string, node string, selected av.MediaType, spec chainSpec) error {
	if selected == "" || spec.media == "" || selected == spec.media {
		return nil
	}
	return &BuildError{
		Family:    errcode.FamilyForCode(flowMediaMismatchCode),
		Code:      flowMediaMismatchCode,
		Operation: operation,
		Node:      firstNonEmpty(spec.name, node, "flow"),
		Reason:    string(spec.media) + " flow cannot be applied to " + string(selected) + " stream",
		fixes: buildErrorFixes([]string{
			"use goav.Flow(name).Audio() with .Audio()",
			"use goav.Flow(name).Video() with .Video()",
		}),
		cause: errUnsupportedBuild,
	}
}

func branchInputCountError(node string, count int) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(inputCountUnsupportedCode),
		Code:      inputCountUnsupportedCode,
		Operation: "build branches",
		Node:      node,
		Reason:    "branches currently compose from one input",
		fields: buildErrorFields([]string{
			fmt.Sprintf("inputs=%d", count),
		}),
		fixes: buildErrorFixes([]string{
			"start branches from goav.From(input).Audio() or goav.From(input).Video() with one input",
			"use the expert graph API when combining several sources manually",
		}),
		cause: errUnsupportedBuild,
	}
}

func branchOutputScopeError(node string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(outputScopeMixedCode),
		Code:      outputScopeMixedCode,
		Operation: "build branches",
		Node:      node,
		Reason:    "branch destinations are declared inside Branch(...).To(...)",
		fixes: buildErrorFixes([]string{
			"route branches with .Branches(goav.Branch(name).To(goav.Write(name, writer)))",
			"use stream .To(goav.Write(...)) or .To(goav.Sink(...)) only for one ordinary stream destination",
		}),
		cause: errUnsupportedBuild,
	}
}
