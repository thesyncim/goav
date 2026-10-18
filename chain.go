// The per-stream chain builder: stream selection options and the Chain fluent methods.

package goav

import (
	"fmt"
	"slices"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

func validateRecipeStreamSelector(operation string, node string, selector av.StreamSelector) error {
	if selector.Index >= 0 {
		return nil
	}
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(streamSelectorInvalidCode),
		Code:      streamSelectorInvalidCode,
		Operation: operation,
		Node:      node,
		Reason:    "stream index must be non-negative",
		fields:    errDetails(errDetail("index", fmt.Sprintf("%d", selector.Index))),
		fixes: buildErrorFixes([]string{
			"use goav.StreamIndex(0) for the first matching stream",
			"use goav.StreamID(...) when stream metadata is stable",
		}),
		cause: errUnsupportedBuild,
	}
}

func codecIntentSet(spec codec.Spec) bool {
	return spec.ID != "" || spec.Auto || spec.Copy
}

// chainEndedError refuses an operation on a chain that already ended at
// .To(...): the step would land after the destinations. It is how reusing one
// chain value as a fork shows up, so the fixes name the two real forks.
func chainEndedError(node string) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(streamStepAfterDestinationCode),
		Code:      streamStepAfterDestinationCode,
		Operation: "build stream",
		Node:      node,
		Reason:    "this chain already ended at .To(...); operations cannot follow its destinations",
		fixes: buildErrorFixes([]string{
			"fan one stream out with .Branches(goav.Branch(name)...To(dst), ...) instead of reusing a chain value",
			"start another stream from the job with .Audio() or .Video() for separate work",
		}),
		cause: errUnsupportedBuild,
	}
}

func chainStepAfterEncodeError(operation string, node string, step string, encode codec.Spec) error {
	if encode.Copy && step != "decode" {
		return chainStepOnPacketCopyError(operation, node, step)
	}
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(streamStepAfterEncodeCode),
		Code:      streamStepAfterEncodeCode,
		Operation: operation,
		Node:      node,
		Reason:    "stream processing steps must be declared before the encoder",
		fields:    errDetails(errNote("step: "+step), errNote("encoder: "+codecIntentName(encode))),
		fixes: buildErrorFixes([]string{
			"place .Do(...), .Resize(...), or .Resample(...) before .Encode(...)",
			"call .To(...) after the encoder to attach outputs",
		}),
		cause: errUnsupportedBuild,
	}
}

// chainStepOnPacketCopyError is the packet-domain transform refusal: .Copy()
// keeps the stream packet-encoded, so frame operations declared after it have
// no decoded frames to consume. The fix is the domain rule itself: decode
// first, or keep the chain a pure packet copy.
func chainStepOnPacketCopyError(operation string, node string, step string) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(operationShapeMismatchCode),
		Code:      operationShapeMismatchCode,
		Operation: operation,
		Node:      node,
		Reason:    step + " needs decoded frames, but .Copy() keeps the stream packet-encoded",
		fields:    errDetails(errNote("step: "+step), errDetail("actual_shape", shape.New(shape.Domain(shape.DomainPacket)).String()), errDetail("expected_shape", shape.New(shape.Domain(shape.DomainFrame)).String())),
		fixes: buildErrorFixes([]string{
			"call .Decode() before .Resize(...), .Resample(...), or .Do(...) — transforms run on decoded frames",
			"remove the processing step to keep a pure packet copy",
			"use .Branches(...) when one input needs both a packet copy and a processed branch",
		}),
		cause: errUnsupportedBuild,
	}
}

func chainFrameInputRequiredError(operation string, node string, step string) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(operationShapeMismatchCode),
		Code:      operationShapeMismatchCode,
		Operation: operation,
		Node:      firstNonEmpty(node, "stream"),
		Reason:    step + " needs decoded frames, but the selected stream is still packet-domain",
		fields:    errDetails(errDetail("step", step), errDetail("actual_shape", shape.New(shape.Domain(shape.DomainPacket)).String()), errDetail("expected_shape", shape.New(shape.Domain(shape.DomainFrame)).String())),
		fixes: buildErrorFixes([]string{
			"write .Decode()." + streamStepMethodName(step) + "(...) for decoded-frame processing",
			"keep the stream packet-domain by using .Copy() and removing frame-domain processing",
		}),
		cause: errUnsupportedBuild,
	}
}

func streamStepMethodName(step string) string {
	switch step {
	case "custom stage":
		return "Do"
	case "flow":
		return "Apply"
	case "encode":
		return "Encode"
	default:
		if step == "" {
			return "Decode"
		}
		return string(step[0]-('a'-'A')) + step[1:]
	}
}

func sinkDomainRequiredError(operation string, node string) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(operationShapeMismatchCode),
		Code:      operationShapeMismatchCode,
		Operation: operation,
		Node:      firstNonEmpty(node, "stream"),
		Reason:    "sink output from a packet stream needs an explicit domain",
		fields:    errDetails(errDetail("destination", "sink"), errDetail("actual_shape", shape.New(shape.Domain(shape.DomainPacket)).String())),
		fixes: buildErrorFixes([]string{
			"decode frames before the sink: .Decode().To(goav.Sink(...))",
			"preserve packets before the sink: .Copy().To(goav.Sink(...))",
		}),
		cause: errUnsupportedBuild,
	}
}

func duplicateStreamEncodeError(operation string, node string, first codec.Spec, second codec.Spec) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(encodeDuplicateCode),
		Code:      encodeDuplicateCode,
		Operation: operation,
		Node:      node,
		Reason:    "stream recipes allow one terminal encoder",
		fields:    errDetails(errNote("first encoder: "+codecIntentName(first)), errNote("second encoder: "+codecIntentName(second))),
		fixes: buildErrorFixes([]string{
			"choose one output codec for the stream chain",
			"use .Branches(...) when one input needs multiple encoded branches",
		}),
		cause: errUnsupportedBuild,
	}
}

func copyEncodeSpellingError(operation string, node string) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(encodeParameterInvalidCode),
		Code:      encodeParameterInvalidCode,
		Operation: operation,
		Node:      firstNonEmpty(node, "stream"),
		Reason:    "codec.Copy() is the internal packet-copy codec; recipe chains use .Copy()",
		fixes: buildErrorFixes([]string{
			"replace .Encode(codec.Copy()) with .Copy()",
			"use " + encodeHint("") + " for real encoders",
		}),
		cause: errUnsupportedBuild,
	}
}

func codecIntentName(spec codec.Spec) string {
	switch {
	case spec.Auto:
		return "auto"
	case spec.Copy:
		return "copy"
	case spec.ID != "":
		return string(spec.ID)
	default:
		return "none"
	}
}

// StreamOption narrows the stream selection made by the chain selectors
// (.Audio, .Video, ...); construct values with StreamID, StreamIndex, or
// InputName. It is sealed: the config it acts on is unexported, so those
// constructors are the only way to build one.
type StreamOption func(*streamSelectConfig)

type streamSelectConfig struct {
	selector av.StreamSelector
	input    string
}

func newStreamSelectConfig(media av.MediaType, options ...StreamOption) streamSelectConfig {
	config := streamSelectConfig{selector: av.StreamSelector{Type: media}}
	for i := range options {
		if options[i] != nil {
			options[i](&config)
		}
	}
	return config
}

type streamBuild struct {
	name             string
	selector         av.StreamSelector
	from             tapRef
	decode           bool
	decodeCodec      codec.Spec
	operations       []operationSpec
	sharedOps        []operationSpec
	privateOps       []operationSpec
	destinationNames []string
	buffer           flow.BranchBuffer
}

// StreamID narrows a stream selection to the stream with the given id, as
// probed or declared by the input.
func StreamID(id av.StreamID) StreamOption {
	return func(config *streamSelectConfig) {
		config.selector.ID = id
	}
}

// StreamIndex narrows a stream selection to the stream at the given probe
// index (0-based).
func StreamIndex(index int) StreamOption {
	return func(config *streamSelectConfig) {
		config.selector.Index = index
		config.selector.UseIndex = true
	}
}

// InputName narrows a stream selection to the named input of a multi-input
// job: goav.From(camera, mic).Video(goav.InputName("camera")). Names come from
// the input constructors (goav.Source(name, ...), goav.FileInput(name, ...))
// or the goav.Name(...) option.
func InputName(name string) StreamOption {
	return func(config *streamSelectConfig) {
		config.input = name
	}
}

// Chain is one stream's operation list under construction: Audio() or Video()
// on a job starts it, operations extend it in order, and To(...) or
// Branches(...) ends it. Helpers can take and return *Chain to package reusable
// steps. Methods extend the chain in place and return it for chaining; a chain
// that already ended at To(...) refuses further operations, so fork one
// stream with Branches(...) rather than by reusing a chain value.
type Chain struct {
	job    *Job
	stream *jobStreamBuild
	region *compositeRegion
}

// Region places this arm's frame at (x, y) on the composite canvas. It is
// composite-only: a chain that ends in To or Branches, or a Mix arm, refuses it.
func (b *Chain) Region(x, y int) *Chain {
	b.region = &compositeRegion{x: x, y: y}
	return b
}

// joinArm lets a source chain stand as one arm of a join (Mix, Composite,
// Select) — the original arm shape, kept compiling unchanged behind the
// sealed JoinArm interface.
func (b *Chain) joinArm() joinArmSpec {
	if b == nil {
		return joinArmSpec{}
	}
	spec := joinArmSpec{region: b.region}
	if b.job != nil {
		spec.chainErr = b.job.err
		if len(b.job.inputs) == 1 {
			spec.chainInput = b.job.inputs[0]
			spec.chainInputOK = true
		}
	}
	if b.stream != nil {
		spec.chainOperations = cloneOperationSpecs(b.stream.operations)
	}
	return spec
}

func (b *Chain) sourceStartsFrameDomain() bool {
	spec, ok := b.sourceFrameShape()
	return ok && spec.Domain == shape.DomainFrame
}

func (b *Chain) sourceFrameShape() (shape.Spec, bool) {
	if b == nil || b.job == nil || len(b.job.inputs) == 0 {
		return shape.Spec{}, false
	}
	if len(b.job.inputs) == 1 {
		spec, ok := declaredSourceShape(b.job.inputs[0])
		if !ok || spec.Domain != shape.DomainFrame {
			return shape.Spec{}, false
		}
		return spec, true
	}
	// Multi-input: resolve which input this chain selects (declared custom
	// source/live streams plus any goav.InputName narrowing) so frame-domain
	// sources keep their no-decode contract per chain.
	stream := b.current()
	sets := inputSpecStreamSets(b.job.inputs)
	index, ok := resolveInputSetIndex(sets, stream.selector, stream.input)
	if !ok {
		return shape.Spec{}, false
	}
	spec, ok := declaredSourceShape(b.job.inputs[index])
	if !ok || spec.Domain != shape.DomainFrame {
		return shape.Spec{}, false
	}
	return spec, true
}

// stageRequiresFrameInput reports whether a custom .Do(stage) needs decoded
// frames, so a packet-domain chain must .Decode() first. Per the operation
// contract only frame stages do: a PacketFunc consumes packets and an EventFunc
// consumes events directly. Stages that declare FrameOnlyInput (the component
// func stages) answer for themselves; any other custom stage keeps the
// conservative default of requiring frames, since its domain is unknown here
// and the shape solver still validates it.
func stageRequiresFrameInput(stage pipeline.Stage) bool {
	if domained, ok := stage.(interface{ FrameOnlyInput() bool }); ok {
		return domained.FrameOnlyInput()
	}
	return true
}

func (b *Chain) requireFrameInput(stream *jobStreamBuild, step string) bool {
	if b.sourceStartsFrameDomain() {
		return true
	}
	if chainHasDecode(stream.operations) {
		return true
	}
	b.job.setErr(chainFrameInputRequiredError("build stream", jobStreamName(stream), step))
	return false
}

func (b *Chain) requireFrameTapInput(stream *jobStreamBuild) bool {
	if b.sourceStartsFrameDomain() || chainHasDecode(stream.operations) {
		return true
	}
	b.job.setErr(chainFrameInputRequiredError("build stream", jobStreamName(stream), "tap"))
	return false
}

func frameSourceDecodeError(operation string, node string) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(sourceShapeMismatchCode),
		Code:      sourceShapeMismatchCode,
		Operation: operation,
		Node:      node,
		Reason:    "frame-domain custom sources are already decoded frames",
		fields:    errDetails(errDetail("source_domain", "frame"), errDetail("operation", "decode")),
		fixes: buildErrorFixes([]string{
			"remove .Decode() when using goav.Source(..., shape.Frame(...), ...)",
			"use shape.Packet(...) when the custom source pushes encoded packets",
		}),
		cause: errUnsupportedBuild,
	}
}

func frameSourceCopyError(operation string, node string) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(sourceShapeMismatchCode),
		Code:      sourceShapeMismatchCode,
		Operation: operation,
		Node:      node,
		Reason:    "frame-domain custom sources cannot use packet copy",
		fields:    errDetails(errDetail("source_domain", "frame"), errDetail("operation", "copy")),
		fixes: buildErrorFixes([]string{
			"send frame-domain media to goav.Sink(...)",
			"encode frames before writing to file, URI, or writer destinations",
		}),
		cause: errUnsupportedBuild,
	}
}

func (b *Chain) Apply(flow chain) *Chain {
	spec, err := chainSpecFrom(flow)
	if err != nil {
		b.job.setErr(err)
		return b
	}
	stream := b.step()
	if err := validateChainMedia("build stream", jobStreamName(stream), stream.selector.Type, spec); err != nil {
		b.job.setErr(err)
		return b
	}
	specSteps := chainStepsFromChainOperations(spec.operations)
	if codecIntentSet(chainEncodeSpec(stream.operations)) && (chainHasDecode(spec.operations) || len(specSteps) != 0 || codecIntentSet(chainEncodeSpec(spec.operations))) {
		b.job.setErr(chainStepAfterEncodeError("build stream", jobStreamName(stream), "flow", chainEncodeSpec(stream.operations)))
		return b
	}
	if chainHasDecode(spec.operations) {
		if b.sourceStartsFrameDomain() {
			b.job.setErr(frameSourceDecodeError("build stream", jobStreamName(stream)))
			return b
		}
		if chainHasDecode(stream.operations) || operationSpecsContainChainStep(stream.operations) {
			b.job.setErr(flowDecodeDomainError("build stream", firstNonEmpty(spec.name, jobStreamName(stream), "flow")))
			return b
		}
		// The flow's plan.OpDecode is appended below with the rest of spec.operations.
	}
	if len(specSteps) != 0 && !chainHasDecode(spec.operations) {
		if !b.requireFrameInput(stream, "flow") {
			return b
		}
	}
	if codecIntentSet(chainEncodeSpec(spec.operations)) && !chainEncodeSpec(spec.operations).Copy && !chainHasDecode(spec.operations) {
		if !b.requireFrameInput(stream, "flow") {
			return b
		}
	}
	stream.operations = append(stream.operations, cloneOperationSpecs(spec.operations)...)
	if codecIntentSet(chainEncodeSpec(spec.operations)) && chainEncodeSpec(spec.operations).Copy {
		if b.sourceStartsFrameDomain() {
			b.job.setErr(frameSourceCopyError("build stream", jobStreamName(stream)))
			return b
		}
		if chainHasDecode(stream.operations) || operationSpecsContainChainStep(stream.operations) {
			b.job.setErr(flowCopyDomainError("build stream", firstNonEmpty(spec.name, jobStreamName(stream), "flow")))
			return b
		}
	}
	return b
}

func (b *Chain) Decode(options ...codec.Option) *Chain {
	stream := b.step()
	if b.sourceStartsFrameDomain() {
		b.job.setErr(frameSourceDecodeError("build stream", jobStreamName(stream)))
		return b
	}
	decodeCodec := mergeDecodeCodecSpec(chainDecodeCodec(stream.operations), codecSpecFromOptions(options...))
	stream.operations = append(stream.operations, operationSpecForDecode(decodeCodec, string(stream.selector.Codec)))
	return b
}

func (b *Chain) Copy() *Chain {
	stream := b.step()
	if b.sourceStartsFrameDomain() {
		b.job.setErr(frameSourceCopyError("build stream", jobStreamName(stream)))
		return b
	}
	stream.operations = append(stream.operations, operationSpecForCopy(codec.Copy()))
	return b
}

func (b *Chain) Tap(tap tapRef) *Chain {
	stream := b.step()
	if tap.name == "" {
		b.job.setErr(&BuildError{
			Phase:     phaseBuild,
			Family:    errcode.FamilyForCode(errcode.TapInvalid),
			Code:      errcode.TapInvalid,
			Operation: "build stream",
			Node:      jobStreamName(stream),
			Reason:    "tap name is empty",
			fixes: buildErrorFixes([]string{
				"call .Tap(goav.FrameTap(\"video.decoded\")) or another stable tap ref",
				"omit .Tap(...) when no runtime branch should attach at that point",
			}),
			cause: errUnsupportedBuild,
		})
		return b
	}
	if codecIntentSet(chainEncodeSpec(stream.operations)) {
		if err := validateTapDomain("build stream", jobStreamName(stream), tap, shape.DomainPacket); err != nil {
			b.job.setErr(err)
			return b
		}
		stream.operations = append(stream.operations, operationSpecForTap(tap, stream.selector.Type, operationSpecAfter(stream.operations, plan.OpEncode)))
		return b
	}
	if err := validateTapDomain("build stream", jobStreamName(stream), tap, shape.DomainFrame); err != nil {
		b.job.setErr(err)
		return b
	}
	if !b.requireFrameTapInput(stream) {
		return b
	}
	stream.operations = append(stream.operations, operationSpecForTap(tap, stream.selector.Type, operationSpecAfter(stream.operations, initialStepAfter(chainHasDecode(stream.operations)))))
	return b
}

func streamSelectFromAV(selector av.StreamSelector) plan.StreamSelect {
	return plan.StreamSelect{
		ID:       selector.ID,
		Index:    selector.Index,
		UseIndex: selector.UseIndex,
		Type:     selector.Type,
		Codec:    selector.Codec,
		Name:     selector.Name,
	}
}

func lastStreamTapRef(stream *jobStreamBuild) tapRef {
	if stream == nil {
		return tapRef{}
	}
	for _, v := range slices.Backward(stream.operations) {
		if operationSpecTapIsTerminalPacket(v) {
			return PacketTap(v.Tap.Name)
		}
	}
	steps := jobStreamChainSteps(stream)
	if chainEncodeSpec(stream.operations).Copy && len(steps) == 0 && stream.selector.Type != "" {
		return PacketTap(defaultPacketTapName(stream.selector.Type, 0))
	}
	for _, step := range slices.Backward(steps) {
		if step.tap != "" {
			return tapWithDomain(tapRef{name: step.tap, domain: step.tapDomain}, shape.DomainFrame)
		}
	}
	if len(steps) == 0 && stream.selector.Type != "" && chainHasDecode(stream.operations) {
		return FrameTap(defaultDecodedTapName(stream.selector.Type))
	}
	return tapRef{}
}

func (b *Chain) Do(stages ...pipeline.Stage) *Chain {
	stream := b.step()
	for i := range stages {
		// After .Encode or .Copy the stream is packets: a stage that consumes
		// packets (a bitstream filter, SEI injection, encryption) runs there;
		// a frame stage cannot.
		if codecIntentSet(chainEncodeSpec(stream.operations)) && stageRequiresFrameInput(stages[i]) {
			b.job.setErr(chainStepAfterEncodeError("build stream", jobStreamName(stream), "custom stage", chainEncodeSpec(stream.operations)))
			return b
		}
		if err := validateStageComponent(stages[i]); err != nil {
			b.job.setErr(streamStageMissingError(streamIntent{Name: jobStreamName(stream)}, err))
			return b
		}
		if stageRequiresFrameInput(stages[i]) && !b.requireFrameInput(stream, "custom stage") {
			return b
		}
		stream.operations = append(stream.operations, operationSpecForStage(stages[i]))
	}
	return b
}

// Sync places this stream chain on a shared media timeline. Reuse one
// flow.SyncPolicy value across audio/video chains or branches to align them.
func (b *Chain) Sync(policy flow.SyncPolicy) *Chain {
	stream := b.step()
	stream.operations = append(stream.operations, operationSpecForSync(policy))
	return b
}

// Playout holds this stream chain's messages at the sink boundary until each
// message's media time is due on the task timeline, so real-time destinations
// render on time. Reuse one flow.PlayoutPolicy value across audio/video chains
// or branches to align their delivery on the shared anchor.
func (b *Chain) Playout(policy flow.PlayoutPolicy) *Chain {
	stream := b.step()
	stream.operations = append(stream.operations, operationSpecForPlayout(policy))
	return b
}

// Auto opts the chain into shape solving with the given conversion policies:
// when a downstream operation pins format facts (an encoder's sample rate, a
// stage contract's geometry) the current media does not satisfy, the planner
// inserts the matching conversion from the runtime's filter registry as a real
// planned operation — visible in Describe and reported as an Explain
// diagnostic. The policy applies to the whole chain; zero policies allow
// nothing, so every needed conversion is refused with the exact policy to add.
func (b *Chain) Auto(policies ...shape.Policy) *Chain {
	stream := b.step()
	stream.operations = append(stream.operations, operationSpecForAutoPolicy(policies))
	return b
}

// Require asserts a hard shape constraint at this point of the chain: the
// stream MUST satisfy the given spec here, or the build fails before any
// resource opens with the actual and required shapes and the exact fix —
// including the .Auto(...) policy to add when a conversion could satisfy it
// (an active policy that covers the conversion inserts it, and the
// requirement holds by construction). The assertion lowers to no runtime node.
func (b *Chain) Require(spec shape.Spec) *Chain {
	stream := b.step()
	stream.operations = append(stream.operations, operationSpecForRequire(spec))
	return b
}

// Prefer biases the shape solver where a choice is genuinely open: a
// conversion-target fact the downstream operation leaves unpinned takes the
// preferred value, and an otherwise-ambiguous adapter selection narrows to the
// adapters whose declared capabilities cover the preference. A preference is
// soft by definition — it never fails the build; one that cannot be honored is
// dropped and surfaced as an Explain diagnostic. It lowers to no runtime node.
func (b *Chain) Prefer(spec shape.Spec) *Chain {
	stream := b.step()
	stream.operations = append(stream.operations, operationSpecForPreference(spec))
	return b
}

func (b *Chain) Shape(shape shape.Spec) *Chain {
	stream := b.step()
	if codecIntentSet(chainEncodeSpec(stream.operations)) {
		b.job.setErr(chainStepAfterEncodeError("build stream", jobStreamName(stream), "shape", chainEncodeSpec(stream.operations)))
		return b
	}
	stream.operations = append(stream.operations, operationSpecForShape(shape))
	return b
}

func (b *Chain) Resize(width int, height int, options ...resizeOption) *Chain {
	stream := b.step()
	if codecIntentSet(chainEncodeSpec(stream.operations)) {
		b.job.setErr(chainStepAfterEncodeError("build stream", jobStreamName(stream), "resize", chainEncodeSpec(stream.operations)))
		return b
	}
	if !b.requireFrameInput(stream, "resize") {
		return b
	}
	transform := resizeSpec(width, height, options...)
	stream.operations = append(stream.operations, operationSpecForTransform(transform))
	return b
}

func (b *Chain) Resample(sampleRate int, channels int, options ...audioOption) *Chain {
	stream := b.step()
	if codecIntentSet(chainEncodeSpec(stream.operations)) {
		b.job.setErr(chainStepAfterEncodeError("build stream", jobStreamName(stream), "resample", chainEncodeSpec(stream.operations)))
		return b
	}
	if !b.requireFrameInput(stream, "resample") {
		return b
	}
	transform := resampleSpec(sampleRate, channels, options...)
	stream.operations = append(stream.operations, operationSpecForTransform(transform))
	return b
}

func (b *Chain) Encode(codec codec.Spec) *Chain {
	stream := b.step()
	if codecIntentSet(chainEncodeSpec(stream.operations)) {
		b.job.setErr(duplicateStreamEncodeError("build stream", jobStreamName(stream), chainEncodeSpec(stream.operations), codec))
		return b
	}
	if codec.Copy {
		b.job.setErr(copyEncodeSpellingError("build stream", jobStreamName(stream)))
		return b
	} else if !b.requireFrameInput(stream, "encode") {
		return b
	}
	stream.operations = append(stream.operations, operationSpecForEncode(cloneCodecSpec(codec)))
	return b
}

func (b *Chain) To(destinations ...Destination) *Job {
	stream := b.current()
	if b.region != nil {
		b.job.setErr(regionOutsideCompositeError(jobStreamName(stream)))
		return b.job
	}
	outputs := make([]destinationSpec, 0, len(destinations))
	for i := range destinations {
		output, err := destinationSpecFromDestination(destinations[i])
		if err != nil {
			b.job.setErr(streamDestinationInvalidError(jobStreamName(stream), err.Error()))
			return b.job
		}
		if err := b.job.checkSharedStreamDestination(stream, output, ""); err != nil {
			b.job.setErr(err)
			return b.job
		}
		stream.outputs = append(stream.outputs, output)
		stream.outputNames = append(stream.outputNames, "")
		outputs = append(outputs, output)
	}
	if outputsContainSinkDestination(outputs) && !codecIntentSet(chainEncodeSpec(stream.operations)) {
		if !b.sourceStartsFrameDomain() && !chainHasDecode(stream.operations) {
			b.job.setErr(sinkDomainRequiredError("build stream", jobStreamName(stream)))
			return b.job
		}
	}
	return b.job
}

// step returns the stream an operation extends. A chain that already ended at
// .To(...) cannot grow: the operation is refused, and it lands on a detached
// scratch stream so the job's real stream stays intact.
func (b *Chain) step() *jobStreamBuild {
	stream := b.current()
	if len(stream.outputs) == 0 {
		return stream
	}
	b.job.setErr(chainEndedError(jobStreamName(stream)))
	return &jobStreamBuild{selector: stream.selector, name: stream.name}
}

func (b *Chain) current() *jobStreamBuild {
	if b.stream != nil {
		return b.stream
	}
	b.stream = &jobStreamBuild{}
	if b.job.currentStream() == nil {
		b.job.streams = append(b.job.streams, b.stream)
	}
	return b.stream
}

// regionOutsideCompositeError refuses a Region on a stream no Composite
// places: the position would otherwise be dropped without a word.
func regionOutsideCompositeError(node string) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(streamRegionOutsideCompositeCode),
		Code:      streamRegionOutsideCompositeCode,
		Operation: "build stream",
		Node:      node,
		Reason:    ".Region(x, y) positions a Composite arm; this stream is not one",
		fixes: buildErrorFixes([]string{
			"pass the chain to goav.Composite(base, chain.Region(x, y)) to place it on a canvas",
			"remove .Region(...) from a chain that is not a Composite arm",
		}),
		cause: errUnsupportedBuild,
	}
}

// refuseTopLevelRegion fails a join job whose own Region no outer Composite
// reads: only a join standing as a Composite arm has a canvas position.
func refuseTopLevelRegion(job *Job, region *compositeRegion, node string) *Job {
	if region != nil {
		job.setErr(regionOutsideCompositeError(node))
	}
	return job
}
