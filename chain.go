// The per-stream chain builder: stream selection options and the jobStreamBuilder fluent methods.

package goav

import (
	"fmt"

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
		Family:    errcode.FamilyForCode(errcode.StreamSelectorInvalid),
		Code:      errcode.StreamSelectorInvalid,
		Operation: operation,
		Node:      node,
		Reason:    "stream index must be non-negative",
		Fields: buildErrorFields([]string{
			fmt.Sprintf("index=%d", selector.Index),
		}),
		Fixes: buildErrorFixes([]string{
			"use goav.StreamIndex(0) for the first matching stream",
			"use goav.StreamID(...) or goav.StreamName(...) when stream metadata is stable",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func codecIntentSet(spec codec.CodecSpec) bool {
	return spec.ID != "" || spec.Auto || spec.Copy
}

func chainStepAfterEncodeError(operation string, node string, step string, encode codec.CodecSpec) error {
	if encode.Copy && step != "decode" {
		return chainStepOnPacketCopyError(operation, node, step)
	}
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.StreamStepAfterEncode),
		Code:      errcode.StreamStepAfterEncode,
		Operation: operation,
		Node:      node,
		Reason:    "stream processing steps must be declared before the encoder",
		Fields: buildErrorFields([]string{
			"step: " + step,
			"encoder: " + codecIntentName(encode),
		}),
		Fixes: buildErrorFixes([]string{
			"place .Do(...), .Resize(...), or .Resample(...) before .Encode(...)",
			"call .To(...) after the encoder to attach outputs",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

// chainStepOnPacketCopyError is the packet-domain transform refusal: .Copy()
// keeps the stream packet-encoded, so frame operations declared after it have
// no decoded frames to consume. The fix is the domain rule itself: decode
// first, or keep the chain a pure packet copy.
func chainStepOnPacketCopyError(operation string, node string, step string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.OperationShapeMismatch),
		Code:      errcode.OperationShapeMismatch,
		Operation: operation,
		Node:      node,
		Reason:    step + " needs decoded frames, but .Copy() keeps the stream packet-encoded",
		Fields: buildErrorFields([]string{
			"step: " + step,
			"actual_shape=" + shape.New(shape.Domain(shape.DomainPacket)).String(),
			"expected_shape=" + shape.New(shape.Domain(shape.DomainFrame)).String(),
		}),
		Fixes: buildErrorFixes([]string{
			"call .Decode() before .Resize(...), .Resample(...), or .Do(...) — transforms run on decoded frames",
			"remove the processing step to keep a pure packet copy",
			"use .Branches(...) when one input needs both a packet copy and a processed branch",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func chainFrameInputRequiredError(operation string, node string, step string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.OperationShapeMismatch),
		Code:      errcode.OperationShapeMismatch,
		Operation: operation,
		Node:      firstNonEmpty(node, "stream"),
		Reason:    step + " needs decoded frames, but the selected stream is still packet-domain",
		Fields: buildErrorFields([]string{
			"step=" + step,
			"actual_shape=" + shape.New(shape.Domain(shape.DomainPacket)).String(),
			"expected_shape=" + shape.New(shape.Domain(shape.DomainFrame)).String(),
		}),
		Fixes: buildErrorFixes([]string{
			"write .Decode()." + streamStepMethodName(step) + "(...) for decoded-frame processing",
			"keep the stream packet-domain by using .Copy() and removing frame-domain processing",
		}),
		Cause: ErrUnsupportedBuild,
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
		Family:    errcode.FamilyForCode(errcode.OperationShapeMismatch),
		Code:      errcode.OperationShapeMismatch,
		Operation: operation,
		Node:      firstNonEmpty(node, "stream"),
		Reason:    "sink output from a packet stream needs an explicit domain",
		Fields: buildErrorFields([]string{
			"destination=sink",
			"actual_shape=" + shape.New(shape.Domain(shape.DomainPacket)).String(),
		}),
		Fixes: buildErrorFixes([]string{
			"decode frames before the sink: .Decode().To(goav.Sink(...))",
			"preserve packets before the sink: .Copy().To(goav.Sink(...))",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func duplicateStreamEncodeError(operation string, node string, first codec.CodecSpec, second codec.CodecSpec) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.EncodeDuplicate),
		Code:      errcode.EncodeDuplicate,
		Operation: operation,
		Node:      node,
		Reason:    "stream recipes allow one terminal encoder",
		Fields: buildErrorFields([]string{
			"first encoder: " + codecIntentName(first),
			"second encoder: " + codecIntentName(second),
		}),
		Fixes: buildErrorFixes([]string{
			"choose one output codec for the stream chain",
			"use .Branches(...) when one input needs multiple encoded branches",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func codecIntentName(spec codec.CodecSpec) string {
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

type streamOption func(*streamSelectConfig)

type streamSelectConfig struct {
	selector av.StreamSelector
	input    string
}

func newStreamSelectConfig(media av.MediaType, options ...streamOption) streamSelectConfig {
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
	from             TapRef
	decode           bool
	decodeCodec      codec.CodecSpec
	operations       []operationSpec
	sharedOps        []operationSpec
	privateOps       []operationSpec
	destinationNames []string
}

// StreamID narrows a stream selection to the stream with the given id, as
// probed or declared by the input.
func StreamID(id av.StreamID) streamOption {
	return func(config *streamSelectConfig) {
		config.selector.ID = id
	}
}

// StreamName narrows a stream selection to the stream with the given
// container-declared name.
func StreamName(name string) streamOption {
	return func(config *streamSelectConfig) {
		config.selector.Name = name
	}
}

// StreamIndex narrows a stream selection to the stream at the given probe
// index (0-based).
func StreamIndex(index int) streamOption {
	return func(config *streamSelectConfig) {
		config.selector.Index = index
		config.selector.UseIndex = true
	}
}

// InputName narrows a stream selection to the named input of a multi-input
// job: goav.From(camera, mic).Video(goav.InputName("camera")). Names come from
// the input constructors (goav.Source(name, ...), goav.FileInput(name, ...))
// or the goav.Name(...) option.
func InputName(name string) streamOption {
	return func(config *streamSelectConfig) {
		config.input = name
	}
}

type jobStreamBuilder struct {
	job    *Job
	stream *jobStreamBuild
	region *compositeRegion
}

// Region places this arm's frame at (x, y) on the composite canvas. It is
// composite-only — it has no effect on arms outside a Composite.
func (b *jobStreamBuilder) Region(x, y int) *jobStreamBuilder {
	b.region = &compositeRegion{x: x, y: y}
	return b
}

// joinArm lets a source chain stand as one arm of a join (Mix, Composite,
// Select) — the original arm shape, kept compiling unchanged behind the
// sealed JoinArm interface.
func (b *jobStreamBuilder) joinArm() joinArmSpec {
	if b == nil {
		return joinArmSpec{}
	}
	return joinArmSpec{chain: b, region: b.region}
}

func (b *jobStreamBuilder) sourceStartsFrameDomain() bool {
	spec, ok := b.sourceFrameShape()
	return ok && spec.Domain == shape.DomainFrame
}

func (b *jobStreamBuilder) sourceFrameShape() (shape.Spec, bool) {
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

func (b *jobStreamBuilder) ensureFrameSourceShapeOperation() {
	stream := b.current()
	if stream == nil {
		return
	}
	shapeSpec, ok := b.sourceFrameShape()
	if !ok {
		return
	}
	operation := operationSpecForShape(shapeSpec)
	if len(stream.operations) != 0 && stream.operations[0].Kind == plan.OpShape && stream.operations[0].Shape == shapeSpec {
		return
	}
	stream.operations = append([]operationSpec{operation}, stream.operations...)
}

func (b *jobStreamBuilder) requireFrameInput(stream *jobStreamBuild, step string) bool {
	if b.sourceStartsFrameDomain() {
		b.ensureFrameSourceShapeOperation()
		return true
	}
	if chainHasDecode(stream.operations) {
		return true
	}
	b.job.setErr(chainFrameInputRequiredError("build stream", jobStreamName(stream), step))
	return false
}

func (b *jobStreamBuilder) requireFrameTapInput(stream *jobStreamBuild) bool {
	if b.sourceStartsFrameDomain() || chainHasDecode(stream.operations) {
		return true
	}
	b.job.setErr(chainFrameInputRequiredError("build stream", jobStreamName(stream), "tap"))
	return false
}

func frameSourceDecodeError(operation string, node string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.SourceShapeMismatch),
		Code:      errcode.SourceShapeMismatch,
		Operation: operation,
		Node:      node,
		Reason:    "frame-domain custom sources are already decoded frames",
		Fields: buildErrorFields([]string{
			"source_domain=frame",
			"operation=decode",
		}),
		Fixes: buildErrorFixes([]string{
			"remove .Decode() when using goav.Source(..., shape.Frame(...), ...)",
			"use shape.Packet(...) when the custom source pushes encoded packets",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func frameSourceCopyError(operation string, node string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.SourceShapeMismatch),
		Code:      errcode.SourceShapeMismatch,
		Operation: operation,
		Node:      node,
		Reason:    "frame-domain custom sources cannot use packet copy",
		Fields: buildErrorFields([]string{
			"source_domain=frame",
			"operation=copy",
		}),
		Fixes: buildErrorFixes([]string{
			"send frame-domain media to goav.Sink(...)",
			"encode frames before writing to file, URI, or writer destinations",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func (b *jobStreamBuilder) Apply(flow Chain) *jobStreamBuilder {
	spec, err := chainSpecFrom(flow)
	if err != nil {
		b.job.setErr(err)
		return b
	}
	stream := b.current()
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

func (b *jobStreamBuilder) Decode(options ...codec.Option) *jobStreamBuilder {
	stream := b.current()
	if b.sourceStartsFrameDomain() {
		b.job.setErr(frameSourceDecodeError("build stream", jobStreamName(stream)))
		return b
	}
	decodeCodec := mergeDecodeCodecSpec(chainDecodeCodec(stream.operations), codecSpecFromOptions(options...))
	stream.operations = append(stream.operations, operationSpecForDecode(decodeCodec, string(stream.selector.Codec)))
	return b
}

func (b *jobStreamBuilder) Copy() *jobStreamBuilder {
	stream := b.current()
	if b.sourceStartsFrameDomain() {
		b.job.setErr(frameSourceCopyError("build stream", jobStreamName(stream)))
		return b
	}
	stream.operations = append(stream.operations, operationSpecForCopy(codec.Copy()))
	return b
}

func (b *jobStreamBuilder) Tap(tap TapRef) *jobStreamBuilder {
	stream := b.current()
	if tap.name == "" {
		b.job.setErr(&BuildError{
			Family:    errcode.FamilyForCode(errcode.TapInvalid),
			Code:      errcode.TapInvalid,
			Operation: "build stream",
			Node:      jobStreamName(stream),
			Reason:    "tap name is empty",
			Fixes: buildErrorFixes([]string{
				"call .Tap(goav.FrameTap(\"video.decoded\")) or another stable tap ref",
				"omit .Tap(...) when no runtime branch should attach at that point",
			}),
			Cause: ErrUnsupportedBuild,
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

func lastStreamTapRef(stream *jobStreamBuild) TapRef {
	if stream == nil {
		return TapRef{}
	}
	for i := len(stream.operations) - 1; i >= 0; i-- {
		if operationSpecTapIsTerminalPacket(stream.operations[i]) {
			return PacketTap(stream.operations[i].Tap.Name)
		}
	}
	steps := jobStreamChainSteps(stream)
	if chainEncodeSpec(stream.operations).Copy && len(steps) == 0 && stream.selector.Type != "" {
		return PacketTap(defaultPacketTapName(stream.selector.Type, 0))
	}
	for i := len(steps) - 1; i >= 0; i-- {
		if steps[i].tap != "" {
			return tapWithDomain(TapRef{name: steps[i].tap, domain: steps[i].tapDomain}, shape.DomainFrame)
		}
	}
	if len(steps) == 0 && stream.selector.Type != "" && chainHasDecode(stream.operations) {
		return FrameTap(defaultDecodedTapName(stream.selector.Type))
	}
	return TapRef{}
}

func (b *jobStreamBuilder) Do(stages ...pipeline.Stage) *jobStreamBuilder {
	stream := b.current()
	for i := range stages {
		if codecIntentSet(chainEncodeSpec(stream.operations)) {
			b.job.setErr(chainStepAfterEncodeError("build stream", jobStreamName(stream), "custom stage", chainEncodeSpec(stream.operations)))
			return b
		}
		if err := validateStageComponent(stages[i]); err != nil {
			b.job.setErr(streamStageMissingError(streamIntent{Name: jobStreamName(stream)}))
			return b
		}
		if !b.requireFrameInput(stream, "custom stage") {
			return b
		}
		stream.operations = append(stream.operations, operationSpecForStage(stages[i]))
	}
	return b
}

// Sync places this stream chain on a shared media timeline. Reuse one
// flow.SyncPolicy value across audio/video chains or branches to align them.
func (b *jobStreamBuilder) Sync(policy flow.SyncPolicy) *jobStreamBuilder {
	stream := b.current()
	stream.operations = append(stream.operations, operationSpecForSync(policy))
	return b
}

// Auto opts the chain into shape solving with the given conversion policies:
// when a downstream operation pins format facts (an encoder's sample rate, a
// stage contract's geometry) the current media does not satisfy, the planner
// inserts the matching conversion from the runtime's filter registry as a real
// planned operation — visible in Describe and reported as an Explain
// diagnostic. The policy applies to the whole chain; zero policies allow
// nothing, so every needed conversion is refused with the exact policy to add.
func (b *jobStreamBuilder) Auto(policies ...shape.Policy) *jobStreamBuilder {
	stream := b.current()
	stream.operations = append(stream.operations, operationSpecForAutoPolicy(policies))
	return b
}

// Require asserts a hard shape constraint at this point of the chain: the
// stream MUST satisfy the given spec here, or the build fails before any
// resource opens with the actual and required shapes and the exact fix —
// including the .Auto(...) policy to add when a conversion could satisfy it
// (an active policy that covers the conversion inserts it, and the
// requirement holds by construction). The assertion lowers to no runtime node.
func (b *jobStreamBuilder) Require(spec shape.Spec) *jobStreamBuilder {
	stream := b.current()
	stream.operations = append(stream.operations, operationSpecForRequire(spec))
	return b
}

// Prefer biases the shape solver where a choice is genuinely open: a
// conversion-target fact the downstream operation leaves unpinned takes the
// preferred value, and an otherwise-ambiguous adapter selection narrows to the
// adapters whose declared capabilities cover the preference. A preference is
// soft by definition — it never fails the build; one that cannot be honored is
// dropped and surfaced as an Explain diagnostic. It lowers to no runtime node.
func (b *jobStreamBuilder) Prefer(spec shape.Spec) *jobStreamBuilder {
	stream := b.current()
	stream.operations = append(stream.operations, operationSpecForPreference(spec))
	return b
}

func (b *jobStreamBuilder) Shape(shape shape.Spec) *jobStreamBuilder {
	stream := b.current()
	if codecIntentSet(chainEncodeSpec(stream.operations)) {
		b.job.setErr(chainStepAfterEncodeError("build stream", jobStreamName(stream), "shape", chainEncodeSpec(stream.operations)))
		return b
	}
	stream.operations = append(stream.operations, operationSpecForShape(shape))
	return b
}

func (b *jobStreamBuilder) Resize(width int, height int, options ...resizeOption) *jobStreamBuilder {
	stream := b.current()
	if codecIntentSet(chainEncodeSpec(stream.operations)) {
		b.job.setErr(chainStepAfterEncodeError("build stream", jobStreamName(stream), "resize", chainEncodeSpec(stream.operations)))
		return b
	}
	if !b.requireFrameInput(stream, "resize") {
		return b
	}
	transform := Resize(width, height, options...)
	stream.operations = append(stream.operations, operationSpecForTransform(transform))
	return b
}

func (b *jobStreamBuilder) Resample(sampleRate int, channels int, options ...audioOption) *jobStreamBuilder {
	stream := b.current()
	if codecIntentSet(chainEncodeSpec(stream.operations)) {
		b.job.setErr(chainStepAfterEncodeError("build stream", jobStreamName(stream), "resample", chainEncodeSpec(stream.operations)))
		return b
	}
	if !b.requireFrameInput(stream, "resample") {
		return b
	}
	transform := Resample(sampleRate, channels, options...)
	stream.operations = append(stream.operations, operationSpecForTransform(transform))
	return b
}

func (b *jobStreamBuilder) Encode(codec codec.CodecSpec) *jobStreamBuilder {
	stream := b.current()
	if codecIntentSet(chainEncodeSpec(stream.operations)) {
		b.job.setErr(duplicateStreamEncodeError("build stream", jobStreamName(stream), chainEncodeSpec(stream.operations), codec))
		return b
	}
	if codec.Copy {
		if b.sourceStartsFrameDomain() {
			b.job.setErr(frameSourceCopyError("build stream", jobStreamName(stream)))
			return b
		}
		if chainHasDecode(stream.operations) || operationSpecsContainChainStep(stream.operations) {
			b.job.setErr(flowCopyDomainError("build stream", jobStreamName(stream)))
			return b
		}
	} else if !b.requireFrameInput(stream, "encode") {
		return b
	}
	stream.operations = append(stream.operations, operationSpecForEncode(cloneCodecSpec(codec)))
	return b
}

func (b *jobStreamBuilder) OnCodecChange(policy CodecChangePolicy) *jobStreamBuilder {
	stream := b.current()
	stream.codecChange = policy
	return b
}

func (b *jobStreamBuilder) To(destinations ...Destination) *Job {
	stream := b.current()
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
		if b.sourceStartsFrameDomain() {
			b.ensureFrameSourceShapeOperation()
		} else if !chainHasDecode(stream.operations) {
			b.job.setErr(sinkDomainRequiredError("build stream", jobStreamName(stream)))
			return b.job
		}
	}
	return b.job
}

func (b *jobStreamBuilder) current() *jobStreamBuild {
	if b.stream != nil {
		return b.stream
	}
	b.stream = &jobStreamBuild{}
	if b.job.currentStream() == nil {
		b.job.streams = append(b.job.streams, b.stream)
	}
	return b.stream
}
