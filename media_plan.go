package goav

import (
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
)

type OperationKind string

const (
	OpProbe       OperationKind = "probe"
	OpDemux       OperationKind = "demux"
	OpDepacketize OperationKind = "depacketize"
	OpSelect      OperationKind = "select"
	OpDecode      OperationKind = "decode"
	OpShape       OperationKind = "shape"
	OpTransform   OperationKind = "transform"
	OpStage       OperationKind = "stage"
	OpEncode      OperationKind = "encode"
	OpCopy        OperationKind = "copy"
	OpMux         OperationKind = "mux"
	OpWrite       OperationKind = "write"
	OpSink        OperationKind = "sink"
	OpTap         OperationKind = "tap"
)

type mediaPlan struct {
	Name        string
	Inputs      []planInput
	Streams     []planStream
	Taps        []planTap
	Branches    []planBranch
	Outputs     []planOutput
	Decisions   []planDecision
	Diagnostics []PlanDiagnostic
}

type planTap struct {
	Name      string
	Node      pipeline.NodeRef
	Domain    MediaDomain
	MediaKind av.MediaType
	After     OperationKind
	Shape     MediaShape
	Shared    bool
}

type planInput struct {
	Name     string
	Protocol av.ProtocolID
	Realtime bool
	Codec    av.CodecID
}

type planStream struct {
	Name   string
	Select StreamSelect
}

type planBranch struct {
	Name       string
	Input      string
	Stream     StreamSelect
	Shape      MediaShape
	Operations []planOperation
	Outputs    []string
}

type planOperation struct {
	Kind      OperationKind
	Component string
	Detail    string
	After     OperationKind
	Shape     MediaShape
	Shared    bool
}

type planOutput struct {
	Name       string
	Operation  OperationKind
	Component  string
	Format     av.FormatID
	BranchRefs []string
}

type planDecision struct {
	Code    string
	Branch  string
	Message string
}

func buildMediaPlan(state *recipeCompileState) mediaPlan {
	intent := state.intent
	plan := mediaPlan{
		Name: firstNonEmpty(intent.Name, state.operation, "job"),
	}
	plan.Inputs = planInputs(intent.Inputs)
	plan.Streams = planStreams(intent.Streams)
	plan.Outputs = planOutputs(intent.Targets, state.outputFormatMap())
	if state.branchCompositionPresent && branchComposePlanReady(state.plan) && branchComposePlanHasOperations(state.plan) {
		plan.Streams = planStreamsFromBranchComposePlan(state.plan)
		plan.Branches, plan.Decisions = planBranchesFromBranchComposePlan(state, plan.Outputs)
	} else {
		plan.Branches, plan.Decisions = planBranches(state, plan.Outputs)
	}
	plan.Taps = planTaps(plan.Branches)
	plan.Outputs = planOutputsWithBranches(plan.Outputs, plan.Branches)
	return plan
}

func planInputs(inputs []InputIntent) []planInput {
	out := make([]planInput, 0, len(inputs))
	for i := range inputs {
		input := inputs[i]
		out = append(out, planInput{
			Name:     firstNonEmpty(input.Name, input.URI, fmt.Sprintf("input-%d", i)),
			Protocol: input.Protocol,
			Realtime: input.Realtime,
			Codec:    input.Codec.ID,
		})
	}
	return out
}

func planStreams(streams []StreamIntent) []planStream {
	out := make([]planStream, 0, len(streams))
	for i := range streams {
		stream := streams[i]
		out = append(out, planStream{
			Name:   firstNonEmpty(stream.Name, string(stream.Select.Type), fmt.Sprintf("stream-%d", i)),
			Select: stream.Select,
		})
	}
	return out
}

func planStreamsFromBranchComposePlan(plan branchComposePlan) []planStream {
	out := make([]planStream, 0, len(plan.Branches))
	for i := range plan.Branches {
		branch := plan.Branches[i]
		out = append(out, planStream{
			Name:   firstNonEmpty(branch.Name, string(branch.Selector.Type), fmt.Sprintf("stream-%d", i)),
			Select: streamSelectFromAV(branch.Selector),
		})
	}
	return out
}

func planOutputs(outputs []TargetIntent, formats map[string]av.FormatID) []planOutput {
	out := make([]planOutput, 0, len(outputs))
	for i := range outputs {
		output := outputs[i]
		name := firstNonEmpty(output.Name, output.URI, fmt.Sprintf("output-%d", i))
		formatID := output.Format
		if formats != nil && formats[name] != "" {
			formatID = formats[name]
		}
		operation := OpSink
		if output.URI != "" || output.Protocol != "" || output.MIMEType != "" || formatID != "" {
			operation = OpMux
		}
		out = append(out, planOutput{
			Name:      name,
			Operation: operation,
			Component: firstNonEmpty(string(formatID), string(output.Protocol), output.MIMEType, "sink"),
			Format:    formatID,
		})
	}
	return out
}

func planBranches(state *recipeCompileState, outputs []planOutput) ([]planBranch, []planDecision) {
	if len(state.intent.Streams) == 0 {
		return planCopyBranches(state.intent, outputs)
	}
	branches := make([]planBranch, 0, len(state.intent.Streams))
	decisions := make([]planDecision, 0, len(state.intent.Streams))
	for i := range state.intent.Streams {
		stream := state.intent.Streams[i]
		var shape MediaShape
		if selected, ok := planSelectedStream(state, stream); ok {
			stream.Select = streamSelectFromStream(selected)
			shape = mediaShapeFromPlanStream(selected, DomainPacket)
		}
		shape = normalizePlanBranchShape(shape, stream, firstInput(state.intent.Inputs))
		branchName := firstNonEmpty(stream.Name, string(stream.Select.Type), fmt.Sprintf("branch-%d", i))
		steps := state.chainSteps
		if len(state.intent.Streams) > 1 {
			steps = nil
		}
		operations, branchDecisions := planStreamOperations(state.intent.Inputs, stream, branchName, steps)
		operations = planOperationsWithShape(branchName, shape, operations)
		branches = append(branches, planBranch{
			Name:       branchName,
			Input:      firstInputName(state.intent.Inputs),
			Stream:     stream.Select,
			Shape:      shape,
			Operations: operations,
			Outputs:    planBranchTargets(stream.Targets, outputs),
		})
		decisions = append(decisions, branchDecisions...)
	}
	return branches, decisions
}

func planBranchesFromBranchComposePlan(state *recipeCompileState, outputs []planOutput) ([]planBranch, []planDecision) {
	branches := make([]planBranch, 0, len(state.plan.Branches))
	decisions := make([]planDecision, 0, len(state.plan.Branches))
	for i := range state.plan.Branches {
		composeBranch := state.plan.Branches[i]
		stream := streamIntentFromBranchComposeBranch(composeBranch)
		var shape MediaShape
		if selected, ok := planSelectedStream(state, stream); ok {
			stream.Select = streamSelectFromStream(selected)
			shape = mediaShapeFromPlanStream(selected, DomainPacket)
		}
		shape = normalizePlanBranchShape(shape, stream, firstInput(state.intent.Inputs))
		branchName := firstNonEmpty(stream.Name, string(stream.Select.Type), fmt.Sprintf("branch-%d", i))
		operations, branchDecisions := planStreamOperations(state.intent.Inputs, stream, branchName, nil)
		operations = planOperationsWithShape(branchName, shape, operations)
		branches = append(branches, planBranch{
			Name:       branchName,
			Input:      firstInputName(state.intent.Inputs),
			Stream:     stream.Select,
			Shape:      shape,
			Operations: operations,
			Outputs:    planBranchTargets(stream.Targets, outputs),
		})
		decisions = append(decisions, branchDecisions...)
	}
	return branches, decisions
}

func streamIntentFromBranchComposeBranch(branch branchComposeBranch) StreamIntent {
	stream := StreamIntent{
		Name:        branch.Name,
		Select:      streamSelectFromAV(branch.Selector),
		Decode:      branch.Decode,
		DecodeCodec: cloneCodecSpec(branch.DecodeConfig),
		Operations:  branchComposeBranchOperations(branch),
		CodecChange: branch.CodecChange,
		Targets:     append([]string(nil), branch.Labels...),
	}
	if branch.Copy {
		stream.Encode = Copy()
	} else {
		stream.Encode = codecSpecFromEncodeConfig(branch.Encode)
	}
	return stream
}

func codecSpecFromEncodeConfig(config codec.EncodeConfig) CodecSpec {
	spec := CodecSpec{
		ID:         config.Parameters.ID,
		Type:       config.Parameters.Type,
		Parameters: config.Parameters,
		Bitrate:    config.Bitrate,
		Config:     config.Config,
		Opaque:     cloneAnyMap(config.Opaque),
		Controls:   append([]any(nil), config.Controls...),
	}
	if spec.ID == "" {
		spec.ID = config.Stream.Codec.ID
	}
	if spec.Type == "" {
		spec.Type = firstNonEmptyMedia(config.Parameters.Type, config.Stream.Type, config.Stream.Codec.Type, codecMedia(spec.ID))
	}
	if spec.Parameters.ID == "" {
		spec.Parameters.ID = spec.ID
	}
	if spec.Parameters.Type == "" {
		spec.Parameters.Type = spec.Type
	}
	return spec
}

func branchComposeBranchOperations(branch branchComposeBranch) []StreamOperation {
	if len(branch.Operations) != 0 {
		return cloneStreamOperations(branch.Operations)
	}
	operations := append(cloneStreamOperations(branch.SharedOperations), cloneStreamOperations(branch.PrivateOperations)...)
	return operations
}

func branchComposePlanHasOperations(plan branchComposePlan) bool {
	for i := range plan.Branches {
		branch := plan.Branches[i]
		if len(branchComposeBranchOperations(branch)) != 0 {
			return true
		}
	}
	return false
}

func streamSelectFromStream(stream av.Stream) StreamSelect {
	return StreamSelect{
		ID:    stream.ID,
		Index: stream.Index,
		Type:  stream.Type,
		Codec: stream.Codec.ID,
		Name:  stream.Name,
	}
}

func planSelectedStream(state *recipeCompileState, stream StreamIntent) (av.Stream, bool) {
	if state == nil {
		return av.Stream{}, false
	}
	probes := state.inputProbes
	if state.branchInputProbeReady {
		probes = []format.ProbeResult{state.branchInputProbe}
	}
	selector := streamIntentSelector(stream)
	for i := range probes {
		if len(probes[i].Streams) == 0 {
			continue
		}
		selected, err := selectDecodeStream(probes[i].Streams, selector)
		if err != nil {
			continue
		}
		return selected, true
	}
	streams := liveIntentStreams(state.intent.Inputs)
	if len(streams) != 0 {
		selected, err := selectDecodeStream(streams, selector)
		if err == nil {
			return selected, true
		}
	}
	return av.Stream{}, false
}

func planCopyBranches(intent Intent, outputs []planOutput) ([]planBranch, []planDecision) {
	branches := make([]planBranch, 0, len(intent.Inputs))
	decisions := make([]planDecision, 0, len(intent.Inputs))
	outputNames := planOutputNames(outputs)
	for i := range intent.Inputs {
		input := intent.Inputs[i]
		name := firstNonEmpty(input.Name, input.URI, fmt.Sprintf("input-%d", i))
		shape := mediaShapeFromInputIntent(input, DomainPacket)
		operations := planInputOperations(input)
		operations = append(operations, planOperation{
			Kind:      OpCopy,
			Component: "packet-copy",
			Detail:    "preserve encoded packets",
		})
		operations = planOperationsWithShape(name, shape, operations)
		branches = append(branches, planBranch{
			Name:       firstNonEmpty(name, fmt.Sprintf("copy-%d", i)),
			Input:      name,
			Shape:      shape,
			Operations: operations,
			Outputs:    append([]string(nil), outputNames...),
		})
		decisions = append(decisions, planDecision{
			Code:    "packet_copy",
			Branch:  name,
			Message: "no decode, transform, or encode requested; packets are copied to outputs",
		})
	}
	return branches, decisions
}

func planStreamOperations(inputs []InputIntent, stream StreamIntent, branchName string, steps []chainStepAttachment) ([]planOperation, []planDecision) {
	operations := planInputOperations(firstInput(inputs))
	operations = append(operations, planOperation{
		Kind:      OpSelect,
		Component: selectorComponent(stream.Select),
		Detail:    "select stream",
	})
	if len(stream.Operations) != 0 {
		streamOperations, decisions := planStreamIntentOperations(stream, branchName)
		operations = append(operations, streamOperations...)
		return operations, decisions
	}
	var decisions []planDecision
	if streamNeedsDecode(stream) {
		operations = append(operations, planOperation{
			Kind:      OpDecode,
			Component: codecComponent(stream.Select.Codec),
			Detail:    "packets to frames",
		})
		decisions = append(decisions, planDecision{
			Code:    "decode_required",
			Branch:  branchName,
			Message: "stream operations require decoded frames",
		})
	} else {
		operations = append(operations, planOperation{
			Kind:      OpCopy,
			Component: "packet-copy",
			Detail:    "no frame operation requested",
		})
		decisions = append(decisions, planDecision{
			Code:    "packet_copy",
			Branch:  branchName,
			Message: "stream can remain packet encoded",
		})
	}
	operations = append(operations, planProcessingOperations(stream, steps)...)
	if stream.Encode.ID != "" {
		operations = append(operations, planOperation{
			Kind:      OpEncode,
			Component: string(stream.Encode.ID),
			Detail:    "frames to packets",
			Shape:     mediaShapeFromCodecSpec(stream.Encode, DomainPacket),
		})
		decisions = append(decisions, planDecision{
			Code:    "encode_required",
			Branch:  branchName,
			Message: "muxed stream output requires encoded packets",
		})
	}
	operations = append(operations, planPostEncodeTapOperations(stream)...)
	return operations, decisions
}

func planStreamIntentOperations(stream StreamIntent, branchName string) ([]planOperation, []planDecision) {
	operations := make([]planOperation, 0, len(stream.Operations))
	var decisions []planDecision
	for i := range stream.Operations {
		operation := stream.Operations[i]
		operations = append(operations, planOperationFromStreamOperation(operation))
	}
	if streamOperationKindPresent(stream.Operations, OpDecode) {
		decisions = append(decisions, planDecision{
			Code:    "decode_required",
			Branch:  branchName,
			Message: "stream operations require decoded frames",
		})
	} else if streamOperationKindPresent(stream.Operations, OpCopy) {
		decisions = append(decisions, planDecision{
			Code:    "packet_copy",
			Branch:  branchName,
			Message: "stream can remain packet encoded",
		})
	}
	if streamOperationKindPresent(stream.Operations, OpEncode) {
		decisions = append(decisions, planDecision{
			Code:    "encode_required",
			Branch:  branchName,
			Message: "muxed stream output requires encoded packets",
		})
	}
	return operations, decisions
}

func planOperationFromStreamOperation(operation StreamOperation) planOperation {
	switch operation.Kind {
	case OpTransform:
		plan := planTransformOperation(operation.Transform)
		plan.Shared = operation.Shared
		return plan
	case OpShape:
		return planOperation{
			Kind:      OpShape,
			Component: "shape",
			Detail:    "media shape annotation",
			Shape:     operation.Shape,
			Shared:    operation.Shared,
		}
	case OpTap:
		plan := planTapOperation(operation.Tap)
		plan.Shared = operation.Shared
		return plan
	case OpEncode:
		return planOperation{
			Kind:      OpEncode,
			Component: string(operation.Encode.ID),
			Detail:    "frames to packets",
			Shape:     mediaShapeFromCodecSpec(operation.Encode, DomainPacket),
			Shared:    operation.Shared,
		}
	case OpDecode:
		return planOperation{
			Kind:      OpDecode,
			Component: firstNonEmpty(string(operation.Decode.ID), operation.Component),
			Detail:    "packets to frames",
			Shared:    operation.Shared,
		}
	case OpStage:
		return planOperation{
			Kind:      OpStage,
			Component: operation.Component,
			Detail:    "custom stage",
			Shared:    operation.Shared,
		}
	case OpCopy:
		return planOperation{
			Kind:      OpCopy,
			Component: firstNonEmpty(operation.Component, "packet-copy"),
			Detail:    "no frame operation requested",
			Shared:    operation.Shared,
		}
	default:
		return planOperation{
			Kind:      operation.Kind,
			Component: operation.Component,
			Shared:    operation.Shared,
		}
	}
}

func streamOperationKindPresent(operations []StreamOperation, kind OperationKind) bool {
	for i := range operations {
		if operations[i].Kind == kind {
			return true
		}
	}
	return false
}

func planInputOperations(input InputIntent) []planOperation {
	switch {
	case input.Protocol == av.ProtocolRTP || input.Protocol == av.ProtocolWebRTC || input.Realtime:
		component := firstNonEmpty(string(input.Codec.ID), string(input.Protocol), "rtp")
		return []planOperation{{
			Kind:      OpDepacketize,
			Component: component,
			Detail:    "receive RTP packets",
			Shape:     mediaShapeFromInputIntent(input, DomainPacket),
		}}
	default:
		return []planOperation{{
			Kind:      OpDemux,
			Component: "container",
			Detail:    "read packets from input",
			Shape:     mediaShapeFromInputIntent(input, DomainPacket),
		}}
	}
}

func planProcessingOperations(stream StreamIntent, steps []chainStepAttachment) []planOperation {
	if len(steps) == 0 {
		operations := make([]planOperation, 0, len(stream.Transforms)+len(stream.Taps))
		for i := range stream.Transforms {
			operations = append(operations, planTransformOperation(stream.Transforms[i]))
		}
		for i := range stream.Taps {
			if stream.Taps[i].After != "" {
				continue
			}
			operations = append(operations, planTapOperation(stream.Taps[i]))
		}
		return operations
	}
	operations := make([]planOperation, 0, len(steps))
	tapIndex := 0
	for i := range steps {
		step := steps[i]
		if step.stage != nil {
			operations = append(operations, planOperation{
				Kind:      OpStage,
				Component: step.stage.Name(),
				Detail:    "custom stage",
			})
			continue
		}
		if !mediaShapeEmpty(step.shape) {
			operations = append(operations, planOperation{
				Kind:      OpShape,
				Component: "shape",
				Detail:    "media shape annotation",
				Shape:     step.shape,
			})
			continue
		}
		if step.hasTransform && step.transformIndex >= 0 && step.transformIndex < len(stream.Transforms) {
			operations = append(operations, planTransformOperation(stream.Transforms[step.transformIndex]))
			continue
		}
		if step.tap != "" {
			tap := TapIntent{Name: step.tap}
			if tapIndex >= 0 && tapIndex < len(stream.Taps) {
				tap = stream.Taps[tapIndex]
			}
			operations = append(operations, planTapOperation(tap))
			tapIndex++
		}
	}
	return operations
}

func planTransformOperation(transform TransformSpec) planOperation {
	name := transformFactoryName(transform)
	return planOperation{
		Kind:      OpTransform,
		Component: firstNonEmpty(name, "transform"),
		Detail:    firstNonEmpty(name, "transform frames"),
		Shape:     mediaShapeFromTransform(transform),
	}
}

func planTapOperation(tap TapIntent) planOperation {
	return planOperation{
		Kind:      OpTap,
		Component: firstNonEmpty(tap.Name, "tap"),
		Detail:    "named media outlet",
		After:     tap.After,
	}
}

func planPostEncodeTapOperations(stream StreamIntent) []planOperation {
	operations := make([]planOperation, 0)
	for i := range stream.Taps {
		if stream.Taps[i].After != OpEncode {
			continue
		}
		operations = append(operations, planTapOperation(stream.Taps[i]))
	}
	return operations
}

func planTaps(branches []planBranch) []planTap {
	var taps []planTap
	for i := range branches {
		branch := branches[i]
		currentShape := branch.Shape
		if currentShape.Domain == "" {
			currentShape.Domain = DomainPacket
		}
		if currentShape.MediaKind == "" {
			currentShape.MediaKind = branch.Stream.Type
		}
		if currentShape.StreamID == "" {
			currentShape.StreamID = av.StreamID(firstNonEmpty(string(branch.Stream.ID), branch.Name))
		}
		if currentShape.Codec == "" {
			currentShape.Codec = branch.Stream.Codec
		}
		currentNode := firstNonEmpty(branch.Input, branch.Name)
		for j := range branch.Operations {
			operation := branch.Operations[j]
			if operation.Kind == OpCopy {
				currentShape = planShapeForOperation(currentShape, branch, operation)
				continue
			}
			if operation.Kind != OpTap {
				currentShape = planShapeForOperation(currentShape, branch, operation)
				currentNode = planOperationNodeName(branch, operation, j)
				continue
			}
			name := operation.Component
			tapShape := operation.Shape
			if mediaShapeEmpty(tapShape) {
				tapShape = currentShape
			}
			tapShape = normalizeTapShape(tapShape)
			taps = append(taps, planTap{
				Name:      name,
				Node:      pipeline.NodeRef(currentNode),
				Domain:    tapShape.Domain,
				MediaKind: tapShape.MediaKind,
				After:     operation.After,
				Shape:     tapShape,
				Shared:    true,
			})
		}
	}
	return taps
}

func planOperationsWithShape(branchName string, baseShape MediaShape, operations []planOperation) []planOperation {
	if len(operations) == 0 {
		return nil
	}
	out := append([]planOperation(nil), operations...)
	shape := normalizeTapShape(baseShape)
	branch := planBranch{Name: branchName}
	for i := range out {
		operation := out[i]
		if operation.Kind == OpTap {
			if mediaShapeEmpty(operation.Shape) {
				operation.Shape = shape
			}
			out[i] = operation
			continue
		}
		shape = planShapeAfterOperation(shape, branch, operation)
		operation.Shape = shape
		out[i] = operation
	}
	return out
}

func planShapeForOperation(current MediaShape, branch planBranch, operation planOperation) MediaShape {
	if !mediaShapeEmpty(operation.Shape) {
		return operation.Shape
	}
	return planShapeAfterOperation(current, branch, operation)
}

func planShapeAfterOperation(shape MediaShape, branch planBranch, operation planOperation) MediaShape {
	switch operation.Kind {
	case OpDepacketize:
		if codecID := knownPlanCodec(operation.Component); codecID != "" {
			shape.Codec = codecID
			shape.MediaKind = firstNonEmptyMedia(shape.MediaKind, codecMedia(codecID))
		}
		shape.Domain = DomainPacket
	case OpDecode, OpStage:
		shape.Domain = DomainFrame
	case OpShape:
		shape = mergeMediaShape(shape, operation.Shape)
	case OpTransform:
		shape.Domain = DomainFrame
		shape = mergeMediaShape(shape, operation.Shape)
	case OpCopy:
		shape.Domain = DomainPacket
	case OpEncode:
		shape.Domain = DomainPacket
		shape.StreamID = av.StreamID(firstNonEmpty(branch.Name, string(shape.StreamID)))
		shape.Codec = av.CodecID(operation.Component)
		if media := codecMedia(shape.Codec); media != "" {
			shape.MediaKind = media
		}
		shape = mergeMediaShape(shape, operation.Shape)
	}
	return shape
}

func normalizeTapShape(shape MediaShape) MediaShape {
	if shape.Domain == "" {
		shape.Domain = DomainPacket
	}
	return shape
}

func normalizePlanBranchShape(shape MediaShape, stream StreamIntent, input InputIntent) MediaShape {
	if shape.Domain == "" {
		shape.Domain = DomainPacket
	}
	if shape.MediaKind == "" {
		shape.MediaKind = firstNonEmptyMedia(stream.Select.Type, stream.Encode.Type, input.Codec.Type)
	}
	if shape.StreamID == "" {
		shape.StreamID = stream.Select.ID
	}
	if shape.Codec == "" {
		shape.Codec = firstNonEmptyCodec(stream.Select.Codec, input.Codec.ID)
	}
	if input.Realtime || input.Protocol == av.ProtocolRTP || input.Protocol == av.ProtocolWebRTC {
		shape.Realtime = true
	}
	return shape
}

func mediaShapeFromPlanStream(stream av.Stream, domain MediaDomain) MediaShape {
	return MediaShapeFromStream(stream, domain)
}

func mediaShapeFromInputIntent(input InputIntent, domain MediaDomain) MediaShape {
	shape := MediaShape{
		Domain:    domain,
		MediaKind: input.Codec.Type,
		StreamID:  av.StreamID(input.Name),
		Codec:     input.Codec.ID,
		Realtime:  input.Realtime || input.Protocol == av.ProtocolRTP || input.Protocol == av.ProtocolWebRTC,
	}
	shape = mergeMediaShape(shape, mediaShapeFromCodecParameters(input.Codec.Parameters))
	if shape.MediaKind == "" {
		shape.MediaKind = codecMedia(shape.Codec)
	}
	return shape
}

func mediaShapeFromCodecParameters(parameters av.CodecParameters) MediaShape {
	return MediaShapeFromCodecParameters(parameters)
}

func mediaShapeFromCodecSpec(spec CodecSpec, domain MediaDomain) MediaShape {
	return MediaShapeFromCodecSpec(spec, domain)
}

func mediaShapeFromTransform(transform TransformSpec) MediaShape {
	if transform.Resize != nil {
		return MediaShape{
			Domain:      DomainFrame,
			MediaKind:   av.MediaVideo,
			Width:       transform.Resize.Width,
			Height:      transform.Resize.Height,
			PixelFormat: transform.Resize.PixelFormat,
		}
	}
	if transform.Resample != nil {
		return MediaShape{
			Domain:       DomainFrame,
			MediaKind:    av.MediaAudio,
			SampleRate:   transform.Resample.SampleRate,
			Channels:     transform.Resample.Channels,
			SampleFormat: transform.Resample.SampleFormat,
		}
	}
	return MediaShape{}
}

func mediaShapeEmpty(shape MediaShape) bool {
	return shape.Domain == "" &&
		shape.MediaKind == "" &&
		shape.StreamID == "" &&
		shape.Codec == "" &&
		shape.Format == "" &&
		shape.Width == 0 &&
		shape.Height == 0 &&
		shape.PixelFormat == "" &&
		shape.Framerate == (Rational{}) &&
		shape.SampleRate == 0 &&
		shape.Channels == 0 &&
		shape.SampleFormat == "" &&
		!shape.Realtime
}

func firstNonEmptyCodec(values ...av.CodecID) av.CodecID {
	for i := range values {
		if values[i] != "" {
			return values[i]
		}
	}
	return ""
}

func knownPlanCodec(component string) av.CodecID {
	codecID := av.CodecID(component)
	if codecMedia(codecID) == "" {
		return ""
	}
	return codecID
}

func planOperationNodeName(branch planBranch, operation planOperation, index int) string {
	switch operation.Kind {
	case OpDecode:
		return "decode-" + planBranchOperationScopeName(branch)
	case OpTransform:
		name := branch.Name
		if operation.Shared {
			name = planBranchOperationScopeName(branch)
		}
		return operation.Component + "-" + name
	case OpStage:
		return operation.Component
	case OpEncode:
		return "encode-" + branch.Name
	case OpSelect:
		return "select-" + firstNonEmpty(operation.Component, planBranchOperationScopeName(branch))
	case OpDepacketize, OpDemux:
		return branch.Name
	default:
		if operation.Component != "" {
			return operation.Component
		}
		return fmt.Sprintf("%s-op-%d", branch.Name, index)
	}
}

func planBranchOperationScopeName(branch planBranch) string {
	return firstNonEmpty(
		string(branch.Stream.Type),
		string(branch.Stream.ID),
		branch.Stream.Name,
		string(branch.Stream.Codec),
		branch.Name,
		"stream",
	)
}

func planBranchTargets(targetRefs []string, outputs []planOutput) []string {
	if len(targetRefs) != 0 {
		return append([]string(nil), targetRefs...)
	}
	return planOutputNames(outputs)
}

func planOutputNames(outputs []planOutput) []string {
	names := make([]string, 0, len(outputs))
	for i := range outputs {
		names = append(names, outputs[i].Name)
	}
	return names
}

func planOutputsWithBranches(outputs []planOutput, branches []planBranch) []planOutput {
	for i := range outputs {
		for j := range branches {
			if stringInSlice(outputs[i].Name, branches[j].Outputs) {
				outputs[i].BranchRefs = append(outputs[i].BranchRefs, branches[j].Name)
			}
		}
	}
	return outputs
}

func clonePlanInputs(inputs []planInput) []planInput {
	if len(inputs) == 0 {
		return nil
	}
	return append([]planInput(nil), inputs...)
}

func clonePlanStreams(streams []planStream) []planStream {
	if len(streams) == 0 {
		return nil
	}
	return append([]planStream(nil), streams...)
}

func clonePlanTaps(taps []planTap) []planTap {
	if len(taps) == 0 {
		return nil
	}
	return append([]planTap(nil), taps...)
}

func clonePlanBranches(branches []planBranch) []planBranch {
	if len(branches) == 0 {
		return nil
	}
	out := make([]planBranch, 0, len(branches))
	for i := range branches {
		branch := branches[i]
		branch.Operations = clonePlanOperations(branch.Operations)
		branch.Outputs = append([]string(nil), branch.Outputs...)
		out = append(out, branch)
	}
	return out
}

func clonePlanOperations(operations []planOperation) []planOperation {
	if len(operations) == 0 {
		return nil
	}
	return append([]planOperation(nil), operations...)
}

func clonePlanOutputs(outputs []planOutput) []planOutput {
	if len(outputs) == 0 {
		return nil
	}
	out := make([]planOutput, 0, len(outputs))
	for i := range outputs {
		output := outputs[i]
		output.BranchRefs = append([]string(nil), output.BranchRefs...)
		out = append(out, output)
	}
	return out
}

func clonePlanDecisions(decisions []planDecision) []planDecision {
	if len(decisions) == 0 {
		return nil
	}
	return append([]planDecision(nil), decisions...)
}

func clonePlanDiagnostics(diagnostics []PlanDiagnostic) []PlanDiagnostic {
	if len(diagnostics) == 0 {
		return nil
	}
	out := make([]PlanDiagnostic, 0, len(diagnostics))
	for i := range diagnostics {
		diagnostic := diagnostics[i]
		diagnostic.Details = append([]string(nil), diagnostic.Details...)
		diagnostic.Suggestions = append([]string(nil), diagnostic.Suggestions...)
		out = append(out, diagnostic)
	}
	return out
}

func firstInput(inputs []InputIntent) InputIntent {
	if len(inputs) == 0 {
		return InputIntent{}
	}
	return inputs[0]
}

func firstInputName(inputs []InputIntent) string {
	if len(inputs) == 0 {
		return "input"
	}
	return firstNonEmpty(inputs[0].Name, inputs[0].URI, "input")
}

func selectorComponent(selector StreamSelect) string {
	return firstNonEmpty(string(selector.ID), selector.Name, string(selector.Type), string(selector.Codec), "stream")
}

func codecComponent(codecID av.CodecID) string {
	return firstNonEmpty(string(codecID), "decoder")
}

func stringInSlice(needle string, haystack []string) bool {
	for i := range haystack {
		if haystack[i] == needle {
			return true
		}
	}
	return false
}
