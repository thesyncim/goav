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
	plan.Outputs = planOutputs(intent.Destinations, state.outputFormatMap())
	if state.branchCompositionPresent && branchComposePlanReady(state.plan) && branchComposePlanHasOperations(state.plan) {
		streams := streamIntentsFromBranchComposePlan(state.plan)
		plan.Streams = planStreams(streams)
		plan.Branches, plan.Decisions = planBranchesFromStreamIntents(state, streams, plan.Outputs)
	} else {
		plan.Branches, plan.Decisions = planBranches(state, plan.Outputs)
	}
	plan.Taps = planTaps(plan.Branches)
	plan.Outputs = planOutputsWithBranches(plan.Outputs, plan.Branches)
	return plan
}

func planInputs(inputs []inputIntent) []planInput {
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

func planStreams(streams []streamIntent) []planStream {
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

// streamIntentsFromBranchComposePlan converts a branchComposePlan to the
// streamIntent list the Explain-side planners consume — the single point where
// the parallel branchComposePlan feeds the streamIntent-based mediaPlan, so
// stream and branch planning both run through the same code as the direct path.
func streamIntentsFromBranchComposePlan(plan branchComposePlan) []streamIntent {
	streams := make([]streamIntent, 0, len(plan.Branches))
	for i := range plan.Branches {
		streams = append(streams, streamIntentFromBranchComposeBranch(plan.Branches[i]))
	}
	return streams
}

func planOutputs(outputs []destinationIntent, formats map[string]av.FormatID) []planOutput {
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

// planBranchFromStreamIntent plans one branch from a resolved stream intent —
// the single per-branch planning body. Both planning sources (direct streams
// and a branchComposePlan converted via streamIntentsFromBranchComposePlan)
// reach it through planBranchesFromStreamIntents, so the Explain-side mediaPlan
// has one branch planner (NORTH_STAR step 3).
func planBranchFromStreamIntent(state *recipeCompileState, stream streamIntent, index int, outputs []planOutput) (planBranch, []planDecision) {
	var shape MediaShape
	sourceShape, sourceShapeOK := compileStateCustomSourceShape(state)
	if selected, ok := planSelectedStream(state, stream); ok {
		stream.Select = streamSelectFromStream(selected)
		domain := DomainPacket
		if sourceShapeOK && sourceShape.Domain != "" {
			domain = sourceShape.Domain
		}
		shape = mediaShapeFromPlanStream(selected, domain)
		if sourceShapeOK {
			shape = mergeMediaShape(shape, sourceShape)
		}
	}
	shape = normalizePlanBranchShape(shape, stream, firstInput(state.intent.Inputs))
	branchName := firstNonEmpty(stream.Name, string(stream.Select.Type), fmt.Sprintf("branch-%d", index))
	operations, branchDecisions := planOperationSpecs(state.intent.Inputs, stream, branchName, shape)
	operations = planOperationsWithShape(branchName, shape, operations)
	return planBranch{
		Name:       branchName,
		Input:      firstInputName(state.intent.Inputs),
		Stream:     stream.Select,
		Shape:      shape,
		Operations: operations,
		Outputs:    planBranchDestinations(stream.Destinations, outputs),
	}, branchDecisions
}

func planBranches(state *recipeCompileState, outputs []planOutput) ([]planBranch, []planDecision) {
	if len(state.intent.Streams) == 0 {
		return planCopyBranches(state, outputs)
	}
	return planBranchesFromStreamIntents(state, state.intent.Streams, outputs)
}

// planBranchesFromStreamIntents plans every branch from a resolved streamIntent
// list — shared by the direct-stream path (planBranches, source
// state.intent.Streams) and the branch-composition path (buildMediaPlan, source
// streamIntentsFromBranchComposePlan). One planner, two sources.
func planBranchesFromStreamIntents(state *recipeCompileState, streams []streamIntent, outputs []planOutput) ([]planBranch, []planDecision) {
	branches := make([]planBranch, 0, len(streams))
	decisions := make([]planDecision, 0, len(streams))
	for i := range streams {
		branch, branchDecisions := planBranchFromStreamIntent(state, streams[i], i, outputs)
		branches = append(branches, branch)
		decisions = append(decisions, branchDecisions...)
	}
	return branches, decisions
}

func streamIntentFromBranchComposeBranch(branch branchComposeBranch) streamIntent {
	stream := streamIntent{
		Name:         branch.Name,
		Select:       streamSelectFromAV(branch.Selector),
		Decode:       branch.Decode,
		DecodeCodec:  cloneCodecSpec(branch.DecodeConfig),
		Operations:   branchComposeBranchOperationSpecs(branch),
		CodecChange:  branch.CodecChange,
		Destinations: append([]string(nil), branch.Labels...),
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
		Settings:   cloneCodecSettings(config.Settings),
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

func branchComposeBranchOperationSpecs(branch branchComposeBranch) []OperationSpec {
	if len(branch.Operations) != 0 {
		return cloneOperationSpecs(branch.Operations)
	}
	operations := append(cloneOperationSpecs(branch.SharedOperations), cloneOperationSpecs(branch.PrivateOperations)...)
	return operations
}

func branchComposePlanHasOperations(plan branchComposePlan) bool {
	for i := range plan.Branches {
		branch := plan.Branches[i]
		if len(branchComposeBranchOperationSpecs(branch)) != 0 {
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

func planSelectedStream(state *recipeCompileState, stream streamIntent) (av.Stream, bool) {
	if state == nil {
		return av.Stream{}, false
	}
	probes := state.inputProbes
	if state.branchInputProbeReady {
		probes = []format.ProbeResult{state.branchInputProbe}
	}
	sourceShape, sourceShapeOK := compileStateCustomSourceShape(state)
	selector := streamIntentSelector(stream)
	for i := range probes {
		if len(probes[i].Streams) == 0 {
			continue
		}
		selected, err := selectDecodeStream(probes[i].Streams, selector)
		if sourceShapeOK && sourceShape.Domain == DomainFrame {
			selected, err = selectStream(probes[i].Streams, selector)
		}
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

func planCopyBranches(state *recipeCompileState, outputs []planOutput) ([]planBranch, []planDecision) {
	intent := state.intent
	branches := make([]planBranch, 0, len(intent.Inputs))
	decisions := make([]planDecision, 0, len(intent.Inputs))
	outputNames := planOutputNames(outputs)
	for i := range intent.Inputs {
		input := intent.Inputs[i]
		name := firstNonEmpty(input.Name, input.URI, fmt.Sprintf("input-%d", i))
		shape := mediaShapeFromInputIntent(input, DomainPacket)
		if i < len(state.inputAttachments) {
			if sourceShape, ok := customSourceShape(state.inputAttachments[i]); ok {
				shape = mergeMediaShape(shape, sourceShape)
			}
		}
		operations := planInputOperationsForShape(input, shape)
		decision := planDecision{
			Code:    "packet_copy",
			Branch:  name,
			Message: "no decode, transform, or encode requested; packets are copied to outputs",
		}
		if shape.Domain == DomainEvent {
			operations = append(operations, planOperation{
				Kind:      OpShape,
				Component: "shape",
				Detail:    "event source",
				Shape:     shape,
			})
			decision = planDecision{
				Code:    "event_source",
				Branch:  name,
				Message: "source produces events for sink destinations",
			}
		} else {
			operations = append(operations, planOperation{
				Kind:      OpCopy,
				Component: "packet-copy",
				Detail:    "preserve encoded packets",
			})
		}
		operations = planOperationsWithShape(name, shape, operations)
		branches = append(branches, planBranch{
			Name:       firstNonEmpty(name, fmt.Sprintf("copy-%d", i)),
			Input:      name,
			Shape:      shape,
			Operations: operations,
			Outputs:    append([]string(nil), outputNames...),
		})
		decisions = append(decisions, decision)
	}
	return branches, decisions
}

func planOperationSpecs(inputs []inputIntent, stream streamIntent, branchName string, initial MediaShape) ([]planOperation, []planDecision) {
	operations := planInputOperationsForShape(firstInput(inputs), initial)
	operations = append(operations, planOperation{
		Kind:      OpSelect,
		Component: selectorComponent(stream.Select),
		Detail:    "select stream",
	})
	if len(stream.Operations) != 0 {
		operationSpecs, decisions := planStreamIntentOperations(stream, branchName)
		operations = append(operations, operationSpecs...)
		return operations, decisions
	}
	var decisions []planDecision
	if initial.Domain == DomainFrame {
		operations = append(operations, planProcessingOperations(stream)...)
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
		} else {
			decisions = append(decisions, planDecision{
				Code:    "frame_source",
				Branch:  branchName,
				Message: "source already produces decoded frames",
			})
		}
		operations = append(operations, planPostEncodeTapOperations(stream)...)
		return operations, decisions
	}
	if streamNeedsDecode(stream) {
		operations = append(operations, planOperation{
			Kind:      OpDecode,
			Component: codecComponent(stream.Select.Codec),
			Detail:    "packets to frames",
		})
		decisions = append(decisions, planDecision{
			Code:    "decode_required",
			Branch:  branchName,
			Message: "operation specs require decoded frames",
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
	operations = append(operations, planProcessingOperations(stream)...)
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

func planStreamIntentOperations(stream streamIntent, branchName string) ([]planOperation, []planDecision) {
	operations := make([]planOperation, 0, len(stream.Operations))
	var decisions []planDecision
	for i := range stream.Operations {
		operation := stream.Operations[i]
		operations = append(operations, planOperationFromOperationSpec(operation))
	}
	if operationSpecKindPresent(stream.Operations, OpDecode) {
		decisions = append(decisions, planDecision{
			Code:    "decode_required",
			Branch:  branchName,
			Message: "operation specs require decoded frames",
		})
	} else if operationSpecKindPresent(stream.Operations, OpCopy) {
		decisions = append(decisions, planDecision{
			Code:    "packet_copy",
			Branch:  branchName,
			Message: "stream can remain packet encoded",
		})
	}
	if operationSpecKindPresent(stream.Operations, OpEncode) {
		decisions = append(decisions, planDecision{
			Code:    "encode_required",
			Branch:  branchName,
			Message: "muxed stream output requires encoded packets",
		})
	}
	return operations, decisions
}

func planOperationFromOperationSpec(operation OperationSpec) planOperation {
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

func operationSpecKindPresent(operations []OperationSpec, kind OperationKind) bool {
	for i := range operations {
		if operations[i].Kind == kind {
			return true
		}
	}
	return false
}

func planInputOperations(input inputIntent) []planOperation {
	return planInputOperationsForShape(input, MediaShape{Domain: DomainPacket})
}

func planInputOperationsForShape(input inputIntent, shape MediaShape) []planOperation {
	if input.Protocol == av.ProtocolCustom {
		return nil
	}
	switch {
	case input.Protocol == av.ProtocolRTP || input.Protocol == av.ProtocolWebRTC || input.Realtime:
		component := firstNonEmpty(string(input.Codec.ID), string(input.Protocol), "rtp")
		return []planOperation{{
			Kind:      OpDepacketize,
			Component: component,
			Detail:    "receive RTP packets",
			Shape:     mediaShapeFromInputIntent(input, firstNonEmptyDomain(shape.Domain, DomainPacket)),
		}}
	default:
		return []planOperation{{
			Kind:      OpDemux,
			Component: "container",
			Detail:    "read packets from input",
			Shape:     mediaShapeFromInputIntent(input, firstNonEmptyDomain(shape.Domain, DomainPacket)),
		}}
	}
}

func planProcessingOperations(stream streamIntent) []planOperation {
	transforms := streamIntentTransformSpecs(stream)
	operations := make([]planOperation, 0, len(transforms)+len(stream.Taps))
	for i := range transforms {
		operations = append(operations, planTransformOperation(transforms[i]))
	}
	for i := range stream.Taps {
		if stream.Taps[i].After != "" {
			continue
		}
		operations = append(operations, planTapOperation(stream.Taps[i]))
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

func planTapOperation(tap tapIntent) planOperation {
	return planOperation{
		Kind:      OpTap,
		Component: firstNonEmpty(tap.Name, "tap"),
		Detail:    "named media outlet",
		After:     tap.After,
	}
}

func planPostEncodeTapOperations(stream streamIntent) []planOperation {
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

func normalizePlanBranchShape(shape MediaShape, stream streamIntent, input inputIntent) MediaShape {
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

func mediaShapeFromInputIntent(input inputIntent, domain MediaDomain) MediaShape {
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

func firstNonEmptyDomain(values ...MediaDomain) MediaDomain {
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

func planBranchDestinations(targetRefs []string, outputs []planOutput) []string {
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

func firstInput(inputs []inputIntent) inputIntent {
	if len(inputs) == 0 {
		return inputIntent{}
	}
	return inputs[0]
}

func firstInputName(inputs []inputIntent) string {
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
