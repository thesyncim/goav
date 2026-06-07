package goav

import (
	"fmt"

	"github.com/thesyncim/goav/av"
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
	Caps      StreamCaps
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
	Caps       StreamCaps
	Operations []planOperation
	Outputs    []string
}

type planOperation struct {
	Kind      OperationKind
	Component string
	Detail    string
	After     OperationKind
	Caps      StreamCaps
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
	plan.Branches, plan.Decisions = planBranches(state, plan.Outputs)
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
		var caps StreamCaps
		if selected, ok := planSelectedStream(state, stream); ok {
			stream.Select = streamSelectFromStream(selected)
			caps = streamCapsFromPlanStream(selected, DomainPacket)
		}
		caps = normalizePlanBranchCaps(caps, stream, firstInput(state.intent.Inputs))
		branchName := firstNonEmpty(stream.Name, string(stream.Select.Type), fmt.Sprintf("branch-%d", i))
		steps := state.chainSteps
		if len(state.intent.Streams) > 1 {
			steps = nil
		}
		operations, branchDecisions := planStreamOperations(state.intent.Inputs, stream, branchName, steps)
		operations = planOperationsWithCaps(branchName, caps, operations)
		branches = append(branches, planBranch{
			Name:       branchName,
			Input:      firstInputName(state.intent.Inputs),
			Stream:     stream.Select,
			Caps:       caps,
			Operations: operations,
			Outputs:    planBranchTargets(stream.Targets, outputs),
		})
		decisions = append(decisions, branchDecisions...)
	}
	return branches, decisions
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
		caps := streamCapsFromInputIntent(input, DomainPacket)
		operations := planInputOperations(input)
		operations = append(operations, planOperation{
			Kind:      OpCopy,
			Component: "packet-copy",
			Detail:    "preserve encoded packets",
		})
		operations = planOperationsWithCaps(name, caps, operations)
		branches = append(branches, planBranch{
			Name:       firstNonEmpty(name, fmt.Sprintf("copy-%d", i)),
			Input:      name,
			Caps:       caps,
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
			Caps:      streamCapsFromCodecSpec(stream.Encode, DomainPacket),
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
	case OpTap:
		plan := planTapOperation(operation.Tap)
		plan.Shared = operation.Shared
		return plan
	case OpEncode:
		return planOperation{
			Kind:      OpEncode,
			Component: string(operation.Encode.ID),
			Detail:    "frames to packets",
			Caps:      streamCapsFromCodecSpec(operation.Encode, DomainPacket),
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
			Caps:      streamCapsFromInputIntent(input, DomainPacket),
		}}
	default:
		return []planOperation{{
			Kind:      OpDemux,
			Component: "container",
			Detail:    "read packets from input",
			Caps:      streamCapsFromInputIntent(input, DomainPacket),
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
		Caps:      streamCapsFromTransform(transform),
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
		currentCaps := branch.Caps
		if currentCaps.Domain == "" {
			currentCaps.Domain = DomainPacket
		}
		if currentCaps.MediaKind == "" {
			currentCaps.MediaKind = branch.Stream.Type
		}
		if currentCaps.StreamID == "" {
			currentCaps.StreamID = av.StreamID(firstNonEmpty(string(branch.Stream.ID), branch.Name))
		}
		if currentCaps.Codec == "" {
			currentCaps.Codec = branch.Stream.Codec
		}
		currentNode := firstNonEmpty(branch.Input, branch.Name)
		for j := range branch.Operations {
			operation := branch.Operations[j]
			if operation.Kind == OpCopy {
				currentCaps = planCapsForOperation(currentCaps, branch, operation)
				continue
			}
			if operation.Kind != OpTap {
				currentCaps = planCapsForOperation(currentCaps, branch, operation)
				currentNode = planOperationNodeName(branch, operation, j)
				continue
			}
			name := operation.Component
			tapCaps := operation.Caps
			if streamCapsEmpty(tapCaps) {
				tapCaps = currentCaps
			}
			tapCaps = normalizeTapCaps(tapCaps)
			taps = append(taps, planTap{
				Name:      name,
				Node:      pipeline.NodeRef(currentNode),
				Domain:    tapCaps.Domain,
				MediaKind: tapCaps.MediaKind,
				After:     operation.After,
				Caps:      tapCaps,
				Shared:    true,
			})
		}
	}
	return taps
}

func planOperationsWithCaps(branchName string, baseCaps StreamCaps, operations []planOperation) []planOperation {
	if len(operations) == 0 {
		return nil
	}
	out := append([]planOperation(nil), operations...)
	caps := normalizeTapCaps(baseCaps)
	branch := planBranch{Name: branchName}
	for i := range out {
		operation := out[i]
		if operation.Kind == OpTap {
			if streamCapsEmpty(operation.Caps) {
				operation.Caps = caps
			}
			out[i] = operation
			continue
		}
		caps = planCapsAfterOperation(caps, branch, operation)
		operation.Caps = caps
		out[i] = operation
	}
	return out
}

func planCapsForOperation(current StreamCaps, branch planBranch, operation planOperation) StreamCaps {
	if !streamCapsEmpty(operation.Caps) {
		return operation.Caps
	}
	return planCapsAfterOperation(current, branch, operation)
}

func planCapsAfterOperation(caps StreamCaps, branch planBranch, operation planOperation) StreamCaps {
	switch operation.Kind {
	case OpDepacketize:
		if codecID := knownPlanCodec(operation.Component); codecID != "" {
			caps.Codec = codecID
			caps.MediaKind = firstNonEmptyMedia(caps.MediaKind, codecMedia(codecID))
		}
		caps.Domain = DomainPacket
	case OpDecode, OpStage:
		caps.Domain = DomainFrame
	case OpTransform:
		caps.Domain = DomainFrame
		caps = mergeStreamCaps(caps, operation.Caps)
	case OpCopy:
		caps.Domain = DomainPacket
	case OpEncode:
		caps.Domain = DomainPacket
		caps.StreamID = av.StreamID(firstNonEmpty(branch.Name, string(caps.StreamID)))
		caps.Codec = av.CodecID(operation.Component)
		if media := codecMedia(caps.Codec); media != "" {
			caps.MediaKind = media
		}
		caps = mergeStreamCaps(caps, operation.Caps)
	}
	return caps
}

func normalizeTapCaps(caps StreamCaps) StreamCaps {
	if caps.Domain == "" {
		caps.Domain = DomainPacket
	}
	return caps
}

func normalizePlanBranchCaps(caps StreamCaps, stream StreamIntent, input InputIntent) StreamCaps {
	if caps.Domain == "" {
		caps.Domain = DomainPacket
	}
	if caps.MediaKind == "" {
		caps.MediaKind = firstNonEmptyMedia(stream.Select.Type, stream.Encode.Type, input.Codec.Type)
	}
	if caps.StreamID == "" {
		caps.StreamID = stream.Select.ID
	}
	if caps.Codec == "" {
		caps.Codec = firstNonEmptyCodec(stream.Select.Codec, input.Codec.ID)
	}
	if input.Realtime || input.Protocol == av.ProtocolRTP || input.Protocol == av.ProtocolWebRTC {
		caps.Realtime = true
	}
	return caps
}

func streamCapsFromPlanStream(stream av.Stream, domain MediaDomain) StreamCaps {
	caps := StreamCaps{Domain: domain}
	if stream.Type != "" {
		caps.MediaKind = stream.Type
	}
	if stream.ID != "" {
		caps.StreamID = stream.ID
	}
	if stream.Codec.ID != "" {
		caps.Codec = stream.Codec.ID
	}
	if stream.Codec.Width != 0 {
		caps.Width = stream.Codec.Width
	}
	if stream.Codec.Height != 0 {
		caps.Height = stream.Codec.Height
	}
	if stream.Codec.PixelFormat != "" {
		caps.PixelFormat = stream.Codec.PixelFormat
	}
	if stream.Codec.SampleRate != 0 {
		caps.SampleRate = stream.Codec.SampleRate
	}
	if stream.Codec.Channels != 0 {
		caps.Channels = stream.Codec.Channels
	}
	if stream.Codec.SampleFormat != "" {
		caps.SampleFormat = stream.Codec.SampleFormat
	}
	return caps
}

func streamCapsFromInputIntent(input InputIntent, domain MediaDomain) StreamCaps {
	caps := StreamCaps{
		Domain:    domain,
		MediaKind: input.Codec.Type,
		StreamID:  av.StreamID(input.Name),
		Codec:     input.Codec.ID,
		Realtime:  input.Realtime || input.Protocol == av.ProtocolRTP || input.Protocol == av.ProtocolWebRTC,
	}
	caps = mergeStreamCaps(caps, streamCapsFromCodecParameters(input.Codec.Parameters))
	if caps.MediaKind == "" {
		caps.MediaKind = codecMedia(caps.Codec)
	}
	return caps
}

func streamCapsFromCodecParameters(parameters av.CodecParameters) StreamCaps {
	return StreamCaps{
		MediaKind:    parameters.Type,
		Codec:        parameters.ID,
		Width:        parameters.Width,
		Height:       parameters.Height,
		PixelFormat:  parameters.PixelFormat,
		SampleRate:   parameters.SampleRate,
		Channels:     parameters.Channels,
		SampleFormat: parameters.SampleFormat,
	}
}

func streamCapsFromCodecSpec(spec CodecSpec, domain MediaDomain) StreamCaps {
	caps := streamCapsFromCodecParameters(spec.Parameters)
	caps.Domain = domain
	if caps.MediaKind == "" {
		caps.MediaKind = firstNonEmptyMedia(spec.Type, codecMedia(spec.ID))
	}
	if caps.Codec == "" {
		caps.Codec = spec.ID
	}
	return caps
}

func streamCapsFromTransform(transform TransformSpec) StreamCaps {
	if transform.Resize != nil {
		return StreamCaps{
			Domain:      DomainFrame,
			MediaKind:   av.MediaVideo,
			Width:       transform.Resize.Width,
			Height:      transform.Resize.Height,
			PixelFormat: transform.Resize.PixelFormat,
		}
	}
	if transform.Resample != nil {
		return StreamCaps{
			Domain:       DomainFrame,
			MediaKind:    av.MediaAudio,
			SampleRate:   transform.Resample.SampleRate,
			Channels:     transform.Resample.Channels,
			SampleFormat: transform.Resample.SampleFormat,
		}
	}
	return StreamCaps{}
}

func mergeStreamCaps(base StreamCaps, next StreamCaps) StreamCaps {
	if next.Domain != "" {
		base.Domain = next.Domain
	}
	if next.MediaKind != "" {
		base.MediaKind = next.MediaKind
	}
	if next.StreamID != "" {
		base.StreamID = next.StreamID
	}
	if next.Codec != "" {
		base.Codec = next.Codec
	}
	if next.Format != "" {
		base.Format = next.Format
	}
	if next.Width != 0 {
		base.Width = next.Width
	}
	if next.Height != 0 {
		base.Height = next.Height
	}
	if next.PixelFormat != "" {
		base.PixelFormat = next.PixelFormat
	}
	if next.SampleRate != 0 {
		base.SampleRate = next.SampleRate
	}
	if next.Channels != 0 {
		base.Channels = next.Channels
	}
	if next.SampleFormat != "" {
		base.SampleFormat = next.SampleFormat
	}
	base.Realtime = base.Realtime || next.Realtime
	return base
}

func streamCapsEmpty(caps StreamCaps) bool {
	return caps.Domain == "" &&
		caps.MediaKind == "" &&
		caps.StreamID == "" &&
		caps.Codec == "" &&
		caps.Format == "" &&
		caps.Width == 0 &&
		caps.Height == 0 &&
		caps.PixelFormat == "" &&
		caps.SampleRate == 0 &&
		caps.Channels == 0 &&
		caps.SampleFormat == "" &&
		!caps.Realtime
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
