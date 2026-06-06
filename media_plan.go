package goav

import (
	"fmt"

	"github.com/thesyncim/goav/av"
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
	OpTee         OperationKind = "tee"
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
	Operations []planOperation
	Outputs    []string
}

type planOperation struct {
	Kind      OperationKind
	Component string
	Detail    string
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
	plan.Outputs = planOutputs(intent.Outputs, state.outputFormatMap())
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

func planOutputs(outputs []OutputIntent, formats map[string]av.FormatID) []planOutput {
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
		branchName := firstNonEmpty(stream.Name, string(stream.Select.Type), fmt.Sprintf("branch-%d", i))
		steps := state.streamSteps
		if len(state.intent.Streams) > 1 {
			steps = nil
		}
		operations, branchDecisions := planStreamOperations(state.intent.Inputs, stream, branchName, steps)
		branches = append(branches, planBranch{
			Name:       branchName,
			Input:      firstInputName(state.intent.Inputs),
			Stream:     stream.Select,
			Operations: operations,
			Outputs:    planBranchOutputs(stream.RouteTo, outputs),
		})
		decisions = append(decisions, branchDecisions...)
	}
	return branches, decisions
}

func planCopyBranches(intent Intent, outputs []planOutput) ([]planBranch, []planDecision) {
	branches := make([]planBranch, 0, len(intent.Inputs))
	decisions := make([]planDecision, 0, len(intent.Inputs))
	outputNames := planOutputNames(outputs)
	for i := range intent.Inputs {
		input := intent.Inputs[i]
		name := firstNonEmpty(input.Name, input.URI, fmt.Sprintf("input-%d", i))
		operations := planInputOperations(input)
		operations = append(operations, planOperation{
			Kind:      OpCopy,
			Component: "packet-copy",
			Detail:    "preserve encoded packets",
		})
		branches = append(branches, planBranch{
			Name:       firstNonEmpty(name, fmt.Sprintf("copy-%d", i)),
			Input:      name,
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

func planStreamOperations(inputs []InputIntent, stream StreamIntent, branchName string, steps []jobStreamStepAttachment) ([]planOperation, []planDecision) {
	operations := planInputOperations(firstInput(inputs))
	operations = append(operations, planOperation{
		Kind:      OpSelect,
		Component: selectorComponent(stream.Select),
		Detail:    "select stream",
	})
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

func planInputOperations(input InputIntent) []planOperation {
	switch {
	case input.Protocol == av.ProtocolRTP || input.Protocol == av.ProtocolWebRTC || input.Realtime:
		component := firstNonEmpty(string(input.Codec.ID), string(input.Protocol), "rtp")
		return []planOperation{{
			Kind:      OpDepacketize,
			Component: component,
			Detail:    "receive RTP packets",
		}}
	default:
		return []planOperation{{
			Kind:      OpDemux,
			Component: "container",
			Detail:    "read packets from input",
		}}
	}
}

func planProcessingOperations(stream StreamIntent, steps []jobStreamStepAttachment) []planOperation {
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
	}
}

func planTapOperation(tap TapIntent) planOperation {
	return planOperation{
		Kind:      OpTap,
		Component: firstNonEmpty(tap.Name, "tap"),
		Detail:    "named media outlet",
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
		currentDomain := DomainPacket
		media := branch.Stream.Type
		currentNode := firstNonEmpty(branch.Input, branch.Name)
		for j := range branch.Operations {
			operation := branch.Operations[j]
			switch operation.Kind {
			case OpDecode, OpTransform, OpStage:
				currentDomain = DomainFrame
			case OpEncode, OpCopy:
				currentDomain = DomainPacket
			}
			if operation.Kind != OpTap {
				currentNode = planOperationNodeName(branch.Name, operation, j)
				continue
			}
			name := operation.Component
			taps = append(taps, planTap{
				Name:      name,
				Node:      pipeline.NodeRef(currentNode),
				Domain:    currentDomain,
				MediaKind: media,
				Caps: StreamCaps{
					Domain:    currentDomain,
					MediaKind: media,
				},
				Shared: true,
			})
		}
	}
	return taps
}

func planOperationNodeName(branch string, operation planOperation, index int) string {
	switch operation.Kind {
	case OpDecode:
		return "decode-" + branch
	case OpTransform:
		return operation.Component + "-" + branch
	case OpStage:
		return operation.Component
	case OpEncode:
		return "encode-" + branch
	case OpSelect:
		return "select-" + branch
	case OpDepacketize, OpDemux:
		return branch
	default:
		if operation.Component != "" {
			return operation.Component
		}
		return fmt.Sprintf("%s-op-%d", branch, index)
	}
}

func planBranchOutputs(routeTo []string, outputs []planOutput) []string {
	if len(routeTo) != 0 {
		return append([]string(nil), routeTo...)
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
