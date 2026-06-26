package goav

import (
	"fmt"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// planBranch, planOperation, and planOutput are the planner's per-branch
// working set: the branch planners lower the intent into these intermediates
// and composeWorkPlan flattens them into the workPlan — the single plan the
// compile emits. They are never stored or rendered after composition.
type planBranch struct {
	Name       string
	Input      string
	Stream     plan.StreamSelect
	Shape      shape.Spec
	Operations []planOperation
	Outputs    []string
}

type planOperation struct {
	Kind      plan.OperationKind
	Component string
	Detail    string
	Codec     codec.CodecSpec
	After     plan.OperationKind
	Shape     shape.Spec
	Shared    bool
}

type planOutput struct {
	Name       string
	Operation  plan.OperationKind
	Component  string
	Format     av.FormatID
	BranchRefs []string
}

type planDecision struct {
	Code    string
	Branch  string
	Message string
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
		operation := plan.OpSink
		if output.URI != "" || output.Protocol != "" || output.MIMEType != "" || formatID != "" {
			operation = plan.OpMux
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

func planOutputsFromRecipeIR(outputs []recipeir.Destination, formats map[string]av.FormatID) []planOutput {
	if len(outputs) == 0 {
		return nil
	}
	intents := make([]destinationIntent, 0, len(outputs))
	for i := range outputs {
		intents = append(intents, destinationIntentFromRecipeIR(outputs[i]))
	}
	return planOutputs(intents, formats)
}

// planBranchFromStreamIntent plans one branch from a resolved stream intent —
// the single per-branch planning body. Both planning sources (direct streams
// and a branchComposePlan converted via streamIntentsFromBranchComposePlan)
// reach it through planBranchesFromStreamIntents, so the work plan has one
// branch planner (NORTH_STAR step 3).
func planBranchFromStreamIntent(state *recipeCompileState, stream streamIntent, index int, outputs []planOutput) (planBranch, []planDecision) {
	var spec shape.Spec
	sourceShape, sourceShapeOK := jobStreamCustomSourceShape(state, stream)
	input, inputName := planStreamInputBinding(state, stream)
	// The select node is named from the DECLARED selector (the chain's scope,
	// e.g. select-audio), exactly like the planned graph spec — stream
	// resolution below enriches shapes and reports, never node names.
	selectComponent := selectorComponent(stream.Select)
	if selected, ok := planSelectedStream(state, stream); ok {
		resolved := streamSelectFromStream(selected)
		resolved.Input = stream.Select.Input
		stream.Select = resolved
		domain := shape.DomainPacket
		if sourceShapeOK && sourceShape.Domain != "" {
			domain = sourceShape.Domain
		}
		spec = mediaShapeFromPlanStream(selected, domain)
		if sourceShapeOK {
			spec = shape.Merge(spec, sourceShape)
		}
	}
	spec = normalizePlanBranchShape(spec, stream, input)
	branchName := firstNonEmpty(stream.Name, string(stream.Select.Type), fmt.Sprintf("branch-%d", index))
	operations, branchDecisions := planOperationSpecs(input, stream, branchName, spec, selectComponent)
	operations = planOperationsWithShape(branchName, spec, operations)
	return planBranch{
		Name:       branchName,
		Input:      inputName,
		Stream:     stream.Select,
		Shape:      spec,
		Operations: operations,
		Outputs:    planBranchDestinations(stream.Destinations, outputs),
	}, branchDecisions
}

// planStreamInputBinding resolves which job input feeds one stream chain: the
// first (and only) input on single-input recipes, or the input the chain's
// selector binds to — honoring goav.InputName narrowing — on multi-input jobs.
func planStreamInputBinding(state *recipeCompileState, stream streamIntent) (inputIntent, string) {
	if state == nil {
		return inputIntent{}, "input"
	}
	inputs := state.intent.Inputs
	if len(inputs) <= 1 {
		return firstInput(inputs), firstInputName(inputs)
	}
	sets := jobInputStreamSetsFromRecipeIR(inputs, state.inputFacts, state.inputProbes)
	if index, ok := resolveInputSetIndex(sets, streamIntentSelector(stream), stream.Select.Input); ok && index < len(inputs) {
		return inputs[index], sets[index].name
	}
	return firstInput(inputs), firstInputName(inputs)
}

// planStreamInput keeps the single-value form for callers that only need the
// resolved input intent.
func planStreamInput(state *recipeCompileState, stream streamIntent) inputIntent {
	input, _ := planStreamInputBinding(state, stream)
	return input
}

func planBranches(state *recipeCompileState, outputs []planOutput) ([]planBranch, []planDecision) {
	if len(state.intent.Streams) == 0 {
		return planCopyBranches(state, outputs)
	}
	return planBranchesFromStreamIntents(state, state.intent.Streams, outputs)
}

func planBranchesFromRecipeIR(state *recipeCompileState, recipe recipeir.Recipe, outputs []planOutput) ([]planBranch, []planDecision) {
	if len(recipe.Streams) == 0 {
		return planCopyBranches(state, outputs)
	}
	return planBranchesFromStreamIntents(state, streamIntentsFromRecipeIR(recipe.Streams), outputs)
}

func streamIntentsFromRecipeIR(streams []recipeir.Stream) []streamIntent {
	if len(streams) == 0 {
		return nil
	}
	out := make([]streamIntent, 0, len(streams))
	for i := range streams {
		out = append(out, streamIntentFromRecipeIR(streams[i]))
	}
	return out
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

func streamSelectFromStream(stream av.Stream) plan.StreamSelect {
	return plan.StreamSelect{
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
	if jobStreamSelectionNeedsUnion(state, stream) {
		sets := jobInputStreamSetsFromRecipeIR(state.intent.Inputs, state.inputFacts, state.inputProbes)
		selected, ok, err := selectStreamAcrossInputSets(sets, streamIntentSelector(stream), stream.Select.Input)
		if err != nil || !ok {
			return av.Stream{}, false
		}
		return selected.stream, true
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
		if sourceShapeOK && sourceShape.Domain == shape.DomainFrame {
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
	// Declared sources expose their stream shape statically. Compiles without
	// preflight probes (Describe-time) resolve from the declaration, so the
	// shape solver sees the same facts in Describe and Build.
	for i := range state.inputFacts {
		if spec, ok := recipeIRInputSourceShape(state.inputFacts[i]); ok {
			selected, err := selectStream([]av.Stream{recipeIRInputDeclaredStream(state.inputFacts[i], spec)}, selector)
			if err == nil {
				return selected, true
			}
		}
	}
	if state.branchCompositionPresent {
		if declared := declaredSourceStreams(state.branchInputAttachment); len(declared) != 0 {
			selected, err := selectStream(declared, selector)
			if err == nil {
				return selected, true
			}
		}
	} else if len(state.inputFacts) == 0 {
		for i := range state.inputAttachments {
			declared := declaredSourceStreams(state.inputAttachments[i])
			if len(declared) == 0 {
				continue
			}
			selected, err := selectStream(declared, selector)
			if err == nil {
				return selected, true
			}
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
		spec := mediaShapeFromInputIntent(input, shape.DomainPacket)
		if sourceShape, ok := state.inputSourceShape(i); ok {
			spec = shape.Merge(spec, sourceShape)
		}
		operations := planInputOperationsForShape(input, spec)
		copyDetail := "preserve encoded packets"
		copyMessage := "no decode, transform, or encode requested; packets are copied to outputs"
		if intent.Copy {
			copyDetail = "explicit packet copy"
			copyMessage = ".Copy requested; packets are copied to outputs"
		}
		decision := planDecision{
			Code:    diagnosticPacketCopy,
			Branch:  name,
			Message: copyMessage,
		}
		if spec.Domain == shape.DomainEvent {
			operations = append(operations, planOperation{
				Kind:      plan.OpShape,
				Component: "shape",
				Detail:    "event source",
				Shape:     spec,
			})
			decision = planDecision{
				Code:    diagnosticEventSource,
				Branch:  name,
				Message: "source produces events for sink destinations",
			}
		} else {
			operations = append(operations, planOperation{
				Kind:      plan.OpCopy,
				Component: "packet-copy",
				Detail:    copyDetail,
			})
		}
		operations = planOperationsWithShape(name, spec, operations)
		branches = append(branches, planBranch{
			Name:       firstNonEmpty(name, fmt.Sprintf("copy-%d", i)),
			Input:      name,
			Shape:      spec,
			Operations: operations,
			Outputs:    append([]string(nil), outputNames...),
		})
		decisions = append(decisions, decision)
	}
	return branches, decisions
}

func planOperationSpecs(input inputIntent, stream streamIntent, branchName string, initial shape.Spec, selectComponent string) ([]planOperation, []planDecision) {
	operations := planInputOperationsForShape(input, initial)
	if initial.Domain == shape.DomainEvent && stream.Select == (plan.StreamSelect{}) && len(stream.Operations) == 0 {
		operations = append(operations, planOperation{
			Kind:      plan.OpShape,
			Component: "shape",
			Detail:    "event source",
			Shape:     initial,
		})
		return operations, []planDecision{{
			Code:    diagnosticEventSource,
			Branch:  branchName,
			Message: "source produces events for sink destinations",
		}}
	}
	operations = append(operations, planOperation{
		Kind:      plan.OpSelect,
		Component: firstNonEmpty(selectComponent, selectorComponent(stream.Select)),
		Detail:    "select stream",
	})
	if len(stream.Operations) != 0 {
		operationSpecs, decisions := planStreamIntentOperations(stream, branchName)
		operations = append(operations, operationSpecs...)
		return operations, decisions
	}
	// No operations were declared, so there is nothing to decode, transform,
	// encode, or tap: frame sources flow through, packet sources stay copied.
	var decisions []planDecision
	if initial.Domain == shape.DomainFrame {
		decisions = append(decisions, planDecision{
			Code:    diagnosticFrameSource,
			Branch:  branchName,
			Message: "source already produces decoded frames",
		})
		return operations, decisions
	}
	operations = append(operations, planOperation{
		Kind:      plan.OpCopy,
		Component: "packet-copy",
		Detail:    "no frame operation requested",
	})
	decisions = append(decisions, planDecision{
		Code:    diagnosticPacketCopy,
		Branch:  branchName,
		Message: "stream can remain packet encoded",
	})
	return operations, decisions
}

func planStreamIntentOperations(stream streamIntent, branchName string) ([]planOperation, []planDecision) {
	operations := make([]planOperation, 0, len(stream.Operations))
	var decisions []planDecision
	for i := range stream.Operations {
		operation := stream.Operations[i]
		operations = append(operations, planOperationFromOperationSpec(operation))
	}
	if operationSpecKindPresent(stream.Operations, plan.OpDecode) {
		decisions = append(decisions, planDecision{
			Code:    diagnosticDecodeRequired,
			Branch:  branchName,
			Message: "operation specs require decoded frames",
		})
	} else if operationSpecKindPresent(stream.Operations, plan.OpCopy) {
		decisions = append(decisions, planDecision{
			Code:    diagnosticPacketCopy,
			Branch:  branchName,
			Message: "stream can remain packet encoded",
		})
	}
	if operationSpecKindPresent(stream.Operations, plan.OpEncode) {
		decisions = append(decisions, planDecision{
			Code:    diagnosticEncodeRequired,
			Branch:  branchName,
			Message: "muxed stream output requires encoded packets",
		})
	}
	return operations, decisions
}

func planOperationFromOperationSpec(operation operationSpec) planOperation {
	switch operation.Kind {
	case plan.OpTransform:
		op := planTransformOperation(operation.Transform)
		op.Shared = operation.Shared
		// The operation may carry a solver-selected adapter that differs from
		// the standard factory; the component names the node and the registry key.
		op.Component = firstNonEmpty(operation.Component, op.Component)
		return op
	case plan.OpShape:
		detail := "media shape annotation"
		switch {
		case operation.Auto != nil:
			detail = "shape solver policy"
		case operation.Require != nil:
			detail = "shape requirement"
		case operation.Prefer != nil:
			detail = "shape preference"
		}
		return planOperation{
			Kind:      plan.OpShape,
			Component: firstNonEmpty(operation.Component, "shape"),
			Detail:    detail,
			Shape:     operation.Shape,
			Shared:    operation.Shared,
		}
	case plan.OpTap:
		op := planTapOperation(operation.Tap)
		op.Shared = operation.Shared
		return op
	case plan.OpEncode:
		return planOperation{
			Kind:      plan.OpEncode,
			Component: string(operation.Encode.ID),
			Detail:    "frames to packets",
			Codec:     cloneCodecSpec(operation.Encode),
			Shape:     mediaShapeFromCodecSpec(operation.Encode, shape.DomainPacket),
			Shared:    operation.Shared,
		}
	case plan.OpDecode:
		return planOperation{
			Kind:      plan.OpDecode,
			Component: firstNonEmpty(string(operation.Decode.ID), operation.Component),
			Detail:    "packets to frames",
			Codec:     cloneCodecSpec(operation.Decode),
			Shared:    operation.Shared,
		}
	case plan.OpStage:
		return planOperation{
			Kind:      plan.OpStage,
			Component: operation.Component,
			Detail:    "custom stage",
			Shared:    operation.Shared,
		}
	case plan.OpCopy:
		return planOperation{
			Kind:      plan.OpCopy,
			Component: firstNonEmpty(operation.Component, "packet-copy"),
			Detail:    firstNonEmpty(operation.Detail, "no frame operation requested"),
			Codec:     cloneCodecSpec(operation.Encode),
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

func operationSpecKindPresent(operations []operationSpec, kind plan.OperationKind) bool {
	for i := range operations {
		if operations[i].Kind == kind {
			return true
		}
	}
	return false
}

func planInputOperationsForShape(input inputIntent, spec shape.Spec) []planOperation {
	if input.Protocol == av.ProtocolCustom {
		return nil
	}
	if spec.Domain == shape.DomainFrame || spec.Domain == shape.DomainEvent {
		return nil
	}
	switch {
	case input.Realtime:
		component := firstNonEmpty(string(input.Codec.ID), string(input.Protocol), "receive")
		return []planOperation{{
			Kind:      plan.OpDepacketize,
			Component: component,
			Detail:    "receive live packets",
			Shape:     mediaShapeFromInputIntent(input, firstNonEmptyDomain(spec.Domain, shape.DomainPacket)),
		}}
	default:
		return []planOperation{{
			Kind:      plan.OpDemux,
			Component: "container",
			Detail:    "read packets from input",
			Shape:     mediaShapeFromInputIntent(input, firstNonEmptyDomain(spec.Domain, shape.DomainPacket)),
		}}
	}
}

func planTransformOperation(transform transformSpec) planOperation {
	name := transformFactoryName(transform)
	return planOperation{
		Kind:      plan.OpTransform,
		Component: firstNonEmpty(name, "transform"),
		Detail:    firstNonEmpty(name, "transform frames"),
		Shape:     mediaShapeFromTransform(transform),
	}
}

func planTapOperation(tap tapIntent) planOperation {
	return planOperation{
		Kind:      plan.OpTap,
		Component: firstNonEmpty(tap.Name, "tap"),
		Detail:    "named media outlet",
		After:     tap.After,
	}
}

func planTaps(branches []planBranch) []workTap {
	var taps []workTap
	for i := range branches {
		branch := branches[i]
		currentShape := branch.Shape
		if currentShape.Domain == "" {
			currentShape.Domain = shape.DomainPacket
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
			// Copy and shape annotations lower to no dedicated node: they advance
			// the shape but never move the tap anchor.
			if operation.Kind == plan.OpCopy || operation.Kind == plan.OpShape {
				currentShape = planShapeForOperation(currentShape, branch, operation)
				continue
			}
			if operation.Kind != plan.OpTap {
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
			taps = append(taps, workTap{
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

func planOperationsWithShape(branchName string, baseShape shape.Spec, operations []planOperation) []planOperation {
	if len(operations) == 0 {
		return nil
	}
	out := append([]planOperation(nil), operations...)
	spec := normalizeTapShape(baseShape)
	branch := planBranch{Name: branchName}
	for i := range out {
		operation := out[i]
		if operation.Kind == plan.OpTap {
			if mediaShapeEmpty(operation.Shape) {
				operation.Shape = spec
			}
			out[i] = operation
			continue
		}
		spec = planShapeAfterOperation(spec, branch, operation)
		operation.Shape = spec
		out[i] = operation
	}
	return out
}

func planShapeForOperation(current shape.Spec, branch planBranch, operation planOperation) shape.Spec {
	if !mediaShapeEmpty(operation.Shape) {
		return operation.Shape
	}
	return planShapeAfterOperation(current, branch, operation)
}

func planShapeAfterOperation(spec shape.Spec, branch planBranch, operation planOperation) shape.Spec {
	switch operation.Kind {
	case plan.OpDepacketize:
		if codecID := knownPlanCodec(operation.Component); codecID != "" {
			spec.Codec = codecID
			spec.MediaKind = firstNonEmptyMedia(spec.MediaKind, codecMedia(codecID))
		}
		spec.Domain = shape.DomainPacket
	case plan.OpDecode:
		spec.Domain = shape.DomainFrame
	case plan.OpStage:
		// Custom stages are shape pass-through unless they carried an explicit
		// shape contract upstream. Sync gates are packet/frame agnostic and must
		// not accidentally turn packet-copy recording into frame-domain media.
	case plan.OpShape:
		spec = shape.Merge(spec, operation.Shape)
	case plan.OpTransform:
		spec.Domain = shape.DomainFrame
		spec = shape.Merge(spec, operation.Shape)
	case plan.OpCopy:
		spec.Domain = shape.DomainPacket
	case plan.OpEncode:
		spec.Domain = shape.DomainPacket
		spec.StreamID = av.StreamID(firstNonEmpty(branch.Name, string(spec.StreamID)))
		spec.Codec = firstNonEmptyCodec(operation.Codec.ID, av.CodecID(operation.Component))
		if media := codecMedia(spec.Codec); media != "" {
			spec.MediaKind = media
		}
		spec = shape.Merge(spec, operation.Shape)
	}
	return spec
}

func normalizeTapShape(spec shape.Spec) shape.Spec {
	if spec.Domain == "" {
		spec.Domain = shape.DomainPacket
	}
	return spec
}

func normalizePlanBranchShape(spec shape.Spec, stream streamIntent, input inputIntent) shape.Spec {
	inputShape := mediaShapeFromCodecSpec(input.Codec, firstNonEmptyDomain(spec.Domain, shape.DomainPacket))
	inputShape.Realtime = input.Realtime
	spec = shape.Merge(inputShape, spec)
	if spec.Domain == "" {
		spec.Domain = shape.DomainPacket
	}
	if spec.MediaKind == "" {
		spec.MediaKind = firstNonEmptyMedia(stream.Select.Type, chainEncodeSpec(stream.Operations).Type, input.Codec.Type)
	}
	if spec.StreamID == "" {
		spec.StreamID = stream.Select.ID
	}
	if spec.Codec == "" {
		spec.Codec = firstNonEmptyCodec(stream.Select.Codec, input.Codec.ID)
	}
	if input.Realtime {
		spec.Realtime = true
	}
	return spec
}

func mediaShapeFromPlanStream(stream av.Stream, domain shape.MediaDomain) shape.Spec {
	return shape.FromStream(stream, domain)
}

func mediaShapeFromInputIntent(input inputIntent, domain shape.MediaDomain) shape.Spec {
	spec := shape.Spec{
		Domain:    domain,
		MediaKind: input.Codec.Type,
		StreamID:  av.StreamID(input.Name),
		Codec:     input.Codec.ID,
		Realtime:  input.Realtime,
	}
	spec = shape.Merge(spec, shape.FromCodecParameters(input.Codec.Parameters))
	if spec.MediaKind == "" {
		spec.MediaKind = codecMedia(spec.Codec)
	}
	return spec
}

func mediaShapeFromTransform(transform transformSpec) shape.Spec {
	if transform.resize != nil {
		return shape.Spec{
			Domain:      shape.DomainFrame,
			MediaKind:   av.MediaVideo,
			Width:       transform.resize.Width,
			Height:      transform.resize.Height,
			PixelFormat: transform.resize.PixelFormat,
		}
	}
	if transform.resample != nil {
		return shape.Spec{
			Domain:       shape.DomainFrame,
			MediaKind:    av.MediaAudio,
			SampleRate:   transform.resample.SampleRate,
			Channels:     transform.resample.Channels,
			SampleFormat: transform.resample.SampleFormat,
		}
	}
	return shape.Spec{}
}

func mediaShapeEmpty(shape shape.Spec) bool {
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

func firstNonEmptyDomain(values ...shape.MediaDomain) shape.MediaDomain {
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
	case plan.OpDecode:
		return "decode-" + planBranchOperationScopeName(branch)
	case plan.OpTransform:
		name := branchEncodeOwnerName(branch)
		if operation.Shared {
			name = planBranchOperationScopeName(branch)
		}
		return operation.Component + "-" + name
	case plan.OpStage:
		if strings.HasPrefix(operation.Component, "sync-") {
			return syncStageName(planBranchOperationScopeName(branch) + "-" + strings.TrimPrefix(operation.Component, "sync-"))
		}
		return operation.Component
	case plan.OpEncode:
		return "encode-" + branchEncodeOwnerName(branch)
	case plan.OpSelect:
		// Qualified exactly like the planned-spec select node: the declared
		// selector scope plus any goav.InputName narrowing.
		return branchComposeInputNodeName("select-"+firstNonEmpty(operation.Component, planBranchOperationScopeName(branch)), branch.Stream.Input)
	case plan.OpDepacketize, plan.OpDemux:
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

// branchEncodeOwnerName names the owner of a branch's encode node. The implicit
// branch name "main" maps to the stream scope so a single Branch("main")
// composition lowers identically to a direct chain — a direct chain is an
// implicit Branch("main") (NORTH_STAR #2). Explicitly named branches keep their
// name so multiple encode variants of one stream stay disambiguated.
func branchEncodeOwnerName(branch planBranch) string {
	if branch.Name == "main" {
		return planBranchOperationScopeName(branch)
	}
	return branch.Name
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

func clonePlannerBranches(branches []planBranch) []planBranch {
	if len(branches) == 0 {
		return nil
	}
	out := make([]planBranch, 0, len(branches))
	for i := range branches {
		branch := branches[i]
		branch.Operations = clonePlannerOperations(branch.Operations)
		branch.Outputs = append([]string(nil), branch.Outputs...)
		out = append(out, branch)
	}
	return out
}

func clonePlannerOperations(operations []planOperation) []planOperation {
	if len(operations) == 0 {
		return nil
	}
	out := make([]planOperation, 0, len(operations))
	for i := range operations {
		operation := operations[i]
		operation.Codec = cloneCodecSpec(operation.Codec)
		out = append(out, operation)
	}
	return out
}

func clonePlannerOutputs(outputs []planOutput) []planOutput {
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

func clonePlanDiagnostics(diagnostics []plan.Diagnostic) []plan.Diagnostic {
	if len(diagnostics) == 0 {
		return nil
	}
	out := make([]plan.Diagnostic, 0, len(diagnostics))
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

func selectorComponent(selector plan.StreamSelect) string {
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
