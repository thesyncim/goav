package compile

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// planBranch, planOperation, and planOutput are the planner's per-branch
// working set: the branch planners lower the intent into these intermediates
// and composeWorkPlan flattens them into the workPlan — the single plan the
// compile emits. They are never stored or rendered after composition.
type PlanBranch struct {
	Name       string
	Input      string
	Stream     plan.StreamSelect
	Shape      shape.Spec
	Operations []PlanOperation
	Outputs    []string
}

type PlanOperation struct {
	Kind      plan.OperationKind
	Component string
	Detail    string
	Codec     codec.Spec
	After     plan.OperationKind
	Shape     shape.Spec
	Shared    bool
	// Node is the planned node the operation runs as; ShapeIn, when set,
	// the shape it reads.
	Node    pipeline.NodeRef
	ShapeIn shape.Spec
	// graphNode is the recipe graph node the operation came from.
	graphNode    recipeir.NodeID
	hasGraphNode bool
}

type PlanOutput struct {
	Name       string
	Operation  plan.OperationKind
	Component  string
	Format     av.FormatID
	BranchRefs []string
}

type PlanDecision struct {
	Code    string
	Branch  string
	Message string
}

func planOutputsFromRecipeIR(outputs []recipeir.Destination, formats map[string]av.FormatID) []PlanOutput {
	out := make([]PlanOutput, 0, len(outputs))
	for i := range outputs {
		output := outputs[i]
		name := cmp.Or(output.Name, output.URI, fmt.Sprintf("output-%d", i))
		formatID := output.Format
		if formats != nil && formats[name] != "" {
			formatID = formats[name]
		}
		operation := plan.OpSink
		if output.URI != "" || output.Protocol != "" || output.MIMEType != "" || formatID != "" {
			operation = plan.OpMux
		}
		out = append(out, PlanOutput{
			Name:      name,
			Operation: operation,
			Component: cmp.Or(string(formatID), string(output.Protocol), output.MIMEType, "sink"),
			Format:    formatID,
		})
	}
	return out
}

func StreamSelectFromStream(stream av.Stream) plan.StreamSelect {
	return plan.StreamSelect{
		ID:    stream.ID,
		Index: stream.Index,
		Type:  stream.Type,
		Codec: stream.Codec.ID,
		Name:  stream.Name,
	}
}

func planCopyBranches(state *State, inputs []recipeir.Input, copyRequested bool, outputs []PlanOutput) ([]PlanBranch, []PlanDecision) {
	branches := make([]PlanBranch, 0, len(inputs))
	decisions := make([]PlanDecision, 0, len(inputs))
	outputNames := planOutputNames(outputs)
	for i := range inputs {
		input := inputs[i]
		name := cmp.Or(input.Name, input.URI, fmt.Sprintf("input-%d", i))
		spec := mediaShapeFromInput(input, shape.DomainPacket)
		if sourceShape, ok := recipeIRInputSourceShape(input); ok {
			spec = shape.Merge(spec, sourceShape)
		}
		operations := planInputOperationsForShape(input, spec)
		copyDetail := "preserve encoded packets"
		copyMessage := "no decode, transform, or encode requested; packets are copied to outputs"
		if copyRequested {
			copyDetail = "explicit packet copy"
			copyMessage = "packet copy requested; packets are copied to outputs"
		}
		decision := PlanDecision{
			Code:    diagnosticPacketCopy,
			Branch:  name,
			Message: copyMessage,
		}
		if spec.Domain == shape.DomainEvent {
			operations = append(operations, PlanOperation{
				Kind:      plan.OpShape,
				Component: "shape",
				Detail:    "event source",
				Shape:     spec,
			})
			decision = PlanDecision{
				Code:    diagnosticEventSource,
				Branch:  name,
				Message: "source produces events for sink destinations",
			}
		} else {
			operations = append(operations, PlanOperation{
				Kind:      plan.OpCopy,
				Component: "packet-copy",
				Detail:    copyDetail,
			})
		}
		operations = planOperationsWithShape(name, spec, operations)
		branches = append(branches, PlanBranch{
			Name:       cmp.Or(name, fmt.Sprintf("copy-%d", i)),
			Input:      name,
			Shape:      spec,
			Operations: operations,
			Outputs:    append([]string(nil), outputNames...),
		})
		decisions = append(decisions, decision)
	}
	return branches, decisions
}

func planRecipeIROperations(input recipeir.Input, stream recipeir.Stream, branchName string, initial shape.Spec, selectComponent string) ([]PlanOperation, []PlanDecision) {
	operations := planInputOperationsForShape(input, initial)
	if initial.Domain == shape.DomainEvent && stream.Selector == (plan.StreamSelect{}) && len(stream.Operations) == 0 {
		operations = append(operations, PlanOperation{
			Kind:      plan.OpShape,
			Component: "shape",
			Detail:    "event source",
			Shape:     initial,
		})
		return operations, []PlanDecision{{
			Code:    diagnosticEventSource,
			Branch:  branchName,
			Message: "source produces events for sink destinations",
		}}
	}
	operations = append(operations, PlanOperation{
		Kind:      plan.OpSelect,
		Component: cmp.Or(selectComponent, selectorComponent(stream.Selector)),
		Detail:    "select stream",
	})
	if len(stream.Operations) != 0 {
		streamOperations, decisions := planRecipeIRStreamOperations(stream, branchName)
		operations = append(operations, streamOperations...)
		return operations, decisions
	}
	var decisions []PlanDecision
	if initial.Domain == shape.DomainFrame {
		decisions = append(decisions, PlanDecision{
			Code:    diagnosticFrameSource,
			Branch:  branchName,
			Message: "source already produces decoded frames",
		})
		return operations, decisions
	}
	operations = append(operations, PlanOperation{
		Kind:      plan.OpCopy,
		Component: "packet-copy",
		Detail:    "no frame operation requested",
	})
	decisions = append(decisions, PlanDecision{
		Code:    diagnosticPacketCopy,
		Branch:  branchName,
		Message: "stream can remain packet encoded",
	})
	return operations, decisions
}

func planRecipeIRStreamOperations(stream recipeir.Stream, branchName string) ([]PlanOperation, []PlanDecision) {
	operations := make([]PlanOperation, 0, len(stream.Operations))
	var decisions []PlanDecision
	for i := range stream.Operations {
		operation := stream.Operations[i]
		operations = append(operations, planOperationFromRecipeIROperation(operation))
	}
	if recipeIROperationKindPresent(stream.Operations, plan.OpDecode) {
		decisions = append(decisions, PlanDecision{
			Code:    diagnosticDecodeRequired,
			Branch:  branchName,
			Message: "operation specs require decoded frames",
		})
	} else if recipeIROperationKindPresent(stream.Operations, plan.OpCopy) {
		decisions = append(decisions, PlanDecision{
			Code:    diagnosticPacketCopy,
			Branch:  branchName,
			Message: "stream can remain packet encoded",
		})
	}
	if recipeIROperationKindPresent(stream.Operations, plan.OpEncode) {
		decisions = append(decisions, PlanDecision{
			Code:    diagnosticEncodeRequired,
			Branch:  branchName,
			Message: "muxed stream output requires encoded packets",
		})
	}
	return operations, decisions
}

func planOperationFromRecipeIROperation(operation recipeir.Operation) PlanOperation {
	out := planOperationOf(operation)
	out.graphNode, out.hasGraphNode = operation.Node, true
	return out
}

func planOperationOf(operation recipeir.Operation) PlanOperation {
	switch operation.Kind {
	case plan.OpTransform:
		op := planRecipeIRTransformOperation(operation.Transform)
		op.Shared = operation.Shared
		op.Component = cmp.Or(operation.Component, op.Component)
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
		return PlanOperation{
			Kind:      plan.OpShape,
			Component: cmp.Or(operation.Component, "shape"),
			Detail:    detail,
			Shape:     operation.Shape,
			Shared:    operation.Shared,
		}
	case plan.OpTap:
		op := planRecipeIRTapOperation(operation.Tap)
		op.Shared = operation.Shared
		return op
	case plan.OpEncode:
		return PlanOperation{
			Kind:      plan.OpEncode,
			Component: string(operation.Encode.ID),
			Detail:    "frames to packets",
			Codec:     CloneCodecSpec(operation.Encode),
			Shape:     MediaShapeFromCodecSpec(operation.Encode, shape.DomainPacket),
			Shared:    operation.Shared,
		}
	case plan.OpDecode:
		return PlanOperation{
			Kind:      plan.OpDecode,
			Component: cmp.Or(string(operation.Decode.ID), operation.Component),
			Detail:    "packets to frames",
			Codec:     CloneCodecSpec(operation.Decode),
			Shared:    operation.Shared,
		}
	case plan.OpStage:
		return PlanOperation{
			Kind:      plan.OpStage,
			Component: operation.Component,
			Detail:    "custom stage",
			Shared:    operation.Shared,
		}
	case plan.OpCopy:
		return PlanOperation{
			Kind:      plan.OpCopy,
			Component: cmp.Or(operation.Component, "packet-copy"),
			Detail:    cmp.Or(operation.Detail, "no frame operation requested"),
			Codec:     CloneCodecSpec(operation.Encode),
			Shared:    operation.Shared,
		}
	default:
		return PlanOperation{
			Kind:      operation.Kind,
			Component: operation.Component,
			Shared:    operation.Shared,
		}
	}
}

func recipeIROperationKindPresent(operations []recipeir.Operation, kind plan.OperationKind) bool {
	for i := range operations {
		if operations[i].Kind == kind {
			return true
		}
	}
	return false
}

func planInputOperationsForShape(input recipeir.Input, spec shape.Spec) []PlanOperation {
	if input.Protocol == av.ProtocolCustom {
		return nil
	}
	if spec.Domain == shape.DomainFrame || spec.Domain == shape.DomainEvent {
		return nil
	}
	switch {
	case input.Realtime:
		component := cmp.Or(string(input.Codec.ID), string(input.Protocol), "receive")
		return []PlanOperation{{
			Kind:      plan.OpDepacketize,
			Component: component,
			Detail:    "receive live packets",
			Shape:     mediaShapeFromInput(input, firstNonEmptyDomain(spec.Domain, shape.DomainPacket)),
		}}
	default:
		return []PlanOperation{{
			Kind:      plan.OpDemux,
			Component: "container",
			Detail:    "read packets from input",
			Shape:     mediaShapeFromInput(input, firstNonEmptyDomain(spec.Domain, shape.DomainPacket)),
		}}
	}
}

func planRecipeIRTransformOperation(transform recipeir.Transform) PlanOperation {
	name := RecipeIRTransformFactoryName(transform)
	return PlanOperation{
		Kind:      plan.OpTransform,
		Component: cmp.Or(name, "transform"),
		Detail:    cmp.Or(name, "transform frames"),
		Shape:     MediaShapeFromRecipeIRTransform(transform),
	}
}

func planRecipeIRTapOperation(tap recipeir.Tap) PlanOperation {
	return PlanOperation{
		Kind:      plan.OpTap,
		Component: cmp.Or(tap.Name, "tap"),
		Detail:    "named media outlet",
		After:     tap.After,
	}
}

func planTaps(branches []PlanBranch) []WorkTap {
	var taps []WorkTap
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
			currentShape.StreamID = av.StreamID(cmp.Or(string(branch.Stream.ID), branch.Name))
		}
		if currentShape.Codec == "" {
			currentShape.Codec = branch.Stream.Codec
		}
		currentNode := cmp.Or(branch.Input, branch.Name)
		for j := range branch.Operations {
			operation := branch.Operations[j]
			// Copy and shape annotations lower to no dedicated node: they advance
			// the shape but never move the tap anchor.
			if operation.Kind == plan.OpCopy || operation.Kind == plan.OpShape {
				currentShape = PlanShapeForOperation(currentShape, branch, operation)
				continue
			}
			if operation.Kind != plan.OpTap {
				currentShape = PlanShapeForOperation(currentShape, branch, operation)
				// The tap anchors on the node the lowering planned for the work before it.
				if operation.Node != "" {
					currentNode = string(operation.Node)
				}
				continue
			}
			name := operation.Component
			tapShape := operation.Shape
			if MediaShapeEmpty(tapShape) {
				tapShape = currentShape
			}
			tapShape = NormalizeTapShape(tapShape)
			taps = append(taps, WorkTap{
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

func planOperationsWithShape(branchName string, baseShape shape.Spec, operations []PlanOperation) []PlanOperation {
	if len(operations) == 0 {
		return nil
	}
	out := append([]PlanOperation(nil), operations...)
	spec := NormalizeTapShape(baseShape)
	branch := PlanBranch{Name: branchName}
	for i := range out {
		operation := out[i]
		if operation.Kind == plan.OpTap {
			if MediaShapeEmpty(operation.Shape) {
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

func PlanShapeForOperation(current shape.Spec, branch PlanBranch, operation PlanOperation) shape.Spec {
	if !MediaShapeEmpty(operation.Shape) {
		return operation.Shape
	}
	return planShapeAfterOperation(current, branch, operation)
}

func planShapeAfterOperation(spec shape.Spec, branch PlanBranch, operation PlanOperation) shape.Spec {
	switch operation.Kind {
	case plan.OpDepacketize:
		if codecID := knownPlanCodec(operation.Component); codecID != "" {
			spec.Codec = codecID
			spec.MediaKind = FirstNonEmptyMedia(spec.MediaKind, CodecMedia(codecID))
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
		if !operation.Shared {
			// A branch's own encoder publishes under its name; one shared by
			// several branches keeps the stream's id.
			spec.StreamID = av.StreamID(cmp.Or(branch.Name, string(spec.StreamID)))
		}
		spec.Codec = FirstNonEmptyCodec(operation.Codec.ID, av.CodecID(operation.Component))
		if media := CodecMedia(spec.Codec); media != "" {
			spec.MediaKind = media
		}
		spec = shape.Merge(spec, operation.Shape)
	}
	return spec
}

func NormalizeTapShape(spec shape.Spec) shape.Spec {
	if spec.Domain == "" {
		spec.Domain = shape.DomainPacket
	}
	return spec
}

func mediaShapeFromInput(input recipeir.Input, domain shape.MediaDomain) shape.Spec {
	spec := shape.Spec{
		Domain:    domain,
		MediaKind: input.Codec.Type,
		StreamID:  av.StreamID(input.Name),
		Codec:     input.Codec.ID,
		Realtime:  input.Realtime,
	}
	spec = shape.Merge(spec, shape.FromCodecParameters(input.Codec.Parameters))
	if spec.MediaKind == "" {
		spec.MediaKind = CodecMedia(spec.Codec)
	}
	return spec
}

func MediaShapeFromRecipeIRTransform(transform recipeir.Transform) shape.Spec {
	switch transform.Kind {
	case recipeir.TransformResize:
		return shape.Spec{
			Domain:      shape.DomainFrame,
			MediaKind:   av.MediaVideo,
			Width:       transform.Resize.Width,
			Height:      transform.Resize.Height,
			PixelFormat: transform.Resize.PixelFormat,
		}
	case recipeir.TransformResample:
		return shape.Spec{
			Domain:       shape.DomainFrame,
			MediaKind:    av.MediaAudio,
			SampleRate:   transform.Resample.SampleRate,
			Channels:     transform.Resample.Channels,
			SampleFormat: transform.Resample.SampleFormat,
		}
	default:
		return shape.Spec{}
	}
}

func MediaShapeEmpty(shape shape.Spec) bool {
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

func FirstNonEmptyCodec(values ...av.CodecID) av.CodecID {
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
	if CodecMedia(codecID) == "" {
		return ""
	}
	return codecID
}

func PlanBranchDestinations(targetRefs []string, outputs []PlanOutput) []string {
	if len(targetRefs) != 0 {
		return append([]string(nil), targetRefs...)
	}
	return planOutputNames(outputs)
}

func planOutputNames(outputs []PlanOutput) []string {
	names := make([]string, 0, len(outputs))
	for i := range outputs {
		names = append(names, outputs[i].Name)
	}
	return names
}

func clonePlanDecisions(decisions []PlanDecision) []PlanDecision {
	if len(decisions) == 0 {
		return nil
	}
	return append([]PlanDecision(nil), decisions...)
}

func ClonePlanDiagnostics(diagnostics []plan.Diagnostic) []plan.Diagnostic {
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

func selectorComponent(selector plan.StreamSelect) string {
	return cmp.Or(string(selector.ID), selector.Name, string(selector.Type), string(selector.Codec), "stream")
}

func codecComponent(codecID av.CodecID) string {
	return cmp.Or(string(codecID), "decoder")
}

func stringInSlice(needle string, haystack []string) bool {
	return slices.Contains(haystack, needle)
}
