package goav

import (
	"context"
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// workPatch is the plan for a runtime attach: the operations, taps, branches,
// destinations, edges, and rollback steps the apply step executes against the
// running graph. It is produced by the attach planner from BranchSpec values
// through the same OperationSpec chain walk the build path lowers, and
// snapshots render from it.
type workPatch struct {
	Name         string
	Operations   []workOperation
	Taps         []workTap
	Branches     []workBranch
	Destinations []workDestination
	Edges        []workEdge
	Rollback     []workRollbackStep
	Diagnostics  []PlanDiagnostic
}

type workRollbackStep struct {
	Action      string
	Node        pipeline.NodeRef
	Destination string
}

// attachComponent is the prepared pipeline component behind one planned patch
// node; the apply step installs it under the node name recorded in the patch.
type attachComponent struct {
	stage pipeline.Stage
	sink  pipeline.Sink
	owned bool
}

// attachStep is the planner's working record for one lowered branch step: the
// patch operation it emits plus the prepared component and destination data
// the pure-data workPatch cannot carry. Steps exist only while a branch is
// being planned — the emitted workPatch is the plan the apply step executes.
type attachStep struct {
	operation   workOperation
	tap         *workTap
	install     bool
	component   attachComponent
	terminal    bool
	destination attachDestination
	stream      av.Stream
}

// attachPlan is the compiled attach group: the workPatch (the plan itself)
// plus the prepared components and per-branch apply ranges that bind the
// pure-data patch back to live pipeline values.
type attachPlan struct {
	work       workPatch
	components map[pipeline.NodeRef]attachComponent
	branches   []attachPlanBranch
}

// attachPlanBranch binds one planned branch to its slice of the patch: the
// anchor it hangs from, its buffer policy, the operation/edge ranges the
// apply step executes in order, the taps it installs, and the prepared stages
// it owns until the graph adopts them.
type attachPlanBranch struct {
	name       string
	from       pipeline.NodeRef
	policy     pipeline.RoutePolicy
	label      string
	buffer     pipeline.BufferPolicy
	operations [2]int
	edges      [2]int
	taps       []TapInfo
	owned      []pipeline.Stage
}

func newAttachPlan() *attachPlan {
	return &attachPlan{components: make(map[pipeline.NodeRef]attachComponent)}
}

// registerBranch records the branch's apply context — anchor, route policy,
// buffer, and the prepared stages it owns — before names are reserved, so a
// later planning failure can still close everything the branch opened.
func (p *attachPlan) registerBranch(spec BranchSpec, from string, steps []attachStep) int {
	policy := spec.source.policy
	if policy == "" {
		policy = pipeline.RouteAll
	}
	var owned []pipeline.Stage
	for i := range steps {
		if steps[i].component.owned && steps[i].component.stage != nil {
			owned = append(owned, steps[i].component.stage)
		}
	}
	p.branches = append(p.branches, attachPlanBranch{
		name:   firstNonEmpty(spec.name, "branch"),
		from:   pipeline.NodeRef(from),
		policy: policy,
		label:  spec.source.label,
		buffer: spec.branchBuffer.PipelinePolicy(),
		owned:  owned,
	})
	return len(p.branches) - 1
}

func (p *attachPlan) closeOwned() {
	for i := range p.branches {
		for _, stage := range p.branches[i].owned {
			markPipelineStageFailed(stage)
			_ = stage.Close()
		}
	}
}

// planAttachBranchSteps lowers one BranchSpec into ordered attach steps by
// walking the canonical operation list once — the same OperationSpec chain the
// build path compiles — opening the stages and destinations each step needs
// against the live anchor shape. Destinations open before any graph mutation;
// on failure every stage the walk owned is closed.
func (t *task) planAttachBranchSteps(ctx context.Context, spec BranchSpec, destinations []attachDestination, anchor TapInfo, group *runtimeAttachGroup) ([]attachStep, error) {
	branchName := firstNonEmpty(spec.name, "branch")
	currentShape := runtimeBranchAnchorShape(anchor)
	if spec.media != "" && currentShape.MediaKind != "" {
		if err := validateChainMedia("attach runtime branch", branchName, currentShape.MediaKind, chainSpec{name: spec.name, media: spec.media}); err != nil {
			return nil, err
		}
	}
	if err := validateAttachBranchShapeContract(spec, destinations, currentShape); err != nil {
		return nil, err
	}
	patchShape := normalizeTapShape(currentShape)
	currentStream := streamFromRuntimeBranchShape(spec.name, currentShape)
	steps := make([]attachStep, 0, len(spec.operations)+len(destinations))
	fail := func(err error) ([]attachStep, error) {
		for i := range steps {
			if steps[i].component.owned && steps[i].component.stage != nil {
				markPipelineStageFailed(steps[i].component.stage)
				_ = steps[i].component.stage.Close()
			}
		}
		return nil, err
	}
	transformIndex := 0
	encoded := false
	copied := false
	for i := range spec.operations {
		operation := spec.operations[i]
		if operation.Kind == "" {
			continue
		}
		step := attachStep{operation: workOperation{
			Kind:    operation.Kind,
			Detail:  attachOperationDetail(operation.Kind),
			ShapeIn: patchShape,
		}}
		out := patchShape
		switch operation.Kind {
		case OpDecode:
			stage, err := t.prepareRuntimeBranchDecode(ctx, spec.name, currentStream, currentShape, operation.Decode)
			if err != nil {
				return fail(err)
			}
			currentShape.Domain = shape.DomainFrame
			step.component = attachComponent{stage: stage, owned: true}
			out = attachStepShape(currentShape, patchShape)
		case OpStage:
			if operation.Stage == nil {
				out = operationSpecOutputShape(patchShape, operation)
				break
			}
			step.component = attachComponent{stage: operation.Stage}
			out = attachStepShape(currentShape, patchShape)
		case OpShape:
			if mediaShapeEmpty(operation.Shape) {
				out = operationSpecOutputShape(patchShape, operation)
				break
			}
			currentShape = shape.Merge(currentShape, operation.Shape)
			out = attachStepShape(currentShape, patchShape)
		case OpTransform:
			if operation.Transform.Resize == nil && operation.Transform.Resample == nil {
				out = operationSpecOutputShape(patchShape, operation)
				break
			}
			if t.runtime == nil {
				return fail(runtimeBranchInvalidError(
					"runtime branch transforms require the standard runtime",
					"build tasks with goav.Default() or goav.New(goav.WithDefaults()) before attaching resize/resample branches",
				))
			}
			if currentShape.Domain != shape.DomainFrame {
				return fail(runtimeBranchInvalidError(
					"runtime branch transforms require a frame tap",
					"attach transform branches with .From(goav.FrameTap(name)) where name is declared after Decode, Resize, Resample, or a frame-stage Tap",
				))
			}
			transformName := transformFactoryName(operation.Transform)
			branchIntent := streamIntent{
				Name:       branchName,
				Select:     StreamSelect{Type: currentStream.Type},
				Operations: []OperationSpec{operation},
			}
			if _, err := t.runtime.filters.Factory(transformName); err != nil {
				return fail(recipeTransformAdapterError("attach runtime branch", branchIntent, transformName, err))
			}
			if desc, err := t.runtime.filters.Descriptor(transformName); err == nil {
				if err := validateTransformAdapterDescriptor("attach runtime branch", branchIntent, operation.Transform, transformName, desc); err != nil {
					return fail(err)
				}
			}
			transform, err := runtimeBranchTransform(spec.name, currentStream, operation.Transform, transformIndex)
			if err != nil {
				return fail(err)
			}
			stage, outputStream, err := (&builder{runtime: t.runtime}).newMediaTransformStage(ctx, transform, currentStream, t.runtime.realtime)
			if err != nil {
				return fail(runtimeBranchTransformError(transform.name, err))
			}
			step.component = attachComponent{stage: stage, owned: true}
			currentStream = outputStream
			currentShape = mediaShapeFromRuntimeBranchStream(outputStream, currentShape)
			out = attachStepShape(currentShape, patchShape)
			transformIndex++
		case OpTap:
			tapName := firstNonEmpty(operation.Tap.Name, operation.Component)
			if tapName == "" {
				out = operationSpecOutputShape(patchShape, operation)
				break
			}
			terminal := operationSpecTapIsTerminalPacket(operation)
			if operation.Tap.Name != "" && !terminal {
				if err := validateTapDomain("attach runtime branch", branchName, TapRef{name: operation.Tap.Name, domain: operation.Tap.Domain}, currentShape.Domain); err != nil {
					return fail(err)
				}
			}
			if terminal && !encoded && !copied {
				out = operationSpecOutputShape(patchShape, operation)
			} else {
				out = attachStepShape(currentShape, patchShape)
			}
			step.tap = &workTap{
				Name:      tapName,
				Domain:    out.Domain,
				MediaKind: out.MediaKind,
				After:     operation.Tap.After,
				Shape:     out,
			}
			step.install = operation.Tap.Name != "" && (!terminal || encoded || copied)
		case OpEncode:
			stage, encodedStream, err := t.planAttachEncode(ctx, spec.name, operation.Encode, currentStream, currentShape)
			if err != nil {
				return fail(err)
			}
			step.component = attachComponent{stage: stage, owned: true}
			currentStream = encodedStream
			currentShape = streamPacketShapeFromRuntimeBranchStream(encodedStream, currentShape)
			out = attachStepShape(currentShape, patchShape)
			encoded = true
		case OpCopy:
			if currentShape.Domain != shape.DomainPacket {
				return fail(runtimeBranchCopyDomainError(spec.name, currentShape))
			}
			copied = true
			out = operationSpecOutputShape(patchShape, operation)
		default:
			out = operationSpecOutputShape(patchShape, operation)
		}
		step.operation.Component = attachOperationComponent(operation, step.component.stage)
		step.operation.ShapeOut = out
		patchShape = out
		steps = append(steps, step)
	}

	hasMux := attachDestinationsHaveMux(destinations)
	if hasMux && !encoded && !copied {
		return fail(runtimeBranchEncodeMissingError(spec.name))
	}
	if !hasMux {
		for i := range destinations {
			steps = append(steps, attachSinkDestinationStep(destinations[i], currentStream, patchShape))
		}
		return steps, nil
	}
	if t.runtime == nil {
		return fail(runtimeBranchInvalidError(
			"runtime branch mux destinations require the standard runtime",
			"build tasks with goav.Default() or goav.New(goav.WithDefaults()) before attaching file or URI branches",
		))
	}
	if currentStream.Codec.ID == "" {
		return fail(runtimeBranchMuxCodecMissingError(spec.name, currentShape))
	}
	for i := range destinations {
		destination := destinations[i]
		switch {
		case destination.sink != nil:
			steps = append(steps, attachSinkDestinationStep(destination, currentStream, patchShape))
		case group.isSharedMux(destination.shareKey):
			steps = append(steps, attachSharedMuxDestinationStep(destination, currentStream, patchShape))
		default:
			step, err := t.planAttachMuxDestinationStep(ctx, spec.name, destination, currentStream, patchShape, i)
			if err != nil {
				return fail(err)
			}
			steps = append(steps, step)
		}
	}
	return steps, nil
}

// planAttachEncode opens the branch encoder against the live frame shape with
// the same validation and stage construction the build path uses.
func (t *task) planAttachEncode(ctx context.Context, branchName string, encode codec.CodecSpec, currentStream av.Stream, currentShape shape.Spec) (pipeline.Stage, av.Stream, error) {
	if t.runtime == nil {
		return nil, av.Stream{}, runtimeBranchInvalidError(
			"runtime branch encoding requires the standard runtime",
			"build tasks with goav.Default() or goav.New(goav.WithDefaults()) before attaching encode branches",
		)
	}
	if currentShape.Domain != shape.DomainFrame {
		return nil, av.Stream{}, runtimeBranchEncodeDomainError(branchName, currentShape)
	}
	if err := validateRecipeEncode(encode, "attach runtime branch", firstNonEmpty(branchName, "branch")); err != nil {
		return nil, av.Stream{}, err
	}
	if _, err := t.runtime.codecs.EncoderFactory(encode.ID); err != nil {
		stream := streamIntent{Name: branchName, Encode: encode}
		return nil, av.Stream{}, recipeEncodeAdapterError("attach runtime branch", stream, t.runtime.codecs, err)
	}
	request := runtimeBranchEncodeRequest(branchName, encode, currentStream)
	config, encodedStream, err := prepareEncodeConfig(currentStream, request, t.runtime.realtime)
	if err != nil {
		return nil, av.Stream{}, err
	}
	stream := streamIntent{Name: branchName, Select: StreamSelect{Type: currentStream.Type}, Encode: encode}
	if err := validateEncodeAdapterDescriptors("attach runtime branch", stream, t.runtime.codecs, encodeAdapterRequestFromPreparedStream(encode, encodedStream)); err != nil {
		return nil, av.Stream{}, err
	}
	stage, err := (&builder{runtime: t.runtime}).newEncodeStage(ctx, request, config)
	if err != nil {
		return nil, av.Stream{}, err
	}
	return stage, encodedStream, nil
}

// attachSinkDestinationStep plans a branch sink destination: the sink itself
// is the prepared component and the patch records the destination.
func attachSinkDestinationStep(destination attachDestination, stream av.Stream, current shape.Spec) attachStep {
	return attachStep{
		operation: workOperation{
			Kind:      attachDestinationOperationKind(destination),
			Component: attachDestinationComponent(destination),
			Detail:    "destination",
			ShapeIn:   current,
			ShapeOut:  current,
		},
		component:   attachComponent{sink: destination.sink},
		terminal:    true,
		destination: destination,
		stream:      stream,
	}
}

// attachSharedMuxDestinationStep plans a branch route into a mux destination
// shared across the attach group; the group opens the mux stage once.
func attachSharedMuxDestinationStep(destination attachDestination, stream av.Stream, current shape.Spec) attachStep {
	return attachStep{
		operation: workOperation{
			Kind:      attachDestinationOperationKind(destination),
			Component: attachDestinationComponent(destination),
			Detail:    "destination",
			ShapeIn:   current,
			ShapeOut:  current,
		},
		terminal:    true,
		destination: destination,
		stream:      stream,
	}
}

// planAttachMuxDestinationStep opens a branch-private mux destination before
// any graph mutation, sharing the probe, compatibility, and open helpers with
// the build path and grouped destinations.
func (t *task) planAttachMuxDestinationStep(ctx context.Context, branchName string, destination attachDestination, stream av.Stream, current shape.Spec, index int) (attachStep, error) {
	formatID, err := runtimeMuxDestinationFormat(ctx, t.runtime, destination.dest, index)
	if err != nil {
		return attachStep{}, err
	}
	name := firstNonEmpty(branchName, destination.name, "branch")
	destinationName := firstNonEmpty(destination.name, destination.dest.label(name))
	if issue, ok := runtimeMuxCompatibilityIssue(destinationName, formatID, []string{name}, []av.Stream{stream}, t.runtime); ok {
		return attachStep{}, muxCompatibilityBuildError("attach runtime branch", issue)
	}
	muxStage, err := (&builder{runtime: t.runtime}).openMuxDestinationStage(
		ctx,
		destination.dest,
		index,
		[]av.Stream{stream},
		formatID,
		destinationGraphFormat(destination.dest),
	)
	if err != nil {
		return attachStep{}, err
	}
	return attachStep{
		operation: workOperation{
			Kind:      attachDestinationOperationKind(destination),
			Component: attachDestinationComponent(destination),
			Detail:    "destination",
			ShapeIn:   current,
			ShapeOut:  current,
		},
		component:   attachComponent{stage: muxStage, owned: true},
		terminal:    true,
		destination: destination,
		stream:      stream,
	}, nil
}

// finalizeBranch reserves the branch's node names against the running graph
// and the attach group, then emits the branch's slice of the patch: its
// operations, taps, destinations, and edges, with the prepared components
// registered under their planned nodes.
func (p *attachPlan) finalizeBranch(index int, spec BranchSpec, destinations []attachDestination, anchor TapInfo, graphSpec pipeline.Spec, group *runtimeAttachGroup, steps []attachStep) error {
	branch := &p.branches[index]
	name := branch.name
	opStart := len(p.work.Operations)
	edgeStart := len(p.work.Edges)
	previous := branch.from
	seen := make(map[string]struct{}, len(steps))
	stageIndex := 0
	terminalIndex := 0
	operationIndex := 0
	ids := make([]string, 0, len(steps))
	var taps []TapInfo

	emit := func(operation workOperation) {
		operation.ID = workOperationIDForKind(name, operationIndex, operation.Kind)
		operation.Branch = name
		p.work.Operations = append(p.work.Operations, operation)
		ids = append(ids, operation.ID)
		operationIndex++
	}
	connect := func(node pipeline.NodeRef) {
		edge := workEdge{From: previous, To: node, Policy: pipeline.RouteAll, Branch: name}
		if previous == branch.from {
			edge.Policy = branch.policy
			edge.Label = branch.label
		}
		p.work.Edges = append(p.work.Edges, edge)
	}

	for si := range steps {
		step := steps[si]
		operation := step.operation
		if step.tap != nil {
			tap := *step.tap
			tap.Node = previous
			p.work.Taps = append(p.work.Taps, tap)
			if step.install {
				taps = append(taps, runtimeBranchTapInfo(tap.Name, previous, tap.Shape, tap.After))
			}
			operation.Node = previous
			operation.Name = tap.Name
			emit(operation)
			continue
		}
		if step.terminal {
			destinationName := firstNonEmpty(step.destination.name, attachComponentName(step.component), "destination")
			var node pipeline.NodeRef
			addsNode := false
			switch {
			case group.isSharedSink(step.destination.shareKey):
				if err := group.reserveSharedSink(graphSpec, step.destination.shareKey, step.destination.name, step.destination.sink); err != nil {
					return err
				}
				node = pipeline.NodeRef(group.sharedSinks[step.destination.shareKey].name)
				addsNode = true
			case group.isSharedMux(step.destination.shareKey):
				if err := group.reserveSharedMux(graphSpec, name, branch.buffer, step.destination.shareKey, step.destination.name, step.destination.dest, step.stream); err != nil {
					return err
				}
				node = pipeline.NodeRef(group.sharedMuxes[step.destination.shareKey].name)
				addsNode = true
			case step.component.stage != nil || step.component.sink != nil:
				nodeName := runtimeBranchNodeName(name, attachComponentName(step.component), fmt.Sprintf("target%d", terminalIndex+1))
				if err := validateRuntimeBranchNodeName(graphSpec, seen, nodeName); err != nil {
					return err
				}
				if err := group.reserveNode(graphSpec, nodeName); err != nil {
					return err
				}
				node = pipeline.NodeRef(nodeName)
				p.components[node] = step.component
				addsNode = true
			}
			operation.Node = node
			operation.Name = firstNonEmpty(node.String(), destinationName)
			operation.Destinations = []string{destinationName}
			p.work.Destinations = append(p.work.Destinations, workDestination{
				ID:        workDestinationID(destinationName, terminalIndex),
				Name:      destinationName,
				Operation: operation.Kind,
				Component: operation.Component,
				Format:    destinationOpenFormat(step.destination.dest),
				Branches:  []string{name},
			})
			terminalIndex++
			if addsNode {
				connect(node)
			}
			emit(operation)
			continue
		}
		stage := step.component.stage
		if stage == nil {
			operation.Name = firstNonEmpty(operation.Component, string(operation.Kind))
			emit(operation)
			continue
		}
		nodeName := runtimeBranchNodeName(name, stage.Name(), fmt.Sprintf("stage%d", stageIndex+1))
		if err := validateRuntimeBranchNodeName(graphSpec, seen, nodeName); err != nil {
			return err
		}
		if err := group.reserveNode(graphSpec, nodeName); err != nil {
			return err
		}
		node := pipeline.NodeRef(nodeName)
		p.components[node] = step.component
		operation.Node = node
		operation.Name = nodeName
		connect(node)
		previous = node
		stageIndex++
		emit(operation)
	}

	p.work.Branches = append(p.work.Branches, workBranch{
		ID:           workBranchID(name, index),
		Name:         name,
		SourceShape:  normalizeTapShape(runtimeBranchAnchorShape(anchor)),
		Operations:   ids,
		Destinations: attachDestinationNames(destinations),
	})
	branch.operations = [2]int{opStart, len(p.work.Operations)}
	branch.edges = [2]int{edgeStart, len(p.work.Edges)}
	branch.taps = taps
	return nil
}

func attachComponentName(component attachComponent) string {
	switch {
	case component.stage != nil:
		return component.stage.Name()
	case component.sink != nil:
		return component.sink.Name()
	default:
		return ""
	}
}

// attachStepShape is the patch shape after a prepared step: the live chain
// shape when it carries information, else the normalized patch lineage.
func attachStepShape(current shape.Spec, patch shape.Spec) shape.Spec {
	if mediaShapeEmpty(current) {
		return patch
	}
	return current
}

func attachOperationDetail(kind OperationKind) string {
	switch kind {
	case OpDecode:
		return "decode"
	case OpTransform:
		return "transform"
	case OpShape:
		return "shape"
	case OpTap:
		return "tap"
	case OpEncode:
		return "encode"
	case OpCopy:
		return "copy"
	case OpStage:
		return "stage"
	default:
		return "operation"
	}
}

func attachOperationComponent(operation OperationSpec, stage pipeline.Stage) string {
	if component := firstNonEmpty(operationSpecComponent(operation), operation.Component); component != "" {
		return component
	}
	if stage != nil {
		return stage.Name()
	}
	if operation.Kind == OpShape && !mediaShapeEmpty(operation.Shape) {
		return "shape"
	}
	return ""
}

func attachDestinationOperationKind(destination attachDestination) OperationKind {
	if destination.sink != nil {
		return OpSink
	}
	if destinationSpecHasOutput(destination.dest) {
		return OpMux
	}
	return OpSink
}

func attachDestinationComponent(destination attachDestination) string {
	if destination.sink != nil {
		return "sink"
	}
	if formatID := destinationOpenFormat(destination.dest); formatID != "" {
		return string(formatID)
	}
	return "mux"
}

func attachDestinationNames(destinations []attachDestination) []string {
	if len(destinations) == 0 {
		return nil
	}
	out := make([]string, 0, len(destinations))
	for i := range destinations {
		out = append(out, firstNonEmpty(destinations[i].name, destinations[i].dest.label("destination")))
	}
	return out
}

func attachDestinationsHaveMux(destinations []attachDestination) bool {
	for i := range destinations {
		if destinations[i].sink == nil && destinationSpecHasOutput(destinations[i].dest) {
			return true
		}
	}
	return false
}

func workOperationIDForKind(branch string, index int, kind OperationKind) string {
	return fmt.Sprintf("%s/%03d/%s", firstNonEmpty(branch, "branch"), index, kind)
}

func workPatchRollbackFromBranches(operations []workOperation, destinations []workDestination) []workRollbackStep {
	out := make([]workRollbackStep, 0, len(operations)+len(destinations))
	for i := range operations {
		if operations[i].Node == "" {
			continue
		}
		out = append(out, workRollbackStep{Action: "remove-node", Node: operations[i].Node})
	}
	for i := range destinations {
		if destinations[i].Name == "" {
			continue
		}
		out = append(out, workRollbackStep{Action: "close-destination", Destination: destinations[i].Name})
	}
	return out
}

func cloneWorkPatch(patch workPatch) workPatch {
	clone := patch
	clone.Operations = cloneWorkOperations(patch.Operations)
	clone.Taps = append([]workTap(nil), patch.Taps...)
	clone.Branches = cloneWorkBranches(patch.Branches)
	clone.Destinations = cloneWorkDestinations(patch.Destinations)
	clone.Edges = append([]workEdge(nil), patch.Edges...)
	clone.Rollback = append([]workRollbackStep(nil), patch.Rollback...)
	clone.Diagnostics = clonePlanDiagnostics(patch.Diagnostics)
	return clone
}

func destinationSnapshotsFromWork(destinations []workDestination, state DestinationState) []DestinationSnapshot {
	if len(destinations) == 0 {
		return nil
	}
	out := make([]DestinationSnapshot, 0, len(destinations))
	for i := range destinations {
		out = append(out, DestinationSnapshot{
			Name:      destinations[i].Name,
			Operation: destinations[i].Operation,
			Component: destinations[i].Component,
			Format:    destinations[i].Format,
			Branches:  append([]string(nil), destinations[i].Branches...),
			State:     state,
			Open:      state == DestinationOpen,
		})
	}
	return out
}
