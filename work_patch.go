package goav

import (
	"fmt"

	"github.com/thesyncim/goav/pipeline"
)

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

func workPatchFromRuntimeBranches(name string, branches []runtimeBranch, nodeNames [][]string, group *runtimeAttachGroup) workPatch {
	patch := workPatch{Name: firstNonEmpty(name, "runtime-attach")}
	for i := range branches {
		names := []string(nil)
		if i < len(nodeNames) {
			names = nodeNames[i]
		}
		workBranch, operations, taps, destinations, edges := workPatchBranchFromRuntimeBranch(branches[i], i, names, group)
		patch.Branches = append(patch.Branches, workBranch)
		patch.Operations = append(patch.Operations, operations...)
		patch.Taps = append(patch.Taps, taps...)
		patch.Destinations = append(patch.Destinations, destinations...)
		patch.Edges = append(patch.Edges, edges...)
	}
	patch.Rollback = workPatchRollbackFromBranches(patch.Operations, patch.Destinations)
	return patch
}

func workPatchBranchFromRuntimeBranch(branch runtimeBranch, index int, nodeNames []string, group *runtimeAttachGroup) (workBranch, []workOperation, []workTap, []workDestination, []workEdge) {
	name := firstNonEmpty(branch.name, "branch")
	sourceShape := normalizeTapShape(runtimeBranchAnchorShape(branch.anchor))
	currentShape := sourceShape
	previous := pipeline.NodeRef(branch.from)
	stageIndex := 0
	operationIndex := 0
	workSteps := runtimeBranchWorkSteps(branch)
	operationIDs := make([]string, 0, len(workSteps)+len(branch.terminals))
	operations := make([]workOperation, 0, len(workSteps)+len(branch.terminals))
	taps := make([]workTap, 0, runtimeBranchTapCount(branch))
	edges := make([]workEdge, 0, len(workSteps)+len(branch.terminals))

	appendOperation := func(operation workOperation) {
		operation.ID = workOperationIDForKind(name, operationIndex, operation.Kind)
		operation.Branch = name
		operations = append(operations, operation)
		operationIDs = append(operationIDs, operation.ID)
		operationIndex++
	}

	for i := range workSteps {
		step := workSteps[i]
		kind := runtimeBranchWorkStepKind(step)
		shapeIn := currentShape
		shapeOut := runtimeBranchWorkStepShapeOut(step, currentShape)
		if tapName := runtimeBranchWorkStepTapName(step); tapName != "" {
			appendOperation(workOperation{
				Name:      tapName,
				Kind:      OpTap,
				Node:      previous,
				Component: runtimeBranchWorkStepComponent(step),
				Detail:    runtimeBranchWorkStepDetail(step),
				ShapeIn:   shapeIn,
				ShapeOut:  shapeOut,
			})
			taps = append(taps, workTap{
				Name:      tapName,
				Node:      previous,
				Domain:    shapeOut.Domain,
				MediaKind: shapeOut.MediaKind,
				After:     runtimeBranchWorkStepTapAfter(step),
				Shape:     shapeOut,
			})
			currentShape = shapeOut
			continue
		}
		node := pipeline.NodeRef("")
		if step.hasStep && step.step.stage != nil {
			node = runtimeBranchStepNode(nodeNames, stageIndex)
			stageIndex++
		}
		appendOperation(workOperation{
			Name:      runtimeBranchWorkStepName(step, node, operationIndex),
			Kind:      kind,
			Node:      node,
			Component: runtimeBranchWorkStepComponent(step),
			Detail:    runtimeBranchWorkStepDetail(step),
			ShapeIn:   shapeIn,
			ShapeOut:  shapeOut,
		})
		if node != "" {
			edges = append(edges, workEdge{
				From:   previous,
				To:     node,
				Policy: runtimeBranchPolicy(branch),
				Label:  branch.label,
				Branch: name,
			})
			previous = node
		}
		currentShape = shapeOut
	}

	terminalIndex := 0
	destinations := make([]workDestination, 0, len(branch.terminals))
	for i := range branch.terminals {
		terminal := branch.terminals[i]
		destinationName := firstNonEmpty(terminal.name, runtimeBranchTerminalName(terminal), "destination")
		node := runtimeBranchTerminalWorkNode(terminal, nodeNames, stageIndex+terminalIndex, group)
		if !runtimeBranchTerminalShared(terminal, group) {
			terminalIndex++
		}
		kind := runtimeBranchTerminalOperationKind(terminal)
		appendOperation(workOperation{
			Name:         firstNonEmpty(node.String(), destinationName),
			Kind:         kind,
			Node:         node,
			Component:    runtimeBranchTerminalWorkComponent(terminal),
			Detail:       "destination",
			ShapeIn:      currentShape,
			ShapeOut:     currentShape,
			Destinations: []string{destinationName},
		})
		destinations = append(destinations, workDestination{
			ID:        workDestinationID(destinationName, i),
			Name:      destinationName,
			Operation: kind,
			Component: runtimeBranchTerminalWorkComponent(terminal),
			Format:    destinationOpenFormat(terminal.dest),
			Branches:  []string{name},
		})
		if node != "" {
			edges = append(edges, workEdge{
				From:   previous,
				To:     node,
				Policy: runtimeBranchPolicy(branch),
				Label:  branch.label,
				Branch: name,
			})
		}
	}

	return workBranch{
		ID:           workBranchID(name, index),
		Name:         name,
		SourceShape:  sourceShape,
		Operations:   operationIDs,
		Destinations: runtimeBranchDestinationNames(branch),
	}, operations, taps, destinations, edges
}

type runtimeBranchWorkStep struct {
	operation OperationSpec
	step      runtimeBranchStep
	hasStep   bool
}

func runtimeBranchWorkSteps(branch runtimeBranch) []runtimeBranchWorkStep {
	operations := runtimeBranchShapeOperationSpecs(branch)
	if len(operations) == 0 && len(branch.steps) == 0 {
		return nil
	}
	out := make([]runtimeBranchWorkStep, 0, len(operations)+len(branch.steps))
	stepIndex := 0
	for i := range operations {
		operation := operations[i]
		if operation.Kind == "" {
			continue
		}
		work := runtimeBranchWorkStep{operation: operation}
		if runtimeBranchOperationNeedsPreparedStep(operation) &&
			stepIndex < len(branch.steps) &&
			runtimeBranchStepMatchesOperation(branch.steps[stepIndex], operation) {
			work.step = branch.steps[stepIndex]
			work.hasStep = true
			stepIndex++
		}
		out = append(out, work)
	}
	for ; stepIndex < len(branch.steps); stepIndex++ {
		out = append(out, runtimeBranchWorkStep{step: branch.steps[stepIndex], hasStep: true})
	}
	return out
}

func runtimeBranchOperationNeedsPreparedStep(operation OperationSpec) bool {
	switch operation.Kind {
	case OpCopy:
		return false
	case OpDecode, OpShape, OpTransform, OpStage, OpEncode, OpTap:
		return true
	default:
		return false
	}
}

func runtimeBranchStepMatchesOperation(step runtimeBranchStep, operation OperationSpec) bool {
	switch operation.Kind {
	case OpDecode:
		return step.kind == OpDecode || step.decode
	case OpShape:
		return step.kind == OpShape || !mediaShapeEmpty(step.shapeUpdate)
	case OpTransform:
		return step.kind == OpTransform || runtimeBranchStepHasTransform(step)
	case OpStage:
		return step.kind == OpStage || (step.kind == "" && step.stage != nil)
	case OpEncode:
		return step.kind == OpEncode
	case OpTap:
		if step.kind != OpTap && step.tap == "" {
			return false
		}
		return operation.Tap.Name == "" || step.tap == operation.Tap.Name
	default:
		return false
	}
}

func runtimeBranchWorkStepKind(work runtimeBranchWorkStep) OperationKind {
	if work.operation.Kind != "" {
		return work.operation.Kind
	}
	if work.hasStep {
		return runtimeBranchStepWorkKind(work.step)
	}
	return OpStage
}

func runtimeBranchWorkStepName(work runtimeBranchWorkStep, node pipeline.NodeRef, index int) string {
	if node != "" {
		return node.String()
	}
	if tapName := runtimeBranchWorkStepTapName(work); tapName != "" {
		return tapName
	}
	component := runtimeBranchWorkStepComponent(work)
	if component != "" {
		return component
	}
	if work.operation.Kind != "" {
		return string(work.operation.Kind)
	}
	if work.hasStep {
		return runtimeBranchStepWorkName(work.step, node, index)
	}
	return "operation"
}

func runtimeBranchWorkStepComponent(work runtimeBranchWorkStep) string {
	if work.operation.Kind != "" {
		component := firstNonEmpty(operationSpecComponent(work.operation), work.operation.Component)
		if component != "" {
			return component
		}
	}
	if work.hasStep {
		return runtimeBranchStepWorkComponent(work.step)
	}
	return ""
}

func runtimeBranchWorkStepDetail(work runtimeBranchWorkStep) string {
	switch runtimeBranchWorkStepKind(work) {
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
		if detail := operationSpecDetail(work.operation); detail != "" {
			return detail
		}
		if work.hasStep {
			return runtimeBranchStepWorkDetail(work.step)
		}
		return "operation"
	}
}

func runtimeBranchWorkStepShapeOut(work runtimeBranchWorkStep, current MediaShape) MediaShape {
	if work.hasStep {
		if shape := firstNonEmptyShape(work.step.shape, current); !mediaShapeEmpty(shape) {
			return shape
		}
	}
	if work.operation.Kind != "" {
		return operationSpecOutputShape(current, work.operation)
	}
	return current
}

func runtimeBranchWorkStepTapName(work runtimeBranchWorkStep) string {
	if work.operation.Kind == OpTap {
		return firstNonEmpty(work.operation.Tap.Name, work.operation.Component)
	}
	if work.hasStep {
		return work.step.tap
	}
	return ""
}

func runtimeBranchWorkStepTapAfter(work runtimeBranchWorkStep) OperationKind {
	if work.operation.Kind == OpTap && work.operation.Tap.After != "" {
		return work.operation.Tap.After
	}
	if work.hasStep {
		return work.step.after
	}
	return ""
}

func runtimeBranchStepNode(nodeNames []string, index int) pipeline.NodeRef {
	if index < 0 || index >= len(nodeNames) {
		return ""
	}
	return pipeline.NodeRef(nodeNames[index])
}

func runtimeBranchStepWorkKind(step runtimeBranchStep) OperationKind {
	if step.kind != "" {
		return step.kind
	}
	switch {
	case step.decode:
		return OpDecode
	case runtimeBranchStepHasTransform(step):
		return OpTransform
	case !mediaShapeEmpty(step.shapeUpdate):
		return OpShape
	case step.tap != "":
		return OpTap
	case step.stage != nil:
		return OpStage
	default:
		return OpStage
	}
}

func runtimeBranchStepWorkName(step runtimeBranchStep, node pipeline.NodeRef, index int) string {
	if node != "" {
		return node.String()
	}
	if step.tap != "" {
		return step.tap
	}
	return firstNonEmpty(runtimeBranchStepWorkComponent(step), "operation")
}

func runtimeBranchStepWorkComponent(step runtimeBranchStep) string {
	switch {
	case step.stage != nil:
		return step.stage.Name()
	case step.decode:
		return string(step.codec.ID)
	case runtimeBranchStepHasTransform(step):
		return transformFactoryName(step.transform)
	case step.tap != "":
		return step.tap
	case !mediaShapeEmpty(step.shapeUpdate):
		return "shape"
	default:
		return ""
	}
}

func runtimeBranchStepWorkDetail(step runtimeBranchStep) string {
	switch runtimeBranchStepWorkKind(step) {
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
	case OpStage:
		return "stage"
	default:
		return "operation"
	}
}

func runtimeBranchTerminalWorkNode(terminal runtimeBranchTerminal, nodeNames []string, index int, group *runtimeAttachGroup) pipeline.NodeRef {
	if group != nil && group.isSharedSink(terminal.shareKey) {
		if destination := group.sharedSinks[terminal.shareKey]; destination != nil {
			return pipeline.NodeRef(destination.name)
		}
	}
	if group != nil && group.isSharedMux(terminal.shareKey) {
		if destination := group.sharedMuxes[terminal.shareKey]; destination != nil {
			return pipeline.NodeRef(destination.name)
		}
	}
	if index >= 0 && index < len(nodeNames) {
		return pipeline.NodeRef(nodeNames[index])
	}
	return pipeline.NodeRef(firstNonEmpty(terminal.name, runtimeBranchTerminalName(terminal)))
}

func runtimeBranchTerminalShared(terminal runtimeBranchTerminal, group *runtimeAttachGroup) bool {
	return group != nil && (group.isSharedSink(terminal.shareKey) || group.isSharedMux(terminal.shareKey))
}

func runtimeBranchTerminalOperationKind(terminal runtimeBranchTerminal) OperationKind {
	if terminal.sink != nil {
		return OpSink
	}
	if destinationSpecHasOutput(terminal.dest) || terminal.stage != nil {
		return OpMux
	}
	return OpSink
}

func runtimeBranchTerminalWorkComponent(terminal runtimeBranchTerminal) string {
	if terminal.sink != nil {
		return "sink"
	}
	if formatID := destinationOpenFormat(terminal.dest); formatID != "" {
		return string(formatID)
	}
	return "mux"
}

func runtimeBranchPolicy(branch runtimeBranch) pipeline.RoutePolicy {
	if branch.policy != "" {
		return branch.policy
	}
	return pipeline.RouteAll
}

func runtimeBranchDestinationNames(branch runtimeBranch) []string {
	if len(branch.destinations) == 0 {
		return nil
	}
	out := make([]string, 0, len(branch.destinations))
	for i := range branch.destinations {
		out = append(out, firstNonEmpty(branch.destinations[i].name, branch.destinations[i].dest.label("destination")))
	}
	return out
}

func firstNonEmptyShape(values ...MediaShape) MediaShape {
	for i := range values {
		if !mediaShapeEmpty(values[i]) {
			return values[i]
		}
	}
	return MediaShape{}
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

func destinationSnapshotsFromWork(destinations []workDestination, open bool) []DestinationSnapshot {
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
			Open:      open,
		})
	}
	return out
}
