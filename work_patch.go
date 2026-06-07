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
	operationIDs := make([]string, 0, len(branch.steps)+len(branch.terminals))
	operations := make([]workOperation, 0, len(branch.steps)+len(branch.terminals))
	taps := make([]workTap, 0, runtimeBranchTapCount(branch))
	edges := make([]workEdge, 0, len(branch.steps)+len(branch.terminals))

	appendOperation := func(operation workOperation) {
		operation.ID = workOperationIDForKind(name, operationIndex, operation.Kind)
		operation.Branch = name
		operations = append(operations, operation)
		operationIDs = append(operationIDs, operation.ID)
		operationIndex++
	}

	for i := range branch.steps {
		step := branch.steps[i]
		kind := runtimeBranchStepWorkKind(step)
		shapeIn := currentShape
		shapeOut := firstNonEmptyShape(step.shape, currentShape)
		if step.tap != "" {
			appendOperation(workOperation{
				Name:      step.tap,
				Kind:      OpTap,
				Node:      previous,
				Component: step.tap,
				Detail:    "tap",
				ShapeIn:   shapeIn,
				ShapeOut:  shapeOut,
			})
			taps = append(taps, workTap{
				Name:      step.tap,
				Node:      previous,
				Domain:    shapeOut.Domain,
				MediaKind: shapeOut.MediaKind,
				After:     step.after,
				Shape:     shapeOut,
			})
			currentShape = shapeOut
			continue
		}
		node := pipeline.NodeRef("")
		if step.stage != nil {
			node = runtimeBranchStepNode(nodeNames, stageIndex)
			stageIndex++
		}
		appendOperation(workOperation{
			Name:      runtimeBranchStepWorkName(step, node, operationIndex),
			Kind:      kind,
			Node:      node,
			Component: runtimeBranchStepWorkComponent(step),
			Detail:    runtimeBranchStepWorkDetail(step),
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
