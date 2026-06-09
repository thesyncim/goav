package goav

import (
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

type workPlan struct {
	Name         string
	Realtime     bool
	Inputs       []workInput
	Streams      []workStream
	Operations   []workOperation
	Taps         []workTap
	Branches     []workBranch
	Destinations []workDestination
	Edges        []workEdge
	Decisions    []planDecision
	Diagnostics  []PlanDiagnostic
}

type workInput struct {
	Name     string
	Protocol av.ProtocolID
	Realtime bool
	Codec    av.CodecID
}

type workStream struct {
	Name   string
	Select StreamSelect
}

type workOperation struct {
	ID           string
	Name         string
	Kind         OperationKind
	Branch       string
	Node         pipeline.NodeRef
	Component    string
	Detail       string
	Shared       bool
	ShapeIn      shape.Spec
	ShapeOut     shape.Spec
	Destinations []string
}

type workTap struct {
	Name      string
	Node      pipeline.NodeRef
	Domain    shape.MediaDomain
	MediaKind av.MediaType
	After     OperationKind
	Shape     shape.Spec
	Shared    bool
}

type workBranch struct {
	ID           string
	Name         string
	Input        string
	Stream       StreamSelect
	SourceShape  shape.Spec
	Operations   []string
	Destinations []string
}

type workDestination struct {
	ID        string
	Name      string
	Operation OperationKind
	Component string
	Format    av.FormatID
	Branches  []string
}

type workEdge struct {
	From   pipeline.NodeRef
	To     pipeline.NodeRef
	Policy pipeline.RoutePolicy
	Label  string
	Branch string
}

func workPlanFromMediaPlan(spec pipeline.Spec, plan mediaPlan, operations []graphPlanOperation) workPlan {
	if len(operations) == 0 {
		operations = graphPlanOperationsFromMediaPlan(spec, plan)
	}
	work := workPlan{
		Name:         firstNonEmpty(spec.Name, plan.Name, "goav"),
		Realtime:     spec.Realtime,
		Inputs:       workInputsFromPlan(plan.Inputs),
		Streams:      workStreamsFromPlan(plan.Streams),
		Taps:         workTapsFromPlan(plan.Taps),
		Destinations: workDestinationsFromPlan(plan.Outputs),
		Decisions:    clonePlanDecisions(plan.Decisions),
		Diagnostics:  clonePlanDiagnostics(plan.Diagnostics),
	}
	work.Operations = workOperationsFromGraphPlan(plan, operations)
	work.Branches = workBranchesFromPlan(plan.Branches, work.Operations)
	work.Edges = workEdgesFromSpec(spec, work.Operations)
	return work
}

func workInputsFromPlan(inputs []planInput) []workInput {
	if len(inputs) == 0 {
		return nil
	}
	out := make([]workInput, 0, len(inputs))
	for i := range inputs {
		out = append(out, workInput{
			Name:     inputs[i].Name,
			Protocol: inputs[i].Protocol,
			Realtime: inputs[i].Realtime,
			Codec:    inputs[i].Codec,
		})
	}
	return out
}

func workStreamsFromPlan(streams []planStream) []workStream {
	if len(streams) == 0 {
		return nil
	}
	out := make([]workStream, 0, len(streams))
	for i := range streams {
		out = append(out, workStream{
			Name:   streams[i].Name,
			Select: streams[i].Select,
		})
	}
	return out
}

func workTapsFromPlan(taps []planTap) []workTap {
	if len(taps) == 0 {
		return nil
	}
	out := make([]workTap, 0, len(taps))
	for i := range taps {
		out = append(out, workTap{
			Name:      taps[i].Name,
			Node:      taps[i].Node,
			Domain:    taps[i].Domain,
			MediaKind: taps[i].MediaKind,
			After:     taps[i].After,
			Shape:     taps[i].Shape,
			Shared:    taps[i].Shared,
		})
	}
	return out
}

func workDestinationsFromPlan(outputs []planOutput) []workDestination {
	if len(outputs) == 0 {
		return nil
	}
	out := make([]workDestination, 0, len(outputs))
	for i := range outputs {
		output := outputs[i]
		out = append(out, workDestination{
			ID:        workDestinationID(output.Name, i),
			Name:      output.Name,
			Operation: output.Operation,
			Component: output.Component,
			Format:    output.Format,
			Branches:  append([]string(nil), output.BranchRefs...),
		})
	}
	return out
}

func workOperationsFromGraphPlan(plan mediaPlan, operations []graphPlanOperation) []workOperation {
	if len(operations) == 0 {
		return nil
	}
	branchShapes := workBranchShapes(plan.Branches)
	out := make([]workOperation, 0, len(operations))
	branchIndexes := make(map[string]int, len(plan.Branches))
	for i := range operations {
		operation := operations[i]
		index := branchIndexes[operation.Branch]
		branchIndexes[operation.Branch] = index + 1
		shapeIn := branchShapes[operation.Branch]
		shapeOut := operation.Shape
		if mediaShapeEmpty(shapeOut) {
			shapeOut = shapeIn
		}
		out = append(out, workOperation{
			ID:           workOperationID(operation.Branch, index, operation),
			Name:         workOperationName(operation, index),
			Kind:         operation.Kind,
			Branch:       operation.Branch,
			Node:         operation.Node,
			Component:    operation.Component,
			Detail:       operation.Detail,
			Shared:       operation.Shared,
			ShapeIn:      shapeIn,
			ShapeOut:     shapeOut,
			Destinations: append([]string(nil), operation.Destinations...),
		})
		if !workOperationTerminal(operation.Kind) {
			branchShapes[operation.Branch] = shapeOut
		}
	}
	return out
}

func workBranchShapes(branches []planBranch) map[string]shape.Spec {
	out := make(map[string]shape.Spec, len(branches))
	for i := range branches {
		out[branches[i].Name] = normalizeTapShape(branches[i].Shape)
	}
	return out
}

func workBranchesFromPlan(branches []planBranch, operations []workOperation) []workBranch {
	if len(branches) == 0 {
		return nil
	}
	operationsByBranch := make(map[string][]string, len(branches))
	for i := range operations {
		operationsByBranch[operations[i].Branch] = append(operationsByBranch[operations[i].Branch], operations[i].ID)
	}
	out := make([]workBranch, 0, len(branches))
	for i := range branches {
		branch := branches[i]
		out = append(out, workBranch{
			ID:           workBranchID(branch.Name, i),
			Name:         branch.Name,
			Input:        branch.Input,
			Stream:       branch.Stream,
			SourceShape:  branch.Shape,
			Operations:   append([]string(nil), operationsByBranch[branch.Name]...),
			Destinations: append([]string(nil), branch.Outputs...),
		})
	}
	return out
}

func workEdgesFromSpec(spec pipeline.Spec, operations []workOperation) []workEdge {
	if len(spec.Edges) == 0 {
		return nil
	}
	branchByNode := make(map[pipeline.NodeRef]string, len(operations))
	for i := range operations {
		if operations[i].Node == "" {
			continue
		}
		branchByNode[operations[i].Node] = operations[i].Branch
	}
	out := make([]workEdge, 0, len(spec.Edges))
	for i := range spec.Edges {
		edge := spec.Edges[i]
		out = append(out, workEdge{
			From:   edge.From,
			To:     edge.To,
			Policy: edge.Policy,
			Label:  edge.Label,
			Branch: workEdgeBranch(edge, branchByNode),
		})
	}
	return out
}

func workEdgeBranch(edge pipeline.EdgeSpec, branchByNode map[pipeline.NodeRef]string) string {
	fromBranch := branchByNode[edge.From]
	toBranch := branchByNode[edge.To]
	if fromBranch != "" && fromBranch == toBranch {
		return fromBranch
	}
	return firstNonEmpty(toBranch, fromBranch)
}

func workOperationID(branch string, index int, operation graphPlanOperation) string {
	owner := firstNonEmpty(branch, "branch")
	return fmt.Sprintf("%s/%03d/%s", owner, index, operation.Kind)
}

func workOperationName(operation graphPlanOperation, index int) string {
	if operation.Node != "" {
		return operation.Node.String()
	}
	return firstNonEmpty(operation.Component, operation.Detail, fmt.Sprintf("op-%d", index))
}

func workBranchID(name string, index int) string {
	return fmt.Sprintf("branch/%s/%d", firstNonEmpty(name, "main"), index)
}

func workDestinationID(name string, index int) string {
	return fmt.Sprintf("destination/%s/%d", firstNonEmpty(name, "unnamed"), index)
}

func workOperationTerminal(kind OperationKind) bool {
	switch kind {
	case OpMux, OpSink, OpWrite:
		return true
	default:
		return false
	}
}

func cloneWorkPlan(plan workPlan) workPlan {
	clone := plan
	clone.Inputs = append([]workInput(nil), plan.Inputs...)
	clone.Streams = append([]workStream(nil), plan.Streams...)
	clone.Operations = cloneWorkOperations(plan.Operations)
	clone.Taps = append([]workTap(nil), plan.Taps...)
	clone.Branches = cloneWorkBranches(plan.Branches)
	clone.Destinations = cloneWorkDestinations(plan.Destinations)
	clone.Edges = append([]workEdge(nil), plan.Edges...)
	clone.Decisions = clonePlanDecisions(plan.Decisions)
	clone.Diagnostics = clonePlanDiagnostics(plan.Diagnostics)
	return clone
}

func cloneWorkOperations(operations []workOperation) []workOperation {
	if len(operations) == 0 {
		return nil
	}
	out := make([]workOperation, 0, len(operations))
	for i := range operations {
		operation := operations[i]
		operation.Destinations = append([]string(nil), operation.Destinations...)
		out = append(out, operation)
	}
	return out
}

func cloneWorkBranches(branches []workBranch) []workBranch {
	if len(branches) == 0 {
		return nil
	}
	out := make([]workBranch, 0, len(branches))
	for i := range branches {
		branch := branches[i]
		branch.Operations = append([]string(nil), branch.Operations...)
		branch.Destinations = append([]string(nil), branch.Destinations...)
		out = append(out, branch)
	}
	return out
}

func cloneWorkDestinations(destinations []workDestination) []workDestination {
	if len(destinations) == 0 {
		return nil
	}
	out := make([]workDestination, 0, len(destinations))
	for i := range destinations {
		destination := destinations[i]
		destination.Branches = append([]string(nil), destination.Branches...)
		out = append(out, destination)
	}
	return out
}
