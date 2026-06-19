package goav

import (
	"fmt"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// workPlan is the executable plan for a build: the operations, taps, branches,
// destinations, and edges the lowering executes against the graph. It is built
// once by the compile (buildWorkPlan) and every view — lowering validation,
// Explain reports, and mux compatibility — renders from it.
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
	Diagnostics  []plan.Diagnostic
}

type workInput struct {
	Name     string
	Protocol av.ProtocolID
	Realtime bool
	Codec    av.CodecID
}

type workStream struct {
	Name         string
	Select       plan.StreamSelect
	Operations   []operationSpec
	Destinations []string
}

// workOperation is one planned operation. Destinations carries the stable
// destination IDs (workDestination.ID) a terminal operation routes into —
// destination routing is by ID, never by display name.
type workOperation struct {
	ID           string
	Name         string
	Kind         plan.OperationKind
	Branch       string
	Node         pipeline.NodeRef
	Component    string
	Detail       string
	Codec        codec.CodecSpec
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
	After     plan.OperationKind
	Shape     shape.Spec
	Shared    bool
}

// workBranch is one planned branch. Operations and Destinations reference the
// plan's operations and destinations by their stable IDs.
type workBranch struct {
	ID           string
	Name         string
	Input        string
	Stream       plan.StreamSelect
	SourceShape  shape.Spec
	Operations   []string
	Destinations []string
}

// workDestination is one planned destination. ID is the stable routing
// identity derived from the destination handle's label (label+config identity
// is deduplicated upstream: one label is one destination).
type workDestination struct {
	ID        string
	Name      string
	Operation plan.OperationKind
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

// buildWorkPlan builds the work plan once per compile: the branch planners
// lower the intent into per-branch operation lists, and one flattening walk
// emits the ordered operations, taps, branches, destinations, and edges.
func buildWorkPlan(state *recipeCompileState, spec pipeline.Spec) workPlan {
	if state.joinPlan != nil {
		// Joins plan multi-upstream convergence: the joinPlan renders its arms,
		// the plan.OpJoin node, and the downstream chain into the same workPlan IR.
		return state.joinPlan.buildJoinWorkPlan(state, spec)
	}
	intent := state.intent
	outputs := planOutputs(intent.Destinations, state.outputFormatMap())
	// The work plan plans every branch straight from intent.Streams:
	// branchCompositionJob.plan() populates each branch streamIntent with its
	// full (shared-op-inherited) operation list, so the parallel
	// branchComposePlan is only the Build-side lowerer input.
	branches, decisions := planBranches(state, outputs)
	outputs = planOutputsWithBranches(outputs, branches)
	work := composeWorkPlan(
		spec,
		firstNonEmpty(intent.Name, state.operation, "job"),
		workInputsFromIntent(intent.Inputs),
		workStreamsFromIntent(intent.Streams),
		branches,
		outputs,
		decisions,
	)
	work.Diagnostics = clonePlanDiagnostics(state.shapeDiagnostics)
	return work
}

// composeWorkPlan flattens the planner's per-branch working set into the work
// plan — the one place the build-side plan is assembled.
func composeWorkPlan(
	spec pipeline.Spec,
	name string,
	inputs []workInput,
	streams []workStream,
	branches []planBranch,
	outputs []planOutput,
	decisions []planDecision,
) workPlan {
	work := workPlan{
		Name:         firstNonEmpty(spec.Name, name, "goav"),
		Realtime:     spec.Realtime,
		Inputs:       inputs,
		Streams:      streams,
		Taps:         planTaps(branches),
		Destinations: workDestinationsFromPlan(outputs),
		Decisions:    clonePlanDecisions(decisions),
	}
	ids := workDestinationIDsByName(work.Destinations)
	work.Operations = workOperationsFromBranches(spec, branches, outputs, ids)
	work.Branches = workBranchesFromPlan(branches, work.Operations, ids)
	work.Edges = workEdgesFromSpec(spec, work.Operations)
	return work
}

// workDestinationIDsByName maps destination display names to their stable IDs
// while the planner intermediates still carry names.
func workDestinationIDsByName(destinations []workDestination) map[string]string {
	out := make(map[string]string, len(destinations))
	for i := range destinations {
		if destinations[i].Name == "" {
			continue
		}
		out[destinations[i].Name] = destinations[i].ID
	}
	return out
}

func workDestinationIDForName(ids map[string]string, name string) string {
	if id, ok := ids[name]; ok {
		return id
	}
	return workDestinationID(name)
}

func workInputsFromIntent(inputs []inputIntent) []workInput {
	if len(inputs) == 0 {
		return nil
	}
	out := make([]workInput, 0, len(inputs))
	for i := range inputs {
		input := inputs[i]
		out = append(out, workInput{
			Name:     firstNonEmpty(input.Name, input.URI, fmt.Sprintf("input-%d", i)),
			Protocol: input.Protocol,
			Realtime: input.Realtime,
			Codec:    input.Codec.ID,
		})
	}
	return out
}

func workStreamsFromIntent(streams []streamIntent) []workStream {
	if len(streams) == 0 {
		return nil
	}
	out := make([]workStream, 0, len(streams))
	for i := range streams {
		stream := streams[i]
		out = append(out, workStream{
			Name:         stream.Name,
			Select:       stream.Select,
			Operations:   cloneOperationSpecs(stream.Operations),
			Destinations: append([]string(nil), stream.Destinations...),
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
			ID:        workDestinationID(output.Name),
			Name:      output.Name,
			Operation: output.Operation,
			Component: output.Component,
			Format:    output.Format,
			Branches:  append([]string(nil), output.BranchRefs...),
		})
	}
	return out
}

// workOperationsFromBranches emits the ordered operation list in one walk over
// the planned branches: each branch's operations followed by its destination
// operations, with per-branch IDs and the running shape lineage.
func workOperationsFromBranches(spec pipeline.Spec, branches []planBranch, outputs []planOutput, ids map[string]string) []workOperation {
	if len(branches) == 0 {
		return nil
	}
	outputsByName := planOutputsByName(outputs)
	outputNodes := workPlanOutputNodesByName(spec, outputs)
	var out []workOperation
	for i := range branches {
		branch := branches[i]
		index := 0
		current := normalizeTapShape(branch.Shape)
		for j := range branch.Operations {
			operation := branch.Operations[j]
			node := pipeline.NodeRef(planOperationNodeName(branch, operation, j))
			if operation.Kind == plan.OpShape {
				node = ""
			}
			shapeOut := operation.Shape
			if mediaShapeEmpty(shapeOut) {
				shapeOut = current
			}
			out = append(out, workOperation{
				ID:        workOperationIDForKind(branch.Name, index, operation.Kind),
				Name:      workOperationName(node, operation.Component, operation.Detail, index),
				Kind:      operation.Kind,
				Branch:    branch.Name,
				Node:      node,
				Component: operation.Component,
				Detail:    operation.Detail,
				Codec:     cloneCodecSpec(operation.Codec),
				Shared:    operation.Shared,
				ShapeIn:   current,
				ShapeOut:  shapeOut,
			})
			index++
			if !workOperationTerminal(operation.Kind) {
				current = shapeOut
			}
		}
		for _, destination := range branch.Outputs {
			output := outputsByName[destination]
			node := outputNodes[destination]
			if node == "" {
				node = pipeline.NodeRef(firstNonEmpty(output.Name, destination))
			}
			out = append(out, workOperation{
				ID:           workOperationIDForKind(branch.Name, index, output.Operation),
				Name:         workOperationName(node, output.Component, "destination", index),
				Kind:         output.Operation,
				Branch:       branch.Name,
				Node:         node,
				Component:    output.Component,
				Detail:       "destination",
				ShapeIn:      current,
				ShapeOut:     current,
				Destinations: []string{workDestinationIDForName(ids, destination)},
			})
			index++
		}
	}
	return out
}

func workPlanOutputNodesByName(spec pipeline.Spec, outputs []planOutput) map[string]pipeline.NodeRef {
	out := make(map[string]pipeline.NodeRef, len(outputs))
	used := make(map[int]struct{}, len(outputs))
	for i := range outputs {
		output := outputs[i]
		if output.Name == "" {
			continue
		}
		for j := range spec.Nodes {
			if _, ok := used[j]; ok {
				continue
			}
			if !workPlanNodeMatchesOutput(spec.Nodes[j], output) {
				continue
			}
			out[output.Name] = pipeline.NodeRef(spec.Nodes[j].Name)
			used[j] = struct{}{}
			break
		}
	}
	return out
}

func workPlanNodeMatchesOutput(node pipeline.NodeSpec, output planOutput) bool {
	switch output.Operation {
	case plan.OpSink:
		return node.Kind == pipeline.NodeSink
	case plan.OpMux, plan.OpWrite:
		return node.Kind == pipeline.NodeStage && strings.HasPrefix(node.Detail, "mux")
	default:
		return false
	}
}

func planOutputsByName(outputs []planOutput) map[string]planOutput {
	out := make(map[string]planOutput, len(outputs))
	for i := range outputs {
		if outputs[i].Name == "" {
			continue
		}
		out[outputs[i].Name] = outputs[i]
	}
	return out
}

func workBranchesFromPlan(branches []planBranch, operations []workOperation, ids map[string]string) []workBranch {
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
		destinations := make([]string, 0, len(branch.Outputs))
		for _, name := range branch.Outputs {
			destinations = append(destinations, workDestinationIDForName(ids, name))
		}
		out = append(out, workBranch{
			ID:           workBranchID(branch.Name, i),
			Name:         branch.Name,
			Input:        branch.Input,
			Stream:       branch.Stream,
			SourceShape:  branch.Shape,
			Operations:   append([]string(nil), operationsByBranch[branch.Name]...),
			Destinations: destinations,
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

func workOperationIDForKind(branch string, index int, kind plan.OperationKind) string {
	return fmt.Sprintf("%s/%03d/%s", firstNonEmpty(branch, "branch"), index, kind)
}

func workOperationName(node pipeline.NodeRef, component string, detail string, index int) string {
	if node != "" {
		return node.String()
	}
	return firstNonEmpty(component, detail, fmt.Sprintf("op-%d", index))
}

func workBranchID(name string, index int) string {
	return fmt.Sprintf("branch/%s/%d", firstNonEmpty(name, "main"), index)
}

// workDestinationID derives the stable destination ID from the destination's
// label — the handle identity. Labels are unique per plan (one handle is one
// destination; the same label with a different config is a planning error), so
// the ID is stable across plan orderings.
func workDestinationID(name string) string {
	return "destination/" + firstNonEmpty(name, "unnamed")
}

func workOperationTerminal(kind plan.OperationKind) bool {
	switch kind {
	case plan.OpMux, plan.OpSink, plan.OpWrite:
		return true
	default:
		return false
	}
}

func workBranchesByName(branches []workBranch) map[string]workBranch {
	out := make(map[string]workBranch, len(branches))
	for i := range branches {
		if branches[i].Name == "" {
			continue
		}
		out[branches[i].Name] = branches[i]
	}
	return out
}

func workOperationsByID(operations []workOperation) map[string]workOperation {
	out := make(map[string]workOperation, len(operations))
	for i := range operations {
		out[operations[i].ID] = operations[i]
	}
	return out
}

func workDestinationIndexByID(destinations []workDestination, id string) (int, bool) {
	for i := range destinations {
		if destinations[i].ID == id {
			return i, true
		}
	}
	return -1, false
}

// workDestinationNameByID resolves a destination's display name from its
// routing ID; views report names while the plan routes by ID.
func workDestinationNameByID(destinations []workDestination, id string) string {
	if index, ok := workDestinationIndexByID(destinations, id); ok {
		return destinations[index].Name
	}
	return id
}

func cloneWorkPlan(wp workPlan) workPlan {
	clone := wp
	clone.Inputs = append([]workInput(nil), wp.Inputs...)
	clone.Streams = cloneWorkStreams(wp.Streams)
	clone.Operations = cloneWorkOperations(wp.Operations)
	clone.Taps = append([]workTap(nil), wp.Taps...)
	clone.Branches = cloneWorkBranches(wp.Branches)
	clone.Destinations = cloneWorkDestinations(wp.Destinations)
	clone.Edges = append([]workEdge(nil), wp.Edges...)
	clone.Decisions = clonePlanDecisions(wp.Decisions)
	clone.Diagnostics = clonePlanDiagnostics(wp.Diagnostics)
	return clone
}

func cloneWorkStreams(streams []workStream) []workStream {
	if len(streams) == 0 {
		return nil
	}
	out := make([]workStream, 0, len(streams))
	for i := range streams {
		stream := streams[i]
		stream.Operations = cloneOperationSpecs(stream.Operations)
		stream.Destinations = append([]string(nil), stream.Destinations...)
		out = append(out, stream)
	}
	return out
}

func cloneWorkOperations(operations []workOperation) []workOperation {
	if len(operations) == 0 {
		return nil
	}
	out := make([]workOperation, 0, len(operations))
	for i := range operations {
		operation := operations[i]
		operation.Codec = cloneCodecSpec(operation.Codec)
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
