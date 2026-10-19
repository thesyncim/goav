package compile

import (
	"cmp"
	"fmt"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// workPlan is the executable plan for a build: the operations, taps, branches,
// destinations, and edges the lowering executes against the graph. It is built
// once by the compile (buildWorkPlan) and every view — lowering validation,
// Explain reports, and mux compatibility — renders from it.
type WorkPlan struct {
	Name         string
	Realtime     bool
	Inputs       []WorkInput
	Streams      []WorkStream
	Operations   []WorkOperation
	Taps         []WorkTap
	Branches     []WorkBranch
	Destinations []WorkDestination
	Edges        []workEdge
	Decisions    []PlanDecision
	Diagnostics  []plan.Diagnostic
}

type WorkInput struct {
	Name      string
	Protocol  av.ProtocolID
	Realtime  bool
	Codec     av.CodecID
	CodecSpec codec.Spec
}

type WorkStream struct {
	Name         string
	Select       plan.StreamSelect
	Operations   []recipeir.Operation
	Destinations []string
}

// workOperation is one planned operation. Destinations carries the stable
// destination IDs (workDestination.ID) a terminal operation routes into —
// destination routing is by ID, never by display name.
type WorkOperation struct {
	ID           string
	Name         string
	Kind         plan.OperationKind
	Branch       string
	Node         pipeline.NodeRef
	Component    string
	Detail       string
	Codec        codec.Spec
	Shared       bool
	ShapeIn      shape.Spec
	ShapeOut     shape.Spec
	Destinations []string
}

type WorkTap struct {
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
type WorkBranch struct {
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
type WorkDestination struct {
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

// workDestinationIDsByName maps destination display names to their stable IDs
// while the planner intermediates still carry names.
func WorkDestinationIDsByName(destinations []WorkDestination) map[string]string {
	out := make(map[string]string, len(destinations))
	for i := range destinations {
		if destinations[i].Name == "" {
			continue
		}
		out[destinations[i].Name] = destinations[i].ID
	}
	return out
}

func WorkDestinationIDForName(ids map[string]string, name string) string {
	if id, ok := ids[name]; ok {
		return id
	}
	return WorkDestinationID(name)
}

func workInputsFromRecipeIR(inputs []recipeir.Input) []WorkInput {
	if len(inputs) == 0 {
		return nil
	}
	out := make([]WorkInput, 0, len(inputs))
	for i := range inputs {
		input := inputs[i]
		out = append(out, WorkInput{
			Name:      cmp.Or(input.Name, input.URI, fmt.Sprintf("input-%d", i)),
			Protocol:  input.Protocol,
			Realtime:  input.Realtime,
			Codec:     input.Codec.ID,
			CodecSpec: CloneCodecSpec(input.Codec),
		})
	}
	return out
}

func recipeIROutputRefsToStrings(refs []recipeir.OutputRef) []string {
	if len(refs) == 0 {
		return nil
	}
	out := make([]string, 0, len(refs))
	for i := range refs {
		out = append(out, string(refs[i]))
	}
	return out
}

func workDestinationsFromPlan(outputs []PlanOutput) []WorkDestination {
	if len(outputs) == 0 {
		return nil
	}
	out := make([]WorkDestination, 0, len(outputs))
	for i := range outputs {
		output := outputs[i]
		out = append(out, WorkDestination{
			ID:        WorkDestinationID(output.Name),
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
func workOperationsFromBranches(spec pipeline.Spec, branches []PlanBranch, outputs []PlanOutput, ids map[string]string) []WorkOperation {
	if len(branches) == 0 {
		return nil
	}
	outputsByName := planOutputsByName(outputs)
	outputNodes := workPlanOutputNodesByName(spec, outputs)
	var out []WorkOperation
	for i := range branches {
		branch := branches[i]
		index := 0
		current := NormalizeTapShape(branch.Shape)
		for j := range branch.Operations {
			operation := branch.Operations[j]
			node := operation.Node
			if operation.Kind == plan.OpShape {
				node = ""
			}
			shapeIn := current
			if !MediaShapeEmpty(operation.ShapeIn) {
				shapeIn = operation.ShapeIn
			}
			shapeOut := operation.Shape
			if MediaShapeEmpty(shapeOut) {
				shapeOut = current
			}
			out = append(out, WorkOperation{
				ID:        workOperationIDForKind(branch.Name, index, operation.Kind),
				Name:      workOperationName(node, operation.Component, operation.Detail, index),
				Kind:      operation.Kind,
				Branch:    branch.Name,
				Node:      node,
				Component: operation.Component,
				Detail:    operation.Detail,
				Codec:     CloneCodecSpec(operation.Codec),
				Shared:    operation.Shared,
				ShapeIn:   shapeIn,
				ShapeOut:  shapeOut,
			})
			index++
			if !WorkOperationTerminal(operation.Kind) {
				current = shapeOut
			}
		}
		for _, destination := range branch.Outputs {
			output := outputsByName[destination]
			node := outputNodes[destination]
			if node == "" {
				node = pipeline.NodeRef(cmp.Or(output.Name, destination))
			}
			out = append(out, WorkOperation{
				ID:           workOperationIDForKind(branch.Name, index, output.Operation),
				Name:         workOperationName(node, output.Component, "destination", index),
				Kind:         output.Operation,
				Branch:       branch.Name,
				Node:         node,
				Component:    output.Component,
				Detail:       "destination",
				ShapeIn:      current,
				ShapeOut:     current,
				Destinations: []string{WorkDestinationIDForName(ids, destination)},
			})
			index++
		}
	}
	return out
}

func workPlanOutputNodesByName(spec pipeline.Spec, outputs []PlanOutput) map[string]pipeline.NodeRef {
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

func workPlanNodeMatchesOutput(node pipeline.NodeSpec, output PlanOutput) bool {
	switch output.Operation {
	case plan.OpSink:
		return node.Kind == pipeline.NodeSink
	case plan.OpMux, plan.OpWrite:
		return node.Kind == pipeline.NodeStage && strings.HasPrefix(node.Detail, "mux")
	default:
		return false
	}
}

func planOutputsByName(outputs []PlanOutput) map[string]PlanOutput {
	out := make(map[string]PlanOutput, len(outputs))
	for i := range outputs {
		if outputs[i].Name == "" {
			continue
		}
		out[outputs[i].Name] = outputs[i]
	}
	return out
}

func workBranchesFromPlanAt(branches []PlanBranch, operations []WorkOperation, ids map[string]string, base int) []WorkBranch {
	if len(branches) == 0 {
		return nil
	}
	operationsByBranch := make(map[string][]string, len(branches))
	for i := range operations {
		operationsByBranch[operations[i].Branch] = append(operationsByBranch[operations[i].Branch], operations[i].ID)
	}
	out := make([]WorkBranch, 0, len(branches))
	for i := range branches {
		branch := branches[i]
		destinations := make([]string, 0, len(branch.Outputs))
		for _, name := range branch.Outputs {
			destinations = append(destinations, WorkDestinationIDForName(ids, name))
		}
		out = append(out, WorkBranch{
			ID:           workBranchID(branch.Name, base+i),
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

func workEdgesFromSpec(spec pipeline.Spec, operations []WorkOperation) []workEdge {
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
	return cmp.Or(toBranch, fromBranch)
}

func workOperationIDForKind(branch string, index int, kind plan.OperationKind) string {
	return fmt.Sprintf("%s/%03d/%s", cmp.Or(branch, "branch"), index, kind)
}

func workOperationName(node pipeline.NodeRef, component string, detail string, index int) string {
	if node != "" {
		return node.String()
	}
	return cmp.Or(component, detail, fmt.Sprintf("op-%d", index))
}

func workBranchID(name string, index int) string {
	return fmt.Sprintf("branch/%s/%d", cmp.Or(name, "main"), index)
}

// workDestinationID derives the stable destination ID from the destination's
// label. Labels are unique per plan: matching labels either belong to one
// explicit mux group or fail as duplicate destinations, so the ID is stable
// across plan orderings.
func WorkDestinationID(name string) string {
	return "destination/" + cmp.Or(name, "unnamed")
}

func WorkOperationTerminal(kind plan.OperationKind) bool {
	switch kind {
	case plan.OpMux, plan.OpSink, plan.OpWrite:
		return true
	default:
		return false
	}
}

func workBranchesByName(branches []WorkBranch) map[string]WorkBranch {
	out := make(map[string]WorkBranch, len(branches))
	for i := range branches {
		if branches[i].Name == "" {
			continue
		}
		out[branches[i].Name] = branches[i]
	}
	return out
}

func WorkOperationsByID(operations []WorkOperation) map[string]WorkOperation {
	out := make(map[string]WorkOperation, len(operations))
	for i := range operations {
		out[operations[i].ID] = operations[i]
	}
	return out
}

func workDestinationIndexByID(destinations []WorkDestination, id string) (int, bool) {
	for i := range destinations {
		if destinations[i].ID == id {
			return i, true
		}
	}
	return -1, false
}

// workDestinationNameByID resolves a destination's display name from its
// routing ID; views report names while the plan routes by ID.
func WorkDestinationNameByID(destinations []WorkDestination, id string) string {
	if index, ok := workDestinationIndexByID(destinations, id); ok {
		return destinations[index].Name
	}
	return id
}

func CloneWorkPlan(wp WorkPlan) WorkPlan {
	clone := wp
	clone.Inputs = cloneWorkInputs(wp.Inputs)
	clone.Streams = cloneWorkStreams(wp.Streams)
	clone.Operations = cloneWorkOperations(wp.Operations)
	clone.Taps = append([]WorkTap(nil), wp.Taps...)
	clone.Branches = cloneWorkBranches(wp.Branches)
	clone.Destinations = CloneWorkDestinations(wp.Destinations)
	clone.Edges = append([]workEdge(nil), wp.Edges...)
	clone.Decisions = clonePlanDecisions(wp.Decisions)
	clone.Diagnostics = ClonePlanDiagnostics(wp.Diagnostics)
	return clone
}

func cloneWorkInputs(inputs []WorkInput) []WorkInput {
	if len(inputs) == 0 {
		return nil
	}
	out := make([]WorkInput, 0, len(inputs))
	for i := range inputs {
		input := inputs[i]
		input.CodecSpec = CloneCodecSpec(input.CodecSpec)
		out = append(out, input)
	}
	return out
}

func cloneWorkStreams(streams []WorkStream) []WorkStream {
	if len(streams) == 0 {
		return nil
	}
	out := make([]WorkStream, 0, len(streams))
	for i := range streams {
		stream := streams[i]
		stream.Operations = append([]recipeir.Operation(nil), stream.Operations...)
		stream.Destinations = append([]string(nil), stream.Destinations...)
		out = append(out, stream)
	}
	return out
}

func cloneWorkOperations(operations []WorkOperation) []WorkOperation {
	if len(operations) == 0 {
		return nil
	}
	out := make([]WorkOperation, 0, len(operations))
	for i := range operations {
		operation := operations[i]
		operation.Codec = CloneCodecSpec(operation.Codec)
		operation.Destinations = append([]string(nil), operation.Destinations...)
		out = append(out, operation)
	}
	return out
}

func cloneWorkBranches(branches []WorkBranch) []WorkBranch {
	if len(branches) == 0 {
		return nil
	}
	out := make([]WorkBranch, 0, len(branches))
	for i := range branches {
		branch := branches[i]
		branch.Operations = append([]string(nil), branch.Operations...)
		branch.Destinations = append([]string(nil), branch.Destinations...)
		out = append(out, branch)
	}
	return out
}

func CloneWorkDestinations(destinations []WorkDestination) []WorkDestination {
	if len(destinations) == 0 {
		return nil
	}
	out := make([]WorkDestination, 0, len(destinations))
	for i := range destinations {
		destination := destinations[i]
		destination.Branches = append([]string(nil), destination.Branches...)
		out = append(out, destination)
	}
	return out
}
