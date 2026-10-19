package compile

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// buildRecipeWorkPlan renders a lowered recipe into its work plan: one
// branch per recipe branch, its operations from its root with the node each
// runs as; a join's arms and its convergence lead the join's branches.
func buildRecipeWorkPlan(s *State, l *Lowering, spec pipeline.Spec) WorkPlan {
	outputs := planOutputsFromRecipeIR(s.Recipe.Destinations, s.outputFormatMap())
	var branches []PlanBranch
	var decisions []PlanDecision
	var operations []WorkOperation
	var workBranches []WorkBranch
	ids := map[string]string{}
	for _, output := range outputs {
		ids[output.Name] = WorkDestinationID(output.Name)
	}
	flush := func() {
		if len(branches) == 0 {
			return
		}
		ops := workOperationsFromBranches(spec, branches, outputs, ids)
		operations = append(operations, ops...)
		workBranches = append(workBranches, workBranchesFromPlanAt(branches, ops, ids, len(workBranches))...)
		branches = nil
	}
	emitted := map[recipeir.NodeID]bool{}
	armShapes := map[recipeir.NodeID]shape.Spec{}
	var taps []WorkTap
	var diagnostics []plan.Diagnostic
	remuxBranches, remuxDecisions := s.planRemuxBranches(outputs)
	branches = append(branches, remuxBranches...)
	decisions = append(decisions, remuxDecisions...)
	for i := range s.Recipe.Streams {
		stream := s.Recipe.Streams[i]
		if stream.Input >= 0 {
			branch, branchDecisions := s.planStreamBranch(l, i, outputs)
			branches = append(branches, branch)
			decisions = append(decisions, branchDecisions...)
			continue
		}
		p := s.joins[stream.Root]
		if !emitted[stream.Root] {
			emitted[stream.Root] = true
			flush()
			ops, joinBranches, armShape := p.joinArmWork(len(workBranches))
			armShapes[stream.Root] = armShape
			operations = append(operations, ops...)
			workBranches = append(workBranches, joinBranches...)
			taps = append(taps, p.allTaps()...)
			diagnostics = append(diagnostics, p.allDiagnostics()...)
			if !s.Facts[i].lone {
				join := p.joinedWorkOperation(p.Name, 0, armShape)
				operations = append(operations, join)
				workBranches = append(workBranches, WorkBranch{
					ID:          workBranchID(p.Name, len(workBranches)),
					Name:        p.Name,
					Stream:      StreamSelectFromStream(p.Joined),
					SourceShape: p.joinedShape(),
					Operations:  []string{join.ID},
				})
			}
		}
		branches = append(branches, s.planJoinBranch(l, i, p, outputs, armShapes[stream.Root]))
	}
	flush()
	work := WorkPlan{
		Name:         cmp.Or(spec.Name, "goav"),
		Realtime:     spec.Realtime,
		Inputs:       workInputsFromRecipeIR(s.Recipe.Inputs),
		Streams:      s.workStreams(),
		Operations:   operations,
		Branches:     workBranches,
		Destinations: workDestinationsFromPlan(planOutputsWithWorkBranches(outputs, workBranches, ids)),
		Decisions:    clonePlanDecisions(decisions),
		Diagnostics:  append(diagnostics, s.shapeDiagnostics...),
	}
	work.Taps = append(planTaps(s.tapBranches(l, outputs)), taps...)
	work.Edges = workEdgesFromSpec(spec, work.Operations)
	return work
}

// planStreamBranch plans one branch of an input's stream: the input's read,
// the select, and the branch's operations, each on its planned node.
func (s *State) planStreamBranch(l *Lowering, index int, outputs []PlanOutput) (PlanBranch, []PlanDecision) {
	stream := s.Recipe.Streams[index]
	input := s.Recipe.Inputs[stream.Input]
	var spec shape.Spec
	sourceShape, sourceShapeOK := recipeIRInputSourceShape(input)
	plannedStream := stream
	if selected, ok := s.selectedStream(stream); ok {
		resolved := StreamSelectFromStream(selected)
		resolved.Input = stream.Selector.Input
		plannedStream.Selector = resolved
		domain := shape.DomainPacket
		if sourceShapeOK && sourceShape.Domain != "" {
			domain = sourceShape.Domain
		}
		spec = shape.FromStream(selected, domain)
		if sourceShapeOK {
			spec = shape.Merge(spec, sourceShape)
		}
	}
	spec = normalizeRecipeIRPlanBranchShape(spec, plannedStream, input)
	name := cmp.Or(plannedStream.Name, string(plannedStream.Selector.Type), fmt.Sprintf("branch-%d", index))
	operations, decisions := planRecipeIROperations(input, plannedStream, name, spec, selectorComponent(stream.Selector))
	operations = planOperationsWithShape(name, spec, operations)
	for i := range operations {
		operations[i].Node = l.operationNode(stream, operations[i], name)
	}
	if latency, ok := planPathLatencyDecision(s.Env, name, plannedStream.Operations); ok {
		decisions = append(decisions, latency)
	}
	return PlanBranch{
		Name:       name,
		Input:      s.inputName(stream.Input),
		Stream:     plannedStream.Selector,
		Shape:      spec,
		Operations: operations,
		Outputs:    PlanBranchDestinations(recipeIROutputRefsToStrings(stream.Outputs), outputs),
	}, decisions
}

// planJoinBranch plans one branch of a join's output: a lone branch carries
// the join's convergence, a fork reads the joined stream.
func (s *State) planJoinBranch(l *Lowering, index int, p *JoinPlan, outputs []PlanOutput, armShape shape.Spec) PlanBranch {
	stream := s.Recipe.Streams[index]
	joined := p.joinedShape()
	branch := PlanBranch{
		Name:    stream.Name,
		Input:   p.Name,
		Stream:  StreamSelectFromStream(p.Joined),
		Shape:   joined,
		Outputs: PlanBranchDestinations(recipeIROutputRefsToStrings(stream.Outputs), outputs),
	}
	var operations []PlanOperation
	if s.Facts[index].lone {
		branch.Input = ""
		operations = append(operations, PlanOperation{
			Kind:      plan.OpJoin,
			Component: p.Name,
			Detail:    p.joinDetail(),
			Shape:     joined,
			Node:      pipeline.NodeRef(p.Name),
			ShapeIn:   armShape,
		})
	}
	for _, operation := range stream.Operations {
		operations = append(operations, planOperationFromRecipeIROperation(operation))
	}
	start := 0
	if s.Facts[index].lone {
		start = 1
	}
	shaped := planOperationsWithShape(stream.Name, joined, operations[start:])
	copy(operations[start:], shaped)
	for i := start; i < len(operations); i++ {
		operations[i].Node = l.operationNode(stream, operations[i], stream.Name)
	}
	branch.Operations = operations
	return branch
}

// operationNode is the planned node one branch operation runs as, read from
// the lowering; operations that plan no node keep their component's name.
func (l *Lowering) operationNode(stream recipeir.Stream, operation PlanOperation, branch string) pipeline.NodeRef {
	if operation.Node != "" {
		return operation.Node
	}
	switch operation.Kind {
	case plan.OpSelect:
		return l.Refs[stream.Root]
	case plan.OpDemux, plan.OpDepacketize:
		return pipeline.NodeRef(l.sourceNames[stream.Input])
	case plan.OpShape:
		return ""
	}
	if operation.hasGraphNode {
		if name, ok := l.nodeNames[operation.graphNode]; ok {
			return pipeline.NodeRef(name)
		}
	}
	if operation.Component != "" {
		return pipeline.NodeRef(operation.Component)
	}
	return pipeline.NodeRef(branch)
}

// tapBranches are the branches whose taps the work plan installs: every
// branch, each tap anchored on the node before it.
func (s *State) tapBranches(l *Lowering, outputs []PlanOutput) []PlanBranch {
	var out []PlanBranch
	for i := range s.Recipe.Streams {
		stream := s.Recipe.Streams[i]
		if stream.Input >= 0 {
			branch, _ := s.planStreamBranch(l, i, outputs)
			out = append(out, branch)
			continue
		}
		out = append(out, s.planJoinBranch(l, i, s.joins[stream.Root], outputs, shape.Spec{}))
	}
	return out
}

func (s *State) inputName(index int) string {
	if index < 0 || index >= len(s.Recipe.Inputs) {
		return "input"
	}
	input := s.Recipe.Inputs[index]
	return cmp.Or(input.Name, input.URI, fmt.Sprintf("input-%d", index))
}

// workStreams are the work plan's streams: the branches of input streams.
func (s *State) workStreams() []WorkStream {
	var out []WorkStream
	for i := range s.Recipe.Streams {
		stream := s.Recipe.Streams[i]
		if stream.Input < 0 {
			continue
		}
		out = append(out, WorkStream{
			Name:         stream.Name,
			Select:       stream.Selector,
			Operations:   workStreamOperations(stream.Operations),
			Destinations: recipeIROutputRefsToStrings(stream.Outputs),
		})
	}
	return out
}

// workStreamOperations are a branch's operations as Explain lists them:
// names and queues are facts of the branch, not operations.
func workStreamOperations(operations []recipeir.Operation) []recipeir.Operation {
	out := make([]recipeir.Operation, 0, len(operations))
	for _, operation := range operations {
		if operation.Kind == recipeir.OpName || operation.Kind == recipeir.OpBuffer {
			continue
		}
		out = append(out, operation)
	}
	return out
}

// selectedStream is the stream a branch of an input selects, when the input's
// streams are known: declared, probed, or a live input's codec intent.
func (s *State) selectedStream(stream recipeir.Stream) (av.Stream, bool) {
	if stream.Input < 0 {
		return av.Stream{}, false
	}
	selector := recipeIRStreamSelector(stream)
	if s.selectionNeedsUnion(stream) {
		selected, ok, err := SelectStreamAcrossInputSets(s.inputStreamSets(), selector, stream.Selector.Input)
		if err != nil || !ok {
			return av.Stream{}, false
		}
		return selected.Stream, true
	}
	input := s.Recipe.Inputs[stream.Input]
	sourceShape, sourceShapeOK := recipeIRInputSourceShape(input)
	if stream.Input < len(s.inputProbes) && len(s.inputProbes[stream.Input].Streams) != 0 {
		probe := s.inputProbes[stream.Input]
		selected, err := SelectDecodeStream(probe.Streams, selector)
		if sourceShapeOK && sourceShape.Domain == shape.DomainFrame {
			selected, err = SelectStream(probe.Streams, selector)
		}
		if err == nil {
			return selected, true
		}
	}
	if streams := LiveInputStreams([]recipeir.Input{input}); len(streams) != 0 {
		if selected, err := SelectDecodeStream(streams, selector); err == nil {
			return selected, true
		}
	}
	if sourceShapeOK {
		if selected, err := SelectStream([]av.Stream{recipeIRInputDeclaredStream(input, sourceShape)}, selector); err == nil {
			return selected, true
		}
	}
	return av.Stream{}, false
}

// joinedShape is the shape of a join's output.
func (p *JoinPlan) joinedShape() shape.Spec {
	return NormalizeTapShape(shape.FromStream(p.Joined, p.joinedDomain))
}

func (p *JoinPlan) joinDetail() string {
	detail := "converge " + fmt.Sprint(len(p.Arms)) + " arms"
	if p.Tree.Sync == JoinSyncPTS {
		detail += " by pts"
	}
	return detail
}

func (p *JoinPlan) joinedWorkOperation(branch string, index int, armShape shape.Spec) WorkOperation {
	return WorkOperation{
		ID:        workOperationIDForKind(branch, index, plan.OpJoin),
		Name:      p.Name,
		Kind:      plan.OpJoin,
		Branch:    branch,
		Node:      pipeline.NodeRef(p.Name),
		Component: p.Name,
		Detail:    p.joinDetail(),
		ShapeIn:   armShape,
		ShapeOut:  p.joinedShape(),
	}
}

// planOutputsWithWorkBranches fills each destination's branch refs from the
// branches that route into it.
func planOutputsWithWorkBranches(outputs []PlanOutput, branches []WorkBranch, ids map[string]string) []PlanOutput {
	for i := range outputs {
		id := ids[outputs[i].Name]
		for j := range branches {
			if stringInSlice(id, branches[j].Destinations) {
				outputs[i].BranchRefs = append(outputs[i].BranchRefs, branches[j].Name)
			}
		}
	}
	return outputs
}

// planRemuxBranches plans the whole-input copies: one packet-copy branch per
// copied input, routed to every destination its Outputs name.
func (s *State) planRemuxBranches(outputs []PlanOutput) ([]PlanBranch, []PlanDecision) {
	var order []int
	dests := map[int][]int{}
	for _, remux := range s.remux {
		if _, ok := dests[remux.input]; !ok {
			order = append(order, remux.input)
		}
		for _, dest := range remux.dests {
			if !slices.Contains(dests[remux.input], dest) {
				dests[remux.input] = append(dests[remux.input], dest)
			}
		}
	}
	var branches []PlanBranch
	var decisions []PlanDecision
	for _, input := range order {
		routed := make([]PlanOutput, 0, len(dests[input]))
		for _, dest := range dests[input] {
			routed = append(routed, outputs[dest])
		}
		inputBranches, inputDecisions := planCopyBranches(s, []recipeir.Input{s.Recipe.Inputs[input]}, true, routed)
		branches = append(branches, inputBranches...)
		decisions = append(decisions, inputDecisions...)
	}
	return branches, decisions
}
