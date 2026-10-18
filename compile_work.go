package goav

import (
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
func buildRecipeWorkPlan(s *recipeCompileState, l *recipeLowering, spec pipeline.Spec) workPlan {
	outputs := planOutputsFromRecipeIR(s.recipe.Destinations, s.outputFormatMap())
	var branches []planBranch
	var decisions []planDecision
	var operations []workOperation
	var workBranches []workBranch
	ids := map[string]string{}
	for _, output := range outputs {
		ids[output.Name] = workDestinationID(output.Name)
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
	var taps []workTap
	var diagnostics []plan.Diagnostic
	remuxBranches, remuxDecisions := s.planRemuxBranches(outputs)
	branches = append(branches, remuxBranches...)
	decisions = append(decisions, remuxDecisions...)
	for i := range s.recipe.Streams {
		stream := s.recipe.Streams[i]
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
			if !s.facts[i].lone {
				join := p.joinedWorkOperation(p.name, 0, armShape)
				operations = append(operations, join)
				workBranches = append(workBranches, workBranch{
					ID:          workBranchID(p.name, len(workBranches)),
					Name:        p.name,
					Stream:      streamSelectFromStream(p.joined),
					SourceShape: p.joinedShape(),
					Operations:  []string{join.ID},
				})
			}
		}
		branches = append(branches, s.planJoinBranch(l, i, p, outputs, armShapes[stream.Root]))
	}
	flush()
	work := workPlan{
		Name:         firstNonEmpty(spec.Name, "goav"),
		Realtime:     spec.Realtime,
		Inputs:       workInputsFromRecipeIR(s.recipe.Inputs),
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
func (s *recipeCompileState) planStreamBranch(l *recipeLowering, index int, outputs []planOutput) (planBranch, []planDecision) {
	stream := s.recipe.Streams[index]
	input := s.recipe.Inputs[stream.Input]
	var spec shape.Spec
	sourceShape, sourceShapeOK := recipeIRInputSourceShape(input)
	plannedStream := stream
	if selected, ok := s.selectedStream(stream); ok {
		resolved := streamSelectFromStream(selected)
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
	name := firstNonEmpty(plannedStream.Name, string(plannedStream.Selector.Type), fmt.Sprintf("branch-%d", index))
	operations, decisions := planRecipeIROperationSpecs(input, plannedStream, name, spec, selectorComponent(stream.Selector))
	operations = planOperationsWithShape(name, spec, operations)
	for i := range operations {
		operations[i].Node = l.operationNode(stream, operations[i], name)
	}
	if latency, ok := planPathLatencyDecision(s.runtime, name, plannedStream.Operations); ok {
		decisions = append(decisions, latency)
	}
	return planBranch{
		Name:       name,
		Input:      s.inputName(stream.Input),
		Stream:     plannedStream.Selector,
		Shape:      spec,
		Operations: operations,
		Outputs:    planBranchDestinations(recipeIROutputRefsToStrings(stream.Outputs), outputs),
	}, decisions
}

// planJoinBranch plans one branch of a join's output: a lone branch carries
// the join's convergence, a fork reads the joined stream.
func (s *recipeCompileState) planJoinBranch(l *recipeLowering, index int, p *joinPlan, outputs []planOutput, armShape shape.Spec) planBranch {
	stream := s.recipe.Streams[index]
	joined := p.joinedShape()
	branch := planBranch{
		Name:    stream.Name,
		Input:   p.name,
		Stream:  streamSelectFromStream(p.joined),
		Shape:   joined,
		Outputs: planBranchDestinations(recipeIROutputRefsToStrings(stream.Outputs), outputs),
	}
	var operations []planOperation
	if s.facts[index].lone {
		branch.Input = ""
		operations = append(operations, planOperation{
			Kind:      plan.OpJoin,
			Component: p.name,
			Detail:    p.joinDetail(),
			Shape:     joined,
			Node:      pipeline.NodeRef(p.name),
			ShapeIn:   armShape,
		})
	}
	for _, operation := range stream.Operations {
		operations = append(operations, planOperationFromRecipeIROperation(operation))
	}
	start := 0
	if s.facts[index].lone {
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
func (l *recipeLowering) operationNode(stream recipeir.Stream, operation planOperation, branch string) pipeline.NodeRef {
	if operation.Node != "" {
		return operation.Node
	}
	switch operation.Kind {
	case plan.OpSelect:
		return l.refs[stream.Root]
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
func (s *recipeCompileState) tapBranches(l *recipeLowering, outputs []planOutput) []planBranch {
	var out []planBranch
	for i := range s.recipe.Streams {
		stream := s.recipe.Streams[i]
		if stream.Input >= 0 {
			branch, _ := s.planStreamBranch(l, i, outputs)
			out = append(out, branch)
			continue
		}
		out = append(out, s.planJoinBranch(l, i, s.joins[stream.Root], outputs, shape.Spec{}))
	}
	return out
}

func (s *recipeCompileState) inputName(index int) string {
	if index < 0 || index >= len(s.recipe.Inputs) {
		return "input"
	}
	input := s.recipe.Inputs[index]
	return firstNonEmpty(input.Name, input.URI, fmt.Sprintf("input-%d", index))
}

// workStreams are the work plan's streams: the branches of input streams.
func (s *recipeCompileState) workStreams() []workStream {
	var out []workStream
	for i := range s.recipe.Streams {
		stream := s.recipe.Streams[i]
		if stream.Input < 0 {
			continue
		}
		out = append(out, workStream{
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
func (s *recipeCompileState) selectedStream(stream recipeir.Stream) (av.Stream, bool) {
	if stream.Input < 0 {
		return av.Stream{}, false
	}
	selector := recipeIRStreamSelector(stream)
	if s.selectionNeedsUnion(stream) {
		selected, ok, err := selectStreamAcrossInputSets(s.inputStreamSets(), selector, stream.Selector.Input)
		if err != nil || !ok {
			return av.Stream{}, false
		}
		return selected.stream, true
	}
	input := s.recipe.Inputs[stream.Input]
	sourceShape, sourceShapeOK := recipeIRInputSourceShape(input)
	if stream.Input < len(s.inputProbes) && len(s.inputProbes[stream.Input].Streams) != 0 {
		probe := s.inputProbes[stream.Input]
		selected, err := selectDecodeStream(probe.Streams, selector)
		if sourceShapeOK && sourceShape.Domain == shape.DomainFrame {
			selected, err = selectStream(probe.Streams, selector)
		}
		if err == nil {
			return selected, true
		}
	}
	if streams := liveIntentStreams([]recipeir.Input{input}); len(streams) != 0 {
		if selected, err := selectDecodeStream(streams, selector); err == nil {
			return selected, true
		}
	}
	if sourceShapeOK {
		if selected, err := selectStream([]av.Stream{recipeIRInputDeclaredStream(input, sourceShape)}, selector); err == nil {
			return selected, true
		}
	}
	return av.Stream{}, false
}

// joinedShape is the shape of a join's output.
func (p *joinPlan) joinedShape() shape.Spec {
	return normalizeTapShape(shape.FromStream(p.joined, p.joinedDomain))
}

func (p *joinPlan) joinDetail() string {
	detail := "converge " + fmt.Sprint(len(p.arms)) + " arms"
	if p.tree.sync == joinSyncPTS {
		detail += " by pts"
	}
	return detail
}

func (p *joinPlan) joinedWorkOperation(branch string, index int, armShape shape.Spec) workOperation {
	return workOperation{
		ID:        workOperationIDForKind(branch, index, plan.OpJoin),
		Name:      p.name,
		Kind:      plan.OpJoin,
		Branch:    branch,
		Node:      pipeline.NodeRef(p.name),
		Component: p.name,
		Detail:    p.joinDetail(),
		ShapeIn:   armShape,
		ShapeOut:  p.joinedShape(),
	}
}

// planOutputsWithWorkBranches fills each destination's branch refs from the
// branches that route into it.
func planOutputsWithWorkBranches(outputs []planOutput, branches []workBranch, ids map[string]string) []planOutput {
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
func (s *recipeCompileState) planRemuxBranches(outputs []planOutput) ([]planBranch, []planDecision) {
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
	var branches []planBranch
	var decisions []planDecision
	for _, input := range order {
		routed := make([]planOutput, 0, len(dests[input]))
		for _, dest := range dests[input] {
			routed = append(routed, outputs[dest])
		}
		inputBranches, inputDecisions := planCopyBranches(s, []recipeir.Input{s.recipe.Inputs[input]}, true, routed)
		branches = append(branches, inputBranches...)
		decisions = append(decisions, inputDecisions...)
	}
	return branches, decisions
}
