package goav

import (
	"context"
	"strconv"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

const (
	graphSpecOriginGraphPlan = "graph_plan"
)

type graphPlanLowerer interface {
	spec() (pipeline.Spec, error)
	runtimeRef() *runtime
	lower(context.Context, graphPlan, pipeline.Graph, *builder) error
}

type graphPlan struct {
	runtime     *runtime
	name        string
	realtime    bool
	nodes       []pipeline.NodeSpec
	edges       []pipeline.EdgeSpec
	operations  []graphPlanOperation
	work        workPlan
	inputs      []planInput
	streams     []planStream
	taps        []planTap
	branches    []planBranch
	outputs     []planOutput
	decisions   []planDecision
	diagnostics []PlanDiagnostic
	lowerer     graphPlanLowerer
}

type graphPlanOperation struct {
	Branch    string
	Node      pipeline.NodeRef
	Kind      OperationKind
	Component string
	Detail    string
	Shape     MediaShape
	Targets   []string
	Shared    bool
}

func (p graphPlan) ready() bool {
	return p.lowerer != nil && p.runtime != nil
}

func (p graphPlan) Describe() (pipeline.Spec, error) {
	if !p.ready() {
		return pipeline.Spec{}, recipeGraphUnsupportedError("describe graph plan", Intent{})
	}
	return p.spec(), nil
}

func (p graphPlan) Build(ctx context.Context) (Task, error) {
	if !p.ready() {
		return nil, recipeGraphUnsupportedError("build graph plan", Intent{})
	}
	return buildGraphPlanTask(ctx, p)
}

func (p graphPlan) lower(ctx context.Context, graph pipeline.Graph, service *builder) error {
	if err := validateGraphPlanLowering(p); err != nil {
		return err
	}
	return p.lowerer.lower(ctx, p, graph, service)
}

func (p graphPlan) spec() pipeline.Spec {
	return pipeline.Spec{
		Name:     p.name,
		Realtime: p.realtime,
		Nodes:    append([]pipeline.NodeSpec(nil), p.nodes...),
		Edges:    append([]pipeline.EdgeSpec(nil), p.edges...),
	}
}

func (p graphPlan) mediaPlan() mediaPlan {
	return mediaPlan{
		Name:        p.name,
		Inputs:      clonePlanInputs(p.inputs),
		Streams:     clonePlanStreams(p.streams),
		Taps:        clonePlanTaps(p.taps),
		Branches:    clonePlanBranches(p.branches),
		Outputs:     clonePlanOutputs(p.outputs),
		Decisions:   clonePlanDecisions(p.decisions),
		Diagnostics: clonePlanDiagnostics(p.diagnostics),
	}
}

func (p graphPlan) operationPlan() []graphPlanOperation {
	return cloneGraphPlanOperations(p.operations)
}

func (p graphPlan) workPlan() workPlan {
	return cloneWorkPlan(p.work)
}

func newGraphPlan(runtime *runtime, spec pipeline.Spec, plan mediaPlan, lowerer graphPlanLowerer) graphPlan {
	operations := graphPlanOperationsFromMediaPlan(spec, plan)
	return graphPlan{
		runtime:     runtime,
		name:        firstNonEmpty(spec.Name, plan.Name, "goav"),
		realtime:    spec.Realtime,
		nodes:       append([]pipeline.NodeSpec(nil), spec.Nodes...),
		edges:       append([]pipeline.EdgeSpec(nil), spec.Edges...),
		operations:  operations,
		work:        workPlanFromMediaPlan(spec, plan, operations),
		inputs:      clonePlanInputs(plan.Inputs),
		streams:     clonePlanStreams(plan.Streams),
		taps:        clonePlanTaps(plan.Taps),
		branches:    clonePlanBranches(plan.Branches),
		outputs:     clonePlanOutputs(plan.Outputs),
		decisions:   clonePlanDecisions(plan.Decisions),
		diagnostics: clonePlanDiagnostics(plan.Diagnostics),
		lowerer:     lowerer,
	}
}

func graphPlanOperationsFromMediaPlan(spec pipeline.Spec, plan mediaPlan) []graphPlanOperation {
	if len(plan.Branches) == 0 {
		return nil
	}
	outputs := planOutputsByName(plan.Outputs)
	outputNodes := graphPlanOutputNodesByName(spec, plan.Outputs)
	var operations []graphPlanOperation
	for i := range plan.Branches {
		branch := plan.Branches[i]
		for j := range branch.Operations {
			operation := branch.Operations[j]
			node := pipeline.NodeRef(planOperationNodeName(branch, operation, j))
			if operation.Kind == OpShape {
				node = ""
			}
			operations = append(operations, graphPlanOperation{
				Branch:    branch.Name,
				Node:      node,
				Kind:      operation.Kind,
				Component: operation.Component,
				Detail:    operation.Detail,
				Shape:     operation.Shape,
				Shared:    operation.Shared,
			})
		}
		for _, target := range branch.Outputs {
			output := outputs[target]
			node := outputNodes[target]
			if node == "" {
				node = pipeline.NodeRef(firstNonEmpty(output.Name, target))
			}
			operations = append(operations, graphPlanOperation{
				Branch:    branch.Name,
				Node:      node,
				Kind:      output.Operation,
				Component: output.Component,
				Detail:    "target",
				Targets:   []string{target},
			})
		}
	}
	return operations
}

func graphPlanOutputNodesByName(spec pipeline.Spec, outputs []planOutput) map[string]pipeline.NodeRef {
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
			if !graphPlanNodeMatchesOutput(spec.Nodes[j], output) {
				continue
			}
			out[output.Name] = pipeline.NodeRef(spec.Nodes[j].Name)
			used[j] = struct{}{}
			break
		}
	}
	return out
}

func graphPlanNodeMatchesOutput(node pipeline.NodeSpec, output planOutput) bool {
	switch output.Operation {
	case OpSink:
		return node.Kind == pipeline.NodeSink
	case OpMux, OpWrite:
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

func cloneGraphPlanOperations(operations []graphPlanOperation) []graphPlanOperation {
	if len(operations) == 0 {
		return nil
	}
	out := make([]graphPlanOperation, 0, len(operations))
	for i := range operations {
		operation := operations[i]
		operation.Targets = append([]string(nil), operation.Targets...)
		out = append(out, operation)
	}
	return out
}

func validateGraphPlanLowering(plan graphPlan) error {
	if !plan.ready() {
		return graphPlanInvalidError("graph plan is not ready", nil)
	}
	if len(plan.nodes) != 0 && len(plan.operations) == 0 {
		return graphPlanInvalidError("graph plan has nodes but no ordered operations", []string{
			"nodes=" + strconv.Itoa(len(plan.nodes)),
		})
	}
	if err := validateGraphPlanEdges(plan); err != nil {
		return err
	}
	for i := range plan.operations {
		operation := plan.operations[i]
		details := []string{
			"operation=" + strconv.Itoa(i),
			"branch=" + operation.Branch,
			"node=" + operation.Node.String(),
			"kind=" + string(operation.Kind),
		}
		if operation.Kind == "" {
			return graphPlanInvalidError("graph-plan operation has no kind", details)
		}
		if operation.Branch == "" {
			return graphPlanInvalidError("graph-plan operation has no branch", details)
		}
		if graphPlanOperationTargetsRequired(operation.Kind) && len(operation.Targets) == 0 {
			return graphPlanInvalidError("graph-plan target operation has no target refs", details)
		}
	}
	return nil
}

func validateGraphPlanEdges(plan graphPlan) error {
	if len(plan.edges) == 0 {
		return nil
	}
	nodes := make(map[pipeline.NodeRef]struct{}, len(plan.nodes))
	for i := range plan.nodes {
		nodes[pipeline.NodeRef(plan.nodes[i].Name)] = struct{}{}
	}
	for i := range plan.edges {
		edge := plan.edges[i]
		details := []string{
			"edge=" + strconv.Itoa(i),
			"from=" + edge.From.String(),
			"to=" + edge.To.String(),
		}
		if edge.From == "" || edge.To == "" {
			return graphPlanInvalidError("graph-plan edge is missing a source or target", details)
		}
		if _, ok := nodes[edge.From]; !ok {
			return graphPlanInvalidError("graph-plan edge source is not a planned node", details)
		}
		if _, ok := nodes[edge.To]; !ok {
			return graphPlanInvalidError("graph-plan edge target is not a planned node", details)
		}
	}
	return nil
}

func graphPlanOperationTargetsRequired(kind OperationKind) bool {
	switch kind {
	case OpMux, OpSink, OpWrite:
		return true
	default:
		return false
	}
}

func graphPlanInvalidError(reason string, details []string) error {
	return &BuildError{
		Code:      "graph_plan_invalid",
		Operation: "build graph plan",
		Reason:    reason,
		Details:   append([]string(nil), details...),
		Suggestions: []string{
			"compile recipes through goav.From(...), chains, branches, and destinations",
			"keep graph-plan nodes, edges, operations, and destinations in sync",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func emitGraphPlanSpecPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "emit graph plan spec", fn: func(state *recipeCompileState) error {
		plan, ok, err := graphPlanForState(state)
		if err != nil || !ok {
			return err
		}
		state.spec = plan.spec()
		state.specReady = true
		state.specOrigin = graphSpecOriginGraphPlan
		state.graphPlan = plan
		return nil
	}}
}

func graphPlanForState(state *recipeCompileState) (graphPlan, bool, error) {
	lowerer, ok, err := graphPlanLowererForState(state)
	if err != nil || !ok {
		return graphPlan{}, ok, err
	}
	spec, err := lowerer.spec()
	if err != nil {
		return graphPlan{}, false, err
	}
	plan := buildMediaPlan(state)
	return newGraphPlan(lowerer.runtimeRef(), spec, plan, lowerer), true, nil
}

func graphPlanLowererForState(state *recipeCompileState) (graphPlanLowerer, bool, error) {
	if graph, ok, err := mediaPlanStreamLowererForState(state); err != nil || ok {
		return graph, ok, err
	}
	if graph, ok, err := mediaPlanBranchComposerLowerer(state); err != nil || ok {
		return graph, ok, err
	}
	return nil, false, nil
}

func mediaPlanStreamLowererForState(state *recipeCompileState) (graphPlanLowerer, bool, error) {
	if graph, ok, err := mediaPlanPacketCopyStreamLowererForState(state); err != nil || ok {
		return graph, ok, err
	}
	return mediaPlanDecodeStreamLowererForState(state)
}

func mediaPlanPacketCopyStreamLowererForState(state *recipeCompileState) (graphPlanLowerer, bool, error) {
	stream, selectedStream, ok := mediaPlanPacketCopyStream(state)
	if !ok {
		return nil, false, nil
	}
	plan, ok, err := newMediaPlanPacketCopyStreamGraph(state.runtime, state.inputAttachments, state.outputAttachments, stream, selectedStream)
	if err != nil || !ok {
		return nil, ok, err
	}
	return plan, true, nil
}

func mediaPlanPacketCopyStream(state *recipeCompileState) (StreamIntent, bool, bool) {
	if state == nil {
		return StreamIntent{}, false, false
	}
	return mediaPlanPacketCopyIntentStream(state.jobPresent, state.intent, state.chainSteps)
}

func mediaPlanPacketCopyIntentStream(jobPresent bool, intent Intent, chainSteps []chainStepAttachment) (StreamIntent, bool, bool) {
	if !jobPresent {
		return StreamIntent{}, false, false
	}
	switch len(intent.Streams) {
	case 0:
		return StreamIntent{}, false, true
	case 1:
		stream := intent.Streams[0]
		if stream.Encode.Copy && !stream.Decode && stream.Encode.ID == "" && !stream.Encode.Auto && len(chainSteps) == 0 {
			return stream, true, true
		}
	}
	return StreamIntent{}, false, false
}

func mediaPlanDecodeStreamLowererForState(state *recipeCompileState) (graphPlanLowerer, bool, error) {
	if state == nil || !state.jobPresent || len(state.intent.Streams) != 1 {
		return nil, false, nil
	}
	stream := state.intent.Streams[0]
	if !mediaPlanDecodeStreamShape(stream, state.outputAttachments) {
		return nil, false, nil
	}
	plan, ok, err := newMediaPlanDecodeStreamGraph(state.runtime, state.inputAttachments, state.outputAttachments, stream)
	if err != nil || !ok {
		return nil, ok, err
	}
	return plan, true, nil
}

func mediaPlanDecodeStreamShape(stream StreamIntent, outputs []destinationSpec) bool {
	return mediaPlanSinkDestinationShape(stream, outputs) || mediaPlanEncodeShape(stream, outputs)
}

func mediaPlanSinkDestinationShape(stream StreamIntent, outputs []destinationSpec) bool {
	return len(outputs) == 1 &&
		outputs[0].sink != nil &&
		stream.Decode &&
		len(stream.Targets) == 1 &&
		!stream.Encode.Copy
}

func mediaPlanEncodeShape(stream StreamIntent, outputs []destinationSpec) bool {
	if !stream.Decode || !codecIntentSet(stream.Encode) || stream.Encode.Copy || len(outputs) == 0 {
		return false
	}
	return len(stream.Targets) == len(outputs)
}

func mediaPlanBranchComposerLowerer(state *recipeCompileState) (graphPlanLowerer, bool, error) {
	if state == nil || !state.branchCompositionPresent {
		return nil, false, nil
	}
	plan, ok, err := newMediaPlanBranchComposeGraph(state.runtime, state.branchInputAttachment, state.plan)
	if err != nil || !ok {
		return nil, ok, err
	}
	return plan, true, nil
}

func mediaPlanSourceSpecs(spec *pipeline.Spec, nodes map[string]plannedNode, inputs []InputSpec) ([]pipeline.NodeRef, bool, error) {
	if len(inputs) == 1 && inputs[0].rtp == nil {
		input := inputs[0].formatInput()
		name := demuxNodeName(input)
		ref := pipeline.NodeRef(name)
		if err := addPlannedNode(nodes, spec, name, pipeline.NodeSource, ref, inputNodeDetail(input)); err != nil {
			return nil, false, err
		}
		return []pipeline.NodeRef{ref}, true, nil
	}
	if !allRTPInputSpecs(inputs) {
		return nil, false, nil
	}
	refs := make([]pipeline.NodeRef, 0, len(inputs))
	for i := range inputs {
		input := inputs[i].rtpBuildInput()
		name := rtpNodeName(input, i)
		ref := pipeline.NodeRef(name)
		if err := addPlannedNode(nodes, spec, name, pipeline.NodeSource, ref, rtpInputDetail(input)); err != nil {
			return nil, false, err
		}
		refs = append(refs, ref)
	}
	return refs, true, nil
}

func mediaPlanPacketCopyTargets(spec *pipeline.Spec, nodes map[string]plannedNode, outputs []destinationSpec) ([]pipeline.NodeRef, error) {
	refs := make([]pipeline.NodeRef, 0, len(outputs))
	for i := range outputs {
		if outputs[i].sink != nil {
			name := firstNonEmpty(outputs[i].sink.Name(), outputs[i].label("sink"))
			ref := pipeline.NodeRef(name)
			if err := addPlannedNode(nodes, spec, name, pipeline.NodeSink, ref, describedNodeDetail(outputs[i].sink)); err != nil {
				return nil, err
			}
			refs = append(refs, ref)
			continue
		}
		output := outputs[i].output
		name := muxNodeName(output, i)
		ref := pipeline.NodeRef(name)
		detail := outputNodeDetailWithFormat(output, destinationGraphFormat(outputs[i]))
		if err := addPlannedNode(nodes, spec, name, pipeline.NodeStage, ref, detail); err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	return refs, nil
}

func allRTPInputSpecs(inputs []InputSpec) bool {
	if len(inputs) == 0 {
		return false
	}
	for i := range inputs {
		if inputs[i].rtp == nil {
			return false
		}
	}
	return true
}

func destinationGraphFormat(output destinationSpec) av.FormatID {
	return output.format
}

func destinationOpenFormat(output destinationSpec) av.FormatID {
	return destinationSpecFormat(output)
}
