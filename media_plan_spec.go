package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

const (
	graphSpecOriginGraphPlan = "graph_plan"
)

type mediaPlanExecutable interface {
	spec() (pipeline.Spec, error)
	runtimeRef() *runtime
	compile(context.Context, pipeline.Graph, *builder) error
}

type graphPlan struct {
	runtime     *runtime
	name        string
	realtime    bool
	nodes       []pipeline.NodeSpec
	edges       []pipeline.EdgeSpec
	operations  []graphPlanOperation
	inputs      []planInput
	streams     []planStream
	taps        []planTap
	branches    []planBranch
	outputs     []planOutput
	decisions   []planDecision
	diagnostics []PlanDiagnostic
	executable  mediaPlanExecutable
}

type graphPlanOperation struct {
	Branch    string
	Node      pipeline.NodeRef
	Kind      OperationKind
	Component string
	Detail    string
	Caps      StreamCaps
	Targets   []string
	Shared    bool
}

func (p graphPlan) ready() bool {
	return p.executable != nil && p.runtime != nil
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

func newGraphPlan(runtime *runtime, spec pipeline.Spec, plan mediaPlan, executable mediaPlanExecutable) graphPlan {
	return graphPlan{
		runtime:     runtime,
		name:        firstNonEmpty(spec.Name, plan.Name, "goav"),
		realtime:    spec.Realtime,
		nodes:       append([]pipeline.NodeSpec(nil), spec.Nodes...),
		edges:       append([]pipeline.EdgeSpec(nil), spec.Edges...),
		operations:  graphPlanOperationsFromMediaPlan(plan),
		inputs:      clonePlanInputs(plan.Inputs),
		streams:     clonePlanStreams(plan.Streams),
		taps:        clonePlanTaps(plan.Taps),
		branches:    clonePlanBranches(plan.Branches),
		outputs:     clonePlanOutputs(plan.Outputs),
		decisions:   clonePlanDecisions(plan.Decisions),
		diagnostics: clonePlanDiagnostics(plan.Diagnostics),
		executable:  executable,
	}
}

func graphPlanOperationsFromMediaPlan(plan mediaPlan) []graphPlanOperation {
	if len(plan.Branches) == 0 {
		return nil
	}
	outputs := planOutputsByName(plan.Outputs)
	var operations []graphPlanOperation
	for i := range plan.Branches {
		branch := plan.Branches[i]
		for j := range branch.Operations {
			operation := branch.Operations[j]
			operations = append(operations, graphPlanOperation{
				Branch:    branch.Name,
				Node:      pipeline.NodeRef(planOperationNodeName(branch.Name, operation, j)),
				Kind:      operation.Kind,
				Component: operation.Component,
				Detail:    operation.Detail,
				Caps:      operation.Caps,
				Shared:    operation.Shared,
			})
		}
		for _, target := range branch.Outputs {
			output := outputs[target]
			operations = append(operations, graphPlanOperation{
				Branch:    branch.Name,
				Node:      pipeline.NodeRef(firstNonEmpty(output.Name, target)),
				Kind:      output.Operation,
				Component: output.Component,
				Detail:    "target",
				Targets:   []string{target},
			})
		}
	}
	return operations
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
	executable, ok, err := mediaPlanGraph(state)
	if err != nil || !ok {
		return graphPlan{}, ok, err
	}
	spec, err := executable.spec()
	if err != nil {
		return graphPlan{}, false, err
	}
	plan := buildMediaPlan(state)
	return newGraphPlan(executable.runtimeRef(), spec, plan, executable), true, nil
}

func mediaPlanGraph(state *recipeCompileState) (mediaPlanExecutable, bool, error) {
	if graph, ok, err := mediaPlanStreamExecutableForState(state); err != nil || ok {
		return graph, ok, err
	}
	if graph, ok, err := mediaPlanBranchComposerExecutable(state); err != nil || ok {
		return graph, ok, err
	}
	return nil, false, nil
}

func mediaPlanStreamExecutableForState(state *recipeCompileState) (mediaPlanExecutable, bool, error) {
	if graph, ok, err := mediaPlanPacketCopyStreamExecutableForState(state); err != nil || ok {
		return graph, ok, err
	}
	return mediaPlanDecodeStreamExecutableForState(state)
}

func mediaPlanPacketCopyStreamExecutableForState(state *recipeCompileState) (mediaPlanExecutable, bool, error) {
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

func mediaPlanDecodeStreamExecutableForState(state *recipeCompileState) (mediaPlanExecutable, bool, error) {
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

func mediaPlanBranchComposerExecutable(state *recipeCompileState) (mediaPlanExecutable, bool, error) {
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
