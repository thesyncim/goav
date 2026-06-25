package goav

import (
	"context"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

const (
	graphSpecOriginGraphPlan = "graph_plan"
)

type graphPlanLowerer interface {
	spec() (pipeline.Spec, error)
	runtimeRef() *runtime
	lower(context.Context, graphPlan, pipeline.Graph, *builder) error
}

// graphPlan binds the compiled work plan — the single executable truth built
// once by the compile — to the pipeline spec and the lowerer that executes it.
type graphPlan struct {
	runtime  *runtime
	name     string
	realtime bool
	nodes    []pipeline.NodeSpec
	edges    []pipeline.EdgeSpec
	work     workPlan
	lowerer  graphPlanLowerer
}

func (p graphPlan) ready() bool {
	return p.lowerer != nil
}

func (p graphPlan) Describe() (pipeline.Spec, error) {
	if !p.ready() {
		return pipeline.Spec{}, recipeGraphUnsupportedError("describe graph plan", intent{})
	}
	return p.spec(), nil
}

func (p graphPlan) Build(ctx context.Context) (LiveTask, error) {
	if !p.ready() {
		return nil, recipeGraphUnsupportedError("build graph plan", intent{})
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

func (p graphPlan) workPlan() workPlan {
	return cloneWorkPlan(p.work)
}

func newGraphPlan(runtime *runtime, spec pipeline.Spec, work workPlan, lowerer graphPlanLowerer) graphPlan {
	return graphPlan{
		runtime:  runtime,
		name:     work.Name,
		realtime: spec.Realtime,
		nodes:    append([]pipeline.NodeSpec(nil), spec.Nodes...),
		edges:    append([]pipeline.EdgeSpec(nil), spec.Edges...),
		work:     work,
		lowerer:  lowerer,
	}
}

func validateGraphPlanLowering(gp graphPlan) error {
	if !gp.ready() {
		return graphPlanInvalidError("graph plan is not ready", nil)
	}
	if len(gp.nodes) != 0 && len(gp.work.Operations) == 0 {
		return graphPlanInvalidError("graph plan has nodes but no ordered operations", []string{
			"nodes=" + strconv.Itoa(len(gp.nodes)),
		})
	}
	if err := validateGraphPlanEdges(gp); err != nil {
		return err
	}
	for i := range gp.work.Operations {
		operation := gp.work.Operations[i]
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
		if graphPlanOperationDestinationsRequired(operation.Kind) && len(operation.Destinations) == 0 {
			return graphPlanInvalidError("graph-plan destination operation has no destination refs", details)
		}
	}
	return nil
}

func validateGraphPlanEdges(gp graphPlan) error {
	if len(gp.edges) == 0 {
		return nil
	}
	nodes := make(map[pipeline.NodeRef]struct{}, len(gp.nodes))
	for i := range gp.nodes {
		nodes[pipeline.NodeRef(gp.nodes[i].Name)] = struct{}{}
	}
	for i := range gp.edges {
		edge := gp.edges[i]
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

func graphPlanOperationDestinationsRequired(kind plan.OperationKind) bool {
	switch kind {
	case plan.OpMux, plan.OpSink, plan.OpWrite:
		return true
	default:
		return false
	}
}

func graphPlanInvalidError(reason string, details []string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.GraphPlanInvalid),
		Code:      errcode.GraphPlanInvalid,
		Operation: "build graph plan",
		Reason:    reason,
		Fields:    buildErrorFields(append([]string(nil), details...)),
		Fixes: buildErrorFixes([]string{
			"compile recipes through goav.From(...), chains, branches, and destinations",
			"keep graph-plan nodes, edges, operations, and destinations in sync",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func emitGraphPlanSpecPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "emit graph plan spec", fn: func(state *recipeCompileState) error {
		gp, ok, err := graphPlanForState(state)
		if err != nil || !ok {
			return err
		}
		state.spec = gp.spec()
		state.specReady = true
		state.specOrigin = graphSpecOriginGraphPlan
		state.graphPlan = gp
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
	work := buildWorkPlan(state, spec)
	return newGraphPlan(lowerer.runtimeRef(), spec, work, lowerer), true, nil
}

func graphPlanLowererForState(state *recipeCompileState) (graphPlanLowerer, bool, error) {
	if graph, ok, err := mediaPlanJoinLowererForState(state); err != nil || ok {
		return graph, ok, err
	}
	if graph, ok, err := mediaPlanStreamLowererForState(state); err != nil || ok {
		return graph, ok, err
	}
	if graph, ok, err := mediaPlanMultiStreamJobLowererForState(state); err != nil || ok {
		return graph, ok, err
	}
	if graph, ok, err := mediaPlanBranchComposerLowerer(state); err != nil || ok {
		return graph, ok, err
	}
	return nil, false, nil
}

// mediaPlanJoinLowererForState plans a Mix/Composite/Select job as a
// multi-upstream join: N arm sub-chains converging into one join node with the
// downstream chain hanging off it. The planned joinPlan is recorded on the
// state so buildWorkPlan renders the join work plan from the same plan.
func mediaPlanJoinLowererForState(state *recipeCompileState) (graphPlanLowerer, bool, error) {
	if state == nil || state.joinAttachment == nil {
		return nil, false, nil
	}
	rt := state.runtime
	gp, err := newJoinPlan(rt, state)
	if err != nil {
		return nil, false, err
	}
	state.joinPlan = gp
	return gp, true, nil
}

// mediaPlanMultiStreamJobLowererForState lowers a job with several direct
// stream chains (goav.From(camera, mic).Video()...To(out).Audio()...To(out))
// through the branch-compose machinery: each chain is one branch and shared
// Destination handles collapse into one mux group.
func mediaPlanMultiStreamJobLowererForState(state *recipeCompileState) (graphPlanLowerer, bool, error) {
	if state == nil || !state.jobPresent || len(state.intent.Streams) < 2 {
		return nil, false, nil
	}
	namedOutputs := make([]namedDestinationSpec, 0, len(state.outputAttachments))
	for i := range state.outputAttachments {
		namedOutputs = append(namedOutputs, namedDestinationSpec{
			name:   jobOutputDestinationName(state.outputAttachments, state.outputDestinationNames, i),
			output: state.outputAttachments[i],
		})
	}
	input := InputSpec{}
	if len(state.inputAttachments) != 0 {
		input = state.inputAttachments[0]
	}
	gp, err := planBranchCompositionRecipe(state.intent, input, namedOutputs)
	if err != nil {
		return nil, false, err
	}
	graph, ok, err := newMediaPlanBranchComposeGraph(state.runtime, state.inputAttachments, gp)
	if err != nil || !ok {
		return nil, ok, err
	}
	return graph, true, nil
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
	gp, ok, err := newMediaPlanPacketCopyStreamGraph(state.runtime, state.inputAttachments, state.outputAttachments, stream, selectedStream)
	if err != nil || !ok {
		return nil, ok, err
	}
	return gp, true, nil
}

func mediaPlanPacketCopyStream(state *recipeCompileState) (streamIntent, bool, bool) {
	if state == nil {
		return streamIntent{}, false, false
	}
	return mediaPlanPacketCopyIntentStream(state.jobPresent, state.intent)
}

func mediaPlanPacketCopyIntentStream(jobPresent bool, intent intent) (streamIntent, bool, bool) {
	if !jobPresent {
		return streamIntent{}, false, false
	}
	switch len(intent.Streams) {
	case 0:
		return streamIntent{}, false, true
	case 1:
		stream := intent.Streams[0]
		if streamIntentPacketCopyOnly(stream) {
			return stream, true, true
		}
	}
	return streamIntent{}, false, false
}

func streamIntentPacketCopyOnly(stream streamIntent) bool {
	encode := chainEncodeSpec(stream.Operations)
	if !encode.Copy || chainHasDecode(stream.Operations) || encode.ID != "" || encode.Auto {
		return false
	}
	hasCopy := false
	for i := range stream.Operations {
		op := stream.Operations[i]
		switch op.Kind {
		case plan.OpCopy:
			if !op.Encode.Copy {
				return false
			}
			hasCopy = true
		case plan.OpTap:
			if op.Tap.Domain != shape.DomainPacket || op.Tap.After != plan.OpCopy {
				return false
			}
		case plan.OpShape:
			// Annotation carriers (.Auto/.Require/.Prefer) lower to nothing;
			// they do not change a packet-copy-only chain.
			if !operationSpecIsAnnotation(op) {
				return false
			}
		default:
			return false
		}
	}
	return hasCopy
}

func mediaPlanDecodeStreamLowererForState(state *recipeCompileState) (graphPlanLowerer, bool, error) {
	if state == nil || !state.jobPresent || len(state.intent.Streams) != 1 {
		return nil, false, nil
	}
	stream := state.intent.Streams[0]
	if !mediaPlanDecodeStreamShape(stream, state.outputAttachments, mediaPlanStreamInputDomain(state.inputAttachments, stream) == shape.DomainFrame) {
		return nil, false, nil
	}
	gp, ok, err := newMediaPlanDecodeStreamGraph(state.runtime, state.inputAttachments, state.outputAttachments, stream)
	if err != nil || !ok {
		return nil, ok, err
	}
	return gp, true, nil
}

func mediaPlanDecodeStreamShape(stream streamIntent, outputs []destinationSpec, frameSource bool) bool {
	return mediaPlanSinkDestinationShape(stream, outputs, frameSource) || mediaPlanEncodeShape(stream, outputs, frameSource)
}

func mediaPlanSinkDestinationShape(stream streamIntent, outputs []destinationSpec, frameSource bool) bool {
	return len(outputs) == 1 &&
		outputs[0].sink != nil &&
		(chainHasDecode(stream.Operations) || frameSource) &&
		len(stream.Destinations) == 1 &&
		!chainEncodeSpec(stream.Operations).Copy
}

func mediaPlanEncodeShape(stream streamIntent, outputs []destinationSpec, frameSource bool) bool {
	encode := chainEncodeSpec(stream.Operations)
	if (!chainHasDecode(stream.Operations) && !frameSource) || !codecIntentSet(encode) || encode.Copy || len(outputs) == 0 {
		return false
	}
	return len(stream.Destinations) == len(outputs)
}

func mediaPlanBranchComposerLowerer(state *recipeCompileState) (graphPlanLowerer, bool, error) {
	if state == nil || !state.branchCompositionPresent {
		return nil, false, nil
	}
	gp, ok, err := newMediaPlanBranchComposeGraph(state.runtime, []InputSpec{state.branchInputAttachment}, state.plan)
	if err != nil || !ok {
		return nil, ok, err
	}
	return gp, true, nil
}

func mediaPlanSourceSpecs(spec *pipeline.Spec, nodes map[string]plannedNode, inputs []InputSpec) ([]pipeline.NodeRef, bool, error) {
	if !mediaPlanStreamInputsSupported(inputs) {
		return nil, false, nil
	}
	refs := make([]pipeline.NodeRef, 0, len(inputs))
	names := graphSourceNodeNames(inputs)
	for i := range inputs {
		name := names[i]
		ref := pipeline.NodeRef(name)
		if err := addPlannedNode(nodes, spec, name, pipeline.NodeSource, ref, inputs[i].graphSourceNodeDetail()); err != nil {
			return nil, false, err
		}
		refs = append(refs, ref)
	}
	return refs, true, nil
}

func mediaPlanPacketCopyDestinations(spec *pipeline.Spec, nodes map[string]plannedNode, outputs []destinationSpec) ([]pipeline.NodeRef, error) {
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

func destinationGraphFormat(output destinationSpec) av.FormatID {
	return output.format
}

func destinationOpenFormat(output destinationSpec) av.FormatID {
	return destinationSpecFormat(output)
}
