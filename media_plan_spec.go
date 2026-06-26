package goav

import (
	"context"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/recipeir"
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

type mediaPlanMultiStreamJobInput struct {
	runtime      *Runtime
	recipe       recipeir.Recipe
	inputs       []InputSpec
	input        InputSpec
	namedOutputs []namedDestinationSpec
}

type mediaPlanPacketCopyStreamInput struct {
	runtime        *Runtime
	inputs         []InputSpec
	outputs        []destinationSpec
	stream         streamIntent
	selectedStream bool
}

type mediaPlanDecodeStreamInput struct {
	runtime *Runtime
	inputs  []InputSpec
	outputs []destinationSpec
	stream  streamIntent
}

type mediaPlanBranchComposerInput struct {
	runtime *Runtime
	input   InputSpec
	plan    branchComposePlan
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
		Family:    errcode.FamilyForCode(graphPlanInvalidCode),
		Code:      graphPlanInvalidCode,
		Operation: "build graph plan",
		Reason:    reason,
		fields:    buildErrorFields(append([]string(nil), details...)),
		fixes: buildErrorFixes([]string{
			"compile recipes through goav.From(...), chains, branches, and destinations",
			"keep graph-plan nodes, edges, operations, and destinations in sync",
		}),
		cause: errUnsupportedBuild,
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
	work := buildWorkPlan(state, spec, lowerer)
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
// downstream chain hanging off it.
func mediaPlanJoinLowererForState(state *recipeCompileState) (graphPlanLowerer, bool, error) {
	if state == nil || state.joinTree == nil {
		return nil, false, nil
	}
	gp, err := newJoinPlan(joinPlanInputFromCompileState(state))
	if err != nil {
		return nil, false, err
	}
	return gp, true, nil
}

// mediaPlanMultiStreamJobLowererForState lowers a job with several direct
// stream chains (goav.From(camera, mic).Video()...To(out).Audio()...To(out))
// through the branch-compose machinery: each chain is one branch and shared
// Destination handles collapse into one mux group.
func mediaPlanMultiStreamJobLowererForState(state *recipeCompileState) (graphPlanLowerer, bool, error) {
	input, ok := mediaPlanMultiStreamJobInputFromCompileState(state)
	if !ok {
		return nil, false, nil
	}
	return newMediaPlanMultiStreamJobLowerer(input)
}

func mediaPlanMultiStreamJobInputFromCompileState(state *recipeCompileState) (mediaPlanMultiStreamJobInput, bool) {
	if state == nil || !state.jobPresent || len(state.recipe.Streams) < 2 {
		return mediaPlanMultiStreamJobInput{}, false
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
	return mediaPlanMultiStreamJobInput{
		runtime:      state.runtime,
		recipe:       cloneRecipeIRRecipe(state.recipe),
		inputs:       append([]InputSpec(nil), state.inputAttachments...),
		input:        input,
		namedOutputs: cloneNamedDestinationSpecs(namedOutputs),
	}, true
}

func newMediaPlanMultiStreamJobLowerer(input mediaPlanMultiStreamJobInput) (graphPlanLowerer, bool, error) {
	recipe := input.recipe
	if len(recipe.Streams) < 2 {
		return nil, false, nil
	}
	gp, err := planBranchCompositionRecipe(recipe, input.input, input.namedOutputs)
	if err != nil {
		return nil, false, err
	}
	graph, ok, err := newMediaPlanBranchComposeGraph(input.runtime, input.inputs, gp)
	if err != nil || !ok {
		return nil, ok, err
	}
	return graph, true, nil
}

func cloneInputIntents(inputs []inputIntent) []inputIntent {
	if len(inputs) == 0 {
		return nil
	}
	out := make([]inputIntent, 0, len(inputs))
	for i := range inputs {
		input := inputs[i]
		input.Codec = cloneCodecSpec(input.Codec)
		out = append(out, input)
	}
	return out
}

func cloneStreamIntent(stream streamIntent) streamIntent {
	stream.Operations = cloneOperationSpecs(stream.Operations)
	stream.Destinations = append([]string(nil), stream.Destinations...)
	return stream
}

func mediaPlanStreamLowererForState(state *recipeCompileState) (graphPlanLowerer, bool, error) {
	if graph, ok, err := mediaPlanPacketCopyStreamLowererForState(state); err != nil || ok {
		return graph, ok, err
	}
	return mediaPlanDecodeStreamLowererForState(state)
}

func mediaPlanPacketCopyStreamLowererForState(state *recipeCompileState) (graphPlanLowerer, bool, error) {
	input, ok := mediaPlanPacketCopyStreamInputFromCompileState(state)
	if !ok {
		return nil, false, nil
	}
	return newMediaPlanPacketCopyStreamLowerer(input)
}

func mediaPlanPacketCopyStreamInputFromCompileState(state *recipeCompileState) (mediaPlanPacketCopyStreamInput, bool) {
	stream, selectedStream, ok := mediaPlanPacketCopyStream(state)
	if !ok {
		return mediaPlanPacketCopyStreamInput{}, false
	}
	return mediaPlanPacketCopyStreamInput{
		runtime:        state.runtime,
		inputs:         append([]InputSpec(nil), state.inputAttachments...),
		outputs:        cloneDestinationSpecs(state.outputAttachments),
		stream:         cloneStreamIntent(stream),
		selectedStream: selectedStream,
	}, true
}

func newMediaPlanPacketCopyStreamLowerer(input mediaPlanPacketCopyStreamInput) (graphPlanLowerer, bool, error) {
	gp, ok, err := newMediaPlanPacketCopyStreamGraph(input.runtime, input.inputs, input.outputs, input.stream, input.selectedStream)
	if err != nil || !ok {
		return nil, ok, err
	}
	return gp, true, nil
}

func mediaPlanPacketCopyStream(state *recipeCompileState) (streamIntent, bool, bool) {
	if state == nil {
		return streamIntent{}, false, false
	}
	return mediaPlanPacketCopyRecipeStream(state.jobPresent, state.recipe)
}

func mediaPlanPacketCopyRecipeStream(jobPresent bool, recipe recipeir.Recipe) (streamIntent, bool, bool) {
	if !jobPresent {
		return streamIntent{}, false, false
	}
	switch len(recipe.Streams) {
	case 0:
		return streamIntent{}, false, true
	case 1:
		stream := streamIntentFromRecipeIR(recipe.Streams[0])
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
	input, ok := mediaPlanDecodeStreamInputFromCompileState(state)
	if !ok {
		return nil, false, nil
	}
	return newMediaPlanDecodeStreamLowerer(input)
}

func mediaPlanDecodeStreamInputFromCompileState(state *recipeCompileState) (mediaPlanDecodeStreamInput, bool) {
	if state == nil || !state.jobPresent || len(state.recipe.Streams) != 1 {
		return mediaPlanDecodeStreamInput{}, false
	}
	stream := streamIntentFromRecipeIR(state.recipe.Streams[0])
	inputs := append([]InputSpec(nil), state.inputAttachments...)
	outputs := cloneDestinationSpecs(state.outputAttachments)
	if !mediaPlanDecodeStreamShape(stream, outputs, mediaPlanStreamInputDomain(inputs, stream) == shape.DomainFrame) {
		return mediaPlanDecodeStreamInput{}, false
	}
	return mediaPlanDecodeStreamInput{
		runtime: state.runtime,
		inputs:  inputs,
		outputs: outputs,
		stream:  stream,
	}, true
}

func newMediaPlanDecodeStreamLowerer(input mediaPlanDecodeStreamInput) (graphPlanLowerer, bool, error) {
	gp, ok, err := newMediaPlanDecodeStreamGraph(input.runtime, input.inputs, input.outputs, input.stream)
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
	input, ok := mediaPlanBranchComposerInputFromCompileState(state)
	if !ok {
		return nil, false, nil
	}
	return newMediaPlanBranchComposerLowerer(input)
}

func mediaPlanBranchComposerInputFromCompileState(state *recipeCompileState) (mediaPlanBranchComposerInput, bool) {
	if state == nil || !state.branchCompositionPresent {
		return mediaPlanBranchComposerInput{}, false
	}
	return mediaPlanBranchComposerInput{
		runtime: state.runtime,
		input:   state.branchInputAttachment,
		plan:    cloneBranchComposePlan(state.plan),
	}, true
}

func newMediaPlanBranchComposerLowerer(input mediaPlanBranchComposerInput) (graphPlanLowerer, bool, error) {
	gp, ok, err := newMediaPlanBranchComposeGraph(input.runtime, []InputSpec{input.input}, input.plan)
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
