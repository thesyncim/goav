package goav

import (
	"context"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
)

func (r recipeResolved) singleStreamIntent() (StreamIntent, bool) {
	if len(r.intent.Streams) != 1 {
		return StreamIntent{}, false
	}
	return r.intent.Streams[0], true
}

type mediaPlanStreamGraph struct {
	runtime        *runtime
	inputs         []InputSpec
	outputs        []destinationSpec
	stream         StreamIntent
	copyPackets    bool
	selectedStream bool
	decode         decodeRequest
	filters        []filterRequest
	encode         *encodeRequest
}

type mediaPlanBranchComposeGraph struct {
	runtime  *runtime
	input    InputSpec
	plan     branchComposePlan
	branches []branchComposeRoute
	targets  []branchComposeTargetRoute
}

type mediaPlanCompiledSources struct {
	refs      []pipeline.NodeRef
	streams   []av.Stream
	rtpBuilds []rtpBuild
	realtime  bool
}

func buildGraphPlanTask(ctx context.Context, plan graphPlan) (Task, error) {
	runtime := plan.runtime
	if runtime == nil {
		return nil, recipeGraphUnsupportedError("build recipe", Intent{})
	}
	service := &builder{runtime: runtime, requireRunOK: true}
	graph, err := service.newGraph(ctx)
	if err != nil {
		return nil, err
	}
	if err := plan.lower(ctx, graph, service); err != nil {
		graph.Close()
		return nil, err
	}
	return newTask(graph, runtime, service.destinationTxs...), nil
}

func (p mediaPlanStreamGraph) spec() (pipeline.Spec, error) {
	if p.copyPackets {
		return p.packetCopySpec()
	}
	if p.hasSingleSinkDestination() {
		return p.sinkDestinationSpec()
	}
	return p.encodeOutputSpec()
}

func (p mediaPlanStreamGraph) runtimeRef() *runtime {
	return p.runtime
}

func (p mediaPlanStreamGraph) lower(ctx context.Context, plan graphPlan, graph pipeline.Graph, service *builder) error {
	if p.copyPackets {
		return p.lowerPacketCopy(ctx, plan, graph, service)
	}
	lowering, err := p.prepareFrameStreamOperationLowering(plan)
	if err != nil {
		return err
	}
	if p.hasSingleSinkDestination() {
		return p.compileSinkDestination(ctx, graph, lowering)
	}
	return p.compileEncodeOutput(ctx, graph, service, lowering)
}

func (p mediaPlanStreamGraph) hasSingleSinkDestination() bool {
	return len(p.outputs) == 1 && p.outputs[0].sink != nil
}

func newMediaPlanDecodeStreamGraph(rt Runtime, inputs []InputSpec, outputs []destinationSpec, stream StreamIntent) (mediaPlanStreamGraph, bool, error) {
	runtime, ok := rt.(*runtime)
	if !ok || runtime == nil {
		return mediaPlanStreamGraph{}, false, nil
	}
	if !mediaPlanStreamInputsSupported(inputs) {
		return mediaPlanStreamGraph{}, false, nil
	}
	selector := streamIntentSelector(stream)
	plan := mediaPlanStreamGraph{
		runtime: runtime,
		inputs:  append([]InputSpec(nil), inputs...),
		outputs: append([]destinationSpec(nil), outputs...),
		stream:  stream,
		decode: decodeRequest{
			selector:    selector,
			codecChange: stream.CodecChange,
			config:      cloneCodecSpec(stream.DecodeCodec),
		},
	}
	filters, err := mediaPlanStreamFilters(stream)
	if err != nil {
		return mediaPlanStreamGraph{}, false, err
	}
	plan.filters = filters
	if codecIntentSet(stream.Encode) && !stream.Encode.Copy {
		request := encodeRequest{
			selector: selector,
			config:   encodeConfigFromSpec(stream.Encode),
		}
		plan.encode = &request
	}
	return plan, true, nil
}

func mediaPlanStreamInputsSupported(inputs []InputSpec) bool {
	if len(inputs) == 1 && inputs[0].rtp == nil {
		return true
	}
	return allRTPInputSpecs(inputs)
}

func newMediaPlanPacketCopyStreamGraph(rt Runtime, inputs []InputSpec, outputs []destinationSpec, stream StreamIntent, selectedStream bool) (mediaPlanStreamGraph, bool, error) {
	runtime, ok := rt.(*runtime)
	if !ok || runtime == nil {
		return mediaPlanStreamGraph{}, false, nil
	}
	if len(inputs) == 0 || len(outputs) == 0 || !mediaPlanStreamInputsSupported(inputs) {
		return mediaPlanStreamGraph{}, false, nil
	}
	return mediaPlanStreamGraph{
		runtime:        runtime,
		inputs:         append([]InputSpec(nil), inputs...),
		outputs:        append([]destinationSpec(nil), outputs...),
		stream:         stream,
		copyPackets:    true,
		selectedStream: selectedStream,
	}, true, nil
}

func (p mediaPlanStreamGraph) packetCopySpec() (pipeline.Spec, error) {
	spec := pipeline.Spec{Name: "goav", Realtime: p.runtime.realtime}
	nodes := make(map[string]plannedNode, len(p.inputs)+len(p.outputs)+1)
	sourceRefs, ok, err := mediaPlanSourceSpecs(&spec, nodes, p.inputs)
	if err != nil {
		return pipeline.Spec{}, err
	}
	if !ok {
		return pipeline.Spec{}, recipeGraphUnsupportedError("describe job", Intent{Streams: []StreamIntent{p.stream}})
	}
	upstreamRefs := sourceRefs
	if p.selectedStream {
		selector := streamIntentSelector(p.stream)
		selectName := selectNodeName(selector)
		selectRef := pipeline.NodeRef(selectName)
		if err := addPlannedNode(nodes, &spec, selectName, pipeline.NodeStage, selectRef, selectNodeDetail(selector)); err != nil {
			return pipeline.Spec{}, err
		}
		for i := range sourceRefs {
			spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
				From:   sourceRefs[i],
				To:     selectRef,
				Policy: pipeline.RouteAll,
			})
		}
		upstreamRefs = []pipeline.NodeRef{selectRef}
	}
	targetRefs, err := mediaPlanPacketCopyTargets(&spec, nodes, p.outputs)
	if err != nil {
		return pipeline.Spec{}, err
	}
	for i := range upstreamRefs {
		for j := range targetRefs {
			spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
				From:   upstreamRefs[i],
				To:     targetRefs[j],
				Policy: pipeline.RouteAll,
			})
		}
	}
	return spec, nil
}

func newMediaPlanBranchComposeGraph(rt Runtime, input InputSpec, plan branchComposePlan) (mediaPlanBranchComposeGraph, bool, error) {
	runtime, ok := rt.(*runtime)
	if !ok || runtime == nil {
		return mediaPlanBranchComposeGraph{}, false, nil
	}
	if input.rtp == nil && input.formatInput().Reader == nil && input.formatInput().URI == "" && input.formatInput().Name == "" {
		return mediaPlanBranchComposeGraph{}, false, nil
	}
	branches, targets, err := prepareBranchComposePlan(plan)
	if err != nil {
		return mediaPlanBranchComposeGraph{}, false, err
	}
	return mediaPlanBranchComposeGraph{
		runtime:  runtime,
		input:    input,
		plan:     plan,
		branches: branches,
		targets:  targets,
	}, true, nil
}

func (p mediaPlanBranchComposeGraph) spec() (pipeline.Spec, error) {
	spec := pipeline.Spec{Name: "goav", Realtime: p.runtime.realtime}
	nodes := make(map[string]plannedNode, p.nodeCapacity())
	sourceRefs, ok, err := mediaPlanSourceSpecs(&spec, nodes, []InputSpec{p.input})
	if err != nil {
		return pipeline.Spec{}, err
	}
	if !ok {
		return pipeline.Spec{}, recipeGraphUnsupportedError("describe branch composition", Intent{Name: p.plan.Name})
	}
	return planBranchComposeRoutes(spec, nodes, sourceRefs, p.branches, p.targets)
}

func (p mediaPlanBranchComposeGraph) nodeCapacity() int {
	return 1 + 3 + len(p.branches) + branchChainStepCount(p.branches) + len(p.targets)
}

func (p mediaPlanBranchComposeGraph) runtimeRef() *runtime {
	return p.runtime
}

func (p mediaPlanBranchComposeGraph) lower(ctx context.Context, plan graphPlan, graph pipeline.Graph, service *builder) error {
	lowering, err := p.prepareBranchComposeOperationLowering(plan)
	if err != nil {
		return err
	}
	sources, err := compileMediaPlanSources(ctx, p.runtime, graph, []InputSpec{p.input}, "build branch composition", Intent{Name: p.plan.Name})
	if err != nil {
		return err
	}
	groups, err := resolveBranchComposeStreamGroups(sources.streams, p.branches)
	if err != nil {
		return err
	}
	branchInputs, branchStreams, err := compileBranchComposeInputs(ctx, p.runtime, graph, sources.refs, groups, sources.rtpBuilds, p.branches, lowering.inputs, sources.realtime)
	if err != nil {
		return err
	}
	return compileBranchComposeRoutes(ctx, service, graph, p.branches, lowering.targets, branchInputs, branchStreams, lowering.sharedSteps, sources.realtime)
}

type graphPlanBranchComposeLowering struct {
	inputs      map[string]graphPlanBranchComposeInputOperation
	sharedSteps map[string][]pipeline.NodeRef
	targets     []branchComposeTargetRoute
}

type graphPlanBranchComposeInputOperation struct {
	selectNode pipeline.NodeRef
	decodeNode pipeline.NodeRef
}

func (p mediaPlanBranchComposeGraph) prepareBranchComposeOperationLowering(plan graphPlan) (graphPlanBranchComposeLowering, error) {
	branchOperations := graphPlanOperationsByBranch(plan.operations)
	for i := range p.branches {
		branch := p.branches[i]
		operations := branchOperations[branch.name]
		if len(operations) == 0 {
			return graphPlanBranchComposeLowering{}, graphPlanInvalidError("branch composition graph plan has no operations for branch", []string{
				"branch=" + branch.name,
			})
		}
		if err := p.validateBranchComposeBranchOperations(branch, operations); err != nil {
			return graphPlanBranchComposeLowering{}, err
		}
	}
	inputs, err := p.prepareBranchComposeInputOperations(branchOperations)
	if err != nil {
		return graphPlanBranchComposeLowering{}, err
	}
	sharedSteps, err := p.prepareBranchComposeSharedStepOperations(branchOperations)
	if err != nil {
		return graphPlanBranchComposeLowering{}, err
	}
	targets, err := p.prepareBranchComposeTargets(plan)
	if err != nil {
		return graphPlanBranchComposeLowering{}, err
	}
	return graphPlanBranchComposeLowering{inputs: inputs, sharedSteps: sharedSteps, targets: targets}, nil
}

func (p mediaPlanBranchComposeGraph) prepareBranchComposeInputOperations(branchOperations map[string][]graphPlanOperation) (map[string]graphPlanBranchComposeInputOperation, error) {
	groups := branchComposeSelectorGroups(p.branches)
	inputs := make(map[string]graphPlanBranchComposeInputOperation, len(groups))
	for i := range groups {
		var input graphPlanBranchComposeInputOperation
		for _, branchIndex := range groups[i].branches {
			if branchIndex < 0 || branchIndex >= len(p.branches) {
				continue
			}
			branch := p.branches[branchIndex]
			operations := branchOperations[branch.name]
			selectOperation, ok := graphPlanBranchOperation(operations, OpSelect)
			if !ok {
				return nil, graphPlanInvalidError("branch composition graph plan has no select operation for branch", []string{
					"branch=" + branch.name,
				})
			}
			if err := assignBranchComposeInputNode(&input.selectNode, selectOperation.Node, branch.name, "select"); err != nil {
				return nil, err
			}
			if !branchComposeRouteNeedsDecode(branch) {
				continue
			}
			decodeOperation, ok := graphPlanBranchOperation(operations, OpDecode)
			if !ok {
				return nil, graphPlanInvalidError("branch composition graph plan has no decode operation for branch", []string{
					"branch=" + branch.name,
				})
			}
			if err := assignBranchComposeInputNode(&input.decodeNode, decodeOperation.Node, branch.name, "decode"); err != nil {
				return nil, err
			}
		}
		inputs[branchComposeSelectorKey(groups[i].selector)] = input
	}
	return inputs, nil
}

func assignBranchComposeInputNode(target *pipeline.NodeRef, node pipeline.NodeRef, branch string, kind string) error {
	if node == "" {
		return graphPlanInvalidError("branch composition graph plan input operation has no node", []string{
			"branch=" + branch,
			"kind=" + kind,
		})
	}
	if target == nil {
		return nil
	}
	if *target == "" {
		*target = node
		return nil
	}
	if *target != node {
		return graphPlanInvalidError("branch composition graph plan input operation is not shared by selector group", []string{
			"branch=" + branch,
			"kind=" + kind,
			"first=" + target.String(),
			"next=" + node.String(),
		})
	}
	return nil
}

func (p mediaPlanBranchComposeGraph) prepareBranchComposeSharedStepOperations(branchOperations map[string][]graphPlanOperation) (map[string][]pipeline.NodeRef, error) {
	stepsByBranch := make(map[string][]pipeline.NodeRef, len(p.branches))
	for i := range p.branches {
		branch := p.branches[i]
		refs, err := graphPlanBranchStepOperationNodes(branchOperations[branch.name], true, branch.name)
		if err != nil {
			return nil, err
		}
		stepsByBranch[branch.name] = refs
	}
	groups := branchComposeSelectorGroups(p.branches)
	for i := range groups {
		decodedBranches := branchComposeDecodedBranchIndices(groups[i].branches, p.branches)
		for _, prefix := range branchComposeSharedStepGroups(decodedBranches, p.branches) {
			var firstBranch string
			var first []pipeline.NodeRef
			for _, branchIndex := range prefix.branches {
				if branchIndex < 0 || branchIndex >= len(p.branches) {
					continue
				}
				branch := p.branches[branchIndex]
				next := stepsByBranch[branch.name]
				if firstBranch == "" {
					firstBranch = branch.name
					first = next
					continue
				}
				if !pipelineNodeRefsEqual(first, next) {
					return nil, graphPlanInvalidError("branch composition graph plan shared operations are not shared by selector group", []string{
						"first=" + firstBranch,
						"next=" + branch.name,
					})
				}
			}
		}
	}
	return stepsByBranch, nil
}

func (p mediaPlanBranchComposeGraph) validateBranchComposeBranchOperations(branch branchComposeRoute, operations []graphPlanOperation) error {
	if !graphPlanBranchOperationsContain(operations, OpSelect) {
		return graphPlanInvalidError("branch composition graph plan has no select operation for branch", []string{
			"branch=" + branch.name,
		})
	}
	hasDecode := graphPlanBranchOperationsContain(operations, OpDecode)
	if branchComposeRouteNeedsDecode(branch) && !hasDecode {
		return graphPlanInvalidError("branch composition graph plan has no decode operation for branch", []string{
			"branch=" + branch.name,
		})
	}
	if !branchComposeRouteNeedsDecode(branch) && hasDecode {
		return graphPlanInvalidError("packet branch composition graph plan has an unexpected decode operation", []string{
			"branch=" + branch.name,
		})
	}
	if !branchComposeRouteNeedsDecode(branch) && !graphPlanBranchOperationsContain(operations, OpCopy) {
		return graphPlanInvalidError("packet branch composition graph plan has no copy operation for branch", []string{
			"branch=" + branch.name,
		})
	}
	if err := p.validateBranchComposeStepOperations(branch, operations); err != nil {
		return err
	}
	hasEncode := graphPlanBranchOperationsContain(operations, OpEncode)
	if branchComposeRouteNeedsEncode(branch) && !hasEncode {
		return graphPlanInvalidError("branch composition graph plan has no encode operation for branch", []string{
			"branch=" + branch.name,
		})
	}
	if !branchComposeRouteNeedsEncode(branch) && hasEncode {
		return graphPlanInvalidError("branch composition graph plan has an unexpected encode operation for branch", []string{
			"branch=" + branch.name,
		})
	}
	return nil
}

func graphPlanBranchOperation(operations []graphPlanOperation, kind OperationKind) (graphPlanOperation, bool) {
	for i := range operations {
		if operations[i].Kind == kind {
			return operations[i], true
		}
	}
	return graphPlanOperation{}, false
}

func (p mediaPlanBranchComposeGraph) validateBranchComposeStepOperations(branch branchComposeRoute, operations []graphPlanOperation) error {
	shared := graphPlanBranchStepOperationCount(operations, true)
	private := graphPlanBranchStepOperationCount(operations, false)
	if shared != len(branch.sharedSteps) {
		return graphPlanInvalidError("branch composition graph plan shared operations do not match branch chain", []string{
			"branch=" + branch.name,
			"planned=" + strconv.Itoa(shared),
			"steps=" + strconv.Itoa(len(branch.sharedSteps)),
		})
	}
	if private != len(branch.steps) {
		return graphPlanInvalidError("branch composition graph plan operations do not match branch chain", []string{
			"branch=" + branch.name,
			"planned=" + strconv.Itoa(private),
			"steps=" + strconv.Itoa(len(branch.steps)),
		})
	}
	return nil
}

func (p mediaPlanBranchComposeGraph) prepareBranchComposeTargets(plan graphPlan) ([]branchComposeTargetRoute, error) {
	operations := graphPlanTargetOperations(plan.operations)
	if len(operations) == 0 {
		return nil, graphPlanInvalidError("branch composition graph plan has no target operations", nil)
	}
	if len(operations) != len(p.targets) {
		return nil, graphPlanInvalidError("branch composition graph plan target count does not match targets", []string{
			"targets=" + strconv.Itoa(len(operations)),
			"routes=" + strconv.Itoa(len(p.targets)),
		})
	}
	branchesByTarget := graphPlanTargetBranchNames(plan.operations)
	targets := make([]branchComposeTargetRoute, len(operations))
	for i := range operations {
		operation := operations[i]
		targetIndex := branchComposeTargetRouteIndex(p.targets, operation.Name)
		if targetIndex < 0 {
			return nil, graphPlanInvalidError("branch composition target operation is not bound to a target", []string{
				"target=" + operation.Name,
				"node=" + operation.Node.String(),
			})
		}
		target := p.targets[targetIndex]
		if err := validateBranchComposeTargetOperation(operation, target); err != nil {
			return nil, err
		}
		if !branchComposeTargetBranchesMatch(target, p.branches, branchesByTarget[operation.Name]) {
			return nil, graphPlanInvalidError("branch composition target operation branches do not match target routes", []string{
				"target=" + operation.Name,
			})
		}
		targets[i] = target
	}
	return targets, nil
}

func validateBranchComposeTargetOperation(operation graphPlanTargetOperation, target branchComposeTargetRoute) error {
	if target.sink != nil {
		if operation.Kind != OpSink {
			return graphPlanInvalidError("branch composition target operation kind does not match sink target", []string{
				"target=" + operation.Name,
				"kind=" + string(operation.Kind),
			})
		}
		return nil
	}
	if operation.Kind != OpMux && operation.Kind != OpWrite {
		return graphPlanInvalidError("branch composition target operation kind does not match byte target", []string{
			"target=" + operation.Name,
			"kind=" + string(operation.Kind),
		})
	}
	return nil
}

func branchComposeTargetRouteIndex(targets []branchComposeTargetRoute, name string) int {
	for i := range targets {
		if branchComposeTargetRouteName(targets[i]) == name {
			return i
		}
	}
	return -1
}

func branchComposeTargetRouteName(target branchComposeTargetRoute) string {
	name := firstNonEmpty(target.output.Name, target.target.Name, target.target.URI)
	if name != "" {
		return name
	}
	if target.sink != nil {
		return target.sink.Name()
	}
	return ""
}

func branchComposeTargetBranchesMatch(target branchComposeTargetRoute, branches []branchComposeRoute, actual map[string]struct{}) bool {
	if len(target.matches) != len(actual) {
		return false
	}
	for _, index := range target.matches {
		if index < 0 || index >= len(branches) {
			return false
		}
		if _, ok := actual[branches[index].name]; !ok {
			return false
		}
	}
	return true
}

func graphPlanOperationsByBranch(operations []graphPlanOperation) map[string][]graphPlanOperation {
	out := make(map[string][]graphPlanOperation)
	for i := range operations {
		branch := operations[i].Branch
		if branch == "" {
			continue
		}
		out[branch] = append(out[branch], operations[i])
	}
	return out
}

func graphPlanBranchOperationsContain(operations []graphPlanOperation, kind OperationKind) bool {
	for i := range operations {
		if operations[i].Kind == kind {
			return true
		}
	}
	return false
}

func graphPlanBranchStepOperationCount(operations []graphPlanOperation, shared bool) int {
	count := 0
	for i := range operations {
		operation := operations[i]
		if operation.Shared != shared {
			continue
		}
		if operation.Kind == OpTransform || operation.Kind == OpStage {
			count++
		}
	}
	return count
}

func graphPlanBranchStepOperationNodes(operations []graphPlanOperation, shared bool, branch string) ([]pipeline.NodeRef, error) {
	refs := make([]pipeline.NodeRef, 0)
	for i := range operations {
		operation := operations[i]
		if operation.Shared != shared {
			continue
		}
		if operation.Kind != OpTransform && operation.Kind != OpStage {
			continue
		}
		if operation.Node == "" {
			return nil, graphPlanInvalidError("branch composition graph plan step operation has no node", []string{
				"branch=" + branch,
				"kind=" + string(operation.Kind),
			})
		}
		refs = append(refs, operation.Node)
	}
	return refs, nil
}

func pipelineNodeRefsEqual(first []pipeline.NodeRef, second []pipeline.NodeRef) bool {
	if len(first) != len(second) {
		return false
	}
	for i := range first {
		if first[i] != second[i] {
			return false
		}
	}
	return true
}

func graphPlanTargetBranchNames(operations []graphPlanOperation) map[string]map[string]struct{} {
	out := make(map[string]map[string]struct{})
	for i := range operations {
		operation := operations[i]
		if !graphPlanOperationTargetsRequired(operation.Kind) {
			continue
		}
		for _, target := range operation.Targets {
			if target == "" || operation.Branch == "" {
				continue
			}
			branches := out[target]
			if branches == nil {
				branches = make(map[string]struct{})
				out[target] = branches
			}
			branches[operation.Branch] = struct{}{}
		}
	}
	return out
}

func (p mediaPlanStreamGraph) lowerPacketCopy(ctx context.Context, plan graphPlan, graph pipeline.Graph, service *builder) error {
	selectOperation, hasSelect, targets, err := p.preparePacketCopyOperationLowering(plan)
	if err != nil {
		return err
	}
	sourceRefs, streams, _, _, err := p.compileSources(ctx, graph)
	if err != nil {
		return err
	}
	targetRefs := sourceRefs
	targetStreams := streams
	if hasSelect {
		selector := streamIntentSelector(p.stream)
		selected, err := selectDecodeStream(streams, selector)
		if err != nil {
			return err
		}
		selectName := firstNonEmpty(selectOperation.Node.String(), selectNodeName(selector))
		selectStage := newStreamSelectStage(selectName, selected, selector, selectNodeDetail(selector))
		selectRef, err := graph.AddStage(selectStage, p.runtime.buffer)
		if err != nil {
			selectStage.Close()
			return err
		}
		for i := range sourceRefs {
			if err := connectRefs(graph, sourceRefs[i], selectRef); err != nil {
				return err
			}
		}
		targetRefs = []pipeline.NodeRef{selectRef}
		targetStreams = []av.Stream{selected}
	}
	return p.lowerPacketCopyTargets(ctx, graph, service, targets, targetRefs, targetStreams)
}

func (p mediaPlanStreamGraph) preparePacketCopyOperationLowering(plan graphPlan) (graphPlanOperation, bool, []graphPlanTargetOperation, error) {
	selectOperation, hasSelect := graphPlanFirstOperation(plan.operations, OpSelect)
	switch {
	case p.selectedStream && !hasSelect:
		return graphPlanOperation{}, false, nil, graphPlanInvalidError("selected packet-copy graph plan has no select operation", []string{
			"stream=" + firstNonEmpty(p.stream.Name, string(p.stream.Select.ID), string(p.stream.Select.Type), "stream"),
		})
	case !p.selectedStream && hasSelect:
		return graphPlanOperation{}, false, nil, graphPlanInvalidError("packet-copy graph plan has an unexpected select operation", []string{
			"node=" + selectOperation.Node.String(),
		})
	}
	targets := graphPlanTargetOperations(plan.operations)
	if len(targets) == 0 {
		return graphPlanOperation{}, false, nil, graphPlanInvalidError("packet-copy graph plan has no target operations", nil)
	}
	for i := range targets {
		target := targets[i]
		outputIndex, ok := graphPlanOutputIndex(plan.outputs, target.Name)
		if !ok || outputIndex < 0 || outputIndex >= len(p.outputs) {
			return graphPlanOperation{}, false, nil, graphPlanInvalidError("packet-copy target operation is not bound to an output", []string{
				"target=" + target.Name,
				"node=" + target.Node.String(),
			})
		}
		target.OutputIndex = outputIndex
		targets[i] = target
		output := p.outputs[outputIndex]
		if output.sink != nil {
			if target.Kind != OpSink {
				return graphPlanOperation{}, false, nil, graphPlanInvalidError("packet-copy target operation kind does not match sink destination", []string{
					"target=" + target.Name,
					"kind=" + string(target.Kind),
				})
			}
			continue
		}
		if target.Kind != OpMux && target.Kind != OpWrite {
			return graphPlanOperation{}, false, nil, graphPlanInvalidError("packet-copy target operation kind does not match byte destination", []string{
				"target=" + target.Name,
				"kind=" + string(target.Kind),
			})
		}
	}
	return selectOperation, hasSelect, targets, nil
}

func (p mediaPlanStreamGraph) lowerPacketCopyTargets(
	ctx context.Context,
	graph pipeline.Graph,
	service *builder,
	targets []graphPlanTargetOperation,
	targetRefs []pipeline.NodeRef,
	streams []av.Stream,
) error {
	for i := range targets {
		target := targets[i]
		output := p.outputs[target.OutputIndex]
		if output.sink != nil {
			sinkRef, err := graph.AddSink(output.sink, p.runtime.buffer)
			if err != nil {
				return err
			}
			for j := range targetRefs {
				if err := connectRefs(graph, targetRefs[j], sinkRef); err != nil {
					return err
				}
			}
			continue
		}
		stage, err := service.openMuxDestinationStage(ctx, output, target.OutputIndex, streams, destinationOpenFormat(output), destinationGraphFormat(output))
		if err != nil {
			return err
		}
		stageRef, err := graph.AddStage(stage, p.runtime.buffer)
		if err != nil {
			stage.Close()
			return err
		}
		for j := range targetRefs {
			if err := connectRefs(graph, targetRefs[j], stageRef); err != nil {
				return err
			}
		}
	}
	return nil
}

func graphPlanFirstOperation(operations []graphPlanOperation, kind OperationKind) (graphPlanOperation, bool) {
	for i := range operations {
		if operations[i].Kind == kind {
			return operations[i], true
		}
	}
	return graphPlanOperation{}, false
}

type graphPlanTargetOperation struct {
	Name        string
	Node        pipeline.NodeRef
	Kind        OperationKind
	OutputIndex int
}

func graphPlanTargetOperations(operations []graphPlanOperation) []graphPlanTargetOperation {
	targets := make([]graphPlanTargetOperation, 0)
	seen := make(map[string]struct{})
	for i := range operations {
		operation := operations[i]
		if !graphPlanOperationTargetsRequired(operation.Kind) {
			continue
		}
		for _, target := range operation.Targets {
			if target == "" {
				continue
			}
			if _, ok := seen[target]; ok {
				continue
			}
			seen[target] = struct{}{}
			targets = append(targets, graphPlanTargetOperation{
				Name: target,
				Node: operation.Node,
				Kind: operation.Kind,
			})
		}
	}
	return targets
}

func graphPlanOutputIndex(outputs []planOutput, target string) (int, bool) {
	for i := range outputs {
		if outputs[i].Name == target {
			return i, true
		}
	}
	return -1, false
}

type graphPlanFrameStreamLowering struct {
	targets []graphPlanTargetOperation
}

func (p mediaPlanStreamGraph) prepareFrameStreamOperationLowering(plan graphPlan) (graphPlanFrameStreamLowering, error) {
	if _, ok := graphPlanFirstOperation(plan.operations, OpSelect); !ok {
		return graphPlanFrameStreamLowering{}, graphPlanInvalidError("frame stream graph plan has no select operation", []string{
			"stream=" + firstNonEmpty(p.stream.Name, string(p.stream.Select.ID), string(p.stream.Select.Type), "stream"),
		})
	}
	if _, ok := graphPlanFirstOperation(plan.operations, OpDecode); !ok {
		return graphPlanFrameStreamLowering{}, graphPlanInvalidError("frame stream graph plan has no decode operation", []string{
			"stream=" + firstNonEmpty(p.stream.Name, string(p.stream.Select.ID), string(p.stream.Select.Type), "stream"),
		})
	}
	if err := p.validateFrameStreamFilterOperations(plan.operations); err != nil {
		return graphPlanFrameStreamLowering{}, err
	}
	_, hasEncode := graphPlanFirstOperation(plan.operations, OpEncode)
	if p.encode != nil && !hasEncode {
		return graphPlanFrameStreamLowering{}, graphPlanInvalidError("encoded frame stream graph plan has no encode operation", []string{
			"stream=" + firstNonEmpty(p.stream.Name, string(p.stream.Select.ID), string(p.stream.Select.Type), "stream"),
		})
	}
	if p.encode == nil && hasEncode {
		return graphPlanFrameStreamLowering{}, graphPlanInvalidError("decoded frame stream graph plan has an unexpected encode operation", []string{
			"stream=" + firstNonEmpty(p.stream.Name, string(p.stream.Select.ID), string(p.stream.Select.Type), "stream"),
		})
	}
	targets, err := p.prepareFrameStreamTargets(plan)
	if err != nil {
		return graphPlanFrameStreamLowering{}, err
	}
	return graphPlanFrameStreamLowering{targets: targets}, nil
}

func (p mediaPlanStreamGraph) validateFrameStreamFilterOperations(operations []graphPlanOperation) error {
	planned := graphPlanOperationCount(operations, OpTransform) + graphPlanOperationCount(operations, OpStage)
	if planned != len(p.filters) {
		return graphPlanInvalidError("frame stream graph plan filter operations do not match concrete filters", []string{
			"planned=" + strconv.Itoa(planned),
			"filters=" + strconv.Itoa(len(p.filters)),
		})
	}
	return nil
}

func (p mediaPlanStreamGraph) prepareFrameStreamTargets(plan graphPlan) ([]graphPlanTargetOperation, error) {
	targets := graphPlanTargetOperations(plan.operations)
	if len(targets) == 0 {
		return nil, graphPlanInvalidError("frame stream graph plan has no target operations", nil)
	}
	if len(targets) != len(p.outputs) {
		return nil, graphPlanInvalidError("frame stream graph plan target count does not match outputs", []string{
			"targets=" + strconv.Itoa(len(targets)),
			"outputs=" + strconv.Itoa(len(p.outputs)),
		})
	}
	for i := range targets {
		target := targets[i]
		outputIndex, ok := graphPlanOutputIndex(plan.outputs, target.Name)
		if !ok || outputIndex < 0 || outputIndex >= len(p.outputs) {
			return nil, graphPlanInvalidError("frame stream target operation is not bound to an output", []string{
				"target=" + target.Name,
				"node=" + target.Node.String(),
			})
		}
		target.OutputIndex = outputIndex
		targets[i] = target
		output := p.outputs[outputIndex]
		if output.sink != nil {
			if target.Kind != OpSink {
				return nil, graphPlanInvalidError("frame stream target operation kind does not match sink destination", []string{
					"target=" + target.Name,
					"kind=" + string(target.Kind),
				})
			}
			continue
		}
		if p.encode == nil {
			return nil, graphPlanInvalidError("decoded frame stream cannot lower to a byte target without encode", []string{
				"target=" + target.Name,
			})
		}
		if target.Kind != OpMux && target.Kind != OpWrite {
			return nil, graphPlanInvalidError("frame stream target operation kind does not match byte destination", []string{
				"target=" + target.Name,
				"kind=" + string(target.Kind),
			})
		}
	}
	return targets, nil
}

func graphPlanOperationCount(operations []graphPlanOperation, kind OperationKind) int {
	count := 0
	for i := range operations {
		if operations[i].Kind == kind {
			count++
		}
	}
	return count
}

func (p mediaPlanStreamGraph) sinkDestinationSpec() (pipeline.Spec, error) {
	spec, sourceRefs, nodes, err := p.specWithSources()
	if err != nil {
		return pipeline.Spec{}, err
	}
	previous, err := planDecodeFilterPath(nodes, &spec, sourceRefs, p.decode, p.filters)
	if err != nil {
		return pipeline.Spec{}, err
	}
	if p.encode != nil {
		if err := planEncodeSinkPath(nodes, &spec, previous, *p.encode, p.outputs[0].sink); err != nil {
			return pipeline.Spec{}, err
		}
		return spec, nil
	}
	if err := planSinkPath(nodes, &spec, previous, p.outputs[0].sink); err != nil {
		return pipeline.Spec{}, err
	}
	return spec, nil
}

func (p mediaPlanStreamGraph) encodeOutputSpec() (pipeline.Spec, error) {
	if p.encode == nil {
		return pipeline.Spec{}, recipeGraphUnsupportedError("describe job", Intent{Streams: []StreamIntent{p.stream}})
	}
	spec, sourceRefs, nodes, err := p.specWithSources()
	if err != nil {
		return pipeline.Spec{}, err
	}
	previous, err := planDecodeFilterPath(nodes, &spec, sourceRefs, p.decode, p.filters)
	if err != nil {
		return pipeline.Spec{}, err
	}
	if err := planEncodeDestinationPath(nodes, &spec, previous, *p.encode, p.outputs); err != nil {
		return pipeline.Spec{}, err
	}
	return spec, nil
}

func (p mediaPlanStreamGraph) specWithSources() (pipeline.Spec, []pipeline.NodeRef, map[string]plannedNode, error) {
	spec := pipeline.Spec{Name: "goav", Realtime: p.runtime.realtime}
	nodes := make(map[string]plannedNode, len(p.inputs)+len(p.outputs)+len(p.filters)+3)
	sourceRefs, ok, err := mediaPlanSourceSpecs(&spec, nodes, p.inputs)
	if err != nil {
		return pipeline.Spec{}, nil, nil, err
	}
	if !ok {
		return pipeline.Spec{}, nil, nil, recipeGraphUnsupportedError("describe job", Intent{Streams: []StreamIntent{p.stream}})
	}
	return spec, sourceRefs, nodes, nil
}

func (p mediaPlanStreamGraph) compileSinkDestination(ctx context.Context, graph pipeline.Graph, lowering graphPlanFrameStreamLowering) error {
	sourceRefs, streams, rtpBuilds, realtime, err := p.compileSources(ctx, graph)
	if err != nil {
		return err
	}
	stream, err := selectDecodeStream(streams, p.decode.selector)
	if err != nil {
		return err
	}
	bounds := codec.DecodeBounds{}
	if len(rtpBuilds) != 0 {
		bounds = rtpDecodeBoundsForStream(stream, rtpBuilds)
	}
	previousRef, filteredStream, err := compileDecodeFilterPath(ctx, p.runtime, graph, sourceRefs, p.decode, stream, realtime, p.encode == nil, bounds, p.filters)
	if err != nil {
		return err
	}
	if p.encode != nil {
		encodeConfig, _, err := prepareEncodeConfig(filteredStream, *p.encode, realtime)
		if err != nil {
			return err
		}
		return p.lowerEncodeTargets(ctx, &builder{runtime: p.runtime}, graph, previousRef, *p.encode, encodeConfig, filteredStream, lowering.targets)
	}
	if len(lowering.targets) != 1 || lowering.targets[0].OutputIndex != 0 {
		return graphPlanInvalidError("decoded frame sink graph plan must have exactly one sink target", []string{
			"targets=" + strconv.Itoa(len(lowering.targets)),
		})
	}
	sinkRef, err := graph.AddSink(p.outputs[0].sink, p.runtime.buffer)
	if err != nil {
		return err
	}
	return connectRefs(graph, previousRef, sinkRef)
}

func (p mediaPlanStreamGraph) compileEncodeOutput(ctx context.Context, graph pipeline.Graph, service *builder, lowering graphPlanFrameStreamLowering) error {
	if p.encode == nil {
		return recipeGraphUnsupportedError("build job", Intent{Streams: []StreamIntent{p.stream}})
	}
	sourceRefs, streams, rtpBuilds, realtime, err := p.compileSources(ctx, graph)
	if err != nil {
		return err
	}
	stream, err := selectDecodeStream(streams, p.decode.selector)
	if err != nil {
		return err
	}
	bounds := codec.DecodeBounds{}
	if len(rtpBuilds) != 0 {
		bounds = rtpDecodeBoundsForStream(stream, rtpBuilds)
	}
	previousRef, filteredStream, err := compileDecodeFilterPath(ctx, p.runtime, graph, sourceRefs, p.decode, stream, realtime, false, bounds, p.filters)
	if err != nil {
		return err
	}
	encodeConfig, encodedStream, err := prepareEncodeConfig(filteredStream, *p.encode, realtime)
	if err != nil {
		return err
	}
	return p.lowerEncodeTargets(ctx, service, graph, previousRef, *p.encode, encodeConfig, encodedStream, lowering.targets)
}

func (p mediaPlanStreamGraph) lowerEncodeTargets(
	ctx context.Context,
	service *builder,
	graph pipeline.Graph,
	upstream pipeline.NodeRef,
	request encodeRequest,
	config codec.EncodeConfig,
	stream av.Stream,
	targets []graphPlanTargetOperation,
) error {
	encodeRef, err := compileEncodeStage(ctx, p.runtime, graph, upstream, request, config)
	if err != nil {
		return err
	}
	streams := []av.Stream{stream}
	for i := range targets {
		target := targets[i]
		output := p.outputs[target.OutputIndex]
		if output.sink != nil {
			sinkRef, err := graph.AddSink(output.sink, p.runtime.buffer)
			if err != nil {
				return err
			}
			if err := connectRefs(graph, encodeRef, sinkRef); err != nil {
				return err
			}
			continue
		}
		muxStage, err := service.openMuxDestinationStage(ctx, output, target.OutputIndex, streams, destinationOpenFormat(output), destinationGraphFormat(output))
		if err != nil {
			return err
		}
		muxRef, err := graph.AddStage(muxStage, p.runtime.buffer)
		if err != nil {
			muxStage.Close()
			return err
		}
		if err := connectRefs(graph, encodeRef, muxRef); err != nil {
			return err
		}
	}
	return nil
}

func (p mediaPlanStreamGraph) compileSources(ctx context.Context, graph pipeline.Graph) ([]pipeline.NodeRef, []av.Stream, []rtpBuild, bool, error) {
	sources, err := compileMediaPlanSources(ctx, p.runtime, graph, p.inputs, "build job", Intent{Streams: []StreamIntent{p.stream}})
	if err != nil {
		return nil, nil, nil, false, err
	}
	return sources.refs, sources.streams, sources.rtpBuilds, sources.realtime, nil
}

func compileMediaPlanSources(
	ctx context.Context,
	runtime *runtime,
	graph pipeline.Graph,
	inputs []InputSpec,
	operation string,
	intent Intent,
) (mediaPlanCompiledSources, error) {
	if runtime == nil {
		return mediaPlanCompiledSources{}, recipeGraphUnsupportedError(operation, intent)
	}
	service := &builder{runtime: runtime}
	if len(inputs) == 1 && inputs[0].rtp == nil {
		input := inputs[0].formatInput()
		demux, err := service.openDemuxSource(ctx, input)
		if err != nil {
			return mediaPlanCompiledSources{}, err
		}
		sourceRef, err := graph.AddSource(demux.source, runtime.buffer)
		if err != nil {
			demux.source.Close()
			return mediaPlanCompiledSources{}, err
		}
		return mediaPlanCompiledSources{
			refs:     []pipeline.NodeRef{sourceRef},
			streams:  demux.streams,
			realtime: runtime.realtime || input.Realtime,
		}, nil
	}
	if !allRTPInputSpecs(inputs) {
		return mediaPlanCompiledSources{}, recipeGraphUnsupportedError(operation, intent)
	}
	sourceRefs := make([]pipeline.NodeRef, 0, len(inputs))
	streams := make([]av.Stream, 0, len(inputs))
	builds := make([]rtpBuild, 0, len(inputs))
	for i := range inputs {
		receiver, err := service.openRTPSource(ctx, inputs[i].rtpBuildInput(), i)
		if err != nil {
			return mediaPlanCompiledSources{}, err
		}
		sourceRef, err := graph.AddSource(receiver.source, runtime.buffer)
		if err != nil {
			receiver.source.Close()
			return mediaPlanCompiledSources{}, err
		}
		sourceRefs = append(sourceRefs, sourceRef)
		streams = append(streams, receiver.streams...)
		builds = append(builds, receiver)
	}
	return mediaPlanCompiledSources{
		refs:      sourceRefs,
		streams:   streams,
		rtpBuilds: builds,
		realtime:  runtime.realtime,
	}, nil
}

func mediaPlanStreamFilters(stream StreamIntent) ([]filterRequest, error) {
	selector := streamIntentSelector(stream)
	if len(stream.Operations) == 0 {
		return mediaPlanStreamTransformFilters(stream, selector)
	}
	filters := make([]filterRequest, 0, len(stream.Operations))
	frameStepIndex := 0
	for i := range stream.Operations {
		operation := stream.Operations[i]
		switch operation.Kind {
		case OpStage:
			if operation.Stage == nil {
				return nil, streamStageMissingError(stream)
			}
			filters = append(filters, filterRequest{selector: selector, stage: operation.Stage})
			frameStepIndex++
		case OpTransform:
			transform, err := streamTransform(stream.Name, selector, operation.Transform, frameStepIndex)
			if err != nil {
				return nil, err
			}
			filters = append(filters, filterRequest{selector: selector, transform: &transform})
			frameStepIndex++
		case OpTap:
			if operation.Tap.After == "" {
				frameStepIndex++
			}
		}
	}
	return filters, nil
}

func mediaPlanStreamTransformFilters(stream StreamIntent, selector av.StreamSelector) ([]filterRequest, error) {
	filters := make([]filterRequest, 0, len(stream.Transforms))
	for i := range stream.Transforms {
		transform, err := streamTransform(stream.Name, selector, stream.Transforms[i], i)
		if err != nil {
			return nil, err
		}
		filters = append(filters, filterRequest{selector: selector, transform: &transform})
	}
	return filters, nil
}

func (r recipeResolved) packetCopyStream() (StreamIntent, bool, bool) {
	return mediaPlanPacketCopyIntentStream(true, r.intent, r.chainAttachments)
}
