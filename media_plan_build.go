package goav

import (
	"context"
	"sort"
	"strconv"
	"strings"

	"github.com/thesyncim/goav/av"
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
	sourceDomain   MediaDomain
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
	refs         []pipeline.NodeRef
	streams      []av.Stream
	streamGroups [][]av.Stream
	rtpBuilds    []rtpBuild
	realtime     bool
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
	lowering, err := p.prepareFrameOperationSpecLowering(plan)
	if err != nil {
		return err
	}
	return p.compileFrameStreamBranchCompose(ctx, graph, service, lowering)
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
		runtime:      runtime,
		inputs:       append([]InputSpec(nil), inputs...),
		outputs:      append([]destinationSpec(nil), outputs...),
		stream:       stream,
		sourceDomain: mediaPlanInputDomain(inputs),
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

func mediaPlanInputDomain(inputs []InputSpec) MediaDomain {
	if len(inputs) == 1 {
		if shape, ok := customSourceShape(inputs[0]); ok && shape.Domain != "" {
			return shape.Domain
		}
	}
	return DomainPacket
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
	if p.selectedStream {
		branches, outputs := p.selectedPacketCopyBranchComposeRoutes()
		return planBranchComposeRoutes(spec, nodes, sourceRefs, branches, outputs)
	}
	if mediaPlanInputsContainDomain(p.inputs, DomainEvent) && outputsContainMuxTarget(p.outputs) {
		return pipeline.Spec{}, graphPlanInvalidError("event source destination must be a sink", nil)
	}
	upstreamRefs := sourceRefs
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

func mediaPlanInputsContainDomain(inputs []InputSpec, domain MediaDomain) bool {
	for i := range inputs {
		if shape, ok := customSourceShape(inputs[i]); ok && shape.Domain == domain {
			return true
		}
	}
	return false
}

func (p mediaPlanStreamGraph) selectedPacketCopyBranchComposeRoutes() ([]branchComposeRoute, []branchComposeTargetRoute) {
	selector := streamIntentSelector(p.stream)
	branchName := firstNonEmpty(p.stream.Name, string(selector.ID), string(selector.Type), "branch")
	branches := []branchComposeRoute{{
		name: branchName,
		branch: branchComposeBranch{
			Name:     branchName,
			Selector: selector,
			Copy:     true,
		},
		copy: true,
		request: encodeRequest{
			name:     branchName,
			selector: selector,
		},
	}}
	return branches, mediaPlanBranchComposeTargetRoutes(p.outputs, branchName)
}

func newMediaPlanBranchComposeGraph(rt Runtime, input InputSpec, plan branchComposePlan) (mediaPlanBranchComposeGraph, bool, error) {
	runtime, ok := rt.(*runtime)
	if !ok || runtime == nil {
		return mediaPlanBranchComposeGraph{}, false, nil
	}
	if input.rtp == nil && input.source == nil && input.formatInput().Reader == nil && input.formatInput().URI == "" && input.formatInput().Name == "" {
		return mediaPlanBranchComposeGraph{}, false, nil
	}
	branches, targets, err := prepareBranchComposePlan(plan)
	if err != nil {
		return mediaPlanBranchComposeGraph{}, false, err
	}
	sourceDomain := mediaPlanInputDomain([]InputSpec{input})
	for i := range branches {
		branches[i].sourceDomain = sourceDomain
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
	return compileBranchComposeRoutes(ctx, service, graph, p.branches, lowering.targets, branchInputs, branchStreams, lowering.sharedSteps, lowering.branches, sources.realtime)
}

type graphPlanBranchComposeLowering struct {
	inputs      map[string]graphPlanBranchComposeInputOperation
	sharedSteps map[string][]pipeline.NodeRef
	branches    map[string]graphPlanBranchComposeBranchOperation
	targets     []branchComposeTargetRoute
}

type graphPlanBranchComposeInputOperation struct {
	selectNode pipeline.NodeRef
	decodeNode pipeline.NodeRef
}

type graphPlanBranchComposeBranchOperation struct {
	privateSteps []pipeline.NodeRef
	encodeNode   pipeline.NodeRef
	encodeShape  MediaShape
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
	branches, err := p.prepareBranchComposeBranchOperations(branchOperations)
	if err != nil {
		return graphPlanBranchComposeLowering{}, err
	}
	targets, err := p.prepareBranchComposeTargets(plan)
	if err != nil {
		return graphPlanBranchComposeLowering{}, err
	}
	return graphPlanBranchComposeLowering{inputs: inputs, sharedSteps: sharedSteps, branches: branches, targets: targets}, nil
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

func (p mediaPlanBranchComposeGraph) prepareBranchComposeBranchOperations(branchOperations map[string][]graphPlanOperation) (map[string]graphPlanBranchComposeBranchOperation, error) {
	out := make(map[string]graphPlanBranchComposeBranchOperation, len(p.branches))
	for i := range p.branches {
		branch := p.branches[i]
		operations := branchOperations[branch.name]
		privateSteps, err := graphPlanBranchStepOperationNodes(operations, false, branch.name)
		if err != nil {
			return nil, err
		}
		var encodeNode pipeline.NodeRef
		var encodeShape MediaShape
		if branchComposeRouteNeedsEncode(branch) {
			operation, ok := graphPlanBranchOperation(operations, OpEncode)
			if !ok {
				return nil, graphPlanInvalidError("branch composition graph plan has no encode operation for branch", []string{
					"branch=" + branch.name,
				})
			}
			if operation.Node == "" {
				return nil, graphPlanInvalidError("branch composition graph plan encode operation has no node", []string{
					"branch=" + branch.name,
				})
			}
			encodeNode = operation.Node
			encodeShape = operation.Shape
		}
		out[branch.name] = graphPlanBranchComposeBranchOperation{
			privateSteps: privateSteps,
			encodeNode:   encodeNode,
			encodeShape:  encodeShape,
		}
	}
	return out, nil
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
	if !branchComposeRouteNeedsDecode(branch) && branch.sourceDomain != DomainFrame && !graphPlanBranchOperationsContain(operations, OpCopy) {
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
	operations, err := graphPlanUniqueDestinationOperations(plan.operations, "branch composition")
	if err != nil {
		return nil, err
	}
	if len(operations) == 0 {
		return nil, graphPlanInvalidError("branch composition graph plan has no destination operations", nil)
	}
	if len(operations) != len(p.targets) {
		return nil, graphPlanInvalidError("branch composition graph plan target count does not match targets", []string{
			"targets=" + strconv.Itoa(len(operations)),
			"routes=" + strconv.Itoa(len(p.targets)),
		})
	}
	branchesByTarget := graphPlanDestinationBranchNames(plan.operations)
	targets := make([]branchComposeTargetRoute, len(operations))
	for i := range operations {
		operation := operations[i]
		targetIndex := branchComposeTargetRouteIndex(p.targets, operation.Name)
		if targetIndex < 0 {
			return nil, graphPlanInvalidError("branch composition destination operation is not bound to a target", []string{
				"destination=" + operation.Name,
				"node=" + operation.Node.String(),
			})
		}
		target := p.targets[targetIndex]
		if err := validateBranchComposeDestinationOperation(operation, target); err != nil {
			return nil, err
		}
		matches, ok := branchComposeDestinationOperationMatches(p.branches, branchesByTarget[operation.Name])
		if !ok || !branchComposeTargetBranchesMatch(target, matches) {
			return nil, graphPlanInvalidError("branch composition destination operation branches do not match target routes", []string{
				"destination=" + operation.Name,
			})
		}
		target.node = operation.Node
		target.matches = matches
		targets[i] = target
	}
	return targets, nil
}

func validateBranchComposeDestinationOperation(operation graphPlanDestinationOperation, target branchComposeTargetRoute) error {
	if err := validateGraphPlanDestinationOperationNode("branch composition", operation); err != nil {
		return err
	}
	if target.sink != nil {
		if operation.Kind != OpSink {
			return graphPlanInvalidError("branch composition destination operation kind does not match sink target", []string{
				"destination=" + operation.Name,
				"kind=" + string(operation.Kind),
			})
		}
		return nil
	}
	if operation.Kind != OpMux && operation.Kind != OpWrite {
		return graphPlanInvalidError("branch composition destination operation kind does not match byte target", []string{
			"destination=" + operation.Name,
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

func branchComposeDestinationOperationMatches(branches []branchComposeRoute, actual map[string]struct{}) ([]int, bool) {
	if len(actual) == 0 {
		return nil, false
	}
	matches := make([]int, 0, len(actual))
	for i := range branches {
		if _, ok := actual[branches[i].name]; ok {
			matches = append(matches, i)
		}
	}
	return matches, len(matches) == len(actual)
}

func branchComposeTargetBranchesMatch(target branchComposeTargetRoute, matches []int) bool {
	if len(target.matches) != len(matches) {
		return false
	}
	seen := make(map[int]struct{}, len(target.matches))
	for _, index := range target.matches {
		seen[index] = struct{}{}
	}
	for _, index := range matches {
		if _, ok := seen[index]; !ok {
			return false
		}
	}
	return true
}

func packetCopyDestinationOperationMatches(branches []planBranch, actual map[string]struct{}) ([]int, bool) {
	if len(actual) == 0 {
		return nil, false
	}
	matches := make([]int, 0, len(actual))
	for i := range branches {
		if _, ok := actual[branches[i].Name]; ok {
			matches = append(matches, i)
		}
	}
	return matches, len(matches) == len(actual)
}

func packetCopyTargetBranchesMatch(output planOutput, matches []int, branches []planBranch) bool {
	if len(output.BranchRefs) != len(matches) {
		return false
	}
	seen := make(map[string]struct{}, len(output.BranchRefs))
	for _, branch := range output.BranchRefs {
		seen[branch] = struct{}{}
	}
	for _, index := range matches {
		if index < 0 || index >= len(branches) {
			return false
		}
		if _, ok := seen[branches[index].Name]; !ok {
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

func graphPlanSingleBranchOperations(operations []graphPlanOperation, scope string) ([]graphPlanOperation, string, error) {
	byBranch := graphPlanOperationsByBranch(operations)
	if len(byBranch) == 0 {
		return nil, "", graphPlanInvalidError(scope+" graph plan has no branch operations", nil)
	}
	if len(byBranch) != 1 {
		branches := make([]string, 0, len(byBranch))
		for branch := range byBranch {
			branches = append(branches, branch)
		}
		sort.Strings(branches)
		return nil, "", graphPlanInvalidError(scope+" graph plan must have exactly one branch operation set", []string{
			"branches=" + strconv.Itoa(len(byBranch)),
			"branch_names=" + strings.Join(branches, ","),
		})
	}
	for branch, branchOperations := range byBranch {
		return branchOperations, branch, nil
	}
	return nil, "", graphPlanInvalidError(scope+" graph plan has no branch operations", nil)
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

func graphPlanDestinationBranchNames(operations []graphPlanOperation) map[string]map[string]struct{} {
	out := make(map[string]map[string]struct{})
	for i := range operations {
		operation := operations[i]
		if !graphPlanOperationDestinationsRequired(operation.Kind) {
			continue
		}
		for _, target := range operation.Destinations {
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
	selectOperation, _, targets, err := p.preparePacketCopyOperationLowering(plan)
	if err != nil {
		return err
	}
	if p.selectedStream {
		return p.compileSelectedPacketCopyBranchCompose(ctx, graph, service, selectOperation, targets)
	}
	sources, err := compileMediaPlanSources(ctx, p.runtime, graph, p.inputs, "build job", Intent{Streams: []StreamIntent{p.stream}})
	if err != nil {
		return err
	}
	return p.lowerPacketCopyTargets(ctx, graph, service, targets, sources.refs, sources.streamGroups)
}

func (p mediaPlanStreamGraph) compileSelectedPacketCopyBranchCompose(ctx context.Context, graph pipeline.Graph, service *builder, selectOperation graphPlanOperation, targets []graphPlanDestinationOperation) error {
	sources, err := compileMediaPlanSources(ctx, p.runtime, graph, p.inputs, "build packet copy", Intent{Streams: []StreamIntent{p.stream}})
	if err != nil {
		return err
	}
	branches, outputs := p.selectedPacketCopyBranchComposeRoutes()
	if len(branches) != 1 {
		return graphPlanInvalidError("selected packet-copy branch route count must be one", []string{
			"branches=" + strconv.Itoa(len(branches)),
		})
	}
	for i := range targets {
		target := targets[i]
		if target.OutputIndex < 0 || target.OutputIndex >= len(outputs) {
			return graphPlanInvalidError("selected packet-copy destination operation is not bound to a branch route destination", []string{
				"destination=" + target.Name,
				"node=" + target.Node.String(),
			})
		}
		outputs[target.OutputIndex].node = target.Node
	}
	groups, err := resolveBranchComposeStreamGroups(sources.streams, branches)
	if err != nil {
		return err
	}
	inputPlan := map[string]graphPlanBranchComposeInputOperation{
		branchComposeSelectorKey(streamIntentSelector(p.stream)): {
			selectNode: selectOperation.Node,
		},
	}
	branchInputs, branchStreams, err := compileBranchComposeInputs(ctx, p.runtime, graph, sources.refs, groups, sources.rtpBuilds, branches, inputPlan, sources.realtime)
	if err != nil {
		return err
	}
	return compileBranchComposeRoutes(ctx, service, graph, branches, outputs, branchInputs, branchStreams, nil, nil, sources.realtime)
}

func (p mediaPlanStreamGraph) preparePacketCopyOperationLowering(plan graphPlan) (graphPlanOperation, bool, []graphPlanDestinationOperation, error) {
	operations := plan.operations
	if p.selectedStream {
		branchOperations, _, err := graphPlanSingleBranchOperations(plan.operations, "selected packet-copy")
		if err != nil {
			return graphPlanOperation{}, false, nil, err
		}
		operations = branchOperations
	}
	selectOperation, hasSelect := graphPlanFirstOperation(operations, OpSelect)
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
	if err := p.validatePacketCopyOperationRecords(plan, operations); err != nil {
		return graphPlanOperation{}, false, nil, err
	}
	targets, err := p.preparePacketCopyDestinations(plan, operations)
	if err != nil {
		return graphPlanOperation{}, false, nil, err
	}
	return selectOperation, hasSelect, targets, nil
}

func (p mediaPlanStreamGraph) preparePacketCopyDestinations(plan graphPlan, operations []graphPlanOperation) ([]graphPlanDestinationOperation, error) {
	targets, err := graphPlanUniqueDestinationOperations(operations, "packet-copy")
	if err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		return nil, graphPlanInvalidError("packet-copy graph plan has no destination operations", nil)
	}
	branchesByTarget := graphPlanDestinationBranchNames(operations)
	for i := range targets {
		target := targets[i]
		if err := validateGraphPlanDestinationOperationNode("packet-copy", target); err != nil {
			return nil, err
		}
		outputIndex, ok := graphPlanOutputIndex(plan.outputs, target.Name)
		if !ok || outputIndex < 0 || outputIndex >= len(p.outputs) {
			return nil, graphPlanInvalidError("packet-copy destination operation is not bound to an output", []string{
				"destination=" + target.Name,
				"node=" + target.Node.String(),
			})
		}
		target.OutputIndex = outputIndex
		if !p.selectedStream {
			matches, ok := packetCopyDestinationOperationMatches(plan.branches, branchesByTarget[target.Name])
			if !ok || !packetCopyTargetBranchesMatch(plan.outputs[outputIndex], matches, plan.branches) {
				return nil, graphPlanInvalidError("packet-copy destination operation branches do not match output branches", []string{
					"destination=" + target.Name,
				})
			}
			target.Matches = matches
		}
		output := p.outputs[outputIndex]
		if !p.selectedStream && packetCopyTargetMatchesDomain(plan.branches, target.Matches, DomainEvent) && output.sink == nil {
			return nil, graphPlanInvalidError("event source destination must be a sink", []string{
				"destination=" + target.Name,
			})
		}
		if output.sink != nil {
			if target.Kind != OpSink {
				return nil, graphPlanInvalidError("packet-copy destination operation kind does not match sink destination", []string{
					"destination=" + target.Name,
					"kind=" + string(target.Kind),
				})
			}
			targets[i] = target
			continue
		}
		if target.Kind != OpMux && target.Kind != OpWrite {
			return nil, graphPlanInvalidError("packet-copy destination operation kind does not match byte destination", []string{
				"destination=" + target.Name,
				"kind=" + string(target.Kind),
			})
		}
		targets[i] = target
	}
	return targets, nil
}

func packetCopyTargetMatchesDomain(branches []planBranch, matches []int, domain MediaDomain) bool {
	for _, index := range matches {
		if index < 0 || index >= len(branches) {
			continue
		}
		if branches[index].Shape.Domain == domain {
			return true
		}
	}
	return false
}

func (p mediaPlanStreamGraph) validatePacketCopyOperationRecords(plan graphPlan, operations []graphPlanOperation) error {
	if p.selectedStream {
		if !graphPlanBranchOperationsContain(operations, OpCopy) {
			return graphPlanInvalidError("selected packet-copy graph plan has no copy operation", []string{
				"stream=" + firstNonEmpty(p.stream.Name, string(p.stream.Select.ID), string(p.stream.Select.Type), "stream"),
			})
		}
		return nil
	}
	byBranch := graphPlanOperationsByBranch(operations)
	if len(plan.branches) == 0 {
		return graphPlanInvalidError("packet-copy graph plan has no copy branches", nil)
	}
	for i := range plan.branches {
		branch := plan.branches[i]
		branchOperations := byBranch[branch.Name]
		if len(branchOperations) == 0 {
			return graphPlanInvalidError("packet-copy graph plan has no operations for branch", []string{
				"branch=" + branch.Name,
			})
		}
		if branch.Shape.Domain == DomainEvent {
			continue
		}
		if !graphPlanBranchOperationsContain(branchOperations, OpCopy) {
			return graphPlanInvalidError("packet-copy graph plan has no copy operation for branch", []string{
				"branch=" + branch.Name,
			})
		}
	}
	return nil
}

func (p mediaPlanStreamGraph) lowerPacketCopyTargets(
	ctx context.Context,
	graph pipeline.Graph,
	service *builder,
	targets []graphPlanDestinationOperation,
	targetRefs []pipeline.NodeRef,
	streamGroups [][]av.Stream,
) error {
	for i := range targets {
		target := targets[i]
		upstreamRefs, err := packetCopyDestinationRefs(target, targetRefs)
		if err != nil {
			return err
		}
		output := p.outputs[target.OutputIndex]
		if output.sink != nil {
			sinkRef, err := graph.AddSink(namedSinkForGraphPlanDestination(target, output.sink), p.runtime.buffer)
			if err != nil {
				return err
			}
			for j := range upstreamRefs {
				if err := connectRefs(graph, upstreamRefs[j], sinkRef); err != nil {
					return err
				}
			}
			continue
		}
		targetStreams, err := packetCopyDestinationStreams(target, streamGroups)
		if err != nil {
			return err
		}
		stage, err := service.openMuxDestinationStage(ctx, output, target.OutputIndex, targetStreams, destinationOpenFormat(output), destinationGraphFormat(output))
		if err != nil {
			return err
		}
		stageRef, err := graph.AddStage(namedStageForGraphPlanDestination(target, stage), p.runtime.buffer)
		if err != nil {
			stage.Close()
			return err
		}
		for j := range upstreamRefs {
			if err := connectRefs(graph, upstreamRefs[j], stageRef); err != nil {
				return err
			}
		}
	}
	return nil
}

func packetCopyDestinationRefs(target graphPlanDestinationOperation, refs []pipeline.NodeRef) ([]pipeline.NodeRef, error) {
	if len(target.Matches) == 0 {
		return nil, graphPlanInvalidError("packet-copy destination operation has no branch matches", []string{
			"destination=" + target.Name,
		})
	}
	out := make([]pipeline.NodeRef, 0, len(target.Matches))
	for _, index := range target.Matches {
		if index < 0 || index >= len(refs) {
			return nil, graphPlanInvalidError("packet-copy destination operation branch match is outside source refs", []string{
				"destination=" + target.Name,
				"match=" + strconv.Itoa(index),
				"sources=" + strconv.Itoa(len(refs)),
			})
		}
		out = append(out, refs[index])
	}
	return out, nil
}

func packetCopyDestinationStreams(target graphPlanDestinationOperation, streamGroups [][]av.Stream) ([]av.Stream, error) {
	if len(target.Matches) == 0 {
		return nil, graphPlanInvalidError("packet-copy destination operation has no branch matches", []string{
			"destination=" + target.Name,
		})
	}
	out := make([]av.Stream, 0, len(target.Matches))
	for _, index := range target.Matches {
		if index < 0 || index >= len(streamGroups) {
			return nil, graphPlanInvalidError("packet-copy destination operation branch match is outside stream groups", []string{
				"destination=" + target.Name,
				"match=" + strconv.Itoa(index),
				"stream_groups=" + strconv.Itoa(len(streamGroups)),
			})
		}
		out = append(out, streamGroups[index]...)
	}
	if len(out) == 0 {
		return nil, graphPlanInvalidError("packet-copy destination operation has no streams for matched branches", []string{
			"destination=" + target.Name,
		})
	}
	return out, nil
}

func graphPlanFirstOperation(operations []graphPlanOperation, kind OperationKind) (graphPlanOperation, bool) {
	for i := range operations {
		if operations[i].Kind == kind {
			return operations[i], true
		}
	}
	return graphPlanOperation{}, false
}

type graphPlanDestinationOperation struct {
	Name        string
	Node        pipeline.NodeRef
	Kind        OperationKind
	OutputIndex int
	Matches     []int
}

func validateGraphPlanDestinationOperationNode(scope string, target graphPlanDestinationOperation) error {
	if target.Node == "" {
		return graphPlanInvalidError(scope+" destination operation has no node", []string{
			"destination=" + target.Name,
			"kind=" + string(target.Kind),
		})
	}
	return nil
}

func namedSinkForGraphPlanDestination(target graphPlanDestinationOperation, sink pipeline.Sink) pipeline.Sink {
	if target.Node == "" {
		return sink
	}
	return namedSink{name: target.Node.String(), sink: sink}
}

func namedStageForGraphPlanDestination(target graphPlanDestinationOperation, stage pipeline.Stage) pipeline.Stage {
	if target.Node == "" {
		return stage
	}
	return namedStage{name: target.Node.String(), stage: stage}
}

func graphPlanDestinationOperations(operations []graphPlanOperation) []graphPlanDestinationOperation {
	targets, _ := graphPlanUniqueDestinationOperations(operations, "")
	return targets
}

func graphPlanUniqueDestinationOperations(operations []graphPlanOperation, scope string) ([]graphPlanDestinationOperation, error) {
	targets := make([]graphPlanDestinationOperation, 0)
	seen := make(map[string]int)
	for i := range operations {
		operation := operations[i]
		if !graphPlanOperationDestinationsRequired(operation.Kind) {
			continue
		}
		for _, target := range operation.Destinations {
			if target == "" {
				continue
			}
			next := graphPlanDestinationOperation{
				Name: target,
				Node: operation.Node,
				Kind: operation.Kind,
			}
			if index, ok := seen[target]; ok {
				if err := validateGraphPlanDuplicateDestinationOperation(scope, target, targets[index], next); err != nil {
					return nil, err
				}
				continue
			}
			seen[target] = len(targets)
			targets = append(targets, next)
		}
	}
	return targets, nil
}

func validateGraphPlanDuplicateDestinationOperation(scope string, target string, first graphPlanDestinationOperation, next graphPlanDestinationOperation) error {
	if first.Node == next.Node && first.Kind == next.Kind {
		return nil
	}
	return graphPlanInvalidError(firstNonEmpty(scope, "graph-plan")+" destination operation is not consistent across branches", []string{
		"destination=" + target,
		"first_node=" + first.Node.String(),
		"next_node=" + next.Node.String(),
		"first_kind=" + string(first.Kind),
		"next_kind=" + string(next.Kind),
	})
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
	selectNode  pipeline.NodeRef
	decodeNode  pipeline.NodeRef
	filterNodes []pipeline.NodeRef
	encodeNode  pipeline.NodeRef
	encodeShape MediaShape
	targets     []graphPlanDestinationOperation
}

func (p mediaPlanStreamGraph) prepareFrameOperationSpecLowering(plan graphPlan) (graphPlanFrameStreamLowering, error) {
	operations, branchName, err := graphPlanSingleBranchOperations(plan.operations, "frame stream")
	if err != nil {
		return graphPlanFrameStreamLowering{}, err
	}
	selectOperation, ok := graphPlanFirstOperation(operations, OpSelect)
	if !ok {
		return graphPlanFrameStreamLowering{}, graphPlanInvalidError("frame stream graph plan has no select operation", []string{
			"stream=" + firstNonEmpty(p.stream.Name, string(p.stream.Select.ID), string(p.stream.Select.Type), "stream"),
			"branch=" + branchName,
		})
	}
	if selectOperation.Node == "" {
		return graphPlanFrameStreamLowering{}, graphPlanInvalidError("frame stream graph plan select operation has no node", []string{
			"stream=" + firstNonEmpty(p.stream.Name, string(p.stream.Select.ID), string(p.stream.Select.Type), "stream"),
			"branch=" + branchName,
		})
	}
	decodeOperation, hasDecode := graphPlanFirstOperation(operations, OpDecode)
	if p.sourceDomain == DomainFrame && hasDecode {
		return graphPlanFrameStreamLowering{}, graphPlanInvalidError("frame source graph plan has an unexpected decode operation", []string{
			"stream=" + firstNonEmpty(p.stream.Name, string(p.stream.Select.ID), string(p.stream.Select.Type), "stream"),
			"branch=" + branchName,
		})
	}
	if p.sourceDomain != DomainFrame && !hasDecode {
		return graphPlanFrameStreamLowering{}, graphPlanInvalidError("frame stream graph plan has no decode operation", []string{
			"stream=" + firstNonEmpty(p.stream.Name, string(p.stream.Select.ID), string(p.stream.Select.Type), "stream"),
			"branch=" + branchName,
		})
	}
	if hasDecode && decodeOperation.Node == "" {
		return graphPlanFrameStreamLowering{}, graphPlanInvalidError("frame stream graph plan decode operation has no node", []string{
			"stream=" + firstNonEmpty(p.stream.Name, string(p.stream.Select.ID), string(p.stream.Select.Type), "stream"),
			"branch=" + branchName,
		})
	}
	filterNodes, err := p.validateFrameStreamFilterOperations(operations)
	if err != nil {
		return graphPlanFrameStreamLowering{}, err
	}
	encodeOperation, hasEncode := graphPlanFirstOperation(operations, OpEncode)
	if p.encode != nil && !hasEncode {
		return graphPlanFrameStreamLowering{}, graphPlanInvalidError("encoded frame stream graph plan has no encode operation", []string{
			"stream=" + firstNonEmpty(p.stream.Name, string(p.stream.Select.ID), string(p.stream.Select.Type), "stream"),
			"branch=" + branchName,
		})
	}
	var encodeNode pipeline.NodeRef
	var encodeShape MediaShape
	if p.encode != nil {
		if encodeOperation.Node == "" {
			return graphPlanFrameStreamLowering{}, graphPlanInvalidError("encoded frame stream graph plan encode operation has no node", []string{
				"stream=" + firstNonEmpty(p.stream.Name, string(p.stream.Select.ID), string(p.stream.Select.Type), "stream"),
				"branch=" + branchName,
			})
		}
		encodeNode = encodeOperation.Node
		encodeShape = encodeOperation.Shape
	}
	if p.encode == nil && hasEncode {
		return graphPlanFrameStreamLowering{}, graphPlanInvalidError("decoded frame stream graph plan has an unexpected encode operation", []string{
			"stream=" + firstNonEmpty(p.stream.Name, string(p.stream.Select.ID), string(p.stream.Select.Type), "stream"),
			"branch=" + branchName,
		})
	}
	targets, err := p.prepareFrameStreamDestinations(operations, plan.outputs)
	if err != nil {
		return graphPlanFrameStreamLowering{}, err
	}
	return graphPlanFrameStreamLowering{
		selectNode:  selectOperation.Node,
		decodeNode:  decodeOperation.Node,
		filterNodes: filterNodes,
		encodeNode:  encodeNode,
		encodeShape: encodeShape,
		targets:     targets,
	}, nil
}

func (p mediaPlanStreamGraph) validateFrameStreamFilterOperations(operations []graphPlanOperation) ([]pipeline.NodeRef, error) {
	planned := graphPlanOperationCount(operations, OpTransform) + graphPlanOperationCount(operations, OpStage)
	if planned != len(p.filters) {
		return nil, graphPlanInvalidError("frame stream graph plan filter operations do not match concrete filters", []string{
			"planned=" + strconv.Itoa(planned),
			"filters=" + strconv.Itoa(len(p.filters)),
		})
	}
	nodes := make([]pipeline.NodeRef, 0, planned)
	for i := range operations {
		operation := operations[i]
		if operation.Kind != OpTransform && operation.Kind != OpStage {
			continue
		}
		if operation.Node == "" {
			return nil, graphPlanInvalidError("frame stream graph plan filter operation has no node", []string{
				"kind=" + string(operation.Kind),
			})
		}
		nodes = append(nodes, operation.Node)
	}
	return nodes, nil
}

func (p mediaPlanStreamGraph) prepareFrameStreamDestinations(operations []graphPlanOperation, outputs []planOutput) ([]graphPlanDestinationOperation, error) {
	targets, err := graphPlanUniqueDestinationOperations(operations, "frame stream")
	if err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		return nil, graphPlanInvalidError("frame stream graph plan has no destination operations", nil)
	}
	if len(targets) != len(p.outputs) {
		return nil, graphPlanInvalidError("frame stream graph plan target count does not match outputs", []string{
			"targets=" + strconv.Itoa(len(targets)),
			"outputs=" + strconv.Itoa(len(p.outputs)),
		})
	}
	for i := range targets {
		target := targets[i]
		if err := validateGraphPlanDestinationOperationNode("frame stream", target); err != nil {
			return nil, err
		}
		outputIndex, ok := graphPlanOutputIndex(outputs, target.Name)
		if !ok || outputIndex < 0 || outputIndex >= len(p.outputs) {
			return nil, graphPlanInvalidError("frame stream destination operation is not bound to an output", []string{
				"destination=" + target.Name,
				"node=" + target.Node.String(),
			})
		}
		target.OutputIndex = outputIndex
		targets[i] = target
		output := p.outputs[outputIndex]
		if output.sink != nil {
			if target.Kind != OpSink {
				return nil, graphPlanInvalidError("frame stream destination operation kind does not match sink destination", []string{
					"destination=" + target.Name,
					"kind=" + string(target.Kind),
				})
			}
			continue
		}
		if p.encode == nil {
			return nil, graphPlanInvalidError("decoded frame stream cannot lower to a byte target without encode", []string{
				"destination=" + target.Name,
			})
		}
		if target.Kind != OpMux && target.Kind != OpWrite {
			return nil, graphPlanInvalidError("frame stream destination operation kind does not match byte destination", []string{
				"destination=" + target.Name,
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
	return p.frameStreamBranchComposeSpec()
}

func (p mediaPlanStreamGraph) encodeOutputSpec() (pipeline.Spec, error) {
	if p.encode == nil {
		return pipeline.Spec{}, recipeGraphUnsupportedError("describe job", Intent{Streams: []StreamIntent{p.stream}})
	}
	return p.frameStreamBranchComposeSpec()
}

func (p mediaPlanStreamGraph) frameStreamBranchComposeSpec() (pipeline.Spec, error) {
	spec, sourceRefs, nodes, err := p.specWithSources()
	if err != nil {
		return pipeline.Spec{}, err
	}
	branches, outputs, err := p.frameStreamBranchComposeRoutes()
	if err != nil {
		return pipeline.Spec{}, err
	}
	return planBranchComposeRoutes(spec, nodes, sourceRefs, branches, outputs)
}

func (p mediaPlanStreamGraph) frameStreamBranchComposeRoutes() ([]branchComposeRoute, []branchComposeTargetRoute, error) {
	branchName := firstNonEmpty(p.stream.Name, string(p.stream.Select.ID), string(p.stream.Select.Type), "branch")
	steps, err := mediaPlanFilterRouteSteps(p.filters)
	if err != nil {
		return nil, nil, err
	}
	request := encodeRequest{name: branchName, selector: p.decode.selector}
	if p.encode != nil {
		request = *p.encode
		if request.name == "" {
			request.name = branchName
		}
	}
	branches := []branchComposeRoute{{
		name:  branchName,
		steps: steps,
		branch: branchComposeBranch{
			Name:         branchName,
			Selector:     p.decode.selector,
			Decode:       p.sourceDomain != DomainFrame,
			DecodeConfig: cloneCodecSpec(p.decode.config),
			CodecChange:  p.decode.codecChange,
		},
		decode:           cloneCodecSpec(p.decode.config),
		codecChange:      p.decode.codecChange,
		dropDecodeEvents: p.encode == nil,
		sourceDomain:     p.sourceDomain,
		request:          request,
	}}
	return branches, mediaPlanBranchComposeTargetRoutes(p.outputs, branchName), nil
}

func mediaPlanBranchComposeTargetRoutes(outputs []destinationSpec, branchName string) []branchComposeTargetRoute {
	routes := make([]branchComposeTargetRoute, len(outputs))
	for i := range outputs {
		output := outputs[i]
		target := branchComposeTarget{
			Name:        output.label("output"),
			Destination: cloneDestinationSpec(output),
			Target:      output.output,
			Sink:        output.sink,
			Format:      destinationGraphFormat(output),
			Branches:    []string{branchName},
		}
		if output.resolvedFormat != "" {
			target = resolveBranchComposeTargetFormat(target, output.resolvedFormat)
		}
		routes[i] = branchComposeTargetRoute{
			output:  target,
			target:  branchComposeFormatTarget(branchComposePlan{}, target),
			sink:    output.sink,
			matches: []int{0},
		}
	}
	return routes
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

func mediaPlanFilterRouteSteps(filters []filterRequest) ([]mediaTransform, error) {
	steps := make([]mediaTransform, 0, len(filters))
	for i := range filters {
		filter := filters[i]
		switch {
		case filter.stage != nil:
			steps = append(steps, mediaTransform{name: filter.stage.Name(), stage: filter.stage})
		case filter.transform != nil:
			steps = append(steps, cloneMediaTransform(*filter.transform))
		default:
			return nil, ErrNilStage
		}
	}
	return steps, nil
}

func cloneMediaTransform(transform mediaTransform) mediaTransform {
	if transform.video != nil {
		video := *transform.video
		transform.video = &video
	}
	if transform.audio != nil {
		audio := *transform.audio
		transform.audio = &audio
	}
	return transform
}

func (p mediaPlanStreamGraph) compileFrameStreamBranchCompose(ctx context.Context, graph pipeline.Graph, service *builder, lowering graphPlanFrameStreamLowering) error {
	sources, err := compileMediaPlanSources(ctx, p.runtime, graph, p.inputs, "build job", Intent{Streams: []StreamIntent{p.stream}})
	if err != nil {
		return err
	}
	branches, outputs, err := p.frameStreamBranchComposeRoutes()
	if err != nil {
		return err
	}
	if len(branches) != 1 {
		return graphPlanInvalidError("frame stream branch route count must be one", []string{
			"branches=" + strconv.Itoa(len(branches)),
		})
	}
	for i := range lowering.targets {
		target := lowering.targets[i]
		if target.OutputIndex < 0 || target.OutputIndex >= len(outputs) {
			return graphPlanInvalidError("frame stream destination operation is not bound to a branch route destination", []string{
				"destination=" + target.Name,
				"node=" + target.Node.String(),
			})
		}
		outputs[target.OutputIndex].node = target.Node
	}
	groups, err := resolveBranchComposeStreamGroups(sources.streams, branches)
	if err != nil {
		return err
	}
	inputPlan := map[string]graphPlanBranchComposeInputOperation{
		branchComposeSelectorKey(p.decode.selector): {
			selectNode: lowering.selectNode,
			decodeNode: lowering.decodeNode,
		},
	}
	branchPlan := map[string]graphPlanBranchComposeBranchOperation{
		branches[0].name: {
			privateSteps: append([]pipeline.NodeRef(nil), lowering.filterNodes...),
			encodeNode:   lowering.encodeNode,
			encodeShape:  lowering.encodeShape,
		},
	}
	branchInputs, branchStreams, err := compileBranchComposeInputs(ctx, p.runtime, graph, sources.refs, groups, sources.rtpBuilds, branches, inputPlan, sources.realtime)
	if err != nil {
		return err
	}
	return compileBranchComposeRoutes(ctx, service, graph, branches, outputs, branchInputs, branchStreams, nil, branchPlan, sources.realtime)
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
	if len(inputs) == 1 && inputs[0].source != nil {
		source, streams, err := newCustomSource(inputs[0])
		if err != nil {
			return mediaPlanCompiledSources{}, err
		}
		sourceRef, err := graph.AddSource(source, runtime.buffer)
		if err != nil {
			source.Close()
			return mediaPlanCompiledSources{}, err
		}
		return mediaPlanCompiledSources{
			refs:         []pipeline.NodeRef{sourceRef},
			streams:      append([]av.Stream(nil), streams...),
			streamGroups: [][]av.Stream{append([]av.Stream(nil), streams...)},
			realtime:     runtime.realtime || inputs[0].source.shape.Realtime,
		}, nil
	}
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
			refs:         []pipeline.NodeRef{sourceRef},
			streams:      append([]av.Stream(nil), demux.streams...),
			streamGroups: [][]av.Stream{append([]av.Stream(nil), demux.streams...)},
			realtime:     runtime.realtime || input.Realtime,
		}, nil
	}
	if !allRTPInputSpecs(inputs) {
		return mediaPlanCompiledSources{}, recipeGraphUnsupportedError(operation, intent)
	}
	sourceRefs := make([]pipeline.NodeRef, 0, len(inputs))
	streams := make([]av.Stream, 0, len(inputs))
	streamGroups := make([][]av.Stream, 0, len(inputs))
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
		sourceStreams := append([]av.Stream(nil), receiver.streams...)
		streams = append(streams, sourceStreams...)
		streamGroups = append(streamGroups, sourceStreams)
		builds = append(builds, receiver)
	}
	return mediaPlanCompiledSources{
		refs:         sourceRefs,
		streams:      streams,
		streamGroups: streamGroups,
		rtpBuilds:    builds,
		realtime:     runtime.realtime,
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
