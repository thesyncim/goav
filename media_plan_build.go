package goav

import (
	"context"
	"sort"
	"strconv"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

func (r recipeResolved) singleStreamIntent() (streamIntent, bool) {
	if len(r.intent.Streams) != 1 {
		return streamIntent{}, false
	}
	return r.intent.Streams[0], true
}

type mediaPlanStreamGraph struct {
	runtime        *runtime
	inputs         []InputSpec
	outputs        []destinationSpec
	stream         streamIntent
	copyPackets    bool
	selectedStream bool
	sourceDomain   shape.MediaDomain
	decode         decodeRequest
	filters        []filterRequest
	encode         *encodeRequest
}

type mediaPlanBranchComposeGraph struct {
	runtime      *runtime
	inputs       []InputSpec
	plan         branchComposePlan
	branches     []branchComposeRoute
	destinations []branchComposeTargetRoute
}

type mediaPlanCompiledSources struct {
	refs         []pipeline.NodeRef
	streams      []av.Stream
	streamGroups [][]av.Stream
	bounds       []decodeBoundsProvider
	realtime     bool
}

// graphPlanGraphConfigurer lets a lowerer pin its graph identity and buffer
// policy (a join names its graph after the kind and may require a buffered
// graph for control-plane injection).
type graphPlanGraphConfigurer interface {
	graphConfig(graphPlan) (pipeline.GraphConfig, error)
}

func buildGraphPlanTask(ctx context.Context, gp graphPlan) (Task, error) {
	runtime := gp.runtime
	if runtime == nil {
		return nil, recipeGraphUnsupportedError("build recipe", intent{})
	}
	if err := validateGraphPlanLowering(gp); err != nil {
		return nil, err
	}
	service := &builder{runtime: runtime, requireRunOK: true}
	var graph pipeline.Graph
	var err error
	if configurer, ok := gp.lowerer.(graphPlanGraphConfigurer); ok {
		config, configErr := configurer.graphConfig(gp)
		if configErr != nil {
			return nil, configErr
		}
		runtime = runtimeWithBuffer(runtime, config.Buffer)
		gp = graphPlanWithRuntime(gp, runtime)
		service.runtime = runtime
		graph, err = pipeline.NewGraph(config)
	} else {
		graph, err = service.newGraph(ctx)
	}
	if err != nil {
		return nil, err
	}
	if err := gp.lower(ctx, graph, service); err != nil {
		graph.Close()
		return nil, err
	}
	return newTask(graph, runtime, service.destinationTxs...), nil
}

func runtimeWithBuffer(rt *runtime, buffer pipeline.BufferPolicy) *runtime {
	if rt == nil {
		return nil
	}
	clone := *rt
	clone.buffer = buffer
	return &clone
}

func graphPlanWithRuntime(gp graphPlan, rt *runtime) graphPlan {
	gp.runtime = rt
	switch lowerer := gp.lowerer.(type) {
	case mediaPlanStreamGraph:
		lowerer.runtime = rt
		gp.lowerer = lowerer
	case mediaPlanBranchComposeGraph:
		lowerer.runtime = rt
		gp.lowerer = lowerer
	case *joinPlan:
		gp.lowerer = joinPlanWithRuntime(lowerer, rt)
	}
	return gp
}

func joinPlanWithRuntime(plan *joinPlan, rt *runtime) *joinPlan {
	if plan == nil {
		return nil
	}
	clone := *plan
	clone.runtime = rt
	if len(plan.arms) != 0 {
		clone.arms = append([]joinArmPlan(nil), plan.arms...)
		for i := range clone.arms {
			clone.arms[i].sub = joinPlanWithRuntime(plan.arms[i].sub, rt)
		}
	}
	return &clone
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

func (p mediaPlanStreamGraph) graphConfig(gp graphPlan) (pipeline.GraphConfig, error) {
	if !p.copyPackets {
		if _, err := p.prepareFrameOperationSpecLowering(gp); err != nil {
			return pipeline.GraphConfig{}, err
		}
	}
	buffered := p.needsRealtimeBufferedGraph()
	return recipeGraphConfig(p.runtime, gp.name, gp.work, buffered)
}

func (p mediaPlanStreamGraph) needsRealtimeBufferedGraph() bool {
	if p.runtime == nil || !p.runtime.realtime || p.copyPackets {
		return false
	}
	return p.encode != nil || p.sourceDomain == shape.DomainPacket
}

func (p mediaPlanStreamGraph) lower(ctx context.Context, gp graphPlan, graph pipeline.Graph, service *builder) error {
	if p.copyPackets {
		return p.lowerPacketCopy(ctx, gp, graph, service)
	}
	lowering, err := p.prepareFrameOperationSpecLowering(gp)
	if err != nil {
		return err
	}
	return p.compileFrameStreamBranchCompose(ctx, graph, service, lowering)
}

func (p mediaPlanStreamGraph) hasSingleSinkDestination() bool {
	return len(p.outputs) == 1 && p.outputs[0].sink != nil
}

func newMediaPlanDecodeStreamGraph(rt Runtime, inputs []InputSpec, outputs []destinationSpec, stream streamIntent) (mediaPlanStreamGraph, bool, error) {
	runtime, ok := rt.(*runtime)
	if !ok || runtime == nil {
		return mediaPlanStreamGraph{}, false, nil
	}
	if !mediaPlanStreamInputsSupported(inputs) {
		return mediaPlanStreamGraph{}, false, nil
	}
	selector := streamIntentSelector(stream)
	sg := mediaPlanStreamGraph{
		runtime:      runtime,
		inputs:       append([]InputSpec(nil), inputs...),
		outputs:      append([]destinationSpec(nil), outputs...),
		stream:       stream,
		sourceDomain: mediaPlanStreamInputDomain(inputs, stream),
		decode: decodeRequest{
			selector:    selector,
			codecChange: stream.CodecChange,
			config:      cloneCodecSpec(chainDecodeCodec(stream.Operations)),
		},
	}
	filters, err := mediaPlanStreamFilters(stream)
	if err != nil {
		return mediaPlanStreamGraph{}, false, err
	}
	sg.filters = filters
	if encode := chainEncodeSpec(stream.Operations); codecIntentSet(encode) && !encode.Copy {
		request := encodeRequest{
			selector: selector,
			config:   encodeConfigFromSpec(encode),
		}
		sg.encode = &request
	}
	return sg, true, nil
}

func mediaPlanInputDomain(inputs []InputSpec) shape.MediaDomain {
	if len(inputs) == 1 {
		if shape, ok := customSourceShape(inputs[0]); ok && shape.Domain != "" {
			return shape.Domain
		}
	}
	return shape.DomainPacket
}

// mediaPlanStreamInputDomain resolves the media domain feeding one stream
// chain: the single input's domain (legacy) or, with several inputs, the
// domain of the input the chain's selector binds to.
func mediaPlanStreamInputDomain(inputs []InputSpec, stream streamIntent) shape.MediaDomain {
	if len(inputs) <= 1 {
		return mediaPlanInputDomain(inputs)
	}
	sets := inputSpecStreamSets(inputs)
	if index, ok := resolveInputSetIndex(sets, streamIntentSelector(stream), stream.Select.Input); ok && sets[index].domain != "" {
		return sets[index].domain
	}
	return shape.DomainPacket
}

func mediaPlanStreamInputsSupported(inputs []InputSpec) bool {
	if len(inputs) == 1 {
		return true
	}
	if len(inputs) == 0 {
		return false
	}
	// Several inputs converge on one job only for independently-running
	// realtime providers and custom sources (mirrors validateJobInputs).
	for i := range inputs {
		if inputs[i].provider == nil && inputs[i].source == nil {
			return false
		}
	}
	return true
}

func newMediaPlanPacketCopyStreamGraph(rt Runtime, inputs []InputSpec, outputs []destinationSpec, stream streamIntent, selectedStream bool) (mediaPlanStreamGraph, bool, error) {
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
		return pipeline.Spec{}, recipeGraphUnsupportedError("describe job", intent{Streams: []streamIntent{p.stream}})
	}
	if p.selectedStream {
		branches, outputs := p.selectedPacketCopyBranchComposeRoutes()
		return planBranchComposeRoutes(spec, nodes, sourceRefs, branches, outputs)
	}
	if mediaPlanInputsContainDomain(p.inputs, shape.DomainEvent) && outputsContainMuxDestination(p.outputs) {
		return pipeline.Spec{}, graphPlanInvalidError("event source destination must be a sink", nil)
	}
	upstreamRefs := sourceRefs
	targetRefs, err := mediaPlanPacketCopyDestinations(&spec, nodes, p.outputs)
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

func mediaPlanInputsContainDomain(inputs []InputSpec, domain shape.MediaDomain) bool {
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
			Input:    p.stream.Select.Input,
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

func newMediaPlanBranchComposeGraph(rt Runtime, inputs []InputSpec, composePlan branchComposePlan) (mediaPlanBranchComposeGraph, bool, error) {
	runtime, ok := rt.(*runtime)
	if !ok || runtime == nil {
		return mediaPlanBranchComposeGraph{}, false, nil
	}
	if len(inputs) == 0 {
		return mediaPlanBranchComposeGraph{}, false, nil
	}
	for i := range inputs {
		input := inputs[i]
		if input.provider == nil && input.source == nil && input.formatInput().Reader == nil && input.formatInput().URI == "" && input.formatInput().Name == "" {
			return mediaPlanBranchComposeGraph{}, false, nil
		}
	}
	branches, destinations, err := prepareBranchComposePlan(composePlan)
	if err != nil {
		return mediaPlanBranchComposeGraph{}, false, err
	}
	if len(inputs) == 1 {
		sourceDomain := mediaPlanInputDomain(inputs)
		for i := range branches {
			branches[i].sourceDomain = sourceDomain
		}
	} else {
		// Each branch keeps the media domain of the input it binds to, so
		// frame-domain custom sources keep their no-decode contract while
		// packet sources still decode.
		sets := inputSpecStreamSets(inputs)
		for i := range branches {
			branches[i].sourceDomain = shape.DomainPacket
			if index, ok := resolveInputSetIndex(sets, branches[i].branch.Selector, branches[i].branch.Input); ok {
				branches[i].sourceDomain = sets[index].domain
			}
		}
	}
	return mediaPlanBranchComposeGraph{
		runtime:      runtime,
		inputs:       append([]InputSpec(nil), inputs...),
		plan:         composePlan,
		branches:     branches,
		destinations: destinations,
	}, true, nil
}

func (p mediaPlanBranchComposeGraph) spec() (pipeline.Spec, error) {
	spec := pipeline.Spec{Name: "goav", Realtime: p.runtime.realtime}
	nodes := make(map[string]plannedNode, p.nodeCapacity())
	sourceRefs, ok, err := mediaPlanSourceSpecs(&spec, nodes, p.inputs)
	if err != nil {
		return pipeline.Spec{}, err
	}
	if !ok {
		return pipeline.Spec{}, recipeGraphUnsupportedError("describe branch composition", intent{Name: p.plan.Name})
	}
	return planBranchComposeRoutes(spec, nodes, sourceRefs, p.branches, p.destinations)
}

func (p mediaPlanBranchComposeGraph) nodeCapacity() int {
	return len(p.inputs) + 3 + len(p.branches) + branchComposeOperationStageCount(p.branches) + len(p.destinations)
}

func (p mediaPlanBranchComposeGraph) runtimeRef() *runtime {
	return p.runtime
}

func (p mediaPlanBranchComposeGraph) graphConfig(gp graphPlan) (pipeline.GraphConfig, error) {
	if _, err := p.prepareBranchComposeOperationLowering(gp); err != nil {
		return pipeline.GraphConfig{}, err
	}
	return recipeGraphConfig(p.runtime, gp.name, gp.work, p.needsRealtimeBufferedGraph())
}

func (p mediaPlanBranchComposeGraph) needsRealtimeBufferedGraph() bool {
	if p.runtime == nil || !p.runtime.realtime {
		return false
	}
	for i := range p.branches {
		if branchComposeRouteNeedsDecode(p.branches[i]) || branchComposeRouteNeedsEncode(p.branches[i]) {
			return true
		}
	}
	return false
}

func (p mediaPlanBranchComposeGraph) lower(ctx context.Context, gp graphPlan, graph pipeline.Graph, service *builder) error {
	lowering, err := p.prepareBranchComposeOperationLowering(gp)
	if err != nil {
		return err
	}
	sources, err := compileMediaPlanSources(ctx, p.runtime, graph, p.inputs, "build branch composition", intent{Name: p.plan.Name})
	if err != nil {
		return err
	}
	groups, err := resolveBranchComposeStreamGroupsForInputs(sources, p.inputs, p.branches)
	if err != nil {
		return err
	}
	branchInputs, branchStreams, err := compileBranchComposeInputs(ctx, p.runtime, graph, sources.refs, groups, sources.bounds, p.branches, lowering.inputs, sources.realtime)
	if err != nil {
		return err
	}
	return compileBranchComposeRoutes(ctx, service, graph, p.branches, lowering.destinations, branchInputs, branchStreams, lowering.sharedStageNodes, lowering.branches, sources.realtime)
}

type graphPlanBranchComposeLowering struct {
	inputs           map[string]graphPlanBranchComposeInputOperation
	sharedStageNodes map[string][]pipeline.NodeRef
	branches         map[string]graphPlanBranchComposeBranchOperation
	destinations     []branchComposeTargetRoute
}

type graphPlanBranchComposeInputOperation struct {
	selectNode pipeline.NodeRef
	decodeNode pipeline.NodeRef
}

type graphPlanBranchComposeBranchOperation struct {
	privateStageNodes []pipeline.NodeRef
	encodeNode        pipeline.NodeRef
	encodeShape       shape.Spec
}

func (p mediaPlanBranchComposeGraph) prepareBranchComposeOperationLowering(gp graphPlan) (graphPlanBranchComposeLowering, error) {
	branchOperations := graphPlanOperationsByBranch(gp.work.Operations)
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
	sharedStageNodes, err := p.prepareBranchComposeSharedStageOperations(branchOperations)
	if err != nil {
		return graphPlanBranchComposeLowering{}, err
	}
	branches, err := p.prepareBranchComposeBranchOperations(branchOperations)
	if err != nil {
		return graphPlanBranchComposeLowering{}, err
	}
	destinations, err := p.prepareBranchComposeDestinations(gp)
	if err != nil {
		return graphPlanBranchComposeLowering{}, err
	}
	return graphPlanBranchComposeLowering{inputs: inputs, sharedStageNodes: sharedStageNodes, branches: branches, destinations: destinations}, nil
}

func (p mediaPlanBranchComposeGraph) prepareBranchComposeInputOperations(branchOperations map[string][]workOperation) (map[string]graphPlanBranchComposeInputOperation, error) {
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
			selectOperation, ok := graphPlanBranchOperation(operations, plan.OpSelect)
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
			decodeOperation, ok := graphPlanBranchOperation(operations, plan.OpDecode)
			if !ok {
				return nil, graphPlanInvalidError("branch composition graph plan has no decode operation for branch", []string{
					"branch=" + branch.name,
				})
			}
			if err := assignBranchComposeInputNode(&input.decodeNode, decodeOperation.Node, branch.name, "decode"); err != nil {
				return nil, err
			}
		}
		inputs[branchComposeSelectorGroupKey(groups[i].selector, groups[i].input)] = input
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

func (p mediaPlanBranchComposeGraph) prepareBranchComposeSharedStageOperations(branchOperations map[string][]workOperation) (map[string][]pipeline.NodeRef, error) {
	nodesByBranch := make(map[string][]pipeline.NodeRef, len(p.branches))
	for i := range p.branches {
		branch := p.branches[i]
		refs, err := graphPlanBranchStepOperationNodes(branchOperations[branch.name], true, branch.name)
		if err != nil {
			return nil, err
		}
		nodesByBranch[branch.name] = refs
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
				next := nodesByBranch[branch.name]
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
	return nodesByBranch, nil
}

func (p mediaPlanBranchComposeGraph) prepareBranchComposeBranchOperations(branchOperations map[string][]workOperation) (map[string]graphPlanBranchComposeBranchOperation, error) {
	out := make(map[string]graphPlanBranchComposeBranchOperation, len(p.branches))
	for i := range p.branches {
		branch := p.branches[i]
		operations := branchOperations[branch.name]
		privateStageNodes, err := graphPlanBranchStepOperationNodes(operations, false, branch.name)
		if err != nil {
			return nil, err
		}
		var encodeNode pipeline.NodeRef
		var encodeShape shape.Spec
		if branchComposeRouteNeedsEncode(branch) {
			operation, ok := graphPlanBranchOperation(operations, plan.OpEncode)
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
			encodeShape = operation.ShapeOut
		}
		out[branch.name] = graphPlanBranchComposeBranchOperation{
			privateStageNodes: privateStageNodes,
			encodeNode:        encodeNode,
			encodeShape:       encodeShape,
		}
	}
	return out, nil
}

func (p mediaPlanBranchComposeGraph) validateBranchComposeBranchOperations(branch branchComposeRoute, operations []workOperation) error {
	if !graphPlanBranchOperationsContain(operations, plan.OpSelect) {
		return graphPlanInvalidError("branch composition graph plan has no select operation for branch", []string{
			"branch=" + branch.name,
		})
	}
	hasDecode := graphPlanBranchOperationsContain(operations, plan.OpDecode)
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
	if !branchComposeRouteNeedsDecode(branch) && branch.sourceDomain != shape.DomainFrame && !graphPlanBranchOperationsContain(operations, plan.OpCopy) {
		return graphPlanInvalidError("packet branch composition graph plan has no copy operation for branch", []string{
			"branch=" + branch.name,
		})
	}
	if err := p.validateBranchComposeStepOperations(branch, operations); err != nil {
		return err
	}
	hasEncode := graphPlanBranchOperationsContain(operations, plan.OpEncode)
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

func graphPlanBranchOperation(operations []workOperation, kind plan.OperationKind) (workOperation, bool) {
	for i := range operations {
		if operations[i].Kind == kind {
			return operations[i], true
		}
	}
	return workOperation{}, false
}

func (p mediaPlanBranchComposeGraph) validateBranchComposeStepOperations(branch branchComposeRoute, operations []workOperation) error {
	shared := graphPlanBranchStepOperationCount(operations, true)
	private := graphPlanBranchStepOperationCount(operations, false)
	wantShared := branchComposeRouteStageOperationCount(branch.sharedOperations)
	wantPrivate := branchComposeRouteStageOperationCount(branch.privateOperations)
	if shared != wantShared {
		return graphPlanInvalidError("branch composition graph plan shared operations do not match branch chain", []string{
			"branch=" + branch.name,
			"planned=" + strconv.Itoa(shared),
			"operations=" + strconv.Itoa(wantShared),
		})
	}
	if private != wantPrivate {
		return graphPlanInvalidError("branch composition graph plan operations do not match branch chain", []string{
			"branch=" + branch.name,
			"planned=" + strconv.Itoa(private),
			"operations=" + strconv.Itoa(wantPrivate),
		})
	}
	return nil
}

func (p mediaPlanBranchComposeGraph) prepareBranchComposeDestinations(gp graphPlan) ([]branchComposeTargetRoute, error) {
	operations, err := graphPlanUniqueDestinationOperations(gp.work.Operations, gp.work.Destinations, "branch composition")
	if err != nil {
		return nil, err
	}
	if len(operations) == 0 {
		return nil, graphPlanInvalidError("branch composition graph plan has no destination operations", nil)
	}
	if len(operations) != len(p.destinations) {
		return nil, graphPlanInvalidError("branch composition graph plan target count does not match destinations", []string{
			"destinations=" + strconv.Itoa(len(operations)),
			"routes=" + strconv.Itoa(len(p.destinations)),
		})
	}
	branchesByTarget := graphPlanDestinationBranchNames(gp.work.Operations)
	destinations := make([]branchComposeTargetRoute, len(operations))
	for i := range operations {
		operation := operations[i]
		targetIndex := branchComposeTargetRouteIndex(p.destinations, operation.Name)
		if targetIndex < 0 {
			return nil, graphPlanInvalidError("branch composition destination operation is not bound to a destination route", []string{
				"destination=" + operation.Name,
				"node=" + operation.Node.String(),
			})
		}
		target := p.destinations[targetIndex]
		if err := validateBranchComposeDestinationOperation(operation, target); err != nil {
			return nil, err
		}
		matches, ok := branchComposeDestinationOperationMatches(p.branches, branchesByTarget[operation.ID])
		if !ok || !branchComposeTargetBranchesMatch(target, matches) {
			return nil, graphPlanInvalidError("branch composition destination operation branches do not match destination routes", []string{
				"destination=" + operation.Name,
			})
		}
		target.node = operation.Node
		target.matches = matches
		destinations[i] = target
	}
	return destinations, nil
}

func validateBranchComposeDestinationOperation(operation graphPlanDestinationOperation, target branchComposeTargetRoute) error {
	if err := validateGraphPlanDestinationOperationNode("branch composition", operation); err != nil {
		return err
	}
	if target.sink != nil {
		if operation.Kind != plan.OpSink {
			return graphPlanInvalidError("branch composition destination operation kind does not match sink destination", []string{
				"destination=" + operation.Name,
				"kind=" + string(operation.Kind),
			})
		}
		return nil
	}
	if operation.Kind != plan.OpMux && operation.Kind != plan.OpWrite {
		return graphPlanInvalidError("branch composition destination operation kind does not match byte destination", []string{
			"destination=" + operation.Name,
			"kind=" + string(operation.Kind),
		})
	}
	return nil
}

func branchComposeTargetRouteIndex(destinations []branchComposeTargetRoute, name string) int {
	for i := range destinations {
		if branchComposeTargetRouteName(destinations[i]) == name {
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

func packetCopyDestinationOperationMatches(branches []workBranch, actual map[string]struct{}) ([]int, bool) {
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

func packetCopyTargetBranchesMatch(output workDestination, matches []int, branches []workBranch) bool {
	if len(output.Branches) != len(matches) {
		return false
	}
	seen := make(map[string]struct{}, len(output.Branches))
	for _, branch := range output.Branches {
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

func graphPlanOperationsByBranch(operations []workOperation) map[string][]workOperation {
	out := make(map[string][]workOperation)
	for i := range operations {
		branch := operations[i].Branch
		if branch == "" {
			continue
		}
		out[branch] = append(out[branch], operations[i])
	}
	return out
}

func graphPlanSingleBranchOperations(operations []workOperation, scope string) ([]workOperation, string, error) {
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

func graphPlanBranchOperationsContain(operations []workOperation, kind plan.OperationKind) bool {
	for i := range operations {
		if operations[i].Kind == kind {
			return true
		}
	}
	return false
}

func graphPlanBranchStepOperationCount(operations []workOperation, shared bool) int {
	count := 0
	for i := range operations {
		operation := operations[i]
		if operation.Shared != shared {
			continue
		}
		if operation.Kind == plan.OpTransform || operation.Kind == plan.OpStage {
			count++
		}
	}
	return count
}

func graphPlanBranchStepOperationNodes(operations []workOperation, shared bool, branch string) ([]pipeline.NodeRef, error) {
	refs := make([]pipeline.NodeRef, 0)
	for i := range operations {
		operation := operations[i]
		if operation.Shared != shared {
			continue
		}
		if operation.Kind != plan.OpTransform && operation.Kind != plan.OpStage {
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

func graphPlanDestinationBranchNames(operations []workOperation) map[string]map[string]struct{} {
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

func (p mediaPlanStreamGraph) lowerPacketCopy(ctx context.Context, gp graphPlan, graph pipeline.Graph, service *builder) error {
	selectOperation, _, destinations, err := p.preparePacketCopyOperationLowering(gp)
	if err != nil {
		return err
	}
	if p.selectedStream {
		return p.compileSelectedPacketCopyBranchCompose(ctx, graph, service, selectOperation, destinations)
	}
	sources, err := compileMediaPlanSources(ctx, p.runtime, graph, p.inputs, "build job", intent{Streams: []streamIntent{p.stream}})
	if err != nil {
		return err
	}
	return p.lowerPacketCopyDestinations(ctx, graph, service, destinations, sources.refs, sources.streamGroups)
}

func (p mediaPlanStreamGraph) compileSelectedPacketCopyBranchCompose(ctx context.Context, graph pipeline.Graph, service *builder, selectOperation workOperation, destinations []graphPlanDestinationOperation) error {
	sources, err := compileMediaPlanSources(ctx, p.runtime, graph, p.inputs, "build packet copy", intent{Streams: []streamIntent{p.stream}})
	if err != nil {
		return err
	}
	branches, outputs := p.selectedPacketCopyBranchComposeRoutes()
	if len(branches) != 1 {
		return graphPlanInvalidError("selected packet-copy branch route count must be one", []string{
			"branches=" + strconv.Itoa(len(branches)),
		})
	}
	for i := range destinations {
		target := destinations[i]
		if target.OutputIndex < 0 || target.OutputIndex >= len(outputs) {
			return graphPlanInvalidError("selected packet-copy destination operation is not bound to a branch route destination", []string{
				"destination=" + target.Name,
				"node=" + target.Node.String(),
			})
		}
		outputs[target.OutputIndex].node = target.Node
	}
	groups, err := resolveBranchComposeStreamGroupsForInputs(sources, p.inputs, branches)
	if err != nil {
		return err
	}
	inputPlan := map[string]graphPlanBranchComposeInputOperation{
		branchComposeSelectorGroupKey(streamIntentSelector(p.stream), p.stream.Select.Input): {
			selectNode: selectOperation.Node,
		},
	}
	branchInputs, branchStreams, err := compileBranchComposeInputs(ctx, p.runtime, graph, sources.refs, groups, sources.bounds, branches, inputPlan, sources.realtime)
	if err != nil {
		return err
	}
	return compileBranchComposeRoutes(ctx, service, graph, branches, outputs, branchInputs, branchStreams, nil, nil, sources.realtime)
}

func (p mediaPlanStreamGraph) preparePacketCopyOperationLowering(gp graphPlan) (workOperation, bool, []graphPlanDestinationOperation, error) {
	operations := gp.work.Operations
	if p.selectedStream {
		branchOperations, _, err := graphPlanSingleBranchOperations(gp.work.Operations, "selected packet-copy")
		if err != nil {
			return workOperation{}, false, nil, err
		}
		operations = branchOperations
	}
	selectOperation, hasSelect := graphPlanFirstOperation(operations, plan.OpSelect)
	switch {
	case p.selectedStream && !hasSelect:
		return workOperation{}, false, nil, graphPlanInvalidError("selected packet-copy graph plan has no select operation", []string{
			"stream=" + firstNonEmpty(p.stream.Name, string(p.stream.Select.ID), string(p.stream.Select.Type), "stream"),
		})
	case !p.selectedStream && hasSelect:
		return workOperation{}, false, nil, graphPlanInvalidError("packet-copy graph plan has an unexpected select operation", []string{
			"node=" + selectOperation.Node.String(),
		})
	}
	if err := p.validatePacketCopyOperationRecords(gp, operations); err != nil {
		return workOperation{}, false, nil, err
	}
	destinations, err := p.preparePacketCopyDestinations(gp, operations)
	if err != nil {
		return workOperation{}, false, nil, err
	}
	return selectOperation, hasSelect, destinations, nil
}

func (p mediaPlanStreamGraph) preparePacketCopyDestinations(gp graphPlan, operations []workOperation) ([]graphPlanDestinationOperation, error) {
	destinations, err := graphPlanUniqueDestinationOperations(operations, gp.work.Destinations, "packet-copy")
	if err != nil {
		return nil, err
	}
	if len(destinations) == 0 {
		return nil, graphPlanInvalidError("packet-copy graph plan has no destination operations", nil)
	}
	branchesByTarget := graphPlanDestinationBranchNames(operations)
	for i := range destinations {
		target := destinations[i]
		if err := validateGraphPlanDestinationOperationNode("packet-copy", target); err != nil {
			return nil, err
		}
		outputIndex, ok := workDestinationIndexByID(gp.work.Destinations, target.ID)
		if !ok || outputIndex < 0 || outputIndex >= len(p.outputs) {
			return nil, graphPlanInvalidError("packet-copy destination operation is not bound to an output", []string{
				"destination=" + target.Name,
				"node=" + target.Node.String(),
			})
		}
		target.OutputIndex = outputIndex
		if !p.selectedStream {
			matches, ok := packetCopyDestinationOperationMatches(gp.work.Branches, branchesByTarget[target.ID])
			if !ok || !packetCopyTargetBranchesMatch(gp.work.Destinations[outputIndex], matches, gp.work.Branches) {
				return nil, graphPlanInvalidError("packet-copy destination operation branches do not match output branches", []string{
					"destination=" + target.Name,
				})
			}
			target.Matches = matches
		}
		output := p.outputs[outputIndex]
		if !p.selectedStream && packetCopyTargetMatchesDomain(gp.work.Branches, target.Matches, shape.DomainEvent) && output.sink == nil {
			return nil, graphPlanInvalidError("event source destination must be a sink", []string{
				"destination=" + target.Name,
			})
		}
		if output.sink != nil {
			if target.Kind != plan.OpSink {
				return nil, graphPlanInvalidError("packet-copy destination operation kind does not match sink destination", []string{
					"destination=" + target.Name,
					"kind=" + string(target.Kind),
				})
			}
			destinations[i] = target
			continue
		}
		if target.Kind != plan.OpMux && target.Kind != plan.OpWrite {
			return nil, graphPlanInvalidError("packet-copy destination operation kind does not match byte destination", []string{
				"destination=" + target.Name,
				"kind=" + string(target.Kind),
			})
		}
		destinations[i] = target
	}
	return destinations, nil
}

func packetCopyTargetMatchesDomain(branches []workBranch, matches []int, domain shape.MediaDomain) bool {
	for _, index := range matches {
		if index < 0 || index >= len(branches) {
			continue
		}
		if branches[index].SourceShape.Domain == domain {
			return true
		}
	}
	return false
}

func (p mediaPlanStreamGraph) validatePacketCopyOperationRecords(gp graphPlan, operations []workOperation) error {
	if p.selectedStream {
		if !graphPlanBranchOperationsContain(operations, plan.OpCopy) {
			return graphPlanInvalidError("selected packet-copy graph plan has no copy operation", []string{
				"stream=" + firstNonEmpty(p.stream.Name, string(p.stream.Select.ID), string(p.stream.Select.Type), "stream"),
			})
		}
		return nil
	}
	byBranch := graphPlanOperationsByBranch(operations)
	if len(gp.work.Branches) == 0 {
		return graphPlanInvalidError("packet-copy graph plan has no copy branches", nil)
	}
	for i := range gp.work.Branches {
		branch := gp.work.Branches[i]
		branchOperations := byBranch[branch.Name]
		if len(branchOperations) == 0 {
			return graphPlanInvalidError("packet-copy graph plan has no operations for branch", []string{
				"branch=" + branch.Name,
			})
		}
		if branch.SourceShape.Domain == shape.DomainEvent {
			continue
		}
		if !graphPlanBranchOperationsContain(branchOperations, plan.OpCopy) {
			return graphPlanInvalidError("packet-copy graph plan has no copy operation for branch", []string{
				"branch=" + branch.Name,
			})
		}
	}
	return nil
}

func (p mediaPlanStreamGraph) lowerPacketCopyDestinations(
	ctx context.Context,
	graph pipeline.Graph,
	service *builder,
	destinations []graphPlanDestinationOperation,
	targetRefs []pipeline.NodeRef,
	streamGroups [][]av.Stream,
) error {
	for i := range destinations {
		target := destinations[i]
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

func graphPlanFirstOperation(operations []workOperation, kind plan.OperationKind) (workOperation, bool) {
	for i := range operations {
		if operations[i].Kind == kind {
			return operations[i], true
		}
	}
	return workOperation{}, false
}

type graphPlanDestinationOperation struct {
	ID          string
	Name        string
	Node        pipeline.NodeRef
	Kind        plan.OperationKind
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

// graphPlanUniqueDestinationOperations collects the planned destination
// operations keyed by their stable destination IDs; the display name is
// resolved from the plan's destination table for diagnostics and lowerer
// route matching.
func graphPlanUniqueDestinationOperations(operations []workOperation, planned []workDestination, scope string) ([]graphPlanDestinationOperation, error) {
	destinations := make([]graphPlanDestinationOperation, 0)
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
				ID:   target,
				Name: workDestinationNameByID(planned, target),
				Node: operation.Node,
				Kind: operation.Kind,
			}
			if index, ok := seen[target]; ok {
				if err := validateGraphPlanDuplicateDestinationOperation(scope, target, destinations[index], next); err != nil {
					return nil, err
				}
				continue
			}
			seen[target] = len(destinations)
			destinations = append(destinations, next)
		}
	}
	return destinations, nil
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

type graphPlanFrameStreamLowering struct {
	selectNode   pipeline.NodeRef
	decodeNode   pipeline.NodeRef
	filterNodes  []pipeline.NodeRef
	encodeNode   pipeline.NodeRef
	encodeShape  shape.Spec
	destinations []graphPlanDestinationOperation
}

func (p mediaPlanStreamGraph) prepareFrameOperationSpecLowering(gp graphPlan) (graphPlanFrameStreamLowering, error) {
	operations, branchName, err := graphPlanSingleBranchOperations(gp.work.Operations, "frame stream")
	if err != nil {
		return graphPlanFrameStreamLowering{}, err
	}
	selectOperation, ok := graphPlanFirstOperation(operations, plan.OpSelect)
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
	decodeOperation, hasDecode := graphPlanFirstOperation(operations, plan.OpDecode)
	if p.sourceDomain == shape.DomainFrame && hasDecode {
		return graphPlanFrameStreamLowering{}, graphPlanInvalidError("frame source graph plan has an unexpected decode operation", []string{
			"stream=" + firstNonEmpty(p.stream.Name, string(p.stream.Select.ID), string(p.stream.Select.Type), "stream"),
			"branch=" + branchName,
		})
	}
	if p.sourceDomain != shape.DomainFrame && !hasDecode {
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
	encodeOperation, hasEncode := graphPlanFirstOperation(operations, plan.OpEncode)
	if p.encode != nil && !hasEncode {
		return graphPlanFrameStreamLowering{}, graphPlanInvalidError("encoded frame stream graph plan has no encode operation", []string{
			"stream=" + firstNonEmpty(p.stream.Name, string(p.stream.Select.ID), string(p.stream.Select.Type), "stream"),
			"branch=" + branchName,
		})
	}
	var encodeNode pipeline.NodeRef
	var encodeShape shape.Spec
	if p.encode != nil {
		if encodeOperation.Node == "" {
			return graphPlanFrameStreamLowering{}, graphPlanInvalidError("encoded frame stream graph plan encode operation has no node", []string{
				"stream=" + firstNonEmpty(p.stream.Name, string(p.stream.Select.ID), string(p.stream.Select.Type), "stream"),
				"branch=" + branchName,
			})
		}
		encodeNode = encodeOperation.Node
		encodeShape = encodeOperation.ShapeOut
	}
	if p.encode == nil && hasEncode {
		return graphPlanFrameStreamLowering{}, graphPlanInvalidError("decoded frame stream graph plan has an unexpected encode operation", []string{
			"stream=" + firstNonEmpty(p.stream.Name, string(p.stream.Select.ID), string(p.stream.Select.Type), "stream"),
			"branch=" + branchName,
		})
	}
	destinations, err := p.prepareFrameStreamDestinations(operations, gp.work.Destinations)
	if err != nil {
		return graphPlanFrameStreamLowering{}, err
	}
	return graphPlanFrameStreamLowering{
		selectNode:   selectOperation.Node,
		decodeNode:   decodeOperation.Node,
		filterNodes:  filterNodes,
		encodeNode:   encodeNode,
		encodeShape:  encodeShape,
		destinations: destinations,
	}, nil
}

func (p mediaPlanStreamGraph) validateFrameStreamFilterOperations(operations []workOperation) ([]pipeline.NodeRef, error) {
	planned := graphPlanOperationCount(operations, plan.OpTransform) + graphPlanOperationCount(operations, plan.OpStage)
	if planned != len(p.filters) {
		return nil, graphPlanInvalidError("frame stream graph plan filter operations do not match concrete filters", []string{
			"planned=" + strconv.Itoa(planned),
			"filters=" + strconv.Itoa(len(p.filters)),
		})
	}
	nodes := make([]pipeline.NodeRef, 0, planned)
	for i := range operations {
		operation := operations[i]
		if operation.Kind != plan.OpTransform && operation.Kind != plan.OpStage {
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

func (p mediaPlanStreamGraph) prepareFrameStreamDestinations(operations []workOperation, outputs []workDestination) ([]graphPlanDestinationOperation, error) {
	destinations, err := graphPlanUniqueDestinationOperations(operations, outputs, "frame stream")
	if err != nil {
		return nil, err
	}
	if len(destinations) == 0 {
		return nil, graphPlanInvalidError("frame stream graph plan has no destination operations", nil)
	}
	if len(destinations) != len(p.outputs) {
		return nil, graphPlanInvalidError("frame stream graph plan target count does not match outputs", []string{
			"destinations=" + strconv.Itoa(len(destinations)),
			"outputs=" + strconv.Itoa(len(p.outputs)),
		})
	}
	for i := range destinations {
		target := destinations[i]
		if err := validateGraphPlanDestinationOperationNode("frame stream", target); err != nil {
			return nil, err
		}
		outputIndex, ok := workDestinationIndexByID(outputs, target.ID)
		if !ok || outputIndex < 0 || outputIndex >= len(p.outputs) {
			return nil, graphPlanInvalidError("frame stream destination operation is not bound to an output", []string{
				"destination=" + target.Name,
				"node=" + target.Node.String(),
			})
		}
		target.OutputIndex = outputIndex
		destinations[i] = target
		output := p.outputs[outputIndex]
		if output.sink != nil {
			if target.Kind != plan.OpSink {
				return nil, graphPlanInvalidError("frame stream destination operation kind does not match sink destination", []string{
					"destination=" + target.Name,
					"kind=" + string(target.Kind),
				})
			}
			continue
		}
		if p.encode == nil {
			return nil, graphPlanInvalidError("decoded frame stream cannot lower to a byte destination without encode", []string{
				"destination=" + target.Name,
			})
		}
		if target.Kind != plan.OpMux && target.Kind != plan.OpWrite {
			return nil, graphPlanInvalidError("frame stream destination operation kind does not match byte destination", []string{
				"destination=" + target.Name,
				"kind=" + string(target.Kind),
			})
		}
	}
	return destinations, nil
}

func graphPlanOperationCount(operations []workOperation, kind plan.OperationKind) int {
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
		return pipeline.Spec{}, recipeGraphUnsupportedError("describe job", intent{Streams: []streamIntent{p.stream}})
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
	operations, err := mediaPlanFilterRouteOperations(p.filters)
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
		name:              branchName,
		privateOperations: operations,
		branch: branchComposeBranch{
			Name:         branchName,
			Selector:     p.decode.selector,
			Input:        p.stream.Select.Input,
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
		return pipeline.Spec{}, nil, nil, recipeGraphUnsupportedError("describe job", intent{Streams: []streamIntent{p.stream}})
	}
	return spec, sourceRefs, nodes, nil
}

func mediaPlanFilterRouteOperations(filters []filterRequest) ([]operationSpec, error) {
	operations := make([]operationSpec, 0, len(filters))
	for i := range filters {
		filter := filters[i]
		switch {
		case filter.stage != nil:
			operations = append(operations, operationSpecForStage(filter.stage))
		case filter.transform != nil:
			operation := operationSpecForTransform(transformSpecFromMediaTransform(*filter.transform))
			// Keep the (possibly solver-selected) adapter as the component, so
			// the route operations name and instantiate the same filter.
			operation.Component = firstNonEmpty(filter.transform.factory, operation.Component)
			operations = append(operations, operation)
		default:
			return nil, ErrNilStage
		}
	}
	return operations, nil
}

// applyTransformComponentOverride re-points a lowered transform at the
// solver-selected adapter when the operation's component differs from the
// standard factory: the component is both the node-name prefix and the filter
// registry key, so the planned spec and the built graph stay identical.
func applyTransformComponentOverride(transform mediaTransform, operation operationSpec) mediaTransform {
	factory := operation.Component
	if operation.Kind != plan.OpTransform || factory == "" || factory == transform.factory {
		return transform
	}
	transform.name = factory + strings.TrimPrefix(transform.name, transform.factory)
	transform.factory = factory
	return transform
}

func transformSpecFromMediaTransform(transform mediaTransform) TransformSpec {
	var spec TransformSpec
	if transform.video != nil {
		resize := *transform.video
		spec.Resize = &resize
	}
	if transform.audio != nil {
		resample := *transform.audio
		spec.Resample = &resample
	}
	return spec
}

func (p mediaPlanStreamGraph) compileFrameStreamBranchCompose(ctx context.Context, graph pipeline.Graph, service *builder, lowering graphPlanFrameStreamLowering) error {
	sources, err := compileMediaPlanSources(ctx, p.runtime, graph, p.inputs, "build job", intent{Streams: []streamIntent{p.stream}})
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
	for i := range lowering.destinations {
		target := lowering.destinations[i]
		if target.OutputIndex < 0 || target.OutputIndex >= len(outputs) {
			return graphPlanInvalidError("frame stream destination operation is not bound to a branch route destination", []string{
				"destination=" + target.Name,
				"node=" + target.Node.String(),
			})
		}
		outputs[target.OutputIndex].node = target.Node
	}
	groups, err := resolveBranchComposeStreamGroupsForInputs(sources, p.inputs, branches)
	if err != nil {
		return err
	}
	inputPlan := map[string]graphPlanBranchComposeInputOperation{
		branchComposeSelectorGroupKey(p.decode.selector, p.stream.Select.Input): {
			selectNode: lowering.selectNode,
			decodeNode: lowering.decodeNode,
		},
	}
	branchPlan := map[string]graphPlanBranchComposeBranchOperation{
		branches[0].name: {
			privateStageNodes: append([]pipeline.NodeRef(nil), lowering.filterNodes...),
			encodeNode:        lowering.encodeNode,
			encodeShape:       lowering.encodeShape,
		},
	}
	branchInputs, branchStreams, err := compileBranchComposeInputs(ctx, p.runtime, graph, sources.refs, groups, sources.bounds, branches, inputPlan, sources.realtime)
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
	intent intent,
) (mediaPlanCompiledSources, error) {
	if runtime == nil {
		return mediaPlanCompiledSources{}, recipeGraphUnsupportedError(operation, intent)
	}
	if !mediaPlanStreamInputsSupported(inputs) {
		return mediaPlanCompiledSources{}, recipeGraphUnsupportedError(operation, intent)
	}
	service := &builder{runtime: runtime}
	sourceRefs := make([]pipeline.NodeRef, 0, len(inputs))
	streams := make([]av.Stream, 0, len(inputs))
	streamGroups := make([][]av.Stream, 0, len(inputs))
	var bounds []decodeBoundsProvider
	realtime := runtime.realtime
	names := graphSourceNodeNames(inputs)
	for i := range inputs {
		build, err := inputs[i].openGraphSourceBuild(ctx, service, names[i])
		if err != nil {
			return mediaPlanCompiledSources{}, err
		}
		sourceRef, err := graph.AddSource(build.source, runtime.buffer)
		if err != nil {
			build.source.Close()
			return mediaPlanCompiledSources{}, err
		}
		sourceRefs = append(sourceRefs, sourceRef)
		sourceStreams := append([]av.Stream(nil), build.streams...)
		streams = append(streams, sourceStreams...)
		streamGroups = append(streamGroups, sourceStreams)
		realtime = realtime || build.realtime
		if build.bounds != nil {
			bounds = append(bounds, build.bounds)
		}
	}
	return mediaPlanCompiledSources{
		refs:         sourceRefs,
		streams:      streams,
		streamGroups: streamGroups,
		bounds:       bounds,
		realtime:     realtime,
	}, nil
}

func mediaPlanStreamFilters(stream streamIntent) ([]filterRequest, error) {
	selector := streamIntentSelector(stream)
	if len(stream.Operations) == 0 {
		return mediaPlanStreamTransformFilters(stream, selector)
	}
	filters := make([]filterRequest, 0, len(stream.Operations))
	frameStepIndex := 0
	for i := range stream.Operations {
		operation := stream.Operations[i]
		switch operation.Kind {
		case plan.OpStage:
			if operation.Stage == nil {
				return nil, streamStageMissingError(stream)
			}
			filters = append(filters, filterRequest{selector: selector, stage: operation.Stage})
			frameStepIndex++
		case plan.OpTransform:
			transform, err := streamTransform(stream.Name, selector, operation.Transform, frameStepIndex)
			if err != nil {
				return nil, err
			}
			transform = applyTransformComponentOverride(transform, operation)
			filters = append(filters, filterRequest{selector: selector, transform: &transform})
			frameStepIndex++
		case plan.OpTap:
			if operation.Tap.After == "" {
				frameStepIndex++
			}
		}
	}
	return filters, nil
}

func mediaPlanStreamTransformFilters(stream streamIntent, selector av.StreamSelector) ([]filterRequest, error) {
	transforms := streamIntentTransformSpecs(stream)
	filters := make([]filterRequest, 0, len(transforms))
	for i := range transforms {
		transform, err := streamTransform(stream.Name, selector, transforms[i], i)
		if err != nil {
			return nil, err
		}
		filters = append(filters, filterRequest{selector: selector, transform: &transform})
	}
	return filters, nil
}
