package goav

import (
	"context"
	"strconv"

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

// streamRoutes holds the facts of a single-stream recipe a composeLowerer is
// built from: its frame-stream route (select, decode, filters, encoder) or
// its selected packet-copy route.
type streamRoutes struct {
	runtime      *runtime
	inputs       []InputSpec
	outputs      []destinationAttachment
	stream       streamIntent
	sourceDomain shape.MediaDomain
	decode       decodeRequest
	filters      []filterRequest
	encode       *encodeRequest
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

func buildGraphPlanTask(ctx context.Context, gp graphPlan) (LiveTask, error) {
	runtime := runtimeForGraphExecution(gp.runtime)
	// One shared timeline per task: the runtime clone carries it as the clock
	// every lowering step threads to paced sources, so a task-wide rate change
	// re-anchors all of them together.
	timeline := newTimeline(runtime.clock)
	runtime = runtimeWithClock(runtime, timeline)
	// The QoS reporter rides the same clone: lowering binds it into playout
	// and sync gates next to the timeline, and the finished task attaches
	// itself below so gate reports reach its Watch surface.
	runtime.qos = newQoSReporter()
	gp = graphPlanWithRuntime(gp, runtime)
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
	task := newTaskWithRootDestinations(graph, runtime, gp.work.Destinations, service.destinationTxs...)
	task.timeline = timeline
	runtime.qos.attach(task)
	task.startQoSPolicy(runtime.qosPolicy)
	return task, nil
}

func runtimeForGraphExecution(rt *runtime) *runtime {
	if rt != nil {
		return rt
	}
	return mustNew()
}

func runtimeRealtime(rt *runtime) bool {
	if rt == nil {
		return true
	}
	return rt.realtime
}

// runtimeWithRealtime is a view of rt with realtime pacing set, sharing its
// registries and pools.
func runtimeWithRealtime(rt *runtime, realtime bool) *runtime {
	if rt == nil {
		return nil
	}
	clone := *rt
	clone.realtime = realtime
	return &clone
}

func runtimeWithBuffer(rt *runtime, buffer pipeline.BufferPolicy) *runtime {
	if rt == nil {
		return nil
	}
	clone := *rt
	clone.buffer = buffer
	return &clone
}

// runtimeWithClock clones the runtime with the task's timeline as its clock,
// so every builder a lowering step creates hands paced sources the shared
// timeline instead of the raw runtime clock.
func runtimeWithClock(rt *runtime, clock av.Clock) *runtime {
	if rt == nil {
		return nil
	}
	clone := *rt
	clone.clock = clock
	return &clone
}

func graphPlanWithRuntime(gp graphPlan, rt *runtime) graphPlan {
	gp.runtime = rt
	switch lowerer := gp.lowerer.(type) {
	case composeLowerer:
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

func (p streamRoutes) hasSingleSinkDestination() bool {
	return len(p.outputs) == 1 && p.outputs[0].details.sink
}

func newMediaPlanDecodeStreamGraph(rt *Runtime, inputs []InputSpec, outputs []destinationSpec, stream streamIntent) (composeLowerer, bool, error) {
	if !mediaPlanStreamInputsSupported(inputs) || operationsHaveStepAfterEncode(stream.Operations) {
		return composeLowerer{}, false, nil
	}
	selector := streamIntentSelector(stream)
	sg := streamRoutes{
		runtime:      rt,
		inputs:       append([]InputSpec(nil), inputs...),
		outputs:      destinationAttachmentsFromSpecs(outputs),
		stream:       stream,
		sourceDomain: mediaPlanStreamInputDomain(inputs, stream),
		decode: decodeRequest{
			selector: selector,
			config:   cloneCodecSpec(chainDecodeCodec(stream.Operations)),
		},
	}
	filters, err := mediaPlanStreamFilters(stream)
	if err != nil {
		return composeLowerer{}, false, err
	}
	sg.filters = filters
	if encode := chainEncodeSpec(stream.Operations); codecIntentSet(encode) && !encode.Copy {
		request := encodeRequest{
			selector: selector,
			config:   encodeConfigFromSpec(encode),
		}
		sg.encode = &request
	}
	branches, routes, err := sg.frameStreamBranchComposeRoutes()
	if err != nil {
		return composeLowerer{}, false, err
	}
	return composeLowerer{
		runtime:     rt,
		inputs:      sg.inputs,
		describe:    "describe job",
		build:       "build job",
		context:     intent{Streams: []streamIntent{stream}},
		branches:    branches,
		outputs:     routes,
		unsupported: !sg.hasSingleSinkDestination() && sg.encode == nil,
	}, true, nil
}

func mediaPlanInputDomain(inputs []InputSpec) shape.MediaDomain {
	if len(inputs) == 1 {
		if shape, ok := declaredSourceShape(inputs[0]); ok && shape.Domain != "" {
			return shape.Domain
		}
	}
	return shape.DomainPacket
}

// mediaPlanStreamInputDomain resolves the media domain feeding one stream
// chain: the single input's domain, or with several inputs, the domain of the
// input the chain's selector binds to.
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

func newMediaPlanPacketCopyStreamGraph(rt *Runtime, inputs []InputSpec, outputs []destinationSpec, stream streamIntent, selectedStream bool) (composeLowerer, bool, error) {
	if len(inputs) == 0 || len(outputs) == 0 || !mediaPlanStreamInputsSupported(inputs) {
		return composeLowerer{}, false, nil
	}
	sg := streamRoutes{
		inputs:  append([]InputSpec(nil), inputs...),
		outputs: destinationAttachmentsFromSpecs(outputs),
		stream:  stream,
	}
	l := composeLowerer{
		runtime:  rt,
		inputs:   sg.inputs,
		describe: "describe job",
		build:    "build job",
		context:  intent{Streams: []streamIntent{stream}},
	}
	if !selectedStream {
		l.remux = sg.outputs
		return l, true, nil
	}
	l.build = "build packet copy"
	l.branches, l.outputs = sg.selectedPacketCopyBranchComposeRoutes()
	return l, true, nil
}

func mediaPlanInputsContainDomain(inputs []InputSpec, domain shape.MediaDomain) bool {
	for i := range inputs {
		if shape, ok := declaredSourceShape(inputs[i]); ok && shape.Domain == domain {
			return true
		}
	}
	return false
}

func (p streamRoutes) selectedPacketCopyBranchComposeRoutes() ([]branchComposeRoute, []branchComposeTargetRoute) {
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

func newMediaPlanBranchComposeGraph(rt *Runtime, inputs []InputSpec, composePlan branchComposePlan) (composeLowerer, bool, error) {
	if len(inputs) == 0 {
		return composeLowerer{}, false, nil
	}
	for i := range inputs {
		input := inputs[i]
		if input.provider == nil && input.source == nil && input.formatInput().Reader == nil && input.formatInput().URI == "" && input.formatInput().Name == "" {
			return composeLowerer{}, false, nil
		}
	}
	branches, destinations, err := prepareBranchComposePlan(composePlan)
	if err != nil {
		return composeLowerer{}, false, err
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
	return composeLowerer{
		runtime:  rt,
		inputs:   append([]InputSpec(nil), inputs...),
		describe: "describe branch composition",
		build:    "build branch composition",
		context:  intent{Name: composePlan.Name},
		branches: branches,
		outputs:  destinations,
	}, true, nil
}

type graphPlanBranchComposeBranchOperation struct {
	// buffer is the branch's declared queue lowered to a node policy, its copy
	// bounds sized from the branch's shapes; direct when none was declared.
	buffer pipeline.BufferPolicy
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

type graphPlanDestinationOperation struct {
	ID          string
	Name        string
	Node        pipeline.NodeRef
	Kind        plan.OperationKind
	OutputIndex int
	Matches     []int
}

func (p streamRoutes) frameStreamBranchComposeRoutes() ([]branchComposeRoute, []branchComposeTargetRoute, error) {
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
		},
		decode:           cloneCodecSpec(p.decode.config),
		dropDecodeEvents: p.encode == nil,
		sourceDomain:     p.sourceDomain,
		request:          request,
	}}
	return branches, mediaPlanBranchComposeTargetRoutes(p.outputs, branchName), nil
}

func mediaPlanBranchComposeTargetRoutes(outputs []destinationAttachment, branchName string) []branchComposeTargetRoute {
	routes := make([]branchComposeTargetRoute, len(outputs))
	for i := range outputs {
		output := outputs[i]
		facts := output.details
		target := branchComposeTarget{
			Name:        firstNonEmpty(facts.label, facts.routingName, "output"),
			Destination: destinationAttachmentDestination(output),
			Target:      destinationAttachmentOutput(output),
			Sink:        destinationAttachmentSink(output),
			Format:      facts.requestedContainer,
			Branches:    []string{branchName},
		}
		if facts.resolvedContainer != "" {
			target = resolveBranchComposeTargetFormat(target, facts.resolvedContainer)
		}
		routes[i] = branchComposeTargetRoute{
			output:  target,
			target:  branchComposeFormatTarget(branchComposePlan{}, target),
			sink:    destinationAttachmentSink(output),
			matches: []int{0},
		}
	}
	return routes
}

func transformSpecFromMediaTransform(transform mediaTransform) transformSpec {
	var spec transformSpec
	if transform.video != nil {
		resize := *transform.video
		spec.resize = &resize
	}
	if transform.audio != nil {
		resample := *transform.audio
		spec.resample = &resample
	}
	return spec
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
			if err := validateStageComponent(operation.Stage); err != nil {
				return nil, streamStageMissingError(stream, err)
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
