package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
)

func (r recipeResolved) buildMediaPlanPacketCopyTask(ctx context.Context) (Task, error) {
	stream, selected, ok := r.packetCopyStream()
	if !ok {
		return nil, recipeGraphUnsupportedError("build job", r.intent)
	}
	plan, ok, err := newMediaPlanPacketCopyGraph(r.runtime, r.inputAttachments, r.outputAttachments, stream, selected)
	if err != nil || !ok {
		if err != nil {
			return nil, err
		}
		return nil, recipeGraphUnsupportedError("build job", r.intent)
	}
	graph, err := plan.newGraph(ctx)
	if err != nil {
		return nil, err
	}
	if err := plan.compile(ctx, graph); err != nil {
		graph.Close()
		return nil, err
	}
	return newTask(graph, plan.runtime), nil
}

func (r recipeResolved) buildMediaPlanSinkEndpointTask(ctx context.Context) (Task, error) {
	stream, ok := r.singleStreamIntent()
	if !ok || !mediaPlanSinkEndpointShape(stream, r.outputAttachments) {
		return nil, recipeGraphUnsupportedError("build job", r.intent)
	}
	plan, ok, err := newMediaPlanSingleStreamGraph(r.runtime, r.inputAttachments, r.outputAttachments, stream)
	if err != nil || !ok {
		if err != nil {
			return nil, err
		}
		return nil, recipeGraphUnsupportedError("build job", r.intent)
	}
	graph, err := plan.newGraph(ctx)
	if err != nil {
		return nil, err
	}
	if err := plan.compileSinkEndpoint(ctx, graph); err != nil {
		graph.Close()
		return nil, err
	}
	return newTask(graph, plan.runtime), nil
}

func (r recipeResolved) buildMediaPlanEncodeTask(ctx context.Context) (Task, error) {
	stream, ok := r.singleStreamIntent()
	if !ok || !mediaPlanEncodeShape(stream, r.outputAttachments) {
		return nil, recipeGraphUnsupportedError("build job", r.intent)
	}
	plan, ok, err := newMediaPlanSingleStreamGraph(r.runtime, r.inputAttachments, r.outputAttachments, stream)
	if err != nil || !ok {
		if err != nil {
			return nil, err
		}
		return nil, recipeGraphUnsupportedError("build job", r.intent)
	}
	graph, err := plan.newGraph(ctx)
	if err != nil {
		return nil, err
	}
	if err := plan.compileEncodeOutput(ctx, graph); err != nil {
		graph.Close()
		return nil, err
	}
	return newTask(graph, plan.runtime), nil
}

func (r recipeResolved) buildMediaPlanBranchComposerTask(ctx context.Context) (Task, error) {
	plan, ok, err := newMediaPlanBranchComposeGraph(r.runtime, r.branchInputAttachment, r.plan)
	if err != nil || !ok {
		if err != nil {
			return nil, err
		}
		return nil, recipeGraphUnsupportedError("build branch composition", r.intent)
	}
	graph, err := plan.newGraph(ctx)
	if err != nil {
		return nil, err
	}
	if err := plan.compile(ctx, graph); err != nil {
		graph.Close()
		return nil, err
	}
	return newTask(graph, plan.runtime), nil
}

func (r recipeResolved) singleStreamIntent() (StreamIntent, bool) {
	if len(r.intent.Streams) != 1 {
		return StreamIntent{}, false
	}
	return r.intent.Streams[0], true
}

type mediaPlanSingleStreamGraph struct {
	runtime *runtime
	inputs  []InputSpec
	outputs []EndpointSpec
	stream  StreamIntent
	decode  decodeRequest
	filters []filterRequest
	encode  *encodeRequest
}

type mediaPlanPacketCopyGraph struct {
	runtime        *runtime
	inputs         []InputSpec
	outputs        []EndpointSpec
	stream         StreamIntent
	selectedStream bool
}

type mediaPlanBranchComposeGraph struct {
	runtime  *runtime
	input    InputSpec
	plan     branchComposePlan
	branches []branchComposeRoute
	targets  []branchComposeTargetRoute
}

func newMediaPlanSingleStreamGraph(rt Runtime, inputs []InputSpec, outputs []EndpointSpec, stream StreamIntent) (mediaPlanSingleStreamGraph, bool, error) {
	runtime, ok := rt.(*runtime)
	if !ok || runtime == nil {
		return mediaPlanSingleStreamGraph{}, false, nil
	}
	if !mediaPlanStreamInputsSupported(inputs) {
		return mediaPlanSingleStreamGraph{}, false, nil
	}
	selector := streamIntentSelector(stream)
	plan := mediaPlanSingleStreamGraph{
		runtime: runtime,
		inputs:  append([]InputSpec(nil), inputs...),
		outputs: append([]EndpointSpec(nil), outputs...),
		stream:  stream,
		decode: decodeRequest{
			selector:    selector,
			codecChange: stream.CodecChange,
		},
	}
	filters, err := mediaPlanStreamFilters(stream)
	if err != nil {
		return mediaPlanSingleStreamGraph{}, false, err
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

func newMediaPlanPacketCopyGraph(rt Runtime, inputs []InputSpec, outputs []EndpointSpec, stream StreamIntent, selectedStream bool) (mediaPlanPacketCopyGraph, bool, error) {
	runtime, ok := rt.(*runtime)
	if !ok || runtime == nil {
		return mediaPlanPacketCopyGraph{}, false, nil
	}
	if len(inputs) == 0 || len(outputs) == 0 || !mediaPlanStreamInputsSupported(inputs) {
		return mediaPlanPacketCopyGraph{}, false, nil
	}
	return mediaPlanPacketCopyGraph{
		runtime:        runtime,
		inputs:         append([]InputSpec(nil), inputs...),
		outputs:        append([]EndpointSpec(nil), outputs...),
		stream:         stream,
		selectedStream: selectedStream,
	}, true, nil
}

func (p mediaPlanPacketCopyGraph) newGraph(ctx context.Context) (pipeline.Graph, error) {
	return (&builder{runtime: p.runtime}).newGraph(ctx)
}

func (p mediaPlanPacketCopyGraph) spec() (pipeline.Spec, error) {
	spec := pipeline.Spec{Name: "goav", Realtime: p.runtime.realtime}
	nodes := make(map[string]plannedNode, len(p.inputs)+len(p.outputs)+1)
	sourceRefs, ok, err := mediaPlanPacketCopySources(&spec, nodes, p.inputs)
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

func (p mediaPlanBranchComposeGraph) newGraph(ctx context.Context) (pipeline.Graph, error) {
	return (&builder{runtime: p.runtime}).newGraph(ctx)
}

func (p mediaPlanBranchComposeGraph) spec() (pipeline.Spec, error) {
	spec := pipeline.Spec{Name: "goav", Realtime: p.runtime.realtime}
	nodes := make(map[string]plannedNode, p.nodeCapacity())
	sourceRefs, ok, err := p.specSources(&spec, nodes)
	if err != nil {
		return pipeline.Spec{}, err
	}
	if !ok {
		return pipeline.Spec{}, recipeGraphUnsupportedError("describe branch composition", Intent{Name: p.plan.Name})
	}
	return planBranchComposeRoutes(spec, nodes, sourceRefs, p.branches, p.targets)
}

func (p mediaPlanBranchComposeGraph) nodeCapacity() int {
	return 1 + 3 + len(p.branches) + branchComposeStepCount(p.branches) + len(p.targets)
}

func (p mediaPlanBranchComposeGraph) specSources(spec *pipeline.Spec, nodes map[string]plannedNode) ([]pipeline.NodeRef, bool, error) {
	if p.input.rtp == nil {
		input := p.input.formatInput()
		name := demuxNodeName(input)
		ref := pipeline.NodeRef(name)
		if err := addPlannedNode(nodes, spec, name, pipeline.NodeSource, ref, inputNodeDetail(input)); err != nil {
			return nil, false, err
		}
		return []pipeline.NodeRef{ref}, true, nil
	}
	input := p.input.rtpBuildInput()
	name := rtpNodeName(input, 0)
	ref := pipeline.NodeRef(name)
	if err := addPlannedNode(nodes, spec, name, pipeline.NodeSource, ref, rtpInputDetail(input)); err != nil {
		return nil, false, err
	}
	return []pipeline.NodeRef{ref}, true, nil
}

func (p mediaPlanBranchComposeGraph) compile(ctx context.Context, graph pipeline.Graph) error {
	sourceRefs, streams, builds, realtime, err := p.compileSources(ctx, graph)
	if err != nil {
		return err
	}
	groups, err := resolveBranchComposeStreamGroups(streams, p.branches)
	if err != nil {
		return err
	}
	branchInputs, branchStreams, err := compileBranchComposeInputs(ctx, p.runtime, graph, sourceRefs, groups, builds, p.branches, realtime)
	if err != nil {
		return err
	}
	return compileBranchComposeRoutes(ctx, p.runtime, graph, p.branches, p.targets, branchInputs, branchStreams, realtime)
}

func (p mediaPlanBranchComposeGraph) compileSources(ctx context.Context, graph pipeline.Graph) ([]pipeline.NodeRef, []av.Stream, []rtpBuild, bool, error) {
	service := &builder{runtime: p.runtime}
	if p.input.rtp == nil {
		input := p.input.formatInput()
		demux, err := service.openDemuxSource(ctx, input)
		if err != nil {
			return nil, nil, nil, false, err
		}
		sourceRef, err := graph.AddSource(demux.source, p.runtime.buffer)
		if err != nil {
			demux.source.Close()
			return nil, nil, nil, false, err
		}
		return []pipeline.NodeRef{sourceRef}, demux.streams, nil, p.runtime.realtime || input.Realtime, nil
	}
	receiver, err := service.openRTPSource(ctx, p.input.rtpBuildInput(), 0)
	if err != nil {
		return nil, nil, nil, false, err
	}
	sourceRef, err := graph.AddSource(receiver.source, p.runtime.buffer)
	if err != nil {
		receiver.source.Close()
		return nil, nil, nil, false, err
	}
	return []pipeline.NodeRef{sourceRef}, receiver.streams, []rtpBuild{receiver}, true, nil
}

func (p mediaPlanPacketCopyGraph) compile(ctx context.Context, graph pipeline.Graph) error {
	sourceRefs, streams, err := p.compileSources(ctx, graph)
	if err != nil {
		return err
	}
	return p.compileTargets(ctx, graph, sourceRefs, streams)
}

func (p mediaPlanPacketCopyGraph) compileSources(ctx context.Context, graph pipeline.Graph) ([]pipeline.NodeRef, []av.Stream, error) {
	service := &builder{runtime: p.runtime}
	if len(p.inputs) == 1 && p.inputs[0].rtp == nil {
		demux, err := service.openDemuxSource(ctx, p.inputs[0].formatInput())
		if err != nil {
			return nil, nil, err
		}
		sourceRef, err := graph.AddSource(demux.source, p.runtime.buffer)
		if err != nil {
			demux.source.Close()
			return nil, nil, err
		}
		return []pipeline.NodeRef{sourceRef}, demux.streams, nil
	}
	if !allRTPInputSpecs(p.inputs) {
		return nil, nil, recipeGraphUnsupportedError("build job", Intent{Streams: []StreamIntent{p.stream}})
	}
	sourceRefs := make([]pipeline.NodeRef, 0, len(p.inputs))
	streams := make([]av.Stream, 0, len(p.inputs))
	for i := range p.inputs {
		receiver, err := service.openRTPSource(ctx, p.inputs[i].rtpBuildInput(), i)
		if err != nil {
			return nil, nil, err
		}
		sourceRef, err := graph.AddSource(receiver.source, p.runtime.buffer)
		if err != nil {
			receiver.source.Close()
			return nil, nil, err
		}
		sourceRefs = append(sourceRefs, sourceRef)
		streams = append(streams, receiver.streams...)
	}
	return sourceRefs, streams, nil
}

func (p mediaPlanPacketCopyGraph) compileTargets(
	ctx context.Context,
	graph pipeline.Graph,
	sourceRefs []pipeline.NodeRef,
	streams []av.Stream,
) error {
	service := &builder{runtime: p.runtime}
	targetRefs := sourceRefs
	targetStreams := streams
	if p.selectedStream {
		selector := streamIntentSelector(p.stream)
		selected, err := selectDecodeStream(streams, selector)
		if err != nil {
			return err
		}
		selectStage := newStreamSelectStage(selectNodeName(selector), selected, selector, selectNodeDetail(selector))
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
	for i := range p.outputs {
		output := p.outputs[i]
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
		stage, err := service.openMuxStageWithFormat(ctx, output.output, i, targetStreams, endpointSpecOpenFormat(output), endpointSpecGraphFormat(output))
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

func (p mediaPlanSingleStreamGraph) newGraph(ctx context.Context) (pipeline.Graph, error) {
	return (&builder{runtime: p.runtime}).newGraph(ctx)
}

func (p mediaPlanSingleStreamGraph) sinkEndpointSpec() (pipeline.Spec, error) {
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

func (p mediaPlanSingleStreamGraph) encodeOutputSpec() (pipeline.Spec, error) {
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
	if err := planEncodeEndpointPath(nodes, &spec, previous, *p.encode, p.outputs); err != nil {
		return pipeline.Spec{}, err
	}
	return spec, nil
}

func (p mediaPlanSingleStreamGraph) specWithSources() (pipeline.Spec, []pipeline.NodeRef, map[string]plannedNode, error) {
	spec := pipeline.Spec{Name: "goav", Realtime: p.runtime.realtime}
	nodes := make(map[string]plannedNode, len(p.inputs)+len(p.outputs)+len(p.filters)+3)
	sourceRefs, ok, err := mediaPlanPacketCopySources(&spec, nodes, p.inputs)
	if err != nil {
		return pipeline.Spec{}, nil, nil, err
	}
	if !ok {
		return pipeline.Spec{}, nil, nil, recipeGraphUnsupportedError("describe job", Intent{Streams: []StreamIntent{p.stream}})
	}
	return spec, sourceRefs, nodes, nil
}

func (p mediaPlanSingleStreamGraph) compileSinkEndpoint(ctx context.Context, graph pipeline.Graph) error {
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
		return compileEncodeSinkPath(ctx, p.runtime, graph, previousRef, *p.encode, encodeConfig, p.outputs[0].sink)
	}
	sinkRef, err := graph.AddSink(p.outputs[0].sink, p.runtime.buffer)
	if err != nil {
		return err
	}
	return connectRefs(graph, previousRef, sinkRef)
}

func (p mediaPlanSingleStreamGraph) compileEncodeOutput(ctx context.Context, graph pipeline.Graph) error {
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
	return compileEncodeEndpointPath(ctx, p.runtime, graph, previousRef, *p.encode, encodeConfig, encodedStream, p.outputs)
}

func (p mediaPlanSingleStreamGraph) compileSources(ctx context.Context, graph pipeline.Graph) ([]pipeline.NodeRef, []av.Stream, []rtpBuild, bool, error) {
	if len(p.inputs) == 1 && p.inputs[0].rtp == nil {
		demux, err := (&builder{runtime: p.runtime}).openDemuxSource(ctx, p.inputs[0].formatInput())
		if err != nil {
			return nil, nil, nil, false, err
		}
		sourceRef, err := graph.AddSource(demux.source, p.runtime.buffer)
		if err != nil {
			demux.source.Close()
			return nil, nil, nil, false, err
		}
		return []pipeline.NodeRef{sourceRef}, demux.streams, nil, p.runtime.realtime || p.inputs[0].formatInput().Realtime, nil
	}
	if !allRTPInputSpecs(p.inputs) {
		return nil, nil, nil, false, recipeGraphUnsupportedError("build job", Intent{Streams: []StreamIntent{p.stream}})
	}
	sourceRefs := make([]pipeline.NodeRef, 0, len(p.inputs))
	streams := make([]av.Stream, 0, len(p.inputs))
	builds := make([]rtpBuild, 0, len(p.inputs))
	service := &builder{runtime: p.runtime}
	for i := range p.inputs {
		receiver, err := service.openRTPSource(ctx, p.inputs[i].rtpBuildInput(), i)
		if err != nil {
			return nil, nil, nil, false, err
		}
		sourceRef, err := graph.AddSource(receiver.source, p.runtime.buffer)
		if err != nil {
			receiver.source.Close()
			return nil, nil, nil, false, err
		}
		sourceRefs = append(sourceRefs, sourceRef)
		streams = append(streams, receiver.streams...)
		builds = append(builds, receiver)
	}
	return sourceRefs, streams, builds, p.runtime.realtime, nil
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
	return mediaPlanPacketCopyIntentStream(true, r.intent, r.streamAttachments)
}
