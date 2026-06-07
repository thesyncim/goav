package goav

import (
	"context"

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

func buildMediaPlanTask(ctx context.Context, plan mediaPlanExecutable) (Task, error) {
	runtime := plan.runtimeRef()
	if runtime == nil {
		return nil, recipeGraphUnsupportedError("build recipe", Intent{})
	}
	graph, err := (&builder{runtime: runtime}).newGraph(ctx)
	if err != nil {
		return nil, err
	}
	if err := plan.compile(ctx, graph); err != nil {
		graph.Close()
		return nil, err
	}
	return newTask(graph, runtime), nil
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

func (p mediaPlanStreamGraph) compile(ctx context.Context, graph pipeline.Graph) error {
	if p.copyPackets {
		return p.compilePacketCopy(ctx, graph)
	}
	if p.hasSingleSinkDestination() {
		return p.compileSinkDestination(ctx, graph)
	}
	return p.compileEncodeOutput(ctx, graph)
}

func (p mediaPlanStreamGraph) hasSingleSinkDestination() bool {
	return len(p.outputs) == 1 && p.outputs[0].sink != nil
}

func (p mediaPlanBranchComposeGraph) runtimeRef() *runtime {
	return p.runtime
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

func (p mediaPlanBranchComposeGraph) compile(ctx context.Context, graph pipeline.Graph) error {
	sources, err := compileMediaPlanSources(ctx, p.runtime, graph, []InputSpec{p.input}, "build branch composition", Intent{Name: p.plan.Name})
	if err != nil {
		return err
	}
	groups, err := resolveBranchComposeStreamGroups(sources.streams, p.branches)
	if err != nil {
		return err
	}
	branchInputs, branchStreams, err := compileBranchComposeInputs(ctx, p.runtime, graph, sources.refs, groups, sources.rtpBuilds, p.branches, sources.realtime)
	if err != nil {
		return err
	}
	return compileBranchComposeRoutes(ctx, p.runtime, graph, p.branches, p.targets, branchInputs, branchStreams, sources.realtime)
}

func (p mediaPlanStreamGraph) compilePacketCopy(ctx context.Context, graph pipeline.Graph) error {
	sourceRefs, streams, _, _, err := p.compileSources(ctx, graph)
	if err != nil {
		return err
	}
	return p.compilePacketCopyTargets(ctx, graph, sourceRefs, streams)
}

func (p mediaPlanStreamGraph) compilePacketCopyTargets(
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
		stage, err := service.openMuxStageWithFormat(ctx, output.output, i, targetStreams, destinationOpenFormat(output), destinationGraphFormat(output))
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

func (p mediaPlanStreamGraph) compileSinkDestination(ctx context.Context, graph pipeline.Graph) error {
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

func (p mediaPlanStreamGraph) compileEncodeOutput(ctx context.Context, graph pipeline.Graph) error {
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
	return compileEncodeDestinationPath(ctx, p.runtime, graph, previousRef, *p.encode, encodeConfig, encodedStream, p.outputs)
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
