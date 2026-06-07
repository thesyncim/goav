package goav

import (
	"context"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
)

type decodeEncodeToOutputGraphCompiler struct{}
type rtpDecodeEncodeToOutputGraphCompiler struct{}

func (decodeEncodeToOutputGraphCompiler) match(b *builder) bool {
	return len(b.inputs) == 1 &&
		len(b.decodes) == 1 &&
		len(b.encodes) == 1 &&
		len(b.outputs) > 0 &&
		len(b.sinks) == 0 &&
		len(b.transcodes) == 0 &&
		len(b.sources) == 0 &&
		len(b.stages) == 0 &&
		len(b.rtpInputs) == 0
}

func (decodeEncodeToOutputGraphCompiler) describe(b *builder, spec pipeline.Spec) (pipeline.Spec, error) {
	return b.planDecodeEncodeToOutput(spec)
}

func (decodeEncodeToOutputGraphCompiler) build(ctx context.Context, b *builder) (Task, error) {
	return b.buildDecodeEncodeToOutput(ctx)
}

func (rtpDecodeEncodeToOutputGraphCompiler) match(b *builder) bool {
	return len(b.rtpInputs) > 0 &&
		len(b.decodes) == 1 &&
		len(b.encodes) == 1 &&
		len(b.outputs) > 0 &&
		len(b.inputs) == 0 &&
		len(b.sinks) == 0 &&
		len(b.transcodes) == 0 &&
		len(b.sources) == 0 &&
		len(b.stages) == 0
}

func (rtpDecodeEncodeToOutputGraphCompiler) describe(b *builder, spec pipeline.Spec) (pipeline.Spec, error) {
	return b.planRTPDecodeEncodeToOutput(spec)
}

func (rtpDecodeEncodeToOutputGraphCompiler) build(ctx context.Context, b *builder) (Task, error) {
	return b.buildRTPDecodeEncodeToOutput(ctx)
}

func (b *builder) planDecodeEncodeToOutput(spec pipeline.Spec) (pipeline.Spec, error) {
	nodes := make(map[string]plannedNode, 4+len(b.filters)+len(b.outputs))
	sourceName := demuxNodeName(b.inputs[0])
	sourceRef := pipeline.NodeRef(sourceName)
	if err := addPlannedNode(nodes, &spec, sourceName, pipeline.NodeSource, sourceRef, inputNodeDetail(b.inputs[0])); err != nil {
		return pipeline.Spec{}, err
	}

	previous, err := planDecodeFilterPath(nodes, &spec, []pipeline.NodeRef{sourceRef}, b.decodes[0], b.filters)
	if err != nil {
		return pipeline.Spec{}, err
	}
	if err := b.planEncodeOutputPath(nodes, &spec, previous, b.encodes[0]); err != nil {
		return pipeline.Spec{}, err
	}
	return spec, nil
}

func (b *builder) planRTPDecodeEncodeToOutput(spec pipeline.Spec) (pipeline.Spec, error) {
	nodes := make(map[string]plannedNode, len(b.rtpInputs)+3+len(b.filters)+len(b.outputs))
	sourceRefs := make([]pipeline.NodeRef, len(b.rtpInputs))
	for i := range b.rtpInputs {
		sourceName := rtpNodeName(b.rtpInputs[i], i)
		sourceRef := pipeline.NodeRef(sourceName)
		if err := addPlannedNode(nodes, &spec, sourceName, pipeline.NodeSource, sourceRef, rtpInputDetail(b.rtpInputs[i])); err != nil {
			return pipeline.Spec{}, err
		}
		sourceRefs[i] = sourceRef
	}

	previous, err := planDecodeFilterPath(nodes, &spec, sourceRefs, b.decodes[0], b.filters)
	if err != nil {
		return pipeline.Spec{}, err
	}
	if err := b.planEncodeOutputPath(nodes, &spec, previous, b.encodes[0]); err != nil {
		return pipeline.Spec{}, err
	}
	return spec, nil
}

func (b *builder) planDecodeEncodeToSink(spec pipeline.Spec) (pipeline.Spec, error) {
	if b.sinks[0] == nil {
		return pipeline.Spec{}, ErrNilSink
	}
	nodes := make(map[string]plannedNode, 4+len(b.filters))
	sourceName := demuxNodeName(b.inputs[0])
	sourceRef := pipeline.NodeRef(sourceName)
	if err := addPlannedNode(nodes, &spec, sourceName, pipeline.NodeSource, sourceRef, inputNodeDetail(b.inputs[0])); err != nil {
		return pipeline.Spec{}, err
	}
	previous, err := planDecodeFilterPath(nodes, &spec, []pipeline.NodeRef{sourceRef}, b.decodes[0], b.filters)
	if err != nil {
		return pipeline.Spec{}, err
	}
	if err := b.planEncodeSinkPath(nodes, &spec, previous, b.encodes[0]); err != nil {
		return pipeline.Spec{}, err
	}
	return spec, nil
}

func (b *builder) planRTPDecodeEncodeToSink(spec pipeline.Spec) (pipeline.Spec, error) {
	if b.sinks[0] == nil {
		return pipeline.Spec{}, ErrNilSink
	}
	nodes := make(map[string]plannedNode, len(b.rtpInputs)+3+len(b.filters))
	sourceRefs := make([]pipeline.NodeRef, len(b.rtpInputs))
	for i := range b.rtpInputs {
		sourceName := rtpNodeName(b.rtpInputs[i], i)
		sourceRef := pipeline.NodeRef(sourceName)
		if err := addPlannedNode(nodes, &spec, sourceName, pipeline.NodeSource, sourceRef, rtpInputDetail(b.rtpInputs[i])); err != nil {
			return pipeline.Spec{}, err
		}
		sourceRefs[i] = sourceRef
	}
	previous, err := planDecodeFilterPath(nodes, &spec, sourceRefs, b.decodes[0], b.filters)
	if err != nil {
		return pipeline.Spec{}, err
	}
	if err := b.planEncodeSinkPath(nodes, &spec, previous, b.encodes[0]); err != nil {
		return pipeline.Spec{}, err
	}
	return spec, nil
}

func (b *builder) planEncodeOutputPath(nodes map[string]plannedNode, spec *pipeline.Spec, upstream pipeline.NodeRef, request encodeRequest) error {
	outputs := make([]destinationSpec, 0, len(b.outputs))
	for i := range b.outputs {
		output := destinationSpec{output: b.outputs[i], format: b.outputFormat(i), resolvedFormat: b.outputOpenFormat(i)}
		outputs = append(outputs, output)
	}
	return planEncodeDestinationPath(nodes, spec, upstream, request, outputs)
}

func planEncodeDestinationPath(nodes map[string]plannedNode, spec *pipeline.Spec, upstream pipeline.NodeRef, request encodeRequest, outputs []destinationSpec) error {
	encodeName := encodeNodeName(request)
	encodeRef := pipeline.NodeRef(encodeName)
	if err := addPlannedNode(nodes, spec, encodeName, pipeline.NodeStage, encodeRef, encodeNodeDetail(request)); err != nil {
		return err
	}
	spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
		From:   upstream,
		To:     encodeRef,
		Policy: pipeline.RouteAll,
	})
	for i := range outputs {
		if outputs[i].sink != nil {
			if err := planSinkPath(nodes, spec, encodeRef, outputs[i].sink); err != nil {
				return err
			}
			continue
		}
		outputName := muxNodeName(outputs[i].output, i)
		outputRef := pipeline.NodeRef(outputName)
		if err := addPlannedNode(nodes, spec, outputName, pipeline.NodeStage, outputRef, outputNodeDetailWithFormat(outputs[i].output, destinationGraphFormat(outputs[i]))); err != nil {
			return err
		}
		spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
			From:   encodeRef,
			To:     outputRef,
			Policy: pipeline.RouteAll,
		})
	}
	return nil
}

func (b *builder) planEncodeSinkPath(nodes map[string]plannedNode, spec *pipeline.Spec, upstream pipeline.NodeRef, request encodeRequest) error {
	return planEncodeSinkPath(nodes, spec, upstream, request, b.sinks[0])
}

func planEncodeSinkPath(nodes map[string]plannedNode, spec *pipeline.Spec, upstream pipeline.NodeRef, request encodeRequest, sink pipeline.Sink) error {
	encodeName := encodeNodeName(request)
	encodeRef := pipeline.NodeRef(encodeName)
	if err := addPlannedNode(nodes, spec, encodeName, pipeline.NodeStage, encodeRef, encodeNodeDetail(request)); err != nil {
		return err
	}
	spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
		From:   upstream,
		To:     encodeRef,
		Policy: pipeline.RouteAll,
	})
	return planSinkPath(nodes, spec, encodeRef, sink)
}

func (b *builder) buildDecodeEncodeToOutput(ctx context.Context) (Task, error) {
	graph, err := b.newGraph(ctx)
	if err != nil {
		return nil, err
	}
	if err := b.compileDecodeEncodeToOutput(ctx, graph); err != nil {
		graph.Close()
		return nil, err
	}
	return newTask(graph, b.runtime, b.destinationTxs...), nil
}

func (b *builder) buildRTPDecodeEncodeToOutput(ctx context.Context) (Task, error) {
	graph, err := b.newGraph(ctx)
	if err != nil {
		return nil, err
	}
	if err := b.compileRTPDecodeEncodeToOutput(ctx, graph); err != nil {
		graph.Close()
		return nil, err
	}
	return newTask(graph, b.runtime, b.destinationTxs...), nil
}

func (b *builder) compileDecodeEncodeToOutput(ctx context.Context, graph pipeline.Graph) error {
	demux, err := b.openDemuxSource(ctx, b.inputs[0])
	if err != nil {
		return err
	}
	sourceRef, err := graph.AddSource(demux.source, b.runtime.buffer)
	if err != nil {
		demux.source.Close()
		return err
	}

	request := b.decodes[0]
	stream, err := selectDecodeStream(demux.streams, request.selector)
	if err != nil {
		return err
	}
	realtime := b.runtime.realtime || b.inputs[0].Realtime
	previousRef, filteredStream, err := b.compileDecodeFilterPath(ctx, graph, []pipeline.NodeRef{sourceRef}, request, stream, realtime, false, codec.DecodeBounds{})
	if err != nil {
		return err
	}
	encodeConfig, encodedStream, err := prepareEncodeConfig(filteredStream, b.encodes[0], realtime)
	if err != nil {
		return err
	}
	return b.compileEncodeOutputPath(ctx, graph, previousRef, b.encodes[0], encodeConfig, encodedStream)
}

func (b *builder) compileRTPDecodeEncodeToOutput(ctx context.Context, graph pipeline.Graph) error {
	sourceRefs := make([]pipeline.NodeRef, 0, len(b.rtpInputs))
	streams := make([]av.Stream, 0, len(b.rtpInputs))
	builds := make([]rtpBuild, 0, len(b.rtpInputs))
	for i := range b.rtpInputs {
		receiver, err := b.openRTPSource(ctx, b.rtpInputs[i], i)
		if err != nil {
			return err
		}
		sourceRef, err := graph.AddSource(receiver.source, b.runtime.buffer)
		if err != nil {
			receiver.source.Close()
			return err
		}
		sourceRefs = append(sourceRefs, sourceRef)
		streams = append(streams, receiver.streams...)
		builds = append(builds, receiver)
	}

	request := b.decodes[0]
	selector := request.selector
	stream, err := selectDecodeStream(streams, selector)
	if err != nil {
		return err
	}
	previousRef, filteredStream, err := b.compileDecodeFilterPath(ctx, graph, sourceRefs, request, stream, b.runtime.realtime, false, rtpDecodeBoundsForStream(stream, builds))
	if err != nil {
		return err
	}
	encodeConfig, encodedStream, err := prepareEncodeConfig(filteredStream, b.encodes[0], b.runtime.realtime)
	if err != nil {
		return err
	}
	return b.compileEncodeOutputPath(ctx, graph, previousRef, b.encodes[0], encodeConfig, encodedStream)
}

func (b *builder) compileDecodeEncodeToSink(ctx context.Context, graph pipeline.Graph) error {
	if b.sinks[0] == nil {
		return ErrNilSink
	}
	demux, err := b.openDemuxSource(ctx, b.inputs[0])
	if err != nil {
		return err
	}
	sourceRef, err := graph.AddSource(demux.source, b.runtime.buffer)
	if err != nil {
		demux.source.Close()
		return err
	}

	request := b.decodes[0]
	stream, err := selectDecodeStream(demux.streams, request.selector)
	if err != nil {
		return err
	}
	realtime := b.runtime.realtime || b.inputs[0].Realtime
	previousRef, filteredStream, err := b.compileDecodeFilterPath(ctx, graph, []pipeline.NodeRef{sourceRef}, request, stream, realtime, false, codec.DecodeBounds{})
	if err != nil {
		return err
	}
	encodeConfig, _, err := prepareEncodeConfig(filteredStream, b.encodes[0], realtime)
	if err != nil {
		return err
	}
	return b.compileEncodeSinkPath(ctx, graph, previousRef, b.encodes[0], encodeConfig)
}

func (b *builder) compileRTPDecodeEncodeToSink(ctx context.Context, graph pipeline.Graph) error {
	if b.sinks[0] == nil {
		return ErrNilSink
	}
	sourceRefs := make([]pipeline.NodeRef, 0, len(b.rtpInputs))
	streams := make([]av.Stream, 0, len(b.rtpInputs))
	builds := make([]rtpBuild, 0, len(b.rtpInputs))
	for i := range b.rtpInputs {
		receiver, err := b.openRTPSource(ctx, b.rtpInputs[i], i)
		if err != nil {
			return err
		}
		sourceRef, err := graph.AddSource(receiver.source, b.runtime.buffer)
		if err != nil {
			receiver.source.Close()
			return err
		}
		sourceRefs = append(sourceRefs, sourceRef)
		streams = append(streams, receiver.streams...)
		builds = append(builds, receiver)
	}

	request := b.decodes[0]
	selector := request.selector
	stream, err := selectDecodeStream(streams, selector)
	if err != nil {
		return err
	}
	previousRef, filteredStream, err := b.compileDecodeFilterPath(ctx, graph, sourceRefs, request, stream, b.runtime.realtime, false, rtpDecodeBoundsForStream(stream, builds))
	if err != nil {
		return err
	}
	encodeConfig, _, err := prepareEncodeConfig(filteredStream, b.encodes[0], b.runtime.realtime)
	if err != nil {
		return err
	}
	return b.compileEncodeSinkPath(ctx, graph, previousRef, b.encodes[0], encodeConfig)
}

func (b *builder) compileEncodeOutputPath(ctx context.Context, graph pipeline.Graph, upstream pipeline.NodeRef, request encodeRequest, config codec.EncodeConfig, stream av.Stream) error {
	outputs := make([]destinationSpec, 0, len(b.outputs))
	for i := range b.outputs {
		output := destinationSpec{output: b.outputs[i], format: b.outputFormat(i), resolvedFormat: b.outputOpenFormat(i)}
		outputs = append(outputs, output)
	}
	return compileEncodeDestinationPath(ctx, b, graph, upstream, request, config, stream, outputs)
}

func compileEncodeDestinationPath(ctx context.Context, service *builder, graph pipeline.Graph, upstream pipeline.NodeRef, request encodeRequest, config codec.EncodeConfig, stream av.Stream, outputs []destinationSpec) error {
	runtime := service.runtime
	encodeRef, err := compileEncodeStage(ctx, runtime, graph, upstream, request, config)
	if err != nil {
		return err
	}

	streams := []av.Stream{stream}
	for i := range outputs {
		if outputs[i].sink != nil {
			sinkRef, err := graph.AddSink(outputs[i].sink, runtime.buffer)
			if err != nil {
				return err
			}
			if err := connectRefs(graph, encodeRef, sinkRef); err != nil {
				return err
			}
			continue
		}
		muxStage, err := service.openMuxDestinationStage(ctx, outputs[i], i, streams, destinationOpenFormat(outputs[i]), destinationGraphFormat(outputs[i]))
		if err != nil {
			return err
		}
		muxRef, err := graph.AddStage(muxStage, runtime.buffer)
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

func (b *builder) compileEncodeSinkPath(ctx context.Context, graph pipeline.Graph, upstream pipeline.NodeRef, request encodeRequest, config codec.EncodeConfig) error {
	return compileEncodeSinkPath(ctx, b.runtime, graph, upstream, request, config, b.sinks[0])
}

func compileEncodeSinkPath(ctx context.Context, runtime *runtime, graph pipeline.Graph, upstream pipeline.NodeRef, request encodeRequest, config codec.EncodeConfig, sink pipeline.Sink) error {
	encodeRef, err := compileEncodeStage(ctx, runtime, graph, upstream, request, config)
	if err != nil {
		return err
	}
	sinkRef, err := graph.AddSink(sink, runtime.buffer)
	if err != nil {
		return err
	}
	if err := connectRefs(graph, encodeRef, sinkRef); err != nil {
		return err
	}
	return nil
}

func (b *builder) compileEncodeStage(ctx context.Context, graph pipeline.Graph, upstream pipeline.NodeRef, request encodeRequest, config codec.EncodeConfig) (pipeline.NodeRef, error) {
	return compileEncodeStage(ctx, b.runtime, graph, upstream, request, config)
}

func compileEncodeStage(ctx context.Context, runtime *runtime, graph pipeline.Graph, upstream pipeline.NodeRef, request encodeRequest, config codec.EncodeConfig) (pipeline.NodeRef, error) {
	return compileEncodeStageNamed(ctx, runtime, graph, "", upstream, request, config)
}

func (b *builder) compileEncodeStageNamed(ctx context.Context, graph pipeline.Graph, name string, upstream pipeline.NodeRef, request encodeRequest, config codec.EncodeConfig) (pipeline.NodeRef, error) {
	return compileEncodeStageNamed(ctx, b.runtime, graph, name, upstream, request, config)
}

func compileEncodeStageNamed(ctx context.Context, runtime *runtime, graph pipeline.Graph, name string, upstream pipeline.NodeRef, request encodeRequest, config codec.EncodeConfig) (pipeline.NodeRef, error) {
	stage, err := (&builder{runtime: runtime}).newEncodeStageNamed(ctx, name, request, config)
	if err != nil {
		return "", err
	}
	encodeRef, err := graph.AddStage(stage, runtime.buffer)
	if err != nil {
		stage.Close()
		return "", err
	}
	if err := connectRefs(graph, upstream, encodeRef); err != nil {
		return "", err
	}
	return encodeRef, nil
}

func (b *builder) newEncodeStage(ctx context.Context, request encodeRequest, config codec.EncodeConfig) (*codec.EncoderStage, error) {
	return b.newEncodeStageNamed(ctx, "", request, config)
}

func (b *builder) newEncodeStageNamed(ctx context.Context, name string, request encodeRequest, config codec.EncodeConfig) (*codec.EncoderStage, error) {
	factory, err := b.runtime.codecs.EncoderFactory(config.Parameters.ID)
	if err != nil {
		return nil, err
	}
	encoder, err := factory.NewEncoder(ctx, config)
	if err != nil {
		return nil, err
	}
	name = firstNonEmpty(name, encodeNodeName(request))
	stage, err := codec.NewEncoderStage(codec.EncoderStageConfig{
		Name:              name,
		Detail:            encodeNodeDetail(request),
		Encoder:           encoder,
		Result:            encodeResultForStream(config.Stream),
		OutputStreamID:    config.Stream.ID,
		OutputCodecEpoch:  config.Stream.Epoch,
		StampOutputStream: true,
	})
	if err != nil {
		encoder.Close()
		return nil, err
	}
	return stage, nil
}

func prepareEncodeConfig(input av.Stream, request encodeRequest, realtime bool) (codec.EncodeConfig, av.Stream, error) {
	if !streamMatchesSelector(input, request.selector) {
		return codec.EncodeConfig{}, av.Stream{}, encodeStreamMismatchError(request, input)
	}
	config := request.config
	targetCodec := config.Parameters.ID
	if targetCodec == "" {
		targetCodec = config.Stream.Codec.ID
	}
	if targetCodec == "" {
		return codec.EncodeConfig{}, av.Stream{}, encodeTargetMissingError(request, input)
	}

	stream := mergeEncodeStream(input, config.Stream)
	baseParameters := input.Codec
	if targetCodec != input.Codec.ID {
		baseParameters.ID = ""
		baseParameters.Profile = ""
		baseParameters.Level = ""
		baseParameters.ExtraData = av.Buffer{}
		baseParameters.Attributes = nil
	}
	parameters := mergeCodecParameters(baseParameters, config.Stream.Codec)
	parameters = mergeCodecParameters(parameters, config.Parameters)
	parameters.ID = targetCodec
	if parameters.Type == "" {
		parameters.Type = stream.Type
	}
	if parameters.Type == "" {
		parameters.Type = input.Type
	}
	if stream.Type == "" {
		stream.Type = parameters.Type
	}
	if stream.ID == "" {
		stream.ID = input.ID
	}
	if stream.TimeBase == (av.TimeBase{}) && parameters.ClockRate != 0 {
		stream.TimeBase = av.RTPTimeBase(parameters.ClockRate)
	}
	stream.Codec = parameters

	config.Stream = stream
	config.Parameters = parameters
	if realtime {
		config.Realtime = true
		config.LowLatency = true
	}
	return config, stream, nil
}

func encodeStreamMismatchError(request encodeRequest, stream av.Stream) error {
	return streamRequestMismatchError(
		"encode_stream_mismatch",
		"configure encode",
		encodeNodeName(request),
		request.selector,
		stream,
		[]string{
			"use the same stream selector for Decode and Encode in the expert builder",
			"use .Audio().Opus(...) for audio streams or .Video().VP8(...)/.VP9(...) for video streams in recipes",
			"narrow ambiguous inputs with StreamID, StreamName, or StreamIndex(0)",
		},
	)
}

func encodeTargetMissingError(request encodeRequest, stream av.Stream) error {
	return &BuildError{
		Code:      "encode_target_missing",
		Operation: "configure encode",
		Node:      encodeNodeName(request),
		Reason:    "no target codec was provided",
		Details:   []string{"selected: " + streamDiagnostic(stream, 0)},
		Suggestions: []string{
			"use .Opus(...), .VP8(...), or .VP9(...) in recipe encode paths",
			"set codec.EncodeConfig.Parameters.ID in the expert builder",
			"use goav.Copy() or .Copy().To(...) when no encode step is intended",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func mergeEncodeStream(base av.Stream, override av.Stream) av.Stream {
	out := base
	if override.ID != "" {
		out.ID = override.ID
	}
	if override.Index != 0 {
		out.Index = override.Index
	}
	if override.Type != "" {
		out.Type = override.Type
	}
	if override.TimeBase != (av.TimeBase{}) {
		out.TimeBase = override.TimeBase
	}
	if override.Language != "" {
		out.Language = override.Language
	}
	if override.Name != "" {
		out.Name = override.Name
	}
	if override.Epoch != 0 {
		out.Epoch = override.Epoch
	}
	if override.Metadata != nil {
		out.Metadata = override.Metadata
	}
	return out
}

func mergeCodecParameters(base av.CodecParameters, override av.CodecParameters) av.CodecParameters {
	out := base
	if override.ID != "" {
		out.ID = override.ID
	}
	if override.Type != "" {
		out.Type = override.Type
	}
	if override.Profile != "" {
		out.Profile = override.Profile
	}
	if override.Level != "" {
		out.Level = override.Level
	}
	if override.ClockRate != 0 {
		out.ClockRate = override.ClockRate
	}
	if override.SampleRate != 0 {
		out.SampleRate = override.SampleRate
	}
	if override.Channels != 0 {
		out.Channels = override.Channels
	}
	if override.ChannelLayout != "" {
		out.ChannelLayout = override.ChannelLayout
	}
	if override.Width != 0 {
		out.Width = override.Width
	}
	if override.Height != 0 {
		out.Height = override.Height
	}
	if override.PixelFormat != "" {
		out.PixelFormat = override.PixelFormat
	}
	if override.SampleFormat != "" {
		out.SampleFormat = override.SampleFormat
	}
	if override.ExtraData.Bytes != nil || override.ExtraData.Ownership != "" || override.ExtraData.Owner != nil {
		out.ExtraData = override.ExtraData
	}
	if override.Attributes != nil {
		out.Attributes = override.Attributes
	}
	return out
}

func encodeResultForStream(stream av.Stream) codec.EncodeResult {
	packet := av.Packet{
		StreamID:   stream.ID,
		CodecEpoch: stream.Epoch,
		Payload: av.Buffer{
			Bytes: make([]byte, 0, encodePacketBufferSize(stream)),
		},
	}
	return codec.EncodeResult{
		Packets: []av.Packet{packet}[:0],
		Events:  make([]av.Event, 0, 1),
	}
}

func encodePacketBufferSize(stream av.Stream) int {
	if stream.Type == av.MediaVideo || stream.Codec.Type == av.MediaVideo {
		return 64 * 1024
	}
	return 4096
}

func encodeNodeName(request encodeRequest) string {
	if request.name != "" {
		return "encode-" + request.name
	}
	selector := request.selector
	if selector.Name != "" {
		return "encode-" + selector.Name
	}
	if selector.ID != "" {
		return "encode-" + string(selector.ID)
	}
	if selector.Codec != "" {
		return "encode-" + string(selector.Codec)
	}
	if selector.Type != "" {
		return "encode-" + string(selector.Type)
	}
	if selectorHasIndex(selector) {
		return "encode-" + strconv.Itoa(selector.Index)
	}
	codecID := request.config.Parameters.ID
	if codecID == "" {
		codecID = request.config.Stream.Codec.ID
	}
	if codecID != "" {
		return "encode-" + string(codecID)
	}
	return "encode"
}
