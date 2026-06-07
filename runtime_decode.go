package goav

import (
	"context"
	"strconv"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
)

const (
	defaultVideoDecodeWidth  = 1920
	defaultVideoDecodeHeight = 1080
)

type decodeToSinkGraphCompiler struct{}

func (decodeToSinkGraphCompiler) match(b *builder) bool {
	return len(b.inputs) == 1 &&
		len(b.decodes) == 1 &&
		len(b.sinks) == 1 &&
		len(b.outputs) == 0 &&
		len(b.encodes) == 0 &&
		len(b.transcodes) == 0 &&
		len(b.sources) == 0 &&
		len(b.stages) == 0
}

func (decodeToSinkGraphCompiler) describe(b *builder, spec pipeline.Spec) (pipeline.Spec, error) {
	return b.planDecodeToSink(spec)
}

func (decodeToSinkGraphCompiler) build(ctx context.Context, b *builder) (Task, error) {
	return b.buildDecodeToSink(ctx)
}

func (b *builder) planDecodeToSink(spec pipeline.Spec) (pipeline.Spec, error) {
	if b.sinks[0] == nil {
		return pipeline.Spec{}, ErrNilSink
	}

	nodes := make(map[string]plannedNode, 4+len(b.filters))
	sourceName := demuxNodeName(b.inputs[0])
	sourceRef := pipeline.NodeRef(sourceName)
	if err := addPlannedNode(nodes, &spec, sourceName, pipeline.NodeSource, sourceRef, inputNodeDetail(b.inputs[0])); err != nil {
		return pipeline.Spec{}, err
	}

	if err := b.planDecodeFramePath(nodes, &spec, []pipeline.NodeRef{sourceRef}, b.decodes[0]); err != nil {
		return pipeline.Spec{}, err
	}
	return spec, nil
}

func (b *builder) buildDecodeToSink(ctx context.Context) (Task, error) {
	graph, err := b.newGraph(ctx)
	if err != nil {
		return nil, err
	}
	if err := b.compileDecodeToSink(ctx, graph); err != nil {
		graph.Close()
		return nil, err
	}
	return newTask(graph, b.runtime, b.destinationTxs...), nil
}

func (b *builder) compileDecodeToSink(ctx context.Context, graph pipeline.Graph) error {
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
	return b.compileDecodeFramePath(ctx, graph, []pipeline.NodeRef{sourceRef}, request, stream, b.runtime.realtime || b.inputs[0].Realtime, codec.DecodeBounds{})
}

func (b *builder) newDecodeStage(ctx context.Context, request decodeRequest, stream av.Stream, realtime bool, dropInputEvents bool, bounds codec.DecodeBounds) (*codec.DecoderStage, error) {
	return b.newDecodeStageNamed(ctx, decodeNodeName(request.selector), request, stream, realtime, dropInputEvents, bounds)
}

func (b *builder) newDecodeStageNamed(ctx context.Context, name string, request decodeRequest, stream av.Stream, realtime bool, dropInputEvents bool, bounds codec.DecodeBounds) (*codec.DecoderStage, error) {
	factory, err := b.runtime.codecs.DecoderFactory(stream.Codec.ID)
	if err != nil {
		return nil, err
	}
	name = firstNonEmpty(name, decodeNodeName(request.selector))
	intent := StreamIntent{
		Name: name,
		Select: StreamSelect{
			ID:       request.selector.ID,
			Index:    request.selector.Index,
			UseIndex: request.selector.UseIndex,
			Type:     request.selector.Type,
			Codec:    request.selector.Codec,
			Name:     request.selector.Name,
		},
	}
	if err := validateDecodeAdapterDescriptors("build decode stage", intent, b.runtime.codecs, decodeAdapterRequestFromStream(stream, intent)); err != nil {
		return nil, err
	}
	stream = decodeStreamWithSpec(stream, request.config)
	result := decodeResultForStream(stream, bounds)
	config := codec.DecodeConfig{
		Stream:     stream,
		Realtime:   realtime,
		LowLatency: realtime,
		Resilience: codec.ResiliencePolicy{
			AcceptLoss:       true,
			ConcealAudio:     stream.Type == av.MediaAudio,
			DropDamagedVideo: stream.Type == av.MediaVideo,
			RequestKeyframes: stream.Type == av.MediaVideo,
		},
		Bounds:   decodeBoundsForStream(stream, result, bounds),
		Config:   request.config.Config,
		Opaque:   cloneAnyMap(request.config.Opaque),
		Controls: append([]any(nil), request.config.Controls...),
	}
	stateFromFactory := false
	if stateFactory, ok := factory.(codec.DecodeStateFactory); ok {
		state, err := stateFactory.NewDecodeState(ctx, config)
		if err != nil {
			return nil, err
		}
		config.OpaqueState = state
		stateFromFactory = true
	}
	decoder, err := factory.NewDecoder(ctx, config)
	if err != nil {
		if stateFromFactory {
			closeDecodeState(config.OpaqueState)
		}
		return nil, err
	}
	stage, err := codec.NewDecoderStage(codec.DecoderStageConfig{
		Name:            name,
		Detail:          decodeRequestDetail(request),
		InputStream:     stream,
		Decoder:         decoder,
		Result:          result,
		DropInputEvents: dropInputEvents,
	})
	if err != nil {
		decoder.Close()
		return nil, err
	}
	return stage, nil
}

func closeDecodeState(state any) {
	closer, ok := state.(interface{ Close() })
	if ok {
		closer.Close()
	}
}

func (b *builder) planDecodeFramePath(nodes map[string]plannedNode, spec *pipeline.Spec, upstream []pipeline.NodeRef, request decodeRequest) error {
	previous, err := planDecodeFilterPath(nodes, spec, upstream, request, b.filters)
	if err != nil {
		return err
	}

	if err := planSinkPath(nodes, spec, previous, b.sinks[0]); err != nil {
		return err
	}
	return nil
}

func (b *builder) planDecodeFilterPath(nodes map[string]plannedNode, spec *pipeline.Spec, upstream []pipeline.NodeRef, request decodeRequest) (pipeline.NodeRef, error) {
	return planDecodeFilterPath(nodes, spec, upstream, request, b.filters)
}

func planDecodeFilterPath(nodes map[string]plannedNode, spec *pipeline.Spec, upstream []pipeline.NodeRef, request decodeRequest, filters []filterRequest) (pipeline.NodeRef, error) {
	selector := request.selector
	selectName := selectNodeName(selector)
	selectRef := pipeline.NodeRef(selectName)
	if err := addPlannedNode(nodes, spec, selectName, pipeline.NodeStage, selectRef, selectNodeDetail(selector)); err != nil {
		return "", err
	}

	decodeName := decodeNodeName(selector)
	decodeRef := pipeline.NodeRef(decodeName)
	if err := addPlannedNode(nodes, spec, decodeName, pipeline.NodeStage, decodeRef, decodeRequestDetail(request)); err != nil {
		return "", err
	}

	for i := range upstream {
		spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
			From:   upstream[i],
			To:     selectRef,
			Policy: pipeline.RouteAll,
		})
	}
	spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
		From:   selectRef,
		To:     decodeRef,
		Policy: pipeline.RouteAll,
	})

	previous := decodeRef
	for i := range filters {
		name, detail, err := filterRequestPlanNode(filters[i])
		if err != nil {
			return "", err
		}
		ref := pipeline.NodeRef(name)
		if err := addPlannedNode(nodes, spec, name, pipeline.NodeStage, ref, detail); err != nil {
			return "", err
		}
		spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
			From:   previous,
			To:     ref,
			Policy: pipeline.RouteAll,
		})
		previous = ref
	}
	return previous, nil
}

func planSinkPath(nodes map[string]plannedNode, spec *pipeline.Spec, upstream pipeline.NodeRef, sink pipeline.Sink) error {
	if sink == nil {
		return ErrNilSink
	}
	sinkName := sink.Name()
	sinkRef := pipeline.NodeRef(sinkName)
	if err := addPlannedNode(nodes, spec, sinkName, pipeline.NodeSink, sinkRef, describedNodeDetail(sink)); err != nil {
		return err
	}
	spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
		From:   upstream,
		To:     sinkRef,
		Policy: pipeline.RouteAll,
	})
	return nil
}

func (b *builder) compileDecodeFramePath(ctx context.Context, graph pipeline.Graph, upstream []pipeline.NodeRef, request decodeRequest, stream av.Stream, realtime bool, bounds codec.DecodeBounds) error {
	previousRef, _, err := compileDecodeFilterPath(ctx, b.runtime, graph, upstream, request, stream, realtime, true, bounds, b.filters)
	if err != nil {
		return err
	}

	sinkRef, err := graph.AddSink(b.sinks[0], b.runtime.buffer)
	if err != nil {
		return err
	}
	if err := connectRefs(graph, previousRef, sinkRef); err != nil {
		return err
	}
	return nil
}

func (b *builder) compileDecodeFilterPath(ctx context.Context, graph pipeline.Graph, upstream []pipeline.NodeRef, request decodeRequest, stream av.Stream, realtime bool, dropDecodeEvents bool, bounds codec.DecodeBounds) (pipeline.NodeRef, av.Stream, error) {
	return compileDecodeFilterPath(ctx, b.runtime, graph, upstream, request, stream, realtime, dropDecodeEvents, bounds, b.filters)
}

type graphPlanDecodeFilterNodes struct {
	selectNode  pipeline.NodeRef
	decodeNode  pipeline.NodeRef
	filterNodes []pipeline.NodeRef
}

func compileDecodeFilterPath(ctx context.Context, runtime *runtime, graph pipeline.Graph, upstream []pipeline.NodeRef, request decodeRequest, stream av.Stream, realtime bool, dropDecodeEvents bool, bounds codec.DecodeBounds, filters []filterRequest) (pipeline.NodeRef, av.Stream, error) {
	return compileDecodeFilterPathNamed(ctx, runtime, graph, upstream, request, stream, realtime, dropDecodeEvents, bounds, filters, graphPlanDecodeFilterNodes{})
}

func compileDecodeFilterPathNamed(ctx context.Context, runtime *runtime, graph pipeline.Graph, upstream []pipeline.NodeRef, request decodeRequest, stream av.Stream, realtime bool, dropDecodeEvents bool, bounds codec.DecodeBounds, filters []filterRequest, nodes graphPlanDecodeFilterNodes) (pipeline.NodeRef, av.Stream, error) {
	if len(nodes.filterNodes) != 0 && len(nodes.filterNodes) != len(filters) {
		return "", av.Stream{}, graphPlanInvalidError("decode/filter graph plan filter node count does not match concrete filters", []string{
			"planned=" + strconv.Itoa(len(nodes.filterNodes)),
			"filters=" + strconv.Itoa(len(filters)),
		})
	}
	service := &builder{runtime: runtime}
	selector := request.selector
	selectName := firstNonEmpty(nodes.selectNode.String(), selectNodeName(selector))
	selectStage := newStreamSelectStage(selectName, stream, selector, selectNodeDetail(selector))
	selectRef, err := graph.AddStage(selectStage, runtime.buffer)
	if err != nil {
		selectStage.Close()
		return "", av.Stream{}, err
	}

	decodeName := firstNonEmpty(nodes.decodeNode.String(), decodeNodeName(selector))
	decodeStage, err := service.newDecodeStageNamed(ctx, decodeName, request, stream, realtime, dropDecodeEvents, bounds)
	if err != nil {
		return "", av.Stream{}, err
	}
	previousRef, err := graph.AddStage(decodeStage, runtime.buffer)
	if err != nil {
		decodeStage.Close()
		return "", av.Stream{}, err
	}
	for i := range upstream {
		if err := connectRefs(graph, upstream[i], selectRef); err != nil {
			return "", av.Stream{}, err
		}
	}
	if err := connectRefs(graph, selectRef, previousRef); err != nil {
		return "", av.Stream{}, err
	}

	currentStream := stream
	for i := range filters {
		if !streamMatchesSelector(currentStream, filters[i].selector) {
			return "", av.Stream{}, filterStreamMismatchError(filters[i], currentStream)
		}
		var filterName string
		if len(nodes.filterNodes) != 0 {
			filterName = nodes.filterNodes[i].String()
		}
		stage, outputStream, err := service.newFilterRequestStageNamed(ctx, filterName, filters[i], currentStream, realtime)
		if err != nil {
			return "", av.Stream{}, err
		}
		stageRef, err := graph.AddStage(stage, runtime.buffer)
		if err != nil {
			stage.Close()
			return "", av.Stream{}, err
		}
		if err := connectRefs(graph, previousRef, stageRef); err != nil {
			return "", av.Stream{}, err
		}
		previousRef = stageRef
		currentStream = outputStream
	}
	return previousRef, currentStream, nil
}

func filterStreamMismatchError(request filterRequest, stream av.Stream) error {
	node, _, err := filterRequestPlanNode(request)
	if err != nil || node == "" {
		node = "filter"
	}
	return streamRequestMismatchError(
		"filter_stream_mismatch",
		"attach filter",
		node,
		request.selector,
		stream,
		[]string{
			"use the same stream selector for Decode and Filter in the expert builder",
			"use .Audio().Resample(...) for audio transforms and .Video().Resize(...) for video transforms in recipes",
			"narrow ambiguous inputs with StreamID, StreamName, or StreamIndex(0)",
		},
	)
}

func filterRequestPlanNode(request filterRequest) (string, string, error) {
	if request.stage != nil {
		return request.stage.Name(), describedNodeDetail(request.stage), nil
	}
	if request.transform != nil {
		return request.transform.name, mediaTransformDetail(*request.transform), nil
	}
	return "", "", ErrNilStage
}

func (b *builder) newFilterRequestStage(ctx context.Context, request filterRequest, stream av.Stream, realtime bool) (pipeline.Stage, av.Stream, error) {
	return b.newFilterRequestStageNamed(ctx, "", request, stream, realtime)
}

func (b *builder) newFilterRequestStageNamed(ctx context.Context, name string, request filterRequest, stream av.Stream, realtime bool) (pipeline.Stage, av.Stream, error) {
	if request.stage != nil {
		if name != "" && name != request.stage.Name() {
			return namedStage{name: name, stage: request.stage}, stream, nil
		}
		return request.stage, stream, nil
	}
	if request.transform != nil {
		stage, outputStream, err := b.newMediaTransformStageNamed(ctx, name, *request.transform, stream, realtime)
		if err != nil {
			return nil, av.Stream{}, err
		}
		return stage, outputStream, nil
	}
	return nil, av.Stream{}, ErrNilStage
}

func selectDecodeStream(streams []av.Stream, selector av.StreamSelector) (av.Stream, error) {
	return selectStreamWithCodecRequirement(streams, selector, true)
}

func selectStream(streams []av.Stream, selector av.StreamSelector) (av.Stream, error) {
	return selectStreamWithCodecRequirement(streams, selector, false)
}

func selectStreamWithCodecRequirement(streams []av.Stream, selector av.StreamSelector, requireCodec bool) (av.Stream, error) {
	var selected av.Stream
	matches := 0
	matched := make([]av.Stream, 0, len(streams))
	for i := range streams {
		if !streamMatchesSelector(streams[i], selector) {
			continue
		}
		selected = streams[i]
		matches++
		matched = append(matched, streams[i])
	}
	if matches == 0 {
		return av.Stream{}, streamSelectionError("stream_missing", selector, streams)
	}
	if matches > 1 {
		return av.Stream{}, streamSelectionError("stream_ambiguous", selector, matched)
	}
	if requireCodec && selected.Codec.ID == "" {
		return av.Stream{}, &BuildError{
			Code:      "stream_codec_missing",
			Operation: "select stream",
			Node:      selectorDetail(selector),
			Reason:    "selected stream has no codec id",
			Details:   []string{streamDiagnostic(selected, 0)},
			Suggestions: []string{
				"provide codec metadata on the input stream",
				"use goav.RTP(reader).Codec(...) for raw RTP receive",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
	return selected, nil
}

func streamMatchesSelector(stream av.Stream, selector av.StreamSelector) bool {
	if selector.ID != "" && stream.ID != selector.ID {
		return false
	}
	if selectorHasIndex(selector) && stream.Index != selector.Index {
		return false
	}
	if selector.Type != "" && stream.Type != selector.Type {
		return false
	}
	if selector.Codec != "" && stream.Codec.ID != selector.Codec {
		return false
	}
	if selector.Name != "" && stream.Name != selector.Name {
		return false
	}
	return true
}

func selectorHasIndex(selector av.StreamSelector) bool {
	return selector.UseIndex || selector.Index != 0
}

func streamSelectionError(code string, selector av.StreamSelector, streams []av.Stream) error {
	operation := "select stream"
	node := selectorDetail(selector)
	reason := "no stream matches " + readableSelector(selector)
	if code == "stream_ambiguous" {
		reason = "multiple streams match " + readableSelector(selector)
	}
	return &BuildError{
		Code:        code,
		Operation:   operation,
		Node:        node,
		Reason:      reason,
		Details:     streamDiagnostics(streams),
		Suggestions: streamSelectionSuggestions(selector, streams),
		Cause:       ErrUnsupportedBuild,
	}
}

func streamRequestMismatchError(code string, operation string, node string, selector av.StreamSelector, stream av.Stream, suggestions []string) error {
	return &BuildError{
		Code:      code,
		Operation: operation,
		Node:      node,
		Reason:    "requested " + readableSelector(selector) + " does not match selected stream",
		Details: []string{
			"selected: " + streamDiagnostic(stream, 0),
			"requested: " + readableSelector(selector),
		},
		Suggestions: suggestions,
		Cause:       ErrUnsupportedBuild,
	}
}

func readableSelector(selector av.StreamSelector) string {
	detail := selectorDetail(selector)
	if detail == "" {
		return "the requested selector"
	}
	return detail
}

func streamDiagnostics(streams []av.Stream) []string {
	if len(streams) == 0 {
		return nil
	}
	details := make([]string, len(streams))
	for i := range streams {
		details[i] = streamDiagnostic(streams[i], i)
	}
	return details
}

func streamDiagnostic(stream av.Stream, fallbackIndex int) string {
	media := string(stream.Type)
	if media == "" {
		media = "stream"
	}
	index := stream.Index
	if index == 0 {
		index = fallbackIndex
	}
	parts := []string{media + "[" + strconv.Itoa(index) + "]"}
	if stream.ID != "" {
		parts = append(parts, "id="+string(stream.ID))
	}
	if stream.Codec.ID != "" {
		parts = append(parts, "codec="+string(stream.Codec.ID))
	}
	if stream.Name != "" {
		parts = append(parts, "name="+stream.Name)
	}
	if stream.Language != "" {
		parts = append(parts, "lang="+stream.Language)
	}
	return strings.Join(parts, " ")
}

func streamSelectionSuggestions(selector av.StreamSelector, streams []av.Stream) []string {
	call, recipeCall := streamOptionCall(selector)
	suggestions := make([]string, 0, 3)
	for i := range streams {
		if streams[i].ID != "" {
			if recipeCall {
				suggestions = append(suggestions, call+"(goav.StreamID("+strconv.Quote(string(streams[i].ID))+"))")
			} else {
				suggestions = append(suggestions, "select stream id "+strconv.Quote(string(streams[i].ID)))
			}
			break
		}
	}
	for i := range streams {
		if streams[i].Name != "" {
			if recipeCall {
				suggestions = append(suggestions, call+"(goav.StreamName("+strconv.Quote(streams[i].Name)+"))")
			} else {
				suggestions = append(suggestions, "select stream name "+strconv.Quote(streams[i].Name))
			}
			break
		}
	}
	if len(streams) != 0 {
		index := streams[0].Index
		if recipeCall {
			suggestions = append(suggestions, call+"(goav.StreamIndex("+strconv.Itoa(index)+"))")
		} else {
			suggestions = append(suggestions, "select stream index "+strconv.Itoa(index))
		}
	}
	if len(suggestions) == 0 {
		suggestions = append(suggestions, "choose a more specific stream selector")
	}
	return suggestions
}

func streamOptionCall(selector av.StreamSelector) (string, bool) {
	switch selector.Type {
	case av.MediaAudio:
		return ".Audio", true
	case av.MediaVideo:
		return ".Video", true
	default:
		return "", false
	}
}

func decodeResultForStream(stream av.Stream, bounds codec.DecodeBounds) codec.DecodeResult {
	frameCount := positiveOrDefault(bounds.MaxFramesPerInput, 1)
	frames := make([]av.Frame, frameCount)
	for i := range frames {
		if stream.Type == av.MediaAudio || stream.Codec.Type == av.MediaAudio {
			frames[i].Planes = []av.Plane{{Buffer: av.Buffer{Bytes: make([]byte, 0, audioDecodeBufferSize(stream))}}}
		}
		if stream.Type == av.MediaVideo || stream.Codec.Type == av.MediaVideo {
			frames[i] = preallocVideoDecodeFrame(stream, bounds)
		}
	}
	return codec.DecodeResult{
		Frames:   frames[:0],
		Events:   make([]av.Event, 0, positiveOrDefault(bounds.MaxEventsPerInput, 1)),
		Requests: make([]codec.ControlRequest, 0, positiveOrDefault(bounds.MaxRequestsPerInput, 1)),
	}
}

func preallocVideoDecodeFrame(stream av.Stream, bounds codec.DecodeBounds) av.Frame {
	width, height := videoDecodeGeometry(stream, bounds)
	frame := av.Frame{Planes: make([]av.Plane, 3)}
	frame.Planes[0].Buffer.Bytes = make([]byte, 0, width*height)
	frame.Planes[1].Buffer.Bytes = make([]byte, 0, ((width+1)/2)*((height+1)/2))
	frame.Planes[2].Buffer.Bytes = make([]byte, 0, ((width+1)/2)*((height+1)/2))
	return frame
}

func videoDecodeGeometry(stream av.Stream, bounds codec.DecodeBounds) (int, int) {
	width := positiveOrDefault(bounds.MaxWidth, stream.Codec.Width)
	height := positiveOrDefault(bounds.MaxHeight, stream.Codec.Height)
	if width <= 0 {
		width = defaultVideoDecodeWidth
	}
	if height <= 0 {
		height = defaultVideoDecodeHeight
	}
	return width, height
}

func decodeStreamWithSpec(stream av.Stream, spec CodecSpec) av.Stream {
	if spec.ID == "" && spec.Type == "" && !codecSpecHasParameters(spec) {
		return stream
	}
	parameters := mergeCodecParameters(stream.Codec, spec.Parameters)
	if spec.ID != "" {
		parameters.ID = spec.ID
	}
	if spec.Type != "" && parameters.Type == "" {
		parameters.Type = spec.Type
	}
	if stream.Type == "" {
		stream.Type = parameters.Type
	}
	stream.Codec = parameters
	if stream.TimeBase == (av.TimeBase{}) && parameters.ClockRate != 0 {
		stream.TimeBase = av.RTPTimeBase(parameters.ClockRate)
	}
	return stream
}

func decodeBoundsForStream(stream av.Stream, result codec.DecodeResult, bounds codec.DecodeBounds) codec.DecodeBounds {
	merged := codec.DecodeBounds{
		MaxFramesPerInput:   cap(result.Frames),
		MaxEventsPerInput:   cap(result.Events),
		MaxRequestsPerInput: cap(result.Requests),
		MaxWidth:            stream.Codec.Width,
		MaxHeight:           stream.Codec.Height,
	}
	if bounds.MaxPayloadBytes > 0 {
		merged.MaxPayloadBytes = bounds.MaxPayloadBytes
	}
	if bounds.MaxRetainedBytes > 0 {
		merged.MaxRetainedBytes = bounds.MaxRetainedBytes
	}
	if bounds.MaxWidth > 0 {
		merged.MaxWidth = bounds.MaxWidth
	}
	if bounds.MaxHeight > 0 {
		merged.MaxHeight = bounds.MaxHeight
	}
	return merged
}

func positiveOrDefault(value int, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

func audioDecodeBufferSize(stream av.Stream) int {
	sampleRate := stream.Codec.SampleRate
	if sampleRate == 0 {
		sampleRate = int(stream.Codec.ClockRate)
	}
	if sampleRate == 0 {
		sampleRate = 48000
	}
	channels := stream.Codec.Channels
	if channels == 0 {
		channels = 2
	}
	maxSamples := sampleRate * 120 / 1000
	if maxSamples < 960 {
		maxSamples = 960
	}
	return maxSamples * channels * 2
}

func decodeNodeName(selector av.StreamSelector) string {
	if selector.Name != "" {
		return "decode-" + selector.Name
	}
	if selector.ID != "" {
		return "decode-" + string(selector.ID)
	}
	if selector.Codec != "" {
		return "decode-" + string(selector.Codec)
	}
	if selector.Type != "" {
		return "decode-" + string(selector.Type)
	}
	if selectorHasIndex(selector) {
		return "decode-" + strconv.Itoa(selector.Index)
	}
	return "decode"
}
