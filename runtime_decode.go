package goav

import (
	"context"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
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
	return &task{graph: graph}, nil
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

	selector := b.decodes[0]
	stream, err := selectDecodeStream(demux.streams, selector)
	if err != nil {
		return err
	}
	return b.compileDecodeFramePath(ctx, graph, []pipeline.NodeRef{sourceRef}, selector, stream, b.runtime.realtime || b.inputs[0].Realtime)
}

func (b *builder) newDecodeStage(ctx context.Context, selector av.StreamSelector, stream av.Stream, realtime bool, dropInputEvents bool) (*codec.DecoderStage, error) {
	factory, err := b.runtime.codecs.DecoderFactory(stream.Codec.ID)
	if err != nil {
		return nil, err
	}
	result := decodeResultForStream(stream)
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
		Bounds: decodeBoundsForStream(stream, result),
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
		Name:            decodeNodeName(selector),
		Detail:          decodeNodeDetail(selector),
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

func (b *builder) planDecodeFramePath(nodes map[string]plannedNode, spec *pipeline.Spec, upstream []pipeline.NodeRef, selector av.StreamSelector) error {
	previous, err := b.planDecodeFilterPath(nodes, spec, upstream, selector)
	if err != nil {
		return err
	}

	sinkName := b.sinks[0].Name()
	sinkRef := pipeline.NodeRef(sinkName)
	if err := addPlannedNode(nodes, spec, sinkName, pipeline.NodeSink, sinkRef); err != nil {
		return err
	}
	spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
		From:   previous,
		To:     sinkRef,
		Policy: pipeline.RouteAll,
	})
	return nil
}

func (b *builder) planDecodeFilterPath(nodes map[string]plannedNode, spec *pipeline.Spec, upstream []pipeline.NodeRef, selector av.StreamSelector) (pipeline.NodeRef, error) {
	selectName := selectNodeName(selector)
	selectRef := pipeline.NodeRef(selectName)
	if err := addPlannedNode(nodes, spec, selectName, pipeline.NodeStage, selectRef, selectNodeDetail(selector)); err != nil {
		return "", err
	}

	decodeName := decodeNodeName(selector)
	decodeRef := pipeline.NodeRef(decodeName)
	if err := addPlannedNode(nodes, spec, decodeName, pipeline.NodeStage, decodeRef, decodeNodeDetail(selector)); err != nil {
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
	for i := range b.filters {
		if b.filters[i].stage == nil {
			return "", ErrNilStage
		}
		name := b.filters[i].stage.Name()
		ref := pipeline.NodeRef(name)
		if err := addPlannedNode(nodes, spec, name, pipeline.NodeStage, ref, describedNodeDetail(b.filters[i].stage)); err != nil {
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

func (b *builder) compileDecodeFramePath(ctx context.Context, graph pipeline.Graph, upstream []pipeline.NodeRef, selector av.StreamSelector, stream av.Stream, realtime bool) error {
	previousRef, err := b.compileDecodeFilterPath(ctx, graph, upstream, selector, stream, realtime, true)
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

func (b *builder) compileDecodeFilterPath(ctx context.Context, graph pipeline.Graph, upstream []pipeline.NodeRef, selector av.StreamSelector, stream av.Stream, realtime bool, dropDecodeEvents bool) (pipeline.NodeRef, error) {
	if err := b.validateFiltersForStream(stream); err != nil {
		return "", err
	}
	selectStage := newStreamSelectStage(selectNodeName(selector), stream, selector, selectNodeDetail(selector))
	selectRef, err := graph.AddStage(selectStage, b.runtime.buffer)
	if err != nil {
		selectStage.Close()
		return "", err
	}

	decodeStage, err := b.newDecodeStage(ctx, selector, stream, realtime, dropDecodeEvents)
	if err != nil {
		return "", err
	}
	previousRef, err := graph.AddStage(decodeStage, b.runtime.buffer)
	if err != nil {
		decodeStage.Close()
		return "", err
	}
	for i := range upstream {
		if err := connectRefs(graph, upstream[i], selectRef); err != nil {
			return "", err
		}
	}
	if err := connectRefs(graph, selectRef, previousRef); err != nil {
		return "", err
	}

	for i := range b.filters {
		stageRef, err := graph.AddStage(b.filters[i].stage, b.runtime.buffer)
		if err != nil {
			return "", err
		}
		if err := connectRefs(graph, previousRef, stageRef); err != nil {
			return "", err
		}
		previousRef = stageRef
	}
	return previousRef, nil
}

func (b *builder) validateFiltersForStream(stream av.Stream) error {
	for i := range b.filters {
		if b.filters[i].stage == nil {
			return ErrNilStage
		}
		if !streamMatchesSelector(stream, b.filters[i].selector) {
			return ErrUnsupportedBuild
		}
	}
	return nil
}

func selectDecodeStream(streams []av.Stream, selector av.StreamSelector) (av.Stream, error) {
	var selected av.Stream
	matches := 0
	for i := range streams {
		if !streamMatchesSelector(streams[i], selector) {
			continue
		}
		selected = streams[i]
		matches++
	}
	if matches != 1 {
		return av.Stream{}, ErrUnsupportedBuild
	}
	if selected.Codec.ID == "" {
		return av.Stream{}, ErrUnsupportedBuild
	}
	return selected, nil
}

func streamMatchesSelector(stream av.Stream, selector av.StreamSelector) bool {
	if selector.ID != "" && stream.ID != selector.ID {
		return false
	}
	if selector.Index != 0 && stream.Index != selector.Index {
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

func decodeResultForStream(stream av.Stream) codec.DecodeResult {
	frame := av.Frame{}
	if stream.Type == av.MediaAudio || stream.Codec.Type == av.MediaAudio {
		frame.Planes = []av.Plane{{Buffer: av.Buffer{Bytes: make([]byte, 0, audioDecodeBufferSize(stream))}}}
	}
	if stream.Type == av.MediaVideo || stream.Codec.Type == av.MediaVideo {
		frame.Planes = make([]av.Plane, 3)
	}
	return codec.DecodeResult{
		Frames:   []av.Frame{frame}[:0],
		Events:   make([]av.Event, 0, 1),
		Requests: make([]codec.ControlRequest, 0, 1),
	}
}

func decodeBoundsForStream(stream av.Stream, result codec.DecodeResult) codec.DecodeBounds {
	return codec.DecodeBounds{
		MaxFramesPerInput:   cap(result.Frames),
		MaxEventsPerInput:   cap(result.Events),
		MaxRequestsPerInput: cap(result.Requests),
		MaxWidth:            stream.Codec.Width,
		MaxHeight:           stream.Codec.Height,
	}
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
	if selector.Index != 0 {
		return "decode-" + strconv.Itoa(selector.Index)
	}
	return "decode"
}
