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
		len(b.filters) == 0 &&
		len(b.transcodes) == 0 &&
		len(b.sources) == 0 &&
		len(b.stages) == 0 &&
		len(b.links) == 0 &&
		len(b.routes) == 0
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

	nodes := make(map[string]plannedNode, 3)
	sourceName := demuxNodeName(b.inputs[0])
	sourceRef := pipeline.NodeRef(sourceName)
	if err := addPlannedNode(nodes, &spec, sourceName, pipeline.NodeSource, sourceRef); err != nil {
		return pipeline.Spec{}, err
	}

	selectName := selectNodeName(b.decodes[0])
	selectRef := pipeline.NodeRef(selectName)
	if err := addPlannedNode(nodes, &spec, selectName, pipeline.NodeStage, selectRef); err != nil {
		return pipeline.Spec{}, err
	}

	decodeName := decodeNodeName(b.decodes[0])
	decodeRef := pipeline.NodeRef(decodeName)
	if err := addPlannedNode(nodes, &spec, decodeName, pipeline.NodeStage, decodeRef); err != nil {
		return pipeline.Spec{}, err
	}

	sinkName := b.sinks[0].Name()
	sinkRef := pipeline.NodeRef(sinkName)
	if err := addPlannedNode(nodes, &spec, sinkName, pipeline.NodeSink, sinkRef); err != nil {
		return pipeline.Spec{}, err
	}

	spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
		From:   sourceRef,
		To:     selectRef,
		Policy: pipeline.RouteAll,
	})
	spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
		From:   selectRef,
		To:     decodeRef,
		Policy: pipeline.RouteAll,
	})
	spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
		From:   decodeRef,
		To:     sinkRef,
		Policy: pipeline.RouteAll,
	})
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
	selectStage := newStreamSelectStage(selectNodeName(selector), stream.ID)
	selectRef, err := graph.AddStage(selectStage, b.runtime.buffer)
	if err != nil {
		selectStage.Close()
		return err
	}

	factory, err := b.runtime.codecs.DecoderFactory(stream.Codec.ID)
	if err != nil {
		return err
	}
	decoder, err := factory.NewDecoder(ctx, codec.DecodeConfig{
		Stream:     stream,
		Realtime:   b.runtime.realtime || b.inputs[0].Realtime,
		LowLatency: b.runtime.realtime || b.inputs[0].Realtime,
		Resilience: codec.ResiliencePolicy{
			AcceptLoss:       true,
			ConcealAudio:     stream.Type == av.MediaAudio,
			DropDamagedVideo: stream.Type == av.MediaVideo,
			RequestKeyframes: stream.Type == av.MediaVideo,
		},
	})
	if err != nil {
		return err
	}
	stage, err := codec.NewDecoderStage(codec.DecoderStageConfig{
		Name:            decodeNodeName(selector),
		Decoder:         decoder,
		Result:          decodeResultForStream(stream),
		DropInputEvents: true,
	})
	if err != nil {
		decoder.Close()
		return err
	}
	stageRef, err := graph.AddStage(stage, b.runtime.buffer)
	if err != nil {
		stage.Close()
		return err
	}
	sinkRef, err := graph.AddSink(b.sinks[0], b.runtime.buffer)
	if err != nil {
		return err
	}

	if err := graph.Link(pipeline.Link{From: sourceRef, To: selectRef}); err != nil {
		return err
	}
	if err := graph.Link(pipeline.Link{From: selectRef, To: stageRef}); err != nil {
		return err
	}
	return graph.Link(pipeline.Link{From: stageRef, To: sinkRef})
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
	return codec.DecodeResult{
		Frames:   []av.Frame{frame}[:0],
		Events:   make([]av.Event, 0, 1),
		Requests: make([]codec.ControlRequest, 0, 1),
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
