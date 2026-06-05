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
	sourcePad := pipeline.PadRef{Node: sourceName, Pad: "out"}
	if err := addPlannedNode(nodes, &spec, sourceName, pipeline.NodeSource, sourcePad); err != nil {
		return pipeline.Spec{}, err
	}

	decodeName := decodeNodeName(b.decodes[0])
	decodePad := pipeline.PadRef{Node: decodeName, Pad: "inout"}
	if err := addPlannedNode(nodes, &spec, decodeName, pipeline.NodeStage, decodePad); err != nil {
		return pipeline.Spec{}, err
	}

	sinkName := b.sinks[0].Name()
	sinkPad := pipeline.PadRef{Node: sinkName, Pad: "in"}
	if err := addPlannedNode(nodes, &spec, sinkName, pipeline.NodeSink, sinkPad); err != nil {
		return pipeline.Spec{}, err
	}

	routeDecodeInput(&spec, sourcePad, decodePad, b.decodes[0])
	spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
		From:   decodePad,
		To:     sinkPad,
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
	sourcePad, err := graph.AddSource(demux.source, b.runtime.buffer)
	if err != nil {
		demux.source.Close()
		return err
	}

	selector := b.decodes[0]
	stream, err := selectDecodeStream(demux.streams, selector)
	if err != nil {
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
	stagePad, err := graph.AddStage(stage, b.runtime.buffer)
	if err != nil {
		stage.Close()
		return err
	}
	sinkPad, err := graph.AddSink(b.sinks[0], b.runtime.buffer)
	if err != nil {
		return err
	}

	if err := linkDecodeInput(graph, sourcePad, stagePad, selector); err != nil {
		return err
	}
	return graph.Link(pipeline.Link{From: stagePad, To: sinkPad})
}

func selectDecodeStream(streams []av.Stream, selector av.StreamSelector) (av.Stream, error) {
	if selector.ID == "" && len(streams) != 1 {
		return av.Stream{}, ErrUnsupportedBuild
	}

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

func routeDecodeInput(spec *pipeline.Spec, from pipeline.PadRef, to pipeline.PadRef, selector av.StreamSelector) {
	policy, label := decodeInputRoute(selector)
	spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
		From:   from,
		To:     to,
		Policy: policy,
		Label:  label,
	})
	if policy == pipeline.RouteByStream {
		spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
			From:   from,
			To:     to,
			Policy: pipeline.RouteByEvent,
			Label:  string(av.EventEndOfStream),
		})
	}
}

func linkDecodeInput(graph pipeline.Graph, from pipeline.PadRef, to pipeline.PadRef, selector av.StreamSelector) error {
	policy, label := decodeInputRoute(selector)
	if err := graph.Route(pipeline.Route{
		From:   from,
		To:     []pipeline.PadRef{to},
		Policy: policy,
		Label:  label,
	}); err != nil {
		return err
	}
	if policy != pipeline.RouteByStream {
		return nil
	}
	return graph.Route(pipeline.Route{
		From:   from,
		To:     []pipeline.PadRef{to},
		Policy: pipeline.RouteByEvent,
		Label:  string(av.EventEndOfStream),
	})
}

func decodeInputRoute(selector av.StreamSelector) (pipeline.RoutePolicy, string) {
	if selector.ID != "" {
		return pipeline.RouteByStream, string(selector.ID)
	}
	return pipeline.RouteAll, ""
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
