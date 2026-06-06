package goav

import (
	"context"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
)

func TestComponentFileRemuxFanout(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &remuxTestDemuxer{streams: streams}
	if err := demuxer.Open(ctx, format.Input{Name: "input.ogg"}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}

	sourcePacket := av.Packet{}
	source, err := format.NewDemuxSource(format.DemuxSourceConfig{
		Name:    "demux",
		Detail:  "component demux",
		Demuxer: demuxer,
		Result:  format.ReadResult{Packet: &sourcePacket, Events: make([]av.Event, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}

	archiveMuxer := &remuxTestMuxer{}
	if err := archiveMuxer.Open(ctx, format.Output{Name: "archive.ogg"}, streams, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	archive, err := format.NewMuxStage(format.MuxStageConfig{
		Name:   "archive",
		Detail: "component mux",
		Muxer:  archiveMuxer,
		Result: format.WriteResult{Events: make([]av.Event, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}

	previewMuxer := &remuxTestMuxer{}
	if err := previewMuxer.Open(ctx, format.Output{Name: "preview.ogg"}, streams, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	preview, err := format.NewMuxStage(format.MuxStageConfig{
		Name:   "preview",
		Detail: "component mux",
		Muxer:  previewMuxer,
		Result: format.WriteResult{Events: make([]av.Event, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}

	graph, err := pipeline.NewGraph(pipeline.GraphConfig{Name: "component-remux"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddStage(archive, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddStage(preview, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(pipeline.Route{
		From:   "demux",
		To:     []string{"archive", "preview"},
		Policy: pipeline.RouteAll,
	}); err != nil {
		t.Fatal(err)
	}

	spec := graph.Spec()
	if len(spec.Nodes) != 3 || len(spec.Edges) != 2 ||
		spec.Nodes[0].Detail != "component demux" ||
		spec.Nodes[1].Detail != "component mux" ||
		spec.Nodes[2].Detail != "component mux" {
		t.Fatalf("spec = %+v", spec)
	}

	if err := graph.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if archiveMuxer.writes != 1 || previewMuxer.writes != 1 ||
		!streamIDsEqual(archiveMuxer.writtenStreams, []av.StreamID{"audio"}) ||
		!streamIDsEqual(previewMuxer.writtenStreams, []av.StreamID{"audio"}) ||
		archiveMuxer.writtenPayloads[0] != 1 ||
		previewMuxer.writtenPayloads[0] != 1 {
		t.Fatalf("archive writes=%d streams=%+v payload=%+v preview writes=%d streams=%+v payload=%+v",
			archiveMuxer.writes, archiveMuxer.writtenStreams, archiveMuxer.writtenPayloads,
			previewMuxer.writes, previewMuxer.writtenStreams, previewMuxer.writtenPayloads)
	}

	stats := graph.Stats()
	if stats.Packets != 1 || stats.Events != 2 || stats.Delivered != 6 {
		t.Fatalf("stats = %+v", stats)
	}

	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
	if !demuxer.closed || !archiveMuxer.closed || !previewMuxer.closed {
		t.Fatalf("closed demux=%v archive=%v preview=%v", demuxer.closed, archiveMuxer.closed, previewMuxer.closed)
	}
}

func TestComponentCustomStageForwardsEvents(t *testing.T) {
	source := &componentEventSource{
		name: "events",
		events: []av.Event{
			{Type: av.EventStreamAdded, StreamID: "audio"},
			{Type: av.EventPacketLoss, StreamID: "audio"},
			{Type: av.EventEndOfStream, StreamID: "audio"},
		},
	}
	stage := &componentForwardStage{name: "forward"}
	sink := &componentEventSink{name: "sink"}

	graph, err := pipeline.NewGraph(pipeline.GraphConfig{Name: "component-custom-stage"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddStage(stage, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(pipeline.Route{From: "events", To: []string{"forward"}, Policy: pipeline.RouteAll}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(pipeline.Route{From: "forward", To: []string{"sink"}, Policy: pipeline.RouteAll}); err != nil {
		t.Fatal(err)
	}

	if err := graph.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if source.starts != 1 || stage.events != 3 || sink.events != 3 || sink.lastEvent != av.EventEndOfStream {
		t.Fatalf("starts=%d stage events=%d sink events=%d last=%s", source.starts, stage.events, sink.events, sink.lastEvent)
	}

	stats := graph.Stats()
	if stats.Events != 6 || stats.Delivered != 6 || stats.LastEvent.Type != av.EventEndOfStream {
		t.Fatalf("stats = %+v", stats)
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
	if !source.closed || !stage.closed || !sink.closed {
		t.Fatalf("closed source=%v stage=%v sink=%v", source.closed, stage.closed, sink.closed)
	}
}

type componentEventSource struct {
	name   string
	events []av.Event
	msg    pipeline.Message
	starts int
	closed bool
}

func (s *componentEventSource) Name() string {
	return s.name
}

func (s *componentEventSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	s.starts++
	for i := range s.events {
		s.msg.Kind = pipeline.MessageEvent
		s.msg.Packet = nil
		s.msg.Frame = nil
		s.msg.Event = &s.events[i]
		if err := emitter.Emit(ctx, &s.msg); err != nil {
			return err
		}
	}
	return nil
}

func (s *componentEventSource) Close() error {
	s.closed = true
	return nil
}

type componentForwardStage struct {
	name   string
	event  av.Event
	msg    pipeline.Message
	events int
	closed bool
}

func (s *componentForwardStage) Name() string {
	return s.name
}

func (s *componentForwardStage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	if msg == nil {
		return nil
	}
	if msg.Kind != pipeline.MessageEvent || msg.Event == nil {
		return emitter.Emit(ctx, msg)
	}
	s.events++
	s.event = *msg.Event
	s.msg.Kind = pipeline.MessageEvent
	s.msg.Packet = nil
	s.msg.Frame = nil
	s.msg.Event = &s.event
	return emitter.Emit(ctx, &s.msg)
}

func (s *componentForwardStage) Close() error {
	s.closed = true
	return nil
}

type componentEventSink struct {
	name      string
	events    int
	lastEvent av.EventType
	closed    bool
}

func (s *componentEventSink) Name() string {
	return s.name
}

func (s *componentEventSink) Handle(_ context.Context, msg *pipeline.Message) error {
	if msg != nil && msg.Kind == pipeline.MessageEvent && msg.Event != nil {
		s.events++
		s.lastEvent = msg.Event.Type
	}
	return nil
}

func (s *componentEventSink) Close() error {
	s.closed = true
	return nil
}
