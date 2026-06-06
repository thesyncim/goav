package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
)

type directTestSource struct {
	name string
	msg  *Message
}

func (s *directTestSource) Name() string {
	return s.name
}

func (s *directTestSource) Start(ctx context.Context, emitter Emitter) error {
	return emitter.Emit(ctx, s.msg)
}

func (s *directTestSource) Close() error {
	return nil
}

type directEventSource struct {
	name   string
	events []av.Event
	msg    Message
}

func (s *directEventSource) Name() string {
	return s.name
}

func (s *directEventSource) Start(ctx context.Context, emitter Emitter) error {
	for i := range s.events {
		s.msg.Kind = MessageEvent
		s.msg.Event = &s.events[i]
		if err := emitter.Emit(ctx, &s.msg); err != nil {
			return err
		}
	}
	return nil
}

func (s *directEventSource) Close() error {
	return nil
}

type directPassStage struct {
	name   string
	detail string
	count  int
}

func (s *directPassStage) Name() string {
	return s.name
}

func (s *directPassStage) DescribeNode() NodeSpec {
	return NodeSpec{Name: s.name, Kind: NodeStage, Detail: s.detail}
}

func (s *directPassStage) Handle(ctx context.Context, msg *Message, emitter Emitter) error {
	s.count++
	return emitter.Emit(ctx, msg)
}

func (s *directPassStage) Close() error {
	return nil
}

type directTestSink struct {
	name       string
	count      int
	lastPacket *av.Packet
}

func (s *directTestSink) Name() string {
	return s.name
}

func (s *directTestSink) Handle(_ context.Context, msg *Message) error {
	s.count++
	if msg.Kind == MessagePacket {
		s.lastPacket = msg.Packet
	}
	return nil
}

func (s *directTestSink) Close() error {
	return nil
}

func TestDirectGraphPassThrough(t *testing.T) {
	ctx := context.Background()
	packet := av.Packet{StreamID: "audio"}
	msg := Message{Kind: MessagePacket, Packet: &packet}
	source := &directTestSource{name: "source", msg: &msg}
	stage := &directPassStage{name: "stage"}
	sink := &directTestSink{name: "sink"}

	graph, err := NewDirectGraph(GraphConfig{Name: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddStage(stage, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Connect("source", "stage")); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Connect("stage", "sink")); err != nil {
		t.Fatal(err)
	}
	if err := graph.Run(ctx); err != nil {
		t.Fatal(err)
	}

	if stage.count != 1 || sink.count != 1 || sink.lastPacket != &packet {
		t.Fatalf("stage=%d sink=%d packet=%p", stage.count, sink.count, sink.lastPacket)
	}
}

func TestDirectGraphSpec(t *testing.T) {
	packet := av.Packet{StreamID: "audio"}
	msg := Message{Kind: MessagePacket, Packet: &packet}
	source := &directTestSource{name: "source", msg: &msg}
	stage := &directPassStage{name: "stage", detail: "meter"}
	sink := &directTestSink{name: "sink"}

	graph, err := NewDirectGraph(GraphConfig{Name: "spec", Realtime: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddStage(stage, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Connect("source", "stage")); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Connect("stage", "sink").ByStream("audio")); err != nil {
		t.Fatal(err)
	}

	spec := graph.Spec()
	if spec.Name != "spec" || !spec.Realtime {
		t.Fatalf("spec metadata = %+v", spec)
	}
	if len(spec.Nodes) != 3 || len(spec.Edges) != 2 {
		t.Fatalf("nodes=%d edges=%d", len(spec.Nodes), len(spec.Edges))
	}
	if spec.Nodes[0].Kind != NodeSource || spec.Nodes[1].Kind != NodeStage || spec.Nodes[2].Kind != NodeSink {
		t.Fatalf("nodes = %+v", spec.Nodes)
	}
	if spec.Nodes[1].Detail != "meter" {
		t.Fatalf("node detail = %q", spec.Nodes[1].Detail)
	}
	if spec.Edges[1].Policy != RouteByStream || spec.Edges[1].Label != "audio" {
		t.Fatalf("edge = %+v", spec.Edges[1])
	}
}

func TestDirectGraphFanoutSharesPayload(t *testing.T) {
	payload := []byte{1, 2, 3}
	packet := av.Packet{Payload: av.Buffer{Bytes: payload, Ownership: av.BufferBorrowed}}
	msg := Message{Kind: MessagePacket, Packet: &packet}
	source := &directTestSource{name: "source", msg: &msg}
	left := &directTestSink{name: "left"}
	right := &directTestSink{name: "right"}

	graph, err := NewDirectGraph(GraphConfig{Name: "fanout"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(left, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(right, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Connect("source", "left", "right")); err != nil {
		t.Fatal(err)
	}
	if err := graph.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	if left.lastPacket != &packet || right.lastPacket != &packet {
		t.Fatalf("fanout copied packet pointers: left=%p right=%p packet=%p", left.lastPacket, right.lastPacket, &packet)
	}
	if &left.lastPacket.Payload.Bytes[0] != &right.lastPacket.Payload.Bytes[0] {
		t.Fatal("fanout copied payload bytes")
	}
}

func TestDirectGraphRouteByStream(t *testing.T) {
	packet := av.Packet{StreamID: "video-main"}
	msg := Message{Kind: MessagePacket, Packet: &packet}
	source := &directTestSource{name: "source", msg: &msg}
	video := &directTestSink{name: "video"}
	audio := &directTestSink{name: "audio"}

	graph, err := NewDirectGraph(GraphConfig{Name: "stream-route"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(video, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(audio, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Connect("source", "video").ByStream("video-main")); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Connect("source", "audio").ByStream("audio-main")); err != nil {
		t.Fatal(err)
	}
	if err := graph.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if video.count != 1 || audio.count != 0 {
		t.Fatalf("video=%d audio=%d", video.count, audio.count)
	}
}

func TestDirectGraphRouteByEvent(t *testing.T) {
	event := av.Event{Type: av.EventPacketLoss}
	msg := Message{Kind: MessageEvent, Event: &event}
	source := &directTestSource{name: "source", msg: &msg}
	loss := &directTestSink{name: "loss"}
	stats := &directTestSink{name: "stats"}

	graph, err := NewDirectGraph(GraphConfig{Name: "event-route"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(loss, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(stats, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Connect("source", "loss").ByEvent(av.EventPacketLoss)); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Connect("source", "stats").ByEvent(av.EventStats)); err != nil {
		t.Fatal(err)
	}
	if err := graph.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if loss.count != 1 || stats.count != 0 {
		t.Fatalf("loss=%d stats=%d", loss.count, stats.count)
	}
}

func TestDirectGraphRejectsBufferedPolicy(t *testing.T) {
	graph, err := NewDirectGraph(GraphConfig{Name: "direct"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = graph.AddSource(&directTestSource{name: "source"}, BufferPolicy{Capacity: 1, Drop: DropOldest})
	if !errors.Is(err, ErrBufferedEdgesUnsupported) {
		t.Fatalf("err = %v, want ErrBufferedEdgesUnsupported", err)
	}
}

func TestDirectGraphEventBackpressure(t *testing.T) {
	source := &directEventSource{
		name: "source",
		events: []av.Event{
			{Type: av.EventPacketLoss},
			{Type: av.EventBackpressure},
		},
	}
	graph, err := NewDirectGraph(GraphConfig{Name: "events", EventCapacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}

	if err := graph.Run(context.Background()); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("err = %v, want ErrBackpressure", err)
	}
}

func TestDirectGraphRunAllocs(t *testing.T) {
	ctx := context.Background()
	packet := av.Packet{StreamID: "audio"}
	msg := Message{Kind: MessagePacket, Packet: &packet}
	source := &directTestSource{name: "source", msg: &msg}
	stage := &directPassStage{name: "stage"}
	sink := &directTestSink{name: "sink"}

	graph, err := NewDirectGraph(GraphConfig{Name: "alloc"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddStage(stage, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Connect("source", "stage")); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Connect("stage", "sink")); err != nil {
		t.Fatal(err)
	}

	if allocs := testing.AllocsPerRun(1000, func() {
		if err := graph.Run(ctx); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("direct graph run allocs = %v, want 0", allocs)
	}
}
