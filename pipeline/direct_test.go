package pipeline

import (
	"context"
	"errors"
	"sync/atomic"
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

func TestGraphDirectPassThrough(t *testing.T) {
	ctx := context.Background()
	packet := av.Packet{StreamID: "audio"}
	msg := Message{Kind: MessagePacket, Packet: &packet}
	source := &directTestSource{name: "source", msg: &msg}
	stage := &directPassStage{name: "stage"}
	sink := &directTestSink{name: "sink"}

	graph, err := NewGraph(GraphConfig{Name: "test"})
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
	if err := graph.Connect(route("source", "stage")); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(route("stage", "sink")); err != nil {
		t.Fatal(err)
	}
	if err := graph.Run(ctx); err != nil {
		t.Fatal(err)
	}

	if stage.count != 1 || sink.count != 1 || sink.lastPacket != &packet {
		t.Fatalf("stage=%d sink=%d packet=%p", stage.count, sink.count, sink.lastPacket)
	}
	stats := graph.Stats()
	if stats.Messages != 2 || stats.Packets != 2 || stats.Delivered != 2 {
		t.Fatalf("stats = %+v", stats)
	}
	if got := stats.Nodes["source"]; got.OutPackets != 1 || got.InPackets != 0 {
		t.Fatalf("source stats = %+v", got)
	}
	if got := stats.Nodes["stage"]; got.InPackets != 1 || got.OutPackets != 1 {
		t.Fatalf("stage stats = %+v", got)
	}
	if got := stats.Nodes["sink"]; got.InPackets != 1 || got.OutPackets != 0 {
		t.Fatalf("sink stats = %+v", got)
	}
}

func TestGraphDirectSpec(t *testing.T) {
	packet := av.Packet{StreamID: "audio"}
	msg := Message{Kind: MessagePacket, Packet: &packet}
	source := &directTestSource{name: "source", msg: &msg}
	stage := &directPassStage{name: "stage", detail: "meter"}
	sink := &directTestSink{name: "sink"}

	graph, err := NewGraph(GraphConfig{Name: "spec", Realtime: true})
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
	if err := graph.Connect(route("source", "stage")); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(route("stage", "sink").ByStream("audio")); err != nil {
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

func TestGraphDirectFanoutSharesPayload(t *testing.T) {
	payload := []byte{1, 2, 3}
	packet := av.Packet{Payload: av.Buffer{Bytes: payload, Ownership: av.BufferBorrowed}}
	msg := Message{Kind: MessagePacket, Packet: &packet}
	source := &directTestSource{name: "source", msg: &msg}
	left := &directTestSink{name: "left"}
	right := &directTestSink{name: "right"}

	graph, err := NewGraph(GraphConfig{Name: "fanout"})
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
	if err := graph.Connect(route("source", "left", "right")); err != nil {
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

func TestGraphDirectRouteByStream(t *testing.T) {
	packet := av.Packet{StreamID: "video-main"}
	msg := Message{Kind: MessagePacket, Packet: &packet}
	source := &directTestSource{name: "source", msg: &msg}
	video := &directTestSink{name: "video"}
	audio := &directTestSink{name: "audio"}

	graph, err := NewGraph(GraphConfig{Name: "stream-route"})
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
	if err := graph.Connect(route("source", "video").ByStream("video-main")); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(route("source", "audio").ByStream("audio-main")); err != nil {
		t.Fatal(err)
	}
	if err := graph.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if video.count != 1 || audio.count != 0 {
		t.Fatalf("video=%d audio=%d", video.count, audio.count)
	}
}

func TestGraphDirectRouteByEvent(t *testing.T) {
	event := av.Event{Type: av.EventPacketLoss}
	msg := Message{Kind: MessageEvent, Event: &event}
	source := &directTestSource{name: "source", msg: &msg}
	loss := &directTestSink{name: "loss"}
	stats := &directTestSink{name: "stats"}

	graph, err := NewGraph(GraphConfig{Name: "event-route"})
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
	if err := graph.Connect(route("source", "loss").ByEvent(av.EventPacketLoss)); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(route("source", "stats").ByEvent(av.EventStats)); err != nil {
		t.Fatal(err)
	}
	if err := graph.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if loss.count != 1 || stats.count != 0 {
		t.Fatalf("loss=%d stats=%d", loss.count, stats.count)
	}
	graphStats := graph.Stats()
	if graphStats.Messages != 1 || graphStats.Events != 1 || graphStats.EventsByType[av.EventPacketLoss] != 1 ||
		graphStats.Delivered != 1 || !graphStats.LastEventPresent || graphStats.LastEvent.Type != av.EventPacketLoss {
		t.Fatalf("stats = %+v", graphStats)
	}
}

func TestGraphDirectRejectsBufferedPolicy(t *testing.T) {
	graph, err := NewGraph(GraphConfig{Name: "direct"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = graph.AddSource(&directTestSource{name: "source"}, BufferPolicy{Capacity: 1, Drop: DropOldest})
	if !errors.Is(err, ErrBufferedEdgesUnsupported) {
		t.Fatalf("err = %v, want ErrBufferedEdgesUnsupported", err)
	}
}

func TestGraphDirectRejectsAddAfterClose(t *testing.T) {
	graph, err := NewGraph(GraphConfig{Name: "direct"})
	if err != nil {
		t.Fatal(err)
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(&directTestSource{name: "source"}, BufferPolicy{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("AddSource err = %v, want ErrClosed", err)
	}
	if _, err := graph.AddStage(&directPassStage{name: "stage"}, BufferPolicy{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("AddStage err = %v, want ErrClosed", err)
	}
	if _, err := graph.AddSink(&directTestSink{name: "sink"}, BufferPolicy{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("AddSink err = %v, want ErrClosed", err)
	}
}

func TestGraphDirectEventBackpressure(t *testing.T) {
	source := &directEventSource{
		name: "source",
		events: []av.Event{
			{Type: av.EventPacketLoss},
			{Type: av.EventBackpressure},
		},
	}
	graph, err := NewGraph(GraphConfig{Name: "events", EventCapacity: 1})
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

func TestGraphDirectRunAllocs(t *testing.T) {
	ctx := context.Background()
	packet := av.Packet{StreamID: "audio"}
	msg := Message{Kind: MessagePacket, Packet: &packet}
	source := &directTestSource{name: "source", msg: &msg}
	stage := &directPassStage{name: "stage"}
	sink := &directTestSink{name: "sink"}

	graph, err := NewGraph(GraphConfig{Name: "alloc"})
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
	if err := graph.Connect(route("source", "stage")); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(route("stage", "sink")); err != nil {
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

// removableTestSink records deliveries that arrive after Close — the contract
// under test is that a live Remove drains in-flight deliveries before closing,
// so no Handle ever runs on a closed node.
type removableTestSink struct {
	name        string
	closed      atomic.Bool
	delivered   atomic.Int64
	afterClosed atomic.Int64
}

func (s *removableTestSink) Name() string {
	return s.name
}

func (s *removableTestSink) Handle(context.Context, *Message) error {
	if s.closed.Load() {
		s.afterClosed.Add(1)
	}
	s.delivered.Add(1)
	return nil
}

func (s *removableTestSink) Close() error {
	s.closed.Store(true)
	return nil
}

// TestGraphDirectRemoveDrainsInFlightDeliveries pins the live-detach contract:
// removing a node while a source goroutine is emitting never closes the node
// under an in-flight Handle, and deliveries racing the removal are shed.
func TestGraphDirectRemoveDrainsInFlightDeliveries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	graph, err := NewGraph(GraphConfig{Name: "live-remove"})
	if err != nil {
		t.Fatal(err)
	}
	defer graph.Close()

	packet := &av.Packet{StreamID: "live", Payload: av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable}}
	stop := make(chan struct{})
	source := &directLoopSource{name: "src", packet: packet, stop: stop}
	sourceRef, err := graph.AddSource(source, BufferPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	sink := &removableTestSink{name: "sink"}
	sinkRef, err := graph.AddSink(sink, BufferPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Route{From: sourceRef.String(), To: []string{sinkRef.String()}, Policy: RouteAll}); err != nil {
		t.Fatal(err)
	}

	runErr := make(chan error, 1)
	go func() { runErr <- graph.Run(ctx) }()
	for sink.delivered.Load() == 0 {
	}
	if err := graph.Remove(sinkRef); err != nil {
		t.Fatal(err)
	}
	if got := sink.afterClosed.Load(); got != 0 {
		t.Fatalf("deliveries after close = %d, want 0 (remove must drain in-flight handles)", got)
	}
	if !sink.closed.Load() {
		t.Fatal("removed sink was not closed")
	}
	close(stop)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
}

// directLoopSource emits the same packet in a tight loop until stop closes.
type directLoopSource struct {
	name   string
	packet *av.Packet
	stop   <-chan struct{}
	msg    Message
}

func (s *directLoopSource) Name() string {
	return s.name
}

func (s *directLoopSource) Start(ctx context.Context, emitter Emitter) error {
	for {
		select {
		case <-s.stop:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		s.msg.Kind = MessagePacket
		s.msg.Packet = s.packet
		s.msg.Frame = nil
		s.msg.Event = nil
		if err := emitter.Emit(ctx, &s.msg); err != nil {
			return err
		}
	}
}

func (s *directLoopSource) Close() error {
	return nil
}
