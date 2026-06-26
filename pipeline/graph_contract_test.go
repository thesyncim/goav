package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
)

func TestGraphConfigCloseWaitTimeoutDefault(t *testing.T) {
	directGraph, err := NewGraph(GraphConfig{Name: "close-wait-default"})
	if err != nil {
		t.Fatal(err)
	}
	direct, ok := directGraph.(*directRunner)
	if !ok {
		t.Fatalf("direct graph = %T, want *directRunner", directGraph)
	}
	if got := direct.config.CloseWaitTimeout; got != defaultCloseWaitTimeout {
		t.Fatalf("direct close wait timeout = %s, want %s", got, defaultCloseWaitTimeout)
	}

	const custom = 5 * time.Millisecond
	bufferedGraph, err := NewGraph(GraphConfig{Name: "close-wait-custom", Buffer: BufferPolicy{Capacity: 1}, CloseWaitTimeout: custom})
	if err != nil {
		t.Fatal(err)
	}
	buffered, ok := bufferedGraph.(*bufferedRunner)
	if !ok {
		t.Fatalf("buffered graph = %T, want *bufferedRunner", bufferedGraph)
	}
	if got := buffered.config.CloseWaitTimeout; got != custom {
		t.Fatalf("buffered close wait timeout = %s, want %s", got, custom)
	}
}

type describedSource struct {
	directTestSource
	detail string
}

func (s *describedSource) DescribeNode() NodeSpec {
	return NodeSpec{Name: s.name, Kind: NodeSource, Detail: s.detail}
}

type describedSink struct {
	directTestSink
	detail string
}

func (s *describedSink) DescribeNode() NodeSpec {
	return NodeSpec{Name: s.name, Kind: NodeSink, Detail: s.detail}
}

func TestGraphDirectEventsChannelAndClose(t *testing.T) {
	source := &directEventSource{
		name:   "source",
		events: []av.Event{{Type: av.EventStats, StreamID: "video"}},
	}
	graph, err := NewGraph(GraphConfig{Name: "events", EventCapacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	select {
	case event := <-graph.Events():
		if event.Type != av.EventStats || event.StreamID != "video" {
			t.Fatalf("event = %+v", event)
		}
	default:
		t.Fatal("event was not published")
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-graph.Events(); ok {
		t.Fatal("events channel stayed open after Close")
	}
	if err := graph.Close(); err != nil {
		t.Fatalf("second close err = %v", err)
	}
}

func TestGraphCloseContextHonorsCanceledContext(t *testing.T) {
	for _, tc := range []struct {
		name   string
		buffer BufferPolicy
	}{
		{name: "direct"},
		{name: "buffered", buffer: BufferPolicy{Capacity: 1, Drop: DropNewest}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			graph, err := NewGraph(GraphConfig{Name: tc.name, Buffer: tc.buffer})
			if err != nil {
				t.Fatal(err)
			}
			closer, ok := graph.(interface {
				CloseContext(context.Context) error
			})
			if !ok {
				t.Fatalf("graph = %T, want CloseContext", graph)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := closer.CloseContext(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("CloseContext(canceled) error = %v, want context.Canceled", err)
			}
			if err := graph.Close(); err != nil {
				t.Fatalf("Close after canceled CloseContext: %v", err)
			}
		})
	}
}

func TestGraphDirectDisconnectPauseAndRemoveContracts(t *testing.T) {
	packet := av.Packet{StreamID: "video"}
	message := Message{Kind: MessagePacket, Packet: &packet}
	source := &directTestSource{name: "source", msg: &message}
	left := &directTestSink{name: "left"}
	right := &directTestSink{name: "right"}

	graph, err := NewGraph(GraphConfig{Name: "direct-contracts"})
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

	pauser := graph.(NodePauser)
	if err := pauser.SetNodePaused("right", true); err != nil {
		t.Fatal(err)
	}
	if err := graph.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if left.count != 1 || right.count != 0 {
		t.Fatalf("paused delivery left=%d right=%d", left.count, right.count)
	}

	if err := pauser.SetNodePaused("right", false); err != nil {
		t.Fatal(err)
	}
	if err := graph.Disconnect(route("source", "left")); err != nil {
		t.Fatal(err)
	}
	if err := graph.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if left.count != 1 || right.count != 1 {
		t.Fatalf("after disconnect left=%d right=%d", left.count, right.count)
	}

	if err := graph.Disconnect(route("source", "right")); err != nil {
		t.Fatal(err)
	}
	if edges := graph.Spec().Edges; len(edges) != 0 {
		t.Fatalf("edges after disconnect = %+v", edges)
	}

	for _, tc := range []struct {
		name string
		run  func() error
		want error
	}{
		{name: "empty target", run: func() error { return graph.Disconnect(Route{From: "source"}) }, want: ErrInvalidLink},
		{name: "unsupported route", run: func() error {
			return graph.Disconnect(Route{From: "source", To: []string{"right"}, Policy: RoutePolicy("later")})
		}, want: ErrUnsupportedRoute},
		{name: "unknown source", run: func() error { return graph.Disconnect(route("missing", "right")) }, want: ErrUnknownNode},
		{name: "unknown target", run: func() error { return graph.Disconnect(route("source", "missing")) }, want: ErrUnknownNode},
		{name: "missing route", run: func() error { return graph.Disconnect(route("source", "right")) }, want: ErrInvalidLink},
		{name: "unknown pause", run: func() error { return pauser.SetNodePaused("missing", true) }, want: ErrUnknownNode},
		{name: "remove source", run: func() error { return graph.Remove("source") }, want: ErrInvalidLink},
		{name: "remove missing", run: func() error { return graph.Remove("missing") }, want: ErrUnknownNode},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}

	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
	if err := graph.Remove("right"); !errors.Is(err, ErrClosed) {
		t.Fatalf("remove after close err = %v, want %v", err, ErrClosed)
	}
}

func TestRouteMatchingCoversFrameAndEventStreams(t *testing.T) {
	streamRoute := directRoute{policy: RouteByStream, label: "video"}
	packet := av.Packet{StreamID: "video"}
	frame := av.Frame{StreamID: "video"}
	event := av.Event{StreamID: "video", Type: av.EventStats}
	if !streamRoute.matches(&Message{Kind: MessagePacket, Packet: &packet}) ||
		!streamRoute.matches(&Message{Kind: MessageFrame, Frame: &frame}) ||
		!streamRoute.matches(&Message{Kind: MessageEvent, Event: &event}) {
		t.Fatal("stream route did not match packet, frame, and event stream ids")
	}
	if streamRoute.matches(&Message{Kind: MessageFrame}) ||
		streamRoute.matches(&Message{Kind: MessageEvent}) ||
		streamRoute.matches(&Message{}) {
		t.Fatal("stream route matched empty payloads")
	}

	allStreams := directRoute{policy: RouteByStream}
	if !allStreams.matches(&Message{}) {
		t.Fatal("empty stream label should match all messages")
	}
	eventRoute := directRoute{policy: RouteByEvent, label: string(av.EventStats)}
	if !eventRoute.matches(&Message{Kind: MessageEvent, Event: &event}) ||
		eventRoute.matches(&Message{Kind: MessagePacket, Packet: &packet}) ||
		eventRoute.matches(&Message{Kind: MessageEvent}) {
		t.Fatal("event route matching changed")
	}
	unknownRoute := directRoute{policy: RoutePolicy("future")}
	if unknownRoute.matches(&Message{Kind: MessagePacket, Packet: &packet}) {
		t.Fatal("unsupported route policy matched")
	}
}

func TestGraphSpecsUseNodeDescribersForDirectAndBuffered(t *testing.T) {
	packet := av.Packet{StreamID: "video"}
	message := Message{Kind: MessagePacket, Packet: &packet}
	for _, tc := range []struct {
		name   string
		buffer BufferPolicy
	}{
		{name: "direct"},
		{name: "buffered", buffer: BufferPolicy{Capacity: 1, Drop: DropNewest}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			graph, err := NewGraph(GraphConfig{Name: tc.name, Realtime: true, Buffer: tc.buffer})
			if err != nil {
				t.Fatal(err)
			}
			source := &describedSource{directTestSource: directTestSource{name: "source", msg: &message}, detail: "fixture source"}
			stage := &directPassStage{name: "stage", detail: "meter"}
			sink := &describedSink{directTestSink: directTestSink{name: "sink"}, detail: "fixture sink"}
			if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
				t.Fatal(err)
			}
			if _, err := graph.AddStage(stage, BufferPolicy{}); err != nil {
				t.Fatal(err)
			}
			if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
				t.Fatal(err)
			}
			if err := graph.Connect(route("source", "stage").ByStream("video")); err != nil {
				t.Fatal(err)
			}
			if err := graph.Connect(route("stage", "sink")); err != nil {
				t.Fatal(err)
			}
			spec := graph.Spec()
			if spec.Name != tc.name || !spec.Realtime || len(spec.Nodes) != 3 || len(spec.Edges) != 2 {
				t.Fatalf("spec = %+v", spec)
			}
			if spec.Nodes[0].Detail != "fixture source" ||
				spec.Nodes[1].Detail != "meter" ||
				spec.Nodes[2].Detail != "fixture sink" {
				t.Fatalf("node details = %+v", spec.Nodes)
			}
			if spec.Edges[0].Policy != RouteByStream || spec.Edges[0].Label != "video" {
				t.Fatalf("edge = %+v", spec.Edges[0])
			}
			if graph.Events() == nil {
				t.Fatal("events channel is nil")
			}
			if err := graph.Disconnect(route("stage", "sink")); err != nil {
				t.Fatal(err)
			}
			if edges := graph.Spec().Edges; len(edges) != 1 {
				t.Fatalf("edges after buffered/direct disconnect = %+v", edges)
			}
			if err := graph.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
