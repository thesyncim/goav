package pipeline

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/internal/interleave"
)

var msBase = av.TimeBase{Num: 1, Den: 1000}

// timedSource emits a packet at each of its times (ms; negative means no
// time) from one reused scratch packet, the way a demux pump reuses its read
// result. ordered opts it into the time order.
type timedSource struct {
	name    string
	times   []int64
	ordered bool
}

func (s *timedSource) Name() string                 { return s.name }
func (s *timedSource) Close() error                 { return nil }
func (s *timedSource) ByTime(interleave.Token) bool { return s.ordered }

func (s *timedSource) Start(ctx context.Context, emitter Emitter) error {
	var packet av.Packet
	msg := Message{Kind: MessagePacket, Packet: &packet}
	for _, ms := range s.times {
		packet.StreamID = av.StreamID(fmt.Sprintf("%s@%d", s.name, ms))
		packet.PTS = av.Timestamp{}
		if ms >= 0 {
			packet.PTS = av.Timestamp{Value: ms, Base: msBase}
		}
		if err := emitter.Emit(ctx, &msg); err != nil {
			return err
		}
	}
	return nil
}

type orderSink struct {
	mu  sync.Mutex
	got []string
}

func (s *orderSink) Name() string { return "sink" }
func (s *orderSink) Close() error { return nil }
func (s *orderSink) Handle(_ context.Context, msg *Message) error {
	s.mu.Lock()
	s.got = append(s.got, string(msg.Packet.StreamID))
	s.mu.Unlock()
	return nil
}

func runTimedGraph(t *testing.T, realtime bool, stage Stage, sources ...*timedSource) []string {
	t.Helper()
	graph, err := NewGraph(GraphConfig{Name: "order", Realtime: realtime})
	if err != nil {
		t.Fatal(err)
	}
	sink := &orderSink{}
	target := "sink"
	if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if stage != nil {
		if _, err := graph.AddStage(stage, BufferPolicy{}); err != nil {
			t.Fatal(err)
		}
		if err := graph.Connect(Route{From: stage.Name(), To: []string{"sink"}}); err != nil {
			t.Fatal(err)
		}
		target = stage.Name()
	}
	for _, source := range sources {
		if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
			t.Fatal(err)
		}
		if err := graph.Connect(Route{From: source.name, To: []string{target}}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := graph.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	return sink.got
}

// TestDirectRunInterleavesTimedSourcesByTime pins the offline scheduler:
// time-ordered sources deliver earliest packet first, ties to the source added
// first, the same on every run. Each source runs up to its first timed packet
// before any is granted, so c's untimed packet leads.
func TestDirectRunInterleavesTimedSourcesByTime(t *testing.T) {
	want := []string{"c@-1", "a@0", "b@0", "c@5", "a@10", "b@15", "a@20", "c@20", "b@30", "c@35"}
	for range 50 {
		got := runTimedGraph(t, false, nil,
			&timedSource{name: "a", times: []int64{0, 10, 20}, ordered: true},
			&timedSource{name: "b", times: []int64{0, 15, 30}, ordered: true},
			&timedSource{name: "c", times: []int64{-1, 5, 20, 35}, ordered: true},
		)
		if !slices.Equal(got, want) {
			t.Fatalf("delivery order = %v, want %v", got, want)
		}
	}
}

// TestDirectRunLeavesOtherSourcesFree pins what stays concurrent: a realtime
// graph, a source that does not opt in, and a graph with a converging stage
// that may hold one upstream back. Each must still finish and deliver all.
func TestDirectRunLeavesOtherSourcesFree(t *testing.T) {
	sources := func(bOrdered bool) []*timedSource {
		return []*timedSource{
			{name: "a", times: []int64{0, 10, 20}, ordered: true},
			{name: "b", times: []int64{0, 15, 30}, ordered: bOrdered},
			{name: "c", times: []int64{5, 25}, ordered: true},
		}
	}
	for _, tc := range []struct {
		name     string
		realtime bool
		stage    Stage
		sources  []*timedSource
		// orderedOf lists the sources still interleaved by time.
		orderedOf []string
	}{
		{"realtime graph", true, nil, sources(true), nil},
		{"one source opts out", false, nil, sources(false), []string{"a", "c"}},
		{"converging stage", false, &admitPassStage{}, sources(true), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := runTimedGraph(t, tc.realtime, tc.stage, tc.sources...)
			if len(got) != 8 {
				t.Fatalf("delivered %v, want all 8 packets", got)
			}
			if tc.orderedOf != nil {
				var ordered []string
				for _, label := range got {
					if label[0] != 'b' {
						ordered = append(ordered, label)
					}
				}
				if want := []string{"a@0", "c@5", "a@10", "a@20", "c@25"}; !slices.Equal(ordered, want) {
					t.Fatalf("ordered sources delivered %v, want %v", ordered, want)
				}
			}
		})
	}
}

// TestDirectRunTimeOrderStopsOnFailure pins that a failing sink ends an
// ordered run promptly: no source stays parked waiting for its turn.
func TestDirectRunTimeOrderStopsOnFailure(t *testing.T) {
	graph, err := NewGraph(GraphConfig{Name: "order"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(&tenthFailSink{name: "sink"}, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	long := make([]int64, 1000)
	for i := range long {
		long[i] = int64(i)
	}
	for _, name := range []string{"a", "b", "c"} {
		if _, err := graph.AddSource(&timedSource{name: name, times: long, ordered: true}, BufferPolicy{}); err != nil {
			t.Fatal(err)
		}
		if err := graph.Connect(Route{From: name, To: []string{"sink"}}); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() { done <- graph.Run(context.Background()) }()
	select {
	case err := <-done:
		if err == nil || !errors.Is(err, errTenthHandle) {
			t.Fatalf("run = %v, want the sink's error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ordered run did not stop after its sink failed")
	}
}

// admitPassStage passes messages on and admits everything: its Admitter
// capability alone takes the graph out of the time order.
type admitPassStage struct{}

func (*admitPassStage) Name() string                          { return "join" }
func (*admitPassStage) Close() error                          { return nil }
func (*admitPassStage) Admit(context.Context, *Message) error { return nil }
func (*admitPassStage) Handle(ctx context.Context, msg *Message, emitter Emitter) error {
	return emitter.Emit(ctx, msg)
}
