package pipeline

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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

// relayStage passes messages on from its own worker on a buffered graph.
type relayStage struct{ name string }

func (s *relayStage) Name() string { return s.name }
func (s *relayStage) Close() error { return nil }
func (s *relayStage) Handle(ctx context.Context, msg *Message, emitter Emitter) error {
	return emitter.Emit(ctx, msg)
}

// TestBufferedRunInterleavesTimedSourcesByTime pins the order on the
// buffered runner: each source reaches the sink through its own worker, and
// the sink still sees packets earliest first on every run, because the turn
// moves only once the previous packet's work is done.
func TestBufferedRunInterleavesTimedSourcesByTime(t *testing.T) {
	want := []string{"c@-1", "a@0", "b@0", "c@5", "a@10", "b@15", "a@20", "c@20", "b@30", "c@35"}
	for range 50 {
		graph, err := NewGraph(GraphConfig{Name: "order", Buffer: BufferPolicy{Capacity: 4, Drop: DropBlock}})
		if err != nil {
			t.Fatal(err)
		}
		sink := &orderSink{}
		if _, err := graph.AddSink(sink, BufferPolicy{Capacity: 4, Drop: DropBlock}); err != nil {
			t.Fatal(err)
		}
		for _, source := range []*timedSource{
			{name: "a", times: []int64{0, 10, 20}, ordered: true},
			{name: "b", times: []int64{0, 15, 30}, ordered: true},
			{name: "c", times: []int64{-1, 5, 20, 35}, ordered: true},
		} {
			relay := &relayStage{name: source.name + "-relay"}
			for _, err := range []error{
				func() error {
					_, err := graph.AddSource(source, BufferPolicy{Capacity: 4, Drop: DropBlock})
					return err
				}(),
				func() error { _, err := graph.AddStage(relay, BufferPolicy{Capacity: 4, Drop: DropBlock}); return err }(),
				graph.Connect(Route{From: source.name, To: []string{relay.name}}),
				graph.Connect(Route{From: relay.name, To: []string{"sink"}}),
			} {
				if err != nil {
					t.Fatal(err)
				}
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = graph.Run(ctx)
		cancel()
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		if !slices.Equal(sink.got, want) {
			t.Fatalf("delivery order = %v, want %v", sink.got, want)
		}
	}
}

// pairGate is a toy join: it pairs one packet of arm a with one of arm b
// and holds an arm back (Admit) once it leads by lead packets. Admits makes
// it an interleave.Gate.
type pairGate struct {
	name     string
	lead     int64
	depth    [2]atomic.Int64
	wake     [2]chan struct{}
	pending  [2][]string
	maxDepth int64
}

func newPairGate(name string, lead int64) *pairGate {
	return &pairGate{name: name, lead: lead, wake: [2]chan struct{}{make(chan struct{}, 1), make(chan struct{}, 1)}}
}

func pairArm(msg *Message) int {
	if msg.Packet != nil && strings.HasPrefix(string(msg.Packet.StreamID), "b") {
		return 1
	}
	return 0
}

func (g *pairGate) Name() string { return g.name }
func (g *pairGate) Close() error { return nil }
func (g *pairGate) Admits(_ interleave.Token, m any) bool {
	msg := m.(*Message)
	return msg.Kind != MessagePacket || g.depth[pairArm(msg)].Load() < g.lead
}
func (g *pairGate) Admit(ctx context.Context, msg *Message) error {
	for !g.Admits(interleave.Token{}, msg) {
		select {
		case <-g.wake[pairArm(msg)]:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
func (g *pairGate) Handle(ctx context.Context, msg *Message, emitter Emitter) error {
	arm := pairArm(msg)
	// Record the arrival, then the pairs it completes: the arrival order is
	// what a free run would leave to the scheduler.
	var arrival av.Packet
	arrival.StreamID = "+" + msg.Packet.StreamID
	if err := emitter.Emit(ctx, &Message{Kind: MessagePacket, Packet: &arrival}); err != nil {
		return err
	}
	g.pending[arm] = append(g.pending[arm], string(msg.Packet.StreamID))
	g.maxDepth = max(g.maxDepth, int64(len(g.pending[arm])))
	for len(g.pending[0]) != 0 && len(g.pending[1]) != 0 {
		for i := range g.pending {
			var packet av.Packet
			packet.StreamID = av.StreamID(g.pending[i][0])
			g.pending[i] = g.pending[i][1:]
			if err := emitter.Emit(ctx, &Message{Kind: MessagePacket, Packet: &packet}); err != nil {
				return err
			}
		}
	}
	for i := range g.pending {
		if g.depth[i].Swap(int64(len(g.pending[i]))) >= g.lead && int64(len(g.pending[i])) < g.lead {
			select {
			case g.wake[i] <- struct{}{}:
			default:
			}
		}
	}
	return nil
}

// TestDirectRunParksASourceItsJoinHoldsBack pins the order through a join:
// b's packets are all later than a's, so a runs first until the join holds
// its arm back; a then gives up the turn, b runs, and a resumes as soon as
// the join admits it again. Without that handoff a would wait in Admit
// holding the turn b needs. Arrivals and pairs come out the same on every
// run, and no arm ever leads by more than the join allows.
func TestDirectRunParksASourceItsJoinHoldsBack(t *testing.T) {
	var first []string
	a := &timedSource{name: "a", ordered: true}
	b := &timedSource{name: "b", ordered: true}
	for i := range int64(20) {
		a.times = append(a.times, i)
		b.times = append(b.times, 1000+i)
	}
	for range 50 {
		gate := newPairGate("join", 2)
		got := runTimedGraph(t, false, gate, a, b)
		if len(got) != 80 || !slices.Equal(got[:3], []string{"+a@0", "+a@1", "+b@1000"}) {
			t.Fatalf("delivered %v, want 40 arrivals and 40 paired packets, a held after two", got)
		}
		if first == nil {
			first = got
		} else if !slices.Equal(got, first) {
			t.Fatalf("delivery order = %v, want %v as on the first run", got, first)
		}
		if gate.maxDepth > gate.lead+1 {
			t.Fatalf("an arm led the join by %d packets, want at most %d", gate.maxDepth, gate.lead+1)
		}
	}
}

// TestTimeOrderCoversOnlyGatedJoins pins which graphs with converging stages
// keep the order: every source must take part and every converging stage
// must be a Gate fed through single-producer nodes. A nested join, a plain
// Admitter, or a source outside the order runs them all freely.
func TestTimeOrderCoversOnlyGatedJoins(t *testing.T) {
	type shape struct {
		name     string
		nested   bool
		plain    bool
		optOut   bool
		buffered bool
		want     bool
	}
	for _, tc := range []shape{
		{name: "gated join", want: true},
		{name: "nested gated joins", nested: true},
		{name: "plain admitter", plain: true},
		{name: "source outside the order", optOut: true},
		{name: "buffered graph", buffered: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := GraphConfig{Name: "order"}
			if tc.buffered {
				config.Buffer = BufferPolicy{Capacity: 4, Drop: DropBlock}
			}
			graph, err := NewGraph(config)
			if err != nil {
				t.Fatal(err)
			}
			add := func(err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
			}
			_, err = graph.AddSink(&orderSink{}, config.Buffer)
			add(err)
			var join Stage = newPairGate("join", 2)
			if tc.plain {
				join = &admitPassStage{}
			}
			_, err = graph.AddStage(join, config.Buffer)
			add(err)
			add(graph.Connect(Route{From: "join", To: []string{"sink"}}))
			into := "join"
			if tc.nested {
				_, err = graph.AddStage(newPairGate("inner", 2), config.Buffer)
				add(err)
				add(graph.Connect(Route{From: "inner", To: []string{"join"}}))
				into = "inner"
			}
			var sources []Source
			for _, name := range []string{"a", "b", "c"} {
				source := &timedSource{name: name, ordered: !(tc.optOut && name == "c")}
				sources = append(sources, source)
				_, err = graph.AddSource(source, config.Buffer)
				add(err)
				to := into
				if name == "c" {
					to = "join"
				}
				add(graph.Connect(Route{From: name, To: []string{to}}))
			}
			var ordered []bool
			switch g := graph.(type) {
			case *directRunner:
				ordered = timeOrderedSources(false, false, g.snap.Load(), sources, func(s Source) Source { return s })
			case *bufferedRunner:
				ordered = timeOrderedSources(false, true, g.snap.Load(), sources, func(s Source) Source { return s })
			}
			if got := ordered != nil; got != tc.want {
				t.Fatalf("ordered = %v, want %v", ordered, tc.want)
			}
		})
	}
}
