package pipeline

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
)

type faultSource struct {
	start func(context.Context, Emitter) error
}

func (*faultSource) Name() string                                 { return "source" }
func (s *faultSource) Start(ctx context.Context, e Emitter) error { return s.start(ctx, e) }
func (*faultSource) Close() error                                 { return nil }

type faultSink struct {
	name     string
	handle   func(context.Context, *Message) error
	closes   atomic.Int64
	closeErr error
}

func (s *faultSink) Name() string                                 { return s.name }
func (s *faultSink) Handle(ctx context.Context, m *Message) error { return s.handle(ctx, m) }
func (s *faultSink) Close() error                                 { s.closes.Add(1); return s.closeErr }

type faultStage struct{ name string }

func (s faultStage) Name() string                                            { return s.name }
func (s faultStage) Handle(ctx context.Context, m *Message, e Emitter) error { return e.Emit(ctx, m) }
func (faultStage) Close() error                                              { return nil }

func TestFaultDomainRetiresAndKeepsHealthySibling(t *testing.T) {
	for _, buffer := range []BufferPolicy{{}, {Capacity: 2, Drop: DropBlock}} {
		t.Run(string(buffer.Drop), func(t *testing.T) {
			g, _ := NewGraph(GraphConfig{Buffer: buffer, CloseWaitTimeout: time.Second})
			cause := errors.New("optional handler")
			bad := &faultSink{name: "bad", handle: func(context.Context, *Message) error { return cause }}
			var delivered atomic.Int64
			good := &faultSink{name: "good", handle: func(context.Context, *Message) error { delivered.Add(1); return nil }}
			src, _ := g.AddSource(&faultSource{func(ctx context.Context, e Emitter) error {
				m := Message{Kind: MessagePacket, Packet: &av.Packet{}}
				for range 64 {
					if err := e.Emit(ctx, &m); err != nil {
						return err
					}
				}
				return nil
			}}, buffer)
			b, _ := g.AddSink(bad, buffer)
			h, _ := g.AddSink(good, buffer)
			_ = g.Connect(Route{From: string(src), To: []string{string(b), string(h)}})
			domain, err := g.(FaultIsolator).IsolateFaults("optional", []NodeRef{b})
			if err != nil {
				t.Fatal(err)
			}
			if err := g.Run(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			s := domain.Snapshot()
			if !errors.Is(s.Err, cause) || !s.Retired || s.PendingCleanup != 0 || bad.closes.Load() != 1 || delivered.Load() != 64 {
				t.Fatalf("domain=%+v closes=%d healthy=%d", s, bad.closes.Load(), delivered.Load())
			}
		})
	}
}

func TestFaultDomainCancelsBlockedPeerAndWakesParkedProducer(t *testing.T) {
	g, _ := NewGraph(GraphConfig{Buffer: BufferPolicy{Capacity: 1, Drop: DropBlock}, CloseWaitTimeout: time.Second})
	entered := make(chan struct{})
	cause := errors.New("peer failed")
	blocked := &faultSink{name: "blocked", handle: func(ctx context.Context, _ *Message) error { close(entered); <-ctx.Done(); return ctx.Err() }}
	failed := &faultSink{name: "failed", handle: func(context.Context, *Message) error { <-entered; return cause }}
	var delivered atomic.Int64
	good := &faultSink{name: "good", handle: func(context.Context, *Message) error { delivered.Add(1); return nil }}
	src, _ := g.AddSource(&faultSource{func(ctx context.Context, e Emitter) error {
		m := Message{Kind: MessagePacket, Packet: &av.Packet{}}
		for range 128 {
			if err := e.Emit(ctx, &m); err != nil {
				return err
			}
		}
		return nil
	}}, BufferPolicy{})
	entry, _ := g.AddStage(faultStage{name: "entry"}, BufferPolicy{})
	b, _ := g.AddSink(blocked, BufferPolicy{})
	f, _ := g.AddSink(failed, BufferPolicy{})
	h, _ := g.AddSink(good, BufferPolicy{})
	_ = g.Connect(Route{From: string(src), To: []string{string(entry), string(h)}})
	_ = g.Connect(Route{From: string(entry), To: []string{string(b), string(f)}})
	d, err := g.(FaultIsolator).IsolateFaults("optional", []NodeRef{entry, b, f})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := g.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if delivered.Load() != 128 || !d.Snapshot().Retired {
		t.Fatalf("healthy=%d domain=%+v", delivered.Load(), d.Snapshot())
	}
}

func TestFaultDomainPreservesExplicitDeliveryContext(t *testing.T) {
	g, _ := NewGraph(GraphConfig{})
	type key struct{}
	parent := context.WithValue(t.Context(), key{}, "value")
	delivery, cancel := context.WithCancel(parent)
	defer cancel()
	seen := make(chan struct{})
	sink := &faultSink{name: "sink", handle: func(ctx context.Context, _ *Message) error {
		if ctx.Value(key{}) != "value" {
			t.Error("lost delivery context value")
		}
		close(seen)
		<-ctx.Done()
		return ctx.Err()
	}}
	src, _ := g.AddSource(&faultSource{func(_ context.Context, e Emitter) error {
		return e.Emit(delivery, &Message{Kind: MessagePacket, Packet: &av.Packet{}})
	}}, BufferPolicy{})
	dst, _ := g.AddSink(sink, BufferPolicy{})
	_ = g.Connect(Route{From: string(src), To: []string{string(dst)}})
	_, err := g.(FaultIsolator).IsolateFaults("optional", []NodeRef{dst})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- g.Run(t.Context()) }()
	<-seen
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run=%v", err)
	}
	_ = g.Close()
}

func TestFaultDomainRejectsEgressAndConvergingBoundary(t *testing.T) {
	g, _ := NewGraph(GraphConfig{})
	defer g.Close()
	a, _ := g.AddStage(faultStage{"a"}, BufferPolicy{})
	b, _ := g.AddSink(&faultSink{name: "b", handle: func(context.Context, *Message) error { return nil }}, BufferPolicy{})
	c, _ := g.AddSink(&faultSink{name: "c", handle: func(context.Context, *Message) error { return nil }}, BufferPolicy{})
	_ = g.Connect(Route{From: string(a), To: []string{string(b)}})
	if _, err := g.(FaultIsolator).IsolateFaults("incomplete", []NodeRef{a}); !errors.Is(err, ErrUnsafeFaultDomain) {
		t.Fatalf("isolate=%v", err)
	}
	if _, err := g.(FaultIsolator).IsolateFaults("safe", []NodeRef{a, b}); err != nil {
		t.Fatal(err)
	}
	if err := g.Connect(Route{From: string(a), To: []string{string(c)}}); !errors.Is(err, ErrUnsafeFaultDomain) {
		t.Fatalf("egress=%v", err)
	}
}

func TestFaultDomainDoesNotClassifyShutdownAsBranchFailure(t *testing.T) {
	for _, buffer := range []BufferPolicy{{}, {Capacity: 2, Drop: DropBlock}} {
		g, _ := NewGraph(GraphConfig{Buffer: buffer, CloseWaitTimeout: time.Millisecond})
		entered, release := make(chan struct{}), make(chan struct{})
		source, _ := g.AddSource(&faultSource{func(ctx context.Context, e Emitter) error {
			return e.Emit(ctx, &Message{Kind: MessagePacket, Packet: &av.Packet{}})
		}}, BufferPolicy{})
		stage, _ := g.AddStage(&heldFaultStage{entered: entered, release: release}, BufferPolicy{})
		sink, _ := g.AddSink(&faultSink{name: "sink", handle: func(context.Context, *Message) error { return nil }}, BufferPolicy{})
		_ = g.Connect(Route{From: source.String(), To: []string{stage.String()}})
		_ = g.Connect(Route{From: stage.String(), To: []string{sink.String()}})
		d, err := g.(FaultIsolator).IsolateFaults("optional", []NodeRef{stage, sink})
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- g.Run(t.Context()) }()
		<-entered
		_ = g.Close()
		close(release)
		<-done
		_ = g.Close()
		if d.Failed() {
			t.Fatalf("shutdown became isolated fault: %+v", d.Snapshot())
		}
	}
}

type heldFaultStage struct{ entered, release chan struct{} }

func (*heldFaultStage) Name() string { return "held" }
func (s *heldFaultStage) Handle(ctx context.Context, m *Message, e Emitter) error {
	close(s.entered)
	<-s.release
	return e.Emit(ctx, m)
}
func (*heldFaultStage) Close() error { return nil }

func TestFaultDomainSteadyDeliveryAllocs(t *testing.T) {
	for _, buffer := range []BufferPolicy{{}, {Capacity: 1024, Drop: DropOldest}} {
		ctx := context.Background()
		g, _ := NewGraph(GraphConfig{Buffer: buffer})
		source := &holdSource{name: "src", ready: make(chan struct{}), release: make(chan struct{})}
		_, _ = g.AddSource(source, BufferPolicy{})
		sink, _ := g.AddSink(&benchSink{name: "sink"}, BufferPolicy{})
		_ = g.Connect(Route{From: "src", To: []string{sink.String()}})
		if _, err := g.(FaultIsolator).IsolateFaults("optional", []NodeRef{sink}); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- g.Run(ctx) }()
		<-source.ready
		message := benchPacketMessage(fanoutPayload, false)
		if allocs := testing.AllocsPerRun(1000, func() {
			if err := source.emitter.Emit(ctx, message); err != nil {
				t.Fatal(err)
			}
		}); allocs != 0 {
			t.Fatalf("buffer=%+v allocations=%v", buffer, allocs)
		}
		close(source.release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
