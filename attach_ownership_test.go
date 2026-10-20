package goav

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/pipeline"
)

type attachOwnedSink struct {
	name   string
	closes int
}

type rollbackSource struct {
	name        string
	ready, stop chan struct{}
	emitter     pipeline.Emitter
}

type rollbackOwnedStage struct {
	name   string
	closes int
}

func (s *rollbackOwnedStage) Name() string { return s.name }
func (*rollbackOwnedStage) Handle(ctx context.Context, msg *pipeline.Message, emit pipeline.Emitter) error {
	return emit.Emit(ctx, msg)
}
func (s *rollbackOwnedStage) Close() error { s.closes++; return nil }

type rollbackFailureGraph struct {
	pipeline.Graph
	at, calls int
}

func (g *rollbackFailureGraph) fail() error {
	g.calls++
	if g.calls == g.at {
		return errors.New("injected graph operation failure")
	}
	return nil
}
func (g *rollbackFailureGraph) AddStage(stage pipeline.Stage, policy pipeline.BufferPolicy) (pipeline.NodeRef, error) {
	if err := g.fail(); err != nil {
		return "", err
	}
	return g.Graph.AddStage(stage, policy)
}
func (g *rollbackFailureGraph) AddSink(sink pipeline.Sink, policy pipeline.BufferPolicy) (pipeline.NodeRef, error) {
	if err := g.fail(); err != nil {
		return "", err
	}
	return g.Graph.AddSink(sink, policy)
}
func (g *rollbackFailureGraph) Connect(route pipeline.Route) error {
	if err := g.fail(); err != nil {
		return err
	}
	return g.Graph.Connect(route)
}

func TestAttachRollbackAtEveryInstallationBoundary(t *testing.T) {
	for at := 1; at <= 6; at++ {
		g, err := pipeline.NewGraph(pipeline.GraphConfig{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := g.AddSource(&rollbackSource{name: "source"}, pipeline.BufferPolicy{}); err != nil {
			t.Fatal(err)
		}
		a, b := &rollbackOwnedStage{name: "a"}, &rollbackOwnedStage{name: "b"}
		c := &attachOwnedSink{name: "c"}
		p := &attachPlan{
			task: &task{graph: &rollbackFailureGraph{Graph: g, at: at}},
			plan: &compile.AttachPlan{Nodes: []compile.ComposeNode{
				{Name: "a", Inputs: []pipeline.NodeRef{"source"}},
				{Name: "b", Inputs: []pipeline.NodeRef{"a"}},
				{Name: "c", Inputs: []pipeline.NodeRef{"b"}},
			}},
			opened:  []openedNode{{stage: a, owned: true}, {stage: b, owned: true}, {sink: c, owned: true}},
			buffers: make([]pipeline.BufferPolicy, 3),
		}
		if _, err := p.apply(); err == nil {
			t.Fatalf("operation %d did not fail", at)
		}
		p.close()
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		if a.closes != 1 || b.closes != 1 || c.closes != 1 {
			t.Fatalf("operation %d: closes=%d,%d,%d", at, a.closes, b.closes, c.closes)
		}
	}
}

func (s *rollbackSource) Name() string { return s.name }
func (s *rollbackSource) Start(_ context.Context, emitter pipeline.Emitter) error {
	s.emitter = emitter
	close(s.ready)
	<-s.stop
	return nil
}
func (*rollbackSource) Close() error { return nil }

type rollbackBusySink struct {
	name              string
	entered, release  chan struct{}
	handling, overlap atomic.Bool
	closes            atomic.Int32
}

func (s *rollbackBusySink) Name() string { return s.name }
func (s *rollbackBusySink) Handle(context.Context, *pipeline.Message) error {
	s.handling.Store(true)
	defer s.handling.Store(false)
	close(s.entered)
	<-s.release
	return nil
}
func (s *rollbackBusySink) Close() error {
	s.overlap.Store(s.handling.Load())
	s.closes.Add(1)
	return nil
}

type rollbackConnectGraph struct {
	pipeline.Graph
	first func()
}

func (g *rollbackConnectGraph) Connect(route pipeline.Route) error {
	if route.From == "second" {
		return errors.New("injected second ingress failure")
	}
	if err := g.Graph.Connect(route); err != nil {
		return err
	}
	if route.From == "first" {
		g.first()
	}
	return nil
}

func TestAttachRollbackWaitsForActiveHandle(t *testing.T) {
	g, err := pipeline.NewGraph(pipeline.GraphConfig{CloseWaitTimeout: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	first := &rollbackSource{name: "first", ready: make(chan struct{}), stop: make(chan struct{})}
	second := &rollbackSource{name: "second", ready: make(chan struct{}), stop: make(chan struct{})}
	for _, source := range []*rollbackSource{first, second} {
		if _, err := g.AddSource(source, pipeline.BufferPolicy{}); err != nil {
			t.Fatal(err)
		}
	}
	run := make(chan error, 1)
	go func() { run <- g.Run(t.Context()) }()
	<-first.ready
	<-second.ready
	a := &rollbackBusySink{name: "a", entered: make(chan struct{}), release: make(chan struct{})}
	b := &attachOwnedSink{name: "b"}
	emit := make(chan error, 1)
	wrapped := &rollbackConnectGraph{Graph: g, first: func() {
		go func() {
			emit <- first.emitter.Emit(t.Context(), &pipeline.Message{Kind: pipeline.MessagePacket, Packet: &av.Packet{}})
		}()
		<-a.entered
	}}
	p := &attachPlan{
		task: &task{graph: wrapped},
		plan: &compile.AttachPlan{Nodes: []compile.ComposeNode{
			{Name: "a", Inputs: []pipeline.NodeRef{"first"}},
			{Name: "b", Inputs: []pipeline.NodeRef{"second"}},
		}},
		opened:  []openedNode{{sink: a, owned: true}, {sink: b, owned: true}},
		buffers: make([]pipeline.BufferPolicy, 2),
	}
	if _, err := p.apply(); err == nil {
		t.Fatal("expected ingress failure")
	}
	if a.closes.Load() != 0 || b.closes != 1 {
		t.Fatalf("rollback closed active sink: a=%d b=%d", a.closes.Load(), b.closes)
	}
	close(a.release)
	if err := <-emit; err != nil {
		t.Fatal(err)
	}
	close(first.stop)
	close(second.stop)
	if err := <-run; err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if a.closes.Load() != 1 || a.overlap.Load() {
		t.Fatalf("closes=%d overlap=%v", a.closes.Load(), a.overlap.Load())
	}
}

func (s *attachOwnedSink) Name() string                                  { return s.name }
func (*attachOwnedSink) Handle(context.Context, *pipeline.Message) error { return nil }
func (s *attachOwnedSink) Close() error                                  { s.closes++; return nil }

func TestAttachRollbackTransfersOwnershipOnAdd(t *testing.T) {
	g, err := pipeline.NewGraph(pipeline.GraphConfig{})
	if err != nil {
		t.Fatal(err)
	}
	a, b := &attachOwnedSink{name: "a"}, &attachOwnedSink{name: "b"}
	p := &attachPlan{
		task:    &task{graph: g},
		plan:    &compile.AttachPlan{Nodes: []compile.ComposeNode{{Name: "a"}, {Name: "b"}}},
		opened:  []openedNode{{sink: a, owned: true}, {sink: b, owned: true}},
		buffers: []pipeline.BufferPolicy{{}, {Capacity: 1}},
	}
	if _, err := p.apply(); !errors.Is(err, pipeline.ErrBufferedEdgesUnsupported) {
		t.Fatalf("apply = %v", err)
	}
	p.close() // Cleanup may be repeated by the caller.
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if a.closes != 1 || b.closes != 1 {
		t.Fatalf("closes = %d, %d; want 1, 1", a.closes, b.closes)
	}
}
