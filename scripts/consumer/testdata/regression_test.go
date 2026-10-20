package consumer_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/ctlserver"
	"github.com/thesyncim/goav/pipeline"
)

// This file is copied into an empty consumer module. It deliberately uses
// public APIs only, so it exercises the resolved dependency, not the checkout.
func TestResolvedClientKeepsReplyBeforeEOF(t *testing.T) {
	for range 20 {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
			var req struct {
				ID json.RawMessage `json:"id"`
			}
			if json.NewDecoder(conn).Decode(&req) == nil {
				_ = json.NewEncoder(conn).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": true})
			}
		}()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		client, err := ctlserver.Dial(ctx, "tcp://"+listener.Addr().String())
		if err != nil {
			cancel()
			listener.Close()
			t.Fatal(err)
		}
		got, err := client.Call[bool](ctx, "probe", nil)
		client.Close()
		cancel()
		listener.Close()
		if err != nil || !got {
			t.Fatalf("completed reply lost: %v %v", got, err)
		}
	}
}

func TestResolvedClientBoundsBlockedWrite(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	peer := make(chan net.Conn, 1)
	go func() { conn, _ := listener.Accept(); peer <- conn }()
	client, err := ctlserver.Dial(t.Context(), "tcp://"+listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	conn := <-peer
	defer conn.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := client.Call[bool](ctx, "probe", make([]byte, 16<<20))
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("deadline = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("resolved client cannot cancel a blocked write")
	}
}

type heldSource struct {
	ready, stop chan struct{}
	emitter     pipeline.Emitter
}

func (*heldSource) Name() string { return "source" }
func (s *heldSource) Start(_ context.Context, e pipeline.Emitter) error {
	s.emitter = e
	close(s.ready)
	<-s.stop
	return nil
}
func (*heldSource) Close() error { return nil }

type heldSink struct {
	entered, release chan struct{}
	closes           atomic.Int32
}

func (*heldSink) Name() string { return "sink" }
func (s *heldSink) Handle(context.Context, *pipeline.Message) error {
	close(s.entered)
	<-s.release
	return nil
}
func (s *heldSink) Close() error { s.closes.Add(1); return nil }

func TestResolvedGraphFinalizesAfterTimeout(t *testing.T) {
	for _, policy := range []pipeline.BufferPolicy{{}, {Capacity: 1}} {
		g, err := pipeline.NewGraph(pipeline.GraphConfig{Buffer: policy, CloseWaitTimeout: 5 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		source := &heldSource{ready: make(chan struct{}), stop: make(chan struct{})}
		sink := &heldSink{entered: make(chan struct{}), release: make(chan struct{})}
		if _, err := g.AddSource(source, policy); err != nil {
			t.Fatal(err)
		}
		if _, err := g.AddSink(sink, policy); err != nil {
			t.Fatal(err)
		}
		if err := g.Connect(pipeline.Route{From: "source", To: []string{"sink"}}); err != nil {
			t.Fatal(err)
		}
		run := make(chan error, 1)
		go func() { run <- g.Run(t.Context()) }()
		<-source.ready
		emit := make(chan error, 1)
		go func() {
			emit <- source.emitter.Emit(t.Context(), &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventDiscontinuity}})
		}()
		select {
		case <-sink.entered:
		case <-time.After(time.Second):
			t.Fatal("no delivery")
		}
		if err := g.Remove("sink"); !errors.Is(err, pipeline.ErrCloseWait) {
			t.Fatalf("remove = %v", err)
		}
		close(sink.release)
		<-emit
		close(source.stop)
		<-run
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		if sink.closes.Load() != 1 {
			t.Fatalf("resolved graph abandoned Close: %d", sink.closes.Load())
		}
	}
}
