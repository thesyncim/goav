package pipeline

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type recoveringSink struct {
	entered, release chan struct{}
	handling         atomic.Bool
	overlap          atomic.Bool
	closes           atomic.Int32
}

func (*recoveringSink) Name() string { return "recovering" }
func (s *recoveringSink) Handle(context.Context, *Message) error {
	s.handling.Store(true)
	defer s.handling.Store(false)
	select {
	case <-s.entered:
	default:
		close(s.entered)
	}
	<-s.release
	return nil
}
func (s *recoveringSink) Close() error {
	s.overlap.Store(s.handling.Load())
	s.closes.Add(1)
	return nil
}

func TestRetirementRecoversAfterTimeout(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, shutdown := range []bool{false, true} {
			name := "direct"
			policy := BufferPolicy{}
			if buffered {
				name = "buffered"
				policy = BufferPolicy{Capacity: 2, Drop: DropBlock}
			}
			if shutdown {
				name += "/close"
			} else {
				name += "/remove"
			}
			t.Run(name, func(t *testing.T) {
				g, err := NewGraph(GraphConfig{Buffer: policy, CloseWaitTimeout: 5 * time.Millisecond})
				if err != nil {
					t.Fatal(err)
				}
				source := &holdSource{name: "src", ready: make(chan struct{}), release: make(chan struct{})}
				if _, err := g.AddSource(source, BufferPolicy{}); err != nil {
					t.Fatal(err)
				}
				run := make(chan error, 1)
				go func() { run <- g.Run(t.Context()) }()
				<-source.ready
				cycles := 20
				if shutdown {
					cycles = 1
				}
				for range cycles {
					s := &recoveringSink{entered: make(chan struct{}), release: make(chan struct{})}
					if _, err := g.AddSink(s, policy); err != nil {
						t.Fatal(err)
					}
					if err := g.Connect(route("src", s.Name())); err != nil {
						t.Fatal(err)
					}
					emit := make(chan error, 1)
					packet := immutablePacket(1)
					go func() { emit <- source.emitter.Emit(t.Context(), &Message{Kind: MessagePacket, Packet: &packet}) }()
					select {
					case <-s.entered:
					case <-time.After(time.Second):
						t.Fatal("Handle did not start")
					}
					if shutdown {
						err = g.Close()
					} else {
						err = g.Remove(NodeRef(s.Name()))
					}
					if !errors.Is(err, ErrCloseWait) {
						t.Fatalf("wait = %v", err)
					}
					if s.closes.Load() != 0 {
						t.Fatal("closed before Handle finished")
					}
					if g.Stats().PendingCleanup != 1 {
						t.Fatalf("pending cleanup = %d", g.Stats().PendingCleanup)
					}
					close(s.release)
					if err := <-emit; err != nil {
						t.Fatal(err)
					}
					deadline := time.Now().Add(time.Second)
					for g.Stats().PendingCleanup != 0 && time.Now().Before(deadline) {
						time.Sleep(time.Millisecond)
					}
					if g.Stats().PendingCleanup != 0 {
						t.Fatal("retired node was abandoned")
					}
					if s.closes.Load() != 1 || s.overlap.Load() {
						t.Fatalf("closes=%d overlap=%v", s.closes.Load(), s.overlap.Load())
					}
				}
				if nodeTableLen(g) != 2 {
					t.Fatalf("node table grew to %d", nodeTableLen(g))
				}
				close(source.release)
				if err := <-run; err != nil {
					t.Fatal(err)
				}
				if err := g.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
