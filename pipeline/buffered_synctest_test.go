package pipeline

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/thesyncim/goav/av"
)

// TestBufferedDropBlockParkWakeDeterministic uses testing/synctest to verify
// that DropBlock parking/waking and worker sleep/wake transitions never suffer
// a lost wakeup under tight queue capacity.
func TestBufferedDropBlockParkWakeDeterministic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g, err := NewGraph(GraphConfig{
			Name:   "synctest-block",
			Buffer: BufferPolicy{Capacity: 1, Drop: DropBlock},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer g.Close()

		const total = 16
		gate := make(chan struct{})
		var received atomic.Int64

		src := &funcSource{
			name: "src",
			fn: func(ctx context.Context, emit Emitter) error {
				for i := range total {
					pkt := &av.Packet{StreamID: "v", Payload: av.Buffer{Bytes: []byte{byte(i)}}}
					if err := emit.Emit(ctx, &Message{Kind: MessagePacket, Packet: pkt}); err != nil {
						return err
					}
				}
				return nil
			},
		}
		sink := &funcSink{
			name: "sink",
			fn: func(_ context.Context, msg *Message) error {
				if msg.Kind == MessagePacket {
					<-gate
					received.Add(1)
				}
				return nil
			},
		}
		srcRef, err := g.AddSource(src, BufferPolicy{})
		if err != nil {
			t.Fatal(err)
		}
		sinkRef, err := g.AddSink(sink, BufferPolicy{Capacity: 1, Drop: DropBlock})
		if err != nil {
			t.Fatal(err)
		}
		if err := g.Connect(Route{From: srcRef.String(), To: []string{sinkRef.String()}, Policy: RouteAll}); err != nil {
			t.Fatal(err)
		}

		runErr := make(chan error, 1)
		go func() {
			runErr <- g.Run(context.Background())
		}()

		for range total {
			// Wait until producer is parked on DropBlock or sink is waiting on gate.
			synctest.Wait()
			gate <- struct{}{}
		}
		synctest.Wait()
		if err := <-runErr; err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
		if got := received.Load(); got != total {
			t.Fatalf("received = %d, want %d", got, total)
		}
	})
}

// TestBufferedRemoveWhileProducerParkedDeterministic verifies that removing a
// DropBlock sink while a producer is parked waiting for queue space unparks the
// producer cleanly without deadlocking.
func TestBufferedRemoveWhileProducerParkedDeterministic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g, err := NewGraph(GraphConfig{
			Name:   "synctest-remove-parked",
			Buffer: BufferPolicy{Capacity: 1, Drop: DropBlock},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer g.Close()

		sinkBlocked := make(chan struct{})
		releaseSink := make(chan struct{})
		srcDone := make(chan struct{})

		src := &funcSource{
			name: "src",
			fn: func(ctx context.Context, emit Emitter) error {
				defer close(srcDone)
				for i := range 8 {
					pkt := &av.Packet{StreamID: "v", Payload: av.Buffer{Bytes: []byte{byte(i)}}}
					if err := emit.Emit(ctx, &Message{Kind: MessagePacket, Packet: pkt}); err != nil {
						return err
					}
				}
				return nil
			},
		}
		var once atomic.Bool
		sink := &funcSink{
			name: "sink",
			fn: func(_ context.Context, msg *Message) error {
				if msg.Kind == MessagePacket && once.CompareAndSwap(false, true) {
					close(sinkBlocked)
					<-releaseSink
				}
				return nil
			},
		}
		srcRef, err := g.AddSource(src, BufferPolicy{})
		if err != nil {
			t.Fatal(err)
		}
		sinkRef, err := g.AddSink(sink, BufferPolicy{Capacity: 1, Drop: DropBlock})
		if err != nil {
			t.Fatal(err)
		}
		if err := g.Connect(Route{From: srcRef.String(), To: []string{sinkRef.String()}, Policy: RouteAll}); err != nil {
			t.Fatal(err)
		}

		runErr := make(chan error, 1)
		go func() {
			runErr <- g.Run(context.Background())
		}()

		<-sinkBlocked
		// Wait until the producer fills capacity=1 and parks on DropBlock.
		synctest.Wait()

		// Release the sink and remove the node concurrently; producer must unpark.
		close(releaseSink)
		if remover, ok := g.(NodeRemoverContext); ok {
			if _, err := remover.RemoveNodeContext(context.Background(), sinkRef); err != nil {
				t.Fatalf("RemoveNodeContext = %v", err)
			}
		}
		<-srcDone
		if err := <-runErr; err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
	})
}

type funcSource struct {
	name string
	fn   func(context.Context, Emitter) error
}

func (s *funcSource) Name() string                               { return s.name }
func (s *funcSource) Start(ctx context.Context, e Emitter) error { return s.fn(ctx, e) }
func (s *funcSource) Close() error                               { return nil }

type funcSink struct {
	name string
	fn   func(context.Context, *Message) error
}

func (s *funcSink) Name() string                                   { return s.name }
func (s *funcSink) Handle(ctx context.Context, msg *Message) error { return s.fn(ctx, msg) }
func (s *funcSink) Close() error                                   { return nil }
