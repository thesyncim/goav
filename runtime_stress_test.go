package goav

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// TestStressConcurrentSourcePushMutableBuffers pushes from many goroutines that
// reuse and mutate one borrowed backing array each, racing the runtime's
// ingest copy. A torn payload at the sink (bytes from two different writes)
// would mean a borrowed buffer was queued without copying; the race detector
// catches the underlying data race directly.
func TestStressConcurrentSourcePushMutableBuffers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const writers = 8
	const perWriter = 256
	const payloadLen = 64

	var torn atomic.Int64
	var seen atomic.Int64
	sink := Sink(SinkFunc("collect", func(_ context.Context, m Message) error {
		if m.Kind != pipeline.MessageFrame || m.Frame == nil || len(m.Frame.Planes) == 0 {
			return nil
		}
		bytes := m.Frame.Planes[0].Buffer.Bytes
		if len(bytes) == 0 {
			return nil
		}
		first := bytes[0]
		for _, b := range bytes {
			if b != first {
				torn.Add(1)
				break
			}
		}
		seen.Add(1)
		return nil
	}))

	src := Source("push",
		shape.Frame(av.MediaAudio, shape.Audio(48000, codec.Mono, av.SampleFormatS16), shape.Stream("push")),
		func(ctx context.Context, push SourcePush) error {
			var writersDone sync.WaitGroup
			for w := 0; w < writers; w++ {
				writersDone.Add(1)
				go func(w int) {
					defer writersDone.Done()
					payload := make([]byte, payloadLen) // reused, mutated between pushes
					for j := 0; j < perWriter; j++ {
						if ctx.Err() != nil {
							return
						}
						v := byte((w*perWriter + j) & 0xff)
						for k := range payload {
							payload[k] = v
						}
						frame := &av.Frame{
							StreamID: "push",
							Type:     av.MediaAudio,
							Audio:    &av.AudioFrame{SampleRate: 48000, Channels: 1, SampleFormat: av.SampleFormatS16, Samples: 1},
							Planes:   []av.Plane{{Buffer: av.Buffer{Bytes: payload, Ownership: av.BufferBorrowed}}},
						}
						_, err := push.Frame(frame)
						if err != nil && !errors.Is(err, ErrBackpressure) {
							return
						}
						// Immediately reuse the backing array with a different value:
						// the runtime must already hold its own copy.
						for k := range payload {
							payload[k] = v ^ 0xff
						}
					}
				}(w)
			}
			writersDone.Wait()
			return push.EOS()
		})

	rt := mustNew(WithBufferPolicy(pipeline.BufferPolicy{Capacity: 8, Drop: pipeline.DropOldest}))
	task, err := From(src).Audio().To(sink).UseRuntime(rt).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = task.Close() })

	if err := task.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v", err)
	}
	if torn.Load() != 0 {
		t.Fatalf("%d frames had torn payloads: a borrowed buffer was queued without an ingest copy", torn.Load())
	}
	if seen.Load() == 0 {
		t.Fatal("sink saw no frames")
	}
}

// TestStressAttachDetachUnderLoadWithSlowWatchers churns runtime Attach/Detach
// from several goroutines while a source streams under load and a deliberately
// slow watcher consumes events. It guards against races, deadlocks, and
// send-on-closed-channel panics in the event close path (run with -race).
func TestStressAttachDetachUnderLoadWithSlowWatchers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := make(chan struct{})
	src := Source("load",
		shape.Frame(av.MediaAudio, shape.Audio(48000, codec.Mono, av.SampleFormatS16), shape.Stream("load")),
		func(ctx context.Context, push SourcePush) error {
			payload := []byte{1, 0}
			for {
				select {
				case <-stop:
					return push.EOS()
				case <-ctx.Done():
					return nil
				default:
				}
				frame := &av.Frame{
					StreamID: "load",
					Type:     av.MediaAudio,
					Audio:    &av.AudioFrame{SampleRate: 48000, Channels: 1, SampleFormat: av.SampleFormatS16, Samples: 1},
					Planes:   []av.Plane{{Buffer: av.Buffer{Bytes: payload, Ownership: av.BufferImmutable}}},
				}
				if _, err := push.Frame(frame); err != nil {
					if errors.Is(err, ErrClosed) || errors.Is(err, context.Canceled) {
						return nil
					}
					if !errors.Is(err, ErrBackpressure) {
						return err
					}
				}
			}
		})

	main := Sink(SinkFunc("main", func(context.Context, Message) error { return nil }))
	rt := mustNew(WithBufferPolicy(pipeline.BufferPolicy{Capacity: 4, Drop: pipeline.DropOldest}))
	task, err := From(src).Audio().Tap(FrameTap("load.frames")).To(main).UseRuntime(rt).BuildLive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	// A deliberately slow watcher: it must shed for itself without blocking the
	// data plane or wedging the event close path.
	sub := task.Watch()
	var watcher sync.WaitGroup
	watcher.Add(1)
	go func() {
		defer watcher.Done()
		for range sub.Events() {
			time.Sleep(time.Millisecond)
		}
	}()

	runDone := make(chan error, 1)
	go func() { runDone <- task.Run(ctx) }()

	const churners = 4
	const cycles = 40
	var churn sync.WaitGroup
	for g := 0; g < churners; g++ {
		churn.Add(1)
		go func(g int) {
			defer churn.Done()
			for i := 0; i < cycles; i++ {
				attachment, err := task.Attach(ctx, Branch(fmt.Sprintf("mon-%d-%d", g, i)).
					From(FrameTap("load.frames")).
					To(Sink(SinkFunc("watch", func(context.Context, Message) error { return nil }))))
				if err != nil {
					if errors.Is(err, ErrClosed) || errors.Is(err, context.Canceled) {
						return
					}
					continue
				}
				if err := task.Detach(ctx, attachment); err != nil &&
					!errors.Is(err, ErrClosed) && !errors.Is(err, context.Canceled) {
					return
				}
			}
		}(g)
	}
	churn.Wait()

	close(stop)
	cancel()
	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after attach/detach churn")
	}
	sub.Close()
	watcher.Wait()
}
