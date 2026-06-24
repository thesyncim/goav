package goav

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// TestSourcePushReportsShedsPerPush pins the result-aware push contract on
// SourcePush: a dropping policy still sheds a full queue deliberately and
// without error, but the shed is no longer silent — the PushResult reports
// Dropped for the shed pushes and Accepted for the delivered ones, so every
// push is accounted for. The error keeps its old meaning (flow control or
// fatal); shedding is normal realtime behavior, not failure.
func TestSourcePushReportsShedsPerPush(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	gate := make(chan struct{})
	var sank atomic.Int64
	sink := Sink(SinkFunc("slow", func(ctx context.Context, m Message) error {
		if m.Kind != pipeline.MessageFrame {
			return nil
		}
		// Block the sink worker until released, so the tiny queue upstream
		// fills and the drop policy has to shed.
		select {
		case <-gate:
		case <-ctx.Done():
		}
		sank.Add(1)
		return nil
	}))

	const pushes = 64
	var backpressured atomic.Int64
	var accepted atomic.Int64
	var dropped atomic.Int64
	src := Source("push",
		shape.Frame(av.MediaAudio, shape.Audio(48000, codec.Mono, av.SampleFormatS16), shape.Stream("push")),
		func(ctx context.Context, push SourcePush) error {
			payload := []byte{1, 0}
			for i := 0; i < pushes; i++ {
				if ctx.Err() != nil {
					return nil
				}
				frame := &av.Frame{
					StreamID: "push", Type: av.MediaAudio,
					Audio:  &av.AudioFrame{SampleRate: 48000, Channels: 1, SampleFormat: av.SampleFormatS16, Samples: 1},
					Planes: []av.Plane{{Buffer: av.Buffer{Bytes: payload, Ownership: av.BufferImmutable}}},
				}
				result, err := push.Frame(frame)
				switch {
				case err == nil:
					if result.Accepted {
						accepted.Add(1)
					}
					if result.Dropped {
						dropped.Add(1)
					}
				case errors.Is(err, ErrBackpressure):
					backpressured.Add(1)
				case errors.Is(err, ErrClosed):
					return nil
				default:
					return err
				}
			}
			close(gate)
			return push.EOS()
		})

	rt := MustNew(WithBufferPolicy(pipeline.BufferPolicy{Capacity: 1, Drop: pipeline.DropNewest}))
	task, err := From(src).Audio().To(sink).UseRuntime(rt).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = task.Close() })
	if err := task.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v", err)
	}

	if backpressured.Load() != 0 {
		t.Fatal("a dropping policy reported ErrBackpressure on a push — sheds must stay nil-error, reported via PushResult")
	}
	if accepted.Load() == 0 {
		t.Fatal("no push reported Accepted")
	}
	if dropped.Load() == 0 {
		t.Fatal("no push reported Dropped despite a gated sink behind a capacity-1 DropNewest queue")
	}
	if got := accepted.Load() + dropped.Load(); got != pushes {
		t.Fatalf("accepted(%d)+dropped(%d) = %d, want %d (every push accounted on a single-target route)",
			accepted.Load(), dropped.Load(), got, pushes)
	}
	if sank.Load() == 0 {
		t.Fatal("sink saw no frames")
	}
}
