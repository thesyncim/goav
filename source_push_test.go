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

// TestSourcePushShedsSilentlyUnderDropPolicies pins the CURRENT per-push
// contract documented on SourcePush: a dropping policy sheds a full queue
// deliberately and silently — every push returns nil even while messages are
// shed (the sheds land in the drop counters, not on the push). This is the
// observability gap a future result-aware push (PushResult) will close; when
// it does, this test must be updated to assert the shed is reported.
func TestSourcePushShedsSilentlyUnderDropPolicies(t *testing.T) {
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

	var backpressured atomic.Int64
	var accepted atomic.Int64
	src := Source("push",
		shape.Frame(av.MediaAudio, shape.Audio(48000, codec.Mono, av.SampleFormatS16), shape.Stream("push")),
		func(ctx context.Context, push SourcePush) error {
			payload := []byte{1, 0}
			for i := 0; i < 64; i++ {
				if ctx.Err() != nil {
					return nil
				}
				frame := &av.Frame{
					StreamID: "push", Type: av.MediaAudio,
					Audio:  &av.AudioFrame{SampleRate: 48000, Channels: 1, SampleFormat: av.SampleFormatS16, Samples: 1},
					Planes: []av.Plane{{Buffer: av.Buffer{Bytes: payload, Ownership: av.BufferImmutable}}},
				}
				err := push.Frame(frame)
				switch {
				case err == nil:
					accepted.Add(1)
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

	rt := New(WithBufferPolicy(pipeline.BufferPolicy{Capacity: 1, Drop: pipeline.DropNewest}))
	task, err := From(src).Audio().To(sink).UseRuntime(rt).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = task.Close() })
	if err := task.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v", err)
	}

	if accepted.Load() == 0 {
		t.Fatal("no push was accepted")
	}
	if backpressured.Load() != 0 {
		t.Fatal("a dropping policy reported ErrBackpressure on a push — sheds are silent today; if this changed deliberately (PushResult), update this pin")
	}
	if accepted.Load() != 64 {
		t.Fatalf("accepted = %d, want all 64 pushes nil under a dropping policy", accepted.Load())
	}
	if sank.Load() == 0 {
		t.Fatal("sink saw no frames")
	}
}
