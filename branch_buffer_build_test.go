package goav_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/source"
)

// TestBuildTimeBranchBufferIsHonored pins that a branch's declared .Buffer
// reaches the built graph, not only the recipe report: a preview branch that
// declares DropNewest(1) and stalls must shed its own frames instead of
// stalling the record branch beside it.
func TestBuildTimeBranchBufferIsHonored(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const frames = 200
	record := goavtest.NewCollector()
	release := make(chan struct{})
	preview := goav.Sink(component.SinkFunc("preview", func(ctx context.Context, _ component.Message) error {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	}))
	task, err := goav.From(liveS16Source("mic", frames)).Audio().Branches(
		goav.Branch("record").To(record.Sink()),
		goav.Branch("preview").Buffer(flow.DropNewest(1)).To(preview),
	).UseRuntime(goavtest.Runtime()).BuildLive(ctx)
	if err != nil {
		t.Fatalf("BuildLive: %v", err)
	}
	defer task.Close()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	if _, err := record.WaitFrames(ctx, frames); err != nil {
		t.Fatalf("record branch stalled behind the preview branch's buffer: %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := task.Stats().DropReasons[pipeline.DropNewest]; got == 0 {
		t.Fatalf("DropReasons[DropNewest] = 0, want the preview branch to shed frames (stats %+v)", task.Stats().DropReasons)
	}
}

// liveS16Source pushes frames one-sample mono S16 frames as a live source,
// retrying on backpressure, then ends.
func liveS16Source(name string, frames int) goav.InputSpec {
	base := av.TimeBase{Num: 1, Den: 48_000}
	return goav.Source(name,
		shape.Frame(av.MediaAudio, shape.Audio(48_000, 1, av.SampleFormatS16), shape.Realtime(true)),
		func(ctx context.Context, push source.Push) error {
			for i := range frames {
				frame := &av.Frame{
					StreamID: av.StreamID(name),
					Type:     av.MediaAudio,
					PTS:      av.Timestamp{Value: int64(i), Base: base},
					Duration: av.Duration{Value: 1, Base: base},
					Audio:    &av.AudioFrame{SampleRate: 48_000, Channels: 1, SampleFormat: av.SampleFormatS16, Samples: 1},
					Planes:   []av.Plane{{Buffer: av.Buffer{Bytes: []byte{byte(i), 0}, Ownership: av.BufferImmutable}}},
				}
				for {
					_, err := push.Frame(frame)
					if err == nil {
						break
					}
					if !errors.Is(err, goav.ErrBackpressure) || ctx.Err() != nil {
						return err
					}
				}
			}
			return push.EOS()
		})
}

// TestBranchBufferPromotesOfflineGraph pins that a declared branch buffer
// decouples the branch on an offline job too: the graph is promoted to
// buffered, so branch a's sink can wait for branch b without deadlocking the
// fork (a direct graph would call a first and never reach b).
func TestBranchBufferPromotesOfflineGraph(t *testing.T) {
	for name, input := range map[string]func() goav.InputSpec{
		"frames": func() goav.InputSpec {
			return goavtest.Audio(48_000, 1, []int16{1, 2}, []int16{3, 4}, []int16{5, 6})
		},
		"decoded packets": func() goav.InputSpec {
			return goavtest.Packets(av.CodecOpus,
				av.Packet{Payload: av.Buffer{Bytes: []byte{1}}},
				av.Packet{Payload: av.Buffer{Bytes: []byte{2}}},
				av.Packet{Payload: av.Buffer{Bytes: []byte{3}}})
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			bGot := make(chan struct{})
			var once sync.Once
			var aFrames, bFrames atomic.Int64
			a := goav.Sink(component.SinkFunc("a", func(ctx context.Context, msg component.Message) error {
				select {
				case <-bGot:
				case <-ctx.Done():
					return ctx.Err()
				}
				if msg.Frame != nil {
					aFrames.Add(1)
				}
				return nil
			}))
			b := goav.Sink(component.SinkFunc("b", func(_ context.Context, msg component.Message) error {
				if msg.Frame != nil {
					bFrames.Add(1)
					once.Do(func() { close(bGot) })
				}
				return nil
			}))
			chain := goav.From(input()).Audio()
			if name == "decoded packets" {
				chain = chain.Decode()
			}
			err := chain.Branches(
				goav.Branch("a").Buffer(flow.Blocking(4)).To(a),
				goav.Branch("b").To(b),
			).UseRuntime(goavtest.Runtime()).Run(ctx)
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if aFrames.Load() != 3 || bFrames.Load() != 3 {
				t.Fatalf("frames a=%d b=%d, want 3 each", aFrames.Load(), bFrames.Load())
			}
		})
	}
}
