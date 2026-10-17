package goav_test

import (
	"context"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/source"
)

// TestJoinBackpressuresFastArm pins bounded join buffering: while one arm of a
// mix or composite withholds its frames, the other arm's source must block
// after a bounded lead instead of the join absorbing its whole stream, and
// every frame still joins once the slow arm delivers.
func TestJoinBackpressuresFastArm(t *testing.T) {
	const frames = 5000
	for _, kind := range []string{"mix", "composite"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			fastDone := make(chan struct{})
			release := make(chan struct{})
			fast := gatedArmSource(kind, "fast", frames, nil, fastDone)
			slow := gatedArmSource(kind, "slow", frames, release, nil)
			out := goavtest.NewCollector()
			task, err := joinOf(kind, fast, slow).To(out.Sink()).UseRuntime(goavtest.Runtime()).BuildLive(ctx)
			if err != nil {
				t.Fatalf("BuildLive: %v", err)
			}
			defer task.Close()
			done := make(chan error, 1)
			go func() { done <- task.Run(ctx) }()
			select {
			case <-fastDone:
				t.Fatalf("fast arm pushed all %d frames while the slow arm withheld its own: the join buffered them without bound", frames)
			case <-time.After(300 * time.Millisecond):
			}
			close(release)
			if err := <-done; err != nil {
				t.Fatalf("Run: %v", err)
			}
			if got := len(out.Frames()); got != frames {
				t.Fatalf("joined frames = %d, want %d", got, frames)
			}
		})
	}
}

// TestJoinBackpressureReleasesOnCancel pins that an arm held back by the join
// cannot wedge shutdown: cancelling the run frees the waiting producer.
func TestJoinBackpressureReleasesOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	never := make(chan struct{})
	fast := gatedArmSource("mix", "fast", 5000, nil, nil)
	slow := gatedArmSource("mix", "slow", 1, never, nil)
	task, err := joinOf("mix", fast, slow).To(goavtest.NewCollector().Sink()).UseRuntime(goavtest.Runtime()).BuildLive(ctx)
	if err != nil {
		t.Fatalf("BuildLive: %v", err)
	}
	defer task.Close()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel while an arm waited on the join")
	}
}

func joinOf(kind string, a, b goav.InputSpec) interface {
	To(...goav.Destination) *goav.Job
} {
	if kind == "composite" {
		return goav.Composite(goav.From(a).Video(), goav.From(b).Video())
	}
	return goav.Mix(goav.From(a).Audio(), goav.From(b).Audio())
}

// gatedArmSource pushes frames tiny frames of the join kind's media, first
// waiting for release when it is non-nil, and closes done when it has pushed
// them all.
func gatedArmSource(kind string, name string, frames int, release <-chan struct{}, done chan<- struct{}) goav.InputSpec {
	spec := shape.Frame(av.MediaAudio, shape.Audio(48_000, 1, av.SampleFormatS16))
	if kind == "composite" {
		spec = shape.Frame(av.MediaVideo, shape.Video(2, 2, av.PixelFormatI420))
	}
	return goav.Source(name, spec, func(ctx context.Context, push source.Push) error {
		if done != nil {
			defer close(done)
		}
		if release != nil {
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		for i := range frames {
			if _, err := push.Frame(tinyFrame(kind, name, i)); err != nil {
				return err
			}
		}
		return push.EOS()
	})
}

func tinyFrame(kind string, name string, i int) *av.Frame {
	if kind == "composite" {
		base := av.TimeBase{Num: 1, Den: 30}
		plane := func(n int) av.Plane {
			return av.Plane{Buffer: av.Buffer{Bytes: make([]byte, n), Ownership: av.BufferImmutable}, Stride: max(n/2, 1)}
		}
		return &av.Frame{
			StreamID: av.StreamID(name),
			Type:     av.MediaVideo,
			PTS:      av.Timestamp{Value: int64(i), Base: base},
			Duration: av.Duration{Value: 1, Base: base},
			Video:    &av.VideoFrame{Width: 2, Height: 2, PixelFormat: av.PixelFormatI420},
			Planes:   []av.Plane{plane(4), plane(1), plane(1)},
		}
	}
	base := av.TimeBase{Num: 1, Den: 48_000}
	return &av.Frame{
		StreamID: av.StreamID(name),
		Type:     av.MediaAudio,
		PTS:      av.Timestamp{Value: int64(i), Base: base},
		Duration: av.Duration{Value: 1, Base: base},
		Audio:    &av.AudioFrame{SampleRate: 48_000, Channels: 1, SampleFormat: av.SampleFormatS16, Samples: 1},
		Planes:   []av.Plane{{Buffer: av.Buffer{Bytes: []byte{1, 0}, Ownership: av.BufferImmutable}}},
	}
}
