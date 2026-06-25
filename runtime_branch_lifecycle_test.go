package goav_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

const lifecycleTapName = "video.decoded"

// frameCountSink counts the decoded frames it receives.
type frameCountSink struct {
	name  string
	count atomic.Int64
}

func (s *frameCountSink) Name() string { return s.name }

func (s *frameCountSink) Handle(_ context.Context, msg *pipeline.Message) error {
	if msg != nil && msg.Kind == pipeline.MessageFrame {
		s.count.Add(1)
	}
	return nil
}

func (s *frameCountSink) Close() error { return nil }

// gateFrameSink counts frames but blocks its node worker until release closes,
// so its branch queue fills and a dropping branch buffer has to shed.
type gateFrameSink struct {
	name    string
	release chan struct{}
	count   atomic.Int64
}

func (s *gateFrameSink) Name() string { return s.name }

func (s *gateFrameSink) Handle(ctx context.Context, msg *pipeline.Message) error {
	select {
	case <-s.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	if msg != nil && msg.Kind == pipeline.MessageFrame {
		s.count.Add(1)
	}
	return nil
}

func (s *gateFrameSink) Close() error { return nil }

// packetCountSink counts encoded packets, for branches that end in an encoder.
type packetCountSink struct {
	name  string
	count atomic.Int64
}

func (s *packetCountSink) Name() string { return s.name }

func (s *packetCountSink) Handle(_ context.Context, msg *pipeline.Message) error {
	if msg != nil && msg.Kind == pipeline.MessagePacket {
		s.count.Add(1)
	}
	return nil
}

func (s *packetCountSink) Close() error { return nil }

// liveMutableVideoPackets is a packet-domain video source that loops forever,
// so a runtime test can attach and detach branches while media keeps flowing.
// The passthrough decoder turns each foreign packet into a fresh mutable frame.
func liveMutableVideoPackets(name string, width, height int, tick time.Duration) goav.InputSpec {
	return goav.Source(name,
		shape.Packet(av.MediaVideo, av.CodecVP8, shape.Video(width, height, av.PixelFormatI420)),
		func(ctx context.Context, push goav.SourcePush) error {
			base := av.TimeBase{Num: 1, Den: 90_000}
			for n := 0; ; n++ {
				if ctx.Err() != nil {
					return nil
				}
				packet := &av.Packet{
					StreamID: av.StreamID(name),
					Type:     av.MediaVideo,
					Keyframe: true,
					PTS:      av.Timestamp{Value: int64(n) * 3000, Base: base},
					DTS:      av.Timestamp{Value: int64(n) * 3000, Base: base},
					Duration: av.Duration{Value: 3000, Base: base},
					Payload:  av.Buffer{Bytes: []byte{byte(n)}, Ownership: av.BufferImmutable},
				}
				for {
					if ctx.Err() != nil {
						return nil
					}
					if _, err := push.Packet(packet); err == nil {
						break
					} else if !errors.Is(err, goav.ErrBackpressure) {
						return nil
					}
				}
				if tick > 0 {
					select {
					case <-time.After(tick):
					case <-ctx.Done():
						return nil
					}
				}
			}
		})
}

func decodedVideoTapTask(t *testing.T, ctx context.Context, input goav.InputSpec, main pipeline.Sink) goav.LiveTask {
	t.Helper()
	task, err := goav.From(input).
		UseRuntime(goavtest.Runtime()).
		Video().
		Decode().
		Shape(shape.Frame(av.MediaVideo, shape.Video(64, 64, av.PixelFormatI420))).
		Tap(goav.FrameTap(lifecycleTapName)).
		To(goav.Sink(main)).
		Build(ctx)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	return task
}

func attachDropBranch(t *testing.T, ctx context.Context, task goav.Mutable, name string, sink pipeline.Sink) goav.Attachment {
	t.Helper()
	attachment, err := task.Attach(ctx, goav.Branch(name).
		From(goav.FrameTap(lifecycleTapName)).
		Buffer(flow.DropOldest(8)).
		Resize(64, 64).
		To(goav.Sink(sink)))
	if err != nil {
		t.Fatalf("Attach(%s) error = %v", name, err)
	}
	return attachment
}

func waitFrameCount(ctx context.Context, c *atomic.Int64, target int64) {
	for c.Load() < target && ctx.Err() == nil {
		time.Sleep(time.Millisecond)
	}
}

// TestRuntimeBranchDropBufferDoesNotStarveSiblings is the runtime-level proof of
// the showcase fix: with a dropping output branch, a stalled slow branch sheds
// for itself and never paces the decode tap, so the main sink (the tap's
// sibling) still receives every decoded frame. With the old blocking branch
// buffer the gated branch would block the tap and the finite source would never
// deliver all frames — so main reaching every frame is the discriminator.
// Deterministic: it waits on frame counts, not on a wall clock.
func TestRuntimeBranchDropBufferDoesNotStarveSiblings(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const frames = 48
	main := &frameCountSink{name: "main"}
	task := decodedVideoTapTask(t, ctx, mutableFrameVideoPackets("camera", 64, 64, frames), main)
	defer task.Close()

	slowRelease := make(chan struct{})
	slow := &gateFrameSink{name: "slow", release: slowRelease}
	slowAttachment := attachDropBranch(t, ctx, task, "slow", slow)

	runDone := make(chan error, 1)
	go func() { runDone <- task.Run(ctx) }()

	waitFrameCount(ctx, &main.count, frames)
	if got := main.count.Load(); got != frames {
		t.Fatalf("main sink saw %d/%d frames; the gated slow branch starved the decode tap", got, frames)
	}
	if got := slow.count.Load(); got >= frames {
		t.Fatalf("slow branch saw %d frames; expected it to shed behind its gate", got)
	}
	// The shed must be observable through the public Attachment API, not just
	// inferred from the missing frames.
	if dropped := slowAttachment.Stats().Dropped; dropped == 0 {
		t.Fatal("slow branch Attachment.Stats() reported 0 drops; expected the gated DropOldest branch's sheds to be visible")
	}

	close(slowRelease)
	if err := <-runDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v", err)
	}
}

// TestRuntimeBranchAttachDetachLifecycle exercises the add/remove branch flow the
// gio showcase drives: attach two branches to a live task (both receive frames),
// detach one mid-run, and prove the detached branch stops while the other keeps
// receiving. The "other branch advances while the detached one stays frozen" is
// the deterministic clock — no wall-clock thresholds.
func TestRuntimeBranchAttachDetachLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	main := &frameCountSink{name: "main"}
	task := decodedVideoTapTask(t, ctx, liveMutableVideoPackets("camera", 64, 64, time.Millisecond), main)
	defer task.Close()

	runDone := make(chan error, 1)
	go func() { runDone <- task.Run(ctx) }()
	waitFrameCount(ctx, &main.count, 1) // source is producing

	a := &frameCountSink{name: "a"}
	b := &frameCountSink{name: "b"}
	attachA := attachDropBranch(t, ctx, task, "a", a)
	attachB := attachDropBranch(t, ctx, task, "b", b)

	// Both attached branches receive frames: attach == add branch works.
	waitFrameCount(ctx, &a.count, 10)
	waitFrameCount(ctx, &b.count, 10)
	if a.count.Load() < 10 || b.count.Load() < 10 {
		t.Fatalf("attached branches did not both receive frames: a=%d b=%d", a.count.Load(), b.count.Load())
	}

	// Detach B; Detach waits for its worker to exit, so B's count is final after.
	if err := task.Detach(ctx, attachB); err != nil {
		t.Fatalf("Detach(b) error = %v", err)
	}
	bFinal := b.count.Load()

	// A keeps advancing — the deterministic proof that media still flows — while a
	// detached B must never receive another frame.
	waitFrameCount(ctx, &a.count, a.count.Load()+20)
	if got := b.count.Load(); got != bFinal {
		t.Fatalf("detached branch b received %d frames after detach, want %d (removal did not stop delivery)", got, bFinal)
	}

	if err := task.Detach(ctx, attachA); err != nil {
		t.Fatalf("Detach(a) error = %v", err)
	}
	cancel()
	if err := <-runDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v", err)
	}
}

// TestRuntimeBranchEncodesVP9ThroughDropBuffer is the end-to-end runtime proof
// that a real (govpx) VP9 output branch behind the realtime drop-oldest buffer
// produces packets: the explicit dropping buffer inherits the runtime's frame
// copy bounds (so the buffered edge accepts the decoder's mutable frames) and
// the encoder runs. bundle.New keeps the real VP9 encoder; only VP8 decode is
// faked so the synthetic source decodes to frames the encoder can consume.
func TestRuntimeBranchEncodesVP9ThroughDropBuffer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	main := &frameCountSink{name: "main"}
	task, err := goav.From(liveMutableVideoPackets("camera", 64, 64, time.Millisecond)).
		UseRuntime(bundle.MustNew(goavtest.Codec(av.CodecVP8))).
		Video().
		Decode().
		Shape(shape.Frame(av.MediaVideo, shape.Video(64, 64, av.PixelFormatI420))).
		Tap(goav.FrameTap(lifecycleTapName)).
		To(goav.Sink(main)).
		Build(ctx)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	defer task.Close()

	runDone := make(chan error, 1)
	go func() { runDone <- task.Run(ctx) }()
	waitFrameCount(ctx, &main.count, 1)

	vp9 := &packetCountSink{name: "vp9"}
	attachment, err := task.Attach(ctx, goav.Branch("vp9").
		From(goav.FrameTap(lifecycleTapName)).
		Buffer(flow.DropOldest(8)).
		Resize(64, 64).
		Encode(codec.VP9(codec.Bitrate(320_000))).
		To(goav.Sink(vp9)))
	if err != nil {
		t.Fatalf("Attach(vp9) error = %v", err)
	}

	waitFrameCount(ctx, &vp9.count, 3)
	if got := vp9.count.Load(); got < 3 {
		t.Fatalf("VP9 branch emitted %d packets through the drop-oldest buffer, want >= 3", got)
	}

	if err := task.Detach(ctx, attachment); err != nil {
		t.Fatalf("Detach(vp9) error = %v", err)
	}
	cancel()
	if err := <-runDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v", err)
	}
}
