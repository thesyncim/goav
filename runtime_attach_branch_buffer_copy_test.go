package goav_test

import (
	"context"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/source"
)

// TestRuntimeBranchExplicitDropBufferInheritsCopyBounds pins that an explicit
// realtime branch buffer (flow.DropOldest/Latest/...) inherits the copy bounds
// the runtime already sized for the graph's shapes — exactly like the default
// branch buffer does. Without that inheritance a buffered branch edge refuses
// the mutable decoded frames a real codec produces with
// pipeline.ErrBufferedMessageUnsafe, forcing callers to redundantly pass
// flow.BufferCopyBounds just to use a dropping buffer.
//
// The decoder fabricates mutable (graph-owned) frames, so the branch buffer
// must copy them into its own backing; the regression is only observable with
// mutable payloads, which is why immutable-fixture tests never caught it.
func TestRuntimeBranchExplicitDropBufferInheritsCopyBounds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out := goavtest.NewCollector()
	task, err := goav.From(mutableFrameVideoPackets("camera", 64, 64, 24)).
		UseRuntime(goavtest.Runtime()).
		Video().
		Decode().
		Shape(shape.Frame(av.MediaVideo, shape.Video(64, 64, av.PixelFormatI420))).
		Tap(goav.FrameTap("video.decoded")).
		To(out.Sink()).
		BuildLive(ctx)
	if err != nil {
		t.Fatalf("BuildLive() error = %v", err)
	}
	t.Cleanup(func() { _ = task.Close() })

	preview := goavtest.NewCollector()
	if _, err := task.Attach(ctx,
		goav.Branch("preview").
			From(goav.FrameTap("video.decoded")).
			Buffer(flow.DropOldest(8)).
			Resize(64, 64).
			To(preview.Sink()),
	); err != nil {
		t.Fatalf("Attach() error = %v", err)
	}

	if err := task.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v; a realtime drop-oldest branch must inherit the runtime copy bounds for mutable frames", err)
	}

	if len(preview.Frames()) == 0 {
		t.Fatalf("drop-oldest branch received no frames; the buffered branch edge dropped every mutable frame")
	}
}

// mutableFrameVideoPackets is a finite packet-domain video source. The
// passthrough decoder fabricates a fresh, mutable (graph-owned) frame for each
// foreign packet, so frames reaching a buffered branch edge must be copied
// into the branch's own backing rather than shared by reference.
func mutableFrameVideoPackets(name string, width, height, count int) goav.InputSpec {
	return goav.Source(name,
		shape.Packet(av.MediaVideo, av.CodecVP8, shape.Video(width, height, av.PixelFormatI420)),
		func(_ context.Context, push source.Push) error {
			base := av.TimeBase{Num: 1, Den: 90_000}
			for n := 0; n < count; n++ {
				packet := &av.Packet{
					StreamID: av.StreamID(name),
					Type:     av.MediaVideo,
					Keyframe: true,
					PTS:      av.Timestamp{Value: int64(n) * 3000, Base: base},
					DTS:      av.Timestamp{Value: int64(n) * 3000, Base: base},
					Duration: av.Duration{Value: 3000, Base: base},
					Payload:  av.Buffer{Bytes: []byte{byte(n)}, Ownership: av.BufferImmutable},
				}
				if _, err := push.Packet(packet); err != nil {
					return err
				}
			}
			return push.EOS()
		})
}
