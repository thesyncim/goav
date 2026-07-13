package goav

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/inspect"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

func waitQoSEvent(t *testing.T, sub inspect.Subscription) av.Event {
	t.Helper()
	select {
	case event, ok := <-sub.Events():
		if !ok {
			t.Fatal("watch subscription closed before a QoS event arrived")
		}
		return event
	case <-time.After(5 * time.Second):
		t.Fatal("no QoS event arrived")
	}
	return av.Event{}
}

// TestQoSReportsLateness pins the playout-gate QoS source: a drop-late gate
// admitting overdue media publishes one av.EventQoS through Watch with the
// exact lateness, the dropped disposition, the stream, and the producing gate
// — and a burst of late messages inside one window yields exactly one report
// (producer-side rate limiting on the task timeline).
func TestQoSReportsLateness(t *testing.T) {
	ctx := context.Background()
	clock := &recordingTestClock{}
	log := newPlayoutArrivalLog(clock)
	release := make(chan struct{})

	live, err := From(playoutFrameSource("cam", av.MediaVideo, release, 0, 20*time.Millisecond, 30*time.Millisecond)).
		UseRuntime(mustNew(WithClock(clock))).
		Video().Playout(flow.Playout("preview").WithDropLate(10 * time.Millisecond)).To(log.sink("preview")).
		BuildLive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	sub := live.Watch(inspect.WatchTypes(av.EventQoS))
	defer sub.Close()

	runErr := make(chan error, 1)
	go func() { runErr <- live.Run(ctx) }()

	release <- struct{}{}
	log.waitArrival(t) // PTS 0 anchors the gate at reading 0

	// The consumer side stalls for 100ms of timeline: PTS 20ms is 80ms overdue
	// and PTS 30ms is 70ms overdue — both shed, one report for the burst.
	clock.Advance(100 * time.Millisecond)
	release <- struct{}{}
	release <- struct{}{}
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}

	report := waitQoSEvent(t, sub)
	if report.StreamID != "cam" || report.Reason != "playout-preview" {
		t.Fatalf("QoS report stream/gate = %q/%q, want cam/playout-preview", report.StreamID, report.Reason)
	}
	lateness, dropped, ok := av.EventQoSReport(&report)
	if !ok || !dropped || lateness != 80*time.Millisecond {
		t.Fatalf("QoS report payload = (%v, %v, %v), want (80ms, dropped, ok)", lateness, dropped, ok)
	}
	// Run has returned, so every gate admit already published what it will
	// ever publish: the second shed must have been rate-limited away.
	select {
	case extra := <-sub.Events():
		t.Fatalf("burst produced a second QoS report inside the window: %+v", extra)
	default:
	}
	if arrivals := log.snapshot(); len(arrivals) != 1 {
		t.Fatalf("sink arrivals = %v, want only the anchor frame", arrivals)
	}
}

// qosEncodeVideoSource is a release-gated I420 frame source with real PTS
// values, so an encode chain downstream produces packets a playout gate can
// schedule and shed deterministically on the fake clock.
func qosEncodeVideoSource(id av.StreamID, release chan struct{}, pts ...time.Duration) InputSpec {
	const width, height = 16, 16
	chromaW, chromaH := (width+1)/2, (height+1)/2
	yPlane := bytes.Repeat([]byte{0x80}, width*height)
	uPlane := bytes.Repeat([]byte{0x80}, chromaW*chromaH)
	vPlane := bytes.Repeat([]byte{0x80}, chromaW*chromaH)
	return Source(string(id),
		shape.Frame(av.MediaVideo, shape.Video(width, height, av.PixelFormatI420), shape.Stream(id)),
		func(sctx context.Context, push SourcePush) error {
			for i := range pts {
				select {
				case <-release:
				case <-sctx.Done():
					return sctx.Err()
				}
				frame := &av.Frame{
					StreamID: id,
					Type:     av.MediaVideo,
					PTS:      av.Timestamp{Value: int64(pts[i]), Base: av.TimeBase{Num: 1, Den: int64(time.Second)}},
					Video:    &av.VideoFrame{Width: width, Height: height, PixelFormat: av.PixelFormatI420},
					Planes: []av.Plane{
						{Buffer: av.Buffer{Bytes: yPlane, Ownership: av.BufferImmutable}, Stride: width},
						{Buffer: av.Buffer{Bytes: uPlane, Ownership: av.BufferImmutable}, Stride: chromaW},
						{Buffer: av.Buffer{Bytes: vPlane, Ownership: av.BufferImmutable}, Stride: chromaW},
					},
				}
				if _, err := push.Frame(frame); err != nil {
					return err
				}
			}
			return push.EOS()
		})
}

// TestQoSPolicyDrivesBitrateControl pins the packaged automatic path: a
// runtime QoS policy maps a playout-gate lateness report to control.SetBitrate,
// the task issues it through its normal Control seam, and the live encoder
// observes av.EventBitrateChanged with the requested rate — exactly what an
// application could do by hand with Watch + Control.
func TestQoSPolicyDrivesBitrateControl(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clock := &recordingTestClock{}
	encoder := &controlRecordingEncoder{}
	release := make(chan struct{})
	arrived := make(chan struct{}, 16)

	rt := mustNew(
		WithClock(clock),
		WithEncoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, &controlRecordingEncoderFactory{encoder: encoder}),
		WithQoSPolicy(func(event av.Event) []control.Control {
			return []control.Control{control.Must(control.SetBitrate(event.StreamID, 250_000))}
		}),
	)
	live, err := From(qosEncodeVideoSource("cam", release, 0, 20*time.Millisecond, 40*time.Millisecond)).
		UseRuntime(rt).
		Video().
		Encode(codec.VP8(codec.Bitrate(600_000))).
		Playout(flow.Playout("qos").WithDropLate(10 * time.Millisecond)).
		To(Sink(SinkFunc("packets", func(_ context.Context, msg Message) error {
			if msg.Kind == pipeline.MessagePacket && msg.Packet != nil {
				select {
				case arrived <- struct{}{}:
				default:
				}
			}
			return nil
		}))).
		BuildLive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()

	runErr := make(chan error, 1)
	go func() { runErr <- live.Run(ctx) }()

	release <- struct{}{}
	select {
	case <-arrived: // PTS 0 anchored the gate and reached the sink
	case <-time.After(5 * time.Second):
		t.Fatal("anchor packet never reached the sink")
	}

	clock.Advance(100 * time.Millisecond)
	release <- struct{}{} // PTS 20ms is 80ms overdue: shed + QoS report

	// The policy's SetBitrate rides the normal control path to the encoder.
	bitrateEvent, err := encoder.waitForEvent(ctx, av.EventBitrateChanged)
	if err != nil {
		t.Fatalf("QoS policy bitrate control never reached the encoder: %v", err)
	}
	bitsPerSecond, ok := codec.EventBitrate(&bitrateEvent)
	if !ok || bitsPerSecond != 250_000 {
		t.Fatalf("encoder bitrate event = %d (ok=%v), want 250000", bitsPerSecond, ok)
	}

	release <- struct{}{} // let the source end naturally
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
}

// TestQoSPolicyRefusalSurfacesThroughWatch pins two contracts: hold-late gates
// report delivered-late media (dropped=false), and a policy control the task
// refuses is published back through Watch as an av.EventQoS carrying the
// refusal on Cause — with no payload, so the policy never reacts to its own
// refusals.
func TestQoSPolicyRefusalSurfacesThroughWatch(t *testing.T) {
	ctx := context.Background()
	clock := &recordingTestClock{}
	log := newPlayoutArrivalLog(clock)
	release := make(chan struct{})

	live, err := From(playoutFrameSource("cam", av.MediaVideo, release, 0, 20*time.Millisecond, 40*time.Millisecond)).
		UseRuntime(mustNew(
			WithClock(clock),
			WithQoSPolicy(func(av.Event) []control.Control {
				return []control.Control{control.Keyframe("cam").AtTap("nope")}
			}),
		)).
		Video().Playout(flow.Playout("preview")).To(log.sink("preview")).
		BuildLive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	sub := live.Watch(inspect.WatchTypes(av.EventQoS))
	defer sub.Close()

	runErr := make(chan error, 1)
	go func() { runErr <- live.Run(ctx) }()

	release <- struct{}{}
	log.waitArrival(t) // PTS 0 anchors the gate

	clock.Advance(100 * time.Millisecond)
	release <- struct{}{}
	log.waitArrival(t) // PTS 20ms delivered 80ms late (hold-late)

	report := waitQoSEvent(t, sub)
	lateness, dropped, ok := av.EventQoSReport(&report)
	if !ok || dropped || lateness != 80*time.Millisecond {
		t.Fatalf("hold-late QoS report = (%v, %v, %v), want (80ms, delivered-late, ok)", lateness, dropped, ok)
	}

	refusal := waitQoSEvent(t, sub)
	if _, _, ok := av.EventQoSReport(&refusal); ok {
		t.Fatalf("refusal event carries a report payload, would loop the policy: %+v", refusal)
	}
	if refusal.Reason != "qos policy control refused" || refusal.Cause == nil {
		t.Fatalf("refusal event = %+v, want the refusal reason and Cause", refusal)
	}
	if !errors.Is(refusal.Cause, pipeline.ErrUnknownNode) {
		t.Fatalf("refusal cause = %v, want the task's unknown-tap refusal", refusal.Cause)
	}

	release <- struct{}{}
	log.waitArrival(t)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
}
