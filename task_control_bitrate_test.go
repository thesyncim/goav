package goav

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// TestTaskControlSetBitrateBroadcastsBitrateEvent is the delivery half of
// north-star #33 ("SetBitrate reaches encoder or fails clearly"): an
// untargeted SetBitrate lowers to av.EventBitrateChanged carrying the rate in
// event metadata, enters the graph at the entry row, and rides the data path
// downstream to encoders — the exact route Keyframe takes.
func TestTaskControlSetBitrateBroadcastsBitrateEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	graph, err := pipeline.NewGraph(pipeline.GraphConfig{
		Name:   "bitrate-broadcast",
		Buffer: pipeline.BufferPolicy{Capacity: 8, Drop: pipeline.DropOldest},
	})
	if err != nil {
		t.Fatal(err)
	}
	source := &controlFrameSource{name: "source", a: "a", b: "b"}
	capture := &controlEventCapture{name: "first"}
	sink := newControlSink("sink")
	if _, err := graph.AddSource(source, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddStage(capture, pipeline.BufferPolicy{Capacity: 32, Drop: pipeline.DropBlock}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(pipeline.Route{From: "source", To: []string{"first"}, Policy: pipeline.RouteAll}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(pipeline.Route{From: "first", To: []string{"sink"}, Policy: pipeline.RouteAll}); err != nil {
		t.Fatal(err)
	}

	task := newTask(graph, nil)
	t.Cleanup(func() { _ = task.Close() })
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	// No At / AtTap: the retarget enters at the graph's entry row (the node fed
	// by the source) and rides the data path downstream, like Keyframe.
	if err := controlUntilAccepted(ctx, task, SetBitrate("a", 250_000)); err != nil {
		t.Fatalf("untargeted SetBitrate: %v", err)
	}
	event, err := capture.waitForEvent(ctx, av.EventBitrateChanged)
	if err != nil {
		t.Fatalf("bitrate retarget never reached the entry row: %v", err)
	}
	if event.StreamID != "a" {
		t.Fatalf("bitrate event stream = %q, want a", event.StreamID)
	}
	bitsPerSecond, ok := codec.EventBitrate(&event)
	if !ok || bitsPerSecond != 250_000 {
		t.Fatalf("event bitrate = %d (ok=%v), want 250000", bitsPerSecond, ok)
	}

	cancel()
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v", err)
	}
}

// TestTaskControlSetBitrateRejectsNonPositiveRate is the "fails clearly" half
// of north-star #33: a non-positive rate is rejected at the Control seam with
// an explanatory error before anything is injected into the graph.
func TestTaskControlSetBitrateRejectsNonPositiveRate(t *testing.T) {
	graph, err := pipeline.NewGraph(pipeline.GraphConfig{
		Name:   "bitrate-reject",
		Buffer: pipeline.BufferPolicy{Capacity: 8, Drop: pipeline.DropOldest},
	})
	if err != nil {
		t.Fatal(err)
	}
	source := &controlFrameSource{name: "source", a: "a", b: "b"}
	sink := newControlSink("sink")
	if _, err := graph.AddSource(source, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(pipeline.Route{From: "source", To: []string{"sink"}, Policy: pipeline.RouteAll}); err != nil {
		t.Fatal(err)
	}
	task := newTask(graph, nil)
	t.Cleanup(func() { _ = task.Close() })

	for _, rate := range []int{0, -1} {
		err := task.Control(context.Background(), SetBitrate("a", rate))
		if err == nil || !strings.Contains(err.Error(), "positive") {
			t.Fatalf("SetBitrate(%d) err = %v, want a positive-rate rejection", rate, err)
		}
	}
}

func TestDefaultRealtimeEncodeRecipeSupportsLiveEncoderControls(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	encoder := &controlRecordingEncoder{}
	rt := New(WithEncoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, &controlRecordingEncoderFactory{encoder: encoder}))
	built, err := From(controlLiveVideoSource("cam")).
		Video().
		Encode(codec.VP8(codec.Bitrate(600_000))).
		To(Sink(SinkFunc("packets", func(context.Context, Message) error { return nil }))).
		UseRuntime(rt).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task := built.(*task)
	t.Cleanup(func() { _ = task.Close() })

	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	if err := controlUntilAccepted(ctx, task, SetBitrate("cam", 250_000)); err != nil {
		t.Fatalf("default realtime encode SetBitrate: %v", err)
	}
	bitrateEvent, err := encoder.waitForEvent(ctx, av.EventBitrateChanged)
	if err != nil {
		t.Fatalf("bitrate event never reached default-built encoder: %v", err)
	}
	bitsPerSecond, ok := codec.EventBitrate(&bitrateEvent)
	if !ok || bitsPerSecond != 250_000 {
		t.Fatalf("encoder bitrate event = %d (ok=%v), want 250000", bitsPerSecond, ok)
	}

	if err := controlUntilAccepted(ctx, task, Keyframe("cam")); err != nil {
		t.Fatalf("default realtime encode Keyframe: %v", err)
	}
	if _, err := encoder.waitForEvent(ctx, av.EventKeyframeRequired); err != nil {
		t.Fatalf("keyframe event never reached default-built encoder: %v", err)
	}

	cancel()
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v", err)
	}
}

func TestOfflineEncodeRecipeKeepsDirectRunnerForEncoderControls(t *testing.T) {
	ctx := context.Background()
	rt := New(
		WithRealtime(false),
		WithEncoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, &controlRecordingEncoderFactory{}),
	)
	built, err := From(controlLiveVideoSource("cam")).
		Video().
		Encode(codec.VP8(codec.Bitrate(600_000))).
		To(Sink(SinkFunc("packets", func(context.Context, Message) error { return nil }))).
		UseRuntime(rt).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task := built.(*task)
	t.Cleanup(func() { _ = task.Close() })

	err = task.Control(ctx, SetBitrate("cam", 250_000))
	if !errors.Is(err, ErrControlUnsupported) {
		t.Fatalf("offline encode SetBitrate err = %v, want ErrControlUnsupported from direct runner", err)
	}
}

type controlRecordingEncoderFactory struct {
	encoder *controlRecordingEncoder
}

func (f *controlRecordingEncoderFactory) NewEncoder(context.Context, codec.EncodeConfig) (codec.Encoder, error) {
	if f.encoder == nil {
		f.encoder = &controlRecordingEncoder{}
	}
	return f.encoder, nil
}

type controlRecordingEncoder struct {
	encodeTestEncoder
	mu     sync.Mutex
	events []av.Event
}

func (e *controlRecordingEncoder) HandleEvent(_ context.Context, event *av.Event) error {
	if event == nil {
		return nil
	}
	e.mu.Lock()
	e.events = append(e.events, *event)
	e.mu.Unlock()
	return nil
}

func (e *controlRecordingEncoder) waitForEvent(ctx context.Context, eventType av.EventType) (av.Event, error) {
	for {
		e.mu.Lock()
		for _, event := range e.events {
			if event.Type == eventType {
				e.mu.Unlock()
				return event, nil
			}
		}
		e.mu.Unlock()
		select {
		case <-ctx.Done():
			return av.Event{}, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

func controlLiveVideoSource(id av.StreamID) InputSpec {
	const (
		width  = 16
		height = 16
	)
	chromaW := (width + 1) / 2
	chromaH := (height + 1) / 2
	yPlane := bytes.Repeat([]byte{0x80}, width*height)
	uPlane := bytes.Repeat([]byte{0x80}, chromaW*chromaH)
	vPlane := bytes.Repeat([]byte{0x80}, chromaW*chromaH)
	return Source(string(id),
		shape.Frame(av.MediaVideo, shape.Video(width, height, av.PixelFormatI420), shape.Stream(id)),
		func(ctx context.Context, push SourcePush) error {
			frame := &av.Frame{
				StreamID: id,
				Type:     av.MediaVideo,
				Video:    &av.VideoFrame{Width: width, Height: height, PixelFormat: av.PixelFormatI420},
				Planes: []av.Plane{
					{Buffer: av.Buffer{Bytes: yPlane, Ownership: av.BufferImmutable}, Stride: width},
					{Buffer: av.Buffer{Bytes: uPlane, Ownership: av.BufferImmutable}, Stride: chromaW},
					{Buffer: av.Buffer{Bytes: vPlane, Ownership: av.BufferImmutable}, Stride: chromaW},
				},
			}
			for {
				if ctx.Err() != nil {
					return nil
				}
				if _, err := push.Frame(frame); err != nil {
					if errors.Is(err, ErrBackpressure) {
						continue
					}
					return nil
				}
			}
		})
}
