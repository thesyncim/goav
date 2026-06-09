package goav

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
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
