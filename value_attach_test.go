package goav

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

// TestValueOnStreamAttachesPerDiscoveredStream pins OnStream in the value
// grammar: Outputs built from input.OnStream(match) attach for each stream
// the input discovers, named per stream, beside the recipe's other Outputs.
func TestValueOnStreamAttachesPerDiscoveredStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stopLate := make(chan struct{})
	finish := make(chan struct{})
	late := lateOpusStream("late-audio")
	input := lateStreamSource(late, 0xA, stopLate, finish)
	state := &syncObjectState{}
	rt := mustNew(withTestFormats(testFormatMuxer(av.FormatOgg, &syncTestMuxer{})))
	record := Writer("s3://bucket/late.ogg", state.open, Format(av.FormatOgg), MIME("audio/ogg"))
	var mainCount atomic.Int32
	mainSink := Sink(SinkFunc("main", func(_ context.Context, msg Message) error {
		if msg.Kind == pipeline.MessagePacket && msg.Packet.StreamID == "mic" {
			mainCount.Add(1)
		}
		return nil
	}))

	task, err := rt.Build(ctx,
		input.OnStream(MatchMedia(av.MediaAudio)).As("record").To(record),
		input.Audio().To(mainSink),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	waitForCondition(t, "late branch destination received media", func() bool {
		_, _, _, _, data, _ := state.snapshot()
		return strings.Contains(string(data), string([]byte{0xA}))
	})
	waitForCondition(t, "late branch attached", func() bool {
		return taskHasBranch(task, "record-late-audio")
	})
	close(stopLate)
	waitForCondition(t, "late branch detached", func() bool {
		return !taskHasBranch(task, "record-late-audio")
	})
	close(finish)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if mainCount.Load() < 1 {
		t.Fatal("the recipe's own Output received no packets")
	}
}

// TestValueAttachFromTap pins task.Attach in the value grammar: an Output
// built from task.Tap(name) joins the running task and receives the tapped
// media, and Detach removes it.
func TestValueAttachFromTap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	in := controlLiveVideoSource("cam")
	main := Sink(SinkFunc("main", func(context.Context, Message) error { return nil }))
	task, err := testBundleRuntime().Build(ctx, in.Video().Tap("live").To(main))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	var frames atomic.Int32
	monitor := Sink(SinkFunc("monitor", func(_ context.Context, msg Message) error {
		if msg.Frame != nil {
			frames.Add(1)
		}
		return nil
	}))
	var attachment Attachment
	waitForCondition(t, "monitor attached", func() bool {
		attachment, err = task.Attach(ctx, task.Tap("live").As("monitor").To(monitor))
		return err == nil
	})
	waitForCondition(t, "monitor received frames", func() bool { return frames.Load() > 0 })
	if err := task.Detach(ctx, attachment); err != nil {
		t.Fatalf("detach: %v", err)
	}
	cancel()
	<-runErr
}

// TestValueAttachTrackOfFrameSource pins that a Track of a frame-domain input
// attaches as frames: an Output without Decode implies a packet copy only for
// packet inputs.
func TestValueAttachTrackOfFrameSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	in := controlLiveVideoSource("cam")
	main := Sink(SinkFunc("main", func(context.Context, Message) error { return nil }))
	task, err := testBundleRuntime().Build(ctx, in.Video().To(main))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	var frames atomic.Int32
	monitor := Sink(SinkFunc("monitor", func(_ context.Context, msg Message) error {
		if msg.Frame != nil {
			frames.Add(1)
		}
		return nil
	}))
	track := task.Track(in, av.Stream{ID: "cam", Type: av.MediaVideo})
	if _, err := task.Attach(ctx, track.As("monitor").To(monitor)); err != nil {
		t.Fatalf("attach: %v", err)
	}
	waitForCondition(t, "monitor received frames", func() bool { return frames.Load() > 0 })
	cancel()
	<-runErr
}

// TestValueRebranchTakesOutputs pins Attachment.Rebranch in the value
// grammar: a replacement Output from task.Tap takes over from the attached
// one, which detaches.
func TestValueRebranchTakesOutputs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	in := controlLiveVideoSource("cam")
	main := Sink(SinkFunc("main", func(context.Context, Message) error { return nil }))
	task, err := testBundleRuntime().Build(ctx, in.Video().Tap("live").To(main))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	counter := func(name string, n *atomic.Int32) Destination {
		return Sink(SinkFunc(name, func(_ context.Context, msg Message) error {
			if msg.Frame != nil {
				n.Add(1)
			}
			return nil
		}))
	}
	var before, after atomic.Int32
	var attachment Attachment
	waitForCondition(t, "first monitor attached", func() bool {
		attachment, err = task.Attach(ctx, task.Tap("live").As("first").To(counter("first", &before)))
		return err == nil
	})
	next, err := attachment.Rebranch(ctx, task.Tap("live").As("second").To(counter("second", &after)))
	if err != nil {
		t.Fatalf("rebranch: %v", err)
	}
	waitForCondition(t, "replacement received frames", func() bool { return after.Load() > 0 })
	if taskHasBranch(task, "first") || !taskHasBranch(task, "second") {
		t.Fatal("rebranch did not swap first for second")
	}
	if err := task.Detach(ctx, next); err != nil {
		t.Fatalf("detach: %v", err)
	}
	cancel()
	<-runErr
}
