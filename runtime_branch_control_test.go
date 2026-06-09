package goav

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// TestRuntimeBranchControlPlaneOnLiveTask drives a phase-gated custom source on a
// running task and exercises the branch control plane end to end: a paused branch
// stops receiving while the source and the main sink keep flowing, and resume
// restores delivery.
func TestRuntimeBranchControlPlaneOnLiveTask(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const phases = 3
	release := make(chan struct{}, phases)
	input := Source("generated",
		shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48_000, Stereo, av.SampleFormatS16)),
		func(sctx context.Context, push SourcePush) error {
			for i := 0; i < phases; i++ {
				select {
				case <-release:
				case <-sctx.Done():
					return sctx.Err()
				}
				p := av.Packet{
					StreamID: "generated",
					Payload:  av.Buffer{Bytes: []byte{byte(i + 1)}, Ownership: av.BufferImmutable},
				}
				if err := push.Packet(&p); err != nil {
					return err
				}
			}
			return push.EOS()
		},
	)

	var mainCount, branchCount atomic.Int32
	mainSink := Sink(SinkFunc("main", func(_ context.Context, msg Message) error {
		if msg.Kind == pipeline.MessagePacket {
			mainCount.Add(1)
		}
		return nil
	}))
	task, err := From(input).Audio().Copy().Tap(PacketTap("pkts")).To(mainSink).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	branchSink := Sink(SinkFunc("branch", func(_ context.Context, msg Message) error {
		if msg.Kind == pipeline.MessagePacket {
			branchCount.Add(1)
		}
		return nil
	}))
	att, err := task.Attach(ctx, Branch("watch").From(PacketTap("pkts")).Copy().To(branchSink))
	if err != nil {
		t.Fatal(err)
	}

	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	waitFor := func(label string, want int32, get func() int32) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for get() < want {
			if time.Now().After(deadline) {
				t.Fatalf("%s: got %d, want %d", label, get(), want)
			}
			time.Sleep(time.Millisecond)
		}
	}

	// Phase 1: branch active — both the main sink and the branch receive it.
	release <- struct{}{}
	waitFor("phase1 branch", 1, branchCount.Load)
	waitFor("phase1 main", 1, mainCount.Load)

	// Pause the branch; phase 2 reaches only the main sink.
	if err := att.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	release <- struct{}{}
	waitFor("phase2 main", 2, mainCount.Load)
	if got := branchCount.Load(); got != 1 {
		t.Fatalf("paused branch received %d, want 1 (phase-2 must be skipped)", got)
	}

	// Resume; phase 3 reaches the branch again.
	if err := att.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	release <- struct{}{}
	waitFor("phase3 branch", 2, branchCount.Load)

	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if got := mainCount.Load(); got != 3 {
		t.Fatalf("main sink received %d, want 3 (unaffected by the branch pause)", got)
	}
	if got := branchCount.Load(); got != 2 {
		t.Fatalf("branch received %d, want 2 (phases 1 and 3; phase 2 was paused)", got)
	}
}

// TestRuntimeBranchRebranchSwapsLiveBranch attaches a branch to one sink, then
// rebranches it to a different sink on the running task and verifies the swap:
// the old sink stops receiving and the new one starts, with the source unaffected.
func TestRuntimeBranchRebranchSwapsLiveBranch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const phases = 2
	release := make(chan struct{}, phases)
	input := Source("generated",
		shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48_000, Stereo, av.SampleFormatS16)),
		func(sctx context.Context, push SourcePush) error {
			for i := 0; i < phases; i++ {
				select {
				case <-release:
				case <-sctx.Done():
					return sctx.Err()
				}
				p := av.Packet{StreamID: "generated", Payload: av.Buffer{Bytes: []byte{byte(i + 1)}, Ownership: av.BufferImmutable}}
				if err := push.Packet(&p); err != nil {
					return err
				}
			}
			return push.EOS()
		},
	)

	var mainCount, aCount, bCount atomic.Int32
	count := func(c *atomic.Int32) Destination {
		return Sink(SinkFunc("c", func(_ context.Context, msg Message) error {
			if msg.Kind == pipeline.MessagePacket {
				c.Add(1)
			}
			return nil
		}))
	}
	task, err := From(input).Audio().Copy().Tap(PacketTap("pkts")).To(count(&mainCount)).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	attA, err := task.Attach(ctx, Branch("a").From(PacketTap("pkts")).Copy().To(count(&aCount)))
	if err != nil {
		t.Fatal(err)
	}

	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	waitFor := func(label string, want int32, get func() int32) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for get() < want {
			if time.Now().After(deadline) {
				t.Fatalf("%s: got %d, want %d", label, get(), want)
			}
			time.Sleep(time.Millisecond)
		}
	}

	// Phase 1: branch A receives.
	release <- struct{}{}
	waitFor("phase1 a", 1, aCount.Load)

	// Rebranch A -> B (different sink) on the live task.
	if _, err := attA.Rebranch(ctx, Branch("b").From(PacketTap("pkts")).Copy().To(count(&bCount))); err != nil {
		t.Fatal(err)
	}

	// Phase 2: branch B receives; A is detached.
	release <- struct{}{}
	waitFor("phase2 b", 1, bCount.Load)

	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if got := mainCount.Load(); got != phases {
		t.Fatalf("main received %d, want %d (unaffected by rebranch)", got, phases)
	}
	if got := aCount.Load(); got != 1 {
		t.Fatalf("old branch a received %d, want 1 (detached after phase 1)", got)
	}
	if got := bCount.Load(); got != 1 {
		t.Fatalf("new branch b received %d, want 1 (phase 2 after rebranch)", got)
	}
}
