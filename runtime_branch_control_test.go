package goav

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/lifecycle"
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
		shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48_000, codec.Stereo, av.SampleFormatS16)),
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
				if _, err := push.Packet(&p); err != nil {
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
		shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48_000, codec.Stereo, av.SampleFormatS16)),
		func(sctx context.Context, push SourcePush) error {
			for i := 0; i < phases; i++ {
				select {
				case <-release:
				case <-sctx.Done():
					return sctx.Err()
				}
				p := av.Packet{StreamID: "generated", Payload: av.Buffer{Bytes: []byte{byte(i + 1)}, Ownership: av.BufferImmutable}}
				if _, err := push.Packet(&p); err != nil {
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

// rebranchPacketSource is a phase-gated packet source: each receive on release
// pushes one packet whose payload is the 1-based phase number, with the
// keyframe flag taken from the phases slice.
func rebranchPacketSource(phases []bool, release <-chan struct{}) InputSpec {
	return Source("generated",
		shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48_000, codec.Stereo, av.SampleFormatS16)),
		func(sctx context.Context, push SourcePush) error {
			for i := range phases {
				select {
				case <-release:
				case <-sctx.Done():
					return sctx.Err()
				}
				p := av.Packet{
					StreamID: "generated",
					Keyframe: phases[i],
					Payload:  av.Buffer{Bytes: []byte{byte(i + 1)}, Ownership: av.BufferImmutable},
				}
				if _, err := push.Packet(&p); err != nil {
					return err
				}
			}
			return push.EOS()
		},
	)
}

func waitForCount(t *testing.T, label string, want int32, get func() int32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for get() < want {
		if time.Now().After(deadline) {
			t.Fatalf("%s: got %d, want %d", label, get(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitForCondition(t *testing.T, label string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: condition not reached", label)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestRebranchSwitchAtNextKeyframeOnPacketStream rebranches a live packet
// branch with SwitchAt(NextKeyframe()): the old branch keeps receiving while
// non-key packets flow, the new branch sheds them, the new branch's first
// delivered message is the keyframe itself, and the old branch is detached at
// that boundary and receives nothing afterwards.
func TestRebranchSwitchAtNextKeyframeOnPacketStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	phases := []bool{false, false, true, false} // keyframe arrives at phase 3
	release := make(chan struct{}, len(phases))
	input := rebranchPacketSource(phases, release)

	var aCount, bCount atomic.Int32
	var bPayloads []byte
	var bKeyframes []bool
	var bMu sync.Mutex
	aSink := Sink(SinkFunc("a-sink", func(_ context.Context, msg Message) error {
		if msg.Kind == pipeline.MessagePacket {
			aCount.Add(1)
		}
		return nil
	}))
	bSink := Sink(SinkFunc("b-sink", func(_ context.Context, msg Message) error {
		if msg.Kind == pipeline.MessagePacket {
			bMu.Lock()
			bPayloads = append(bPayloads, msg.Packet.Payload.Bytes[0])
			bKeyframes = append(bKeyframes, msg.Packet.Keyframe)
			bMu.Unlock()
			bCount.Add(1)
		}
		return nil
	}))

	task, err := From(input).Audio().Copy().Tap(PacketTap("pkts")).To(Sink(SinkFunc("main", func(context.Context, Message) error { return nil }))).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	attA, err := task.Attach(ctx, Branch("a").From(PacketTap("pkts")).Copy().To(aSink))
	if err != nil {
		t.Fatal(err)
	}

	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	// Phase 1 (non-key): the old branch receives it.
	release <- struct{}{}
	waitForCount(t, "phase1 a", 1, aCount.Load)

	attB, err := attA.Rebranch(ctx,
		Branch("b").From(PacketTap("pkts")).Copy().To(bSink),
		SwitchAt(NextKeyframe()),
		KeepOldOnFailure(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if attB == nil {
		t.Fatal("rebranch returned no replacement attachment")
	}

	// Phase 2 (non-key): the old branch still receives; the gated new branch
	// sheds it. Wait until the gate has actually handled the packet before
	// asserting the new sink stayed empty.
	release <- struct{}{}
	waitForCount(t, "phase2 a", 2, aCount.Load)
	waitForCondition(t, "phase2 gate handled", func() bool {
		return task.Stats().Nodes["b/switch-gate"].InMessages >= 1
	})
	if got := bCount.Load(); got != 0 {
		t.Fatalf("gated branch received %d packets before the keyframe, want 0", got)
	}

	// Phase 3 (keyframe): the boundary. The new branch's first message IS the
	// keyframe, and the old branch detaches at the same boundary.
	release <- struct{}{}
	waitForCount(t, "phase3 b", 1, bCount.Load)
	bMu.Lock()
	firstPayload, firstKeyframe := bPayloads[0], bKeyframes[0]
	bMu.Unlock()
	if firstPayload != 3 || !firstKeyframe {
		t.Fatalf("new branch first message payload=%d keyframe=%v, want the phase-3 keyframe", firstPayload, firstKeyframe)
	}
	waitForCondition(t, "old branch detached at boundary", func() bool {
		return attA.Snapshot().State == lifecycle.BranchDetached
	})
	aBefore := aCount.Load()

	// Phase 4 (non-key): only the new branch receives.
	release <- struct{}{}
	waitForCount(t, "phase4 b", 2, bCount.Load)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if got := aCount.Load(); got != aBefore {
		t.Fatalf("old branch received %d packets after its detach boundary, want %d", got, aBefore)
	}
	bMu.Lock()
	defer bMu.Unlock()
	if len(bPayloads) != 2 || bPayloads[1] != 4 {
		t.Fatalf("new branch payloads = %v, want [3 4]", bPayloads)
	}
}

// TestRebranchSwitchAtNextFrameOnFrameStream rebranches a live frame branch
// with SwitchAt(NextFrame()): the new branch's first delivered message is the
// next frame after the rebranch, and the old branch detaches at that frame.
func TestRebranchSwitchAtNextFrameOnFrameStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const frames = 3
	release := make(chan struct{}, frames)
	input := Source("pcm",
		shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Stereo, av.SampleFormatS16)),
		func(sctx context.Context, push SourcePush) error {
			for i := 0; i < frames; i++ {
				select {
				case <-release:
				case <-sctx.Done():
					return sctx.Err()
				}
				frame := av.Frame{
					StreamID: "pcm",
					Type:     av.MediaAudio,
					Audio: &av.AudioFrame{
						SampleRate:   48_000,
						Channels:     codec.Stereo,
						SampleFormat: av.SampleFormatS16,
						Samples:      1,
					},
					Planes: []av.Plane{{Buffer: av.Buffer{Bytes: []byte{byte(i + 1), 0, byte(i + 1), 0}, Ownership: av.BufferImmutable}}},
				}
				if _, err := push.Frame(&frame); err != nil {
					return err
				}
			}
			return push.EOS()
		},
	)

	var aCount, bCount atomic.Int32
	var bFirst atomic.Int32
	aSink := Sink(SinkFunc("a-sink", func(_ context.Context, msg Message) error {
		if msg.Kind == pipeline.MessageFrame {
			aCount.Add(1)
		}
		return nil
	}))
	bSink := Sink(SinkFunc("b-sink", func(_ context.Context, msg Message) error {
		if msg.Kind == pipeline.MessageFrame {
			bFirst.CompareAndSwap(0, int32(msg.Frame.Planes[0].Buffer.Bytes[0]))
			bCount.Add(1)
		}
		return nil
	}))

	task, err := From(input).Audio().Tap(FrameTap("frames")).To(Sink(SinkFunc("main", func(context.Context, Message) error { return nil }))).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	attA, err := task.Attach(ctx, Branch("a").From(FrameTap("frames")).To(aSink))
	if err != nil {
		t.Fatal(err)
	}

	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	// Frame 1: the old branch receives it.
	release <- struct{}{}
	waitForCount(t, "frame1 a", 1, aCount.Load)

	if _, err := attA.Rebranch(ctx,
		Branch("b").From(FrameTap("frames")).To(bSink),
		SwitchAt(NextFrame()),
	); err != nil {
		t.Fatal(err)
	}

	// Frame 2: the boundary. The new branch's first message is this frame and
	// the old branch detaches at it.
	release <- struct{}{}
	waitForCount(t, "frame2 b", 1, bCount.Load)
	if got := bFirst.Load(); got != 2 {
		t.Fatalf("new branch first frame payload = %d, want 2 (the next frame after the rebranch)", got)
	}
	waitForCondition(t, "old branch detached at boundary", func() bool {
		return attA.Snapshot().State == lifecycle.BranchDetached
	})
	aBefore := aCount.Load()

	// Frame 3: only the new branch receives.
	release <- struct{}{}
	waitForCount(t, "frame3 b", 2, bCount.Load)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if got := aCount.Load(); got != aBefore {
		t.Fatalf("old branch received %d frames after its detach boundary, want %d", got, aBefore)
	}
}

// TestRebranchFailureKeepsOldBranchAttached pins the failure policy spelled by
// KeepOldOnFailure: when attaching the replacement fails, the old branch stays
// attached and keeps receiving — including on the SwitchAt path.
func TestRebranchFailureKeepsOldBranchAttached(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	phases := []bool{false, false}
	release := make(chan struct{}, len(phases))
	input := rebranchPacketSource(phases, release)

	var aCount atomic.Int32
	aSink := Sink(SinkFunc("a-sink", func(_ context.Context, msg Message) error {
		if msg.Kind == pipeline.MessagePacket {
			aCount.Add(1)
		}
		return nil
	}))

	task, err := From(input).Audio().Copy().Tap(PacketTap("pkts")).To(Sink(SinkFunc("main", func(context.Context, Message) error { return nil }))).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	attA, err := task.Attach(ctx, Branch("a").From(PacketTap("pkts")).Copy().To(aSink))
	if err != nil {
		t.Fatal(err)
	}

	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	release <- struct{}{}
	waitForCount(t, "phase1 a", 1, aCount.Load)

	next, err := attA.Rebranch(ctx,
		Branch("bad").From(PacketTap("missing")).Copy().To(Sink(SinkFunc("bad-sink", func(context.Context, Message) error { return nil }))),
		SwitchAt(NextKeyframe()),
		KeepOldOnFailure(),
	)
	if err == nil {
		t.Fatal("rebranch to a missing tap succeeded, want error")
	}
	if next != nil {
		t.Fatalf("failed rebranch returned attachment %v, want nil", next.ID())
	}
	if got := attA.Snapshot().State; got != lifecycle.BranchAttached {
		t.Fatalf("old branch state after failed rebranch = %v, want %v", got, lifecycle.BranchAttached)
	}

	// The old branch keeps receiving after the failed rebranch.
	release <- struct{}{}
	waitForCount(t, "phase2 a", 2, aCount.Load)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
}

// TestRebranchOldBranchDispositionReportsDestinationStates pins the drain and
// abort dispositions: DrainOldBranch commits the replaced branch's
// destinations, AbortOldBranch aborts them, and without a disposition the
// detached branch reports plain closed — today's behavior.
func TestRebranchOldBranchDispositionReportsDestinationStates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	phases := []bool{false}
	release := make(chan struct{}, len(phases))
	input := rebranchPacketSource(phases, release)

	sink := func(name string) Destination {
		return Sink(SinkFunc(name, func(context.Context, Message) error { return nil }))
	}
	task, err := From(input).Audio().Copy().Tap(PacketTap("pkts")).To(sink("main")).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	attA, err := task.Attach(ctx, Branch("a").From(PacketTap("pkts")).Copy().To(sink("a-sink")))
	if err != nil {
		t.Fatal(err)
	}

	destinationState := func(att Attachment, label string) lifecycle.DestinationState {
		t.Helper()
		snap := att.Snapshot()
		if snap.State != lifecycle.BranchDetached {
			t.Fatalf("%s: state = %v, want %v", label, snap.State, lifecycle.BranchDetached)
		}
		if len(snap.Destinations) != 1 {
			t.Fatalf("%s: destinations = %d, want 1", label, len(snap.Destinations))
		}
		if snap.Destinations[0].Open {
			t.Fatalf("%s: destination still reports open", label)
		}
		return snap.Destinations[0].State
	}

	// Drain: the replaced branch's destination commits.
	attB, err := attA.Rebranch(ctx,
		Branch("b").From(PacketTap("pkts")).Copy().To(sink("b-sink")),
		DrainOldBranch(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := destinationState(attA, "drained branch a"); got != lifecycle.DestinationCommitted {
		t.Fatalf("drained branch destination state = %v, want %v", got, lifecycle.DestinationCommitted)
	}

	// Abort: the replaced branch's destination aborts.
	attC, err := attB.Rebranch(ctx,
		Branch("c").From(PacketTap("pkts")).Copy().To(sink("c-sink")),
		AbortOldBranch(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := destinationState(attB, "aborted branch b"); got != lifecycle.DestinationAborted {
		t.Fatalf("aborted branch destination state = %v, want %v", got, lifecycle.DestinationAborted)
	}

	// Default: a plain detach keeps reporting closed.
	if err := task.Detach(ctx, attC); err != nil {
		t.Fatal(err)
	}
	if got := destinationState(attC, "detached branch c"); got != lifecycle.DestinationClosed {
		t.Fatalf("plain detached branch destination state = %v, want %v", got, lifecycle.DestinationClosed)
	}

	release <- struct{}{}
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
}
