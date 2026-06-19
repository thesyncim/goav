package goav

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

type syncCollectEmitter struct {
	mu       sync.Mutex
	messages int
}

func (e *syncCollectEmitter) Emit(context.Context, *pipeline.Message) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.messages++
	return nil
}

func (e *syncCollectEmitter) Count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.messages
}

type syncNoopEmitter struct{}

func (syncNoopEmitter) Emit(context.Context, *pipeline.Message) error { return nil }

func TestSyncDropLateDropsBehindSharedTimeline(t *testing.T) {
	policy := Sync("room", SyncTolerance(10*time.Millisecond), SyncDropLate())
	audio := newSyncGate(policy)
	video := newSyncGate(policy)
	emit := &syncCollectEmitter{}

	if err := audio.Handle(context.Background(), syncPacketMessage("a", 100*time.Millisecond), emit); err != nil {
		t.Fatal(err)
	}
	if err := video.Handle(context.Background(), syncPacketMessage("v", 50*time.Millisecond), emit); err != nil {
		t.Fatal(err)
	}
	if got := emit.Count(); got != 1 {
		t.Fatalf("emitted messages = %d, want only first message", got)
	}
	if got := video.DroppedMessages(); got != 1 {
		t.Fatalf("video dropped = %d, want 1", got)
	}
}

func TestSyncDropLateNormalizesMultipleTimebases(t *testing.T) {
	policy := Sync("room", SyncTolerance(5*time.Millisecond), SyncDropLate())
	audio := newSyncGate(policy)
	video := newSyncGate(policy)
	emit := &syncCollectEmitter{}

	if err := audio.Handle(context.Background(), syncPacketMessageWithTimestamp("a", 4800, av.RTPTimeBase(48000)), emit); err != nil {
		t.Fatal(err)
	}
	if err := video.Handle(context.Background(), syncPacketMessageWithTimestamp("v", 4500, av.RTPTimeBase(90000)), emit); err != nil {
		t.Fatal(err)
	}
	if err := video.Handle(context.Background(), syncPacketMessageWithTimestamp("v", 9000, av.RTPTimeBase(90000)), emit); err != nil {
		t.Fatal(err)
	}
	if got := emit.Count(); got != 2 {
		t.Fatalf("emitted messages = %d, want audio plus caught-up video", got)
	}
	if got := video.DroppedMessages(); got != 1 {
		t.Fatalf("video dropped = %d, want one 50ms packet dropped against 100ms audio", got)
	}
}

func TestSyncDiscontinuityResetsTimeline(t *testing.T) {
	policy := Sync("room", SyncTolerance(10*time.Millisecond), SyncDropLate())
	audio := newSyncGate(policy)
	video := newSyncGate(policy)
	emit := &syncCollectEmitter{}

	if err := audio.Handle(context.Background(), syncPacketMessage("a", 100*time.Millisecond), emit); err != nil {
		t.Fatal(err)
	}
	if err := video.Handle(context.Background(), syncPacketMessage("v", 50*time.Millisecond), emit); err != nil {
		t.Fatal(err)
	}
	if got := video.DroppedMessages(); got != 1 {
		t.Fatalf("video dropped before reset = %d, want 1", got)
	}
	if err := audio.Handle(context.Background(), syncEventMessage(av.Event{Type: av.EventDiscontinuity, StreamID: "a"}), emit); err != nil {
		t.Fatal(err)
	}
	if err := video.Handle(context.Background(), syncPacketMessage("v", 50*time.Millisecond), emit); err != nil {
		t.Fatal(err)
	}
	if got := video.DroppedMessages(); got != 1 {
		t.Fatalf("video dropped after reset = %d, want still 1", got)
	}
	if got := emit.Count(); got != 3 {
		t.Fatalf("emitted messages = %d, want first audio, reset event, and post-reset video", got)
	}
}

func TestSyncHoldLateWaitsForSlowStream(t *testing.T) {
	policy := Sync("room", SyncTolerance(10*time.Millisecond))
	blocked := make(chan struct{}, 1)
	release := make(chan struct{})
	policy.scheduler.wait = func(ctx context.Context) error {
		select {
		case blocked <- struct{}{}:
		default:
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
			return nil
		}
	}
	audio := newSyncGate(policy)
	video := newSyncGate(policy)
	emit := &syncCollectEmitter{}

	if err := video.Handle(context.Background(), syncPacketMessage("v", 0), emit); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- audio.Handle(context.Background(), syncPacketMessage("a", 100*time.Millisecond), emit)
	}()
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("audio sync did not wait for slow stream")
	}
	select {
	case err := <-done:
		t.Fatalf("audio sync returned before slow stream caught up: %v", err)
	default:
	}
	if err := video.Handle(context.Background(), syncPacketMessage("v", 95*time.Millisecond), emit); err != nil {
		t.Fatal(err)
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("audio sync did not release after slow stream caught up")
	}
	if got := emit.Count(); got != 3 {
		t.Fatalf("emitted messages = %d, want 3", got)
	}
}

func TestSyncGateRequiresValidMediaTimebase(t *testing.T) {
	gate := newSyncGate(Sync("room"))
	err := gate.Handle(context.Background(), &pipeline.Message{
		Kind:   pipeline.MessagePacket,
		Packet: &av.Packet{StreamID: "a"},
	}, &syncCollectEmitter{})
	if err == nil {
		t.Fatal("sync gate accepted packet without valid PTS timebase")
	}
}

func TestSyncGateSteadyStateAllocs(t *testing.T) {
	gate := newSyncGate(Sync("room", SyncDropLate()))
	msg := syncPacketMessage("a", 100*time.Millisecond)
	emit := syncNoopEmitter{}
	if err := gate.Handle(context.Background(), msg, emit); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		if err := gate.Handle(context.Background(), msg, emit); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("sync gate steady-state allocs = %f, want 0", allocs)
	}
}

func TestSyncOperationCloneUsesSeparateGateWithSharedScheduler(t *testing.T) {
	operation := operationSpecForSync(Sync("room"))
	cloned := cloneOperationSpecs([]operationSpec{operation})
	if len(cloned) != 1 {
		t.Fatalf("cloned operations = %d, want 1", len(cloned))
	}
	originalGate, ok := operation.Stage.(*syncGate)
	if !ok {
		t.Fatalf("original stage = %T, want sync gate", operation.Stage)
	}
	clonedGate, ok := cloned[0].Stage.(*syncGate)
	if !ok {
		t.Fatalf("cloned stage = %T, want sync gate", cloned[0].Stage)
	}
	if originalGate == clonedGate {
		t.Fatal("clone reused sync gate instance")
	}
	if originalGate.policy.scheduler != clonedGate.policy.scheduler {
		t.Fatal("clone did not preserve shared sync scheduler")
	}
}

func syncPacketMessage(stream av.StreamID, pts time.Duration) *pipeline.Message {
	base := av.TimeBase{Num: 1, Den: int64(time.Second)}
	return syncPacketMessageWithTimestamp(stream, int64(pts), base)
}

func syncPacketMessageWithTimestamp(stream av.StreamID, value int64, base av.TimeBase) *pipeline.Message {
	return &pipeline.Message{
		Kind: pipeline.MessagePacket,
		Packet: &av.Packet{
			StreamID: stream,
			PTS:      av.Timestamp{Value: value, Base: base},
		},
	}
}

func syncEventMessage(event av.Event) *pipeline.Message {
	return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &event}
}
