package goav

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/internal/stage"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

type testStateEmitter func(context.Context, *pipeline.Message) error

func (f testStateEmitter) Emit(ctx context.Context, msg *pipeline.Message) error {
	return f(ctx, msg)
}

func TestStateMachineTaskRejectsDuplicateRun(t *testing.T) {
	task := newTask(newWatchTestGraph(1), nil)
	defer task.Close()

	if err := task.Run(t.Context()); err != nil {
		t.Fatalf("first Run() = %v, want nil", err)
	}
	if err := task.Run(t.Context()); err == nil {
		t.Fatal("second Run() = nil, want already-started error")
	}
}

func TestStateMachineTimelineControlRefusedWhileClosing(t *testing.T) {
	task := newTask(newWatchTestGraph(1), nil)
	task.timeline = stage.NewTimeline(av.MonotonicClock())
	task.timeline.MarkPaced()

	task.lifecycleMu.Lock()
	task.phase = taskClosing
	task.lifecycleMu.Unlock()

	if err := task.Pause(t.Context()); !errors.Is(err, control.ErrNotRunning) {
		t.Fatalf("Pause during taskClosing = %v, want control.ErrNotRunning", err)
	}
	if err := task.Resume(t.Context()); !errors.Is(err, control.ErrNotRunning) {
		t.Fatalf("Resume during taskClosing = %v, want control.ErrNotRunning", err)
	}
}

func TestStateMachineDestinationTransactionStickyFailAndFinalize(t *testing.T) {
	tx := &destinationTransaction{requireSuccess: true}
	tx.Fail()
	tx.Succeed()
	if !tx.Failed() || !tx.ShouldAbort() {
		t.Fatal("Succeed() overrode sticky Fail()")
	}

	var calls int
	tx.onFinalized = func(DestinationState, error) { calls++ }
	tx.finalize(DestinationAborted, nil)
	tx.Succeed()
	tx.finalize(DestinationCommitted, nil)
	if state, err := tx.outcome(); state != DestinationAborted || err != nil {
		t.Fatalf("outcome = (%q, %v), want (%q, nil)", state, err, DestinationAborted)
	}
	if calls != 1 {
		t.Fatalf("onFinalized calls = %d, want 1", calls)
	}
}

func TestStateMachineSwitchGateClosedCannotOpenGroup(t *testing.T) {
	group := newSwitchGroup(switchNextKeyframe)
	gate := group.gate()
	if err := gate.Close(); err != nil {
		t.Fatalf("gate.Close() = %v", err)
	}
	select {
	case <-group.abandoned:
	default:
		t.Fatal("group.abandoned not closed after sole gate closed")
	}

	var emitted int
	emitter := testStateEmitter(func(context.Context, *pipeline.Message) error {
		emitted++
		return nil
	})
	frame := &av.Frame{}
	if err := gate.Handle(t.Context(), &pipeline.Message{Kind: pipeline.MessageFrame, Frame: frame}, emitter); err != nil {
		t.Fatalf("gate.Handle() = %v", err)
	}
	if emitted != 0 {
		t.Fatalf("emitted = %d after Close, want 0", emitted)
	}
	select {
	case <-group.opened:
		t.Fatal("closed switchGate opened switchGroup")
	default:
	}
}

func TestStateMachineJoinSyncAndSelectorEOSOnce(t *testing.T) {
	sync := stage.NewJoinSyncStateWithPendingCap(compile.JoinSyncArrival, []av.StreamID{"a", "b"}, 2)
	if sync.MarkEOS("a") {
		t.Fatal("markEOS(a) returned true before b ended")
	}
	if !sync.MarkEOS("b") {
		t.Fatal("markEOS(b) returned false when all arms ended")
	}
	if sync.MarkEOS("b") || sync.MarkEOS("") {
		t.Fatal("markEOS returned true a second time after EOS was emitted")
	}
	sync.Buffer(&av.Frame{StreamID: "a"})
	if sync.PendingCount("a") != 0 {
		t.Fatalf("post-EOS frame buffered: len=%d, want 0", sync.PendingCount("a"))
	}

	sel := stage.NewSelector("sel", []av.StreamID{"a", "b"}, "out")
	var eosCount int
	emitter := testStateEmitter(func(_ context.Context, msg *pipeline.Message) error {
		if msg.Kind == pipeline.MessageEvent && msg.Event != nil && msg.Event.Type == av.EventEndOfStream {
			eosCount++
		}
		return nil
	})
	sendEOS := func(id av.StreamID) {
		ev := &av.Event{Type: av.EventEndOfStream, StreamID: id}
		if err := sel.Handle(t.Context(), &pipeline.Message{Kind: pipeline.MessageEvent, Event: ev}, emitter); err != nil {
			t.Fatalf("selector Handle(%q) = %v", id, err)
		}
	}
	sendEOS("unknown")
	sendEOS("a")
	if eosCount != 0 {
		t.Fatalf("eosCount = %d after unknown+a, want 0", eosCount)
	}
	sendEOS("b")
	sendEOS("b")
	sendEOS("")
	if eosCount != 1 {
		t.Fatalf("eosCount = %d, want 1", eosCount)
	}
}

type failingControlSource struct {
	name    string
	failErr error
}

func (s *failingControlSource) Name() string { return s.name }
func (s *failingControlSource) Start(context.Context, pipeline.Emitter) error {
	return s.failErr
}
func (s *failingControlSource) Close() error { return nil }
func (s *failingControlSource) Control(context.Context, *pipeline.Message) error {
	return nil
}

func TestStateMachineReconnectingSourceCloseStopsBackoffAndControl(t *testing.T) {
	first := &failingControlSource{name: "live", failErr: errors.New("dropped")}
	policy := reconnectPolicy{attempts: 0, backoff: 10 * time.Second}
	src := newReconnectingSource(
		reconnectSession{source: first},
		"live",
		func(context.Context) (reconnectSession, error) {
			return reconnectSession{}, errors.New("unreachable")
		},
		policy,
		av.MonotonicClock(),
		nil,
	)
	done := make(chan error, 1)
	go func() {
		done <- src.Start(t.Context(), testStateEmitter(func(context.Context, *pipeline.Message) error { return nil }))
	}()
	time.Sleep(20 * time.Millisecond)
	if err := src.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Start() did not exit promptly after Close() during reconnect backoff")
	}
	ctrl := src.(pipeline.ControllableSource)
	if err := ctrl.Control(t.Context(), &pipeline.Message{}); !errors.Is(err, pipeline.ErrClosed) {
		t.Fatalf("Control after Close() = %v, want pipeline.ErrClosed", err)
	}
}

func TestJoinConformStageFailedBindDoesNotBypass(t *testing.T) {
	rt := mustNew() // no filters registered -> resize bind fails
	stage := newJoinConformStage(rt.newBuilder(), "conform", "conform", shape.Frame(av.MediaVideo, shape.Video(64, 48, av.PixelFormatI420)), false)
	frame := &av.Frame{
		StreamID: "v",
		Type:     av.MediaVideo,
		Video:    &av.VideoFrame{Width: 128, Height: 96, PixelFormat: av.PixelFormatI420},
	}
	msg := &pipeline.Message{Kind: pipeline.MessageFrame, Frame: frame}
	emitted := 0
	emitter := testStateEmitter(func(_ context.Context, _ *pipeline.Message) error {
		emitted++
		return nil
	})
	if err := stage.Handle(t.Context(), msg, emitter); err == nil {
		t.Fatal("first Handle with missing resize adapter succeeded, want error")
	}
	if err := stage.Handle(t.Context(), msg, emitter); err == nil {
		t.Fatal("second Handle with same format bypassed failed bind and succeeded, want error")
	}
	if emitted != 0 {
		t.Fatalf("emitted = %d after failed binds, want 0", emitted)
	}
	if err := stage.Close(); err != nil {
		t.Fatalf("first Close = %v", err)
	}
	if err := stage.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
	if err := stage.Handle(t.Context(), msg, emitter); !errors.Is(err, pipeline.ErrClosed) {
		t.Fatalf("Handle after Close = %v, want pipeline.ErrClosed", err)
	}
}

func TestSwitchGroupOpenedAndAbandonedMutuallyExclusive(t *testing.T) {
	group := newSwitchGroup(NextFrame().kind)
	gate := group.gate()
	group.markOpen()
	_ = gate.Close()
	select {
	case <-group.opened:
	default:
		t.Fatal("opened channel not closed after markOpen")
	}
	select {
	case <-group.abandoned:
		t.Fatal("abandoned channel closed after group was already opened")
	default:
	}
}
