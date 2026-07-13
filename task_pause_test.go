package goav

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	matroskaadapter "github.com/thesyncim/goav/container/matroska"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/inspect"
	"github.com/thesyncim/goav/lifecycle"
)

// TestTaskPauseFreezesPacedFlow is the T3 acceptance for the public pause
// surface: task.Pause freezes deliver-when-due playout mid-stream, a second
// Pause is a no-op, and Resume continues from the frozen reading — the wall
// trace on the fake clock shows the exact media gaps and no pause gap.
func TestTaskPauseFreezesPacedFlow(t *testing.T) {
	ctx := context.Background()
	clock := &recordingTestClock{}
	log := newPlayoutArrivalLog(clock)
	release := make(chan struct{})

	live, err := From(playoutFrameSource("cam", av.MediaVideo, release, 0, 20*time.Millisecond, 40*time.Millisecond)).
		UseRuntime(mustNew(WithClock(clock))).
		Video().Playout(flow.Playout("preview")).To(log.sink("preview")).
		BuildLive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()

	runErr := make(chan error, 1)
	go func() { runErr <- live.Run(ctx) }()

	release <- struct{}{}
	log.waitArrival(t) // PTS 0 delivered at reading 0

	if err := live.Pause(ctx); err != nil {
		t.Fatalf("Pause err = %v", err)
	}
	release <- struct{}{} // PTS 20ms enters the gate and must park
	select {
	case <-log.arrived:
		t.Fatal("playout delivered while the task was paused")
	case <-time.After(30 * time.Millisecond):
	}
	// Pausing a paused task is a no-op, mirroring the timeline semantics.
	if err := live.Pause(ctx); err != nil {
		t.Fatalf("second Pause err = %v", err)
	}

	if err := live.Resume(ctx); err != nil {
		t.Fatalf("Resume err = %v", err)
	}
	log.waitArrival(t) // PTS 20ms continues from the frozen reading

	release <- struct{}{}
	log.waitArrival(t)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}

	assertPlayoutArrivals(t, log.snapshot(), []playoutArrival{
		{stream: "cam", reading: 0},
		{stream: "cam", reading: 20 * time.Millisecond},
		{stream: "cam", reading: 40 * time.Millisecond},
	})
	// The pause itself costs no wall wait: the parked gate re-derives its
	// remaining 20ms after Resume, so the trace is the two media gaps only.
	assertClockSleeps(t, clock, 20*time.Millisecond, 20*time.Millisecond)
}

// TestTaskPauseSnapshotAndCloseContract pins the cold-side contract: the task
// snapshot reports the paused fact orthogonally to lifecycle state, pause is
// allowed before Run (the timeline is anchored at build), and a closed task
// refuses pause and resume with control.ErrNotRunning.
func TestTaskPauseSnapshotAndCloseContract(t *testing.T) {
	ctx := context.Background()
	clock := &recordingTestClock{}
	log := newPlayoutArrivalLog(clock)

	live, err := From(playoutFrameSource("cam", av.MediaVideo, nil, 0)).
		UseRuntime(mustNew(WithClock(clock))).
		Video().Playout(flow.Playout("preview")).To(log.sink("preview")).
		BuildLive(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if snap := live.Snapshot(); snap.Paused {
		t.Fatal("built task reports paused before any Pause")
	}
	if err := live.Pause(ctx); err != nil {
		t.Fatalf("Pause before Run err = %v", err)
	}
	snap := live.Snapshot()
	if !snap.Paused || snap.State != lifecycle.TaskBuilt {
		t.Fatalf("snapshot = state %q paused %v, want built and paused", snap.State, snap.Paused)
	}
	if err := live.Resume(ctx); err != nil {
		t.Fatalf("Resume err = %v", err)
	}
	if err := live.Resume(ctx); err != nil {
		t.Fatalf("second Resume err = %v", err)
	}
	if snap := live.Snapshot(); snap.Paused {
		t.Fatal("resumed task still reports paused")
	}

	if err := live.Close(); err != nil {
		t.Fatal(err)
	}
	if err := live.Pause(ctx); !errors.Is(err, control.ErrNotRunning) {
		t.Fatalf("Pause after Close err = %v, want control.ErrNotRunning", err)
	}
	if err := live.Resume(ctx); !errors.Is(err, control.ErrNotRunning) {
		t.Fatalf("Resume after Close err = %v, want control.ErrNotRunning", err)
	}
}

// TestTaskPauseRefusesFreeRunning pins the honesty rule: pause freezes TIME,
// not the data plane, so a task with nothing paced (an offline pump runs as
// fast as the graph drains) refuses pause the way it refuses control.Rate —
// format.ErrRateUnsupported, with the message naming the paced alternatives.
func TestTaskPauseRefusesFreeRunning(t *testing.T) {
	ctx := context.Background()
	data := muxSeekFileMKV(t)
	sink := &seekFileSink{}
	task := seekFileTask(t, bytes.NewReader(data), sink, WithRealtime(false), WithClock(&paceFileClock{}))

	err := task.Pause(ctx)
	if !errors.Is(err, format.ErrRateUnsupported) {
		t.Fatalf("Pause err = %v, want format.ErrRateUnsupported", err)
	}
	for _, fix := range []string{"realtime", "UseClock", ".Playout("} {
		if !strings.Contains(err.Error(), fix) {
			t.Fatalf("Pause refusal does not name fix %q: %v", fix, err)
		}
	}
	if err := task.Resume(ctx); !errors.Is(err, format.ErrRateUnsupported) {
		t.Fatalf("Resume err = %v, want format.ErrRateUnsupported", err)
	}
}

// TestTaskRateChangeWhilePausedAppliesOnResume pins the pause/rate
// interaction: a rate change lands on a paused timeline without unfreezing
// it, and Resume continues at the new rate — every paced wait costs the
// rate-scaled wall time on the fake clock.
func TestTaskRateChangeWhilePausedAppliesOnResume(t *testing.T) {
	ctx := context.Background()
	data := muxSeekFileMKV(t)
	sink := &seekFileSink{}
	clock := &paceFileClock{}
	task := seekFileTask(t, bytes.NewReader(data), sink, WithClock(clock))

	if err := task.Pause(ctx); err != nil {
		t.Fatalf("Pause err = %v", err)
	}
	if err := task.Control(ctx, control.Must(control.Rate(2.0))); err != nil {
		t.Fatalf("Rate while paused err = %v", err)
	}
	if snap := task.Snapshot(); !snap.Paused {
		t.Fatal("rate change unfroze the paused timeline")
	}

	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	// The first packet anchors the timeline and is already due, so it delivers
	// even while paused; the second parks on its 100ms media wait.
	waitSeekFileLogLen(t, sink, 1)
	if err := task.Resume(ctx); err != nil {
		t.Fatalf("Resume err = %v", err)
	}
	if err := <-runErr; err != nil {
		t.Fatalf("Run err = %v", err)
	}

	// The rate accepted while paused took effect on resume: nine 100ms media
	// gaps at 2x cost 50ms of clock each.
	assertPacingSleeps(t, clock.recorded(), repeatSleeps(50*time.Millisecond, 9))
	want := []int64{}
	for ms := int64(0); ms <= 900; ms += 100 {
		want = append(want, ms*int64(time.Millisecond))
	}
	want = append(want, -2)
	assertSeekFileLog(t, sink.snapshot(), want)
}

// pauseStepSink wraps seekFileSink with a step gate: every message the log
// records signals the test and parks until released, so the pump's position
// in its read/emit loop is exact at every assertion.
type pauseStepSink struct {
	inner   *seekFileSink
	arrived chan struct{}
	release chan struct{}
}

func newPauseStepSink() *pauseStepSink {
	return &pauseStepSink{
		inner:   &seekFileSink{},
		arrived: make(chan struct{}, 16),
		release: make(chan struct{}),
	}
}

func (s *pauseStepSink) record(ctx context.Context, msg Message) error {
	before := len(s.inner.snapshot())
	if err := s.inner.record(ctx, msg); err != nil {
		return err
	}
	if len(s.inner.snapshot()) == before {
		return nil
	}
	s.arrived <- struct{}{}
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *pauseStepSink) step(t *testing.T) {
	t.Helper()
	select {
	case <-s.arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("no sink arrival")
	}
	select {
	case s.release <- struct{}{}:
	case <-time.After(5 * time.Second):
		t.Fatal("sink did not accept release")
	}
}

// waitSeekFileLogLen polls the sink until its log reaches n entries.
func waitSeekFileLogLen(t *testing.T, sink *seekFileSink, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for len(sink.snapshot()) < n {
		if time.Now().After(deadline) {
			t.Fatalf("sink log = %v, want %d entries", sink.snapshot(), n)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestTaskSeekWhilePausedAppliesAtNextReadBoundary pins the pause/seek
// interaction, exactly as observed: a seek is accepted while paused and
// recorded at the source; it applies at the pump's next read boundary; the
// post-seek discontinuity resets the pacing anchor, so the first repositioned
// packet delivers even while paused (pause holds waits, it does not gate
// already-due delivery); and the packet after that parks until Resume.
func TestTaskSeekWhilePausedAppliesAtNextReadBoundary(t *testing.T) {
	ctx := context.Background()
	data := muxSeekFileMKV(t)
	step := newPauseStepSink()
	clock := &paceFileClock{}
	task, err := From(FileInput("seek.mkv", bytes.NewReader(data))).
		UseRuntime(mustNew(WithFormatAdapter(matroskaadapter.Register), WithClock(clock))).
		Audio().Copy().
		To(Sink(SinkFunc("rec", step.record))).
		BuildLive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	if err := task.Pause(ctx); err != nil {
		t.Fatalf("Pause err = %v", err)
	}
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	// Packet 0 anchors and is already due: it delivers while paused and the
	// pump now sits inside the gated sink, between reads.
	select {
	case <-step.arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("first packet did not arrive")
	}
	// Seek while paused: accepted, recorded at the source.
	if err := task.Control(ctx, control.Seek(450*time.Millisecond)); err != nil {
		t.Fatalf("Seek while paused err = %v", err)
	}
	select {
	case step.release <- struct{}{}:
	case <-time.After(5 * time.Second):
		t.Fatal("sink did not accept release")
	}

	// The pump applies the seek before reading the next packet: the
	// discontinuity and the re-anchored 400ms packet deliver while paused.
	step.step(t) // -1 discontinuity
	step.step(t) // 400ms: anchor reset, no wait to hold

	// The 500ms packet's 100ms media wait parks on the frozen timeline.
	select {
	case <-step.arrived:
		t.Fatal("paced packet delivered while the task was paused")
	case <-time.After(30 * time.Millisecond):
	}

	if err := task.Resume(ctx); err != nil {
		t.Fatalf("Resume err = %v", err)
	}
	for i := 0; i < 6; i++ { // 500..900 and EOS
		step.step(t)
	}
	if err := <-runErr; err != nil {
		t.Fatalf("Run err = %v", err)
	}

	// Pre-seek media past packet 0 never delivered: the seek applied before
	// the pump's next read.
	want := []int64{0, -1}
	for ms := int64(400); ms <= 900; ms += 100 {
		want = append(want, ms*int64(time.Millisecond))
	}
	want = append(want, -2)
	assertSeekFileLog(t, step.inner.snapshot(), want)
	// Wall trace: the five post-seek media gaps at 1x; the pause itself and
	// the re-anchored packet cost nothing.
	assertPacingSleeps(t, clock.recorded(), repeatSleeps(100*time.Millisecond, 5))
}

// TestTaskReadinessEventFires is the T3 readiness acceptance: a task with two
// sinks reports av.EventTaskReady through Watch exactly once, only after
// every sink received its first media message.
func TestTaskReadinessEventFires(t *testing.T) {
	ctx := context.Background()
	clock := &recordingTestClock{}

	cam := &timelinePacedProviderSource{
		id: "cam", media: av.MediaVideo, codecID: av.CodecVP8,
		interval: 100 * time.Millisecond, phase1: 2, phase2: 1,
		phase1Done: make(chan struct{}),
		phase2Gate: make(chan struct{}),
	}
	mic := &timelinePacedProviderSource{
		id: "mic", media: av.MediaAudio, codecID: av.CodecOpus,
		interval:   60 * time.Millisecond,
		phase2:     2, // everything waits for the gate
		phase2Gate: make(chan struct{}),
		finished:   make(chan struct{}),
	}
	video := &countingPacketSink{}
	audio := &countingPacketSink{}

	task, err := From(Input(&timelinePacedProvider{source: cam}), Input(&timelinePacedProvider{source: mic})).
		UseRuntime(mustNew(WithClock(clock))).
		Video().Copy().To(Sink(SinkFunc("video", video.record))).
		Audio().Copy().To(Sink(SinkFunc("audio", audio.record))).
		BuildLive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	ready := task.Watch(inspect.WatchTypes(av.EventTaskReady))
	defer ready.Close()

	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	// Phase 1: only the video sink has media — no readiness yet.
	<-cam.phase1Done
	select {
	case event := <-ready.Events():
		t.Fatalf("readiness event %v before every sink was fed", event)
	default:
	}

	// The audio sink's first packet completes readiness.
	close(mic.phase2Gate)
	<-mic.finished
	select {
	case event, ok := <-ready.Events():
		if !ok || event.Type != av.EventTaskReady {
			t.Fatalf("readiness event = %v (ok %v), want av.EventTaskReady", event, ok)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no readiness event arrived")
	}

	close(cam.phase2Gate)
	if err := <-runErr; err != nil {
		t.Fatalf("Run err = %v", err)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}

	// The subscription closed with the task; later media never re-fired.
	extra := 0
	for range ready.Events() {
		extra++
	}
	if extra != 0 {
		t.Fatalf("readiness fired %d more times, want exactly once", extra+1)
	}
	if video.count() != 3 || audio.count() != 2 {
		t.Fatalf("delivered packets video=%d audio=%d, want 3 and 2", video.count(), audio.count())
	}
}
