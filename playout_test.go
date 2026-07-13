package goav

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// playoutFrameSource returns a gated frame input: it pushes one frame per
// release token with the given PTS values, then EOS. With direct execution a
// push returns only after the frame cleared the playout gate and reached the
// sink, so tests step delivery deterministically. A nil release pushes every
// frame back to back.
func playoutFrameSource(id av.StreamID, media av.MediaType, release chan struct{}, pts ...time.Duration) InputSpec {
	return Source(string(id),
		shape.Frame(media, shape.Stream(id)),
		func(sctx context.Context, push SourcePush) error {
			for i := range pts {
				if release != nil {
					select {
					case <-release:
					case <-sctx.Done():
						return sctx.Err()
					}
				}
				frame := &av.Frame{
					StreamID: id,
					Type:     media,
					PTS:      av.Timestamp{Value: int64(pts[i]), Base: av.TimeBase{Num: 1, Den: int64(time.Second)}},
				}
				if _, err := push.Frame(frame); err != nil {
					return err
				}
			}
			return push.EOS()
		})
}

type playoutArrival struct {
	stream  av.StreamID
	reading time.Duration
}

// playoutArrivalLog records the fake-clock reading at every sink arrival —
// the timeline reading at 1x — and signals each arrival for step conductors.
type playoutArrivalLog struct {
	clock   *recordingTestClock
	arrived chan struct{}

	mu      sync.Mutex
	entries []playoutArrival
}

func newPlayoutArrivalLog(clock *recordingTestClock) *playoutArrivalLog {
	return &playoutArrivalLog{clock: clock, arrived: make(chan struct{}, 64)}
}

func (l *playoutArrivalLog) sink(name string) Destination {
	return Sink(SinkFunc(name, func(_ context.Context, msg Message) error {
		if msg.Kind == pipeline.MessageFrame && msg.Frame != nil {
			l.mu.Lock()
			l.entries = append(l.entries, playoutArrival{stream: msg.Frame.StreamID, reading: l.clock.Now()})
			l.mu.Unlock()
			l.arrived <- struct{}{}
		}
		return nil
	}))
}

func (l *playoutArrivalLog) snapshot() []playoutArrival {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]playoutArrival(nil), l.entries...)
}

func (l *playoutArrivalLog) waitArrival(t *testing.T) {
	t.Helper()
	select {
	case <-l.arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("no sink arrival")
	}
}

func assertPlayoutArrivals(t *testing.T, got []playoutArrival, want []playoutArrival) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("sink arrivals = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sink arrivals = %v, want %v", got, want)
		}
	}
}

// TestPlayoutSinkDeliversWhenDue pins the sink-sync contract: with offset 0,
// frames with PTS 0/20ms/40ms reach the sink at task-timeline readings
// 0/20/40 — the gate anchors the first frame and sleeps the exact media gap
// on the timeline for each later one.
func TestPlayoutSinkDeliversWhenDue(t *testing.T) {
	ctx := context.Background()
	clock := &recordingTestClock{}
	log := newPlayoutArrivalLog(clock)

	err := From(playoutFrameSource("cam", av.MediaVideo, nil, 0, 20*time.Millisecond, 40*time.Millisecond)).
		UseRuntime(mustNew(WithClock(clock))).
		Video().Playout(flow.Playout("preview")).To(log.sink("preview")).
		Run(ctx)
	if err != nil {
		t.Fatal(err)
	}

	assertPlayoutArrivals(t, log.snapshot(), []playoutArrival{
		{stream: "cam", reading: 0},
		{stream: "cam", reading: 20 * time.Millisecond},
		{stream: "cam", reading: 40 * time.Millisecond},
	})
	assertClockSleeps(t, clock, 20*time.Millisecond, 20*time.Millisecond)
}

// TestPlayoutSinkFreezesOnPause pins pause coherence at the sink boundary:
// pausing the task's internal timeline mid-stream stalls due-time delivery,
// and resume continues from the frozen reading with no pause gap in the
// arrival readings.
func TestPlayoutSinkFreezesOnPause(t *testing.T) {
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
	internal, ok := live.(*task)
	if !ok {
		t.Fatalf("live task = %T, want *task", live)
	}

	runErr := make(chan error, 1)
	go func() { runErr <- live.Run(ctx) }()

	release <- struct{}{}
	log.waitArrival(t) // PTS 0 delivered at reading 0

	internal.timeline.Pause()
	release <- struct{}{} // PTS 20ms enters the gate and must park
	select {
	case <-log.arrived:
		t.Fatal("playout delivered while the task timeline was paused")
	case <-time.After(30 * time.Millisecond):
	}

	internal.timeline.Resume()
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
}

// TestPlayoutAlignsBranchesSharingPolicy is the A/V alignment acceptance: two
// branches reusing one Playout policy share the first-message anchor, so their
// sink arrivals interleave by PTS on the one task timeline — the audio branch
// waits against the anchor the video branch set, not its own arrival time.
func TestPlayoutAlignsBranchesSharingPolicy(t *testing.T) {
	ctx := context.Background()
	clock := &recordingTestClock{}
	log := newPlayoutArrivalLog(clock)
	video := make(chan struct{})
	audio := make(chan struct{})
	policy := flow.Playout("room")

	live, err := From(
		playoutFrameSource("cam", av.MediaVideo, video, 0, 40*time.Millisecond, 80*time.Millisecond),
		playoutFrameSource("mic", av.MediaAudio, audio, 20*time.Millisecond, 60*time.Millisecond),
	).
		UseRuntime(mustNew(WithClock(clock))).
		Video().Playout(policy).To(log.sink("video-out")).
		Audio().Playout(policy).To(log.sink("audio-out")).
		BuildLive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()

	runErr := make(chan error, 1)
	go func() { runErr <- live.Run(ctx) }()

	for _, step := range []chan struct{}{video, audio, video, audio, video} {
		step <- struct{}{}
		log.waitArrival(t)
	}
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}

	assertPlayoutArrivals(t, log.snapshot(), []playoutArrival{
		{stream: "cam", reading: 0},
		{stream: "mic", reading: 20 * time.Millisecond},
		{stream: "cam", reading: 40 * time.Millisecond},
		{stream: "mic", reading: 60 * time.Millisecond},
		{stream: "cam", reading: 80 * time.Millisecond},
	})
}

// TestPlayoutRateChangeRetimesDelivery pins the timeline interaction: playout
// gates mark the timeline paced, so an untargeted control.Rate is accepted,
// and 2x rate halves the wall gap of the next due wait (100ms of media costs
// 50ms of clock).
func TestPlayoutRateChangeRetimesDelivery(t *testing.T) {
	ctx := context.Background()
	clock := &recordingTestClock{}
	log := newPlayoutArrivalLog(clock)
	release := make(chan struct{})

	live, err := From(playoutFrameSource("cam", av.MediaVideo, release, 0, 100*time.Millisecond, 200*time.Millisecond)).
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
	log.waitArrival(t)
	release <- struct{}{}
	log.waitArrival(t) // 100ms of media at 1x cost 100ms of clock

	if err := live.Control(ctx, control.Must(control.Rate(2.0))); err != nil {
		t.Fatalf("Rate err = %v", err)
	}

	release <- struct{}{}
	log.waitArrival(t)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}

	assertClockSleeps(t, clock, 100*time.Millisecond, 50*time.Millisecond)
}

// TestPlayoutDropLateShedsOverdueMedia pins drop-late shedding: a stalled
// consumer leaves messages overdue past the tolerance, the gate sheds them and
// reports the count through the DropReporter capability, and media within the
// tolerance still delivers.
func TestPlayoutDropLateShedsOverdueMedia(t *testing.T) {
	clock := &recordingTestClock{}
	timeline := newTimeline(clock)
	gate := newPlayoutGate(flow.Playout("preview").WithDropLate(10 * time.Millisecond))
	gate.bindClock(timeline)
	emit := &syncCollectEmitter{}

	if err := gate.Handle(context.Background(), syncPacketMessage("v", 0), emit); err != nil {
		t.Fatal(err)
	}
	// The consumer stalls for 100ms of timeline; the 20ms message is now 80ms
	// overdue, beyond the 10ms tolerance.
	clock.Advance(100 * time.Millisecond)
	if err := gate.Handle(context.Background(), syncPacketMessage("v", 20*time.Millisecond), emit); err != nil {
		t.Fatal(err)
	}
	if got := gate.DroppedMessages(); got != 1 {
		t.Fatalf("dropped = %d, want 1 overdue message shed", got)
	}
	// 95ms is only 5ms overdue — within tolerance, delivered immediately.
	if err := gate.Handle(context.Background(), syncPacketMessage("v", 95*time.Millisecond), emit); err != nil {
		t.Fatal(err)
	}
	if got := emit.Count(); got != 2 {
		t.Fatalf("emitted = %d, want anchor and in-tolerance messages only", got)
	}
	// Overdue media never sleeps.
	assertClockSleeps(t, clock)
}

// TestPlayoutDiscontinuityResetsAnchor pins the seek interaction: an
// EventDiscontinuity clears the shared anchor, so post-seek media re-anchors
// on its first message instead of sleeping out the media-time jump.
func TestPlayoutDiscontinuityResetsAnchor(t *testing.T) {
	clock := &recordingTestClock{}
	timeline := newTimeline(clock)
	gate := newPlayoutGate(flow.Playout("preview"))
	gate.bindClock(timeline)
	emit := &syncCollectEmitter{}

	if err := gate.Handle(context.Background(), syncPacketMessage("v", 0), emit); err != nil {
		t.Fatal(err)
	}
	clock.Advance(50 * time.Millisecond)
	if err := gate.Handle(context.Background(), syncEventMessage(av.Event{Type: av.EventDiscontinuity, StreamID: "v"}), emit); err != nil {
		t.Fatal(err)
	}
	// Without the reset this would sleep out the 950ms media jump.
	if err := gate.Handle(context.Background(), syncPacketMessage("v", time.Second), emit); err != nil {
		t.Fatal(err)
	}
	// No sleep after the re-anchor.
	assertClockSleeps(t, clock)
	if got := emit.Count(); got != 3 {
		t.Fatalf("emitted = %d, want both packets and the event", got)
	}
}

// TestPlayoutOperationCloneUsesSeparateGateWithSharedAnchor mirrors the sync
// clone contract: cloning the operation spec makes a fresh gate instance, and
// the clone shares the policy anchor so branches built from clones still align.
func TestPlayoutOperationCloneUsesSeparateGateWithSharedAnchor(t *testing.T) {
	operation := operationSpecForPlayout(flow.Playout("room"))
	cloned := cloneOperationSpecs([]operationSpec{operation})
	if len(cloned) != 1 {
		t.Fatalf("cloned operations = %d, want 1", len(cloned))
	}
	originalGate, ok := operation.Stage.(*playoutGate)
	if !ok {
		t.Fatalf("original stage = %T, want playout gate", operation.Stage)
	}
	clonedGate, ok := cloned[0].Stage.(*playoutGate)
	if !ok {
		t.Fatalf("cloned stage = %T, want playout gate", cloned[0].Stage)
	}
	if originalGate == clonedGate {
		t.Fatal("clone reused playout gate instance")
	}

	clock := &recordingTestClock{}
	timeline := newTimeline(clock)
	originalGate.bindClock(timeline)
	clonedGate.bindClock(timeline)
	if err := originalGate.Handle(context.Background(), syncPacketMessage("v", 0), syncNoopEmitter{}); err != nil {
		t.Fatal(err)
	}
	if err := clonedGate.Handle(context.Background(), syncPacketMessage("a", 20*time.Millisecond), syncNoopEmitter{}); err != nil {
		t.Fatal(err)
	}
	// The cloned gate waited against the original gate's anchor: 20ms of
	// timeline, not zero (a private anchor would deliver immediately).
	if got := clock.Sleeps(); len(got) != 1 || got[0] != 20*time.Millisecond {
		t.Fatalf("clock sleeps = %v, want [20ms] from the shared anchor", got)
	}
}

// TestPlayoutGateRequiresValidMediaTimebase mirrors the sync gate refusal:
// media without a valid PTS timebase cannot be scheduled, and the refusal
// names the exact fixes.
func TestPlayoutGateRequiresValidMediaTimebase(t *testing.T) {
	gate := newPlayoutGate(flow.Playout("room"))
	err := gate.Handle(context.Background(), &pipeline.Message{
		Kind:   pipeline.MessagePacket,
		Packet: &av.Packet{StreamID: "a"},
	}, &syncCollectEmitter{})
	if err == nil {
		t.Fatal("playout gate accepted packet without valid PTS timebase")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) {
		t.Fatalf("err = %T, want *BuildError", err)
	}
	fixes := strings.Join(buildErr.FixLines(), "\n")
	if !strings.Contains(fixes, "av.TimeBase") || !strings.Contains(fixes, ".Playout(") {
		t.Fatalf("fixes do not name the timebase fix and the .Playout(...) removal: %v", buildErr.FixLines())
	}
}

// TestPlayoutAttachRefusesStreamWithoutTimebase mirrors the sync attach
// refusal: a runtime branch carrying a playout gate is refused before graph
// mutation when the stream has no timebase facts, and the refusal names the
// exact fixes.
func TestPlayoutAttachRefusesStreamWithoutTimebase(t *testing.T) {
	operations := []operationSpec{operationSpecForPlayout(flow.Playout("room"))}
	err := validatePlayoutPolicyForStream("attach stream rule branch", "preview", av.Stream{ID: "cam"}, operations)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) {
		t.Fatalf("err = %T (%v), want *BuildError", err, err)
	}
	fixes := strings.Join(buildErr.FixLines(), "\n")
	for _, fragment := range []string{"av.Stream.TimeBase", "Codec.ClockRate", ".Playout("} {
		if !strings.Contains(fixes, fragment) {
			t.Fatalf("fixes missing %q: %v", fragment, buildErr.FixLines())
		}
	}

	withTimebase := av.Stream{ID: "cam", TimeBase: av.TimeBase{Num: 1, Den: 90000}}
	if err := validatePlayoutPolicyForStream("attach stream rule branch", "preview", withTimebase, operations); err != nil {
		t.Fatalf("stream with timebase refused: %v", err)
	}
}

// TestExplainReportsPathLatency pins the plan-time latency model: Explain
// composes the declared latency contributions along a gated path — sync
// tolerance, playout offset, and the runtime's declared buffered queue
// MaxLatency — into one path_latency_budget decision. Every figure is
// declared policy (no runtime measurement), and queue capacity is reported
// as a message count, never converted into a fake duration. Ungated paths
// get no row.
func TestExplainReportsPathLatency(t *testing.T) {
	ctx := context.Background()
	rt := mustNew(WithBufferPolicy(pipeline.BufferPolicy{
		Capacity:   16,
		Drop:       pipeline.DropBlock,
		MaxLatency: 50 * time.Millisecond,
	}))
	report, err := From(playoutFrameSource("cam", av.MediaVideo, nil, 0)).
		UseRuntime(rt).
		Video().
		Sync(flow.Sync("room", flow.SyncTolerance(40*time.Millisecond))).
		Playout(flow.Playout("preview").WithOffset(200 * time.Millisecond)).
		To(Sink(SinkFunc("preview", func(context.Context, Message) error { return nil }))).
		Explain(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var budgets []plan.Decision
	for _, decision := range report.Decisions {
		if decision.Code == diagnosticPathLatencyBudget {
			budgets = append(budgets, decision)
		}
	}
	if len(budgets) != 1 {
		t.Fatalf("path_latency_budget decisions = %+v, want exactly one", budgets)
	}
	if budgets[0].Branch != "video" {
		t.Fatalf("budget branch = %q, want %q", budgets[0].Branch, "video")
	}
	for _, fragment := range []string{
		"declared latency budget 290ms",
		"sync tolerance 40ms",
		"playout offset 200ms",
		"queue max latency 50ms",
		"queue capacity 16 messages",
		"a count, not a duration",
	} {
		if !strings.Contains(budgets[0].Message, fragment) {
			t.Fatalf("budget message missing %q: %s", fragment, budgets[0].Message)
		}
	}

	ungated, err := From(playoutFrameSource("cam", av.MediaVideo, nil, 0)).
		UseRuntime(rt).
		Video().
		To(Sink(SinkFunc("preview", func(context.Context, Message) error { return nil }))).
		Explain(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, decision := range ungated.Decisions {
		if decision.Code == diagnosticPathLatencyBudget {
			t.Fatalf("ungated path reported a latency budget: %+v", decision)
		}
	}
}
