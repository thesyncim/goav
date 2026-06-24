package goav

import (
	"context"
	"errors"
	"github.com/thesyncim/goav/control"
	"math"
	"strings"
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

// rateTickSource is a pipeline.ControllableSource for goav-level rate tests:
// its Start loop emits frames whose PTS advances by 1000*rate per emit — the
// media time a paced realtime source covers per fixed wall tick — so a rate
// change shows up in the output cadence deterministically, with no wall clocks
// in the test. control.Control(av.EventRate) records the rate in one atomic the loop
// reads; per the ControllableSource contract a pure pacing change emits NO
// discontinuity.
type rateTickSource struct {
	name string
	rate atomic.Uint64 // math.Float64bits of the applied playback rate
}

func newRateTickSource(name string) *rateTickSource {
	s := &rateTickSource{name: name}
	s.rate.Store(math.Float64bits(1))
	return s
}

func (s *rateTickSource) Name() string { return s.name }

func (s *rateTickSource) applied() float64 { return math.Float64frombits(s.rate.Load()) }

func (s *rateTickSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	pos := int64(0)
	for {
		if ctx.Err() != nil {
			return nil
		}
		msg := &pipeline.Message{Kind: pipeline.MessageFrame, Frame: &av.Frame{
			PTS: av.Timestamp{Value: pos, Base: av.TimeBase{Num: 1, Den: int64(time.Second)}},
		}}
		if err := emitter.Emit(ctx, msg); err != nil {
			if errors.Is(err, pipeline.ErrBackpressure) {
				continue
			}
			return nil
		}
		pos += int64(1000 * s.applied())
	}
}

func (s *rateTickSource) Control(_ context.Context, msg *pipeline.Message) error {
	if msg == nil || msg.Kind != pipeline.MessageEvent || msg.Event == nil || msg.Event.Type != av.EventRate {
		return errors.New("rateTickSource: unsupported control")
	}
	rate, ok := av.EventRateValue(msg.Event)
	if !ok {
		return errors.New("rateTickSource: bad rate")
	}
	s.rate.Store(math.Float64bits(rate))
	return nil
}

func (s *rateTickSource) Close() error { return nil }

// segmentWindow is one [start, end) request in nanosecond ticks.
type segmentWindow struct {
	start int64
	end   int64
}

// parseSegmentWindow reads an av.EventSegment control message: start from
// Event.Timestamp (the Seek carrier) and the exclusive end through
// av.EventSegmentEnd (the second carrier).
func parseSegmentWindow(msg *pipeline.Message) (segmentWindow, error) {
	if msg == nil || msg.Kind != pipeline.MessageEvent || msg.Event == nil || msg.Event.Type != av.EventSegment {
		return segmentWindow{}, errors.New("unsupported control")
	}
	start, ok := msg.Event.Timestamp.ToDuration()
	if !ok || start < 0 {
		return segmentWindow{}, errors.New("bad segment start")
	}
	end, ok := av.EventSegmentEnd(msg.Event)
	if !ok || end <= start {
		return segmentWindow{}, errors.New("bad segment end")
	}
	return segmentWindow{start: int64(start), end: int64(end)}, nil
}

// segmentTickSource is a pipeline.ControllableSource that plays one requested
// [start, end) window per the Segment contract: discontinuity before the first
// tick at start (the seek half), one frame per nanosecond tick in the window,
// then a natural end — av.EventEndOfStream and a returning Start loop.
type segmentTickSource struct {
	name   string
	window atomic.Pointer[segmentWindow]
}

func (s *segmentTickSource) Name() string { return s.name }

func (s *segmentTickSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	window := s.window.Load()
	if window == nil {
		return errors.New("segmentTickSource: no segment set before Start")
	}
	disc := &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventDiscontinuity, Reason: "segment"}}
	if err := emitter.Emit(ctx, disc); err != nil {
		return err
	}
	for pos := window.start; pos < window.end; pos++ {
		msg := &pipeline.Message{Kind: pipeline.MessageFrame, Frame: &av.Frame{
			PTS: av.Timestamp{Value: pos, Base: av.TimeBase{Num: 1, Den: int64(time.Second)}},
		}}
		if err := emitter.Emit(ctx, msg); err != nil {
			return err
		}
	}
	eos := &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventEndOfStream}}
	return emitter.Emit(ctx, eos)
}

func (s *segmentTickSource) Control(_ context.Context, msg *pipeline.Message) error {
	window, err := parseSegmentWindow(msg)
	if err != nil {
		return errors.New("segmentTickSource: " + err.Error())
	}
	s.window.Store(&window)
	return nil
}

func (s *segmentTickSource) Close() error { return nil }

// timeControlSink records frame PTS ticks, discontinuities (-1), and
// end-of-stream events (-2) in arrival order.
type timeControlSink struct {
	name string
	mu   sync.Mutex
	log  []int64
}

func (s *timeControlSink) Name() string { return s.name }

func (s *timeControlSink) Handle(_ context.Context, msg *pipeline.Message) error {
	if msg == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case msg.Kind == pipeline.MessageFrame && msg.Frame != nil:
		s.log = append(s.log, msg.Frame.PTS.Value)
	case msg.Kind == pipeline.MessageEvent && msg.Event != nil && msg.Event.Type == av.EventDiscontinuity:
		s.log = append(s.log, -1)
	case msg.Kind == pipeline.MessageEvent && msg.Event != nil && msg.Event.Type == av.EventEndOfStream:
		s.log = append(s.log, -2)
	}
	return nil
}

func (s *timeControlSink) Close() error { return nil }

func (s *timeControlSink) snapshot() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int64(nil), s.log...)
}

// waitForLog polls until predicate holds for a snapshot of the log.
func (s *timeControlSink) waitForLog(ctx context.Context, predicate func([]int64) bool) error {
	for {
		if predicate(s.snapshot()) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

// hasDelta reports whether the log contains two consecutive frames whose PTS
// differ by exactly delta.
func hasDelta(log []int64, delta int64) bool {
	for i := 1; i < len(log); i++ {
		if log[i-1] >= 0 && log[i]-log[i-1] == delta {
			return true
		}
	}
	return false
}

func newTimeControlGraph(t *testing.T, buffer pipeline.BufferPolicy, sources ...pipeline.Source) (pipeline.Graph, *timeControlSink) {
	t.Helper()
	graph, err := pipeline.NewGraph(pipeline.GraphConfig{Name: "time-control", Buffer: buffer})
	if err != nil {
		t.Fatal(err)
	}
	sink := &timeControlSink{name: "sink"}
	for _, source := range sources {
		if _, err := graph.AddSource(source, pipeline.BufferPolicy{}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := graph.AddSink(sink, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	for _, source := range sources {
		if err := graph.Connect(pipeline.Route{From: source.Name(), To: []string{"sink"}, Policy: pipeline.RouteAll}); err != nil {
			t.Fatal(err)
		}
	}
	return graph, sink
}

func TestTaskRateChangesSourcePacingMidRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ticker := newRateTickSource("ticker")
	plain := &seekPlainSource{name: "plain"}
	// DropBlock keeps every frame so the cadence change cannot be shed away.
	graph, sink := newTimeControlGraph(t, pipeline.BufferPolicy{Capacity: 8, Drop: pipeline.DropBlock}, ticker, plain)

	task := newTask(graph, nil)
	t.Cleanup(func() { _ = task.Close() })
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	// Let 1x frames flow first: PTS deltas of 1000 per tick.
	if err := sink.waitForLog(ctx, func(log []int64) bool { return hasDelta(log, 1000) }); err != nil {
		t.Fatalf("pre-rate frames never arrived: %v", err)
	}

	// Untargeted Rate broadcasts to ALL sources: the controllable one applies it
	// synchronously, the plain one is reported clearly — errors collect per source.
	err := task.Control(ctx, control.Rate(2.0))
	if err == nil {
		t.Fatal("Rate err = nil, want a clear error for the uncontrollable source")
	}
	if !errors.Is(err, pipeline.ErrInvalidLink) {
		t.Fatalf("Rate err = %v, want ErrInvalidLink for the uncontrollable source", err)
	}
	if !strings.Contains(err.Error(), `"plain"`) || !strings.Contains(err.Error(), "ControllableSource") {
		t.Fatalf("Rate err = %v, want it to name the source and the missing capability", err)
	}
	if strings.Contains(err.Error(), `"ticker"`) {
		t.Fatalf("Rate err = %v, must not blame the adjustable source", err)
	}

	// control.Control records the rate synchronously before returning, so this is
	// deterministic — no sleeps.
	if applied := ticker.applied(); applied != 2.0 {
		t.Fatalf("applied rate = %v, want 2.0", applied)
	}

	// The cadence change is observable in the output: the loop reads the atomic,
	// so frames eventually advance 2000 per tick instead of 1000 — flow
	// continued at the new pace.
	if err := sink.waitForLog(ctx, func(log []int64) bool { return hasDelta(log, 2000) }); err != nil {
		t.Fatalf("post-rate cadence never appeared: %v", err)
	}
	cancel()
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v", err)
	}

	// A pure pacing change repositions nothing, so the contract forbids any
	// discontinuity (and nothing ended the stream).
	for _, entry := range sink.snapshot() {
		if entry < 0 {
			t.Fatalf("rate change must not emit discontinuities or EOS, log = %v", sink.snapshot())
		}
	}
}

func TestTaskSegmentPlaysWindowThenEndsNaturally(t *testing.T) {
	ctx := context.Background()

	// A direct (unbuffered) graph delivers segments like seeks: sources are
	// controlled by a synchronous method call, so a control landing before Run
	// pre-positions the source.
	source := &segmentTickSource{name: "ticker"}
	graph, sink := newTimeControlGraph(t, pipeline.BufferPolicy{}, source)

	task := newTask(graph, nil)
	t.Cleanup(func() { _ = task.Close() })

	if err := task.Control(ctx, control.Segment(100*time.Nanosecond, 105*time.Nanosecond)); err != nil {
		t.Fatalf("Segment on direct graph: %v", err)
	}
	// The window plays [start, end) and the run finishes naturally — no cancel.
	if err := task.Run(ctx); err != nil {
		t.Fatalf("Run err = %v", err)
	}

	want := []int64{-1, 100, 101, 102, 103, 104, -2}
	got := sink.snapshot()
	if len(got) != len(want) {
		t.Fatalf("sink log = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sink log = %v, want %v", got, want)
		}
	}
}

func TestTaskTimeControlRejectionMatrix(t *testing.T) {
	ctx := context.Background()

	buffers := map[string]pipeline.BufferPolicy{
		"direct":   {},
		"buffered": {Capacity: 8, Drop: pipeline.DropBlock},
	}
	for name, buffer := range buffers {
		t.Run(name, func(t *testing.T) {
			ticker := &segmentTickSource{name: "ticker"}
			plain := &seekPlainSource{name: "plain"}
			graph, _ := newTimeControlGraph(t, buffer, ticker, plain)
			task := newTask(graph, nil)
			t.Cleanup(func() { _ = task.Close() })

			// Invalid payloads are rejected at Controllable.Control with a clear error,
			// before any delivery — identically on both runners.
			for _, control := range []control.Control{
				control.Rate(0),
				control.Rate(-1),
				control.Rate(math.Inf(1)),
				control.Rate(math.NaN()),
			} {
				err := task.Control(ctx, control)
				if err == nil || !strings.Contains(err.Error(), "positive, finite playback rate") {
					t.Fatalf("control.Rate(%v) err = %v, want a positive-finite rejection", control.Rate, err)
				}
			}
			for _, control := range []control.Control{
				control.Segment(2*time.Second, time.Second),
				control.Segment(time.Second, time.Second),
				control.Segment(-time.Nanosecond, time.Second),
			} {
				err := task.Control(ctx, control)
				if err == nil || !strings.Contains(err.Error(), "0 <= start < end") {
					t.Fatalf("Segment[%v,%v) err = %v, want a window rejection", control.Position, control.End, err)
				}
			}
		})
	}

	// A source without the ControllableSource capability reports clearly when
	// targeted directly (the direct runner delivers before Run; the buffered
	// runner's mid-run collection is proven in the rate test above).
	ticker := &segmentTickSource{name: "ticker"}
	plain := &seekPlainSource{name: "plain"}
	graph, _ := newTimeControlGraph(t, pipeline.BufferPolicy{}, ticker, plain)
	task := newTask(graph, nil)
	t.Cleanup(func() { _ = task.Close() })
	for _, control := range []control.Control{control.Rate(2.0), control.Segment(0, time.Second)} {
		err := task.Control(ctx, control.At("plain"))
		if !errors.Is(err, pipeline.ErrInvalidLink) {
			t.Fatalf("%s at plain err = %v, want ErrInvalidLink", control.Type, err)
		}
		if !strings.Contains(err.Error(), `"plain"`) || !strings.Contains(err.Error(), "ControllableSource") {
			t.Fatalf("%s at plain err = %v, want it to name the source and the missing capability", control.Type, err)
		}
	}
}

// segmentPacketSource is the provider-opened source for the segment-export
// test: a pipeline.ControllableSource that plays the requested packet window
// [start, end) per the Segment contract and then ends naturally.
type segmentPacketSource struct {
	stream av.StreamID
	window atomic.Pointer[segmentWindow]
}

func (s *segmentPacketSource) Name() string { return string(s.stream) }

func (s *segmentPacketSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	window := s.window.Load()
	if window == nil {
		return errors.New("segmentPacketSource: no segment set before Start")
	}
	disc := &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
		Type: av.EventDiscontinuity, StreamID: s.stream, Reason: "segment",
	}}
	if err := emitter.Emit(ctx, disc); err != nil {
		return err
	}
	for pos := window.start; pos < window.end; pos++ {
		msg := &pipeline.Message{Kind: pipeline.MessagePacket, Packet: &av.Packet{
			StreamID: s.stream,
			Type:     av.MediaAudio,
			Payload:  av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable},
			PTS:      av.Timestamp{Value: pos, Base: av.TimeBase{Num: 1, Den: int64(time.Second)}},
		}}
		if err := emitter.Emit(ctx, msg); err != nil {
			return err
		}
	}
	eos := &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
		Type: av.EventEndOfStream, StreamID: s.stream,
	}}
	return emitter.Emit(ctx, eos)
}

func (s *segmentPacketSource) Control(_ context.Context, msg *pipeline.Message) error {
	window, err := parseSegmentWindow(msg)
	if err != nil {
		return errors.New("segmentPacketSource: " + err.Error())
	}
	s.window.Store(&window)
	return nil
}

func (s *segmentPacketSource) Close() error { return nil }

// segmentProvider plugs the controllable packet source into the recipe grammar
// through the provider.Source seam, so Segment is proven against a grammar-built
// task, not just raw graphs.
type segmentProvider struct {
	source *segmentPacketSource
}

func (p *segmentProvider) Name() string { return string(p.source.stream) }

func (p *segmentProvider) OpenSource(context.Context) (pipeline.Source, []av.Stream, error) {
	return p.source, []av.Stream{{
		ID:   p.source.stream,
		Type: av.MediaAudio,
		Codec: av.CodecParameters{
			ID:           av.CodecOpus,
			Type:         av.MediaAudio,
			SampleRate:   48_000,
			Channels:     codec.Stereo,
			SampleFormat: av.SampleFormatS16,
		},
		Name: string(p.source.stream),
	}}, nil
}

func (p *segmentProvider) SourceShape() shape.Spec {
	return shape.Packet(av.MediaAudio, av.CodecOpus,
		shape.Audio(48_000, codec.Stereo, av.SampleFormatS16),
	)
}

func TestTaskSegmentExportCommitsDestination(t *testing.T) {
	ctx := context.Background()

	// Segment-export as a real use case: a grammar-built task plays one window
	// and its transactional destination commits on the natural EOS — the
	// trim-to-file shape, with a recording branch attached at runtime.
	source := &segmentPacketSource{stream: "audio"}
	task, err := From(Input(&segmentProvider{source: source})).
		Audio().Copy().
		Tap(PacketTap("audio.packets")).
		To(lifecycleTestSink("base")).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	if _, err := task.Attach(ctx, Branch("rec").
		From(PacketTap("audio.packets")).
		To(lifecycleTestSink("rec"))); err != nil {
		t.Fatal(err)
	}
	before, ok := destinationSnapshotByName(task.Snapshot().Destinations, "rec")
	if !ok || before.State != lifecycle.DestinationOpen {
		t.Fatalf("destination before run = %+v, want open rec destination", before)
	}

	const start, end = 100 * time.Nanosecond, 104 * time.Nanosecond
	if err := task.Control(ctx, control.Segment(start, end)); err != nil {
		t.Fatalf("Segment err = %v", err)
	}
	// The window plays [start, end) and Run returns nil naturally on the
	// source's EOS — exactly like reaching the end of a file.
	if err := task.Run(ctx); err != nil {
		t.Fatalf("Run err = %v", err)
	}

	snap := task.Snapshot()
	if snap.State != lifecycle.TaskClosed {
		t.Fatalf("task state = %q, want %q", snap.State, lifecycle.TaskClosed)
	}
	committed, ok := destinationSnapshotByName(snap.Destinations, "rec")
	if !ok || committed.State != lifecycle.DestinationCommitted || committed.Open {
		t.Fatalf("destination after segment run = %+v, want committed rec destination", committed)
	}
}
