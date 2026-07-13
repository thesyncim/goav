package goav

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// timelinePacedProviderSource is a clock-aware provider source for the
// task-timeline acceptance test: it paces packets one interval of timeline
// time apart on the clock the runtime injects through UseClock, in two phases
// separated by test-controlled gates so a mid-run rate change lands at a
// deterministic point with no sleeps in flight.
type timelinePacedProviderSource struct {
	id       av.StreamID
	media    av.MediaType
	codecID  av.CodecID
	interval time.Duration
	phase1   int
	phase2   int

	// phase1Done is closed after the phase-1 packets emit; phase2Gate holds
	// the source until the test releases it; finished is closed after the
	// phase-2 packets emit. Any may be nil.
	phase1Done chan struct{}
	phase2Gate chan struct{}
	finished   chan struct{}

	clock av.Clock
}

func (s *timelinePacedProviderSource) Name() string { return string(s.id) }

// UseClock records the injected task clock; the root hands the shared task
// timeline here after OpenSource.
func (s *timelinePacedProviderSource) UseClock(clock av.Clock) {
	if clock != nil {
		s.clock = clock
	}
}

func (s *timelinePacedProviderSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	if err := s.emitPaced(ctx, emitter, 0, s.phase1); err != nil {
		return err
	}
	if s.phase1Done != nil {
		close(s.phase1Done)
	}
	if s.phase2Gate != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.phase2Gate:
		}
	}
	if err := s.emitPaced(ctx, emitter, s.phase1, s.phase1+s.phase2); err != nil {
		return err
	}
	if s.finished != nil {
		close(s.finished)
	}
	eos := &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
		Type: av.EventEndOfStream, StreamID: s.id,
	}}
	return emitter.Emit(ctx, eos)
}

// emitPaced emits packets [from, to), sleeping one interval of timeline time
// before every packet after the first.
func (s *timelinePacedProviderSource) emitPaced(ctx context.Context, emitter pipeline.Emitter, from int, to int) error {
	for i := from; i < to; i++ {
		if i > 0 {
			if err := s.clock.Sleep(ctx, s.interval); err != nil {
				return err
			}
		}
		msg := &pipeline.Message{Kind: pipeline.MessagePacket, Packet: &av.Packet{
			StreamID: s.id,
			Type:     s.media,
			Payload:  av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable},
			PTS:      av.Timestamp{Value: int64(i) * int64(s.interval), Base: av.TimeBase{Num: 1, Den: int64(time.Second)}},
		}}
		if err := emitter.Emit(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

func (s *timelinePacedProviderSource) Close() error { return nil }

// timelinePacedProvider plugs the paced source into the recipe grammar
// through the provider.Source seam.
type timelinePacedProvider struct {
	source *timelinePacedProviderSource
}

func (p *timelinePacedProvider) Name() string { return string(p.source.id) }

func (p *timelinePacedProvider) OpenSource(context.Context) (pipeline.Source, []av.Stream, error) {
	return p.source, []av.Stream{{
		ID:   p.source.id,
		Type: p.source.media,
		Codec: av.CodecParameters{
			ID:   p.source.codecID,
			Type: p.source.media,
		},
		Name: string(p.source.id),
	}}, nil
}

func (p *timelinePacedProvider) SourceShape() shape.Spec {
	spec := shape.Packet(p.source.media, p.source.codecID, shape.Stream(p.source.id))
	spec.Realtime = true
	return spec
}

// countingPacketSink counts delivered packets per test destination.
type countingPacketSink struct {
	mu      sync.Mutex
	packets int
}

func (s *countingPacketSink) record(_ context.Context, msg Message) error {
	if msg.Kind == pipeline.MessagePacket && msg.Packet != nil {
		s.mu.Lock()
		s.packets++
		s.mu.Unlock()
	}
	return nil
}

func (s *countingPacketSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.packets
}

// TestTaskTimelineRateReanchorsAllSources pins the task timeline as the one
// clock service every paced source shares: two clock-aware provider sources in
// one task, one mid-run control.Rate (no target), and BOTH deliver at 2x. The
// gated phases make the wall-time trace on the fake runtime clock exact:
// cam paces 100ms intervals at 1x (100ms of clock each), then the rate change
// lands while both sources are parked, then mic paces its 60ms intervals at 2x
// (30ms of clock) and cam resumes its 100ms intervals at 2x (50ms of clock) —
// the same task-wide control re-anchored both.
func TestTaskTimelineRateReanchorsAllSources(t *testing.T) {
	ctx := context.Background()
	clock := &recordingTestClock{}

	cam := &timelinePacedProviderSource{
		id: "cam", media: av.MediaVideo, codecID: av.CodecVP8,
		interval: 100 * time.Millisecond, phase1: 4, phase2: 3,
		phase1Done: make(chan struct{}),
		phase2Gate: make(chan struct{}),
	}
	mic := &timelinePacedProviderSource{
		id: "mic", media: av.MediaAudio, codecID: av.CodecOpus,
		interval:   60 * time.Millisecond,
		phase2:     4, // everything waits for the gate
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

	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	// Phase 1: cam paces four packets at 1x, mic holds. All sleeps land
	// before the rate change, so the rate boundary in the trace is exact.
	<-cam.phase1Done

	// One untargeted control moves the shared timeline for the whole task.
	if err := task.Control(ctx, control.Must(control.Rate(2.0))); err != nil {
		t.Fatalf("Rate err = %v", err)
	}

	// Phase 2: mic plays fully at 2x, then cam resumes at 2x.
	close(mic.phase2Gate)
	<-mic.finished
	close(cam.phase2Gate)
	if err := <-runErr; err != nil {
		t.Fatalf("Run err = %v", err)
	}

	if video.count() != 7 || audio.count() != 4 {
		t.Fatalf("delivered packets video=%d audio=%d, want 7 and 4", video.count(), audio.count())
	}

	// The fake runtime clock recorded every wall wait the shared timeline
	// asked for: 100ms intervals at 1x, then 60ms intervals at 2x from mic,
	// then 100ms intervals at 2x from cam — both sources re-anchored by the
	// one task-wide control.
	want := []time.Duration{
		100 * time.Millisecond, 100 * time.Millisecond, 100 * time.Millisecond,
		30 * time.Millisecond, 30 * time.Millisecond, 30 * time.Millisecond,
		50 * time.Millisecond, 50 * time.Millisecond, 50 * time.Millisecond,
	}
	got := clock.Sleeps()
	if len(got) != len(want) {
		t.Fatalf("clock sleeps = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("clock sleeps = %v, want %v", got, want)
		}
	}
}
