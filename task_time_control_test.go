package goav

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav/control"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/lifecycle"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

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

func TestTaskRateIsTaskWide(t *testing.T) {
	ctx := context.Background()

	ticker := &segmentTickSource{name: "ticker"}
	graph, _ := newTimeControlGraph(t, pipeline.BufferPolicy{}, ticker)
	task := newTask(graph, nil)
	t.Cleanup(func() { _ = task.Close() })

	// Rate scales the shared task timeline, so a node or tap target has no
	// meaning; the refusal names the fix.
	for name, ctrl := range map[string]control.Control{
		"node": control.Must(control.Rate(2.0)).At("ticker"),
		"tap":  control.Must(control.Rate(2.0)).AtTap("tap"),
	} {
		err := task.Control(ctx, ctrl)
		if !errors.Is(err, control.ErrInvalid) {
			t.Fatalf("targeted rate (%s) err = %v, want control.ErrInvalid", name, err)
		}
		if !strings.Contains(err.Error(), "task-wide") || !strings.Contains(err.Error(), "without At/AtTap") {
			t.Fatalf("targeted rate (%s) err = %v, want it to say rate is task-wide and name the fix", name, err)
		}
	}

	// A task with nothing paced to scale (no realtime demux pacer, no
	// clock-aware source) refuses rate honestly instead of pretending.
	err := task.Control(ctx, control.Must(control.Rate(2.0)))
	if !errors.Is(err, format.ErrRateUnsupported) {
		t.Fatalf("rate on unpaced task err = %v, want format.ErrRateUnsupported", err)
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

	if err := task.Control(ctx, control.Must(control.Segment(100*time.Nanosecond, 105*time.Nanosecond))); err != nil {
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

			// Invalid payloads are rejected by the control constructors, before any
			// direct or buffered runner can deliver them.
			for _, rate := range []float64{0, -1, math.Inf(1), math.NaN()} {
				_, err := control.Rate(rate)
				if !errors.Is(err, control.ErrInvalid) || !strings.Contains(err.Error(), "positive, finite playback rate") {
					t.Fatalf("control.Rate(%v) err = %v, want a positive-finite rejection", rate, err)
				}
			}
			for _, window := range []struct {
				start time.Duration
				end   time.Duration
			}{
				{start: 2 * time.Second, end: time.Second},
				{start: time.Second, end: time.Second},
				{start: -time.Nanosecond, end: time.Second},
			} {
				_, err := control.Segment(window.start, window.end)
				if !errors.Is(err, control.ErrInvalid) || !strings.Contains(err.Error(), "0 <= start < end") {
					t.Fatalf("Segment[%v,%v) err = %v, want a window rejection", window.start, window.end, err)
				}
			}
		})
	}

	// A source without the ControllableSource capability reports clearly when
	// a reposition control targets it directly (the direct runner delivers
	// before Run). Rate is not in this family: it is task-wide and never
	// targets a source (TestTaskRateIsTaskWide).
	ticker := &segmentTickSource{name: "ticker"}
	plain := &seekPlainSource{name: "plain"}
	graph, _ := newTimeControlGraph(t, pipeline.BufferPolicy{}, ticker, plain)
	task := newTask(graph, nil)
	t.Cleanup(func() { _ = task.Close() })
	for _, control := range []control.Control{control.Seek(time.Second), control.Must(control.Segment(0, time.Second))} {
		err := task.Control(ctx, control.At("plain"))
		if !errors.Is(err, pipeline.ErrInvalidLink) {
			t.Fatalf("%s at plain err = %v, want ErrInvalidLink", control.Type(), err)
		}
		if !strings.Contains(err.Error(), `"plain"`) || !strings.Contains(err.Error(), "ControllableSource") {
			t.Fatalf("%s at plain err = %v, want it to name the source and the missing capability", control.Type(), err)
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
		To(lifecycleTestSink("base")).BuildLive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	if _, err := task.Attach(ctx, Branch("rec").
		From(PacketTap("audio.packets")).
		Copy().
		To(lifecycleTestSink("rec"))); err != nil {
		t.Fatal(err)
	}
	before, ok := destinationSnapshotByName(task.Snapshot().Destinations, "rec")
	if !ok || before.State != lifecycle.DestinationOpen {
		t.Fatalf("destination before run = %+v, want open rec destination", before)
	}

	const start, end = 100 * time.Nanosecond, 104 * time.Nanosecond
	if err := task.Control(ctx, control.Must(control.Segment(start, end))); err != nil {
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
