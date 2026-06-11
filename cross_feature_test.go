// Cross-feature pins: the combinations the single-feature waves never wrote —
// time controls against joins, runtime attach/rebranch/pause against join
// taps, dynamic-stream rules against joins and multi-input jobs, and the
// shape solver inside join branches and per multi-input chain. Each test pins
// a guarantee at a seam between two campaigns.

package goav

import (
	"context"
	"encoding/binary"
	"errors"
	"reflect"
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

// --- Seek on a PTS-synced Mix arm ---

// crossSeekArmSource is a controllable packet source standing as a Mix arm:
// it plays pts 0ms, waits for a Seek control, then replays from the seek
// target with the contractual discontinuity first, and ends.
type crossSeekArmSource struct {
	id     av.StreamID
	sought chan struct{}
	once   atomic.Bool
}

func (s *crossSeekArmSource) Name() string { return string(s.id) }

func (s *crossSeekArmSource) packetMsg(ptsMS int64, samples ...int16) *pipeline.Message {
	payload := make([]byte, len(samples)*2)
	for i := range samples {
		binary.LittleEndian.PutUint16(payload[i*2:], uint16(samples[i]))
	}
	return &pipeline.Message{Kind: pipeline.MessagePacket, Packet: &av.Packet{
		StreamID: s.id,
		Type:     av.MediaAudio,
		Payload:  av.Buffer{Bytes: payload, Ownership: av.BufferImmutable},
		PTS:      av.Timestamp{Value: ptsMS, Base: av.TimeBase{Num: 1, Den: 1000}},
		Duration: av.Duration{Value: 20, Base: av.TimeBase{Num: 1, Den: 1000}},
		Keyframe: true,
	}}
}

func (s *crossSeekArmSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	if err := emitter.Emit(ctx, s.packetMsg(0, 10, 10)); err != nil {
		return err
	}
	select {
	case <-s.sought:
	case <-ctx.Done():
		return ctx.Err()
	}
	disc := &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventDiscontinuity, StreamID: s.id, Reason: "seek"}}
	if err := emitter.Emit(ctx, disc); err != nil {
		return err
	}
	if err := emitter.Emit(ctx, s.packetMsg(1000, 30, 30)); err != nil {
		return err
	}
	eos := &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventEndOfStream, StreamID: s.id}}
	return emitter.Emit(ctx, eos)
}

func (s *crossSeekArmSource) Control(_ context.Context, msg *pipeline.Message) error {
	if msg == nil || msg.Kind != pipeline.MessageEvent || msg.Event == nil || msg.Event.Type != av.EventSeek {
		return errors.New("crossSeekArmSource: unsupported control")
	}
	if s.once.CompareAndSwap(false, true) {
		close(s.sought)
	}
	return nil
}

func (s *crossSeekArmSource) Close() error { return nil }

// crossSeekArmProvider plugs the controllable arm into the grammar through
// the provider seam, declaring a packet shape the test decoder handles.
type crossSeekArmProvider struct {
	source *crossSeekArmSource
	codec  av.CodecID
}

func (p *crossSeekArmProvider) Name() string { return string(p.source.id) }

func (p *crossSeekArmProvider) OpenSource(context.Context) (pipeline.Source, []av.Stream, error) {
	return p.source, []av.Stream{{
		ID:   p.source.id,
		Type: av.MediaAudio,
		Codec: av.CodecParameters{
			ID:           p.codec,
			Type:         av.MediaAudio,
			SampleRate:   48_000,
			Channels:     codec.Mono,
			SampleFormat: av.SampleFormatS16,
		},
		Name: string(p.source.id),
	}}, nil
}

func (p *crossSeekArmProvider) SourceShape() shape.Spec {
	return shape.Packet(av.MediaAudio, p.codec,
		shape.Audio(48_000, codec.Mono, av.SampleFormatS16), shape.Stream(p.source.id),
		shape.Realtime(true))
}

// crossPCMDecoderFactory opens crossPCMDecoder: payload bytes become one mono
// S16 frame carrying the packet's PTS and duration, so PTS joins see the
// source timeline.
type crossPCMDecoderFactory struct{}

func (crossPCMDecoderFactory) NewDecoder(context.Context, codec.DecodeConfig) (codec.Decoder, error) {
	return &crossPCMDecoder{}, nil
}

type crossPCMDecoder struct{}

func (d *crossPCMDecoder) Descriptor() codec.Descriptor { return codec.Descriptor{ID: "x_pcm_seek"} }

func (d *crossPCMDecoder) Open(context.Context, codec.DecodeConfig) error { return nil }

func (d *crossPCMDecoder) DecodeInto(_ context.Context, packet *av.Packet, out *codec.DecodeResult) error {
	if packet == nil {
		return nil
	}
	if len(out.Frames) == cap(out.Frames) {
		return codec.ErrResultFull
	}
	index := len(out.Frames)
	out.Frames = out.Frames[:index+1]
	frame := &out.Frames[index]
	frame.Reset()
	frame.StreamID = packet.StreamID
	frame.Type = av.MediaAudio
	frame.PTS = packet.PTS
	frame.Duration = packet.Duration
	frame.Audio = &av.AudioFrame{SampleRate: 48000, Channels: 1, SampleFormat: av.SampleFormatS16, Samples: len(packet.Payload.Bytes) / 2}
	if cap(frame.Planes) < 1 {
		frame.Planes = make([]av.Plane, 1)
	} else {
		frame.Planes = frame.Planes[:1]
	}
	frame.Planes[0].Buffer.Bytes = append(frame.Planes[0].Buffer.Bytes[:0], packet.Payload.Bytes...)
	frame.Planes[0].Stride = 2
	return nil
}

func (d *crossPCMDecoder) FlushInto(context.Context, *codec.DecodeResult) error { return nil }
func (d *crossPCMDecoder) HandleEvent(context.Context, *av.Event) error         { return nil }
func (d *crossPCMDecoder) Close() error                                         { return nil }

// TestMixSyncByPTSSeekArmMidRun seeks one arm of a running PTS-synced Mix:
// the repositioned arm's discontinuity flushes its pending queue, the join
// re-syncs at the new position and pairs the arms again there, one
// discontinuity reaches the joined stream, and an untargeted Seek reports
// the non-seekable sibling arm by name without blocking the seekable one.
//
// The runtime is explicitly buffered with copy budgets: an arm that BLOCKS
// mid-run (any live source) needs per-node workers — the default direct graph
// starts sources sequentially, so a blocking arm starves its siblings (the
// documented live-join gap, docs/ROADMAP.md) — and the decode arm's mutable
// frames need a copy budget to ride buffered queues.
func TestMixSyncByPTSSeekArmMidRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pcm := av.CodecID("x_pcm_seek")
	seekable := &crossSeekArmSource{id: "a", sought: make(chan struct{})}
	rt := New(
		WithBufferPolicy(pipeline.BufferPolicy{
			Capacity:        8,
			Drop:            pipeline.DropBlock,
			CopyPacketBytes: 4096,
			CopyFrameBytes:  4096,
		}),
		WithDecoder(
			codec.Descriptor{ID: pcm, Name: "PCM seek", Type: av.MediaAudio, Capabilities: codec.Capabilities{SampleFormats: []string{av.SampleFormatS16}}},
			crossPCMDecoderFactory{},
		))

	var mixed [][]int16
	var mixedPTS []int64
	var discontinuities atomic.Int32
	mixedReady := make(chan struct{}, 4)
	sink := Sink(SinkFunc("out", func(_ context.Context, m Message) error {
		switch {
		case m.Kind == pipeline.MessageFrame && m.Frame != nil && m.Frame.Audio != nil:
			mixed = append(mixed, mixTestReadS16(m.Frame))
			mixedPTS = append(mixedPTS, m.Frame.PTS.Value)
			mixedReady <- struct{}{}
		case m.Kind == pipeline.MessageEvent && m.Event != nil && m.Event.Type == av.EventDiscontinuity:
			discontinuities.Add(1)
		}
		return nil
	}))

	task, err := Mix(
		From(Input(&crossSeekArmProvider{source: seekable, codec: pcm})).Audio().Decode(),
		From(mixSyncTestSource("b", []int64{0, 1000}, [][]int16{{1, 1}, {2, 2}})).Audio(),
	).SyncByPTS().To(sink).UseRuntime(rt).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	// The pre-seek step must be mixed before the seek repositions arm a.
	select {
	case <-mixedReady:
	case <-ctx.Done():
		t.Logf("spec:\n%s", specText(task.Describe()))
		t.Logf("stats: %+v", task.Stats().Nodes)
		t.Fatal(ctx.Err())
	}

	// Untargeted Seek: the controllable arm repositions; the push-source arm
	// is reported clearly by name — never silently skipped.
	err = task.Control(ctx, Seek(time.Second))
	if err == nil {
		t.Fatal("Seek err = nil, want a clear error for the non-seekable arm")
	}
	if !strings.Contains(err.Error(), `"b"`) || !strings.Contains(err.Error(), "ControllableSource") {
		t.Fatalf("Seek err = %v, want it to name arm b and the missing capability", err)
	}
	if strings.Contains(err.Error(), `to "a"`) {
		t.Fatalf("Seek err = %v, must not blame the seekable arm", err)
	}

	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	want := [][]int16{{11, 11}, {32, 32}}
	if !reflect.DeepEqual(mixed, want) {
		t.Fatalf("mixed = %v, want %v (pre-seek pair, then the re-synced pair at the target)", mixed, want)
	}
	if wantPTS := []int64{0, 1000}; !reflect.DeepEqual(mixedPTS, wantPTS) {
		t.Fatalf("pts = %v, want %v", mixedPTS, wantPTS)
	}
	if got := discontinuities.Load(); got != 1 {
		t.Fatalf("forwarded discontinuities = %d, want exactly 1", got)
	}
}

// TestMixDecodedArmEOSStopsGatingEndToEnd pins the join EOS contract through
// a DECODED arm: the arm's end-of-stream must flow through its decode stage
// to the join (the join arm decoder forwards input events), so the ended arm
// stops gating — the surviving arm keeps mixing solo — and the joined
// stream's own EOS reaches the sink once every arm has ended. Before the fix
// the arm decode stage dropped input events, so a decoded arm could never end
// from the join's point of view: later steps were silently truncated and the
// joined EOS never fired.
func TestMixDecodedArmEOSStopsGatingEndToEnd(t *testing.T) {
	ctx := context.Background()
	pcm := av.CodecID("x_pcm_mono")
	rt := New(WithDecoder(
		codec.Descriptor{ID: pcm, Name: "PCM mono", Type: av.MediaAudio, Capabilities: codec.Capabilities{SampleFormats: []string{av.SampleFormatS16}}},
		tapArmTestDecoderFactory{decoder: &tapArmTestDecoder{}},
	))
	release := make(chan struct{}, 2)
	release <- struct{}{}
	release <- struct{}{}
	var mixed [][]int16
	var joinedEOS atomic.Int32
	sink := Sink(SinkFunc("out", func(_ context.Context, m Message) error {
		switch {
		case m.Kind == pipeline.MessageFrame && m.Frame != nil && m.Frame.Audio != nil:
			mixed = append(mixed, mixTestReadS16(m.Frame))
		case m.Kind == pipeline.MessageEvent && m.Event != nil && m.Event.Type == av.EventEndOfStream:
			joinedEOS.Add(1)
		}
		return nil
	}))
	task, err := Mix(
		From(tapArmTestPacketSource("a", pcm, 100, 100)).Audio().Decode(),
		From(crossGatedFrameSource("b", release, []int16{1, 1}, []int16{2, 2})).Audio(),
	).To(sink).UseRuntime(rt).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if want := [][]int16{{101, 101}, {2, 2}}; !reflect.DeepEqual(mixed, want) {
		t.Fatalf("mixed = %v, want %v (the ended decoded arm stops gating; b finishes solo)", mixed, want)
	}
	if got := joinedEOS.Load(); got != 1 {
		t.Fatalf("joined EOS at sink = %d, want exactly 1 once every arm ended", got)
	}
}

// --- OnStream against joins ---

// TestOnStreamRefusedOnJoinJobs pins the validateStreamRulesPass join branch:
// a Mix job is not a single-input anchor, so declaring an OnStream rule on it
// is a typed refusal, not a silent no-op.
func TestOnStreamRefusedOnJoinJobs(t *testing.T) {
	monitor := Sink(SinkFunc("monitor", func(context.Context, Message) error { return nil }))
	_, err := Mix(
		From(mixTestAudioSource("a", 100)).Audio(),
		From(mixTestAudioSource("b", 50)).Audio(),
	).To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		OnStream(MatchMedia(av.MediaAudio), Branch("late").Copy().To(monitor)).
		Build(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_rule_invalid" {
		t.Fatalf("err = %v, want stream_rule_invalid", err)
	}
	if !strings.Contains(buildErr.Reason, "Mix/Composite/Select") {
		t.Fatalf("reason = %q, want it to name the join limitation", buildErr.Reason)
	}
}

// --- Segment + runtime Attach mid-window ---

// crossSegmentGatedSource plays one requested [start, end) packet window like
// segmentPacketSource, but pauses after the first packet until resumed — the
// window the test attaches a recording branch in.
type crossSegmentGatedSource struct {
	stream av.StreamID
	window atomic.Pointer[segmentWindow]
	ready  chan struct{}
	resume chan struct{}
}

func (s *crossSegmentGatedSource) Name() string { return string(s.stream) }

func (s *crossSegmentGatedSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	window := s.window.Load()
	if window == nil {
		return errors.New("crossSegmentGatedSource: no segment set before Start")
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
		if pos == window.start {
			close(s.ready)
			select {
			case <-s.resume:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	eos := &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
		Type: av.EventEndOfStream, StreamID: s.stream,
	}}
	return emitter.Emit(ctx, eos)
}

func (s *crossSegmentGatedSource) Control(_ context.Context, msg *pipeline.Message) error {
	window, err := parseSegmentWindow(msg)
	if err != nil {
		return errors.New("crossSegmentGatedSource: " + err.Error())
	}
	s.window.Store(&window)
	return nil
}

func (s *crossSegmentGatedSource) Close() error { return nil }

// crossSegmentGatedProvider opens the gated segment source as a grammar input.
type crossSegmentGatedProvider struct {
	source *crossSegmentGatedSource
}

func (p *crossSegmentGatedProvider) Name() string { return string(p.source.stream) }

func (p *crossSegmentGatedProvider) OpenSource(context.Context) (pipeline.Source, []av.Stream, error) {
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

func (p *crossSegmentGatedProvider) SourceShape() shape.Spec {
	return shape.Packet(av.MediaAudio, av.CodecOpus,
		shape.Audio(48_000, codec.Stereo, av.SampleFormatS16))
}

// TestSegmentAttachMidWindowCommitsAtEOS attaches a recording branch while a
// Segment window is already playing: the late branch receives the rest of the
// window and its destination commits on the window's natural EOS, exactly
// like a destination attached before Run.
func TestSegmentAttachMidWindowCommitsAtEOS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	source := &crossSegmentGatedSource{stream: "audio", ready: make(chan struct{}), resume: make(chan struct{})}
	task, err := From(Input(&crossSegmentGatedProvider{source: source})).
		Audio().Copy().
		Tap(PacketTap("audio.packets")).
		To(lifecycleTestSink("base")).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	const start, end = 100 * time.Nanosecond, 104 * time.Nanosecond
	if err := task.Control(ctx, Segment(start, end)); err != nil {
		t.Fatalf("Segment err = %v", err)
	}
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	select {
	case <-source.ready:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var recorded atomic.Int32
	if _, err := task.Attach(ctx, Branch("rec").
		From(PacketTap("audio.packets")).
		To(Sink(SinkFunc("rec", func(_ context.Context, msg Message) error {
			if msg.Kind == pipeline.MessagePacket {
				recorded.Add(1)
			}
			return nil
		})))); err != nil {
		t.Fatal(err)
	}
	close(source.resume)
	if err := <-runErr; err != nil {
		t.Fatalf("Run err = %v", err)
	}

	snap := task.Snapshot()
	if snap.State != lifecycle.TaskClosed {
		t.Fatalf("task state = %q, want %q", snap.State, lifecycle.TaskClosed)
	}
	committed, ok := destinationSnapshotByName(snap.Destinations, "rec")
	if !ok || committed.State != lifecycle.DestinationCommitted || committed.Open {
		t.Fatalf("mid-window destination after run = %+v, want committed rec destination", committed)
	}
	if got := recorded.Load(); got != 3 {
		t.Fatalf("late branch packets = %d, want 3 (the window after the attach point)", got)
	}
}

// --- Rebranch on a join tap ---

// crossGatedFrameSource pushes one mono S16 frame per release and ends.
func crossGatedFrameSource(id av.StreamID, release chan struct{}, frames ...[]int16) InputSpec {
	return Source(string(id),
		shape.Frame(av.MediaAudio, shape.Audio(48000, codec.Mono, av.SampleFormatS16), shape.Stream(id)),
		func(sctx context.Context, push SourcePush) error {
			for i := range frames {
				select {
				case <-release:
				case <-sctx.Done():
					return sctx.Err()
				}
				b := make([]byte, len(frames[i])*2)
				for j := range frames[i] {
					binary.LittleEndian.PutUint16(b[j*2:], uint16(frames[i][j]))
				}
				frame := &av.Frame{
					StreamID: id, Type: av.MediaAudio,
					Audio:  &av.AudioFrame{SampleRate: 48000, Channels: 1, SampleFormat: av.SampleFormatS16, Samples: len(frames[i])},
					Planes: []av.Plane{{Buffer: av.Buffer{Bytes: b, Ownership: av.BufferImmutable}}},
				}
				if _, err := push.Frame(frame); err != nil {
					return err
				}
			}
			return push.EOS()
		})
}

// crossLiveJoinRuntime is the explicitly buffered runtime live (gated) join
// arms need: per-node workers so concurrently blocking arms cannot starve
// each other on the direct runner's sequential source starts.
func crossLiveJoinRuntime() Runtime {
	return New(WithBufferPolicy(pipeline.BufferPolicy{Capacity: 8, Drop: pipeline.DropBlock}))
}

// crossLockedFrames collects S16 frames behind a mutex so tests can poll the
// count while the sink goroutine still delivers.
type crossLockedFrames struct {
	mu     sync.Mutex
	frames [][]int16
}

func (c *crossLockedFrames) sink(name string) Destination {
	return Sink(SinkFunc(name, func(_ context.Context, m Message) error {
		if m.Kind == pipeline.MessageFrame && m.Frame != nil && m.Frame.Audio != nil {
			c.mu.Lock()
			c.frames = append(c.frames, mixTestReadS16(m.Frame))
			c.mu.Unlock()
		}
		return nil
	}))
}

func (c *crossLockedFrames) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.frames)
}

func (c *crossLockedFrames) snapshot() [][]int16 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]int16(nil), c.frames...)
}

// TestRebranchSwapsBranchOnJoinTap rebranches a runtime branch anchored on a
// Mix output tap while the mix runs: the replacement takes over mixed-frame
// delivery on the same anchor, the replaced sink stops, and the mix itself
// never misses a step.
func TestRebranchSwapsBranchOnJoinTap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	releaseA := make(chan struct{}, 2)
	releaseB := make(chan struct{}, 2)
	var main, old, replacement crossLockedFrames
	task, err := Mix(
		From(crossGatedFrameSource("a", releaseA, []int16{100, 100}, []int16{200, 200})).Audio(),
		From(crossGatedFrameSource("b", releaseB, []int16{1, 1}, []int16{2, 2})).Audio(),
	).Tap(FrameTap("mixed")).To(main.sink("out")).UseRuntime(crossLiveJoinRuntime()).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	attachment, err := task.Attach(ctx, Branch("watch").
		From(FrameTap("mixed")).
		To(old.sink("old")))
	if err != nil {
		t.Fatal(err)
	}
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	waitFor := func(label string, want int, got func() int) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for got() < want {
			if time.Now().After(deadline) {
				t.Fatalf("%s: got %d, want %d", label, got(), want)
			}
			time.Sleep(time.Millisecond)
		}
	}

	// Step 1 reaches the main sink and the attached branch.
	releaseA <- struct{}{}
	releaseB <- struct{}{}
	waitFor("step1 main", 1, main.count)
	waitFor("step1 old branch", 1, old.count)

	// Swap the branch on the join tap, then step 2 reaches the replacement.
	if _, err := attachment.Rebranch(ctx, Branch("watch2").
		From(FrameTap("mixed")).
		To(replacement.sink("new"))); err != nil {
		t.Fatalf("Rebranch err = %v", err)
	}
	releaseA <- struct{}{}
	releaseB <- struct{}{}
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}

	if want := [][]int16{{101, 101}, {202, 202}}; !reflect.DeepEqual(main.snapshot(), want) {
		t.Fatalf("main = %v, want %v (the mix never misses a step)", main.snapshot(), want)
	}
	if want := [][]int16{{101, 101}}; !reflect.DeepEqual(old.snapshot(), want) {
		t.Fatalf("replaced branch = %v, want only the pre-rebranch step %v", old.snapshot(), want)
	}
	if want := [][]int16{{202, 202}}; !reflect.DeepEqual(replacement.snapshot(), want) {
		t.Fatalf("replacement branch = %v, want the post-rebranch step %v", replacement.snapshot(), want)
	}
}

// --- Pause/Resume on a join-tap branch ---

// TestPauseResumeBranchOnJoinTap pauses a runtime branch anchored on a Mix
// output tap: the paused branch skips a mixed step without backpressuring the
// join (the main sink keeps advancing), and resume restores delivery.
func TestPauseResumeBranchOnJoinTap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	releaseA := make(chan struct{}, 3)
	releaseB := make(chan struct{}, 3)
	var main, watched crossLockedFrames
	task, err := Mix(
		From(crossGatedFrameSource("a", releaseA, []int16{10}, []int16{20}, []int16{30})).Audio(),
		From(crossGatedFrameSource("b", releaseB, []int16{1}, []int16{2}, []int16{3})).Audio(),
	).Tap(FrameTap("mixed")).To(main.sink("out")).UseRuntime(crossLiveJoinRuntime()).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	attachment, err := task.Attach(ctx, Branch("watch").
		From(FrameTap("mixed")).
		To(watched.sink("watch")))
	if err != nil {
		t.Fatal(err)
	}
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	waitFor := func(label string, want int, got func() int) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for got() < want {
			if time.Now().After(deadline) {
				t.Fatalf("%s: got %d, want %d", label, got(), want)
			}
			time.Sleep(time.Millisecond)
		}
	}

	releaseA <- struct{}{}
	releaseB <- struct{}{}
	waitFor("step1 main", 1, main.count)
	waitFor("step1 watch", 1, watched.count)

	if err := attachment.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	releaseA <- struct{}{}
	releaseB <- struct{}{}
	waitFor("step2 main", 2, main.count)
	if got := watched.count(); got != 1 {
		t.Fatalf("paused join-tap branch received %d steps, want 1", got)
	}

	if err := attachment.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	releaseA <- struct{}{}
	releaseB <- struct{}{}
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if want := [][]int16{{11}, {22}, {33}}; !reflect.DeepEqual(main.snapshot(), want) {
		t.Fatalf("main = %v, want %v (the join is never stalled by the paused branch)", main.snapshot(), want)
	}
	if want := [][]int16{{11}, {33}}; !reflect.DeepEqual(watched.snapshot(), want) {
		t.Fatalf("watch branch = %v, want steps 1 and 3 only %v", watched.snapshot(), want)
	}
}

// --- Detach of a branch sharing a tap-arm join's anchor ---

// TestDetachBranchOnTapArmAnchorKeepsJoin attaches and detaches a runtime
// branch on the SAME tap a TapRef join arm converges from, before any media
// flows: the planned tap-arm edge survives the detach and the mix still sums
// the tapped stream twice.
func TestDetachBranchOnTapArmAnchorKeepsJoin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	release := make(chan struct{}, 1)
	var got, monitored [][]int16
	task, err := Mix(
		From(crossGatedFrameSource("a", release, []int16{100, 200})).Audio().Tap(FrameTap("dry")),
		FrameTap("dry"),
		From(mixTestAudioSource("live", 50, -50)).Audio(),
	).To(joinTestCollectSink("out", &got)).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	attachment, err := task.Attach(ctx, Branch("monitor").
		From(FrameTap("dry")).
		To(joinTestCollectSink("monitor", &monitored)))
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Detach(ctx, attachment); err != nil {
		t.Fatalf("Detach err = %v", err)
	}
	if state := attachment.Snapshot().State; state != lifecycle.BranchDetached {
		t.Fatalf("attachment state = %v, want %v", state, lifecycle.BranchDetached)
	}

	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()
	release <- struct{}{}
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if want := [][]int16{{250, 350}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("mixed = %v, want %v (tap arm survives the detach of its tap sibling)", got, want)
	}
	if len(monitored) != 0 {
		t.Fatalf("detached branch received %d frames, want 0", len(monitored))
	}
}

// --- EOS during a SelectActive switch targeting the ending arm ---

// TestSelectActiveSwitchToEndedArm switches a running Select to an arm that
// has already delivered its EOS: the switch is accepted (the arm is
// configured), the output goes deliberately silent, and the task still ends
// naturally once every arm has ended — no wedge, exactly one final EOS.
func TestSelectActiveSwitchToEndedArm(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	releaseA := make(chan struct{}, 2)
	var got crossLockedFrames
	task, err := Select(
		From(crossGatedFrameSource("a", releaseA, []int16{100}, []int16{200})).Audio(),
		From(mixTestAudioSource("b", 7)).Audio(),
	).To(got.sink("out")).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	// Arm b plays its single frame and EOSes immediately; it is never active,
	// so only arm a's first frame reaches the sink.
	releaseA <- struct{}{}
	deadline := time.Now().Add(10 * time.Second)
	for got.count() < 1 {
		if time.Now().After(deadline) {
			t.Fatalf("active arm frame never arrived, got %v", got.snapshot())
		}
		time.Sleep(time.Millisecond)
	}

	// Switch to the ended arm mid-run. The control is accepted: ended arms
	// stay configured, selecting one mutes the output by design.
	if err := controlWhenRunningInternal(ctx, task, SelectActive("b")); err != nil {
		t.Fatalf("SelectActive to the ended arm: %v", err)
	}

	// Arm a's second frame is dropped (b is active), then a ends — the
	// selector's EOS bookkeeping must still complete the task.
	releaseA <- struct{}{}
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if want := [][]int16{{100}}; !reflect.DeepEqual(got.snapshot(), want) {
		t.Fatalf("delivered = %v, want %v (nothing after switching to the ended arm)", got.snapshot(), want)
	}
}

// controlWhenRunningInternal retries a control until the running graph
// accepts it (the internal twin of the dogfood helper).
func controlWhenRunningInternal(ctx context.Context, task Task, control Control) error {
	for {
		err := task.Control(ctx, control)
		if err == nil || !errors.Is(err, ErrControlNotRunning) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

// --- Solver per multi-input chain and inside join branches ---

// TestFromMultiInputChainsKeepIndependentAutoPolicies pins per-chain .Auto
// isolation on one multi-input job: two identical 44.1kHz inputs feed two
// Opus chains, only the first chain grants AllowResample, and exactly that
// chain gets the planned conversion — the sibling's grant never leaks. The
// planned and built graphs stay byte-identical across the multi-source edge
// fanout.
func TestFromMultiInputChainsKeepIndependentAutoPolicies(t *testing.T) {
	sinkDest := func(name string) Destination {
		return Sink(SinkFunc(name, func(context.Context, Message) error { return nil }))
	}
	rt := solverTestOpusRuntime(WithStdFilters())

	job := From(
		solverTestAudioSource("micA", 44_100, codec.Stereo, av.SampleFormatS16),
		solverTestAudioSource("micB", 44_100, codec.Stereo, av.SampleFormatS16),
	).
		Audio(InputName("micA")).Auto(shape.AllowResample()).Encode(codec.Opus(codec.Bitrate(96_000))).To(sinkDest("outA")).
		Audio(InputName("micB")).Encode(codec.Opus(codec.Bitrate(96_000))).To(sinkDest("outB")).
		UseRuntime(rt)
	planned, err := job.Describe()
	if err != nil {
		t.Fatalf("Describe(): %v", err)
	}
	text := specText(planned)
	if got := strings.Count(text, "[resample"); got != 1 {
		t.Fatalf("planned resample nodes = %d, want exactly 1 (chain A only):\n%s", got, text)
	}
	if !strings.Contains(text, "select-audio@micA -> resample-audio") {
		t.Fatalf("the conversion must sit on chain A's path:\n%s", text)
	}
	if strings.Contains(text, "select-audio@micB -> resample") {
		t.Fatalf("chain B must not borrow chain A's Auto grant:\n%s", text)
	}
	task, err := job.Build(context.Background())
	if err != nil {
		t.Fatalf("Build(): %v", err)
	}
	defer task.Close()
	if specText(task.Describe()) != text {
		t.Fatalf("Describe() != Build():\nplanned:\n%s\nbuilt:\n%s", text, specText(task.Describe()))
	}
}

// TestMixBranchesSolverGapIsDocumented records the known gap (docs/ROADMAP.md
// "Known gaps"): the shape solver does not run downstream of a join, so a
// 44.1kHz mix feeding an Opus-encoding branch plans NO conversion and NO
// refusal even under the branch's .Auto(AllowResample). When the gap is
// closed this test fails so the pin flips to the solved behavior (a planned
// resample on the branch path, mirroring TestAutoInsertsResampleBeforeEncode).
func TestMixBranchesSolverGapIsDocumented(t *testing.T) {
	rt := solverTestOpusRuntime(WithStdFilters())
	job := Mix(
		From(mixTestAudioSourceRate("a", 44_100)).Audio(),
		From(mixTestAudioSourceRate("b", 44_100)).Audio(),
	).Branches(
		Branch("enc").
			Auto(shape.AllowResample()).
			Encode(codec.Opus(codec.Bitrate(96_000))).
			To(Sink(SinkFunc("enc", func(context.Context, Message) error { return nil }))),
	).UseRuntime(rt)

	planned, err := job.Describe()
	if err != nil {
		t.Fatalf("Describe(): %v", err)
	}
	if strings.Contains(specText(planned), "[resample") {
		t.Fatalf("the join-branch solver gap is closed: a conversion is now planned — flip this test to assert the solved behavior and drop the docs/ROADMAP.md known-gap entry:\n%s", specText(planned))
	}
}
