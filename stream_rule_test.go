package goav

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/lifecycle"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/rtpav"
	"github.com/thesyncim/goav/shape"
)

// syncObjectState is a mutex-guarded transactional destination recorder: the
// pipeline writes from graph goroutines while the test polls, so every access
// is synchronized.
type syncObjectState struct {
	mu      sync.Mutex
	bytes   []byte
	info    DestinationInfo
	opens   int
	closes  int
	commits int
	aborts  int
}

func (s *syncObjectState) open(_ context.Context, info DestinationInfo) (io.WriteCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.opens++
	s.info = info
	return &syncObjectWriter{state: s}, nil
}

func (s *syncObjectState) snapshot() (opens, closes, commits, aborts int, data []byte, info DestinationInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.opens, s.closes, s.commits, s.aborts, append([]byte(nil), s.bytes...), s.info
}

type syncObjectWriter struct {
	state *syncObjectState
}

func (w *syncObjectWriter) Write(p []byte) (int, error) {
	w.state.mu.Lock()
	defer w.state.mu.Unlock()
	w.state.bytes = append(w.state.bytes, p...)
	return len(p), nil
}

func (w *syncObjectWriter) Close() error {
	w.state.mu.Lock()
	defer w.state.mu.Unlock()
	w.state.closes++
	return nil
}

func (w *syncObjectWriter) Commit(context.Context) error {
	w.state.mu.Lock()
	defer w.state.mu.Unlock()
	w.state.commits++
	return nil
}

func (w *syncObjectWriter) Abort(context.Context) error {
	w.state.mu.Lock()
	defer w.state.mu.Unlock()
	w.state.aborts++
	return nil
}

// syncTestMuxer is a synchronized passthrough muxer for late-attach tests:
// it opens on the engine goroutine and writes on a pipeline goroutine.
type syncTestMuxer struct {
	mu     sync.Mutex
	writer io.Writer
}

func (m *syncTestMuxer) NewMuxer(context.Context, av.FormatID) (format.Muxer, error) {
	return m, nil
}

func (m *syncTestMuxer) Format() av.FormatID {
	return av.FormatOgg
}

func (m *syncTestMuxer) Open(_ context.Context, output format.Output, _ []av.Stream, _ format.OpenOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writer = output.Writer
	return nil
}

func (m *syncTestMuxer) Write(_ context.Context, packet *av.Packet, _ *format.WriteResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, err := m.writer.Write(packet.Payload.Bytes)
	return err
}

func (m *syncTestMuxer) Close() error {
	return nil
}

func lateOpusStream(id av.StreamID) av.Stream {
	return av.Stream{
		ID:       id,
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecOpus,
			Type:       av.MediaAudio,
			ClockRate:  48000,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
	}
}

// lateStreamSource is a custom source that pushes one packet for its declared
// stream, announces a late stream, pumps that stream's packets until stopLate
// closes, announces its removal, and waits for finish before EOS.
func lateStreamSource(late av.Stream, payload byte, stopLate <-chan struct{}, finish <-chan struct{}) InputSpec {
	return Source("mic",
		shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48_000, codec.Stereo, av.SampleFormatS16)),
		func(sctx context.Context, push SourcePush) error {
			declared := av.Packet{StreamID: "mic", Type: av.MediaAudio, Payload: av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable}}
			if _, err := push.Packet(&declared); err != nil {
				return err
			}
			announced := late
			if _, err := push.Event(av.Event{Type: av.EventStreamAdded, Stream: &announced}); err != nil {
				return err
			}
			for done := false; !done; {
				select {
				case <-stopLate:
					done = true
				case <-sctx.Done():
					return sctx.Err()
				case <-time.After(time.Millisecond):
					packet := av.Packet{
						StreamID: late.ID,
						Type:     av.MediaAudio,
						Keyframe: true,
						Payload:  av.Buffer{Bytes: []byte{payload}, Ownership: av.BufferImmutable},
					}
					if _, err := push.Packet(&packet); err != nil {
						return err
					}
				}
			}
			if _, err := push.Event(av.Event{Type: av.EventStreamRemoved, StreamID: late.ID}); err != nil {
				return err
			}
			select {
			case <-finish:
			case <-sctx.Done():
				return sctx.Err()
			}
			return push.EOS()
		},
	)
}

func taskHasBranch(task Task, name string) bool {
	branches := task.Snapshot().Branches
	for i := range branches {
		if branches[i].Name == name && branches[i].State == lifecycle.BranchAttached {
			return true
		}
	}
	return false
}

// TestOnStreamAttachesLateBranchAndDetachesOnRemoval is the end-to-end pass
// over the dynamic stream grammar: a custom source announces a late audio
// stream mid-run, the OnStream rule attaches a recording branch to it without
// any source rebuild, the branch's transactional destination receives the late
// stream's media, and the stream's removal detaches the branch with drain
// semantics — the destination commits while the task keeps running.
func TestOnStreamAttachesLateBranchAndDetachesOnRemoval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stopLate := make(chan struct{})
	finish := make(chan struct{})
	late := lateOpusStream("late-audio")
	input := lateStreamSource(late, 0xA, stopLate, finish)

	state := &syncObjectState{}
	rt := New(withTestFormats(testFormatMuxer(av.FormatOgg, &syncTestMuxer{})))
	record := Writer("s3://bucket/late.ogg", state.open, Format(av.FormatOgg), MIME("audio/ogg"))

	var mainCount atomic.Int32
	mainSink := Sink(SinkFunc("main", func(_ context.Context, msg Message) error {
		if msg.Kind == pipeline.MessagePacket && msg.Packet.StreamID == "mic" {
			mainCount.Add(1)
		}
		return nil
	}))

	task, err := From(input).
		OnStream(MatchMedia(av.MediaAudio), Branch("record").Copy().To(record)).
		Audio().Copy().To(mainSink).
		UseRuntime(rt).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	attachErrors := task.Watch(WatchTypes(av.EventAttachError))
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	// The rule reacts to the announce: the branch attaches to the late stream
	// and its destination starts receiving that stream's packets.
	waitForCondition(t, "late branch destination received media", func() bool {
		_, _, _, _, data, _ := state.snapshot()
		return len(data) != 0
	})
	opens, _, _, _, data, destInfo := state.snapshot()
	if opens != 1 || len(destInfo.Streams) != 1 || destInfo.Streams[0].ID != late.ID {
		t.Fatalf("destination opened with %d opens, streams %+v", opens, destInfo.Streams)
	}
	if !strings.Contains(string(data), string([]byte{0xA})) {
		t.Fatalf("destination bytes = %v, want late payload", data)
	}
	waitForCondition(t, "late branch attached", func() bool {
		return taskHasBranch(task, "record-late-audio")
	})
	snap := task.Snapshot()
	for _, branch := range snap.Branches {
		if branch.Name != "record-late-audio" {
			continue
		}
		if len(branch.Destinations) == 0 || branch.Destinations[0].State != lifecycle.DestinationOpen {
			t.Fatalf("attached branch destinations = %+v, want open", branch.Destinations)
		}
	}

	// Removing the stream detaches the branch with drain semantics: the
	// transactional destination commits while the task keeps running.
	close(stopLate)
	waitForCondition(t, "late branch detached", func() bool {
		return !taskHasBranch(task, "record-late-audio")
	})
	waitForCondition(t, "transactional destination committed", func() bool {
		_, _, commits, _, _, _ := state.snapshot()
		return commits == 1
	})
	if _, _, _, aborts, _, _ := state.snapshot(); aborts != 0 {
		t.Fatalf("aborts = %d, want 0 (removal drains, never aborts)", aborts)
	}
	close(finish)

	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if got := mainCount.Load(); got < 1 {
		t.Fatalf("main chain received %d declared packets, want at least 1", got)
	}
	select {
	case event := <-attachErrors:
		t.Fatalf("unexpected attach error event: %+v", event)
	default:
	}
}

// TestOnStreamLateBranchReceivesFramesFromFrameSource pins the frame-domain
// anchor: a frame-producing custom source announces a late stream and the rule
// branch's sink receives that stream's frames directly (no decode).
func TestOnStreamLateBranchReceivesFramesFromFrameSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stop := make(chan struct{})
	late := av.Stream{
		ID:   "late-pcm",
		Type: av.MediaAudio,
		Codec: av.CodecParameters{
			Type:         av.MediaAudio,
			SampleRate:   48_000,
			Channels:     codec.Stereo,
			SampleFormat: av.SampleFormatS16,
		},
	}
	input := Source("pcm",
		shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Stereo, av.SampleFormatS16)),
		func(sctx context.Context, push SourcePush) error {
			announced := late
			if _, err := push.Event(av.Event{Type: av.EventStreamAdded, Stream: &announced}); err != nil {
				return err
			}
			for {
				select {
				case <-stop:
					return push.EOS()
				case <-sctx.Done():
					return sctx.Err()
				case <-time.After(time.Millisecond):
					frame := av.Frame{
						StreamID: late.ID,
						Type:     av.MediaAudio,
						Audio: &av.AudioFrame{
							SampleRate:   48_000,
							Channels:     codec.Stereo,
							SampleFormat: av.SampleFormatS16,
							Samples:      1,
						},
						Planes: []av.Plane{{Buffer: av.Buffer{Bytes: []byte{7, 0, 7, 0}, Ownership: av.BufferImmutable}}},
					}
					if _, err := push.Frame(&frame); err != nil {
						return err
					}
				}
			}
		},
	)

	var lateFrames atomic.Int32
	monitor := Sink(SinkFunc("monitor", func(_ context.Context, msg Message) error {
		if msg.Kind == pipeline.MessageFrame && msg.Frame.StreamID == late.ID {
			lateFrames.Add(1)
		}
		return nil
	}))

	task, err := From(input).
		OnStream(MatchStreamID(late.ID), Branch("watch").To(monitor)).
		Audio().To(Sink(SinkFunc("main", func(context.Context, Message) error { return nil }))).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	waitForCount(t, "late frames", 1, lateFrames.Load)
	close(stop)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
}

// TestOnStreamLateBranchAutoInsertsConversion verifies that late branches go
// through the same shape compile pass as planned chains: a 44.1kHz late frame
// stream feeding a 48kHz Opus encoder under .Auto(shape.AllowResample()) gets
// the resample inserted at attach time, and the branch's sink receives the
// encoded packets.
func TestOnStreamLateBranchAutoInsertsConversion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const rate = 44_100
	stop := make(chan struct{})
	late := av.Stream{
		ID:   "late-mic",
		Type: av.MediaAudio,
		Codec: av.CodecParameters{
			Type:         av.MediaAudio,
			SampleRate:   rate,
			Channels:     codec.Stereo,
			SampleFormat: av.SampleFormatS16,
		},
	}
	input := Source("pcm",
		shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Stereo, av.SampleFormatS16)),
		func(sctx context.Context, push SourcePush) error {
			announced := late
			if _, err := push.Event(av.Event{Type: av.EventStreamAdded, Stream: &announced}); err != nil {
				return err
			}
			samples := rate / 100
			payload := make([]byte, samples*codec.Stereo*2)
			for {
				select {
				case <-stop:
					return push.EOS()
				case <-sctx.Done():
					return sctx.Err()
				case <-time.After(time.Millisecond):
					frame := av.Frame{
						StreamID: late.ID,
						Type:     av.MediaAudio,
						Audio: &av.AudioFrame{
							SampleRate:   rate,
							Channels:     codec.Stereo,
							SampleFormat: av.SampleFormatS16,
							Samples:      samples,
						},
						Planes: []av.Plane{{Buffer: av.Buffer{Bytes: payload, Ownership: av.BufferImmutable}}},
					}
					if _, err := push.Frame(&frame); err != nil {
						return err
					}
				}
			}
		},
	)

	var encoded atomic.Int32
	encodedSink := Sink(SinkFunc("encoded", func(_ context.Context, msg Message) error {
		// The branch sink only ever receives the branch's own output: encoded
		// packets derived from the late stream's frames.
		if msg.Kind == pipeline.MessagePacket {
			encoded.Add(1)
		}
		return nil
	}))

	task, err := From(input).
		OnStream(MatchStreamID(late.ID), Branch("enc").
			Auto(shape.AllowResample()).
			Encode(codec.Opus(codec.Bitrate(96_000))).
			To(encodedSink)).
		Audio().To(Sink(SinkFunc("main", func(context.Context, Message) error { return nil }))).
		UseRuntime(solverTestOpusRuntime(WithStdFilters())).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	attachErrors := task.Watch(WatchTypes(av.EventAttachError))
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	waitForCondition(t, "inserted resample node attached", func() bool {
		for _, node := range task.Describe().Nodes {
			if strings.Contains(node.Name, "enc-late-mic/") && strings.Contains(node.Name, "resample") {
				return true
			}
		}
		return false
	})
	waitForCount(t, "encoded late packets", 1, encoded.Load)
	close(stop)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-attachErrors:
		t.Fatalf("unexpected attach error event: %+v", event)
	default:
	}
}

// TestOnStreamRuleVisibleInExplain pins the plan visibility: declared rules
// appear as decisions before any stream exists.
func TestOnStreamRuleVisibleInExplain(t *testing.T) {
	ctx := context.Background()
	input := Source("mic",
		shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48_000, codec.Stereo, av.SampleFormatS16)),
		func(context.Context, SourcePush) error { return nil },
	)
	monitor := Sink(SinkFunc("monitor", func(context.Context, Message) error { return nil }))
	job := From(input).
		OnStream(MatchMedia(av.MediaAudio), Branch("record").Copy().To(monitor)).
		Audio().Copy().To(Sink(SinkFunc("main", func(context.Context, Message) error { return nil })))

	report, err := job.Explain(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, decision := range report.Decisions {
		if decision.Code != "stream_rule" {
			continue
		}
		found = true
		if !strings.Contains(decision.Message, "media=audio") || !strings.Contains(decision.Message, "record-<stream>") {
			t.Fatalf("stream rule decision = %+v", decision)
		}
	}
	if !found {
		t.Fatalf("no stream_rule decision in plan: %+v", report.Decisions)
	}
}

// TestOnStreamAttachFailureSurfacesEventAndRollsBack drives the failure path:
// the rule branch needs an ogg muxer the runtime does not have, so the late
// attach fails. The failure surfaces as av.EventAttachError on Watch, the
// graph is left unchanged (rollback), and the task keeps running.
func TestOnStreamAttachFailureSurfacesEventAndRollsBack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stopLate := make(chan struct{})
	finish := make(chan struct{})
	late := lateOpusStream("late-audio")
	input := lateStreamSource(late, 0xB, stopLate, finish)

	// No ogg muxer registered: the rule branch cannot build — its destination
	// opens first and must be aborted by the rollback.
	rt := New(withTestFormats())
	state := &syncObjectState{}
	bad := Writer("late.ogg", state.open, Format(av.FormatOgg))

	task, err := From(input).
		OnStream(MatchMedia(av.MediaAudio), Branch("bad").Copy().To(bad)).
		Audio().Copy().To(Sink(SinkFunc("main", func(context.Context, Message) error { return nil }))).
		UseRuntime(rt).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	nodesBefore := len(task.Describe().Nodes)

	attachErrors := task.Watch(WatchTypes(av.EventAttachError))
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	select {
	case event := <-attachErrors:
		if event.Type != av.EventAttachError || event.StreamID != late.ID || event.Cause == nil {
			t.Fatalf("attach error event = %+v", event)
		}
		if !strings.Contains(event.Reason, "bad") {
			t.Fatalf("attach error reason = %q, want the rule branch name", event.Reason)
		}
	case <-ctx.Done():
		t.Fatal("no attach error event surfaced")
	}

	if got := len(task.Describe().Nodes); got != nodesBefore {
		t.Fatalf("graph nodes = %d after failed rule attach, want %d (rollback)", got, nodesBefore)
	}
	if len(task.Snapshot().Branches) != 0 {
		t.Fatalf("branches = %+v, want none after rollback", task.Snapshot().Branches)
	}
	if opens, closes, commits, aborts, _, _ := state.snapshot(); opens != 1 || aborts != 1 || closes != 1 || commits != 0 {
		t.Fatalf("destination opens=%d aborts=%d closes=%d commits=%d, want the opened writer aborted by rollback",
			opens, aborts, closes, commits)
	}

	close(stopLate)
	close(finish)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
}

// TestOnStreamMultipleRulesAllApply documents the composition semantics: every
// rule matching a discovered stream attaches independently.
func TestOnStreamMultipleRulesAllApply(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stopLate := make(chan struct{})
	finish := make(chan struct{})
	late := lateOpusStream("late-audio")
	input := lateStreamSource(late, 0xC, stopLate, finish)

	var byMedia, byCodec atomic.Int32
	count := func(name string, counter *atomic.Int32) Destination {
		return Sink(SinkFunc(name, func(_ context.Context, msg Message) error {
			if msg.Kind == pipeline.MessagePacket && msg.Packet.StreamID == late.ID {
				counter.Add(1)
			}
			return nil
		}))
	}

	task, err := From(input).
		OnStream(MatchMedia(av.MediaAudio), Branch("media").Copy().To(count("media-sink", &byMedia))).
		OnStream(MatchCodec(av.CodecOpus), Branch("codec").Copy().To(count("codec-sink", &byCodec))).
		Audio().Copy().To(Sink(SinkFunc("main", func(context.Context, Message) error { return nil }))).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	waitForCount(t, "media rule", 1, byMedia.Load)
	waitForCount(t, "codec rule", 1, byCodec.Load)
	close(stopLate)
	waitForCondition(t, "both branches detached", func() bool {
		return len(task.Snapshot().Branches) == 0
	})
	close(finish)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
}

// TestOnStreamValidation pins the build-time refusals of the grammar.
func TestOnStreamValidation(t *testing.T) {
	ctx := context.Background()
	monitor := Sink(SinkFunc("monitor", func(context.Context, Message) error { return nil }))
	main := Sink(SinkFunc("main", func(context.Context, Message) error { return nil }))
	source := func(name string) InputSpec {
		return Source(name,
			shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48_000, codec.Stereo, av.SampleFormatS16)),
			func(context.Context, SourcePush) error { return nil },
		)
	}

	requireRuleError := func(t *testing.T, job *Job, fragment string) {
		t.Helper()
		_, err := job.Build(ctx)
		var buildErr *BuildError
		if !errors.As(err, &buildErr) || buildErr.Code != "stream_rule_invalid" {
			t.Fatalf("err = %v, want stream_rule_invalid", err)
		}
		if !strings.Contains(buildErr.Reason, fragment) {
			t.Fatalf("reason = %q, want %q", buildErr.Reason, fragment)
		}
	}

	t.Run("empty matcher", func(t *testing.T) {
		job := From(source("mic")).
			OnStream(StreamMatch{}, Branch("late").Copy().To(monitor)).
			Audio().Copy().To(main)
		requireRuleError(t, job, "no matcher")
	})
	t.Run("branch with From", func(t *testing.T) {
		job := From(source("mic")).
			OnStream(MatchMedia(av.MediaAudio), Branch("late").From(PacketTap("pkts")).Copy().To(monitor)).
			Audio().Copy().To(main)
		requireRuleError(t, job, "must not declare .From")
	})
	t.Run("no branches", func(t *testing.T) {
		job := From(source("mic")).
			OnStream(MatchMedia(av.MediaAudio)).
			Audio().Copy().To(main)
		requireRuleError(t, job, "no branches")
	})
	t.Run("multi input", func(t *testing.T) {
		job := From(source("mic"), source("cam")).
			OnStream(MatchMedia(av.MediaAudio), Branch("late").Copy().To(monitor)).
			Audio(InputName("mic")).Copy().To(main)
		requireRuleError(t, job, "exactly one input")
	})
}

// onStreamRTPReceiver feeds an opus stream, then announces a late VP8 stream
// on its event channel and pumps that stream's RTP packets until stopped.
type onStreamRTPReceiver struct {
	audio    av.Stream
	video    av.Stream
	payloads rtpav.PayloadMap
	events   chan av.Event

	announced bool
	sent      int
	stop      <-chan struct{}
}

func (r *onStreamRTPReceiver) Streams(context.Context) ([]av.Stream, error) {
	return []av.Stream{r.audio}, nil
}

func (r *onStreamRTPReceiver) PayloadMap() rtpav.PayloadMap {
	return r.payloads
}

func (r *onStreamRTPReceiver) ReadRTP(ctx context.Context) (*rtp.Packet, error) {
	if !r.announced {
		r.announced = true
		announced := r.video
		r.events <- av.Event{Type: av.EventStreamAdded, StreamID: announced.ID, Stream: &announced}
		return &rtp.Packet{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
			Payload: []byte{1, 2, 3},
		}, nil
	}
	select {
	case <-r.stop:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(time.Millisecond):
		r.sent++
		return &rtp.Packet{
			Header:  rtp.Header{PayloadType: 96, Marker: true, Timestamp: uint32(90 * r.sent), SequenceNumber: uint16(r.sent)},
			Payload: []byte{0x10, 0x00, 0x11, 0x22},
		}, nil
	}
}

func (r *onStreamRTPReceiver) Events() <-chan av.Event {
	return r.events
}

func (r *onStreamRTPReceiver) Close() error {
	return nil
}

// TestOnStreamRTPLateStreamAttachesBranch wires the real producer end to end:
// an RTP receive provider announces a late VP8 stream mid-run; the rtpav
// source derives a depacketizer for it and forwards the announce, and the
// OnStream rule attaches a packet branch that receives the late stream's
// packets under its own stream id.
func TestOnStreamRTPLateStreamAttachesBranch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	audio := audioOpusTestStream()
	video := av.Stream{
		ID:       "cam",
		Type:     av.MediaVideo,
		TimeBase: av.TimeBase{Num: 1, Den: 90000},
		Codec: av.CodecParameters{
			ID:        av.CodecVP8,
			Type:      av.MediaVideo,
			ClockRate: 90000,
		},
	}
	stop := make(chan struct{})
	receiver := &onStreamRTPReceiver{
		audio: audio,
		video: video,
		payloads: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{
			{PayloadType: 111, Parameters: audio.Codec, MIMEType: rtpav.MIMEOpus, ClockRate: 48000, Channels: codec.Stereo},
			{PayloadType: 96, Parameters: video.Codec, MIMEType: rtpav.MIMEVP8, ClockRate: 90000},
		}),
		events: make(chan av.Event, 2),
		stop:   stop,
	}

	var lateVideo atomic.Int32
	monitor := Sink(SinkFunc("monitor", func(_ context.Context, msg Message) error {
		if msg.Kind == pipeline.MessagePacket && msg.Packet.StreamID == video.ID {
			lateVideo.Add(1)
		}
		return nil
	}))

	task, err := From(
		Input(rtpav.Receive(receiver, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus()))),
	).
		OnStream(MatchMedia(av.MediaVideo), Branch("cam-watch").Copy().To(monitor)).
		Audio().Copy().To(Sink(SinkFunc("main", func(context.Context, Message) error { return nil }))).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	waitForCount(t, "late video packets", 1, lateVideo.Load)
	waitForCondition(t, "rule branch attached", func() bool {
		return taskHasBranch(task, "cam-watch-cam")
	})
	close(stop)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
}
