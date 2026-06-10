package goav

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	matroskaadapter "github.com/thesyncim/goav/container/matroska"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/info"
	"github.com/thesyncim/goav/pipeline"
)

// seekFileWriteSeeker is the minimal io.WriteSeeker the matroska muxer needs
// to write a finished, cue-indexed (seekable) file into memory.
type seekFileWriteSeeker struct {
	bytes []byte
	pos   int64
}

func (w *seekFileWriteSeeker) Write(p []byte) (int, error) {
	end := w.pos + int64(len(p))
	if end > int64(len(w.bytes)) {
		next := make([]byte, end)
		copy(next, w.bytes)
		w.bytes = next
	}
	copy(w.bytes[w.pos:end], p)
	w.pos = end
	return len(p), nil
}

func (w *seekFileWriteSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		w.pos = offset
	case io.SeekCurrent:
		w.pos += offset
	case io.SeekEnd:
		w.pos = int64(len(w.bytes)) + offset
	}
	return w.pos, nil
}

// muxSeekFileMKV builds a real in-memory Matroska file: one Opus track, one
// packet every 100ms from 0 through 900ms, each in its own cluster so the
// muxer cues every packet (audio packets are cue-indexed by default).
func muxSeekFileMKV(t *testing.T) []byte {
	t.Helper()
	ws := &seekFileWriteSeeker{}
	muxer, err := matroskaadapter.NewMuxer(ws, matroskaadapter.MuxerOptions{ClusterMaxDurationNS: int64(time.Millisecond)})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(matroskaadapter.Track{
		Type:              matroskaadapter.TrackAudio,
		Codec:             matroskaadapter.CodecOpus,
		DefaultDurationNS: int64(100 * time.Millisecond),
		Audio:             matroskaadapter.AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err := muxer.WritePacket(matroskaadapter.Packet{
			TrackID:    trackID,
			TimeNS:     int64(i) * int64(100*time.Millisecond),
			DurationNS: int64(100 * time.Millisecond),
			Keyframe:   true,
			Data:       []byte{0xf8, 0xff, 0xfe},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}
	return ws.bytes
}

// seekFileSink records packet PTS nanoseconds, discontinuities (-1), and
// end-of-stream (-2) in arrival order at the task's destination.
type seekFileSink struct {
	mu  sync.Mutex
	log []int64
}

func (s *seekFileSink) record(_ context.Context, msg Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case msg.Kind == pipeline.MessagePacket && msg.Packet != nil:
		s.log = append(s.log, msg.Packet.PTS.Value)
	case msg.Kind == pipeline.MessageEvent && msg.Event != nil && msg.Event.Type == av.EventDiscontinuity:
		s.log = append(s.log, -1)
	case msg.Kind == pipeline.MessageEvent && msg.Event != nil && msg.Event.Type == av.EventEndOfStream:
		s.log = append(s.log, -2)
	}
	return nil
}

func (s *seekFileSink) snapshot() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int64(nil), s.log...)
}

func seekFileTask(t *testing.T, reader io.Reader, sink *seekFileSink) Task {
	t.Helper()
	task, err := From(FileInput("seek.mkv", reader)).
		UseRuntime(New(WithFormatAdapter(matroskaadapter.Register))).
		Audio().Copy().
		To(Sink(SinkFunc("rec", sink.record))).
		Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = task.Close() })
	return task
}

func assertSeekFileLog(t *testing.T, got []int64, want []int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("sink log = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sink log = %v, want %v", got, want)
		}
	}
}

func TestTaskSeekRepositionsRealMatroskaFile(t *testing.T) {
	ctx := context.Background()
	data := muxSeekFileMKV(t)
	sink := &seekFileSink{}
	task := seekFileTask(t, bytes.NewReader(data), sink)

	// 450ms sits between the 400ms and 500ms cues: the demuxer repositions
	// at-or-before, so playback resumes at the 400ms keyframe with a
	// discontinuity ahead of it. Delivered before Run, the control
	// pre-positions the source — no wall-clock waiting in the test.
	if err := task.Control(ctx, Seek(450*time.Millisecond)); err != nil {
		t.Fatalf("Seek err = %v", err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatalf("Run err = %v", err)
	}

	want := []int64{-1}
	for ms := int64(400); ms <= 900; ms += 100 {
		want = append(want, ms*int64(time.Millisecond))
	}
	want = append(want, -2)
	assertSeekFileLog(t, sink.snapshot(), want)
}

func TestTaskSegmentExportsRealMatroskaWindow(t *testing.T) {
	ctx := context.Background()
	data := muxSeekFileMKV(t)
	sink := &seekFileSink{}
	task, err := From(FileInput("seek.mkv", bytes.NewReader(data))).
		UseRuntime(New(WithFormatAdapter(matroskaadapter.Register))).
		Audio().Copy().
		Tap(PacketTap("audio.packets")).
		To(Sink(SinkFunc("base", sink.record))).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	// A recording branch attached at runtime gets its own transactional
	// destination — the seam destination snapshots report on.
	if _, err := task.Attach(ctx, Branch("rec").
		From(PacketTap("audio.packets")).
		To(lifecycleTestSink("rec"))); err != nil {
		t.Fatal(err)
	}

	// Trim-to-file from a real container: play [300ms, 600ms) and end
	// naturally, so the destination commits exactly as at the end of media.
	if err := task.Control(ctx, Segment(300*time.Millisecond, 600*time.Millisecond)); err != nil {
		t.Fatalf("Segment err = %v", err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatalf("Run err = %v", err)
	}

	want := []int64{
		-1,
		int64(300 * time.Millisecond),
		int64(400 * time.Millisecond),
		int64(500 * time.Millisecond),
		-2,
	}
	assertSeekFileLog(t, sink.snapshot(), want)

	committed, ok := destinationSnapshotByName(task.Snapshot().Destinations, "rec")
	if !ok || committed.State != info.DestinationCommitted {
		t.Fatalf("destination after segment run = %+v, want committed rec destination", committed)
	}
}

func TestTaskSeekNonSeekableFileReaderFailsClearly(t *testing.T) {
	ctx := context.Background()
	data := muxSeekFileMKV(t)
	// Hide the reader's Seek method: a pure io.Reader cannot reposition, like
	// a pipe or a network stream.
	sink := &seekFileSink{}
	task := seekFileTask(t, struct{ io.Reader }{bytes.NewReader(data)}, sink)

	// The control is recorded (the demuxer supports seeking in general); the
	// reposition itself fails when the pump applies it, stopping the task with
	// the typed error instead of playing on from the wrong position.
	if err := task.Control(ctx, Seek(450*time.Millisecond)); err != nil {
		t.Fatalf("Seek err = %v", err)
	}
	err := task.Run(ctx)
	if !errors.Is(err, format.ErrNotSeekable) {
		t.Fatalf("Run err = %v, want it to wrap format.ErrNotSeekable", err)
	}
}

func TestTaskRateOnFileInputRejectedHonestly(t *testing.T) {
	ctx := context.Background()
	data := muxSeekFileMKV(t)
	sink := &seekFileSink{}
	task := seekFileTask(t, bytes.NewReader(data), sink)

	// A demux pump has no pacing to scale, so Rate reports the typed
	// not-supported error instead of lying about playback speed.
	if err := task.Control(ctx, Rate(2.0)); !errors.Is(err, format.ErrRateUnsupported) {
		t.Fatalf("Rate err = %v, want format.ErrRateUnsupported", err)
	}
}
