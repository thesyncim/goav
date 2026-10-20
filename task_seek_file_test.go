package goav

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/goav/control"

	"github.com/thesyncim/goav/av"
	matroskaadapter "github.com/thesyncim/goav/container/matroska"
	"github.com/thesyncim/goav/format"
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
	for i := range 10 {
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

// paceFileClock is the deterministic av.Clock the realtime file tests inject:
// Sleep records each requested wait and advances Now by it, so paced playback
// runs instantly and tests assert the exact pacing the pump asked for.
type paceFileClock struct {
	mu     sync.Mutex
	now    time.Duration
	sleeps []time.Duration
}

func (c *paceFileClock) Now() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *paceFileClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sleeps = append(c.sleeps, d)
	if d > 0 {
		c.now += d
	}
	return nil
}

func (c *paceFileClock) recorded() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.sleeps...)
}

func assertPacingSleeps(t *testing.T, got []time.Duration, want []time.Duration) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("pacing sleeps = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pacing sleeps = %v, want %v", got, want)
		}
	}
}

// repeatSleeps builds the expected pacing sequence: count waits of d each.
func repeatSleeps(d time.Duration, count int) []time.Duration {
	out := make([]time.Duration, count)
	for i := range out {
		out[i] = d
	}
	return out
}

func seekFileTask(t *testing.T, reader io.Reader, sink *seekFileSink, options ...Option) LiveTask {
	t.Helper()
	task, err := mustNew(append([]Option{WithFormatAdapter(matroskaadapter.Register)}, options...)...).Build(context.Background(), FileInput("seek.mkv", reader).
		Audio().
		To(Sink(SinkFunc("rec", sink.record))))
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
	// Realtime tasks pace file playback; the fake clock keeps the test instant.
	task := seekFileTask(t, bytes.NewReader(data), sink, WithClock(&paceFileClock{}))

	// 450ms sits between the 400ms and 500ms cues: the demuxer repositions
	// at-or-before, so playback resumes at the 400ms keyframe with a
	// discontinuity ahead of it. Delivered before Run, the control
	// pre-positions the source — no wall-clock waiting in the test.
	if err := task.Control(ctx, control.Seek(450*time.Millisecond)); err != nil {
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
	task, err := mustNew(WithFormatAdapter(matroskaadapter.Register), WithClock(&paceFileClock{})).Build(ctx, FileInput("seek.mkv", bytes.NewReader(data)).
		Audio().
		Tap("audio.packets").
		To(Sink(SinkFunc("base", sink.record))))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	// A recording branch attached at runtime gets its own transactional
	// destination — the seam destination snapshots report on.
	if _, err := task.Attach(ctx, task.Tap("audio.packets").
		As("rec").
		To(lifecycleTestSink("rec"))); err != nil {
		t.Fatal(err)
	}

	// Trim-to-file from a real container: play [300ms, 600ms) and end
	// naturally, so the destination commits exactly as at the end of media.
	if err := task.Control(ctx, control.Must(control.Segment(300*time.Millisecond, 600*time.Millisecond))); err != nil {
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
	if !ok || committed.State != DestinationFinalizing {
		t.Fatalf("destination after segment run = %+v, want committed rec destination", committed)
	}
}

func TestTaskSeekNonSeekableFileReaderIsRefusedAndPlaysOn(t *testing.T) {
	data := muxSeekFileMKV(t)
	t.Run("plain reader", func(t *testing.T) {
		// Hide the reader's Seek method: a pure io.Reader cannot reposition.
		assertSeekRefusedPlaysOn(t, struct{ io.Reader }{bytes.NewReader(data)})
	})
	t.Run("pipe", func(t *testing.T) {
		// An *os.File has a Seek method, but over a pipe it cannot seek.
		read, write, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer read.Close()
		go func() {
			_, _ = write.Write(data)
			_ = write.Close()
		}()
		assertSeekRefusedPlaysOn(t, read)
	})
}

func assertSeekRefusedPlaysOn(t *testing.T, reader io.Reader) {
	ctx := context.Background()
	sink := &seekFileSink{}
	task := seekFileTask(t, reader, sink, WithClock(&paceFileClock{}))

	// The seek is refused when it is asked, with the typed error and the
	// fix, and the task plays on: every packet arrives, in order, with no
	// discontinuity.
	err := task.Control(ctx, control.Seek(450*time.Millisecond))
	if !errors.Is(err, format.ErrNotSeekable) {
		t.Fatalf("Seek err = %v, want it to wrap format.ErrNotSeekable", err)
	}
	if !strings.Contains(err.Error(), "read a file or an io.ReadSeeker") {
		t.Fatalf("Seek err = %v, want the fix named", err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatalf("Run err = %v, want the task to play on", err)
	}
	var want []int64
	for ms := int64(0); ms <= 900; ms += 100 {
		want = append(want, ms*int64(time.Millisecond))
	}
	assertSeekFileLog(t, sink.snapshot(), append(want, -2))
}

func TestTaskRealtimeFilePlaybackIsPaced(t *testing.T) {
	ctx := context.Background()
	data := muxSeekFileMKV(t)
	sink := &seekFileSink{}
	clock := &paceFileClock{}
	// A realtime runtime playing a file gets PTS pacing: that is the point of
	// a clock-driven pump, and how a file stands in for a live feed.
	task := seekFileTask(t, bytes.NewReader(data), sink, WithRealtime(true), WithClock(clock))

	if err := task.Run(ctx); err != nil {
		t.Fatalf("Run err = %v", err)
	}

	// The first packet anchors the timeline; each of the nine that follow
	// waits its 100ms media-time delta on the runtime clock.
	assertPacingSleeps(t, clock.recorded(), repeatSleeps(100*time.Millisecond, 9))
	want := []int64{}
	for ms := int64(0); ms <= 900; ms += 100 {
		want = append(want, ms*int64(time.Millisecond))
	}
	want = append(want, -2)
	assertSeekFileLog(t, sink.snapshot(), want)
}

func TestTaskOfflineFileTaskPumpsUnpaced(t *testing.T) {
	ctx := context.Background()
	data := muxSeekFileMKV(t)
	sink := &seekFileSink{}
	clock := &paceFileClock{}
	// A file job with no live input is the offline transcode shape by default:
	// full-speed pumping, the clock is never consulted.
	task := seekFileTask(t, bytes.NewReader(data), sink, WithClock(clock))

	// With no pacing there is nothing for Rate to scale — the typed rejection
	// says offline tasks run unpaced instead of lying about playback speed.
	if err := task.Control(ctx, control.Must(control.Rate(2.0))); !errors.Is(err, format.ErrRateUnsupported) {
		t.Fatalf("Rate err = %v, want format.ErrRateUnsupported", err)
	}

	if err := task.Run(ctx); err != nil {
		t.Fatalf("Run err = %v", err)
	}
	if sleeps := clock.recorded(); len(sleeps) != 0 {
		t.Fatalf("offline task slept %v, want full-speed pumping", sleeps)
	}
}

func TestTaskRateOnFileScalesPacing(t *testing.T) {
	ctx := context.Background()
	data := muxSeekFileMKV(t)
	sink := &seekFileSink{}
	clock := &paceFileClock{}
	task := seekFileTask(t, bytes.NewReader(data), sink, WithRealtime(true), WithClock(clock))

	// Rate on a file is a pacing multiplier now: at 2x the 100ms cadence
	// plays in 50ms steps of clock time. Delivered before Run, the control
	// pre-positions the source (direct-runner delivery).
	if err := task.Control(ctx, control.Must(control.Rate(2.0))); err != nil {
		t.Fatalf("Rate err = %v", err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatalf("Run err = %v", err)
	}

	assertPacingSleeps(t, clock.recorded(), repeatSleeps(50*time.Millisecond, 9))
	// A pure pacing change repositions nothing: the full timeline plays with
	// no discontinuity.
	want := []int64{}
	for ms := int64(0); ms <= 900; ms += 100 {
		want = append(want, ms*int64(time.Millisecond))
	}
	want = append(want, -2)
	assertSeekFileLog(t, sink.snapshot(), want)
}

func TestTaskSeekAndRateComposeOnFile(t *testing.T) {
	ctx := context.Background()
	data := muxSeekFileMKV(t)
	sink := &seekFileSink{}
	clock := &paceFileClock{}
	task := seekFileTask(t, bytes.NewReader(data), sink, WithRealtime(true), WithClock(clock))

	// Seek and Rate compose: playback resumes at the 400ms keyframe (anchor
	// reset — the first packet after the discontinuity emits immediately) and
	// the remaining 100ms cadence paces at 50ms of clock per step.
	if err := task.Control(ctx, control.Seek(450*time.Millisecond)); err != nil {
		t.Fatalf("Seek err = %v", err)
	}
	if err := task.Control(ctx, control.Must(control.Rate(2.0))); err != nil {
		t.Fatalf("Rate err = %v", err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatalf("Run err = %v", err)
	}

	assertPacingSleeps(t, clock.recorded(), repeatSleeps(50*time.Millisecond, 5))
	want := []int64{-1}
	for ms := int64(400); ms <= 900; ms += 100 {
		want = append(want, ms*int64(time.Millisecond))
	}
	want = append(want, -2)
	assertSeekFileLog(t, sink.snapshot(), want)
}
