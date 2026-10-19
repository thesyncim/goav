package engine

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

// fakeDevice is a deterministic speaker: time moves only when the test steps
// it, each step pulls exactly that much audio from the engine. Between Fill
// and the DAC it models what oto has: buffered frames in the player (which it
// reports), then the device queue plus hardware latency (which the device
// reports once, as a constant: see setQueue).
type fakeDevice struct {
	t        *testing.T
	e        *Engine
	now      time.Duration
	buffered int
	queue    time.Duration
	last     []byte
}

func newFakeDevice(t *testing.T, latency time.Duration) *fakeDevice {
	d := &fakeDevice{t: t}
	d.e = New(latency, func() time.Duration { return d.now })
	d.e.SetDevice(func() int { return d.buffered }, 0)
	d.e.Clock().Now() // anchor the clock at zero
	return d
}

// setQueue models a device queue (and hardware latency) of q past the player
// buffer, and tells the engine, as openSpeaker does.
func (d *fakeDevice) setQueue(q time.Duration) {
	d.queue = q
	d.e.SetDevice(func() int { return d.buffered }, q)
}

// step plays d of device time in 1ms pulls, keeping buffered frames in hand.
func (d *fakeDevice) step(total time.Duration) {
	for elapsed := time.Duration(0); elapsed < total; elapsed += time.Millisecond {
		d.now += time.Millisecond
		played := durationToFrames(d.now)
		want := played + int64(d.buffered) + durationToFrames(d.queue)
		if n := want - d.e.written.Load(); n > 0 {
			d.last = make([]byte, n*BytesPerFrame)
			d.e.Fill(d.last)
		}
	}
}

// master reads the presenter's view of the master clock.
func (d *fakeDevice) master() (time.Duration, bool) {
	now := d.e.Clock().Now()
	d.e.mu.Lock()
	defer d.e.mu.Unlock()
	return d.e.masterLocked(now)
}

func audioFrame(pts time.Duration, rate, channels, samples int, value int16) *av.Frame {
	data := make([]byte, 2*channels*samples)
	for i := 0; i < channels*samples; i++ {
		binary.LittleEndian.PutUint16(data[2*i:], uint16(value))
	}
	return &av.Frame{
		Type:   av.MediaAudio,
		PTS:    av.Timestamp{Value: pts.Microseconds(), Base: av.TimeBase{Num: 1, Den: 1_000_000}},
		Audio:  &av.AudioFrame{SampleRate: rate, Channels: channels, SampleFormat: av.SampleFormatS16, Samples: samples},
		Planes: []av.Plane{{Buffer: av.Buffer{Bytes: data}}},
	}
}

func videoFrame(pts time.Duration) *av.Frame {
	f := planarFrame(av.PixelFormatI420, 2, 2, func(int, int) byte { return 126 }, func(int, int) byte { return 128 }, func(int, int) byte { return 128 })
	f.PTS = av.Timestamp{Value: pts.Microseconds(), Base: av.TimeBase{Num: 1, Den: 1_000_000}}
	return f
}

func (d *fakeDevice) pushAudio(from, to time.Duration) {
	d.t.Helper()
	for pts := from; pts < to; pts += 20 * time.Millisecond {
		if err := d.e.PushAudio(context.Background(), audioFrame(pts, OutputRate, 2, 960, 1000)); err != nil {
			d.t.Fatal(err)
		}
	}
}

func (d *fakeDevice) pushVideo(from, to, interval time.Duration) {
	d.t.Helper()
	for pts := from; pts < to; pts += interval {
		if err := d.e.PushVideo(context.Background(), videoFrame(pts)); err != nil {
			d.t.Fatal(err)
		}
	}
}

func near(got, want, tolerance time.Duration) bool {
	return got >= want-tolerance && got <= want+tolerance
}

func TestDecide(t *testing.T) {
	ms := time.Millisecond
	frames := []time.Duration{0, 33 * ms, 67 * ms, 100 * ms}
	for _, tc := range []struct {
		name    string
		pts     []time.Duration
		m       time.Duration
		drop    int
		present bool
	}{
		{"empty queue waits", nil, 50 * ms, 0, false},
		{"early head waits", frames, -10 * ms, 0, false},
		{"due within tolerance presents", frames, -3 * ms, 0, true},
		{"exactly due presents", frames, 0, 0, true},
		{"head due, successor not yet: present head", frames, 20 * ms, 0, true},
		{"successor due: drop the late head", frames, 40 * ms, 1, true},
		{"two late frames dropped", frames, 70 * ms, 2, true},
		{"last frame kept however late", frames, 900 * ms, 3, true},
		{"single late frame presents", frames[:1], 900 * ms, 0, true},
	} {
		drop, present := Decide(tc.pts, tc.m, 4*ms)
		if drop != tc.drop || present != tc.present {
			t.Errorf("%s: Decide = (%d, %v), want (%d, %v)", tc.name, drop, present, tc.drop, tc.present)
		}
	}
}

// Audio is the master: at a 100 Hz display every 30 fps frame is shown on
// time against the heard position, none dropped.
func TestVideoFollowsTheHeardAudioPosition(t *testing.T) {
	d := newFakeDevice(t, 100*time.Millisecond)
	d.pushAudio(0, time.Second)
	d.pushVideo(0, time.Second, 33*time.Millisecond)
	if f := d.e.NextVideo(d.e.Clock().Now()); f != nil {
		t.Fatal("presented a frame before any audio was heard")
	}
	presented := 0
	for range 100 {
		d.step(10 * time.Millisecond)
		m, ok := d.master()
		if !ok {
			t.Fatal("no master clock while audio plays")
		}
		if f := d.e.NextVideo(d.e.Clock().Now()); f != nil {
			pts, _ := f.PTS.ToDuration()
			if late := m - pts; late < -earlyTolerance || late > 10*time.Millisecond {
				t.Fatalf("frame %v presented at master %v", pts, m)
			}
			f.Release()
			presented++
		}
	}
	st := d.e.Stats()
	if presented != 31 || st.Dropped != 0 {
		t.Fatalf("presented %d dropped %d, want 31 and 0", presented, st.Dropped)
	}
	if st.MaxAVOffset > 10*time.Millisecond {
		t.Fatalf("max A/V offset %v, want <= one display interval", st.MaxAVOffset)
	}
}

// A display slower than the video drops the frames it cannot show and keeps
// the picture on the master clock.
func TestSlowDisplayDropsLateFrames(t *testing.T) {
	d := newFakeDevice(t, 100*time.Millisecond)
	d.pushAudio(0, time.Second)
	d.pushVideo(0, time.Second, 25*time.Millisecond)
	presented := 0
	for range 10 {
		d.step(100 * time.Millisecond)
		m, _ := d.master()
		if f := d.e.NextVideo(d.e.Clock().Now()); f != nil {
			pts, _ := f.PTS.ToDuration()
			if !near(pts, m, 25*time.Millisecond) {
				t.Fatalf("presented %v at master %v: not the latest due frame", pts, m)
			}
			f.Release()
			presented++
		}
	}
	st := d.e.Stats()
	if presented != 10 || st.Dropped != 30 {
		t.Fatalf("presented %d dropped %d, want 10 and 30", presented, st.Dropped)
	}
}

// The heard position subtracts what the device holds but has not played.
func TestHeardPositionExcludesDeviceBuffer(t *testing.T) {
	d := newFakeDevice(t, 50*time.Millisecond)
	d.buffered = 2048 // ~42.7ms in the player buffer
	// oto's 40ms device queue (darwin: 4 x 10ms) plus ~12ms of hardware
	// latency: unaccounted, the picture would lead the sound by 52ms.
	d.setQueue(52 * time.Millisecond)
	d.pushAudio(0, time.Second)
	d.step(300 * time.Millisecond)
	m, ok := d.master()
	if !ok {
		t.Fatal("no master clock")
	}
	// Playback started on the first pull (1s >= latency); the speaker has
	// played 300ms of it, the player buffer and device queue still pending.
	if !near(m, 300*time.Millisecond, 2*time.Millisecond) {
		t.Fatalf("master = %v, want ~300ms heard", m)
	}
}

func TestPrerollHoldsUntilTheLatencyIsQueued(t *testing.T) {
	d := newFakeDevice(t, 100*time.Millisecond)
	d.pushAudio(0, 60*time.Millisecond)
	d.step(50 * time.Millisecond)
	if _, ok := d.master(); ok {
		t.Fatal("master started with less than the latency queued")
	}
	d.pushAudio(60*time.Millisecond, 200*time.Millisecond)
	d.step(50 * time.Millisecond)
	m, ok := d.master()
	if !ok || !near(m, 50*time.Millisecond, 2*time.Millisecond) {
		t.Fatalf("master = %v %v after preroll, want ~50ms", m, ok)
	}
}

func TestPauseFreezesAndResumeContinues(t *testing.T) {
	d := newFakeDevice(t, 50*time.Millisecond)
	d.pushAudio(0, time.Second)
	d.step(200 * time.Millisecond)
	before, _ := d.master()
	d.e.SetPaused(true)
	d.step(500 * time.Millisecond)
	paused, _ := d.master()
	if !near(paused, before, 2*time.Millisecond) {
		t.Fatalf("master moved while paused: %v -> %v", before, paused)
	}
	for _, b := range d.last {
		if b != 0 {
			t.Fatal("paused device is not playing silence")
		}
	}
	if !d.e.Stats().Paused {
		t.Fatal("stats do not report the pause")
	}
	d.e.SetPaused(false)
	d.step(300 * time.Millisecond)
	after, _ := d.master()
	if !near(after, before+300*time.Millisecond, 2*time.Millisecond) {
		t.Fatalf("master after resume = %v, want %v", after, before+300*time.Millisecond)
	}
	if d.e.Stats().Underruns != 0 {
		t.Fatal("pause counted as an underrun")
	}
}

func TestRateScalesTheMasterClock(t *testing.T) {
	d := newFakeDevice(t, 50*time.Millisecond)
	d.pushAudio(0, time.Second)
	d.step(100 * time.Millisecond)
	before, _ := d.master()
	d.e.SetRate(2)
	d.step(200 * time.Millisecond)
	after, _ := d.master()
	if !near(after-before, 400*time.Millisecond, 3*time.Millisecond) {
		t.Fatalf("at 2x, 200ms of device time advanced media by %v, want 400ms", after-before)
	}
}

func TestUnderrunCountsAndEndDoesNot(t *testing.T) {
	d := newFakeDevice(t, 50*time.Millisecond)
	d.pushAudio(0, 300*time.Millisecond)
	d.step(400 * time.Millisecond)
	if got := d.e.Stats().Underruns; got != 1 {
		t.Fatalf("underruns = %d, want 1", got)
	}
	d.pushAudio(300*time.Millisecond, 340*time.Millisecond)
	d.e.End()
	d.step(100 * time.Millisecond)
	if got := d.e.Stats().Underruns; got != 1 {
		t.Fatalf("underruns after end = %d, want 1 (the final drain is not one)", got)
	}
	if !d.e.Drained() {
		t.Fatal("ended engine with nothing queued is not drained")
	}
}

// Without audio the external clock starts at the first frame once the
// latency passes, and runs at the device rate.
func TestVideoOnlyRunsOnTheExternalClock(t *testing.T) {
	d := newFakeDevice(t, 100*time.Millisecond)
	d.pushVideo(500*time.Millisecond, 1500*time.Millisecond, 40*time.Millisecond)
	d.step(50 * time.Millisecond)
	if f := d.e.NextVideo(d.e.Clock().Now()); f != nil {
		t.Fatal("presented before the latency elapsed")
	}
	d.step(60 * time.Millisecond)
	f := d.e.NextVideo(d.e.Clock().Now())
	if f == nil {
		t.Fatal("first frame not presented after the latency")
	}
	if pts, _ := f.PTS.ToDuration(); pts != 500*time.Millisecond {
		t.Fatalf("first frame %v, want 500ms", pts)
	}
	f.Release()
	d.step(400 * time.Millisecond)
	if m, _ := d.master(); !near(m, 900*time.Millisecond, 2*time.Millisecond) {
		t.Fatalf("external master = %v, want ~900ms", m)
	}
}

func TestFlushForgetsTheOldPosition(t *testing.T) {
	d := newFakeDevice(t, 50*time.Millisecond)
	d.pushAudio(0, time.Second)
	d.pushVideo(0, time.Second, 33*time.Millisecond)
	d.step(200 * time.Millisecond)
	d.e.Flush()
	if _, ok := d.master(); ok {
		t.Fatal("master survived the flush")
	}
	if f := d.e.NextVideo(d.e.Clock().Now()); f != nil {
		t.Fatal("flushed frame presented")
	}
	d.pushAudio(5*time.Second, 6*time.Second)
	d.step(20 * time.Millisecond)
	if m, ok := d.master(); !ok || m < 5*time.Second {
		t.Fatalf("master after flush and new media = %v %v, want >= 5s", m, ok)
	}
}

// A paused seek still shows the new position's first picture.
func TestPausedFlushShowsTheFirstNewFrame(t *testing.T) {
	d := newFakeDevice(t, 50*time.Millisecond)
	d.e.SetPaused(true)
	d.pushAudio(3*time.Second, 4*time.Second)
	d.pushVideo(3*time.Second, 3100*time.Millisecond, 33*time.Millisecond)
	d.step(20 * time.Millisecond)
	f := d.e.NextVideo(d.e.Clock().Now())
	if f == nil {
		t.Fatal("paused engine showed nothing at the new position")
	}
	f.Release()
	if f := d.e.NextVideo(d.e.Clock().Now()); f != nil {
		t.Fatal("paused engine kept presenting")
	}
}

// Video never blocks the shared sink while audio still needs data: it grows
// to the hard cap and sheds its oldest frame instead.
func TestVideoDoesNotStarveAudio(t *testing.T) {
	d := newFakeDevice(t, 100*time.Millisecond)
	d.pushAudio(0, 20*time.Millisecond) // audio seen, far below the latency
	done := make(chan struct{})
	go func() {
		defer close(done)
		d.pushVideo(0, 4*time.Second, 40*time.Millisecond) // 100 frames, 4s ahead
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("video push blocked while audio needed data")
	}
	if got := len(d.e.video); got != videoHardCap {
		t.Fatalf("video queue = %d, want the hard cap %d", got, videoHardCap)
	}
	if got := d.e.Stats().Dropped; got != 100-videoHardCap {
		t.Fatalf("dropped = %d, want %d", got, 100-videoHardCap)
	}
}

// With audio satisfied, video far ahead of the master backpressures until a
// flush releases it (the stale frame is discarded) or ctx ends.
func TestVideoBackpressureReleasesOnFlushAndContext(t *testing.T) {
	d := newFakeDevice(t, 50*time.Millisecond)
	d.pushAudio(0, time.Second)
	d.step(10 * time.Millisecond)
	errc := make(chan error, 1)
	go func() { errc <- d.e.PushVideo(context.Background(), videoFrame(5*time.Second)) }()
	select {
	case err := <-errc:
		t.Fatalf("push 5s ahead returned at once: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	d.e.Flush()
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	if len(d.e.video) != 0 {
		t.Fatal("stale frame queued after the flush")
	}
	d.pushAudio(0, time.Second)
	d.step(10 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := d.e.PushVideo(ctx, videoFrame(5*time.Second)); err != context.DeadlineExceeded {
		t.Fatalf("blocked push returned %v, want the context error", err)
	}
}

func TestAudioBackpressure(t *testing.T) {
	d := newFakeDevice(t, 50*time.Millisecond)
	d.pushAudio(0, 50*time.Millisecond+readAhead)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := d.e.PushAudio(ctx, audioFrame(2*time.Second, OutputRate, 2, 960, 1)); err != context.DeadlineExceeded {
		t.Fatalf("over-full audio push returned %v, want to block", err)
	}
	d.step(100 * time.Millisecond)
	if err := d.e.PushAudio(context.Background(), audioFrame(2*time.Second, OutputRate, 2, 960, 1)); err != nil {
		t.Fatal(err)
	}
}

func TestAudioConversion(t *testing.T) {
	// Mono 24 kHz is duplicated to stereo and played at its own rate: 10ms of
	// device time consumes 10ms of media.
	d := newFakeDevice(t, 10*time.Millisecond)
	if err := d.e.PushAudio(context.Background(), audioFrame(0, 24000, 1, 2400, 1234)); err != nil {
		t.Fatal(err)
	}
	d.step(10 * time.Millisecond)
	l := int16(binary.LittleEndian.Uint16(d.last[0:]))
	r := int16(binary.LittleEndian.Uint16(d.last[2:]))
	if l != 1234 || r != 1234 {
		t.Fatalf("mono sample rendered as %d/%d, want 1234 on both", l, r)
	}
	if m, _ := d.master(); !near(m, 10*time.Millisecond, time.Millisecond) {
		t.Fatalf("master = %v, want 10ms", m)
	}
	f := audioFrame(0, OutputRate, 2, 10, 0)
	f.Audio.SampleFormat = "s32"
	if err := d.e.PushAudio(context.Background(), f); err == nil {
		t.Fatal("unsupported sample format accepted")
	}
}

func TestDeviceClockSmoothsStepsAndTracksDrift(t *testing.T) {
	var mono time.Duration
	var frames int64
	c := NewDeviceClock(OutputRate, func() int64 { return frames }, func() time.Duration { return mono })
	c.Now()
	last := time.Duration(0)
	// The device reports in 10ms steps and runs 1% fast; read every 1ms.
	for i := 1; i <= 4000; i++ {
		mono += time.Millisecond
		if i%10 == 0 {
			frames = durationToFrames(time.Duration(float64(mono) * 1.01))
		}
		now := c.Now()
		if now < last {
			t.Fatalf("clock went backwards at %v: %v < %v", mono, now, last)
		}
		last = now
	}
	device := time.Duration(float64(mono) * 1.01)
	if !near(last, device, 12*time.Millisecond) {
		t.Fatalf("after 4s the clock reads %v, device %v", last, device)
	}
	// A momentary overshoot (a read counted before it is buffered) barely
	// moves the clock.
	before := c.Now()
	frames += 2048
	mono += time.Millisecond
	jump := c.Now() - before
	frames -= 2048
	if jump > 2*time.Millisecond {
		t.Fatalf("a transient 42ms overshoot moved the clock %v in 1ms", jump)
	}
}

func TestDeviceClockSleep(t *testing.T) {
	e := New(0, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go RunNullDevice(ctx, e)
	start := time.Now()
	if err := e.Clock().Sleep(ctx, 30*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 25*time.Millisecond {
		t.Fatalf("slept %v of a 30ms device wait", elapsed)
	}
	cancel()
	if err := e.Clock().Sleep(ctx, time.Hour); err != context.Canceled {
		t.Fatalf("sleep on a done context returned %v", err)
	}
}

// A seek drops the old position's in-flight media until its discontinuity
// lands; a refused seek stops dropping at once.
func TestBeginSeekDropsStaleMediaUntilTheDiscontinuity(t *testing.T) {
	d := newFakeDevice(t, 50*time.Millisecond)
	sink := Sink{Engine: d.e}
	d.pushAudio(0, 200*time.Millisecond)
	d.e.BeginSeek()
	d.pushAudio(200*time.Millisecond, 400*time.Millisecond) // stale, in flight
	d.pushVideo(200*time.Millisecond, 300*time.Millisecond, 33*time.Millisecond)
	if d.e.queuedLocked() != 0 || len(d.e.video) != 0 {
		t.Fatal("stale media queued during a seek")
	}
	disc := &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventDiscontinuity}}
	if err := sink.Handle(context.Background(), disc); err != nil {
		t.Fatal(err)
	}
	d.pushAudio(5*time.Second, 5100*time.Millisecond)
	if d.e.queuedLocked() == 0 {
		t.Fatal("media after the discontinuity dropped")
	}
	d.e.BeginSeek()
	d.e.EndSeek()
	d.pushAudio(6*time.Second, 6100*time.Millisecond)
	if d.e.queuedLocked() == 0 {
		t.Fatal("media dropped after a refused seek")
	}
}

// Through a device queue, frames are still shown when their audio is heard,
// not when it is handed to the device.
func TestVideoWaitsForAudioThroughTheDeviceQueue(t *testing.T) {
	d := newFakeDevice(t, 100*time.Millisecond)
	d.buffered = 1920
	d.setQueue(52 * time.Millisecond)
	d.pushAudio(0, time.Second)
	d.pushVideo(0, time.Second, 40*time.Millisecond)
	for range 80 {
		d.step(10 * time.Millisecond)
		if f := d.e.NextVideo(d.e.Clock().Now()); f != nil {
			pts, _ := f.PTS.ToDuration()
			f.Release()
			// The speaker has played d.now of audio (the first pull started
			// playback at once), so the frame due is the one at d.now.
			if late := d.now - pts; late < -earlyTolerance || late > 10*time.Millisecond {
				t.Fatalf("frame %v shown with the speaker at %v", pts, d.now)
			}
		}
	}
}

func discontinuity(t *testing.T, e *Engine) {
	t.Helper()
	msg := &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventDiscontinuity}}
	if err := (Sink{Engine: e}).Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
}

// Both branches of a seek deliver a discontinuity; the seek flushes once, so
// the second leaves the first branch's fresh media alone.
func TestOneSeekFlushesOnce(t *testing.T) {
	d := newFakeDevice(t, 50*time.Millisecond)
	d.pushAudio(0, 500*time.Millisecond)
	d.step(100 * time.Millisecond) // the epoch has been heard
	discontinuity(t, d.e)          // the video branch's: flushes
	if d.e.queuedLocked() != 0 {
		t.Fatal("first discontinuity of a seek did not flush")
	}
	d.pushVideo(3*time.Second, 3100*time.Millisecond, 40*time.Millisecond)
	discontinuity(t, d.e) // the audio branch's, for the same seek
	if len(d.e.video) != 3 {
		t.Fatalf("second discontinuity of one seek flushed fresh video: %d queued", len(d.e.video))
	}
	// A later seek, once the new position has been heard, flushes again.
	d.pushAudio(3*time.Second, 3500*time.Millisecond)
	d.step(100 * time.Millisecond)
	discontinuity(t, d.e)
	if len(d.e.video) != 0 || d.e.queuedLocked() != 0 {
		t.Fatal("a new seek did not flush")
	}
}

// A seek made while paused waits for its discontinuity however long the
// pause lasts, then previews its first picture; the wait for a missing
// discontinuity is charged only while playing.
func TestPausedSeekWaitsAndPreviews(t *testing.T) {
	d := newFakeDevice(t, 50*time.Millisecond)
	d.pushAudio(0, 500*time.Millisecond)
	d.step(100 * time.Millisecond)
	d.e.SetPaused(true)
	d.e.BeginSeek()
	d.step(3 * time.Second)
	d.pushVideo(200*time.Millisecond, 300*time.Millisecond, 40*time.Millisecond) // stale
	if len(d.e.video) != 0 {
		t.Fatal("stale media accepted after a long paused seek")
	}
	discontinuity(t, d.e)
	d.pushVideo(5*time.Second, 5100*time.Millisecond, 40*time.Millisecond)
	if !d.e.WaitPicture(context.Background(), time.Second) {
		t.Fatal("no picture queued after the paused seek")
	}
	f := d.e.NextVideo(d.e.Clock().Now())
	if f == nil {
		t.Fatal("paused seek previewed nothing")
	}
	if pts, _ := f.PTS.ToDuration(); pts != 5*time.Second {
		t.Fatalf("previewed %v, want the target 5s", pts)
	}
	f.Release()

	d.e.SetPaused(false)
	d.e.BeginSeek()
	d.step(seekWait + 10*time.Millisecond)
	d.pushVideo(7*time.Second, 7040*time.Millisecond, 40*time.Millisecond)
	if len(d.e.video) == 0 {
		t.Fatal("a seek with no discontinuity kept dropping past its wait")
	}
}

// The source's end of stream ends playback at the sink: the queue drains
// without counting its final dry pull as an underrun.
func TestEndOfStreamIsNotAnUnderrun(t *testing.T) {
	d := newFakeDevice(t, 50*time.Millisecond)
	d.pushAudio(0, 200*time.Millisecond)
	eos := &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventEndOfStream}}
	if err := (Sink{Engine: d.e}).Handle(context.Background(), eos); err != nil {
		t.Fatal(err)
	}
	d.step(400 * time.Millisecond)
	if got := d.e.Stats().Underruns; got != 0 {
		t.Fatalf("underruns = %d after the end of stream, want 0", got)
	}
	if !d.e.Drained() {
		t.Fatal("not drained after the end of stream played out")
	}
}
