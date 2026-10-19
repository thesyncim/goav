// Package engine is the player's media core, free of any window or audio
// backend: the audio renderer the device pulls from, the device clock, the
// audio-mastered video presenter, the goav sink that feeds them, and the
// Y'CbCr conversion. The window and the speaker are thin shells around it, so
// everything here runs headless under test.
package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/goav/av"
)

// The device format: every source is rendered to 48 kHz stereo s16.
const (
	OutputRate     = 48000
	OutputChannels = 2
	// BytesPerFrame is one stereo s16 output frame.
	BytesPerFrame = 4
)

const (
	// DefaultLatency is the audio lead held before playback starts.
	DefaultLatency = 200 * time.Millisecond
	// earlyTolerance presents a frame due within half a 120 Hz refresh.
	earlyTolerance = 4 * time.Millisecond
	// videoHardCap bounds queued video frames whatever the timestamps say.
	videoHardCap = 48
	// readAhead is how far past the latency the sink accepts media before it
	// backpressures the graph.
	readAhead = time.Second
	segments  = 64
)

// Stats is a point-in-time view of playback.
type Stats struct {
	// Position is the master clock: the media time being heard (or, without
	// audio, the time on the free-running external clock).
	Position time.Duration
	// Presented and Dropped count video frames shown and discarded as late.
	Presented, Dropped uint64
	// Underruns counts times the audio queue ran dry mid-playback.
	Underruns uint64
	// AVOffset is master minus the last presented frame's PTS (positive:
	// the frame was late); MaxAVOffset is the largest magnitude seen.
	AVOffset, MaxAVOffset time.Duration
	Rate                  float64
	Paused                bool
}

// Engine holds the queues and clocks of one playback.
type Engine struct {
	latency time.Duration
	clock   *DeviceClock
	written atomic.Int64
	device  atomic.Pointer[func() int]

	mu    sync.Mutex
	wake  chan struct{}
	epoch uint64

	// Audio renderer state.
	chunks    []audioChunk
	pos       float64
	queued    time.Duration
	lastPTS   time.Duration
	playing   bool
	waitSet   bool
	waitStart int64
	segs      [segments]segment
	segHead   int
	segCount  int
	audibleTo int64

	// Video queue: frames held by reference, PTS kept alongside.
	video          []*av.Frame
	vpts           []time.Duration
	firstVideoAt   time.Duration
	firstVideoSet  bool
	presentedFresh bool

	ext      extClock
	rate     float64
	paused   bool
	ended    bool
	closed   bool
	sawAudio bool

	presented, dropped, underruns uint64
	avOffset, maxAV               time.Duration
	position                      time.Duration
}

type audioChunk struct {
	pts     time.Duration
	rate    int
	samples []int16 // stereo interleaved
}

func (c *audioChunk) frames() int { return len(c.samples) / 2 }

func (c *audioChunk) duration() time.Duration {
	return framesToDuration(int64(c.frames()), int64(c.rate))
}

// segment maps a run of device frames to the media time rendered into it.
type segment struct {
	start, end int64
	pts0, pts1 time.Duration
	audible    bool
	epoch      uint64
}

// extClock is the media clock video follows when no audio is audible: it is
// resynchronized to the heard position whenever audio plays and runs freely
// (at the playback rate, on the device clock) otherwise.
type extClock struct {
	valid   bool
	running bool
	base    time.Duration
	at      time.Duration
}

func (c *extClock) now(t time.Duration, rate float64) time.Duration {
	if !c.running {
		return c.base
	}
	return c.base + time.Duration(float64(t-c.at)*rate)
}

// New returns an engine holding latency of audio before playback starts;
// mono is the interpolation time source of its device clock (nil: the
// process monotonic clock).
func New(latency time.Duration, mono func() time.Duration) *Engine {
	if latency <= 0 {
		latency = DefaultLatency
	}
	e := &Engine{latency: latency, rate: 1, wake: make(chan struct{})}
	e.clock = NewDeviceClock(OutputRate, e.playedFrames, mono)
	return e
}

// Clock is the device clock: hand it to the goav runtime so the task timeline
// runs in the speaker's time.
func (e *Engine) Clock() *DeviceClock { return e.clock }

// SetDevice installs the device's report of output frames it has accepted
// but not played yet. Without one, every frame handed out counts as played.
func (e *Engine) SetDevice(buffered func() int) {
	if buffered != nil {
		e.device.Store(&buffered)
	}
}

func (e *Engine) playedFrames() int64 {
	played := e.written.Load()
	if buffered := e.device.Load(); buffered != nil {
		played -= int64((*buffered)())
	}
	return max(played, 0)
}

// signalLocked wakes every producer waiting for queue space.
func (e *Engine) signalLocked() {
	close(e.wake)
	e.wake = make(chan struct{})
}

// Fill renders the next len(out)/BytesPerFrame device frames into out as
// s16le stereo. It never blocks and always fills the whole buffer: silence
// while prerolling, paused, or starved.
func (e *Engine) Fill(out []byte) int {
	n := len(out) / BytesPerFrame
	if n == 0 {
		return 0
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	start := e.written.Load()
	if !e.paused && !e.playing && len(e.chunks) > 0 {
		waited := framesToDuration(start-e.waitStart, OutputRate)
		if e.queuedLocked() >= e.latency || e.ended || waited >= 2*e.latency {
			e.playing = true
		}
	}
	rendered := 0
	if e.playing && !e.paused {
		pts0 := e.headPTSLocked()
		rendered = e.renderLocked(out, n)
		if rendered > 0 {
			e.recordLocked(segment{start: start, end: start + int64(rendered), pts0: pts0, pts1: e.headPTSLocked(), audible: true, epoch: e.epoch})
			e.audibleTo = start + int64(rendered)
		}
		if rendered < n {
			e.playing = false
			e.waitSet = false
			if !e.ended {
				e.underruns++
			}
		}
	}
	if rendered < n {
		clear(out[rendered*BytesPerFrame : n*BytesPerFrame])
		p := e.headPTSLocked()
		e.recordLocked(segment{start: start + int64(rendered), end: start + int64(n), pts0: p, pts1: p, epoch: e.epoch})
	}
	e.written.Store(start + int64(n))
	if rendered > 0 {
		e.signalLocked()
	}
	return n * BytesPerFrame
}

// renderLocked resamples queued audio into out at the playback rate (linear
// interpolation: varispeed, so the pitch follows the rate).
func (e *Engine) renderLocked(out []byte, n int) int {
	i := 0
	for i < n && len(e.chunks) > 0 {
		c := &e.chunks[0]
		frames := c.frames()
		idx := int(e.pos)
		if idx >= frames {
			e.popChunkLocked()
			continue
		}
		frac := float32(e.pos - float64(idx))
		l0, r0 := c.samples[2*idx], c.samples[2*idx+1]
		l1, r1 := l0, r0
		if idx+1 < frames {
			l1, r1 = c.samples[2*idx+2], c.samples[2*idx+3]
		} else if len(e.chunks) > 1 && len(e.chunks[1].samples) >= 2 {
			l1, r1 = e.chunks[1].samples[0], e.chunks[1].samples[1]
		}
		binary.LittleEndian.PutUint16(out[i*BytesPerFrame:], uint16(lerp(l0, l1, frac)))
		binary.LittleEndian.PutUint16(out[i*BytesPerFrame+2:], uint16(lerp(r0, r1, frac)))
		e.pos += e.rate * float64(c.rate) / OutputRate
		i++
	}
	for len(e.chunks) > 0 && int(e.pos) >= e.chunks[0].frames() {
		e.popChunkLocked()
	}
	return i
}

func lerp(a, b int16, t float32) int16 {
	return int16(math.Round(float64(float32(a) + (float32(b)-float32(a))*t)))
}

func (e *Engine) popChunkLocked() {
	c := e.chunks[0]
	e.pos -= float64(c.frames())
	e.queued -= c.duration()
	e.lastPTS = c.pts + c.duration()
	e.chunks[0] = audioChunk{}
	e.chunks = e.chunks[1:]
	if len(e.chunks) == 0 {
		e.chunks = nil
		e.pos = 0
		e.queued = 0
	}
}

// headPTSLocked is the media time of the next sample to render.
func (e *Engine) headPTSLocked() time.Duration {
	if len(e.chunks) == 0 {
		return e.lastPTS
	}
	c := e.chunks[0]
	return c.pts + time.Duration(e.pos/float64(c.rate)*float64(time.Second))
}

func (e *Engine) queuedLocked() time.Duration {
	if len(e.chunks) == 0 {
		return 0
	}
	return e.queued - time.Duration(e.pos/float64(e.chunks[0].rate)*float64(time.Second))
}

func (e *Engine) recordLocked(s segment) {
	e.segs[e.segHead] = s
	e.segHead = (e.segHead + 1) % segments
	e.segCount = min(e.segCount+1, segments)
}

// heardLocked is the media time audible at device frame dev, when the device
// is playing audio of the current epoch there.
func (e *Engine) heardLocked(dev int64) (time.Duration, bool) {
	for k := 1; k <= e.segCount; k++ {
		s := &e.segs[(e.segHead-k+segments)%segments]
		if dev < s.start {
			continue
		}
		if dev > s.end || !s.audible || s.epoch != e.epoch {
			return 0, false
		}
		return s.pts0 + time.Duration(float64(s.pts1-s.pts0)*float64(dev-s.start)/float64(s.end-s.start)), true
	}
	return 0, false
}

// masterLocked is the clock video is presented against: the heard audio
// position when audio is audible, else the external clock. now is a device
// clock reading.
func (e *Engine) masterLocked(now time.Duration) (time.Duration, bool) {
	if pts, ok := e.heardLocked(durationToFrames(now)); ok {
		e.ext = extClock{valid: true, running: !e.paused, base: pts, at: now}
		e.position = pts
		return pts, true
	}
	if !e.ext.valid && len(e.vpts) > 0 && !e.sawAudio && now-e.firstVideoAt >= e.latency {
		// Video only: start the external clock at the first frame once the
		// latency has passed without any audio arriving.
		e.ext = extClock{valid: true, running: !e.paused, base: e.vpts[0], at: now}
	}
	if !e.ext.valid {
		return 0, false
	}
	e.position = e.ext.now(now, e.rate)
	return e.position, true
}

// Decide is the presenter's rule for the head of a video queue holding frames
// at pts (in order) when the master clock reads m: drop every frame whose
// successor is already due (it is late and a newer picture exists), then
// present the head when it is due within early, else wait for it.
func Decide(pts []time.Duration, m, early time.Duration) (drop int, present bool) {
	for len(pts)-drop >= 2 && pts[drop+1] <= m {
		drop++
	}
	if drop == len(pts) {
		return drop, false
	}
	return drop, pts[drop] <= m+early
}

// NextVideo applies Decide at device time now and returns the frame to
// present, or nil to keep showing the current one. The caller owns the
// returned frame's references and releases it after converting.
func (e *Engine) NextVideo(now time.Duration) *av.Frame {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.video) == 0 {
		return nil
	}
	m, ok := e.masterLocked(now)
	drop, present := 0, false
	if ok {
		drop, present = Decide(e.vpts, m, earlyTolerance)
	}
	for range drop {
		e.popVideoLocked().Release()
		e.dropped++
	}
	// A seek while paused still shows the new position's first picture.
	if !present && e.paused && !e.presentedFresh && len(e.video) > 0 {
		present, m, ok = true, e.vpts[0], true
	}
	if !present {
		if drop > 0 {
			e.signalLocked()
		}
		return nil
	}
	pts := e.vpts[0]
	f := e.popVideoLocked()
	e.presented++
	e.presentedFresh = true
	if ok {
		e.avOffset = m - pts
		e.maxAV = max(e.maxAV, abs(e.avOffset))
	}
	e.signalLocked()
	return f
}

func abs(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

func (e *Engine) popVideoLocked() *av.Frame {
	f := e.video[0]
	e.video[0] = nil
	e.video = e.video[1:]
	e.vpts = e.vpts[1:]
	if len(e.video) == 0 {
		e.video, e.vpts = e.video[:0:0], e.vpts[:0:0]
	}
	return f
}

// PushAudio queues an audio frame, converted to stereo s16 at its own rate,
// blocking while the queue holds more than latency+readAhead of media.
func (e *Engine) PushAudio(ctx context.Context, f *av.Frame) error {
	chunk, err := audioChunkOf(f)
	if err != nil || len(chunk.samples) == 0 {
		return err
	}
	e.mu.Lock()
	epoch := e.epoch
	e.sawAudio = true
	e.mu.Unlock()
	for {
		e.mu.Lock()
		if e.closed || e.epoch != epoch {
			e.mu.Unlock()
			return nil
		}
		if e.queuedLocked() < e.latency+readAhead {
			if !e.playing && !e.waitSet {
				e.waitSet, e.waitStart = true, e.written.Load()
			}
			e.chunks = append(e.chunks, chunk)
			e.queued += chunk.duration()
			e.mu.Unlock()
			return nil
		}
		wake := e.wake
		e.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// PushVideo queues a reference to a video frame. It blocks while the queue
// runs more than latency+readAhead ahead of the master clock — except while
// audio still needs data, when blocking would starve it through the shared
// serial sink: then the queue grows to its hard cap and sheds its oldest frame.
func (e *Engine) PushVideo(ctx context.Context, f *av.Frame) error {
	pts, ok := f.PTS.ToDuration()
	if !ok {
		return nil
	}
	if f.Video == nil {
		return fmt.Errorf("player: video frame without a video header")
	}
	if _, _, _, err := subsampling(f.Video.PixelFormat); err != nil {
		return err
	}
	ref := f.Ref()
	e.mu.Lock()
	epoch := e.epoch
	e.mu.Unlock()
	for {
		now := e.clock.Now()
		e.mu.Lock()
		if e.closed || e.epoch != epoch {
			e.mu.Unlock()
			ref.Release()
			return nil
		}
		if !e.firstVideoSet {
			e.firstVideoSet, e.firstVideoAt = true, now
		}
		ahead := time.Duration(0)
		if m, ok := e.masterLocked(now); ok {
			ahead = pts - m
		} else if len(e.vpts) > 0 {
			ahead = pts - e.vpts[0]
		}
		audioNeedsData := e.sawAudio && !e.ended && e.queuedLocked() < e.latency
		if len(e.video) < videoHardCap && (ahead < e.latency+readAhead || audioNeedsData) {
			e.video, e.vpts = append(e.video, ref), append(e.vpts, pts)
			e.mu.Unlock()
			return nil
		}
		if audioNeedsData {
			e.popVideoLocked().Release()
			e.dropped++
			e.video, e.vpts = append(e.video, ref), append(e.vpts, pts)
			e.mu.Unlock()
			return nil
		}
		wake := e.wake
		e.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			ref.Release()
			return ctx.Err()
		}
	}
}

// Flush discards everything queued and forgets the clocks' anchors: the next
// media re-prerolls and re-anchors. Producers waiting for space give up
// their (stale) frame.
func (e *Engine) Flush() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.epoch++
	for _, f := range e.video {
		f.Release()
	}
	e.video, e.vpts = nil, nil
	e.chunks, e.pos, e.queued = nil, 0, 0
	e.playing, e.waitSet = false, false
	e.ext = extClock{}
	e.firstVideoSet, e.presentedFresh = false, false
	e.signalLocked()
}

// SetPaused pauses or resumes playback: the renderer stops consuming audio
// (the device keeps playing silence, so the device clock keeps running) and
// the external clock freezes.
func (e *Engine) SetPaused(paused bool) {
	now := e.clock.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	if paused == e.paused {
		return
	}
	if e.ext.valid {
		e.ext = extClock{valid: true, running: !paused, base: e.ext.now(now, e.rate), at: now}
	}
	e.paused = paused
	e.signalLocked()
}

// SetRate changes the playback rate the renderer consumes audio at.
func (e *Engine) SetRate(rate float64) {
	if !(rate > 0) || math.IsInf(rate, 0) {
		return
	}
	now := e.clock.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.ext.valid {
		e.ext = extClock{valid: true, running: e.ext.running, base: e.ext.now(now, e.rate), at: now}
	}
	e.rate = rate
}

// End marks the input finished: queued media plays out without waiting for
// the preroll, and the final dry queue is not an underrun.
func (e *Engine) End() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ended = true
	e.signalLocked()
}

// Drained reports that the input ended and everything queued has been
// presented and heard.
func (e *Engine) Drained() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ended && len(e.chunks) == 0 && len(e.video) == 0 && e.playedFrames() >= e.audibleTo
}

// Close releases queued frames and unblocks producers for good.
func (e *Engine) Close() {
	e.Flush()
	e.mu.Lock()
	e.closed = true
	e.ended = true
	e.mu.Unlock()
}

// Stats reports playback counters at device time now.
func (e *Engine) Stats() Stats {
	now := e.clock.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	e.masterLocked(now)
	return Stats{
		Position:    e.position,
		Presented:   e.presented,
		Dropped:     e.dropped,
		Underruns:   e.underruns,
		AVOffset:    e.avOffset,
		MaxAVOffset: e.maxAV,
		Rate:        e.rate,
		Paused:      e.paused,
	}
}

// audioChunkOf converts an interleaved s16 or f32 frame of any channel count
// to stereo s16 (mono is duplicated; beyond two channels the front pair is
// kept).
func audioChunkOf(f *av.Frame) (audioChunk, error) {
	a := f.Audio
	if a == nil || a.Channels <= 0 || a.SampleRate <= 0 {
		return audioChunk{}, fmt.Errorf("player: audio frame without a valid audio header")
	}
	pts, ok := f.PTS.ToDuration()
	if !ok {
		return audioChunk{}, nil
	}
	if len(f.Planes) != 1 {
		return audioChunk{}, fmt.Errorf("player: planar audio (%d planes) is not supported; resample to interleaved s16 with .Resample(%d, %d)", len(f.Planes), OutputRate, OutputChannels)
	}
	p := f.Planes[0]
	data := p.Buffer.Bytes[min(p.Offset, len(p.Buffer.Bytes)):]
	var width int
	switch a.SampleFormat {
	case av.SampleFormatS16, "":
		width = 2
	case av.SampleFormatF32:
		width = 4
	default:
		return audioChunk{}, fmt.Errorf("player: unsupported sample format %q; resample to s16 with .Resample(%d, %d)", a.SampleFormat, OutputRate, OutputChannels)
	}
	frames := len(data) / (width * a.Channels)
	if a.Samples > 0 {
		frames = min(frames, a.Samples)
	}
	out := make([]int16, 2*frames)
	sample := func(i int) int16 {
		if width == 2 {
			return int16(binary.LittleEndian.Uint16(data[2*i:]))
		}
		v := math.Float32frombits(binary.LittleEndian.Uint32(data[4*i:]))
		return int16(max(-32768, min(32767, math.Round(float64(v)*32767))))
	}
	for i := range frames {
		l := sample(i * a.Channels)
		r := l
		if a.Channels > 1 {
			r = sample(i*a.Channels + 1)
		}
		out[2*i], out[2*i+1] = l, r
	}
	return audioChunk{pts: pts, rate: a.SampleRate, samples: out}, nil
}
