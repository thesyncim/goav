package engine

import (
	"encoding/binary"
	"math"
	"time"

	"github.com/thesyncim/goav/av"
)

func (e *Engine) Fill(out []byte) int {
	n := len(out) / BytesPerFrame
	if n == 0 {
		return 0
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	start := e.written.Load()
	if !e.paused && e.audio == audioPrerolling && e.chunkCount > 0 {
		waited := framesToDuration(start-e.waitStart, OutputRate)
		if e.queuedLocked() >= e.latency || e.phase != engineActive || waited >= 2*e.latency {
			e.audio = audioPlaying
		}
	}
	rendered := 0
	if e.audio == audioPlaying && !e.paused {
		pts0 := e.headPTSLocked()
		rendered = e.renderLocked(out, n)
		if rendered > 0 {
			e.recordLocked(segment{start: start, end: start + int64(rendered), pts0: pts0, pts1: e.headPTSLocked(), audible: true, epoch: e.epoch})
			e.audibleTo = start + int64(rendered)
			e.audioPlayed = true
		}
		if rendered < n {
			e.audio = audioEmpty
			if e.phase == engineActive {
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
	for i < n && e.chunkCount > 0 {
		c := e.chunkAtLocked(0)
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
		} else if e.chunkCount > 1 {
			next := e.chunkAtLocked(1)
			if len(next.samples) >= 2 {
				l1, r1 = next.samples[0], next.samples[1]
			}
		}
		binary.LittleEndian.PutUint16(out[i*BytesPerFrame:], uint16(lerp(l0, l1, frac)))
		binary.LittleEndian.PutUint16(out[i*BytesPerFrame+2:], uint16(lerp(r0, r1, frac)))
		e.pos += e.rate * float64(c.rate) / OutputRate
		i++
	}
	for e.chunkCount > 0 && int(e.pos) >= e.chunkAtLocked(0).frames() {
		e.popChunkLocked()
	}
	return i
}

func lerp(a, b int16, t float32) int16 {
	return int16(math.Round(float64(float32(a) + (float32(b)-float32(a))*t)))
}

func (e *Engine) popChunkLocked() {
	c := e.chunks[e.chunkHead]
	e.pos -= float64(c.frames())
	e.queued -= c.duration()
	e.lastPTS = c.pts + c.duration()
	e.putSamplesLocked(c.samples)
	e.chunks[e.chunkHead] = audioChunk{}
	e.chunkHead = (e.chunkHead + 1) % len(e.chunks)
	e.chunkCount--
	if e.chunkCount == 0 {
		e.chunkHead = 0
		e.pos = 0
		e.queued = 0
	}
}

// headPTSLocked is the media time of the next sample to render.
func (e *Engine) headPTSLocked() time.Duration {
	if e.chunkCount == 0 {
		return e.lastPTS
	}
	c := e.chunkAtLocked(0)
	return c.pts + time.Duration(e.pos/float64(c.rate)*float64(time.Second))
}

func (e *Engine) queuedLocked() time.Duration {
	if e.chunkCount == 0 {
		return 0
	}
	return e.queued - time.Duration(e.pos/float64(e.chunkAtLocked(0).rate)*float64(time.Second))
}

// recordLocked appends a device segment, extending the last one instead when
// s continues it (same epoch and audibility, contiguous media at the same
// rate), so the ring spans the whole device path however small the pulls.
func (e *Engine) recordLocked(s segment) {
	if e.segCount > 0 {
		last := &e.segs[(e.segHead-1+segments)%segments]
		if last.end == s.start && last.audible == s.audible && last.epoch == s.epoch && continues(last, &s) {
			last.end, last.pts1 = s.end, s.pts1
			return
		}
	}
	e.segs[e.segHead] = s
	e.segHead = (e.segHead + 1) % segments
	e.segCount = min(e.segCount+1, segments)
}

// continues reports whether s carries on a's media timeline: it starts within
// a millisecond of where a ends and advances media at the same rate.
func continues(a, s *segment) bool {
	if d := s.pts0 - a.pts1; d < -time.Millisecond || d > time.Millisecond {
		return false
	}
	slopeA := float64(a.pts1-a.pts0) / float64(a.end-a.start)
	slopeS := float64(s.pts1-s.pts0) / float64(s.end-s.start)
	return math.Abs(slopeA-slopeS) <= 1e-3*math.Max(math.Abs(slopeA), 1)
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
		e.ext.set(pts, now, e.paused)
		e.position = pts
		return pts, true
	}
	if e.ext.state == extClockUnset && e.videoCount > 0 && !e.sawAudio && now-e.firstVideoAt >= e.latency {
		// Video only: start the external clock at the first frame once the
		// latency has passed without any audio arriving.
		e.ext.set(e.vpts[e.videoHead], now, e.paused)
	}
	if e.ext.state == extClockUnset {
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
	if e.videoCount == 0 {
		return nil
	}
	m, ok := e.masterLocked(now)
	drop, present := 0, false
	if ok {
		var ptsBuf [videoHardCap]time.Duration
		n := copy(ptsBuf[:], e.vpts[e.videoHead:min(e.videoHead+e.videoCount, videoHardCap)])
		if n < e.videoCount {
			copy(ptsBuf[n:], e.vpts[:e.videoCount-n])
		}
		drop, present = Decide(ptsBuf[:e.videoCount], m, earlyTolerance)
	}
	for range drop {
		e.popVideoLocked().Release()
		e.dropped++
	}
	// A seek while paused still shows the new position's first picture: a
	// preview, which leaves the epoch unplayed.
	preview := false
	if !present && e.paused && e.videoEpoch < videoEpochPreviewed && e.videoCount > 0 {
		present, m, ok, preview = true, e.vpts[e.videoHead], true, true
	}
	if !present {
		if drop > 0 {
			e.signalLocked()
		}
		return nil
	}
	pts := e.vpts[e.videoHead]
	f := e.popVideoLocked()
	e.presented++
	if preview {
		if e.videoEpoch < videoEpochPreviewed {
			e.videoEpoch = videoEpochPreviewed
		}
	} else {
		e.videoEpoch = videoEpochPresented
	}
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
	f := e.video[e.videoHead]
	e.video[e.videoHead] = nil
	e.videoHead = (e.videoHead + 1) % videoHardCap
	e.videoCount--
	if e.videoCount == 0 {
		e.videoHead = 0
	}
	return f
}
