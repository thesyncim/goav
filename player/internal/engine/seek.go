package engine

import (
	"context"
	"time"

	"github.com/thesyncim/goav/control"
)

// seekMode is the state of the post-seek stale-media cut.
type seekMode uint8

const (
	seekIdle seekMode = iota
	seekCutting
)

// A seek has to drop the old position's media still in flight, without
// dropping the new position's. Each branch delivers its own discontinuity, and
// the sink cannot tell the branches' apart (the events carry no stream), so
// the cut is made per media type on the timeline instead: in-flight media of
// a type continues where that type left off — each within staleBehind..
// staleAhead of the newest old PTS seen — while the new position starts
// elsewhere (a seek lands more than staleAhead away). The first media of a type off that span is the new position, and
// everything of that type after it is kept.
const (
	staleBehind = 50 * time.Millisecond
	staleAhead  = 250 * time.Millisecond
	// seekWait bounds the cut, counted in unpaused device time, for a seek
	// that lands inside the old span.
	seekWait = time.Second
)

// Media kinds the seek cut tracks.
const (
	kindAudio = iota
	kindVideo
	kinds
)

// seekCutState tracks the per-kind stale-media cut after a seek: cutting marks
// each kind still on the old timeline and cutAt the newest PTS it had queued;
// budget is the unpaused device time left, as of at.
type seekCutState struct {
	mode    seekMode
	budget  time.Duration
	at      time.Duration
	cutAt   [kinds]time.Duration
	cutting [kinds]bool
}

func (s *seekCutState) begin(newest [kinds]time.Duration, have [kinds]bool, now time.Duration) {
	if have == [kinds]bool{} {
		*s = seekCutState{}
		return
	}
	*s = seekCutState{
		mode:    seekCutting,
		budget:  seekWait,
		at:      now,
		cutAt:   newest,
		cutting: have,
	}
}

func (s *seekCutState) reset() {
	*s = seekCutState{}
}

func (s *seekCutState) pause(now time.Duration) {
	if s.mode != seekCutting {
		return
	}
	s.budget -= max(now-s.at, 0)
	s.at = now
	if s.budget <= 0 {
		s.reset()
	}
}

func (s *seekCutState) resume(now time.Duration) {
	if s.mode == seekCutting {
		s.at = now
	}
}

func (s *seekCutState) stale(kind int, pts, now time.Duration, paused bool) bool {
	if s.mode != seekCutting {
		return false
	}
	if !paused {
		s.budget -= max(now-s.at, 0)
	}
	s.at = now
	if s.budget <= 0 {
		s.reset()
		return false
	}
	if !s.cutting[kind] {
		return false
	}
	if pts >= s.cutAt[kind]-staleBehind && pts <= s.cutAt[kind]+staleAhead {
		// Still the old timeline: follow it, so a run of in-flight media
		// stays recognized however far it reaches.
		s.cutAt[kind] = max(s.cutAt[kind], pts)
		return true
	}
	s.cutting[kind] = false
	if s.cutting == [kinds]bool{} {
		s.reset()
	}
	return false
}

// BeginSeek flushes for a seek the caller is about to issue and cuts the
// old position's in-flight media, so no stale picture or sound slips in
// after the jump. Call EndSeek if the seek is refused.
func (e *Engine) BeginSeek() {
	now := e.clock.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	e.beginSeekLocked(now)
}

func (e *Engine) beginSeekLocked(now time.Duration) {
	newest, have := e.newest, e.have
	e.flushLocked()
	e.seek.begin(newest, have, now)
}

// EndSeek stops cutting after a refused seek.
func (e *Engine) EndSeek() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.seek.reset()
}

// Discontinuity handles a timeline break arriving at the sink. Every branch
// of a seek delivers one, and one seek flushes once: a break while a seek is
// cutting belongs to it, and so does one before anything of the current
// epoch was heard or shown. Any other break is a seek made elsewhere (a
// control socket): it flushes and cuts like BeginSeek.
func (e *Engine) Discontinuity() {
	now := e.clock.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.seek.mode == seekIdle && e.played {
		e.beginSeekLocked(now)
	}
}

// Seeker is the part of a goav task a seek drives.
type Seeker interface {
	Control(context.Context, control.Control) error
	Pause(context.Context) error
	Resume(context.Context) error
}

// Seek moves task to target: BeginSeek releases a sink blocked on stale media
// (so the seek lands promptly) and drops the old position's in-flight media
// until the seek's discontinuity. While paused, the task's timeline would
// hold the new position's media at the source, so the task runs until the
// first picture is queued — the engine stays paused and previews it — and
// then pauses again.
func (e *Engine) Seek(ctx context.Context, task Seeker, target time.Duration, paused bool) error {
	e.BeginSeek()
	if err := task.Control(ctx, control.Seek(max(target, 0))); err != nil {
		e.EndSeek()
		return err
	}
	if paused && task.Resume(ctx) == nil {
		e.WaitPicture(ctx, 2*time.Second)
		return task.Pause(ctx)
	}
	return nil
}
