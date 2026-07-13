package flow

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/thesyncim/goav/av"
)

// PlayoutPolicy names one deliver-when-due schedule at the sink boundary: a
// playout gate holds each packet or frame until its media time is due on the
// task timeline, so real-time destinations render on time. Reuse the returned
// value across audio/video chains or branches when those branches should
// align on one shared anchor.
type PlayoutPolicy struct {
	name      string
	offset    time.Duration
	tolerance time.Duration
	mode      playoutMode
	scheduler *playoutScheduler
}

type playoutMode uint8

const (
	playoutHoldLate playoutMode = iota
	playoutDropLate
)

// Playout creates a deliver-when-due policy. The first admitted message
// anchors its PTS to the current task-timeline reading; every later message
// with PTS p is due at that anchor plus (p - first PTS) plus the offset. The
// default holds messages until due and delivers late ones immediately; use
// WithDropLate for preview-style branches that should shed overdue media.
func Playout(name string) PlayoutPolicy {
	return PlayoutPolicy{
		name:      firstNonEmpty(strings.TrimSpace(name), "playout"),
		mode:      playoutHoldLate,
		scheduler: newPlayoutScheduler(),
	}
}

// WithOffset returns a copy of the policy whose messages are due offset later
// than their anchored media time — the latency budget downstream rendering
// gets. Non-positive offsets leave the policy unchanged.
func (p PlayoutPolicy) WithOffset(offset time.Duration) PlayoutPolicy {
	if offset > 0 {
		p.offset = offset
	}
	return p
}

// WithDropLate returns a copy of the policy that sheds messages already
// overdue by more than tolerance instead of delivering them late. Early
// messages still wait until due.
func (p PlayoutPolicy) WithDropLate(tolerance time.Duration) PlayoutPolicy {
	p.mode = playoutDropLate
	if tolerance > 0 {
		p.tolerance = tolerance
	}
	return p
}

// Normalize fills the default name and scheduler used by a zero PlayoutPolicy.
func (p PlayoutPolicy) Normalize() PlayoutPolicy {
	if strings.TrimSpace(p.name) == "" {
		p.name = "playout"
	}
	if p.scheduler == nil {
		p.scheduler = newPlayoutScheduler()
	}
	return p
}

// Name returns the stable name used for diagnostics and stage naming.
func (p PlayoutPolicy) Name() string {
	return p.name
}

// Offset returns the latency offset added to every due time.
func (p PlayoutPolicy) Offset() time.Duration {
	return p.offset
}

// Tolerance returns the overdue budget before a drop-late gate sheds.
func (p PlayoutPolicy) Tolerance() time.Duration {
	return p.tolerance
}

// DropLate reports whether overdue messages are shed instead of delivered late.
func (p PlayoutPolicy) DropLate() bool {
	return p.mode == playoutDropLate
}

// Admit blocks on clock until the message with media time pts is due, and
// reports whether it should be dropped instead. Branches sharing this policy
// value share the anchor, so their deliveries interleave by PTS on the one
// clock. The returned lateness is how far past due the message already was
// when it reached the gate — positive for messages delivered late (hold-late)
// or shed (drop-late), non-positive for on-time media — the measurement QoS
// reports carry.
func (p PlayoutPolicy) Admit(ctx context.Context, clock av.Clock, pts time.Duration) (time.Duration, bool, error) {
	if p.scheduler == nil || clock == nil {
		return 0, false, nil
	}
	return p.scheduler.admit(ctx, clock, pts, p.offset, p.tolerance, p.mode)
}

// ObserveEvent resets the shared anchor after a discontinuity so post-seek
// media re-anchors on its first message instead of paying the seek gap as a
// playout delay.
func (p PlayoutPolicy) ObserveEvent(event *av.Event) {
	if p.scheduler != nil {
		p.scheduler.observeEvent(event)
	}
}

// playoutScheduler is the shared anchor behind one policy value: the first
// admitted message pins pts0 to the timeline reading now0. The mutex guards
// only the anchor fields for the same brief per-message window sync gates use;
// all waiting happens outside it on the clock.
type playoutScheduler struct {
	mu       sync.Mutex
	anchored bool
	pts0     time.Duration
	now0     time.Duration
}

func newPlayoutScheduler() *playoutScheduler {
	return &playoutScheduler{}
}

func (s *playoutScheduler) admit(ctx context.Context, clock av.Clock, pts time.Duration, offset time.Duration, tolerance time.Duration, mode playoutMode) (time.Duration, bool, error) {
	s.mu.Lock()
	if !s.anchored {
		s.anchored = true
		s.pts0 = pts
		s.now0 = clock.Now()
	}
	due := s.now0 + (pts - s.pts0) + offset
	s.mu.Unlock()
	now := clock.Now()
	late := now - due
	if mode == playoutDropLate && late > tolerance {
		return late, true, nil
	}
	// A rate or pause change mid-wait re-derives the remaining wall wait
	// inside the timeline's Sleep; the loop only guards clocks that may wake
	// before the requested duration elapsed.
	for now < due {
		if err := clock.Sleep(ctx, due-now); err != nil {
			return late, false, err
		}
		now = clock.Now()
	}
	return late, false, nil
}

// observeEvent clears the anchor on discontinuities. The anchor is
// timeline-global — one reset re-anchors every stream sharing the policy, the
// coherent behavior after a seek; codec changes and stream removals do not
// move media time, so they keep the anchor.
func (s *playoutScheduler) observeEvent(event *av.Event) {
	if s == nil || event == nil {
		return
	}
	if event.Type != av.EventDiscontinuity {
		return
	}
	s.mu.Lock()
	s.anchored = false
	s.pts0 = 0
	s.now0 = 0
	s.mu.Unlock()
}
