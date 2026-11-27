package flow

import (
	"cmp"
	"context"
	"strings"
	"sync"
	"time"

	"github.com/thesyncim/goav/av"
)

// SyncPolicy names one shared media timeline. Reuse the returned value across
// audio/video branches when those branches should align with each other.
type SyncPolicy struct {
	name      string
	tolerance time.Duration
	mode      syncMode
	scheduler *syncScheduler
}

type syncMode uint8

const (
	syncHoldLate syncMode = iota
	syncDropLate
)

// Sync creates a shared media timeline policy. The default holds early media
// until slower streams catch up; call WithDropLate for preview-style branches
// that should shed stale media instead.
func Sync(name string) SyncPolicy {
	return SyncPolicy{
		name:      cmp.Or(strings.TrimSpace(name), "sync"),
		mode:      syncHoldLate,
		scheduler: newSyncScheduler(),
	}
}

// WithTolerance sets the allowed media-time skew before a sync gate holds or
// drops a message.
func (p SyncPolicy) WithTolerance(tolerance time.Duration) SyncPolicy {
	if tolerance > 0 {
		p.tolerance = tolerance
	}
	return p
}

// WithDropLate makes a sync gate drop messages that arrive behind the shared
// timeline by more than the tolerance.
func (p SyncPolicy) WithDropLate() SyncPolicy {
	p.mode = syncDropLate
	return p
}

// Normalize fills the default name and scheduler used by a zero SyncPolicy.
func (p SyncPolicy) Normalize() SyncPolicy {
	if strings.TrimSpace(p.name) == "" {
		p.name = "sync"
	}
	if p.scheduler == nil {
		p.scheduler = newSyncScheduler()
	}
	return p
}

// Name returns the stable name used for diagnostics and stage naming.
func (p SyncPolicy) Name() string {
	return p.name
}

// Tolerance returns the allowed media-time skew before the gate holds or drops.
func (p SyncPolicy) Tolerance() time.Duration {
	return p.tolerance
}

// DropLate reports whether stale messages should be shed instead of holding.
func (p SyncPolicy) DropLate() bool {
	return p.mode == syncDropLate
}

// Admit updates the shared timeline and reports whether the message should be
// dropped before it reaches the branch. The returned lateness is how far the
// message trailed the policy's shared timeline in media time when it was
// dropped — the measurement QoS reports carry — and zero for admitted media
// (hold-late gates hold early media, never late media).
func (p SyncPolicy) Admit(ctx context.Context, stream av.StreamID, pts time.Duration) (time.Duration, bool, error) {
	if p.scheduler == nil {
		return 0, false, nil
	}
	return p.scheduler.admit(ctx, stream, pts, p.tolerance, p.mode)
}

// ObserveEvent updates the shared timeline after stream resets or removals.
func (p SyncPolicy) ObserveEvent(event *av.Event) {
	if p.scheduler != nil {
		p.scheduler.observeEvent(event)
	}
}

type syncScheduler struct {
	mu      sync.Mutex
	latest  map[av.StreamID]time.Duration
	changed chan struct{}
}

func newSyncScheduler() *syncScheduler {
	return &syncScheduler{latest: make(map[av.StreamID]time.Duration)}
}

func (s *syncScheduler) notifyLocked() {
	if s.changed != nil {
		close(s.changed)
		s.changed = nil
	}
}

func (s *syncScheduler) admit(ctx context.Context, stream av.StreamID, pts time.Duration, tolerance time.Duration, mode syncMode) (time.Duration, bool, error) {
	if s == nil {
		return 0, false, nil
	}
	if stream == "" {
		stream = "_"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.latest == nil {
		s.latest = make(map[av.StreamID]time.Duration)
	}
	if previous, ok := s.latest[stream]; ok && pts+tolerance < previous {
		return previous - pts, true, nil
	}
	if mode == syncDropLate {
		if max, ok := s.maxLocked(); ok && pts+tolerance < max {
			return max - pts, true, nil
		}
		s.latest[stream] = pts
		s.notifyLocked()
		return 0, false, nil
	}
	s.latest[stream] = pts
	s.notifyLocked()
	for s.tooEarlyLocked(pts, tolerance) {
		if err := ctx.Err(); err != nil {
			return 0, false, err
		}
		if s.changed == nil {
			s.changed = make(chan struct{})
		}
		ch := s.changed
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			s.mu.Lock()
			return 0, false, ctx.Err()
		case <-ch:
			s.mu.Lock()
		}
	}
	return 0, false, nil
}

func (s *syncScheduler) observeEvent(event *av.Event) {
	if s == nil || event == nil {
		return
	}
	switch event.Type {
	case av.EventDiscontinuity, av.EventCodecChanged, av.EventStreamRemoved, av.EventEndOfStream:
		s.mu.Lock()
		if event.StreamID != "" {
			delete(s.latest, event.StreamID)
		} else {
			clear(s.latest)
		}
		s.notifyLocked()
		s.mu.Unlock()
	}
}

func (s *syncScheduler) maxLocked() (time.Duration, bool) {
	if len(s.latest) == 0 {
		return 0, false
	}
	first := true
	var max time.Duration
	for _, pts := range s.latest {
		if first || pts > max {
			max = pts
			first = false
		}
	}
	return max, true
}

func (s *syncScheduler) minLocked() (time.Duration, bool) {
	if len(s.latest) == 0 {
		return 0, false
	}
	first := true
	var min time.Duration
	for _, pts := range s.latest {
		if first || pts < min {
			min = pts
			first = false
		}
	}
	return min, true
}

func (s *syncScheduler) tooEarlyLocked(pts time.Duration, tolerance time.Duration) bool {
	if len(s.latest) <= 1 {
		return false
	}
	min, ok := s.minLocked()
	if !ok {
		return false
	}
	return pts > min+tolerance
}
