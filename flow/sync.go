package flow

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

// SyncPolicy names one shared media timeline. Reuse the returned value across
// audio/video branches when those branches should align with each other.
type SyncPolicy struct {
	name      string
	tolerance time.Duration
	mode      syncMode
	scheduler *syncScheduler
	flags     syncPolicyFlags
}

type syncMode uint8

const (
	syncHoldLate syncMode = iota
	syncDropLate
)

type syncPolicyFlags uint8

const (
	syncPolicyHasTolerance syncPolicyFlags = 1 << iota
	syncPolicyHasMode
)

// Sync creates a shared media timeline policy. The default holds early media
// until slower streams catch up; use SyncDropLate for preview-style branches
// that should shed stale media instead.
func Sync(name string, opts ...SyncPolicy) SyncPolicy {
	policy := SyncPolicy{
		name:      firstNonEmpty(strings.TrimSpace(name), "sync"),
		mode:      syncHoldLate,
		scheduler: newSyncScheduler(),
	}
	for i := range opts {
		if opts[i].flags&syncPolicyHasTolerance != 0 {
			policy.tolerance = opts[i].tolerance
		}
		if opts[i].flags&syncPolicyHasMode != 0 {
			policy.mode = opts[i].mode
		}
	}
	return policy
}

// SyncTolerance sets the allowed media-time skew before a sync gate holds or
// drops a message.
func SyncTolerance(tolerance time.Duration) SyncPolicy {
	if tolerance <= 0 {
		return SyncPolicy{}
	}
	return SyncPolicy{tolerance: tolerance, flags: syncPolicyHasTolerance}
}

// SyncDropLate makes a sync gate drop messages that arrive behind the shared
// timeline by more than the tolerance.
func SyncDropLate() SyncPolicy {
	return SyncPolicy{mode: syncDropLate, flags: syncPolicyHasMode}
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
	mu     sync.Mutex
	latest map[av.StreamID]time.Duration
	wait   func(context.Context) error
	closed bool
}

func newSyncScheduler() *syncScheduler {
	return &syncScheduler{latest: make(map[av.StreamID]time.Duration), wait: syncSchedulerWait}
}

func syncSchedulerWait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Millisecond):
		return nil
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
	if s.closed {
		return 0, false, pipeline.ErrClosed
	}
	if previous, ok := s.latest[stream]; ok && pts+tolerance < previous {
		return previous - pts, true, nil
	}
	if mode == syncDropLate {
		if max, ok := s.maxLocked(); ok && pts+tolerance < max {
			return max - pts, true, nil
		}
		s.latest[stream] = pts
		return 0, false, nil
	}
	s.latest[stream] = pts
	for s.tooEarlyLocked(stream, pts, tolerance) {
		if err := ctx.Err(); err != nil {
			return 0, false, err
		}
		wait := s.wait
		if wait == nil {
			wait = syncSchedulerWait
		}
		s.mu.Unlock()
		err := wait(ctx)
		s.mu.Lock()
		if err != nil {
			return 0, false, err
		}
		if s.closed {
			return 0, false, pipeline.ErrClosed
		}
	}
	return 0, false, nil
}

func (s *syncScheduler) observeEvent(event *av.Event) {
	if s == nil || event == nil {
		return
	}
	switch event.Type {
	case av.EventDiscontinuity, av.EventCodecChanged, av.EventStreamRemoved:
		s.mu.Lock()
		if event.StreamID != "" {
			delete(s.latest, event.StreamID)
		} else {
			clear(s.latest)
		}
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

func (s *syncScheduler) tooEarlyLocked(stream av.StreamID, pts time.Duration, tolerance time.Duration) bool {
	if len(s.latest) <= 1 {
		return false
	}
	min, ok := s.minLocked()
	if !ok {
		return false
	}
	return pts > min+tolerance
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
