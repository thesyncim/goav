package goav

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

// Reconnect makes a live input reopen when it fails: when its source stops
// with an error before the run's context ends, goav waits backoff (doubling
// for each consecutive failure, capped at 30s, with jitter) and opens the
// input again, up to attempts consecutive failures (0 retries until the run
// ends). A reconnect that delivers media resets the count. Each attempt, and
// the recovery, publish an av.EventReconnect on the task's Watch, and the
// input announces av.EventDiscontinuity downstream once it is back. It applies to live
// provider and URI inputs (a whep URL, a live transport, an http stream that
// opens live, or any input marked Live); a file or an application-pushed
// source refuses it.
func Reconnect(attempts int, backoff time.Duration) InputOption {
	return inputOption(func(spec *InputSpec) {
		spec.reconnect = &reconnectPolicy{attempts: max(attempts, 0), backoff: backoff}
	})
}

// inputOption is the concrete input-only option (Reconnect).
type inputOption func(*InputSpec)

func (o inputOption) applyInput(spec *InputSpec) {
	if spec != nil && o != nil {
		o(spec)
	}
}

type reconnectPolicy struct {
	attempts int
	backoff  time.Duration
}

const maxReconnectDelay = 30 * time.Second

// delay is the wait before the nth consecutive attempt: backoff doubled per
// earlier failure, capped, then jittered into its upper half so a fleet of
// inputs that dropped together does not reconnect together.
func (p reconnectPolicy) delay(n int) time.Duration {
	d := p.backoff
	if d <= 0 {
		d = time.Second
	}
	for i := 1; i < n && d < maxReconnectDelay; i++ {
		d *= 2
	}
	d = min(d, max(maxReconnectDelay, p.backoff))
	return d/2 + rand.N(d/2+1)
}

func (p reconnectPolicy) limit() string {
	if p.attempts == 0 {
		return "unlimited"
	}
	return strconv.Itoa(p.attempts)
}

var errReconnectNotLive = errors.New("goav.Reconnect applies to live inputs")

// reconnectingSource runs a live input's source and, when it fails, opens a
// fresh one through reopen. Its node identity is the first source's, so
// Describe and Build agree.
type reconnectingSource struct {
	name   string
	detail string
	reopen func(context.Context) (pipeline.Source, error)
	policy reconnectPolicy
	clock  av.Clock
	// report publishes on the task's Watch, off the data path.
	report func(av.Event)

	mu      sync.Mutex
	current pipeline.Source
	closed  bool

	message pipeline.Message
	event   av.Event
}

func newReconnectingSource(first pipeline.Source, detail string, reopen func(context.Context) (pipeline.Source, error), policy reconnectPolicy, clock av.Clock, report func(av.Event)) pipeline.Source {
	if tl, ok := clock.(*timeline); ok {
		// Backoff waits wall time, not the task's (pausable) media time.
		clock = tl.inner
	}
	if clock == nil {
		clock = av.MonotonicClock()
	}
	s := &reconnectingSource{
		name:    first.Name(),
		detail:  detail,
		reopen:  reopen,
		policy:  policy,
		clock:   clock,
		report:  report,
		current: first,
	}
	if _, ok := first.(pipeline.ControllableSource); ok {
		return &controllableReconnectingSource{reconnectingSource: s}
	}
	return s
}

func (s *reconnectingSource) Name() string { return s.name }

func (s *reconnectingSource) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: s.name, Kind: pipeline.NodeSource, Detail: s.detail}
}

func (s *reconnectingSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	failures := 0
	for {
		source := s.source()
		if source == nil {
			return nil
		}
		counted := countEmits(emitter)
		err := source.Start(ctx, counted)
		if err == nil || ctx.Err() != nil || s.isClosed() {
			return err
		}
		if counted.count() > 0 {
			failures = 0
		}
		for {
			failures++
			if s.policy.attempts > 0 && failures > s.policy.attempts {
				return fmt.Errorf("goav: %s: gave up after %d reconnect attempts: %w", s.name, s.policy.attempts, err)
			}
			delay := s.policy.delay(failures)
			s.publish(av.Event{
				Type:   av.EventReconnect,
				Reason: fmt.Sprintf("%s dropped: reconnecting in %s (attempt %d of %s)", s.name, delay.Round(time.Millisecond), failures, s.policy.limit()),
				Cause:  err,
				Metadata: av.Metadata{
					"attempt": strconv.Itoa(failures),
					"delay":   delay.String(),
				},
			})
			if sleepErr := s.clock.Sleep(ctx, delay); sleepErr != nil {
				return err
			}
			next, openErr := s.reopen(ctx)
			if openErr == nil {
				if !s.swap(next) {
					return nil
				}
				s.publish(av.Event{
					Type:     av.EventReconnect,
					Reason:   s.name + " reconnected",
					Metadata: av.Metadata{"attempt": strconv.Itoa(failures), "state": "reconnected"},
				})
				s.event = av.Event{Type: av.EventDiscontinuity, Reason: s.name + " reconnected"}
				s.message = pipeline.Message{Kind: pipeline.MessageEvent, Event: &s.event}
				if emitErr := emitter.Emit(ctx, &s.message); emitErr != nil {
					return emitErr
				}
				break
			}
			if ctx.Err() != nil {
				return err
			}
			err = openErr
		}
	}
}

func (s *reconnectingSource) publish(event av.Event) {
	if s.report != nil {
		s.report(event)
	}
}

func (s *reconnectingSource) source() pipeline.Source {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	return s.current
}

// swap installs a reopened source, closing the failed one; it refuses when
// the input closed meanwhile.
func (s *reconnectingSource) swap(next pipeline.Source) bool {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = next.Close()
		return false
	}
	old := s.current
	s.current = next
	s.mu.Unlock()
	_ = old.Close()
	return true
}

func (s *reconnectingSource) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *reconnectingSource) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	current := s.current
	s.mu.Unlock()
	return current.Close()
}

type controllableReconnectingSource struct {
	*reconnectingSource
}

// Control reaches the source running now.
func (s *controllableReconnectingSource) Control(ctx context.Context, msg *pipeline.Message) error {
	controllable, ok := s.source().(pipeline.ControllableSource)
	if !ok {
		return fmt.Errorf("goav: %s: the reconnected source takes no controls", s.name)
	}
	return controllable.Control(ctx, msg)
}

// countingEmitter counts what one source run emitted, keeping the runner's
// delivery capability visible.
type countingEmitter struct {
	pipeline.Emitter
	n int
}

type countingDeliveryEmitter struct {
	*countingEmitter
	delivery pipeline.DeliveryEmitter
}

type emitCounter interface {
	pipeline.Emitter
	count() int
}

func countEmits(emitter pipeline.Emitter) emitCounter {
	counted := &countingEmitter{Emitter: emitter}
	if delivery, ok := emitter.(pipeline.DeliveryEmitter); ok {
		return countingDeliveryEmitter{countingEmitter: counted, delivery: delivery}
	}
	return counted
}

func (e *countingEmitter) Emit(ctx context.Context, msg *pipeline.Message) error {
	e.n++
	return e.Emitter.Emit(ctx, msg)
}

func (e *countingEmitter) count() int { return e.n }

func (e countingDeliveryEmitter) EmitDelivery(ctx context.Context, msg *pipeline.Message) (pipeline.Delivery, error) {
	e.n++
	return e.delivery.EmitDelivery(ctx, msg)
}

// Live marks an input live: it delivers on its own clock (a capture device on
// a pipe, an Icecast mount, an MPEG-TS stream over HTTP), so its run plans
// realtime, Explain reports it live, and Reconnect applies. Providers declare
// liveness with SourceShape().Realtime, and an http input that opens with no
// length and no ranges, or as an Icecast stream, is live without it; Live
// says so before anything opens.
func Live() InputOption {
	return inputOption(func(spec *InputSpec) {
		spec.realtime = true
		spec.input.Realtime = true
	})
}
