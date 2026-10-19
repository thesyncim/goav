package goav

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

// reconnectSession is one opened session of a live input: its source, the
// streams it carries, and the qualifier its packets' ids carry.
type reconnectSession struct {
	source    pipeline.Source
	streams   []av.Stream
	qualifier string
}

// reconnectingSource runs a live input's source and, when it fails, opens a
// fresh one through reopen. Its node identity is the first source's, so
// Describe and Build agree, and its stream ids are the first session's: a
// reopened session's streams are matched to them (by id, else by media and
// codec) and renamed, and a stream that is new or gone is announced with
// av.EventStreamAdded or av.EventStreamRemoved.
type reconnectingSource struct {
	name   string
	detail string
	reopen func(context.Context) (reconnectSession, error)
	policy reconnectPolicy
	clock  av.Clock
	// report publishes on the task's Watch, off the data path.
	report func(av.Event)
	// streams are the ids downstream knows, with the qualifier they carry.
	streams   []av.Stream
	qualifier string

	mu      sync.Mutex
	current pipeline.Source
	closed  bool

	message pipeline.Message
	event   av.Event
	stream  av.Stream
}

func newReconnectingSource(first reconnectSession, detail string, reopen func(context.Context) (reconnectSession, error), policy reconnectPolicy, clock av.Clock, report func(av.Event)) pipeline.Source {
	if tl, ok := clock.(*timeline); ok {
		// Backoff waits wall time, not the task's (pausable) media time.
		clock = tl.inner
	}
	if clock == nil {
		clock = av.MonotonicClock()
	}
	s := &reconnectingSource{
		name:      first.source.Name(),
		detail:    detail,
		reopen:    reopen,
		policy:    policy,
		clock:     clock,
		report:    report,
		streams:   append([]av.Stream(nil), first.streams...),
		qualifier: first.qualifier,
		current:   first.source,
	}
	if _, ok := first.source.(pipeline.ControllableSource); ok {
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
	var ids map[av.StreamID]av.StreamID
	for {
		source := s.source()
		if source == nil {
			return nil
		}
		session := sessionEmitter(emitter, ids)
		err := source.Start(ctx, session)
		if err == nil || ctx.Err() != nil || s.isClosed() {
			return err
		}
		if session.media() > 0 {
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
				if !s.swap(next.source) {
					return nil
				}
				var drift reconnectDrift
				ids, drift = s.reconcile(next)
				recovered := av.Metadata{"attempt": strconv.Itoa(failures), "state": "reconnected"}
				if drift.changed() {
					recovered["drift"] = drift.String()
				}
				s.publish(av.Event{Type: av.EventReconnect, Reason: s.name + " reconnected", Metadata: recovered})
				if emitErr := s.announce(ctx, emitter, drift); emitErr != nil {
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

// reconnectDrift is how a reopened session's streams differ from the ones
// downstream knows: renamed ones (new id to the known id), new ones, and
// gone ones.
type reconnectDrift struct {
	renamed []string
	added   []av.Stream
	removed []av.Stream
}

func (d reconnectDrift) changed() bool {
	return len(d.renamed) != 0 || len(d.added) != 0 || len(d.removed) != 0
}

func (d reconnectDrift) String() string {
	parts := append([]string(nil), d.renamed...)
	for _, stream := range d.added {
		parts = append(parts, "+"+string(stream.ID))
	}
	for _, stream := range d.removed {
		parts = append(parts, "-"+string(stream.ID))
	}
	return strings.Join(parts, " ")
}

// reconcile matches the reopened session's streams to the known ones, by id
// and then by media type and codec in order, and returns the renames its
// emitted ids need (in emitted, qualified form). The known set becomes the
// session's, under the known ids.
func (s *reconnectingSource) reconcile(next reconnectSession) (map[av.StreamID]av.StreamID, reconnectDrift) {
	var drift reconnectDrift
	matched := make([]bool, len(s.streams))
	known := make([]av.Stream, 0, len(next.streams))
	var unmatched []av.Stream
	for _, stream := range next.streams {
		i := slices.IndexFunc(s.streams, func(old av.Stream) bool { return old.ID == stream.ID })
		if i >= 0 && !matched[i] {
			matched[i] = true
			known = append(known, stream)
			continue
		}
		unmatched = append(unmatched, stream)
	}
	ids := map[av.StreamID]av.StreamID{}
	for _, stream := range unmatched {
		i := slices.IndexFunc(s.streams, func(old av.Stream) bool { return old.Type == stream.Type && old.Codec.ID == stream.Codec.ID })
		for i >= 0 && matched[i] {
			j := slices.IndexFunc(s.streams[i+1:], func(old av.Stream) bool { return old.Type == stream.Type && old.Codec.ID == stream.Codec.ID })
			if j < 0 {
				i = -1
				break
			}
			i += 1 + j
		}
		if i < 0 {
			drift.added = append(drift.added, stream)
			known = append(known, stream)
			continue
		}
		matched[i] = true
		ids[qualifyStreamID(stream.ID, next.qualifier)] = qualifyStreamID(s.streams[i].ID, s.qualifier)
		drift.renamed = append(drift.renamed, string(stream.ID)+"="+string(s.streams[i].ID))
		renamed := stream
		renamed.ID = s.streams[i].ID
		known = append(known, renamed)
	}
	for i, old := range s.streams {
		if !matched[i] {
			drift.removed = append(drift.removed, old)
		}
	}
	s.streams, s.qualifier = known, next.qualifier
	if len(ids) == 0 {
		ids = nil
	}
	return ids, drift
}

// announce emits, on the data path, the discontinuity of the reconnect and
// the streams the reopened session added or no longer carries.
func (s *reconnectingSource) announce(ctx context.Context, emitter pipeline.Emitter, drift reconnectDrift) error {
	for _, stream := range drift.removed {
		if err := s.emit(ctx, emitter, av.Event{Type: av.EventStreamRemoved, StreamID: qualifyStreamID(stream.ID, s.qualifier)}); err != nil {
			return err
		}
	}
	for _, stream := range drift.added {
		s.stream = qualifyStream(stream, s.qualifier)
		if err := s.emit(ctx, emitter, av.Event{Type: av.EventStreamAdded, StreamID: s.stream.ID, Stream: &s.stream}); err != nil {
			return err
		}
	}
	return s.emit(ctx, emitter, av.Event{Type: av.EventDiscontinuity, Reason: s.name + " reconnected"})
}

func (s *reconnectingSource) emit(ctx context.Context, emitter pipeline.Emitter, event av.Event) error {
	s.event = event
	s.message = pipeline.Message{Kind: pipeline.MessageEvent, Event: &s.event}
	return emitter.Emit(ctx, &s.message)
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

// sessionCounter is the emitter one session runs on: it counts the media
// the session delivered, atomically (a source may emit from more than one
// goroutine), and renames its stream ids to the ones downstream knows,
// keeping the runner's delivery capability visible.
type sessionCounter interface {
	pipeline.Emitter
	media() int64
}

type countingEmitter struct {
	pipeline.Emitter
	n   atomic.Int64
	ids map[av.StreamID]av.StreamID
	// mu guards stream, the renamed copy of an event's stream: the event's
	// own belongs to the source.
	mu     sync.Mutex
	stream av.Stream
}

type countingDeliveryEmitter struct {
	*countingEmitter
	delivery pipeline.DeliveryEmitter
}

func sessionEmitter(emitter pipeline.Emitter, ids map[av.StreamID]av.StreamID) sessionCounter {
	counted := &countingEmitter{Emitter: emitter, ids: ids}
	if delivery, ok := emitter.(pipeline.DeliveryEmitter); ok {
		return countingDeliveryEmitter{countingEmitter: counted, delivery: delivery}
	}
	return counted
}

func (e *countingEmitter) Emit(ctx context.Context, msg *pipeline.Message) error {
	unlock := e.observe(msg)
	defer unlock()
	return e.Emitter.Emit(ctx, msg)
}

func (e countingDeliveryEmitter) EmitDelivery(ctx context.Context, msg *pipeline.Message) (pipeline.Delivery, error) {
	unlock := e.observe(msg)
	defer unlock()
	return e.delivery.EmitDelivery(ctx, msg)
}

func (e *countingEmitter) media() int64 { return e.n.Load() }

func noUnlock() {}

// observe counts a packet or frame and renames its stream; an event with a
// renamed stream holds the scratch copy until delivered.
func (e *countingEmitter) observe(msg *pipeline.Message) func() {
	if msg == nil {
		return noUnlock
	}
	switch {
	case msg.Kind == pipeline.MessagePacket && msg.Packet != nil:
		e.n.Add(1)
		if id, ok := e.ids[msg.Packet.StreamID]; ok {
			msg.Packet.StreamID = id
		}
	case msg.Kind == pipeline.MessageFrame && msg.Frame != nil:
		e.n.Add(1)
		if id, ok := e.ids[msg.Frame.StreamID]; ok {
			msg.Frame.StreamID = id
		}
	case msg.Kind == pipeline.MessageEvent && msg.Event != nil && e.ids != nil:
		if id, ok := e.ids[msg.Event.StreamID]; ok {
			msg.Event.StreamID = id
		}
		if msg.Event.Stream != nil {
			if id, ok := e.ids[msg.Event.Stream.ID]; ok {
				e.mu.Lock()
				e.stream = *msg.Event.Stream
				e.stream.ID = id
				msg.Event.Stream = &e.stream
				return e.mu.Unlock
			}
		}
	}
	return noUnlock
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
