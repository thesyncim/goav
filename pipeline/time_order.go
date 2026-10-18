package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/thesyncim/goav/internal/interleave"
)

// timeOrder interleaves the time-ordered sources (interleave.Source) of a
// direct, offline graph the way ffmpeg picks its next input: exactly one of
// them runs at a time, and at every packet the source whose packet is
// earliest goes next (DTS, else PTS; ties go to the source added first).
// Which source runs is decided only at those points, never by the Go
// scheduler, so the graph sees the same message sequence on every run and a
// shared mux writes the same bytes.
//
// A source yields inside Emit, holding the packet it is about to deliver.
// Its pump is parked there, so the read scratch the packet lives in cannot be
// overwritten while it waits: the pending emit is the peek, and no second
// buffer is needed. Sources keep their own goroutines; the turn is a token
// passed between them, one mutex round per packet and a channel handoff only
// when the turn changes hands.
type timeOrder struct {
	mu    sync.Mutex
	turns []timeTurn
}

type timeTurn struct {
	state timeTurnState
	at    time.Duration
	grant chan struct{} // capacity 1: the turn, handed to a parked source
}

type timeTurnState uint8

const (
	timeTurnUnstarted timeTurnState = iota // waiting to run its Start
	timeTurnWaiting                        // parked on a packet at `at`
	timeTurnRunning                        // holds the turn
	timeTurnDone                           // ended, or left the order
)

// timeOrderedSources returns the sources Run may interleave by time, or nil
// when it must run them all freely: a realtime graph (live sources interleave
// by arrival, which is right), fewer than two qualifying sources, or a graph
// with a converging stage that holds one upstream back (Admitter) — waiting
// for a sibling while holding the turn would never end.
func timeOrderedSources(realtime bool, topo *directTopo, sources []directSourceRun) []bool {
	if realtime || topo == nil {
		return nil
	}
	for i := range topo.nodes {
		if topo.nodes[i].active && topo.nodes[i].admit != nil {
			return nil
		}
	}
	count := 0
	for i := range sources {
		if byTime(sources[i].source) {
			count++
		}
	}
	if count < 2 {
		return nil // and allocates nothing: most graphs end here
	}
	ordered := make([]bool, len(sources))
	for i := range sources {
		ordered[i] = byTime(sources[i].source)
	}
	return ordered
}

func byTime(source Source) bool {
	timed, ok := source.(interleave.Source)
	return ok && timed.ByTime(interleave.Token{})
}

// newTimeOrder starts an order over the sources marked in ordered and hands
// the first of them the turn; the others never take part.
func newTimeOrder(ordered []bool) *timeOrder {
	o := &timeOrder{turns: make([]timeTurn, len(ordered))}
	for i := range o.turns {
		o.turns[i].grant = make(chan struct{}, 1)
		if !ordered[i] {
			o.turns[i].state = timeTurnDone
		}
	}
	o.mu.Lock()
	o.passLocked(-1)
	o.mu.Unlock()
	return o
}

// passLocked hands the turn on: to the first source not yet started, else to
// the parked source with the earliest packet. It reports whether self got it,
// in which case no handoff is sent.
func (o *timeOrder) passLocked(self int) bool {
	next := -1
	for i := range o.turns {
		if o.turns[i].state == timeTurnUnstarted {
			next = i
			break
		}
	}
	if next < 0 {
		for i := range o.turns {
			if o.turns[i].state == timeTurnWaiting && (next < 0 || o.turns[i].at < o.turns[next].at) {
				next = i
			}
		}
	}
	if next < 0 {
		return false
	}
	o.turns[next].state = timeTurnRunning
	if next == self {
		return true
	}
	o.turns[next].grant <- struct{}{}
	return false
}

// enter parks source i until its first turn.
func (o *timeOrder) enter(ctx context.Context, i int) {
	select {
	case <-o.turns[i].grant:
	case <-ctx.Done():
		o.leave(i)
	}
}

// yield parks source i, which holds the turn, on a packet at `at` until that
// packet is the earliest. A source that is not in the order passes through.
func (o *timeOrder) yield(ctx context.Context, i int, at time.Duration) error {
	o.mu.Lock()
	turn := &o.turns[i]
	if turn.state != timeTurnRunning {
		o.mu.Unlock()
		return nil
	}
	turn.state = timeTurnWaiting
	turn.at = at
	mine := o.passLocked(i)
	o.mu.Unlock()
	if mine {
		return nil
	}
	select {
	case <-turn.grant:
		return nil
	case <-ctx.Done():
		o.leave(i)
		return ctx.Err()
	}
}

// leave takes source i out of the order (it ended, or stopped waiting) and,
// when it held the turn, passes it on.
func (o *timeOrder) leave(i int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	turn := &o.turns[i]
	if turn.state == timeTurnDone {
		return
	}
	held := turn.state == timeTurnRunning
	turn.state = timeTurnDone
	select {
	case <-turn.grant: // a handoff that raced the leave: the turn was ours
		held = true
	default:
	}
	if held {
		o.passLocked(-1)
	}
}

// timeOrderedEmitter is a time-ordered source's emitter: a packet with a
// time waits for its turn, then delivers like any other emit.
type timeOrderedEmitter struct {
	emitter *directEmitter
	order   *timeOrder
	index   int
}

func (e *timeOrderedEmitter) Emit(ctx context.Context, msg *Message) error {
	if err := e.wait(ctx, msg); err != nil {
		return err
	}
	return e.emitter.Emit(ctx, msg)
}

// EmitDelivery keeps the DeliveryEmitter capability of the wrapped emitter.
func (e *timeOrderedEmitter) EmitDelivery(ctx context.Context, msg *Message) (Delivery, error) {
	if err := e.wait(ctx, msg); err != nil {
		return Delivery{}, err
	}
	return e.emitter.EmitDelivery(ctx, msg)
}

func (e *timeOrderedEmitter) wait(ctx context.Context, msg *Message) error {
	if msg == nil || msg.Kind != MessagePacket || msg.Packet == nil {
		return nil
	}
	at, ok := msg.Packet.DTS.ToDuration()
	if !ok {
		if at, ok = msg.Packet.PTS.ToDuration(); !ok {
			// No time to order by: deliver in place, keeping the turn.
			return nil
		}
	}
	return e.order.yield(ctx, e.index, at)
}
