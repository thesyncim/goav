package pipeline

import (
	"context"
	"sync/atomic"
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
// passed between them with no lock. Only the source holding the turn passes
// it, so the choice of the next source is never made twice at once; each
// turn's state is an atomic, and the one race left (a waiting source
// leaving just as the turn is handed to it) is settled by a CAS: if the
// leave loses, the leaver holds the turn and passes it on. A channel handoff
// happens only when the turn changes hands.
type timeOrder struct {
	turns []timeTurn
}

type timeTurn struct {
	state atomic.Uint32 // a timeTurnState
	// at is the parked packet's time, stored before state goes waiting and
	// read by the passer after it sees waiting.
	at    atomic.Int64
	grant chan struct{} // capacity 1: the turn, handed to a parked source
}

type timeTurnState = uint32

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
func timeOrderedSources(realtime bool, topo *snapshot[*directNode], sources []directSourceRun) []bool {
	if realtime || topo == nil {
		return nil
	}
	for i := range topo.nodes {
		if topo.nodes[i].active && topo.nodes[i].node.admit != nil {
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
			o.turns[i].state.Store(timeTurnDone)
		}
	}
	o.pass(-1)
	return o
}

// pass hands the turn on: to the first source not yet started, else to the
// parked source with the earliest packet. Only the turn's holder calls it
// (self, or -1 when the holder is leaving), so passes never overlap. It
// reports whether self got the turn back, in which case no handoff is sent.
func (o *timeOrder) pass(self int) bool {
	for {
		next := -1
		for i := range o.turns {
			if o.turns[i].state.Load() == timeTurnUnstarted {
				next = i
				break
			}
		}
		if next < 0 {
			var earliest int64
			for i := range o.turns {
				if o.turns[i].state.Load() != timeTurnWaiting {
					continue
				}
				if at := o.turns[i].at.Load(); next < 0 || at < earliest {
					next, earliest = i, at
				}
			}
		}
		if next < 0 {
			return false
		}
		turn := &o.turns[next]
		from := turn.state.Load()
		if (from != timeTurnUnstarted && from != timeTurnWaiting) || !turn.state.CompareAndSwap(from, timeTurnRunning) {
			continue // it left meanwhile: choose again
		}
		if next == self {
			return true
		}
		turn.grant <- struct{}{}
		return false
	}
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
	turn := &o.turns[i]
	if turn.state.Load() != timeTurnRunning {
		return nil
	}
	turn.at.Store(int64(at))
	turn.state.Store(timeTurnWaiting)
	if o.pass(i) {
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
// when it holds the turn, passes it on. A waiting source that loses the race
// to a passer handing it the turn finds itself running, and passes it too;
// the grant that passer sends lands in a channel nobody reads again.
func (o *timeOrder) leave(i int) {
	turn := &o.turns[i]
	for {
		switch s := turn.state.Load(); s {
		case timeTurnDone:
			return
		case timeTurnRunning:
			turn.state.Store(timeTurnDone)
			o.pass(-1)
			return
		default:
			if turn.state.CompareAndSwap(s, timeTurnDone) {
				return
			}
		}
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
