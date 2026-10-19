package pipeline

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/thesyncim/goav/internal/interleave"
)

// timeOrder interleaves the time-ordered sources (interleave.Source) of an
// offline graph the way ffmpeg picks its next input: exactly one of them runs
// at a time, and at every packet the source whose packet is earliest goes
// next (DTS, else PTS; ties go to the source added first). Which source runs
// is decided only at those points, never by the Go scheduler, so the graph
// sees the same message sequence on every run and a shared mux writes the
// same bytes.
//
// A source yields inside Emit, holding the packet it is about to deliver.
// Its pump is parked there, so the read scratch the packet lives in cannot be
// overwritten while it waits: the pending emit is the peek, and no second
// buffer is needed. Each source holds back at most that one packet, so a
// source far ahead of the others reads nothing more until they catch up.
// Sources keep their own goroutines; the turn is a token passed between them
// with no lock. Only the source holding the turn passes it, so the choice of
// the next source is never made twice at once; each turn's state is an
// atomic, and the one race left (a waiting source leaving just as the turn is
// handed to it) is settled by a CAS: if the leave loses, the leaver holds the
// turn and passes it on. A channel handoff happens only when the turn changes
// hands.
//
// Joins (converging stages that are interleave.Gates) bound their arms here,
// at the turn, instead of blocking a delivery in Admit: every frame a gate
// receives is charged to the source holding the turn (observe), and a source
// feeding an arm its gate holds back is passed over until the gate admits
// again. No source ever waits mid-delivery, so no source waits on nodes
// another one owns, and nested joins and joins on either runner keep the
// order. An arm may overshoot its gate by the frames one packet yields (more
// at a decoder's flush), which stays bounded. When every waiting source is
// held back, the earliest goes anyway.
//
// On the buffered runner settle waits, before every pass, until every message
// in flight was handled, so the node workers, which run concurrently, finish
// one source packet's work before the next packet enters the graph; a gate's
// state, and so each pass's choice, is then the same on every run.
type timeOrder struct {
	turns []timeTurn
	// holder is the index of the source holding the turn: the one every
	// frame a gate receives meanwhile is charged to.
	holder atomic.Int64
	// settle, when set, runs before every pass (the buffered runner's
	// quiescence wait).
	settle func()
}

type timeTurn struct {
	state atomic.Uint32 // a timeTurnState
	// at is the parked packet's time, stored before state goes waiting and
	// read by the passer after it sees waiting.
	at    atomic.Int64
	grant chan struct{} // capacity 1: the turn, handed to a parked source
	// feeds lists the gate arms this source's frames reached, copied on
	// write: it grows only when a new arm is first seen.
	feeds atomic.Pointer[[]gateArm]
}

// gateArm is one arm of a join a source feeds.
type gateArm struct {
	gate interleave.Gate
	arm  int
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
// by arrival, which is right) or fewer than two qualifying sources. Two
// shapes order only when every source qualifies, since a free source would
// break their rules:
//   - a graph with converging stages (Admitter), when each is an
//     interleave.Gate: a plain Admitter can only bound an arm by blocking a
//     delivery, which the order cannot see past;
//   - a buffered graph with a node fed by more than one producer (a shared
//     mux, a join), whose turn moves only once the graph settled. With none,
//     as in a promoted ladder, every output reads one chain of FIFO queues
//     and is in order already, so its workers keep pipelining across packets.
func timeOrderedSources[P graphNode, S any](realtime, buffered bool, topo *snapshot[P], sources []S, source func(S) Source) []bool {
	if realtime || topo == nil {
		return nil
	}
	count := 0
	for i := range sources {
		if byTime(source(sources[i])) {
			count++
		}
	}
	if count < 2 {
		return nil // and allocates nothing: most graphs end here
	}
	every := count == len(sources)
	converging := false
	for i := range topo.nodes {
		admit := topo.nodes[i].node.base().admit
		if !topo.nodes[i].active || admit == nil {
			continue
		}
		if _, ok := admit.(interleave.Gate); !ok {
			return nil
		}
		converging = true
	}
	if (buffered || converging) && !every {
		return nil
	}
	if buffered && !merges(topo) {
		return nil // each node has one producer chain, already in order
	}
	ordered := make([]bool, len(sources))
	for i := range sources {
		ordered[i] = byTime(source(sources[i]))
	}
	return ordered
}

// merges reports whether some node of topo has more than one producer, the
// only place a buffered graph's concurrent workers can reorder messages.
func merges[P graphNode](topo *snapshot[P]) bool {
	producer := make([]int, len(topo.nodes))
	for i := range producer {
		producer[i] = -1
	}
	for from := range topo.nodes {
		if !topo.nodes[from].active {
			continue
		}
		for _, route := range topo.nodes[from].routes {
			for _, to := range route.to {
				if to < 0 || to >= len(producer) || producer[to] == from {
					continue
				}
				if producer[to] >= 0 {
					return true
				}
				producer[to] = from
			}
		}
	}
	return false
}

func byTime(source Source) bool {
	timed, ok := source.(interleave.Source)
	return ok && timed.ByTime(interleave.Token{})
}

// newTimeOrder starts an order over the sources marked in ordered and hands
// the first of them the turn; the others never take part.
func newTimeOrder(ordered []bool, settle func()) *timeOrder {
	o := &timeOrder{turns: make([]timeTurn, len(ordered)), settle: settle}
	for i := range o.turns {
		o.turns[i].grant = make(chan struct{}, 1)
		if !ordered[i] {
			o.turns[i].state.Store(timeTurnDone)
		}
	}
	o.pass(-1)
	return o
}

// admit stands in for a converging stage's Admit in an ordered run: a Gate
// never blocks the delivery, the frame is charged to the source holding the
// turn, and the order bounds the arm at the next pass. It reports false for a
// plain Admitter, which an ordered graph never has.
func (o *timeOrder) admit(admit Admitter, msg *Message) bool {
	gate, ok := admit.(interleave.Gate)
	if !ok {
		return false
	}
	arm, ok := gate.Arm(interleave.Token{}, msg)
	if !ok {
		return true
	}
	turn := &o.turns[o.holder.Load()]
	for {
		feeds := turn.feeds.Load()
		var list []gateArm
		if feeds != nil {
			list = *feeds
		}
		for _, fed := range list {
			if fed.arm == arm && fed.gate == gate {
				return true
			}
		}
		grown := append(append(make([]gateArm, 0, len(list)+1), list...), gateArm{gate: gate, arm: arm})
		if turn.feeds.CompareAndSwap(feeds, &grown) {
			return true
		}
	}
}

// heldBack reports whether a gate holds back an arm turn feeds.
func (turn *timeTurn) heldBack() bool {
	feeds := turn.feeds.Load()
	if feeds == nil {
		return false
	}
	for _, fed := range *feeds {
		if !fed.gate.Admits(interleave.Token{}, fed.arm) {
			return true
		}
	}
	return false
}

// pass hands the turn on: to the first source not yet started, else to the
// parked source with the earliest packet that no gate holds back, else (all
// held) to the earliest parked source. Only the turn's holder calls it (self,
// or -1 when the holder is leaving), so passes never overlap. It reports
// whether self got the turn back, in which case no handoff is sent.
func (o *timeOrder) pass(self int) bool {
	if o.settle != nil {
		o.settle()
	}
	for {
		next := -1
		for i := range o.turns {
			if o.turns[i].state.Load() == timeTurnUnstarted {
				next = i
				break
			}
		}
		if next < 0 {
			held := -1
			var earliest, heldAt int64
			for i := range o.turns {
				turn := &o.turns[i]
				if turn.state.Load() != timeTurnWaiting {
					continue
				}
				at := turn.at.Load()
				if turn.heldBack() {
					if held < 0 || at < heldAt {
						held, heldAt = i, at
					}
					continue
				}
				if next < 0 || at < earliest {
					next, earliest = i, at
				}
			}
			if next < 0 {
				next = held
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
		o.holder.Store(int64(next))
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

// orderedEmitter is the emitter a runner hands a time-ordered source.
type orderedEmitter interface {
	Emitter
	DeliveryEmitter
}

// timeOrderedEmitter is a time-ordered source's emitter: a packet with a
// time waits for its turn, then delivers like any other emit.
type timeOrderedEmitter struct {
	emitter orderedEmitter
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
