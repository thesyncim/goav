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
// Two runner-specific rules keep the order deterministic past the source:
//   - Direct: a converging stage (an interleave.Gate) that holds a source's
//     arm back parks the source holding the turn (admit) and the turn moves
//     on; the holder hands it back once the gate admits, so a join never waits
//     on a sibling whose source cannot run.
//   - Buffered: settle waits until every message in flight was handled before
//     the turn moves, so the node workers, which run concurrently, finish one
//     source packet's work before the next packet enters the graph.
type timeOrder struct {
	turns []timeTurn
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
	index int
	// gate and msg are what a source parked by admission waits on, stored
	// before state goes blocked and read by the passer after it sees
	// blocked; the parked source does not touch them until it runs again.
	gate interleave.Gate
	msg  *Message
}

type timeTurnState = uint32

const (
	timeTurnUnstarted timeTurnState = iota // waiting to run its Start
	timeTurnWaiting                        // parked on a packet at `at`
	timeTurnBlocked                        // parked until gate admits msg
	timeTurnRunning                        // holds the turn
	timeTurnDone                           // ended, or left the order
)

// timeTurnKey is the context key under which a source in the order finds
// its turn, so an admission it waits in can tell it holds the turn.
type timeTurnKey struct{}

// timeOrderedSources returns the sources Run may interleave by time, or nil
// when it must run them all freely: a realtime graph (live sources interleave
// by arrival, which is right) or fewer than two qualifying sources. Two
// shapes order only when every source qualifies, since a free source would
// break their rules:
//   - a graph with converging stages (Admitter), on the direct runner only,
//     when each is an interleave.Gate and none is fed through a node with
//     more than one producer (a nested join): a source the stage holds back
//     is parked mid-delivery owning its arm's nodes, and no sibling may need
//     one of them;
//   - a buffered graph, whose turn moves only once the graph settled.
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
		if topo.nodes[i].active && topo.nodes[i].node.base().admit != nil {
			converging = true
			break
		}
	}
	if (buffered || converging) && !every {
		return nil
	}
	if converging && (buffered || !gatedConvergence(topo)) {
		return nil
	}
	ordered := make([]bool, len(sources))
	for i := range sources {
		ordered[i] = byTime(source(sources[i]))
	}
	return ordered
}

// gatedConvergence reports whether every converging stage of topo is an
// interleave.Gate fed only through nodes with at most one producer each.
func gatedConvergence[P graphNode](topo *snapshot[P]) bool {
	producers := make([][]int, len(topo.nodes))
	for from := range topo.nodes {
		if !topo.nodes[from].active {
			continue
		}
		for _, route := range topo.nodes[from].routes {
			for _, to := range route.to {
				if to >= 0 && to < len(producers) && !containsIndex(producers[to], from) {
					producers[to] = append(producers[to], from)
				}
			}
		}
	}
	for i := range topo.nodes {
		node := topo.nodes[i].node.base()
		if !topo.nodes[i].active || node.admit == nil {
			continue
		}
		if _, ok := node.admit.(interleave.Gate); !ok {
			return false
		}
		seen := make([]bool, len(topo.nodes))
		up := append([]int(nil), producers[i]...)
		for len(up) != 0 {
			at := up[len(up)-1]
			up = up[:len(up)-1]
			if seen[at] {
				continue
			}
			seen[at] = true
			if len(producers[at]) > 1 {
				return false
			}
			up = append(up, producers[at]...)
		}
	}
	return true
}

func containsIndex(list []int, v int) bool {
	for _, x := range list {
		if x == v {
			return true
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
		o.turns[i].index = i
		o.turns[i].grant = make(chan struct{}, 1)
		if !ordered[i] {
			o.turns[i].state.Store(timeTurnDone)
		}
	}
	o.pass(-1)
	return o
}

// context is the context source i starts under: it carries the source's
// turn, for admit.
func (o *timeOrder) context(ctx context.Context, i int) context.Context {
	return context.WithValue(ctx, timeTurnKey{}, &o.turns[i])
}

// pass hands the turn on: to the first source not yet started, else to the
// parked source with the earliest packet, counting a source parked by
// admission only once its gate admits. When nothing else can run, a source
// parked by admission goes anyway and waits in Admit, as it would unordered.
// Only the turn's holder calls it (self, or -1 when the holder is leaving),
// so passes never overlap. It reports whether self got the turn back, in
// which case no handoff is sent.
func (o *timeOrder) pass(self int) bool {
	if o.settle != nil {
		o.settle()
	}
	for {
		next, held := -1, -1
		for i := range o.turns {
			if o.turns[i].state.Load() == timeTurnUnstarted {
				next = i
				break
			}
		}
		if next < 0 {
			var earliest, heldAt int64
			for i := range o.turns {
				turn := &o.turns[i]
				switch turn.state.Load() {
				case timeTurnWaiting:
				case timeTurnBlocked:
					if !turn.gate.Admits(interleave.Token{}, turn.msg) {
						if at := turn.at.Load(); held < 0 || at < heldAt {
							held, heldAt = i, at
						}
						continue
					}
				default:
					continue
				}
				if at := turn.at.Load(); next < 0 || at < earliest {
					next, earliest = i, at
				}
			}
		}
		if next < 0 {
			next = held
		}
		if next < 0 {
			return false
		}
		turn := &o.turns[next]
		from := turn.state.Load()
		if (from != timeTurnUnstarted && from != timeTurnWaiting && from != timeTurnBlocked) || !turn.state.CompareAndSwap(from, timeTurnRunning) {
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
	return o.park(ctx, turn)
}

// admit runs before a direct delivery into a converging stage. When the
// stage would hold msg back and the delivery runs on the source holding the
// turn, that source parks until the gate admits, and the turn moves to the
// earliest source the stage is not waiting on. Its packet time stays the one
// it last yielded at. Any other delivery passes, to wait in Admit as before.
func (o *timeOrder) admit(ctx context.Context, admit Admitter, msg *Message) error {
	gate, ok := admit.(interleave.Gate)
	if !ok || gate.Admits(interleave.Token{}, msg) {
		return nil
	}
	turn, _ := ctx.Value(timeTurnKey{}).(*timeTurn)
	if turn == nil || turn.state.Load() != timeTurnRunning {
		return nil
	}
	turn.gate, turn.msg = gate, msg
	turn.state.Store(timeTurnBlocked)
	return o.park(ctx, turn)
}

// park passes the turn on from a source that just parked, and waits until it
// comes back.
func (o *timeOrder) park(ctx context.Context, turn *timeTurn) error {
	if o.pass(turn.index) {
		return nil
	}
	select {
	case <-turn.grant:
		return nil
	case <-ctx.Done():
		o.leave(turn.index)
		return ctx.Err()
	}
}

// leave takes source i out of the order (it ended, or stopped waiting) and,
// when it holds the turn, passes it on. A parked source that loses the race
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
