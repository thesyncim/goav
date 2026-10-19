package goav

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/pipeline"
)

// joinSyncNSUnset marks the step clock as underived: the next step re-derives
// it from the minimum head PTS (first step, or after a discontinuity re-sync).
const joinSyncNSUnset = math.MinInt64

// joinSyncState is the arm buffering and alignment state shared by the
// convergence stages (audioMixStage, videoCompositeStage). Every field is
// touched only inside the owning stage's serial Handle — lock-free by design,
// no mutex on the hot path — and alignment is pure PTS math: no wall clock,
// deterministic, unit-testable.
//
// EOS semantics (both modes): an ended arm stops gating — its buffered frames
// still participate, then the remaining arms keep joining without it; the
// joined stream ends only when every arm has ended.
type joinSyncState struct {
	mode    compile.JoinSyncMode
	inputs  []av.StreamID
	pending map[av.StreamID][]*av.Frame
	eos     map[av.StreamID]struct{}
	frames  []*av.Frame
	recycle func(*av.Frame)

	// nextNS is the expected start of the next PTS step — the end of the last
	// emitted step on the common nanosecond clock.
	nextNS int64

	// dropped counts frames discarded to stay aligned: stale heads behind the
	// already-emitted timeline plus frames flushed by an arm discontinuity.
	// Written only inside the owning stage's serial Handle; atomic because the
	// join stages surface it through pipeline.DropReporter, which the runners
	// poll concurrently at snapshot time (Stats/Snapshot show it as the join
	// node's Dropped count under the "sync" reason).
	dropped atomic.Uint64

	// gates hold each arm back once it leads the join by joinArmLeadFrames
	// buffered frames (see admit); nil when admission is off. Built once and
	// only read afterwards, so producers read the map without a lock.
	gates map[av.StreamID]*joinArmGate
	// arms are the same gates in input order.
	arms []*joinArmGate
	// released frees every waiting producer when the stage closes.
	released     chan struct{}
	releasedOnce sync.Once
}

// joinArmLeadFrames is how many buffered frames an arm may lead the join by
// before its producer waits: enough slack for PTS alignment across arms of
// different cadences, small enough that a fast arm cannot pull its whole
// stream into memory while a slow sibling catches up.
const joinArmLeadFrames = 16

// joinArmGate is one arm's admission state. depth mirrors len(pending[arm])
// for the producer's goroutine; wake carries a single "room freed" token, so a
// producer that saw a full arm and then waits cannot miss the pop that frees
// it.
type joinArmGate struct {
	depth atomic.Int64
	wake  chan struct{}
	arm   int // the arm's index in inputs: its interleave.Gate name
}

// enableAdmission turns on per-arm backpressure. The planner enables it only
// when every arm has its own upstream: an arm that shares an upstream with a
// sibling (a tap arm) could otherwise block the very producer the sibling
// needs to deliver the frame that frees it.
func (s *joinSyncState) enableAdmission() {
	s.gates = make(map[av.StreamID]*joinArmGate, len(s.inputs))
	s.arms = make([]*joinArmGate, len(s.inputs))
	for i, id := range s.inputs {
		s.arms[i] = &joinArmGate{wake: make(chan struct{}, 1), arm: i}
		s.gates[id] = s.arms[i]
	}
	s.released = make(chan struct{})
}

// admit runs on an arm producer's goroutine (pipeline.Admitter): it waits
// while the frame's arm already leads the join by joinArmLeadFrames, so the
// arm's upstream backpressures instead of the join buffering without bound.
// Events and frames of unknown arms pass. Frames in flight between admit and
// Handle are not counted, so the bound is the lead plus the join's input
// queue, and nothing leaks when a delivery is shed.
func (s *joinSyncState) admit(ctx context.Context, msg *pipeline.Message) error {
	if s.gates == nil || msg == nil || msg.Kind != pipeline.MessageFrame || msg.Frame == nil {
		return nil
	}
	gate := s.gates[msg.Frame.StreamID]
	if gate == nil {
		return nil
	}
	for gate.depth.Load() >= joinArmLeadFrames {
		select {
		case <-gate.wake:
		case <-s.released:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// arm names the arm a frame message belongs to (interleave.Gate), false for
// what admit always passes.
func (s *joinSyncState) arm(m any) (int, bool) {
	msg, _ := m.(*pipeline.Message)
	if s.gates == nil || msg == nil || msg.Kind != pipeline.MessageFrame || msg.Frame == nil {
		return 0, false
	}
	gate := s.gates[msg.Frame.StreamID]
	if gate == nil {
		return 0, false
	}
	return gate.arm, true
}

// admits reports, without blocking, whether admit would take more of arm
// now: the time order's check (interleave.Gate) before it grants the turn to
// a source that feeds the arm.
func (s *joinSyncState) admits(arm int) bool {
	if arm < 0 || arm >= len(s.arms) || s.arms[arm].depth.Load() < joinArmLeadFrames {
		return true
	}
	select {
	case <-s.released:
		return true
	default:
		return false
	}
}

// publishDepths mirrors each arm's pending depth for admit and wakes an arm
// whose depth fell below the lead. The stages call it after every Handle.
func (s *joinSyncState) publishDepths() {
	for _, id := range s.inputs {
		gate := s.gates[id]
		if gate == nil {
			continue
		}
		depth := int64(len(s.pending[id]))
		if gate.depth.Swap(depth) >= joinArmLeadFrames && depth < joinArmLeadFrames {
			select {
			case gate.wake <- struct{}{}:
			default:
			}
		}
	}
}

// releaseAdmission frees every waiting producer; the stages call it on Close.
func (s *joinSyncState) releaseAdmission() {
	if s.released != nil {
		s.releasedOnce.Do(func() { close(s.released) })
	}
}

func newJoinSyncStateWithPendingCap(mode compile.JoinSyncMode, inputs []av.StreamID, pendingCap int) *joinSyncState {
	if pendingCap < 1 {
		pendingCap = 1
	}
	pending := make(map[av.StreamID][]*av.Frame, len(inputs))
	for i := range inputs {
		pending[inputs[i]] = make([]*av.Frame, 0, pendingCap)
	}
	return &joinSyncState{
		mode:    mode,
		inputs:  append([]av.StreamID(nil), inputs...),
		pending: pending,
		eos:     make(map[av.StreamID]struct{}, len(inputs)),
		frames:  make([]*av.Frame, len(inputs)),
		nextNS:  joinSyncNSUnset,
	}
}

// buffer queues one arm frame (already cloned by the stage).
func (s *joinSyncState) buffer(frame *av.Frame) {
	s.pending[frame.StreamID] = append(s.pending[frame.StreamID], frame)
}

// markEOS records an arm's end of stream and reports whether every arm ended.
func (s *joinSyncState) markEOS(id av.StreamID) bool {
	s.eos[id] = struct{}{}
	return len(s.eos) >= len(s.inputs)
}

// discontinuity flushes the named arm's pending queue (every arm when the
// event carries no stream id) and resets the step clock, so a PTS join
// re-syncs at the arm's new position instead of mixing stale frames. It
// reports whether it acted: arrival joins ignore discontinuities — arrival
// order is their only contract, preserved unchanged.
func (s *joinSyncState) discontinuity(id av.StreamID) bool {
	if s.mode != compile.JoinSyncPTS {
		return false
	}
	for _, arm := range s.inputs {
		if id != "" && arm != id {
			continue
		}
		queue := s.pending[arm]
		for i := range queue {
			s.recycleFrame(queue[i])
			queue[i] = nil
		}
		s.dropped.Add(uint64(len(queue)))
		s.pending[arm] = queue[:0]
	}
	s.nextNS = joinSyncNSUnset
	return true
}

// ready reports whether a step can advance: at least one arm has a buffered
// frame and every arm without one has ended.
func (s *joinSyncState) ready() bool {
	any := false
	for _, id := range s.inputs {
		if len(s.pending[id]) != 0 {
			any = true
			continue
		}
		if _, ended := s.eos[id]; !ended {
			return false
		}
	}
	return any
}

// step pops the next aligned set of head frames: one slot per input arm in
// input order, nil where an arm contributes nothing this step (gap or ended).
// ref indexes the timing-reference frame the output stamps PTS/Duration from;
// it is never nil when ok.
//
// Arrival mode consumes every buffered head. PTS mode rescales each head to
// nanoseconds and lets t = min(head PTS) define the step: heads within
// tolerance of t (half the reference frame's duration) are consumed, newer
// heads stay queued — their arms sit the step out (audio: silence by absence;
// video: not painted). Heads entirely behind the already-emitted timeline are
// dropped to catch up, and heads without a usable PTS participate
// unconditionally (arrival degradation — a PTS-less arm never stalls the join).
func (s *joinSyncState) step() (frames []*av.Frame, ref int, ok bool) {
	if s.mode == compile.JoinSyncPTS {
		s.dropStale()
	}
	if !s.ready() {
		return nil, 0, false
	}
	frames = s.stepFrames()
	if s.mode == compile.JoinSyncPTS {
		ref = s.selectPTS(frames)
	} else {
		ref = s.selectArrival(frames)
	}
	for i, id := range s.inputs {
		if frames[i] != nil {
			s.pending[id], _ = popJoinFrame(s.pending[id])
		}
	}
	return frames, ref, true
}

func (s *joinSyncState) stepFrames() []*av.Frame {
	if cap(s.frames) < len(s.inputs) {
		s.frames = make([]*av.Frame, len(s.inputs))
	}
	frames := s.frames[:len(s.inputs)]
	for i := range frames {
		frames[i] = nil
	}
	return frames
}

// selectArrival fills every pending head into its slot (the arrival step).
func (s *joinSyncState) selectArrival(frames []*av.Frame) int {
	ref := -1
	for i, id := range s.inputs {
		if len(s.pending[id]) == 0 {
			continue
		}
		frames[i] = s.pending[id][0]
		if ref < 0 {
			ref = i
		}
	}
	return ref
}

// selectPTS fills the participant slots for one PTS step and returns the
// timing-reference index — the arm whose head defines t.
func (s *joinSyncState) selectPTS(frames []*av.Frame) int {
	tIndex, tNS := -1, int64(0)
	for i, id := range s.inputs {
		queue := s.pending[id]
		if len(queue) == 0 {
			continue
		}
		ns, usable := framePTSNS(queue[0])
		if !usable {
			continue
		}
		if tIndex < 0 || ns < tNS {
			tIndex, tNS = i, ns
		}
	}
	if tIndex < 0 {
		// No usable PTS on any head: degrade to arrival for this step.
		return s.selectArrival(frames)
	}
	tolNS := frameDurationNS(s.pending[s.inputs[tIndex]][0]) / 2
	for i, id := range s.inputs {
		queue := s.pending[id]
		if len(queue) == 0 {
			continue
		}
		ns, usable := framePTSNS(queue[0])
		if !usable || ns <= tNS+tolNS {
			frames[i] = queue[0]
		}
	}
	s.nextNS = tNS + frameDurationNS(s.pending[s.inputs[tIndex]][0])
	return tIndex
}

// dropStale discards heads whose presentation time lies behind the
// already-emitted timeline by more than half their own duration — the arm fell
// behind (duplicate or backwards PTS without a discontinuity), so the join
// catches up instead of replaying time.
func (s *joinSyncState) dropStale() {
	if s.nextNS == joinSyncNSUnset {
		return
	}
	for _, id := range s.inputs {
		queue := s.pending[id]
		for len(queue) != 0 {
			ns, usable := framePTSNS(queue[0])
			if !usable || ns >= s.nextNS-frameDurationNS(queue[0])/2 {
				break
			}
			var dropped *av.Frame
			queue, dropped = popJoinFrame(queue)
			s.recycleFrame(dropped)
			s.dropped.Add(1)
		}
		s.pending[id] = queue
	}
}

func (s *joinSyncState) recycleFrame(frame *av.Frame) {
	if s.recycle != nil && frame != nil {
		s.recycle(frame)
	}
}

func (s *joinSyncState) recyclePending() {
	if s == nil {
		return
	}
	for _, id := range s.inputs {
		queue := s.pending[id]
		for i := range queue {
			s.recycleFrame(queue[i])
			queue[i] = nil
		}
		s.pending[id] = queue[:0]
	}
	for i := range s.frames {
		s.frames[i] = nil
	}
}

func popJoinFrame(queue []*av.Frame) ([]*av.Frame, *av.Frame) {
	if len(queue) == 0 {
		return queue, nil
	}
	frame := queue[0]
	copy(queue, queue[1:])
	queue[len(queue)-1] = nil
	return queue[:len(queue)-1], frame
}

// droppedFrames reports the running catch-up drop total — the value the join
// stages expose through pipeline.DropReporter. Safe concurrently with Handle.
func (s *joinSyncState) droppedFrames() uint64 {
	return s.dropped.Load()
}

// framePTSNS rescales a frame's PTS to the common nanosecond clock. ok=false
// means the frame carries no usable PTS (invalid timebase) and cannot gate a
// PTS step.
func framePTSNS(frame *av.Frame) (int64, bool) {
	d, ok := frame.PTS.ToDuration()
	if !ok {
		return 0, false
	}
	return int64(d), true
}

// frameDurationNS reports the frame's span on the nanosecond clock: the
// declared Duration when usable, else the audio length derived from
// samples/rate, else 0 (exact-match tolerance).
func frameDurationNS(frame *av.Frame) int64 {
	if d, ok := frame.Duration.ToDuration(); ok && d > 0 {
		return int64(d)
	}
	if frame.Audio != nil && frame.Audio.SampleRate > 0 && frame.Audio.Samples > 0 {
		return int64(time.Second) * int64(frame.Audio.Samples) / int64(frame.Audio.SampleRate)
	}
	return 0
}
