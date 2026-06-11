package goav

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/thesyncim/goav/av"
)

// joinSyncMode selects how a convergent join (Mix/Composite) aligns its arms.
type joinSyncMode int

const (
	// joinSyncArrival pairs arm frames by arrival: one buffered frame per arm
	// per step, no timestamp math. Right for live sources on one clock; the
	// default.
	joinSyncArrival joinSyncMode = iota
	// joinSyncPTS aligns arm frames by presentation time on a common
	// nanosecond clock — opt-in via Mix(...).SyncByPTS() and
	// Composite(...).SyncByPTS().
	joinSyncPTS
)

// joinSyncNSUnset marks the step clock as underived: the next step re-derives
// it from the minimum head PTS (first step, or after a discontinuity re-sync).
const joinSyncNSUnset = math.MinInt64

// joinSyncNodeDetail renders the join node's planned/built graph detail.
// Arrival is the default and stays unannotated, so existing plans are
// byte-identical; the planner and the stages' DescribeNode share this one
// function, keeping Describe() ≡ Build().
func joinSyncNodeDetail(mode joinSyncMode) string {
	if mode == joinSyncPTS {
		return "sync=pts"
	}
	return ""
}

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
	mode    joinSyncMode
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
}

func newJoinSyncStateWithPendingCap(mode joinSyncMode, inputs []av.StreamID, pendingCap int) *joinSyncState {
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
	if s.mode != joinSyncPTS {
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
	if s.mode == joinSyncPTS {
		s.dropStale()
	}
	if !s.ready() {
		return nil, 0, false
	}
	frames = s.stepFrames()
	if s.mode == joinSyncPTS {
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
