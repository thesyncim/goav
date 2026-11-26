package goav

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/internal/interleave"
	"github.com/thesyncim/goav/pipeline"
)

// joinSyncNSUnset marks the step clock as underived: the next step re-derives
// it from the minimum head PTS (first step, or after a discontinuity re-sync).
const joinSyncNSUnset = math.MinInt64

// joinArmQueue is a growable ring buffer of buffered frames for one join arm.
// Pop is O(1) without shifting elements.
type joinArmQueue struct {
	buf   []*av.Frame
	head  int
	count int
}

func (q *joinArmQueue) push(frame *av.Frame) {
	if q.count == len(q.buf) {
		newCap := max(len(q.buf)*2, 4)
		grown := make([]*av.Frame, newCap)
		if q.count > 0 {
			n := copy(grown, q.buf[q.head:])
			copy(grown[n:], q.buf[:q.head])
		}
		q.buf = grown
		q.head = 0
	}
	q.buf[(q.head+q.count)%len(q.buf)] = frame
	q.count++
}

func (q *joinArmQueue) peek() *av.Frame {
	if q.count == 0 {
		return nil
	}
	return q.buf[q.head]
}

func (q *joinArmQueue) pop() *av.Frame {
	if q.count == 0 {
		return nil
	}
	frame := q.buf[q.head]
	q.buf[q.head] = nil
	q.head = (q.head + 1) % len(q.buf)
	q.count--
	if q.count == 0 {
		q.head = 0
	}
	return frame
}

type joinArmState struct {
	id    av.StreamID
	queue joinArmQueue
	eos   bool
	gate  *joinArmGate
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
	mode       compile.JoinSyncMode
	armStates  []joinArmState
	armMap     map[av.StreamID]int // populated only when len(armStates) > 8
	eosCount   int
	eosEmitted bool
	frames     []*av.Frame
	recycle    func(armIdx int, frame *av.Frame)

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

	// admissionEnabled is set once by enableAdmission before Run starts.
	admissionEnabled bool
	// released frees every waiting producer when the stage closes.
	released     chan struct{}
	releasedOnce sync.Once
}

// joinArmLeadFrames is how many buffered frames an arm may lead the join by
// before its producer waits: enough slack for PTS alignment across arms of
// different cadences, small enough that a fast arm cannot pull its whole
// stream into memory while a slow sibling catches up.
const joinArmLeadFrames = 16

// joinArmGate is one arm's admission state. depth mirrors arm.queue.count
// for the producer's goroutine; wake carries a single "room freed" token, so a
// producer that saw a full arm and then waits cannot miss the pop that frees
// it.
type joinArmGate struct {
	depth atomic.Int64
	wake  chan struct{}
	arm   int // the arm's index in armStates: its interleave.Gate name
}

// armIndex returns the index of id in armStates, or -1 if unknown.
// Linear scan for <= 8 arms avoids string map hashing on the hot path.
func (s *joinSyncState) armIndex(id av.StreamID) int {
	if s.armMap != nil {
		if idx, ok := s.armMap[id]; ok {
			return idx
		}
		return -1
	}
	for i := range s.armStates {
		if s.armStates[i].id == id {
			return i
		}
	}
	return -1
}

// enableAdmission turns on per-arm backpressure. The planner enables it only
// when every arm has its own upstream: an arm that shares an upstream with a
// sibling (a tap arm) could otherwise block the very producer the sibling
// needs to deliver the frame that frees it.
func (s *joinSyncState) enableAdmission() {
	for i := range s.armStates {
		s.armStates[i].gate = &joinArmGate{wake: make(chan struct{}, 1), arm: i}
	}
	s.admissionEnabled = true
	s.released = make(chan struct{})
}

// admit runs on an arm producer's goroutine (pipeline.Admitter): it waits
// while the frame's arm already leads the join by joinArmLeadFrames, so the
// arm's upstream backpressures instead of the join buffering without bound.
// Events and frames of unknown arms pass. Frames in flight between admit and
// Handle are not counted, so the bound is the lead plus the join's input
// queue, and nothing leaks when a delivery is shed.
func (s *joinSyncState) admit(ctx context.Context, msg *pipeline.Message) error {
	if !s.admissionEnabled || msg == nil || msg.Kind != pipeline.MessageFrame || msg.Frame == nil {
		return nil
	}
	idx := s.armIndex(msg.Frame.StreamID)
	if idx < 0 {
		return nil
	}
	gate := s.armStates[idx].gate
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
	if !s.admissionEnabled || msg == nil || msg.Kind != pipeline.MessageFrame || msg.Frame == nil {
		return 0, false
	}
	idx := s.armIndex(msg.Frame.StreamID)
	if idx < 0 || s.armStates[idx].gate == nil {
		return 0, false
	}
	return idx, true
}

// admits reports, without blocking, whether admit would take more of arm
// now: the time order's check (interleave.Gate) before it grants the turn to
// a source that feeds the arm.
func (s *joinSyncState) admits(arm int) bool {
	if arm < 0 || arm >= len(s.armStates) || s.armStates[arm].gate == nil || s.armStates[arm].gate.depth.Load() < joinArmLeadFrames {
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
	if !s.admissionEnabled {
		return
	}
	for i := range s.armStates {
		gate := s.armStates[i].gate
		if gate == nil {
			continue
		}
		depth := int64(s.armStates[i].queue.count)
		prev := gate.depth.Load()
		if prev == depth {
			continue
		}
		gate.depth.Store(depth)
		if prev >= joinArmLeadFrames && depth < joinArmLeadFrames {
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
	arms := make([]joinArmState, len(inputs))
	var armMap map[av.StreamID]int
	if len(inputs) > 8 {
		armMap = make(map[av.StreamID]int, len(inputs))
	}
	for i, id := range inputs {
		arms[i] = joinArmState{
			id:    id,
			queue: joinArmQueue{buf: make([]*av.Frame, pendingCap)},
		}
		if armMap != nil {
			armMap[id] = i
		}
	}
	return &joinSyncState{
		mode:      mode,
		armStates: arms,
		armMap:    armMap,
		frames:    make([]*av.Frame, len(inputs)),
		nextNS:    joinSyncNSUnset,
	}
}

// bufferAt queues one arm frame (already cloned by the stage) into armIdx,
// recycling frames whose arm is unknown or already ended.
func (s *joinSyncState) bufferAt(armIdx int, frame *av.Frame) {
	if s.eosEmitted || armIdx < 0 || armIdx >= len(s.armStates) || s.armStates[armIdx].eos {
		s.recycleFrame(armIdx, frame)
		return
	}
	s.armStates[armIdx].queue.push(frame)
}

// buffer queues one arm frame by StreamID lookup.
func (s *joinSyncState) buffer(frame *av.Frame) {
	s.bufferAt(s.armIndex(frame.StreamID), frame)
}

// pendingCount returns the number of buffered frames for id (used by tests).
func (s *joinSyncState) pendingCount(id av.StreamID) int {
	idx := s.armIndex(id)
	if idx < 0 {
		return 0
	}
	return s.armStates[idx].queue.count
}

// markEOS records an arm's end of stream and reports whether every arm ended
// for the first time. A stream-less EOS marks every declared arm ended; unknown
// stream IDs are ignored.
func (s *joinSyncState) markEOS(id av.StreamID) bool {
	if s.eosEmitted {
		return false
	}
	if id == "" {
		for i := range s.armStates {
			s.armStates[i].eos = true
		}
		s.eosCount = len(s.armStates)
		s.eosEmitted = true
		return true
	}
	if idx := s.armIndex(id); idx >= 0 && !s.armStates[idx].eos {
		s.armStates[idx].eos = true
		s.eosCount++
	}
	if s.eosCount >= len(s.armStates) {
		s.eosEmitted = true
		return true
	}
	return false
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
	for i := range s.armStates {
		if id != "" && s.armStates[i].id != id {
			continue
		}
		q := &s.armStates[i].queue
		dropped := uint64(q.count)
		for q.count > 0 {
			s.recycleFrame(i, q.pop())
		}
		if dropped > 0 {
			s.dropped.Add(dropped)
		}
	}
	s.nextNS = joinSyncNSUnset
	return true
}

// ready reports whether a step can advance: at least one arm has a buffered
// frame and every arm without one has ended.
func (s *joinSyncState) ready() bool {
	any := false
	for i := range s.armStates {
		if s.armStates[i].queue.count != 0 {
			any = true
			continue
		}
		if !s.armStates[i].eos {
			return false
		}
	}
	return any
}

// step pops the next aligned set of head frames: one slot per input arm in
// input order, nil where an arm contributes nothing this step (gap or ended).
// ref indexes the timing-reference frame the output stamps PTS/Duration from;
// it is never nil when ok.
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
	for i := range s.armStates {
		if frames[i] != nil {
			s.armStates[i].queue.pop()
		}
	}
	return frames, ref, true
}

func (s *joinSyncState) stepFrames() []*av.Frame {
	if cap(s.frames) < len(s.armStates) {
		s.frames = make([]*av.Frame, len(s.armStates))
	}
	frames := s.frames[:len(s.armStates)]
	clear(frames)
	return frames
}

// selectArrival fills every pending head into its slot (the arrival step).
func (s *joinSyncState) selectArrival(frames []*av.Frame) int {
	ref := -1
	for i := range s.armStates {
		head := s.armStates[i].queue.peek()
		if head == nil {
			continue
		}
		frames[i] = head
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
	for i := range s.armStates {
		head := s.armStates[i].queue.peek()
		if head == nil {
			continue
		}
		ns, usable := framePTSNS(head)
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
	refHead := s.armStates[tIndex].queue.peek()
	refDurNS := frameDurationNS(refHead)
	tolNS := refDurNS / 2
	for i := range s.armStates {
		head := s.armStates[i].queue.peek()
		if head == nil {
			continue
		}
		ns, usable := framePTSNS(head)
		if !usable || ns <= tNS+tolNS {
			frames[i] = head
		}
	}
	s.nextNS = tNS + refDurNS
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
	for i := range s.armStates {
		q := &s.armStates[i].queue
		for q.count != 0 {
			head := q.peek()
			ns, usable := framePTSNS(head)
			if !usable || ns >= s.nextNS-frameDurationNS(head)/2 {
				break
			}
			s.recycleFrame(i, q.pop())
			s.dropped.Add(1)
		}
	}
}

func (s *joinSyncState) recycleFrame(armIdx int, frame *av.Frame) {
	if s.recycle != nil && frame != nil {
		s.recycle(armIdx, frame)
	}
}

func (s *joinSyncState) recyclePending() {
	if s == nil {
		return
	}
	for i := range s.armStates {
		q := &s.armStates[i].queue
		for q.count > 0 {
			s.recycleFrame(i, q.pop())
		}
	}
	clear(s.frames)
}

// droppedFrames reports the running catch-up drop total — the value the join
// stages expose through pipeline.DropReporter. Safe concurrently with Handle.
func (s *joinSyncState) droppedFrames() uint64 {
	return s.dropped.Load()
}

// joinStageBase holds the shared lifecycle, admission, freelist, and event
// plumbing for convergence stages (audioMixStage, videoCompositeStage).
type joinStageBase struct {
	name    string
	out     av.StreamID
	sync    *joinSyncState
	free    [][]*av.Frame
	pools   *mediaPools
	closed  bool
	output  av.Frame
	message pipeline.Message
}

func (b *joinStageBase) Name() string { return b.name }

// DescribeNode reports the same detail the join planner records on the planned
// node, keeping Describe() ≡ Build() when ByPTS annotates the join.
func (b *joinStageBase) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: b.name, Kind: pipeline.NodeStage, Detail: compile.JoinSyncNodeDetail(b.sync.mode)}
}

// DroppedMessages implements pipeline.DropReporter.
func (b *joinStageBase) DroppedMessages() uint64 { return b.sync.droppedFrames() }

// Admit implements pipeline.Admitter.
func (b *joinStageBase) Admit(ctx context.Context, msg *pipeline.Message) error {
	return b.sync.admit(ctx, msg)
}

// Arm and Admits implement interleave.Gate.
func (b *joinStageBase) Arm(_ interleave.Token, msg any) (int, bool) { return b.sync.arm(msg) }

func (b *joinStageBase) Admits(_ interleave.Token, arm int) bool { return b.sync.admits(arm) }

func (b *joinStageBase) enableAdmission() { b.sync.enableAdmission() }

func (b *joinStageBase) handleEvent(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter, drain func(context.Context, pipeline.Emitter) error) error {
	if msg.Event == nil {
		return nil
	}
	switch msg.Event.Type {
	case av.EventEndOfStream:
		ended := b.sync.markEOS(msg.Event.StreamID)
		if err := drain(ctx, emitter); err != nil {
			return err
		}
		if ended {
			out := av.Event{Type: av.EventEndOfStream, StreamID: b.out}
			return emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageEvent, Event: &out})
		}
		return nil
	case av.EventDiscontinuity:
		if !b.sync.discontinuity(msg.Event.StreamID) {
			return nil
		}
		out := *msg.Event
		out.StreamID = b.out
		return emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageEvent, Event: &out})
	case av.EventSelectActive:
		// Control-plane events ride the data path through joins unchanged.
		return emitter.Emit(ctx, msg)
	}
	return nil
}

func (b *joinStageBase) recycleArmFrame(armIdx int, frame *av.Frame) {
	if frame == nil {
		return
	}
	frame.Release()
	if armIdx < 0 || armIdx >= len(b.free) {
		b.releaseFramePlanes(frame)
		return
	}
	b.free[armIdx] = append(b.free[armIdx], frame)
}

func (b *joinStageBase) recycleStepFrames(frames []*av.Frame) {
	for i := range frames {
		if frames[i] != nil {
			b.recycleArmFrame(i, frames[i])
			frames[i] = nil
		}
	}
}

func (b *joinStageBase) releaseFramePlanes(frame *av.Frame) {
	if frame == nil {
		return
	}
	if b.pools != nil {
		b.pools.putPlanes(frame.Planes)
		frame.Planes = nil
	}
}

func (b *joinStageBase) Close() error {
	if b.closed {
		return nil
	}
	b.closed = true
	if b.sync != nil {
		b.sync.releaseAdmission()
		b.sync.recyclePending()
	}
	for i := range b.free {
		for j := range b.free[i] {
			b.releaseFramePlanes(b.free[i][j])
			b.free[i][j] = nil
		}
		b.free[i] = nil
	}
	if b.pools != nil {
		b.pools.putPlanes(b.output.Planes)
		b.output.Planes = nil
	}
	return nil
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
