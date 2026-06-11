package goav

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

// Select switches one of N live arms through to a single destination — the
// runtime is a passthrough one-of-N switch (active-speaker camera, simulcast
// layer, failover). Each arm is a source chain or another join:
// Select(Mix(a, b), Mix(c, d)) switches between two live mixes, where each
// sub-mix contributes its joined output under its output id (mix, mix-2).
// Unlike Mix/Composite it does NOT decode/encode/resample: the active arm's
// frames or packets are forwarded as-is, only the StreamID rewritten to the
// output. Arms must have distinct stream ids; the first arm is active by
// default and SelectActive switches live through the control plane. This
// reuses the existing Job, so .To/Build/Run are unchanged.
func Select(arms ...JoinArm) *selectorStream {
	return &selectorStream{arms: arms}
}

// selectBufferCapacity sizes the non-lossy queues on the Select graph. It is small
// — a passthrough switch holds no real backlog — but large enough that an injected
// SelectActive control rides alongside live arm traffic without forcing a stall.
const selectBufferCapacity = 32

type selectorStream struct {
	arms   []JoinArm
	taps   []TapRef
	region *compositeRegion
}

// joinArm lets a Select stand as an arm of an outer join: the outer join
// consumes the SWITCHED output stream under the selector's output id.
func (s *selectorStream) joinArm() joinArmSpec {
	if s == nil {
		return joinArmSpec{}
	}
	return joinArmSpec{join: &joinSpec{kind: joinSelect, arms: s.arms, taps: s.taps}, region: s.region}
}

// Region places the switched stream at (x, y) when the Select stands as an
// arm of an outer Composite — an active-speaker switch as one canvas tile,
// mirroring .Region on source chains and nested composites. It has no effect
// outside a Composite.
func (s *selectorStream) Region(x, y int) *selectorStream {
	s.region = &compositeRegion{x: x, y: y}
	return s
}

// Tap names the switched stream as a stable attach point — the same tap a
// normal chain declares: it appears in task.Taps() and runtime branches can
// Attach from it later. Its domain follows the arms (frame arms switch frames,
// packet arms switch packets).
func (s *selectorStream) Tap(tap TapRef) *selectorStream {
	s.taps = append(s.taps, tap)
	return s
}

// Branches fans the switched stream out to planned branch chains, each with its
// own destinations — the same goav.Branch specs an ordinary stream chain
// accepts at this stream point.
func (s *selectorStream) Branches(branches ...BranchSpec) *Job {
	return newJoinBranchesJob(joinSelect, joinSpec{arms: s.arms, taps: s.taps}, branches)
}

// To delivers the switched stream to one or more sink destinations (a fanout
// when several are given — each sink receives the switched stream) and
// returns a Job, so the select runs through the same Build/Run as every other
// recipe. Select stays sink-only: it forwards the active arm as-is, so muxed
// destinations need .Branches(...) with an encoding branch. It lowers to the
// one joinSpec shared by every convergence builder.
func (s *selectorStream) To(destinations ...Destination) *Job {
	return newJoinJob(joinSelect, joinSpec{arms: s.arms, dests: destinations, taps: s.taps})
}

// SelectActive switches a running Select to forward the arm identified by id —
// task.Control(ctx, goav.SelectActive("cam2")) flips the live output without
// restarting the task and without naming any graph node: the switch enters at
// the source boundary and rides the data path to the selector, which consumes
// it. For a graph with several selectors, narrow with .At(node) (expert).
func SelectActive(id av.StreamID) Control {
	return Control{Type: ControlSelect, StreamID: id}
}

// selectJoinProfile is Select's entry in the join table: media-agnostic
// passthrough arms (the zero media selector picks each arm's single stream, so a
// frame or packet arm switches as-is), no decode, selectorStage convergence,
// sink-only delivery.
var selectJoinProfile = joinProfile{
	// Select is driven live by SelectActive through the control plane, which
	// needs a per-node serial worker — i.e. a buffered graph. A direct (zero)
	// runtime buffer would build a direct graph with no injector, so default it
	// to a non-lossy buffered policy. An explicitly configured buffer is
	// honoured as-is.
	graphBuffer: &pipeline.BufferPolicy{Capacity: selectBufferCapacity, Drop: pipeline.DropBlock},
	newStage: func(p *joinPlan, armIDs []av.StreamID) (pipeline.Stage, *pipeline.BufferPolicy) {
		// The selector is the control target: an injected SelectActive shares its
		// input queue with the flooding arms, so a lossy queue could evict the
		// control before the worker applies it. The pinned non-lossy DropBlock
		// queue backpressures instead, so the switch is always delivered (the
		// DropBlock control-plane lesson).
		return newSelectorStage(p.name, armIDs, av.StreamID(p.name)),
			&pipeline.BufferPolicy{Capacity: selectBufferCapacity, Drop: pipeline.DropBlock}
	},
	// The switched output is the first arm's stream forwarded as-is under the
	// selector's output id (its planned node name) — Select is passthrough, so
	// the arm's codec and format facts describe the joined stream. A nested
	// join arm contributes its joined stream the same way.
	joinedStream: func(p *joinPlan) av.Stream {
		stream := p.arms[0].stream
		stream.ID = av.StreamID(p.name)
		return stream
	},
	sinkOnlyReason: "select to a non-Sink destination is not supported yet",
	sinkOnlySuggestions: []string{
		"deliver the selected stream with .To(goav.Sink(sink))",
		"fan the selected stream out with .Branches(...) when it must reach muxed destinations",
	},
}
