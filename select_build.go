package goav

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

// selectBufferCapacity sizes the non-lossy queues on the Select graph. It is small
// — a passthrough switch holds no real backlog — but large enough that an injected
// SelectActive control rides alongside live arm traffic without forcing a stall.
const selectBufferCapacity = 32

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
		stream := p.arms[0].joinStream()
		stream.ID = av.StreamID(p.name)
		return stream
	},
	sinkOnlyReason: "select to a non-Sink destination is not supported yet",
	sinkOnlySuggestions: []string{
		"deliver the selected stream with .To(goav.Sink(sink))",
		"encode the selected stream when it must reach muxed destinations: goav.Select(a, b).Encode(codec...).To(out)",
	},
}
