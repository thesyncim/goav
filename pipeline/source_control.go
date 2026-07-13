package pipeline

import "context"

// ControllableSource is an optional capability of a Source: a source whose
// running Start loop can be steered by out-of-band control messages (a seek,
// a segment) delivered through a graph's SourceInjector.
//
// Concurrency contract: Control is called synchronously from the controlling
// goroutine while Start runs concurrently (or before Start has begun).
// Implementations must be safe under that overlap — record the request in an
// atomic or send it on a channel the Start loop reads; do not touch
// Start-loop-local state directly and do not block waiting for the loop.
//
// Repositioning contract: a source that repositions (av.EventSeek, or
// av.EventSegment's jump to its start bound) must emit av.EventDiscontinuity
// from its Start loop before the first message at the new position, so
// downstream decoders reset their reference state. The graph adds no flush
// machinery on a seek.
//
// Time-axis contract: a segment (av.EventSegment, start on Event.Timestamp,
// exclusive end via av.EventSegmentEnd) behaves like a seek to start followed
// by a natural end at end: the source emits av.EventEndOfStream and returns
// from Start when the window completes, exactly as at the end of the media.
// Playback rate is not a source control: it scales the task-wide timeline
// clock, so paced sources observe it through the clock they sleep on, never
// through Control. A source that cannot honour a control returns an error
// from Control instead of ignoring it.
type ControllableSource interface {
	Source
	Control(ctx context.Context, msg *Message) error
}

// SourceInjector is implemented by graphs that can deliver an out-of-band
// control message to a single SOURCE node. It is the source-side sibling of
// NodeInjector: a source has no queue or serial worker to ride, so the message
// is handed to the source's Control method (ControllableSource) instead. It is
// an optional capability discovered by type assertion, like NodeInjector and
// NodePauser. Both the buffered and direct runners implement it.
type SourceInjector interface {
	// InjectSource delivers msg to the source named by ref by calling its
	// Control method synchronously on the caller's goroutine, concurrently with
	// the source's running Start loop (see ControllableSource for the contract
	// that makes this race-safe). Targets that are not sources, and sources
	// that do not implement ControllableSource, report ErrInvalidLink.
	InjectSource(ctx context.Context, ref NodeRef, msg *Message) error
}

// InjectSource delivers an out-of-band control message to a source in a running
// buffered graph by calling the source's Control method synchronously on the
// caller's goroutine. The source's Start loop runs concurrently on its own
// goroutine; the ControllableSource contract (Control only records the request
// via atomics or a channel the loop reads) is what makes the overlap race-safe —
// the graph takes no lock around the call.
//
// Like Inject, delivery requires the graph to be running, so the source's Start
// loop is live to observe the control; before Run it reports
// ErrDynamicGraphUnsupported.
func (g *bufferedRunner) InjectSource(ctx context.Context, ref NodeRef, msg *Message) error {
	if msg == nil {
		return ErrNilMessage
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	g.mu.RLock()
	if g.closed {
		g.mu.RUnlock()
		return ErrClosed
	}
	if !g.running {
		g.mu.RUnlock()
		return ErrDynamicGraphUnsupported
	}
	index, ok := g.index[ref.String()]
	if !ok || index < 0 || index >= len(g.nodes) || !g.nodes[index].active {
		g.mu.RUnlock()
		return ErrUnknownNode
	}
	node := g.nodes[index]
	if node.kind != nodeSource {
		g.mu.RUnlock()
		return ErrInvalidLink
	}
	controllable, ok := node.source.(ControllableSource)
	g.mu.RUnlock()
	if !ok {
		return ErrInvalidLink
	}
	// Called outside g.mu: Control is user code and may take its time; holding
	// the graph lock across it could stall topology changes.
	return controllable.Control(ctx, msg)
}

// InjectSource delivers an out-of-band control message to a source in a direct
// graph by calling the source's Control method synchronously on the caller's
// goroutine. The direct runner tracks no running state — sources execute inside
// Run's call — so delivery is accepted any time before Close: a control landing
// before Start simply pre-positions the source (ControllableSource implementations
// record the request; the Start loop observes it).
func (g *directRunner) InjectSource(ctx context.Context, ref NodeRef, msg *Message) error {
	if msg == nil {
		return ErrNilMessage
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	g.mu.RLock()
	if g.closed {
		g.mu.RUnlock()
		return ErrClosed
	}
	index, ok := g.index[ref.String()]
	if !ok || index < 0 || index >= len(g.nodes) || !g.nodes[index].active {
		g.mu.RUnlock()
		return ErrUnknownNode
	}
	node := &g.nodes[index]
	if node.kind != nodeSource {
		g.mu.RUnlock()
		return ErrInvalidLink
	}
	// Copy the interface value before releasing g.mu: g.nodes may be reallocated
	// by a concurrent AddSource/AddStage, but the source itself is stable.
	source := node.source
	g.mu.RUnlock()
	controllable, ok := source.(ControllableSource)
	if !ok {
		return ErrInvalidLink
	}
	return controllable.Control(ctx, msg)
}
