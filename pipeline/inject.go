package pipeline

import "context"

// NodeInjector is implemented by graphs that can deliver an out-of-band message
// to a single named node in a running graph. Both the buffered and direct
// runners implement it: buffered graphs enqueue onto the node's serial worker,
// and direct graphs serialize delivery through the node's ownership gate.
type NodeInjector interface {
	// Inject delivers msg to the node named by ref, serialized with inbound
	// traffic on that node. It is safe to call concurrently with the running
	// graph, so the node's Handle still observes one message at a time with no
	// extra locking.
	Inject(ctx context.Context, ref NodeRef, msg *Message) error
}

// Inject delivers an out-of-band control message to a single node in a running
// buffered graph. The message is enqueued onto the target node's queue through the
// same lock-free producer path as routed traffic, so the node's serial worker
// drains and Handles it without any extra synchronization — the injection is
// race-safe by construction.
//
// Injection requires the graph to be running (the node's worker must be draining
// its queue). A message for a paused, removed, or closed node is shed silently,
// matching the behaviour of routed delivery to the same node.
func (g *bufferedRunner) Inject(ctx context.Context, ref NodeRef, msg *Message) error {
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
	if node.kind == nodeSource || !node.opened {
		g.mu.RUnlock()
		return ErrInvalidLink
	}
	// Hold a stable *bufferedNode and release g.mu before enqueuing: enqueue takes
	// no lock at all (it meets queue teardown on the ring's tail), so a
	// blocking send cannot deadlock a concurrent topology mutation. The injected
	// message is counted as inbound on the target by deliver's observeIn, exactly
	// like routed traffic; there is no upstream node, so nothing counts it as out.
	g.mu.RUnlock()
	delivered, err := g.enqueue(ctx, node, msg, nil)
	if err != nil {
		return err
	}
	if !delivered {
		// A control silently swallowed by a full lossy queue is the worst outcome
		// for a caller who believes the switch/keyframe landed — surface the shed
		// as backpressure so the caller can retry.
		return ErrBackpressure
	}
	return nil
}

// Inject delivers an out-of-band control message to a single node in a running
// direct graph, serialized with media deliveries through the node's ownership
// gate (handoff.go). Like bufferedRunner.Inject, it requires the graph to be
// running and refuses nil messages, closed graphs, unknown nodes, and sources.
func (g *directRunner) Inject(ctx context.Context, ref NodeRef, msg *Message) error {
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
	if !g.running.Load() {
		g.mu.RUnlock()
		return ErrDynamicGraphUnsupported
	}
	index, ok := g.index[ref.String()]
	if !ok || index < 0 || index >= len(g.nodes) || !g.nodes[index].active {
		g.mu.RUnlock()
		return ErrUnknownNode
	}
	node := g.nodes[index]
	if node.kind == nodeSource {
		g.mu.RUnlock()
		return ErrInvalidLink
	}
	g.mu.RUnlock()
	delivered, err := g.deliver(ctx, nil, node, msg)
	if err != nil {
		return err
	}
	if !delivered {
		return ErrBackpressure
	}
	return nil
}
