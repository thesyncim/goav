package pipeline

import "context"

// NodeInjector is implemented by graphs that can deliver an out-of-band message
// to a single named node in a running graph. It is an optional capability
// discovered by type assertion (like NodePauser), so it does not widen the Graph
// interface for every implementer. Direct graphs do not implement it: they have
// no per-node serial worker to deliver on, so there is nothing to serialize an
// injection against.
type NodeInjector interface {
	// Inject delivers msg to the node named by ref, processed on that node's
	// serial worker exactly like inbound traffic. It is safe to call concurrently
	// with the running graph: the message rides the node's normal queue, so the
	// node's Handle still observes one message at a time with no extra locking.
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
	if node.kind == nodeSource || node.queue == nil {
		g.mu.RUnlock()
		return ErrInvalidLink
	}
	// Hold a stable *bufferedNode and release g.mu before enqueuing: enqueue takes
	// no g.mu (it serializes against queue teardown via node.queueMutex), so a
	// blocking send cannot deadlock a concurrent topology mutation. The injected
	// message is counted as inbound on the target by deliver's observeIn, exactly
	// like routed traffic; there is no upstream node, so nothing counts it as out.
	g.mu.RUnlock()
	delivered, err := g.enqueue(ctx, node, msg)
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
