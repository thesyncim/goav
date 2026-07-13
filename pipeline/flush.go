package pipeline

import "github.com/thesyncim/goav/av"

// FlushDownstream drains every buffered queue downstream of the named source,
// shedding queued packet/frame messages (recorded under DropFlush) and keeping
// queued events in order — an EndOfStream or Discontinuity sitting in a queue
// is lifecycle, not stale media. From the first queued av.EventDiscontinuity
// on, everything is preserved wholesale: the discontinuity is the reposition
// marker a ControllableSource emits at its new position, so what follows it is
// post-seek media, never backlog.
//
// This is the flush half of an accurate seek: the caller delivers the
// reposition control to the source first (so it is recorded), then flushes the
// source's downstream closure. The capability is structural — asserted as
// interface{ FlushDownstream(NodeRef) error }, like a source's
// UseClock(av.Clock) — and deliberately not part of Graph: the direct runner
// queues nothing, so a seek there has nothing to flush.
//
// The closure is computed from the live routing table at call time, so nodes
// attached at runtime are flushed too. A message already handed to a worker or
// sink cannot be recalled; it was emitted before the reposition applied and is
// delivered before the discontinuity, so order is preserved.
func (g *bufferedRunner) FlushDownstream(ref NodeRef) error {
	g.mu.RLock()
	if g.closed {
		g.mu.RUnlock()
		return ErrClosed
	}
	start, ok := g.index[ref.String()]
	if !ok || !g.nodes[start].active {
		g.mu.RUnlock()
		return ErrUnknownNode
	}
	// Downstream closure over the live routes, breadth-first; the start node
	// itself (a source has no queue) is excluded.
	visited := make([]bool, len(g.nodes))
	visited[start] = true
	frontier := []int{start}
	var targets []*bufferedNode
	for len(frontier) > 0 {
		from := frontier[0]
		frontier = frontier[1:]
		routes := g.nodes[from].routes
		for i := range routes {
			for _, to := range routes[i].to {
				if to < 0 || to >= len(g.nodes) || visited[to] {
					continue
				}
				visited[to] = true
				next := g.nodes[to]
				if !next.active {
					continue
				}
				frontier = append(frontier, to)
				if next.kind != nodeSource && next.queue != nil {
					targets = append(targets, next)
				}
			}
		}
	}
	// Release g.mu before draining: flushNodeQueue serializes against producers
	// and teardown through node.queueMutex alone, exactly like enqueue, so a
	// drain never holds the topology lock (the node pointers are stable).
	g.mu.RUnlock()

	for _, node := range targets {
		g.flushNodeQueue(node)
	}
	return nil
}

// flushNodeQueue drains one node's queue atomically with respect to producers:
// queueMutex serializes the drain-and-requeue against enqueue and close, so it
// never interleaves with a send and never touches a closed channel. The node's
// worker keeps consuming concurrently — whatever it takes during the drain is
// an in-flight delivery, not flushable backlog. Requeueing the preserved slots
// cannot block: producers are locked out, so occupancy only shrank since the
// drain. Dropped slots release like a DropOldest shed (releaseSlot + pending),
// counted under DropFlush.
func (g *bufferedRunner) flushNodeQueue(node *bufferedNode) {
	node.queueMutex.Lock()
	defer node.queueMutex.Unlock()
	if node.queueClosed || node.queue == nil {
		return
	}
	var preserved []*bufferedMessage
	keepRest := false
	for done := false; !done; {
		select {
		case slot := <-node.queue:
			if keepRest || slot.message.Kind == MessageEvent {
				if !keepRest && slot.message.Event != nil && slot.message.Event.Type == av.EventDiscontinuity {
					// The reposition marker: everything queued after it is
					// post-seek media, not backlog.
					keepRest = true
				}
				preserved = append(preserved, slot)
				continue
			}
			node.releaseSlot(slot)
			g.pending.Done()
			observeDrop(node.counters, DropFlush, &g.cold)
		default:
			done = true
		}
	}
	for _, slot := range preserved {
		node.queue <- slot
	}
}
