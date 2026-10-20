package pipeline

import (
	"runtime"

	"github.com/thesyncim/goav/av"
)

// FlushDownstream drains buffered queues exclusively downstream of the named
// source, shedding queued packet/frame messages (recorded under DropFlush) and
// keeping queued events in order. From the first queued av.EventDiscontinuity
// on, everything is preserved: the marker starts the source's new position.
//
// A queue reached by an unrelated upstream is shared. It and its downstream
// queues are preserved, because their media cannot be attributed to the seeking
// source without inspecting arbitrary component behavior. A same-source fork
// and rejoin remains exclusive. Shared queues can therefore deliver a bounded
// admitted pre-seek tail before the source's discontinuity, while retaining
// the other sources' media. Per-arm join state handles that discontinuity.
//
// The caller probes the source's control capability, flushes, then delivers the
// reposition control, so this operation cannot discard media emitted after
// that control. The capability is structural — asserted as
// interface{ FlushDownstream(NodeRef) error } — and deliberately not part of
// Graph: the direct runner queues nothing.
//
// Ownership is computed from the live routes at call time, including attached
// nodes. A message already handed to a worker or sink cannot be recalled; it
// precedes the discontinuity on that path.
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
			}
		}
	}
	// Mark every entry from outside this closure, then propagate that shared
	// ownership through its descendants. Computing the full closure first
	// makes this independent of traversal order: a fork from this source can
	// rejoin exclusively, while an external arm contaminates every downstream
	// queue even if another path from this source reaches it first.
	shared := make([]bool, len(g.nodes))
	for from, node := range g.nodes {
		if !node.active || visited[from] {
			continue
		}
		for _, route := range node.routes {
			for _, to := range route.to {
				if to >= 0 && to < len(g.nodes) && visited[to] && !shared[to] && g.nodes[to].active {
					shared[to] = true
					frontier = append(frontier, to)
				}
			}
		}
	}
	for len(frontier) > 0 {
		from := frontier[0]
		frontier = frontier[1:]
		for _, route := range g.nodes[from].routes {
			for _, to := range route.to {
				if to >= 0 && to < len(g.nodes) && visited[to] && !shared[to] && g.nodes[to].active {
					shared[to] = true
					frontier = append(frontier, to)
				}
			}
		}
	}
	var targets []*bufferedNode
	for i, node := range g.nodes {
		if i != start && visited[i] && !shared[i] && node.active && node.kind != nodeSource && node.opened {
			targets = append(targets, node)
		}
	}
	// Release g.mu before draining: flushNodeQueue serializes against producers
	// and teardown through the queue ring alone, so a drain never holds the
	// topology lock (the node pointers are stable).
	g.mu.RUnlock()

	for _, node := range targets {
		g.flushNodeQueue(node)
	}
	return nil
}

// flushNodeQueue drains one node's queue atomically with respect to
// producers: it locks the ring's tail, so a push waits (parked) or
// claims its position before the lock and lands before the drain finishes;
// the drain then pops everything and requeues the preserved slots in order,
// so no new message can slip in between. The node's worker keeps consuming
// concurrently — whatever it takes during the drain is an in-flight delivery,
// not flushable backlog. Requeueing the preserved slots cannot overflow:
// producers are locked out, so occupancy only shrank since the drain. A close
// waits for the lock to clear, so the requeue never meets a closed queue.
// Dropped slots release like a DropOldest shed (releaseSlot + pending),
// counted under DropFlush. Cold path: the lock is the ring's, held for one
// pass over at most Capacity slots.
func (g *bufferedRunner) flushNodeQueue(node *bufferedNode) {
	q := &node.queue
	if !q.lock() {
		return // closed
	}
	var preserved []*bufferedMessage
	keepRest := false
	for {
		slot := q.pop()
		if slot == nil {
			if q.drained() {
				break
			}
			// A push that claimed its position before the lock is still
			// publishing it.
			runtime.Gosched()
			continue
		}
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
		g.pending.done()
		observeDrop(node.counters, DropFlush, &g.cold)
	}
	for _, slot := range preserved {
		for q.push(slot, true) == ringFull {
			// The worker is handing a cell back.
			runtime.Gosched()
		}
	}
	q.unlock()
	node.wakeWorker()
	node.spaceLine.wakeAll()
}
