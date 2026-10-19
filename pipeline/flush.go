package pipeline

import (
	"runtime"

	"github.com/thesyncim/goav/av"
)

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
				if next.kind != nodeSource && next.opened {
					targets = append(targets, next)
				}
			}
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
