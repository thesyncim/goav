package pipeline

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"runtime"

	"github.com/thesyncim/goav/av"
)

func (t *topology[P]) Spec() Spec {
	t.mu.RLock()
	defer t.mu.RUnlock()
	spec := Spec{
		Name:      t.config.Name,
		Realtime:  t.config.Realtime,
		Execution: DescribeExecution(t.config),
	}
	spec.Execution.Admission = t.executionAdmission()
	for i := range t.nodes {
		node := t.nodes[i].base()
		if !node.isActive() {
			continue
		}
		specNode := node.spec()
		specNode.Execution = DescribeNodeExecution(specNode.Kind, t.config, node.policy)
		spec.Nodes = append(spec.Nodes, specNode)
		for j := range node.routes {
			route := &node.routes[j]
			for k := range route.to {
				to := t.nodes[route.to[k]].base()
				if !to.isActive() {
					continue
				}
				spec.Edges = append(spec.Edges, EdgeSpec{
					From:   NodeRef(node.name),
					To:     NodeRef(to.name),
					Policy: route.policy,
					Label:  route.label,
				})
			}
		}
	}
	return spec
}

func (t *topology[P]) Events() <-chan av.Event {
	return t.events
}

func (t *topology[P]) Stats() GraphStats {
	t.mu.RLock()
	defer t.mu.RUnlock()
	// Like Spec, Nodes covers the live nodes; a removed node still counts in
	// the graph totals (an unnamed entry), and so do the nodes whose index was
	// reused (the retired entry).
	names := make([]string, len(t.nodes)+1)
	nodes := make([]*nodeCounters, len(t.nodes)+1)
	reported := make([]uint64, len(t.nodes)+1)
	for i := range t.nodes {
		n := t.nodes[i].base()
		if n.isActive() {
			names[i] = n.name
		}
		nodes[i] = n.counters
		reported[i] = reportedDrops(n.source, n.stage, n.sink)
	}
	nodes[len(t.nodes)] = &t.retired.counters
	reported[len(t.nodes)] = t.retired.reported
	stats := snapshotStats(&t.cold, names, nodes, reported)
	for _, n := range t.nodes {
		if b := n.base(); b.isRetiring() && b.index >= 0 {
			stats.PendingCleanup++
		}
	}
	return stats
}

// SetNodePaused pauses or resumes delivery to a single node. The lock-free
// producer reads the flag on every delivery, so pausing one branch leaves
// the source and sibling branches untouched.
func (t *topology[P]) SetNodePaused(ref NodeRef, paused bool) error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.closed {
		return ErrClosed
	}
	index, ok := t.index[ref.String()]
	if !ok || index < 0 || index >= len(t.nodes) || !t.nodes[index].base().isActive() {
		return ErrUnknownNode
	}
	t.nodes[index].base().paused.Store(paused)
	return nil
}

// FinalizationError identifies one node's actual Close failure. ID distinguishes
// successive nodes that reused a display name; errors.Is matches Cause.
type FinalizationError struct {
	ID    uint64
	Node  NodeRef
	Cause error
}

func (e *FinalizationError) Error() string {
	return fmt.Sprintf("pipeline: cleanup of %q (#%d): %v", e.Node, e.ID, e.Cause)
}
func (e *FinalizationError) Unwrap() error { return e.Cause }

func (t *topology[P]) Close() error {
	return t.CloseContext(context.Background())
}

// CloseContext shuts the graph down. New emits stop at the nil routing
// snapshot; the strategy tears each node's delivery down and waits out what
// is in flight, so Close never runs under a concurrent Handle (the contract
// a live Remove keeps). A node whose wait times out remains owned and is
// finalized once its work finishes. Later calls also wait for pending cleanup
// and report retained deferred finalization errors, including after events close.
func (t *topology[P]) CloseContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	t.mu.Lock()
	firstClose := !t.closed
	t.closed = true
	t.snap.Store(nil)
	type closing struct {
		node         P
		await, stuck bool
	}
	first := ctx.Err()
	nodes := make([]closing, 0, len(t.nodes))
	for i := range t.nodes {
		n := closing{node: t.nodes[i]}
		b := n.node.base()
		if b.index < 0 {
			continue
		}
		var err error
		if n.await, err = t.strategy.stopLocked(ctx, n.node); err != nil {
			first = cmp.Or(first, err)
			n.stuck = true
		}
		b.phase = nodeRetired
		nodes = append(nodes, n)
	}
	t.mu.Unlock()

	for i := range nodes {
		n := &nodes[i]
		if n.stuck || !n.await {
			continue
		}
		if err := t.strategy.awaitStopped(ctx, n.node, t.config.CloseWaitTimeout); err != nil {
			first = cmp.Or(first, err)
			n.stuck = true
		}
	}
	for i := range nodes {
		if nodes[i].stuck {
			// Even a canceled stop must stop delivery before the reaper waits.
			t.mu.Lock()
			_, _ = t.strategy.stopLocked(context.Background(), nodes[i].node)
			t.mu.Unlock()
			t.reap(nodes[i].node)
			continue
		}
		if err := t.finalize(nodes[i].node); err != nil {
			first = cmp.Or(first, err)
		}
	}
	// Close events once no send is in flight, so a concurrent emit can never
	// send on the closed channel.
	if firstClose {
		t.sending.Add(eventsClosed)
		for t.sending.Load() != eventsClosed {
			runtime.Gosched()
		}
		close(t.events)
	}
	t.mu.RLock()
	deferred := t.deferredErr
	t.mu.RUnlock()
	return errors.Join(first, deferred)
}

// armReadinessLocked snapshots the readiness set at Run start: the active
// sinks that have not yet seen media. Caller must hold t.mu (write).
func (t *topology[P]) armReadinessLocked() {
	var sinks, unfed int64
	for i := range t.nodes {
		node := t.nodes[i].base()
		if !node.isActive() || node.kind != nodeSink {
			continue
		}
		sinks++
		if !node.sawMedia.Load() {
			node.sinkCounted = true
			unfed++
		} else {
			node.sinkCounted = false
		}
	}
	if t.ready.arm(sinks, unfed) {
		t.publishReady()
	}
}

// noteSinkMedia counts a sink out of the readiness set (first media delivery,
// or removal before any media); the last count publishes readiness.
func (t *topology[P]) noteSinkMedia(node *nodeBase) {
	if !node.sawMedia.CompareAndSwap(false, true) {
		return
	}
	if !node.sinkCounted {
		return
	}
	if t.ready.note() {
		t.publishReady()
	}
}

// publishReady reports av.EventTaskReady on the graph's observer stream, at
// most once. Cold path: it runs once per graph, never per message.
func (t *topology[P]) publishReady() {
	if !t.ready.claim() {
		return
	}
	t.observe(taskReadyEvent(), nil)
}

// publishEvent mirrors an emitted event onto the observer stream.
func (t *topology[P]) publishEvent(msg *Message, counters *nodeCounters) {
	if msg.Kind != MessageEvent || msg.Event == nil {
		return
	}
	t.observe(*msg.Event, counters)
}

// observe sends event to the observer stream without blocking; a full
// stream counts a DropObserver against counters (nil: graph-level only).
func (t *topology[P]) observe(event av.Event, counters *nodeCounters) {
	if t.sending.Add(1)&eventsClosed != 0 {
		t.sending.Add(-1)
		return
	}
	select {
	case t.events <- event:
	default:
		observeDrop(counters, DropObserver, &t.cold)
	}
	t.sending.Add(-1)
}

// eventsClosed marks the observer stream closed in topology.sending.
const eventsClosed = int64(1) << 62

func (n *nodeBase) close() error {
	switch n.kind {
	case nodeSource:
		return n.source.Close()
	case nodeStage:
		return n.stage.Close()
	case nodeSink:
		return n.sink.Close()
	default:
		return nil
	}
}

func (n *nodeBase) spec() NodeSpec {
	spec := NodeSpec{Name: n.name, Kind: n.kind.spec()}
	if describer := n.describer(); describer != nil {
		spec.Detail = describer.DescribeNode().Detail
	}
	return spec
}

func (n *nodeBase) describer() NodeDescriber {
	var node any
	switch n.kind {
	case nodeSource:
		node = n.source
	case nodeStage:
		node = n.stage
	case nodeSink:
		node = n.sink
	}
	describer, _ := node.(NodeDescriber)
	return describer
}

func (k nodeKind) spec() NodeKind {
	switch k {
	case nodeSource:
		return NodeSource
	case nodeStage:
		return NodeStage
	case nodeSink:
		return NodeSink
	default:
		return ""
	}
}

func (r *nodeRoute) matches(msg *Message) bool {
	switch r.policy {
	case "", RouteAll:
		return true
	case RouteByStream:
		return r.matchesStream(msg)
	case RouteByEvent:
		return r.matchesEvent(msg)
	default:
		return false
	}
}

func (r *nodeRoute) matchesStream(msg *Message) bool {
	if r.label == "" {
		return true
	}
	switch msg.Kind {
	case MessagePacket:
		return msg.Packet != nil && string(msg.Packet.StreamID) == r.label
	case MessageFrame:
		return msg.Frame != nil && string(msg.Frame.StreamID) == r.label
	case MessageEvent:
		return msg.Event != nil && string(msg.Event.StreamID) == r.label
	default:
		return false
	}
}

func (r *nodeRoute) matchesEvent(msg *Message) bool {
	if msg.Kind != MessageEvent || msg.Event == nil {
		return false
	}
	return r.label == "" || string(msg.Event.Type) == r.label
}
