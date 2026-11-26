package pipeline

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/thesyncim/goav/av"
)

// runNode is a node's serial worker. A delivery error is reported the moment
// it happens — which cancels the run — and from then on every worker sheds
// what is still queued instead of delivering it, so a failed task stops
// promptly rather than finishing its backlog.
func (g *bufferedRunner) runNode(ctx context.Context, node *bufferedNode) {
	for {
		msg := node.queue.pop()
		if msg == nil {
			if !node.idle() {
				return
			}
			continue
		}
		// Taking a message frees a queue position a DropBlock producer holding
		// a slot may be waiting on.
		if spaceWaiters(node.waiting.Load()) > 0 {
			node.spaceLine.wakeOne()
		}
		switch {
		case g.fail.failed.Load() || node.faulted():
			// The run failed; shed without delivering.
		case g.staleByLatency(node, msg):
			observeDrop(node.counters, DropStale, &g.cold)
			g.reportStaleQoS(node, msg)
		default:
			handleCtx, releaseContext := node.handlerContext(ctx)
			g.nodeError(ctx, &node.nodeBase, g.deliver(handleCtx, node, &msg.message))
			if releaseContext != nil {
				releaseContext()
			}
		}
		node.releaseSlot(msg)
		g.pending.done()
	}
}

// staleByLatency reports whether a queued message has waited longer than the
// node's MaxLatency and should be shed instead of delivered late (realtime).
func (g *bufferedRunner) staleByLatency(node *bufferedNode, msg *bufferedMessage) bool {
	return node.policy.MaxLatency > 0 &&
		!msg.enqueuedAt.IsZero() &&
		time.Since(msg.enqueuedAt) > node.policy.MaxLatency
}

// deliver runs lock-free: the worker owns a stable *bufferedNode whose
// kind/stage/sink/emitter/counters/free are immutable after the queue opens, so
// no g.mu is taken per delivered message. Teardown is observed via two atomics:
// the routing snapshot going nil (graph Close) sheds with ErrClosed; node.removed (a node Remove
// retired before its queue drained) sheds silently.
func (g *bufferedRunner) deliver(ctx context.Context, node *bufferedNode, msg *Message) error {
	if g.snap.Load() == nil {
		return ErrClosed
	}
	if node.removed.Load() {
		return nil
	}
	observeIn(node.counters, msg, &g.cold)
	switch node.kind {
	case nodeStage:
		return node.stage.Handle(ctx, msg, &node.emitter)
	case nodeSink:
		if (msg.Kind == MessagePacket || msg.Kind == MessageFrame) && !node.sawMedia.Load() {
			g.noteSinkMedia(&node.nodeBase)
		}
		return node.sink.Handle(ctx, msg)
	default:
		return ErrInvalidLink
	}
}

// qosReportWindow rate-limits MaxLatency-shed QoS reports to one per node per
// window, so a persistently stale queue cannot storm the observer stream. The
// window runs on the process monotonic clock (time.Since a fixed origin, like
// staleByLatency's measurement); 0 in qosLast means "never reported", and the
// origin bias below keeps a stored stamp from ever being 0.
const qosReportWindow = time.Second

var qosWindowOrigin = time.Now()

// reportStaleQoS publishes one rate-limited av.EventQoS for a message shed
// under BufferPolicy.MaxLatency: lateness is how long past the budget it
// waited, dropped is always true. Runs only on the shed path — never per
// delivered message — with one atomic load when suppressed and the cold
// observer send when claiming.
func (g *bufferedRunner) reportStaleQoS(node *bufferedNode, msg *bufferedMessage) {
	now := time.Since(qosWindowOrigin)
	last := node.qosLast.Load()
	if last != 0 && now-time.Duration(last) < qosReportWindow {
		return
	}
	stamp := int64(now)
	if stamp == 0 {
		stamp = 1
	}
	if !node.qosLast.CompareAndSwap(last, stamp) {
		return
	}
	event := av.Event{
		Type:     av.EventQoS,
		StreamID: qosStreamID(&msg.message),
		Reason:   node.name,
		Metadata: av.QoSMetadata(time.Since(msg.enqueuedAt)-node.policy.MaxLatency, true),
	}
	g.observe(event, node.counters)
}

func qosStreamID(msg *Message) av.StreamID {
	switch {
	case msg == nil:
		return ""
	case msg.Packet != nil:
		return msg.Packet.StreamID
	case msg.Frame != nil:
		return msg.Frame.StreamID
	case msg.Event != nil:
		return msg.Event.StreamID
	default:
		return ""
	}
}

// releaseSlot drops the references a slot holds and returns it to the free
// list, handing it straight on to the oldest producer waiting for one.
func (n *bufferedNode) releaseSlot(slot *bufferedMessage) {
	if slot == nil {
		return
	}
	if slot.bytes != 0 {
		n.queuedBytes.Add(-slot.bytes)
		slot.bytes = 0
	}
	slot.release()
	n.free.pushWait(slot)
	// One load: published after the push, it pairs with a waiter's count
	// published before its join, so a producer that joined meanwhile is not
	// missed.
	if slotWaiters(n.waiting.Load()) > 0 {
		n.pumpSlots()
	}
}

// slotPlanes is the plane capacity each slot reserves up front, enough for
// planar video and audio; a frame with more planes grows its slot once.
const slotPlanes = 4

// bind queues src by reference: the slot copies the message header (packet,
// frame with its planes and Video or Audio header, or event) and takes a
// reference to every payload buffer, so the node reads the same bytes as
// every sibling branch and the producer may release its own reference as
// soon as Emit returns. Nothing is copied but the header.
func (m *bufferedMessage) bind(src *Message, policy BufferPolicy) error {
	if src == nil {
		return ErrNilMessage
	}
	if policy.MaxLatency > 0 {
		m.enqueuedAt = time.Now()
	}
	m.message.Kind = src.Kind
	switch src.Kind {
	case MessagePacket:
		if src.Packet == nil {
			return nil
		}
		m.packet = *src.Packet
		m.packet.Payload.Retain()
		m.message.Packet = &m.packet
	case MessageFrame:
		if src.Frame == nil {
			return nil
		}
		m.frame = *src.Frame
		if src.Frame.Audio != nil {
			m.audio = *src.Frame.Audio
			m.frame.Audio = &m.audio
		}
		if src.Frame.Video != nil {
			m.video = *src.Frame.Video
			m.frame.Video = &m.video
		}
		m.planes = append(m.planes[:0], src.Frame.Planes...)
		for i := range m.planes {
			m.planes[i].Buffer.Retain()
		}
		m.frame.Planes = m.planes
		m.message.Frame = &m.frame
	case MessageEvent:
		if src.Event == nil {
			return nil
		}
		m.event = *src.Event
		m.message.Event = &m.event
	}
	return nil
}

// release drops the payload references the slot holds and clears it for the
// next message, keeping its plane capacity.
func (m *bufferedMessage) release() {
	m.packet.Payload.Release()
	for i := range m.planes {
		m.planes[i].Buffer.Release()
	}
	planes := m.planes[:0]
	*m = bufferedMessage{planes: planes}
}

// inflight counts queued, not-yet-handled messages. It replaces a WaitGroup
// because an increment from zero may race the drain wait (Inject from another
// goroutine, or a stage emitting from a goroutine of its own), which the
// WaitGroup contract forbids. The drain wait polls nothing: the decrement that
// reaches zero leaves a token in idle, and the waiter rechecks the count.
type inflight struct {
	n    atomic.Int64
	idle chan struct{} // capacity 1
}

func (f *inflight) add() { f.n.Add(1) }

func (f *inflight) done() {
	if f.n.Add(-1) == 0 {
		select {
		case f.idle <- struct{}{}:
		default:
		}
	}
}

// wait blocks until the count is zero or stop is closed, reporting whether
// it drained.
func (f *inflight) wait(stop <-chan struct{}) bool {
	for f.n.Load() != 0 {
		select {
		case <-f.idle:
		case <-stop:
			return false
		}
	}
	return true
}
