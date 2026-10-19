package pipeline

import (
	"maps"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/goav/av"
)

// nodeCounters holds one node's per-message counts. The numeric fields and
// the drop reasons are updated lock-free on the hot path; lastEvent is cold
// and guarded by the owning runner's coldStats mutex. Each node owns a
// stable *nodeCounters for the life of the graph, so the hot path reaches it
// through the routing snapshot and increments without locking.
type nodeCounters struct {
	inMessages  atomic.Uint64
	inPackets   atomic.Uint64
	inFrames    atomic.Uint64
	inEvents    atomic.Uint64
	outMessages atomic.Uint64
	outPackets  atomic.Uint64
	outFrames   atomic.Uint64
	outEvents   atomic.Uint64
	dropped     atomic.Uint64
	drops       dropCounts

	// cold: guarded by coldStats.mu.
	lastEvent        av.Event
	lastEventPresent bool
}

// coldStats guards the rarely-written event detail for a whole graph (the
// per-type event counts and last events, graph-wide and per node). It is
// never taken on the packet/frame path, nor for a shed: drops count in
// atomics (dropCounts). drops holds the sheds no node owns (an observer
// event dropped at graph level).
type coldStats struct {
	mu               sync.Mutex
	eventsByType     map[av.EventType]uint64
	drops            dropCounts
	lastEvent        av.Event
	lastEventPresent bool
}

// observeOut records a message emitted out of node n toward its routes. Only the
// emitting node's own atomic counters are touched on the hot path; the graph-wide
// totals are derived by summing per-node counters at snapshot time, so there is no
// shared aggregate counter to contend on across cores.
func observeOut(n *nodeCounters, msg *Message, cold *coldStats) {
	if n == nil || msg == nil {
		return
	}
	n.outMessages.Add(1)
	switch msg.Kind {
	case MessagePacket:
		if msg.Packet != nil {
			n.outPackets.Add(1)
		}
	case MessageFrame:
		if msg.Frame != nil {
			n.outFrames.Add(1)
		}
	case MessageEvent:
		if msg.Event != nil {
			n.outEvents.Add(1)
			cold.recordOutEvent(n, msg.Event)
		}
	}
}

// observeIn records a message delivered into node n.
func observeIn(n *nodeCounters, msg *Message, cold *coldStats) {
	if n == nil || msg == nil {
		return
	}
	n.inMessages.Add(1)
	switch msg.Kind {
	case MessagePacket:
		if msg.Packet != nil {
			n.inPackets.Add(1)
		}
	case MessageFrame:
		if msg.Frame != nil {
			n.inFrames.Add(1)
		}
	case MessageEvent:
		if msg.Event != nil {
			n.inEvents.Add(1)
			cold.recordNodeEvent(n, msg.Event)
		}
	}
}

// observeDrop records a dropped message at node n.
func observeDrop(n *nodeCounters, policy DropPolicy, cold *coldStats) {
	if n != nil {
		n.dropped.Add(1)
	}
	cold.recordDrop(n, policy)
}

func (c *coldStats) recordOutEvent(n *nodeCounters, ev *av.Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.eventsByType == nil {
		c.eventsByType = make(map[av.EventType]uint64)
	}
	c.eventsByType[ev.Type]++
	c.lastEvent = *ev
	c.lastEventPresent = true
	if n != nil {
		n.lastEvent = *ev
		n.lastEventPresent = true
	}
}

func (c *coldStats) recordNodeEvent(n *nodeCounters, ev *av.Event) {
	if n == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	n.lastEvent = *ev
	n.lastEventPresent = true
}

// recordDrop counts one shed under its reason at node n (nil: graph level).
// Every reason the pipeline records has a lock-free counter, so a node that
// sheds on every message under overload takes no lock; only a custom
// BufferPolicy.Drop string falls back to the cold map.
func (c *coldStats) recordDrop(n *nodeCounters, policy DropPolicy) {
	if policy == "" {
		policy = DropNever
	}
	d := &c.drops
	if n != nil {
		d = &n.drops
	}
	if i := dropReasonSlot(policy); i >= 0 {
		d.known[i].Add(1)
		return
	}
	c.recordCustomDrop(d, policy)
}

// recordCustomDrop counts a shed under a reason the pipeline does not know (a
// custom BufferPolicy.Drop string), in the cold map.
func (c *coldStats) recordCustomDrop(d *dropCounts, policy DropPolicy) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if d.other == nil {
		d.other = make(map[DropPolicy]uint64)
	}
	d.other[policy]++
}

// dropReasons lists the reasons the pipeline records itself, in the order of
// dropCounts.known.
var dropReasons = [...]DropPolicy{
	DropNever, DropOldest, DropNewest, DropUntilSync, DropNonKeyVideo, DropBlock,
	DropStale, DropOverflow, DropSync, DropFlush, DropObserver,
}

func dropReasonSlot(policy DropPolicy) int {
	switch policy {
	case DropNever:
		return 0
	case DropOldest:
		return 1
	case DropNewest:
		return 2
	case DropUntilSync:
		return 3
	case DropNonKeyVideo:
		return 4
	case DropBlock:
		return 5
	case DropStale:
		return 6
	case DropOverflow:
		return 7
	case DropSync:
		return 8
	case DropFlush:
		return 9
	case DropObserver:
		return 10
	default:
		return -1
	}
}

// dropCounts is one node's (or the graph's) sheds by reason: an atomic per
// known reason, and other for custom reasons, guarded by coldStats.mu.
type dropCounts struct {
	known [len(dropReasons)]atomic.Uint64
	other map[DropPolicy]uint64
}

// addTo adds the counts into reasons, allocating it on the first nonzero
// count. The caller holds coldStats.mu (for other).
func (d *dropCounts) addTo(reasons map[DropPolicy]uint64) map[DropPolicy]uint64 {
	for i := range d.known {
		if v := d.known[i].Load(); v > 0 {
			if reasons == nil {
				reasons = make(map[DropPolicy]uint64)
			}
			reasons[dropReasons[i]] += v
		}
	}
	for policy, v := range d.other {
		if reasons == nil {
			reasons = make(map[DropPolicy]uint64)
		}
		reasons[policy] += v
	}
	return reasons
}

// snapshotStats assembles the public GraphStats. The graph-wide totals are
// derived by summing each node's atomic counters (no shared hot counter to
// contend on); the cold maps and last-event are read under the cold mutex. It is
// a cold-path call (Stats/Snapshot). names, nodes, and reported are indexed in
// parallel; a nil node entry is skipped. reported carries each node's
// self-reported internal drops (the optional DropReporter capability, polled
// by the caller), folded into the node's Dropped under DropSync; a nil slice
// means no node reports.
func snapshotStats(cold *coldStats, names []string, nodes []*nodeCounters, reported []uint64) GraphStats {
	var stats GraphStats
	cold.mu.Lock()
	defer cold.mu.Unlock()
	if len(cold.eventsByType) > 0 {
		stats.EventsByType = make(map[av.EventType]uint64, len(cold.eventsByType))
		maps.Copy(stats.EventsByType, cold.eventsByType)
	}
	// Graph drop reasons: the sheds no node owns, plus every node's (summed
	// below from the same reads the node stats report).
	stats.DropReasons = cold.drops.addTo(nil)
	stats.LastEvent = cold.lastEvent
	stats.LastEventPresent = cold.lastEventPresent

	count := min(len(names), len(nodes))
	var nodeStats map[string]NodeStats
	for i := range count {
		if nodes[i] == nil {
			continue
		}
		ns := nodes[i].snapshotLocked()
		if len(ns.DropReasons) > 0 && stats.DropReasons == nil {
			stats.DropReasons = make(map[DropPolicy]uint64, len(ns.DropReasons))
		}
		for policy, v := range ns.DropReasons {
			stats.DropReasons[policy] += v
		}
		if i < len(reported) && reported[i] > 0 {
			ns.Dropped += reported[i]
			if ns.DropReasons == nil {
				ns.DropReasons = make(map[DropPolicy]uint64, 1)
			}
			ns.DropReasons[DropSync] += reported[i]
			if stats.DropReasons == nil {
				stats.DropReasons = make(map[DropPolicy]uint64, 1)
			}
			stats.DropReasons[DropSync] += reported[i]
		}
		// Graph totals: messages/packets/frames/events count emissions (node
		// "out"); delivered counts arrivals (node "in"); dropped sums per node.
		stats.Messages += ns.OutMessages
		stats.Packets += ns.OutPackets
		stats.Frames += ns.OutFrames
		stats.Events += ns.OutEvents
		stats.Delivered += ns.InMessages
		stats.Dropped += ns.Dropped
		if names[i] == "" {
			continue
		}
		if nodeStats == nil {
			nodeStats = make(map[string]NodeStats, count)
		}
		nodeStats[names[i]] = ns
	}
	stats.Nodes = nodeStats
	return stats
}

// reportedDrops polls a node component's optional DropReporter capability:
// the node's internal shed total at snapshot time. Exactly one of the three
// components is non-nil per node kind.
func reportedDrops(source Source, stage Stage, sink Sink) uint64 {
	var component any
	switch {
	case source != nil:
		component = source
	case stage != nil:
		component = stage
	case sink != nil:
		component = sink
	default:
		return 0
	}
	if reporter, ok := component.(DropReporter); ok {
		return reporter.DroppedMessages()
	}
	return 0
}

// snapshotLocked reads one node's counters into the public NodeStats. The caller
// must hold the owning coldStats mutex (cold fields are read here).
func (n *nodeCounters) snapshotLocked() NodeStats {
	stats := NodeStats{
		InMessages:       n.inMessages.Load(),
		InPackets:        n.inPackets.Load(),
		InFrames:         n.inFrames.Load(),
		InEvents:         n.inEvents.Load(),
		OutMessages:      n.outMessages.Load(),
		OutPackets:       n.outPackets.Load(),
		OutFrames:        n.outFrames.Load(),
		OutEvents:        n.outEvents.Load(),
		Dropped:          n.dropped.Load(),
		LastEvent:        n.lastEvent,
		LastEventPresent: n.lastEventPresent,
	}
	stats.DropReasons = n.drops.addTo(nil)
	return stats
}

// retiredCounts accumulates the counters of removed nodes whose index was
// reused, so the graph totals keep what they did. Guarded by topology.mu.
type retiredCounts struct {
	counters nodeCounters
	reported uint64
}

// fold adds a removed node's counts, drop reasons included, to the retired
// totals. cold guards the custom-reason maps.
func (r *retiredCounts) fold(n *nodeBase, cold *coldStats) {
	if c := n.counters; c != nil {
		r.counters.inMessages.Add(c.inMessages.Load())
		r.counters.inPackets.Add(c.inPackets.Load())
		r.counters.inFrames.Add(c.inFrames.Load())
		r.counters.inEvents.Add(c.inEvents.Load())
		r.counters.outMessages.Add(c.outMessages.Load())
		r.counters.outPackets.Add(c.outPackets.Load())
		r.counters.outFrames.Add(c.outFrames.Load())
		r.counters.outEvents.Add(c.outEvents.Load())
		r.counters.dropped.Add(c.dropped.Load())
		for i := range c.drops.known {
			r.counters.drops.known[i].Add(c.drops.known[i].Load())
		}
		if len(c.drops.other) > 0 {
			cold.mu.Lock()
			if r.counters.drops.other == nil {
				r.counters.drops.other = make(map[DropPolicy]uint64, len(c.drops.other))
			}
			for policy, v := range c.drops.other {
				r.counters.drops.other[policy] += v
			}
			cold.mu.Unlock()
		}
	}
	r.reported += reportedDrops(n.source, n.stage, n.sink)
}
