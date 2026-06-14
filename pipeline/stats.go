package pipeline

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/thesyncim/goav/av"
)

const counterShards = 64

// nodeCounters holds one node's per-message counts. The numeric fields are
// updated lock-free on the hot path; lastEvent/dropReasons are cold and guarded
// by the owning runner's coldStats mutex. Each node owns a stable *nodeCounters
// for the life of the graph, so the hot path can capture the pointer once from a
// routing snapshot and increment without further locking. Hot counters are
// sharded by message pointer to avoid one cache line serializing concurrent
// producers that hit the same source or fanout node.
type nodeCounters struct {
	shards  [counterShards]nodeCounterShard
	dropped atomic.Uint64

	// cold: guarded by coldStats.mu.
	lastEvent        av.Event
	lastEventPresent bool
	dropReasons      map[DropPolicy]uint64
}

type nodeCounterShard struct {
	inMessages  atomic.Uint64
	inPackets   atomic.Uint64
	inFrames    atomic.Uint64
	inEvents    atomic.Uint64
	outMessages atomic.Uint64
	outPackets  atomic.Uint64
	outFrames   atomic.Uint64
	outEvents   atomic.Uint64
}

// coldStats guards the rarely-written event/drop detail for a whole graph
// (per-graph maps plus the per-node cold fields). It is never taken on the
// packet/frame hot path.
type coldStats struct {
	mu               sync.Mutex
	eventsByType     map[av.EventType]uint64
	dropReasons      map[DropPolicy]uint64
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
	shard := n.shard(msg)
	shard.outMessages.Add(1)
	switch msg.Kind {
	case MessagePacket:
		if msg.Packet != nil {
			shard.outPackets.Add(1)
		}
	case MessageFrame:
		if msg.Frame != nil {
			shard.outFrames.Add(1)
		}
	case MessageEvent:
		if msg.Event != nil {
			shard.outEvents.Add(1)
			cold.recordOutEvent(n, msg.Event)
		}
	}
}

// observeIn records a message delivered into node n.
func observeIn(n *nodeCounters, msg *Message, cold *coldStats) {
	if n == nil || msg == nil {
		return
	}
	shard := n.shard(msg)
	shard.inMessages.Add(1)
	switch msg.Kind {
	case MessagePacket:
		if msg.Packet != nil {
			shard.inPackets.Add(1)
		}
	case MessageFrame:
		if msg.Frame != nil {
			shard.inFrames.Add(1)
		}
	case MessageEvent:
		if msg.Event != nil {
			shard.inEvents.Add(1)
			cold.recordNodeEvent(n, msg.Event)
		}
	}
}

func (n *nodeCounters) shard(msg *Message) *nodeCounterShard {
	h := uintptr(unsafe.Pointer(msg)) >> 4
	h ^= h >> 7
	h ^= h >> 13
	return &n.shards[h&(counterShards-1)]
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

func (c *coldStats) recordDrop(n *nodeCounters, policy DropPolicy) {
	if policy == "" {
		policy = DropNever
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dropReasons == nil {
		c.dropReasons = make(map[DropPolicy]uint64)
	}
	c.dropReasons[policy]++
	if n != nil {
		if n.dropReasons == nil {
			n.dropReasons = make(map[DropPolicy]uint64)
		}
		n.dropReasons[policy]++
	}
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
		for key, value := range cold.eventsByType {
			stats.EventsByType[key] = value
		}
	}
	if len(cold.dropReasons) > 0 {
		stats.DropReasons = make(map[DropPolicy]uint64, len(cold.dropReasons))
		for key, value := range cold.dropReasons {
			stats.DropReasons[key] = value
		}
	}
	stats.LastEvent = cold.lastEvent
	stats.LastEventPresent = cold.lastEventPresent

	count := min(len(names), len(nodes))
	var nodeStats map[string]NodeStats
	for i := 0; i < count; i++ {
		if nodes[i] == nil {
			continue
		}
		ns := nodes[i].snapshotLocked()
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
	var stats NodeStats
	for i := range n.shards {
		shard := &n.shards[i]
		stats.InMessages += shard.inMessages.Load()
		stats.InPackets += shard.inPackets.Load()
		stats.InFrames += shard.inFrames.Load()
		stats.InEvents += shard.inEvents.Load()
		stats.OutMessages += shard.outMessages.Load()
		stats.OutPackets += shard.outPackets.Load()
		stats.OutFrames += shard.outFrames.Load()
		stats.OutEvents += shard.outEvents.Load()
	}
	stats.Dropped = n.dropped.Load()
	stats.LastEvent = n.lastEvent
	stats.LastEventPresent = n.lastEventPresent
	if len(n.dropReasons) > 0 {
		stats.DropReasons = make(map[DropPolicy]uint64, len(n.dropReasons))
		for key, value := range n.dropReasons {
			stats.DropReasons[key] = value
		}
	}
	return stats
}
