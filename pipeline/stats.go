package pipeline

import (
	"sync"
	"sync/atomic"

	"github.com/thesyncim/goav/av"
)

// nodeCounters holds one node's per-message counts. The numeric fields are
// updated lock-free on the hot path; lastEvent/dropReasons are cold and guarded
// by the owning runner's coldStats mutex. Each node owns a stable *nodeCounters
// for the life of the graph, so the hot path can capture the pointer once (under
// the topology lock it already holds) and increment without further locking.
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

	// cold: guarded by coldStats.mu.
	lastEvent        av.Event
	lastEventPresent bool
	dropReasons      map[DropPolicy]uint64
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
// a cold-path call (Stats/Snapshot). names and nodes are indexed in parallel; a
// nil node entry is skipped.
func snapshotStats(cold *coldStats, names []string, nodes []*nodeCounters) GraphStats {
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
	if len(n.dropReasons) > 0 {
		stats.DropReasons = make(map[DropPolicy]uint64, len(n.dropReasons))
		for key, value := range n.dropReasons {
			stats.DropReasons[key] = value
		}
	}
	return stats
}
