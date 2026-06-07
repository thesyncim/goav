package pipeline

import "github.com/thesyncim/goav/av"

func (s *GraphStats) observeMessage(msg *Message) {
	if msg == nil {
		return
	}
	s.Messages++
	switch msg.Kind {
	case MessagePacket:
		if msg.Packet != nil {
			s.Packets++
		}
	case MessageFrame:
		if msg.Frame != nil {
			s.Frames++
		}
	case MessageEvent:
		if msg.Event != nil {
			s.Events++
			if s.EventsByType == nil {
				s.EventsByType = make(map[av.EventType]uint64)
			}
			s.EventsByType[msg.Event.Type]++
			s.LastEvent = *msg.Event
			s.LastEventPresent = true
		}
	}
}

func (s *GraphStats) observeDrop(policy DropPolicy) {
	s.Dropped++
	if s.DropReasons == nil {
		s.DropReasons = make(map[DropPolicy]uint64)
	}
	if policy == "" {
		policy = DropNever
	}
	s.DropReasons[policy]++
}

func (s *NodeStats) observeDrop(policy DropPolicy) {
	s.Dropped++
	if s.DropReasons == nil {
		s.DropReasons = make(map[DropPolicy]uint64)
	}
	if policy == "" {
		policy = DropNever
	}
	s.DropReasons[policy]++
}

func (s *NodeStats) observeIn(msg *Message) {
	s.InMessages++
	s.observeEvent(msg)
	switch msg.Kind {
	case MessagePacket:
		if msg.Packet != nil {
			s.InPackets++
		}
	case MessageFrame:
		if msg.Frame != nil {
			s.InFrames++
		}
	case MessageEvent:
		if msg.Event != nil {
			s.InEvents++
		}
	}
}

func (s *NodeStats) observeOut(msg *Message) {
	s.OutMessages++
	s.observeEvent(msg)
	switch msg.Kind {
	case MessagePacket:
		if msg.Packet != nil {
			s.OutPackets++
		}
	case MessageFrame:
		if msg.Frame != nil {
			s.OutFrames++
		}
	case MessageEvent:
		if msg.Event != nil {
			s.OutEvents++
		}
	}
}

func (s *NodeStats) observeEvent(msg *Message) {
	if msg.Kind != MessageEvent || msg.Event == nil {
		return
	}
	s.LastEvent = *msg.Event
	s.LastEventPresent = true
}

func cloneGraphStats(stats GraphStats) GraphStats {
	cloned := stats
	if stats.EventsByType != nil {
		cloned.EventsByType = make(map[av.EventType]uint64, len(stats.EventsByType))
		for key, value := range stats.EventsByType {
			cloned.EventsByType[key] = value
		}
	}
	if stats.DropReasons != nil {
		cloned.DropReasons = make(map[DropPolicy]uint64, len(stats.DropReasons))
		for key, value := range stats.DropReasons {
			cloned.DropReasons[key] = value
		}
	}
	if stats.Nodes != nil {
		cloned.Nodes = make(map[string]NodeStats, len(stats.Nodes))
		for name, nodeStats := range stats.Nodes {
			cloned.Nodes[name] = cloneNodeStats(nodeStats)
		}
	}
	return cloned
}

func cloneGraphStatsWithNodes(stats GraphStats, names []string, nodes []NodeStats) GraphStats {
	cloned := cloneGraphStats(stats)
	count := len(names)
	if len(nodes) < count {
		count = len(nodes)
	}
	if count == 0 {
		return cloned
	}
	cloned.Nodes = make(map[string]NodeStats, count)
	for i := 0; i < count; i++ {
		if names[i] == "" {
			continue
		}
		cloned.Nodes[names[i]] = cloneNodeStats(nodes[i])
	}
	if len(cloned.Nodes) == 0 {
		cloned.Nodes = nil
	}
	return cloned
}

func cloneNodeStats(stats NodeStats) NodeStats {
	cloned := stats
	if stats.DropReasons != nil {
		cloned.DropReasons = make(map[DropPolicy]uint64, len(stats.DropReasons))
		for key, value := range stats.DropReasons {
			cloned.DropReasons[key] = value
		}
	}
	return cloned
}
