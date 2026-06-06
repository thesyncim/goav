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
	return cloned
}
