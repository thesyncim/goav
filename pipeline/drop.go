package pipeline

import "github.com/thesyncim/goav/av"

type bufferAction uint8

const (
	bufferAdmit bufferAction = iota
	bufferDropIncoming
	bufferDropOldest
	bufferBackpressure
)

type dropController struct {
	policy            BufferPolicy
	droppingUntilSync bool
}

func newDropController(policy BufferPolicy) dropController {
	if policy.Drop == "" {
		policy.Drop = DropNever
	}
	return dropController{policy: policy}
}

func (c *dropController) decide(used int, msg *Message) bufferAction {
	capacity := c.policy.Capacity
	if capacity <= 0 {
		return bufferAdmit
	}

	if c.droppingUntilSync {
		if messageIsSync(msg) {
			c.droppingUntilSync = false
			if used >= capacity {
				return bufferDropOldest
			}
			return bufferAdmit
		}
		return bufferDropIncoming
	}

	if used < capacity {
		return bufferAdmit
	}

	switch c.policy.Drop {
	case "", DropNever:
		return bufferBackpressure
	case DropOldest:
		return bufferDropOldest
	case DropNewest:
		return bufferDropIncoming
	case DropUntilSync:
		if messageIsSync(msg) {
			return bufferDropOldest
		}
		c.droppingUntilSync = true
		return bufferDropIncoming
	case DropNonKeyVideo:
		if messageIsNonKeyVideo(msg) {
			return bufferDropIncoming
		}
		return bufferBackpressure
	default:
		return bufferBackpressure
	}
}

func messageIsSync(msg *Message) bool {
	if msg == nil {
		return false
	}
	switch msg.Kind {
	case MessagePacket:
		if msg.Packet == nil {
			return false
		}
		if msg.Packet.Keyframe {
			return true
		}
		return packetMediaType(msg.Packet) == av.MediaAudio
	case MessageFrame:
		return msg.Frame != nil && msg.Frame.Type != av.MediaVideo
	case MessageEvent:
		return eventIsSync(msg.Event)
	default:
		return false
	}
}

func messageIsNonKeyVideo(msg *Message) bool {
	if msg == nil {
		return false
	}
	switch msg.Kind {
	case MessagePacket:
		if msg.Packet == nil {
			return false
		}
		return !msg.Packet.Keyframe && packetMediaType(msg.Packet) == av.MediaVideo
	case MessageFrame:
		return msg.Frame != nil && msg.Frame.Type == av.MediaVideo
	default:
		return false
	}
}

func packetMediaType(packet *av.Packet) av.MediaType {
	if packet == nil || packet.Metadata == nil {
		return av.MediaUnknown
	}
	return av.MediaType(packet.Metadata[av.MetadataMediaType])
}

func eventIsSync(event *av.Event) bool {
	if event == nil {
		return false
	}
	switch event.Type {
	case av.EventCodecChanged, av.EventDiscontinuity, av.EventEndOfStream:
		return true
	default:
		return false
	}
}
