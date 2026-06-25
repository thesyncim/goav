package goav

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/component"
)

type (
	Message = component.Message
	Emit    = component.Emit
	Packet  = av.Packet
	Frame   = av.Frame
	Event   = av.Event
	Stream  = av.Stream
)

var (
	PacketFunc = component.PacketFunc
	FrameFunc  = component.FrameFunc
	EventFunc  = component.EventFunc
	SinkFunc   = component.SinkFunc
)
