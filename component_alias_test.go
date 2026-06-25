package goav

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/component"
	sourcepkg "github.com/thesyncim/goav/source"
)

type (
	Message = component.Message
	Emit    = component.Emit
	Packet  = av.Packet
	Frame   = av.Frame
	Event   = av.Event
	Stream  = av.Stream

	SourceFunc  = sourcepkg.Func
	SourcePush  = sourcepkg.Push
	PushResult  = sourcepkg.Result
	StreamMatch = sourcepkg.StreamMatch
)

var (
	PacketFunc = component.PacketFunc
	FrameFunc  = component.FrameFunc
	EventFunc  = component.EventFunc
	SinkFunc   = component.SinkFunc

	MatchMedia    = sourcepkg.MatchMedia
	MatchCodec    = sourcepkg.MatchCodec
	MatchStreamID = sourcepkg.MatchStreamID
	MatchStream   = sourcepkg.MatchStream
)
