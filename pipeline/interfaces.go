package pipeline

import (
	"context"
	"time"

	"github.com/thesyncim/goav/av"
)

type MessageKind string

const (
	MessagePacket MessageKind = "packet"
	MessageFrame  MessageKind = "frame"
	MessageEvent  MessageKind = "event"
)

type Message struct {
	Kind   MessageKind
	Packet *av.Packet
	Frame  *av.Frame
	Event  *av.Event
}

type DropPolicy string

const (
	DropNever       DropPolicy = "never"
	DropOldest      DropPolicy = "oldest"
	DropNewest      DropPolicy = "newest"
	DropUntilSync   DropPolicy = "until_sync"
	DropNonKeyVideo DropPolicy = "non_key_video"
)

type BufferPolicy struct {
	Capacity      int
	Drop          DropPolicy
	TargetLatency time.Duration
	MaxLatency    time.Duration
}

type Emitter interface {
	Emit(context.Context, Message) error
}

type Source interface {
	Name() string
	Start(context.Context, Emitter) error
	Close() error
}

type Stage interface {
	Name() string
	Handle(context.Context, Message, Emitter) error
	Close() error
}

type Sink interface {
	Name() string
	Handle(context.Context, Message) error
	Close() error
}

type PadRef struct {
	Node string
	Pad  string
}

type Link struct {
	From PadRef
	To   PadRef
}

type RoutePolicy string

const (
	RouteAll      RoutePolicy = "all"
	RouteByStream RoutePolicy = "by_stream"
	RouteByEvent  RoutePolicy = "by_event"
	RouteByLabel  RoutePolicy = "by_label"
)

type Route struct {
	From   PadRef
	To     []PadRef
	Policy RoutePolicy
	Label  string
}

type Graph interface {
	AddSource(Source, BufferPolicy) (PadRef, error)
	AddStage(Stage, BufferPolicy) (PadRef, error)
	AddSink(Sink, BufferPolicy) (PadRef, error)
	Link(Link) error
	Route(Route) error
	Run(context.Context) error
	Events() <-chan av.Event
	Close() error
}

type Factory interface {
	NewGraph(context.Context, GraphConfig) (Graph, error)
}

type GraphConfig struct {
	Name     string
	Realtime bool
	Buffer   BufferPolicy
	Metadata av.Metadata
}
