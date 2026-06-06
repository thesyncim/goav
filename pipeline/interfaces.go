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

func (m *Message) Reset() {
	m.Kind = ""
	m.Packet = nil
	m.Frame = nil
	m.Event = nil
}

type Scratch struct {
	Message Message
	Events  []av.Event
}

func (s *Scratch) Reset() {
	s.Message.Reset()
	for i := range s.Events {
		s.Events[i].Reset()
	}
	s.Events = s.Events[:0]
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
	// CopyPacketBytes bounds graph-owned packet payload copies for buffered
	// execution when a packet buffer is not immutable.
	CopyPacketBytes int
	// CopyFrameBytes bounds graph-owned frame plane copies for buffered
	// execution when frame plane buffers are not immutable.
	CopyFrameBytes int
}

func (p BufferPolicy) IsDirect() bool {
	return p.Capacity == 0 && (p.Drop == "" || p.Drop == DropNever)
}

type Emitter interface {
	Emit(context.Context, *Message) error
}

type Source interface {
	Name() string
	Start(context.Context, Emitter) error
	Close() error
}

type Stage interface {
	Name() string
	Handle(context.Context, *Message, Emitter) error
	Close() error
}

type Sink interface {
	Name() string
	Handle(context.Context, *Message) error
	Close() error
}

type NodeDescriber interface {
	DescribeNode() NodeSpec
}

type NodeRef string

func (r NodeRef) String() string {
	return string(r)
}

type RoutePolicy string

const (
	RouteAll      RoutePolicy = "all"
	RouteByStream RoutePolicy = "by_stream"
	RouteByEvent  RoutePolicy = "by_event"
)

type Route struct {
	From   string
	To     []string
	Policy RoutePolicy
	Label  string
}

func (r Route) ByStream(stream av.StreamID) Route {
	r.Policy = RouteByStream
	r.Label = string(stream)
	return r
}

func (r Route) ByEvent(event av.EventType) Route {
	r.Policy = RouteByEvent
	r.Label = string(event)
	return r
}

type Graph interface {
	AddSource(Source, BufferPolicy) (NodeRef, error)
	AddStage(Stage, BufferPolicy) (NodeRef, error)
	AddSink(Sink, BufferPolicy) (NodeRef, error)
	Connect(Route) error
	Disconnect(Route) error
	Remove(NodeRef) error
	Spec() Spec
	Run(context.Context) error
	Events() <-chan av.Event
	Stats() GraphStats
	Close() error
}

type GraphStats struct {
	Messages         uint64
	Packets          uint64
	Frames           uint64
	Events           uint64
	EventsByType     map[av.EventType]uint64
	Dropped          uint64
	DropReasons      map[DropPolicy]uint64
	Delivered        uint64
	LastEvent        av.Event
	LastEventPresent bool
}

type GraphConfig struct {
	Name          string
	Realtime      bool
	Buffer        BufferPolicy
	EventCapacity int
	Metadata      av.Metadata
}
