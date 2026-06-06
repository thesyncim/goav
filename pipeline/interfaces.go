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

type NodeRef string

func Node(name string) NodeRef {
	return NodeRef(name)
}

func (r NodeRef) String() string {
	return string(r)
}

type Link struct {
	From NodeRef
	To   NodeRef
}

type RoutePolicy string

const (
	RouteAll      RoutePolicy = "all"
	RouteByStream RoutePolicy = "by_stream"
	RouteByEvent  RoutePolicy = "by_event"
	RouteByLabel  RoutePolicy = "by_label"
)

type Route struct {
	From   NodeRef
	To     []NodeRef
	Policy RoutePolicy
	Label  string
}

type Graph interface {
	AddSource(Source, BufferPolicy) (NodeRef, error)
	AddStage(Stage, BufferPolicy) (NodeRef, error)
	AddSink(Sink, BufferPolicy) (NodeRef, error)
	Link(Link) error
	Route(Route) error
	Spec() Spec
	Run(context.Context) error
	Events() <-chan av.Event
	Close() error
}

type Factory interface {
	NewGraph(context.Context, GraphConfig) (Graph, error)
}

type GraphConfig struct {
	Name          string
	Realtime      bool
	Buffer        BufferPolicy
	EventCapacity int
	Metadata      av.Metadata
}
