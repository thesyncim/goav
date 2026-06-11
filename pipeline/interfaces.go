package pipeline

import (
	"context"
	"time"

	"github.com/thesyncim/goav/av"
)

// MessageKind says which payload field of a Message is set.
type MessageKind string

// The message kinds: exactly one payload per message.
const (
	MessagePacket MessageKind = "packet"
	MessageFrame  MessageKind = "frame"
	MessageEvent  MessageKind = "event"
)

// Message is the one unit that flows between nodes: a packet, a frame, or an
// event, tagged by Kind. Exactly one payload pointer is non-nil.
type Message struct {
	Kind   MessageKind
	Packet *av.Packet
	Frame  *av.Frame
	Event  *av.Event
}

// Reset clears the message for reuse.
func (m *Message) Reset() {
	m.Kind = ""
	m.Packet = nil
	m.Frame = nil
	m.Event = nil
}

// Scratch is reusable per-worker working memory for nodes that assemble
// messages and event bursts without allocating on the hot path.
type Scratch struct {
	Message Message
	Events  []av.Event
}

// Reset clears the scratch for reuse, keeping allocated capacity.
func (s *Scratch) Reset() {
	s.Message.Reset()
	for i := range s.Events {
		s.Events[i].Reset()
	}
	s.Events = s.Events[:0]
}

// DropPolicy says what a full node queue does with new messages — and doubles
// as the recorded reason on drop counters (Stats DropReasons).
type DropPolicy string

const (
	// DropNever refuses to shed: a full queue fails the emit with
	// ErrBackpressure instead of dropping.
	DropNever DropPolicy = "never"
	// DropOldest sheds the oldest queued message to admit the new one —
	// keep-latest semantics for realtime previews.
	DropOldest DropPolicy = "oldest"
	// DropNewest sheds the incoming message when the queue is full.
	DropNewest DropPolicy = "newest"
	// DropUntilSync sheds until a sync point (keyframe) arrives, so a consumer
	// resumes on decodable media.
	DropUntilSync DropPolicy = "until_sync"
	// DropNonKeyVideo sheds non-keyframe video first, preserving sync points
	// and non-video media.
	DropNonKeyVideo DropPolicy = "non_key_video"
	// DropBlock never drops: a full queue blocks the producer until the consumer
	// drains (true backpressure), instead of erroring. Source paces to the
	// slowest blocking consumer; no pipeline teardown.
	DropBlock DropPolicy = "block"
	// DropStale is the reason recorded when a buffered message is shed because it
	// waited longer than BufferPolicy.MaxLatency (orthogonal to the drop policy:
	// any policy can also shed stale messages when MaxLatency is set).
	DropStale DropPolicy = "stale"
	// DropOverflow is the reason recorded when a buffered message is shed because
	// admitting it would exceed the node's BufferPolicy.MaxBytes byte budget
	// (orthogonal to the drop policy, like DropStale).
	DropOverflow DropPolicy = "overflow"
	// DropSync is the reason recorded for messages a node sheds internally to
	// stay time-aligned (a PTS-sync join dropping stale frames to catch up).
	// These drops happen inside the node, not in its queue, so they reach the
	// counters through the optional DropReporter capability at snapshot time.
	DropSync DropPolicy = "sync"
)

// DropReporter is an optional Source/Stage/Sink capability: a node that sheds
// messages internally (rather than through its queue policy) reports its
// running total, and the runners fold it into the node's NodeStats.Dropped
// under DropSync at snapshot time. DroppedMessages is called concurrently
// with the node's hot path, so implementations must read an atomic counter.
type DropReporter interface {
	DroppedMessages() uint64
}

// BufferPolicy configures one node's queue: capacity, overflow behavior
// (Drop), staleness and byte budgets, and the copy bounds buffered execution
// uses to keep branch payloads isolated. The zero value means direct
// (unbuffered) execution.
type BufferPolicy struct {
	Capacity   int
	Drop       DropPolicy
	MaxLatency time.Duration
	// MaxBytes caps the total queued payload bytes for a buffered node; admitting
	// a message that would exceed it sheds the message (DropOverflow) instead of
	// queuing. Zero disables byte-budget shedding (the default — no per-message
	// byte accounting runs on the hot path).
	MaxBytes int64
	// CopyPacketBytes bounds graph-owned packet payload copies for buffered
	// execution when a packet buffer is not immutable.
	CopyPacketBytes int
	// CopyFrameBytes bounds graph-owned frame plane copies for buffered
	// execution when frame plane buffers are not immutable.
	CopyFrameBytes int
	// CopyAlways makes buffered execution copy every non-empty payload into
	// graph-owned backing, including payloads declared av.BufferImmutable
	// (which are otherwise shared by reference). Copies still require
	// CopyPacketBytes/CopyFrameBytes capacity: a payload that cannot be copied
	// is refused with ErrBufferedMessageUnsafe instead of shared.
	CopyAlways bool
}

// IsDirect reports whether the policy means unbuffered execution: no
// capacity and no dropping mode.
func (p BufferPolicy) IsDirect() bool {
	return p.Capacity == 0 && (p.Drop == "" || p.Drop == DropNever)
}

// Emitter is how a source or stage hands a message downstream. Emit returns
// ErrBackpressure when a strict queue is full and ErrClosed once the graph
// has shut down.
type Emitter interface {
	Emit(context.Context, *Message) error
}

// Source is a graph entry node: Start runs the producing loop, emitting
// until the media ends or the context is cancelled. Optional capabilities
// (ControllableSource, NodeDescriber, DropReporter) are discovered by type
// assertion.
type Source interface {
	Name() string
	Start(context.Context, Emitter) error
	Close() error
}

// Stage is a graph interior node: Handle processes one message and emits any
// output downstream. A stage is called serially per node — no internal
// locking is needed for per-message state.
type Stage interface {
	Name() string
	Handle(context.Context, *Message, Emitter) error
	Close() error
}

// Sink is a graph terminal node: Handle consumes one delivered message.
// Returning an error fails the task.
type Sink interface {
	Name() string
	Handle(context.Context, *Message) error
	Close() error
}

// NodeDescriber is an optional node capability: a node that can describe
// itself contributes its details (component, codec, format) to Spec.
type NodeDescriber interface {
	DescribeNode() NodeSpec
}

// NodeRef is the graph-unique name of an added node, used to connect routes
// and look up stats.
type NodeRef string

// String returns the node name.
func (r NodeRef) String() string {
	return string(r)
}

// RoutePolicy says which messages a route forwards.
type RoutePolicy string

// The route policies: everything, one stream's messages, or one event type.
const (
	RouteAll      RoutePolicy = "all"
	RouteByStream RoutePolicy = "by_stream"
	RouteByEvent  RoutePolicy = "by_event"
)

// Route is one edge declaration: messages leaving From reach every To node,
// filtered by Policy (Label carries the stream id or event type).
type Route struct {
	From   string
	To     []string
	Policy RoutePolicy
	Label  string
}

// ByStream narrows the route to one stream's messages.
func (r Route) ByStream(stream av.StreamID) Route {
	r.Policy = RouteByStream
	r.Label = string(stream)
	return r
}

// ByEvent narrows the route to one event type.
func (r Route) ByEvent(event av.EventType) Route {
	r.Policy = RouteByEvent
	r.Label = string(event)
	return r
}

// NodePauser is implemented by graphs that can pause/resume delivery to a single
// node (the buffered runner). It is an optional capability discovered by type
// assertion, so it does not widen the Graph interface for every implementer.
type NodePauser interface {
	SetNodePaused(ref NodeRef, paused bool) error
}

// Graph is the executable node graph: add nodes, connect routes, run, and
// inspect. Mutations (Connect/Disconnect/Remove) are atomic with respect to
// the running data plane — the runtime swaps routing snapshots, never locks
// the message path.
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

// GraphStats is a point-in-time counter snapshot for the whole graph, with
// per-node detail under Nodes. Dropped counts deliberate sheds, keyed by
// reason in DropReasons.
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
	Nodes            map[string]NodeStats
}

// NodeStats is one node's counter snapshot: messages in and out by kind,
// drops by reason, and the last event seen.
type NodeStats struct {
	InMessages       uint64
	InPackets        uint64
	InFrames         uint64
	InEvents         uint64
	OutMessages      uint64
	OutPackets       uint64
	OutFrames        uint64
	OutEvents        uint64
	Dropped          uint64
	DropReasons      map[DropPolicy]uint64
	LastEvent        av.Event
	LastEventPresent bool
}

// GraphConfig configures a new graph: its name, realtime pacing, the default
// node buffer policy, and the event channel capacity.
type GraphConfig struct {
	Name          string
	Realtime      bool
	Buffer        BufferPolicy
	EventCapacity int
	Metadata      av.Metadata
}
