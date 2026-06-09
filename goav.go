// Package goav exposes the top-level runtime contracts for composing media
// inputs, chains, taps, branches, destinations, and tasks.
package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
)

type Packet = av.Packet
type Frame = av.Frame
type Event = av.Event
type Stream = av.Stream
type CodecDescriptor = codec.Descriptor
type CodecSettings = codec.CodecSettings
type DecodeConfig = codec.DecodeConfig
type EncodeConfig = codec.EncodeConfig
type DecodeResult = codec.DecodeResult
type EncodeResult = codec.EncodeResult
type Decoder = codec.Decoder
type Encoder = codec.Encoder
type DecoderFactory = codec.DecoderFactory
type EncoderFactory = codec.EncoderFactory
type TaskStats = pipeline.GraphStats
type BranchStats = pipeline.GraphStats

// NodeStats is the per-node counter view found in TaskStats.Nodes.
type NodeStats = pipeline.NodeStats

type MediaDomain string

const (
	DomainPacket MediaDomain = "packet"
	DomainFrame  MediaDomain = "frame"
	DomainEvent  MediaDomain = "event"
)

type MediaShape struct {
	Domain       MediaDomain
	MediaKind    av.MediaType
	StreamID     av.StreamID
	Codec        av.CodecID
	Format       av.FormatID
	Width        int
	Height       int
	PixelFormat  string
	SampleRate   int
	Channels     int
	SampleFormat string
	Realtime     bool
}

type ShapeSet []MediaShape

type TapInfo struct {
	Name      string
	MediaKind av.MediaType
	Domain    MediaDomain
	After     OperationKind
	Shape     MediaShape
	Node      pipeline.NodeRef
}

// TaskSnapshot is an immutable point-in-time view of a task's graph, taps, active
// runtime branches, and counters.
type TaskSnapshot struct {
	Spec         pipeline.Spec
	Stats        TaskStats
	Taps         []TapInfo
	Branches     []BranchSnapshot
	Destinations []DestinationSnapshot
}

// BranchSnapshot is an immutable point-in-time view of one runtime branch attached
// to a running task.
type BranchSnapshot struct {
	ID           string
	Name         string
	State        string
	AnchorTaps   []string
	AnchorNodes  []string
	Nodes        []pipeline.NodeRef
	Taps         []TapInfo
	Destinations []DestinationSnapshot
	Spec         pipeline.Spec
	Stats        BranchStats
}

// DestinationSnapshot is an immutable point-in-time view of a planned task or
// branch destination.
type DestinationSnapshot struct {
	Name      string
	Operation OperationKind
	Component string
	Format    av.FormatID
	Branches  []string
	Open      bool
}

// Runtime is the composition root for applications embedding goav.
type Runtime interface {
	Probe(context.Context, format.ProbeRequest) (format.ProbeResult, error)
}

// GraphBuilder is the handle-based expert graph layer. Most applications should
// start with From and compose chains, taps, branches, destinations, and tasks.
type GraphBuilder interface {
	Source(string, pipeline.Source) GraphNode
	Stage(string, pipeline.Stage) GraphNode
	Sink(string, pipeline.Sink) GraphNode
	Connect(GraphOutlet, ...GraphInlet) GraphBuilder
	Describe() (pipeline.Spec, error)
	Build(context.Context) (Task, error)
}

// Task is a runnable media composition.
type Task interface {
	Describe() pipeline.Spec
	Explain(context.Context) (PlanReport, error)
	Attach(context.Context, ...BranchSpec) (Attachment, error)
	Detach(context.Context, Attachment) error
	Taps() []TapInfo
	// Snapshot returns a point-in-time diagnostic view without exposing graph handles.
	Snapshot() TaskSnapshot
	Run(context.Context) error
	Events() <-chan av.Event
	Stats() TaskStats
	Close() error
}
