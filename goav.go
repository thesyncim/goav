// Package goav exposes the top-level runtime contracts for composing media
// inputs, codecs, pipelines, and outputs.
package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/rtpav"
	"github.com/thesyncim/goav/transcode"
)

type Input = format.Input
type Output = format.Output
type ProbeRequest = format.ProbeRequest
type ProbeResult = format.ProbeResult
type Packet = av.Packet
type Frame = av.Frame
type Event = av.Event
type Stream = av.Stream
type Metadata = av.Metadata
type CodecParameters = av.CodecParameters
type Source = pipeline.Source
type Stage = pipeline.Stage
type Sink = pipeline.Sink

// Runtime is the composition root for applications embedding goav.
type Runtime interface {
	Probe(context.Context, ProbeRequest) (ProbeResult, error)
	Graph() GraphBuilder
}

// GraphBuilder is the handle-based expert graph layer. Most applications should
// start with recipes such as Record, Decode, From, or Transcode.
type GraphBuilder interface {
	Source(string, Source) GraphNode
	Stage(string, Stage) GraphNode
	Sink(string, Sink) GraphNode
	Connect(GraphOutlet, ...GraphInlet) GraphBuilder
	Describe() (pipeline.Spec, error)
	Build(context.Context) (Task, error)
}

// Builder is the legacy advanced builder and compiler target used by recipes
// and graph handles. Prefer recipes or Runtime.Graph for new application code.
type Builder interface {
	Input(Input) Builder
	RTP(rtpav.PacketReader, ...RTPOption) Builder
	Output(Output) Builder
	Decode(av.StreamSelector) Builder
	Encode(av.StreamSelector, codec.EncodeConfig) Builder
	Filter(av.StreamSelector, Stage) Builder
	Transcode(transcode.Plan) Builder
	Source(Source) Builder
	Stage(Stage) Builder
	Sink(Sink) Builder
	Routes(...pipeline.Route) Builder
	Describe() (pipeline.Spec, error)
	Build(context.Context) (Task, error)
}

// Task is a runnable media job such as receive, record, remux, or transcode.
type Task interface {
	Describe() pipeline.Spec
	Run(context.Context) error
	Events() <-chan av.Event
	Close() error
}
