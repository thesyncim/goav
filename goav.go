// Package goav exposes the top-level runtime contracts for composing media
// inputs, codecs, pipelines, and outputs.
package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
)

type Packet = av.Packet
type Frame = av.Frame
type Event = av.Event
type Stream = av.Stream

// Runtime is the composition root for applications embedding goav.
type Runtime interface {
	Probe(context.Context, format.ProbeRequest) (format.ProbeResult, error)
	Graph() GraphBuilder
}

// GraphBuilder is the handle-based expert graph layer. Most applications should
// start with recipes such as Record, Decode, From, or Transcode.
type GraphBuilder interface {
	Source(string, pipeline.Source) GraphNode
	Stage(string, pipeline.Stage) GraphNode
	Sink(string, pipeline.Sink) GraphNode
	Connect(GraphOutlet, ...GraphInlet) GraphBuilder
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
