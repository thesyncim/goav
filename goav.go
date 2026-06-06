// Package goav exposes the top-level runtime contracts for composing media
// inputs, codecs, pipelines, and outputs.
package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/rtpav"
	"github.com/thesyncim/goav/transcode"
)

type Input = format.Input
type Output = format.Output
type ProbeRequest = format.ProbeRequest
type ProbeResult = format.ProbeResult

// Runtime is the composition root for applications embedding goav.
type Runtime interface {
	Codecs() codec.Registry
	Formats() format.Registry
	Filters() filter.Registry
	Pipelines() pipeline.Factory
	Probe(context.Context, ProbeRequest) (ProbeResult, error)
	New() Builder
}

type Connection = pipeline.Connection

// Builder describes the intended fluent connection API without constraining the
// internal graph representation.
type Builder interface {
	Input(Input) Builder
	RTP(rtpav.PacketReader, ...RTPOption) Builder
	Output(Output) Builder
	Decode(av.StreamSelector) Builder
	Encode(av.StreamSelector, codec.EncodeConfig) Builder
	Filter(av.StreamSelector, pipeline.Stage) Builder
	Transcode(transcode.Plan) Builder
	Source(pipeline.Source) Builder
	Stage(pipeline.Stage) Builder
	Sink(pipeline.Sink) Builder
	Connect(from string, to ...string) Builder
	ConnectStream(from string, stream av.StreamID, to ...string) Builder
	ConnectEvent(from string, event av.EventType, to ...string) Builder
	Connection(pipeline.Connection) Builder
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

func SelectAudio() av.StreamSelector {
	return av.StreamSelector{Type: av.MediaAudio}
}

func SelectVideo() av.StreamSelector {
	return av.StreamSelector{Type: av.MediaVideo}
}
