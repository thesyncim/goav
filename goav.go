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

// Runtime is the composition root for applications embedding goav.
type Runtime interface {
	Probe(context.Context, ProbeRequest) (ProbeResult, error)
	Graph() Builder
	New() Builder
}

// Builder is the advanced graph-building layer. Most applications should start
// with recipes such as Record, Decode, From, or Transcode.
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

func SelectAudio() av.StreamSelector {
	return av.StreamSelector{Type: av.MediaAudio}
}

func SelectVideo() av.StreamSelector {
	return av.StreamSelector{Type: av.MediaVideo}
}

func Route(from string, to ...string) pipeline.Route {
	return pipeline.Route{
		From:   from,
		To:     append([]string(nil), to...),
		Policy: pipeline.RouteAll,
	}
}
