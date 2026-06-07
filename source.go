package goav

import (
	"context"
	"sync/atomic"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
)

type SourceFunc func(context.Context, SourcePush) error

type SourcePush struct {
	emit   Emit
	stream av.StreamID
}

func (p *SourcePush) Packet(packet *av.Packet) error {
	if packet == nil {
		return nil
	}
	if packet.StreamID == "" {
		packet.StreamID = p.stream
	}
	return p.emit.Packet(packet)
}

func (p *SourcePush) Event(event av.Event) error {
	if event.StreamID == "" {
		event.StreamID = p.stream
	}
	return p.emit.Event(event)
}

func (p *SourcePush) EOS(streams ...av.StreamID) error {
	if len(streams) == 0 && p.stream != "" {
		streams = []av.StreamID{p.stream}
	}
	return p.emit.EOS(streams...)
}

type sourceInputSpec struct {
	shape MediaShape
	fn    SourceFunc
}

func Source(name string, shape MediaShape, fn SourceFunc) InputSpec {
	shape = normalizeCustomSourceShape(name, shape)
	return InputSpec{
		input: format.Input{
			Name:     name,
			Protocol: av.ProtocolCustom,
			Realtime: shape.Realtime,
		},
		source: &sourceInputSpec{shape: shape, fn: fn},
		codec:  codecSpecFromSourceShape(shape),
		name:   name,
	}
}

func normalizeCustomSourceShape(name string, shape MediaShape) MediaShape {
	if shape.Domain == "" {
		shape.Domain = DomainPacket
	}
	if shape.StreamID == "" {
		shape.StreamID = av.StreamID(firstNonEmpty(name, string(shape.MediaKind), "stream"))
	}
	return shape
}

func codecSpecFromSourceShape(shape MediaShape) CodecSpec {
	return CodecSpec{
		ID:   shape.Codec,
		Type: shape.MediaKind,
		Parameters: av.CodecParameters{
			ID:           shape.Codec,
			Type:         shape.MediaKind,
			SampleRate:   shape.SampleRate,
			Channels:     shape.Channels,
			Width:        shape.Width,
			Height:       shape.Height,
			PixelFormat:  shape.PixelFormat,
			SampleFormat: shape.SampleFormat,
		},
	}
}

func customSourceStreams(input InputSpec) []av.Stream {
	if input.source == nil {
		return nil
	}
	return []av.Stream{customSourceStream(input)}
}

func customSourceProbeResult(input InputSpec) format.ProbeResult {
	return format.ProbeResult{
		Score:   100,
		Streams: customSourceStreams(input),
		Reason:  "declared custom source shape",
	}
}

func customSourceStream(input InputSpec) av.Stream {
	shape := normalizeCustomSourceShape(input.inputName("source"), input.source.shape)
	return av.Stream{
		ID:   shape.StreamID,
		Type: shape.MediaKind,
		Codec: av.CodecParameters{
			ID:           shape.Codec,
			Type:         shape.MediaKind,
			SampleRate:   shape.SampleRate,
			Channels:     shape.Channels,
			Width:        shape.Width,
			Height:       shape.Height,
			PixelFormat:  shape.PixelFormat,
			SampleFormat: shape.SampleFormat,
		},
		Name: string(shape.StreamID),
	}
}

func newCustomSource(input InputSpec) (pipeline.Source, []av.Stream, error) {
	if input.source == nil {
		return nil, nil, ErrNilSource
	}
	streams := customSourceStreams(input)
	if len(streams) == 0 {
		return nil, nil, ErrNilSource
	}
	source := &customSource{
		name:   customSourceNodeName(input),
		detail: customSourceDetail(input),
		stream: streams[0].ID,
		fn:     input.source.fn,
	}
	return source, streams, nil
}

type customSource struct {
	name   string
	detail string
	stream av.StreamID
	fn     SourceFunc
	closed atomic.Bool
}

func (s *customSource) Name() string {
	return s.name
}

func (s *customSource) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: s.name, Kind: pipeline.NodeSource, Detail: s.detail}
}

func (s *customSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	if s.closed.Load() {
		return pipeline.ErrClosed
	}
	if s.fn == nil {
		return ErrNilSource
	}
	push := SourcePush{
		emit:   Emit{ctx: ctx, emitter: emitter},
		stream: s.stream,
	}
	return s.fn(ctx, push)
}

func (s *customSource) Close() error {
	s.closed.Store(true)
	return nil
}
