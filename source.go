package goav

import (
	"context"
	"sync/atomic"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
)

type SourceFunc func(context.Context, SourcePush) error

// SourcePush is how a custom Source delivers packets, frames, and events into
// the pipeline. The Packet/Frame/Event methods return a flow-control error the
// source can react to: errors.Is(err, ErrBackpressure) means a downstream buffer
// was full (slow down or expect a shed message), and errors.Is(err, ErrClosed)
// means the task has stopped and the source should return. Any other error is
// fatal to the push.
type SourcePush struct {
	emit   Emit
	stream av.StreamID
}

// Packet delivers one packet. See SourcePush for the ErrBackpressure/ErrClosed
// flow-control contract on the returned error.
func (p *SourcePush) Packet(packet *av.Packet) error {
	if packet == nil {
		return nil
	}
	if packet.StreamID == "" {
		packet.StreamID = p.stream
	}
	return p.emit.Packet(packet)
}

func (p *SourcePush) Frame(frame *av.Frame) error {
	if frame == nil {
		return nil
	}
	if frame.StreamID == "" {
		frame.StreamID = p.stream
	}
	return p.emit.Frame(frame)
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

func customSourceShape(input InputSpec) (MediaShape, bool) {
	if input.source == nil {
		return MediaShape{}, false
	}
	return normalizeCustomSourceShape(input.inputName("source"), input.source.shape), true
}

func compileStateCustomSourceShape(state *recipeCompileState) (MediaShape, bool) {
	if state == nil {
		return MediaShape{}, false
	}
	if state.branchCompositionPresent {
		return customSourceShape(state.branchInputAttachment)
	}
	if len(state.inputAttachments) != 1 {
		return MediaShape{}, false
	}
	return customSourceShape(state.inputAttachments[0])
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

// openGraphSource is the single source-opening seam: every input kind (custom
// Source, file, RTP) resolves to a running pipeline source + its streams + media
// domain through here, so callers never branch on the input kind. Returning all
// streams keeps it composable — the caller selects what it needs. (RTP is the
// remaining kind to fold in; until then it returns a clear error.)
func (s InputSpec) openGraphSource(ctx context.Context, service *builder) (pipeline.Source, []av.Stream, MediaDomain, error) {
	switch {
	case s.source != nil:
		source, streams, err := newCustomSource(s)
		if err != nil {
			return nil, nil, "", err
		}
		shape, _ := customSourceShape(s)
		return source, streams, shape.Domain, nil
	case s.rtp != nil:
		return nil, nil, "", &BuildError{Code: "source_unsupported", Operation: "open source", Node: firstNonEmpty(s.name, "rtp"), Reason: "RTP source is not yet folded into the unified source opener; use a custom Source or a file input for now", Cause: ErrUnsupportedBuild}
	default:
		build, err := service.openDemuxSource(ctx, s.input)
		if err != nil {
			return nil, nil, "", err
		}
		return build.source, build.streams, DomainPacket, nil
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
