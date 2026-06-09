package goav

import (
	"context"
	"sync/atomic"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
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
	shape shape.Spec
	fn    SourceFunc
}

func Source(name string, spec shape.Spec, fn SourceFunc) InputSpec {
	spec = normalizeCustomSourceShape(name, spec)
	return InputSpec{
		input: format.Input{
			Name:     name,
			Protocol: av.ProtocolCustom,
			Realtime: spec.Realtime,
		},
		source: &sourceInputSpec{shape: spec, fn: fn},
		codec:  codecSpecFromSourceShape(spec),
		name:   name,
	}
}

func normalizeCustomSourceShape(name string, spec shape.Spec) shape.Spec {
	if spec.Domain == "" {
		spec.Domain = shape.DomainPacket
	}
	if spec.StreamID == "" {
		spec.StreamID = av.StreamID(firstNonEmpty(name, string(spec.MediaKind), "stream"))
	}
	return spec
}

func codecSpecFromSourceShape(shape shape.Spec) CodecSpec {
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

func customSourceShape(input InputSpec) (shape.Spec, bool) {
	if input.source == nil {
		return shape.Spec{}, false
	}
	return normalizeCustomSourceShape(input.inputName("source"), input.source.shape), true
}

func compileStateCustomSourceShape(state *recipeCompileState) (shape.Spec, bool) {
	if state == nil {
		return shape.Spec{}, false
	}
	if state.branchCompositionPresent {
		return customSourceShape(state.branchInputAttachment)
	}
	if len(state.inputAttachments) != 1 {
		return shape.Spec{}, false
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
func (s InputSpec) openGraphSource(ctx context.Context, service *builder, index int) (pipeline.Source, []av.Stream, shape.MediaDomain, error) {
	switch {
	case s.source != nil:
		source, streams, err := newCustomSource(s)
		if err != nil {
			return nil, nil, "", err
		}
		shape, _ := customSourceShape(s)
		return source, streams, shape.Domain, nil
	case s.rtp != nil:
		build, err := service.openRTPSource(ctx, s.rtpBuildInput(), index)
		if err != nil {
			return nil, nil, "", err
		}
		return build.source, build.streams, shape.DomainPacket, nil
	default:
		build, err := service.openDemuxSource(ctx, s.input)
		if err != nil {
			return nil, nil, "", err
		}
		return build.source, build.streams, shape.DomainPacket, nil
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
