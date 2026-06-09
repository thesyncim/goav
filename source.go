package goav

import (
	"context"
	"sync/atomic"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
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

func codecSpecFromSourceShape(shape shape.Spec) codec.CodecSpec {
	return codec.CodecSpec{
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

// graphSourceBuild is the unified result of opening one input as a graph source:
// the running pipeline source, the streams it carries, its media domain, the
// realtime contribution of that input, and the optional RTP build metadata (decode
// bounds) that only RTP inputs provide. Carrying everything in one value lets the
// recipe build path iterate inputs without branching on the input kind.
type graphSourceBuild struct {
	source   pipeline.Source
	streams  []av.Stream
	domain   shape.MediaDomain
	realtime bool
	rtp      *rtpBuild
}

// openGraphSourceBuild is the single source-opening seam: every input kind (custom
// Source, file/URI, RTP) resolves to a running pipeline source + its streams +
// media domain + realtime + optional RTP metadata through here, so callers never
// branch on the input kind. Returning all streams keeps it composable — the caller
// selects what it needs.
func (s InputSpec) openGraphSourceBuild(ctx context.Context, service *builder, index int) (graphSourceBuild, error) {
	switch {
	case s.source != nil:
		source, streams, err := newCustomSource(s)
		if err != nil {
			return graphSourceBuild{}, err
		}
		shapeSpec, _ := customSourceShape(s)
		return graphSourceBuild{
			source:   source,
			streams:  streams,
			domain:   shapeSpec.Domain,
			realtime: shapeSpec.Realtime,
		}, nil
	case s.rtp != nil:
		build, err := service.openRTPSource(ctx, s.rtpBuildInput(), index)
		if err != nil {
			return graphSourceBuild{}, err
		}
		rtp := build
		return graphSourceBuild{
			source:  build.source,
			streams: build.streams,
			domain:  shape.DomainPacket,
			rtp:     &rtp,
		}, nil
	default:
		input := s.formatInput()
		build, err := service.openDemuxSource(ctx, input)
		if err != nil {
			return graphSourceBuild{}, err
		}
		return graphSourceBuild{
			source:   build.source,
			streams:  build.streams,
			domain:   shape.DomainPacket,
			realtime: input.Realtime,
		}, nil
	}
}

// openGraphSource keeps the streams/domain 4-tuple shape used by the Mix and
// Composite arms; it delegates to openGraphSourceBuild so all opening goes through
// one seam.
func (s InputSpec) openGraphSource(ctx context.Context, service *builder, index int) (pipeline.Source, []av.Stream, shape.MediaDomain, error) {
	build, err := s.openGraphSourceBuild(ctx, service, index)
	if err != nil {
		return nil, nil, "", err
	}
	return build.source, build.streams, build.domain, nil
}

// graphSourceNodeName returns the planner node name for this input, matching the
// running source's node name for every input kind so describe and build agree.
func (s InputSpec) graphSourceNodeName(index int) string {
	switch {
	case s.source != nil:
		return customSourceNodeName(s)
	case s.rtp != nil:
		return rtpNodeName(s.rtpBuildInput(), index)
	default:
		return demuxNodeName(s.formatInput())
	}
}

// graphSourceNodeDetail returns the planner node detail for this input, matching
// the running source's detail for every input kind.
func (s InputSpec) graphSourceNodeDetail(index int) string {
	switch {
	case s.source != nil:
		return customSourceDetail(s)
	case s.rtp != nil:
		return rtpInputDetail(s.rtpBuildInput())
	default:
		return inputNodeDetail(s.formatInput())
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
