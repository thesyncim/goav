package format

import (
	"context"
	"errors"
	"io"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

type DemuxSourceConfig struct {
	Name string
	// Detail is optional graph-rendering context for humans.
	Detail string
	// Demuxer reads container input into caller-owned Result scratch.
	Demuxer Demuxer
	// Result owns the packet pointer and event slice reused for every read.
	Result ReadResult
	// EmitStreamEvents emits initial stream_added events from Demuxer.Streams.
	EmitStreamEvents bool
	// DropEndOfStream suppresses the synthetic EOS event emitted on io.EOF.
	DropEndOfStream bool
}

type MuxStageConfig struct {
	Name string
	// Detail is optional graph-rendering context for humans.
	Detail string
	// Muxer writes packet messages to the configured output.
	Muxer Muxer
	// Result owns the event slice reused for every write.
	Result WriteResult
	// DropInputEvents suppresses forwarding upstream events.
	DropInputEvents bool
	// PassthroughPackets forwards packet messages after writing them.
	PassthroughPackets bool
}

type DemuxSource struct {
	name    string
	detail  string
	demuxer Demuxer
	result  ReadResult
	message pipeline.Message
	eos     av.Event
	streams []av.Stream
	events  []av.Event
	dropEOS bool
	closed  bool
}

type MuxStage struct {
	name               string
	detail             string
	muxer              Muxer
	result             WriteResult
	message            pipeline.Message
	dropEvents         bool
	passthroughPackets bool
	closed             bool
}

var _ pipeline.Source = (*DemuxSource)(nil)
var _ pipeline.Stage = (*MuxStage)(nil)
var _ pipeline.NodeDescriber = (*DemuxSource)(nil)
var _ pipeline.NodeDescriber = (*MuxStage)(nil)

func NewDemuxSource(config DemuxSourceConfig) (*DemuxSource, error) {
	if config.Demuxer == nil {
		return nil, ErrNilDemuxer
	}
	if config.Result.Packet == nil {
		return nil, ErrNilPacket
	}
	name := config.Name
	if name == "" {
		name = "demux"
	}

	source := &DemuxSource{
		name:    name,
		detail:  config.Detail,
		demuxer: config.Demuxer,
		result:  config.Result,
		dropEOS: config.DropEndOfStream,
	}
	if config.EmitStreamEvents {
		source.initStreamEvents(config.Demuxer.Streams())
	}
	return source, nil
}

func NewMuxStage(config MuxStageConfig) (*MuxStage, error) {
	if config.Muxer == nil {
		return nil, ErrNilMuxer
	}
	name := config.Name
	if name == "" {
		name = "mux"
	}
	return &MuxStage{
		name:               name,
		detail:             config.Detail,
		muxer:              config.Muxer,
		result:             config.Result,
		dropEvents:         config.DropInputEvents,
		passthroughPackets: config.PassthroughPackets,
	}, nil
}

func (s *DemuxSource) Name() string {
	return s.name
}

func (s *MuxStage) Name() string {
	return s.name
}

func (s *DemuxSource) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: s.name, Kind: pipeline.NodeSource, Detail: s.detail}
}

func (s *MuxStage) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: s.name, Kind: pipeline.NodeStage, Detail: s.detail}
}

func (s *DemuxSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return pipeline.ErrClosed
	}
	if err := s.emitStreamEvents(ctx, emitter); err != nil {
		return err
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.result.Reset()
		if err := s.demuxer.ReadInto(ctx, &s.result); err != nil {
			if errors.Is(err, io.EOF) {
				return s.emitEndOfStream(ctx, emitter)
			}
			return err
		}
		if err := s.emitEvents(ctx, emitter, s.result.Events); err != nil {
			return err
		}
		if s.result.PacketReady {
			if err := s.emitPacket(ctx, emitter, s.result.Packet); err != nil {
				return err
			}
		}
	}
}

func (s *MuxStage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return pipeline.ErrClosed
	}
	if msg == nil {
		return nil
	}
	switch msg.Kind {
	case pipeline.MessagePacket:
		if msg.Packet == nil {
			return nil
		}
		s.result.Reset()
		if err := s.muxer.Write(ctx, msg.Packet, &s.result); err != nil {
			return err
		}
		if err := s.emitEvents(ctx, emitter); err != nil {
			return err
		}
		if s.passthroughPackets {
			return emitter.Emit(ctx, msg)
		}
		return nil
	case pipeline.MessageEvent:
		if s.dropEvents {
			return nil
		}
		return emitter.Emit(ctx, msg)
	case pipeline.MessageFrame:
		return emitter.Emit(ctx, msg)
	default:
		return nil
	}
}

func (s *DemuxSource) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.demuxer.Close()
}

func (s *MuxStage) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.muxer.Close()
}

func (s *DemuxSource) initStreamEvents(streams []av.Stream) {
	if len(streams) == 0 {
		return
	}
	s.streams = make([]av.Stream, len(streams))
	copy(s.streams, streams)
	s.events = make([]av.Event, len(streams))
	for i := range s.streams {
		stream := &s.streams[i]
		s.events[i] = av.Event{
			Type:     av.EventStreamAdded,
			StreamID: stream.ID,
			Epoch:    stream.Epoch,
			Stream:   stream,
		}
	}
}

func (s *DemuxSource) emitStreamEvents(ctx context.Context, emitter pipeline.Emitter) error {
	return s.emitEvents(ctx, emitter, s.events)
}

func (s *DemuxSource) emitEvents(ctx context.Context, emitter pipeline.Emitter, events []av.Event) error {
	for i := range events {
		s.message.Kind = pipeline.MessageEvent
		s.message.Packet = nil
		s.message.Frame = nil
		s.message.Event = &events[i]
		if err := emitter.Emit(ctx, &s.message); err != nil {
			return err
		}
	}
	return nil
}

func (s *DemuxSource) emitPacket(ctx context.Context, emitter pipeline.Emitter, packet *av.Packet) error {
	if packet == nil {
		return nil
	}
	s.message.Kind = pipeline.MessagePacket
	s.message.Packet = packet
	s.message.Frame = nil
	s.message.Event = nil
	return emitter.Emit(ctx, &s.message)
}

func (s *DemuxSource) emitEndOfStream(ctx context.Context, emitter pipeline.Emitter) error {
	if s.dropEOS {
		return nil
	}
	s.eos.Reset()
	s.eos.Type = av.EventEndOfStream
	s.message.Kind = pipeline.MessageEvent
	s.message.Packet = nil
	s.message.Frame = nil
	s.message.Event = &s.eos
	return emitter.Emit(ctx, &s.message)
}

func (s *MuxStage) emitEvents(ctx context.Context, emitter pipeline.Emitter) error {
	for i := range s.result.Events {
		s.message.Kind = pipeline.MessageEvent
		s.message.Packet = nil
		s.message.Frame = nil
		s.message.Event = &s.result.Events[i]
		if err := emitter.Emit(ctx, &s.message); err != nil {
			return err
		}
	}
	return nil
}
