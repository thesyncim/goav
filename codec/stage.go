package codec

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

type DecoderStageConfig struct {
	Name string
	// Detail is optional graph-rendering context for humans.
	Detail string
	// Decoder receives packets and events and writes decoded frames into Result.
	Decoder Decoder
	// Result is caller-owned scratch. Its slice capacities define how many
	// frames, events, and control requests can be emitted per input message.
	Result DecodeResult
	// DropInputEvents suppresses forwarding upstream events after the decoder
	// has observed them. By default, events stay visible downstream.
	DropInputEvents bool
}

type EncoderStageConfig struct {
	Name string
	// Detail is optional graph-rendering context for humans.
	Detail string
	// Encoder receives frames and events and writes encoded packets into Result.
	Encoder Encoder
	// Result is caller-owned scratch. Its slice capacities define how many
	// packets and events can be emitted per input message.
	Result EncodeResult
	// OutputStreamID stamps emitted packets with the runtime-owned encoded
	// stream ID when StampOutputStream is true.
	OutputStreamID av.StreamID
	// OutputCodecEpoch stamps emitted packets with the encoded stream epoch when
	// StampOutputStream is true.
	OutputCodecEpoch av.Epoch
	// StampOutputStream overwrites packet StreamID and CodecEpoch before packets
	// leave the stage. This is useful when one decoded stream fans out into
	// several encoded output streams.
	StampOutputStream bool
	// DropInputEvents suppresses forwarding upstream events after the encoder
	// has observed them. By default, events stay visible downstream.
	DropInputEvents bool
}

type DecoderStage struct {
	name         string
	detail       string
	decoder      Decoder
	result       DecodeResult
	message      pipeline.Message
	requestEvent av.Event
	dropEvents   bool
	closed       bool
}

type EncoderStage struct {
	name              string
	detail            string
	encoder           Encoder
	result            EncodeResult
	message           pipeline.Message
	outputStreamID    av.StreamID
	outputCodecEpoch  av.Epoch
	stampOutputStream bool
	dropEvents        bool
	closed            bool
}

var _ pipeline.Stage = (*DecoderStage)(nil)
var _ pipeline.Stage = (*EncoderStage)(nil)
var _ pipeline.NodeDescriber = (*DecoderStage)(nil)
var _ pipeline.NodeDescriber = (*EncoderStage)(nil)

func NewDecoderStage(config DecoderStageConfig) (*DecoderStage, error) {
	if config.Decoder == nil {
		return nil, ErrNilDecoder
	}
	name := config.Name
	if name == "" {
		name = "decode"
	}
	return &DecoderStage{
		name:       name,
		detail:     config.Detail,
		decoder:    config.Decoder,
		result:     config.Result,
		dropEvents: config.DropInputEvents,
	}, nil
}

func NewEncoderStage(config EncoderStageConfig) (*EncoderStage, error) {
	if config.Encoder == nil {
		return nil, ErrNilEncoder
	}
	name := config.Name
	if name == "" {
		name = "encode"
	}
	return &EncoderStage{
		name:              name,
		detail:            config.Detail,
		encoder:           config.Encoder,
		result:            config.Result,
		outputStreamID:    config.OutputStreamID,
		outputCodecEpoch:  config.OutputCodecEpoch,
		stampOutputStream: config.StampOutputStream,
		dropEvents:        config.DropInputEvents,
	}, nil
}

func (s *DecoderStage) Name() string {
	return s.name
}

func (s *EncoderStage) Name() string {
	return s.name
}

func (s *DecoderStage) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: s.name, Kind: pipeline.NodeStage, Detail: s.detail}
}

func (s *EncoderStage) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: s.name, Kind: pipeline.NodeStage, Detail: s.detail}
}

func (s *DecoderStage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
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
		if err := s.decoder.DecodeInto(ctx, msg.Packet, &s.result); err != nil {
			return err
		}
		return s.emitResult(ctx, emitter)
	case pipeline.MessageEvent:
		return s.handleEvent(ctx, msg, emitter)
	case pipeline.MessageFrame:
		return emitter.Emit(ctx, msg)
	default:
		return nil
	}
}

func (s *EncoderStage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
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
	case pipeline.MessageFrame:
		if msg.Frame == nil {
			return nil
		}
		s.result.Reset()
		if err := s.encoder.EncodeInto(ctx, msg.Frame, &s.result); err != nil {
			return err
		}
		return s.emitResult(ctx, emitter)
	case pipeline.MessageEvent:
		return s.handleEvent(ctx, msg, emitter)
	case pipeline.MessagePacket:
		return emitter.Emit(ctx, msg)
	default:
		return nil
	}
}

func (s *DecoderStage) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.decoder.Close()
}

func (s *EncoderStage) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.encoder.Close()
}

func (s *DecoderStage) handleEvent(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	if msg.Event == nil {
		return nil
	}
	if err := s.decoder.HandleEvent(ctx, msg.Event); err != nil {
		return err
	}

	if msg.Event.Type == av.EventEndOfStream {
		s.result.Reset()
		if err := s.decoder.FlushInto(ctx, &s.result); err != nil {
			return err
		}
		if err := s.emitResult(ctx, emitter); err != nil {
			return err
		}
		return s.emitInputEvent(ctx, msg, emitter)
	}

	if err := s.emitInputEvent(ctx, msg, emitter); err != nil {
		return err
	}
	if msg.Event.Type != av.EventPacketLoss {
		return nil
	}

	s.result.Reset()
	if err := s.decoder.DecodeInto(ctx, nil, &s.result); err != nil {
		return err
	}
	return s.emitResult(ctx, emitter)
}

func (s *EncoderStage) handleEvent(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	if msg.Event == nil {
		return nil
	}
	if err := s.encoder.HandleEvent(ctx, msg.Event); err != nil {
		return err
	}

	if msg.Event.Type == av.EventEndOfStream {
		s.result.Reset()
		if err := s.encoder.FlushInto(ctx, &s.result); err != nil {
			return err
		}
		if err := s.emitResult(ctx, emitter); err != nil {
			return err
		}
		return s.emitInputEvent(ctx, msg, emitter)
	}

	return s.emitInputEvent(ctx, msg, emitter)
}

func (s *DecoderStage) emitInputEvent(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	if s.dropEvents {
		return nil
	}
	return emitter.Emit(ctx, msg)
}

func (s *EncoderStage) emitInputEvent(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	if s.dropEvents {
		return nil
	}
	return emitter.Emit(ctx, msg)
}

func (s *DecoderStage) emitResult(ctx context.Context, emitter pipeline.Emitter) error {
	for i := range s.result.Events {
		s.message.Kind = pipeline.MessageEvent
		s.message.Packet = nil
		s.message.Frame = nil
		s.message.Event = &s.result.Events[i]
		if err := emitter.Emit(ctx, &s.message); err != nil {
			return err
		}
	}
	for i := range s.result.Requests {
		if !controlRequestEvent(&s.result.Requests[i], &s.requestEvent) {
			continue
		}
		s.message.Kind = pipeline.MessageEvent
		s.message.Packet = nil
		s.message.Frame = nil
		s.message.Event = &s.requestEvent
		if err := emitter.Emit(ctx, &s.message); err != nil {
			return err
		}
	}
	for i := range s.result.Frames {
		s.message.Kind = pipeline.MessageFrame
		s.message.Packet = nil
		s.message.Frame = &s.result.Frames[i]
		s.message.Event = nil
		if err := emitter.Emit(ctx, &s.message); err != nil {
			return err
		}
	}
	return nil
}

func (s *EncoderStage) emitResult(ctx context.Context, emitter pipeline.Emitter) error {
	for i := range s.result.Events {
		s.message.Kind = pipeline.MessageEvent
		s.message.Packet = nil
		s.message.Frame = nil
		s.message.Event = &s.result.Events[i]
		if err := emitter.Emit(ctx, &s.message); err != nil {
			return err
		}
	}
	for i := range s.result.Packets {
		if s.stampOutputStream {
			s.result.Packets[i].StreamID = s.outputStreamID
			s.result.Packets[i].CodecEpoch = s.outputCodecEpoch
		}
		s.message.Kind = pipeline.MessagePacket
		s.message.Packet = &s.result.Packets[i]
		s.message.Frame = nil
		s.message.Event = nil
		if err := emitter.Emit(ctx, &s.message); err != nil {
			return err
		}
	}
	return nil
}

func controlRequestEvent(request *ControlRequest, event *av.Event) bool {
	event.Reset()
	event.StreamID = request.StreamID
	switch request.Type {
	case ControlRequestKeyframe:
		event.Type = av.EventKeyframeRequired
	case ControlResetDecoder:
		event.Type = av.EventDiscontinuity
	case ControlDropUntilSync:
		event.Type = av.EventDiscontinuity
	default:
		return false
	}
	event.Reason = request.Reason
	return true
}
