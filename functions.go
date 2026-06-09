package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

type Message = pipeline.Message

// Emit is how a custom stage (PacketFunc/FrameFunc/EventFunc) forwards output
// downstream. Packet/Frame/Event return the same flow-control errors as a
// source push: errors.Is(err, ErrBackpressure) for a full downstream buffer and
// errors.Is(err, ErrClosed) once the task has stopped. A stage should propagate
// these rather than treat them as fatal.
type Emit struct {
	ctx     context.Context
	emitter pipeline.Emitter
	message pipeline.Message
}

func (e *Emit) Packet(packet *av.Packet) error {
	if packet == nil {
		return nil
	}
	e.message.Kind = pipeline.MessagePacket
	e.message.Packet = packet
	e.message.Frame = nil
	e.message.Event = nil
	return e.emitter.Emit(e.ctx, &e.message)
}

func (e *Emit) Frame(frame *av.Frame) error {
	if frame == nil {
		return nil
	}
	e.message.Kind = pipeline.MessageFrame
	e.message.Packet = nil
	e.message.Frame = frame
	e.message.Event = nil
	return e.emitter.Emit(e.ctx, &e.message)
}

func (e *Emit) Event(event av.Event) error {
	e.message.Kind = pipeline.MessageEvent
	e.message.Packet = nil
	e.message.Frame = nil
	e.message.Event = &event
	return e.emitter.Emit(e.ctx, &e.message)
}

func (e *Emit) EOS(streams ...av.StreamID) error {
	if len(streams) == 0 {
		return e.Event(av.Event{Type: av.EventEndOfStream})
	}
	for i := range streams {
		if err := e.Event(av.Event{Type: av.EventEndOfStream, StreamID: streams[i]}); err != nil {
			return err
		}
	}
	return nil
}

func PacketFunc(name string, fn func(context.Context, *av.Packet, Emit) error) pipeline.Stage {
	if fn == nil {
		return nil
	}
	return mediaFuncStage{name: name, packet: fn}
}

func FrameFunc(name string, fn func(context.Context, *av.Frame, Emit) error) pipeline.Stage {
	if fn == nil {
		return nil
	}
	return mediaFuncStage{name: name, frame: fn}
}

func EventFunc(name string, fn func(context.Context, av.Event) error) pipeline.Stage {
	if fn == nil {
		return nil
	}
	return mediaFuncStage{name: name, event: fn}
}

func SinkFunc(name string, fn func(context.Context, Message) error) pipeline.Sink {
	if fn == nil {
		return nil
	}
	return mediaFuncSink{name: name, fn: fn}
}

type mediaFuncStage struct {
	name   string
	packet func(context.Context, *av.Packet, Emit) error
	frame  func(context.Context, *av.Frame, Emit) error
	event  func(context.Context, av.Event) error
}

func (s mediaFuncStage) Name() string {
	if s.name != "" {
		return s.name
	}
	return "func"
}

func (s mediaFuncStage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	if msg == nil {
		return nil
	}
	emit := Emit{ctx: ctx, emitter: emitter}
	switch msg.Kind {
	case pipeline.MessagePacket:
		if s.packet != nil {
			return s.packet(ctx, msg.Packet, emit)
		}
	case pipeline.MessageFrame:
		if s.frame != nil {
			return s.frame(ctx, msg.Frame, emit)
		}
	case pipeline.MessageEvent:
		if s.event != nil && msg.Event != nil {
			if err := s.event(ctx, *msg.Event); err != nil {
				return err
			}
		}
	}
	return emitter.Emit(ctx, msg)
}

func (s mediaFuncStage) Close() error {
	return nil
}

type mediaFuncSink struct {
	name string
	fn   func(context.Context, Message) error
}

func (s mediaFuncSink) Name() string {
	if s.name != "" {
		return s.name
	}
	return "sink"
}

func (s mediaFuncSink) Handle(ctx context.Context, msg *pipeline.Message) error {
	if s.fn == nil || msg == nil {
		return nil
	}
	return s.fn(ctx, *msg)
}

func (s mediaFuncSink) Close() error {
	return nil
}
