// Package component contains small adapters for custom pipeline components.
package component

import (
	"context"
	"errors"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

var (
	errNilStage = errors.New("goav/component: nil stage callback")
	errNilSink  = errors.New("goav/component: nil sink callback")
)

// Message is what a SinkFunc receives: one packet, frame, or event
// (pipeline.Message). Exactly one of Packet, Frame, or Event is set,
// according to Kind.
type Message = pipeline.Message

// Emit is how a custom stage (PacketFunc/FrameFunc/EventFunc) forwards output
// downstream. Packet/Frame/Event return the same flow-control errors as a
// source push: errors.Is(err, goav.ErrBackpressure) for a full downstream
// buffer and errors.Is(err, goav.ErrClosed) once the task has stopped. A stage
// should propagate these rather than treat them as fatal.
type Emit struct {
	ctx     context.Context
	emitter pipeline.Emitter
}

// Packet forwards one packet downstream; nil is a no-op.
func (e *Emit) Packet(packet *av.Packet) error {
	if packet == nil {
		return nil
	}
	msg := packetMessage(packet)
	return e.emitter.Emit(e.ctx, &msg)
}

// Frame forwards one frame downstream; nil is a no-op.
func (e *Emit) Frame(frame *av.Frame) error {
	if frame == nil {
		return nil
	}
	msg := frameMessage(frame)
	return e.emitter.Emit(e.ctx, &msg)
}

// Event forwards one out-of-band event downstream.
func (e *Emit) Event(event av.Event) error {
	msg := eventMessage(event)
	return e.emitter.Emit(e.ctx, &msg)
}

// EOS emits end-of-stream for the given streams (or unscoped when none are
// listed), letting downstream nodes flush and finalize.
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

// PacketFunc wraps a function as a packet-processing stage for .Do(...):
// fn observes or transforms each packet and forwards output through emit.
// Frames and events pass through untouched.
func PacketFunc(name string, fn func(context.Context, *av.Packet, Emit) error) pipeline.Stage {
	return mediaFuncStage{name: name, packet: fn}
}

// FrameFunc wraps a function as a frame-processing stage for .Do(...): fn
// observes or transforms each frame and forwards output through emit. Packets
// and events pass through untouched.
func FrameFunc(name string, fn func(context.Context, *av.Frame, Emit) error) pipeline.Stage {
	return mediaFuncStage{name: name, frame: fn}
}

// EventFunc wraps a function as an event-observing stage for .Do(...): fn
// sees each event, and every message (the event included) continues
// downstream.
func EventFunc(name string, fn func(context.Context, av.Event) error) pipeline.Stage {
	return mediaFuncStage{name: name, event: fn}
}

// SinkFunc wraps a function as a terminal sink for goav.Sink(...): fn receives
// every delivered message. Returning an error fails the task.
func SinkFunc(name string, fn func(context.Context, Message) error) pipeline.Sink {
	return mediaFuncSink{name: name, fn: fn}
}

type mediaFuncStage struct {
	name   string
	packet func(context.Context, *av.Packet, Emit) error
	frame  func(context.Context, *av.Frame, Emit) error
	event  func(context.Context, av.Event) error
}

// ValidateComponent reports constructor-time misuse that the root recipe
// compiler maps onto its typed build errors.
func (s mediaFuncStage) ValidateComponent() error {
	if s.packet == nil && s.frame == nil && s.event == nil {
		return errNilStage
	}
	return nil
}

func (s mediaFuncStage) Name() string {
	if s.name != "" {
		return s.name
	}
	return "func"
}

func (s mediaFuncStage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	if err := s.ValidateComponent(); err != nil {
		return err
	}
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

// ValidateComponent reports constructor-time misuse that the root recipe
// compiler maps onto its typed build errors.
func (s mediaFuncSink) ValidateComponent() error {
	if s.fn == nil {
		return errNilSink
	}
	return nil
}

func (s mediaFuncSink) Name() string {
	if s.name != "" {
		return s.name
	}
	return "sink"
}

func (s mediaFuncSink) Handle(ctx context.Context, msg *pipeline.Message) error {
	if err := s.ValidateComponent(); err != nil {
		return err
	}
	if msg == nil {
		return nil
	}
	return s.fn(ctx, *msg)
}

func (s mediaFuncSink) Close() error {
	return nil
}

func packetMessage(packet *av.Packet) pipeline.Message {
	return pipeline.Message{Kind: pipeline.MessagePacket, Packet: packet}
}

func frameMessage(frame *av.Frame) pipeline.Message {
	return pipeline.Message{Kind: pipeline.MessageFrame, Frame: frame}
}

func eventMessage(event av.Event) pipeline.Message {
	return pipeline.Message{Kind: pipeline.MessageEvent, Event: &event}
}
