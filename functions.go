package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

// Message is what a SinkFunc receives: one packet, frame, or event
// (pipeline.Message). Exactly one of Packet, Frame, or Event is set,
// according to Kind.
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

// Packet forwards one packet downstream; nil is a no-op.
func (e *Emit) Packet(packet *av.Packet) error {
	if packet == nil {
		return nil
	}
	return e.emitter.Emit(e.ctx, e.packetMessage(packet))
}

// Frame forwards one frame downstream; nil is a no-op.
func (e *Emit) Frame(frame *av.Frame) error {
	if frame == nil {
		return nil
	}
	return e.emitter.Emit(e.ctx, e.frameMessage(frame))
}

// Event forwards one out-of-band event downstream.
func (e *Emit) Event(event av.Event) error {
	return e.emitter.Emit(e.ctx, e.eventMessage(event))
}

func (e *Emit) packetMessage(packet *av.Packet) *pipeline.Message {
	e.message.Kind = pipeline.MessagePacket
	e.message.Packet = packet
	e.message.Frame = nil
	e.message.Event = nil
	return &e.message
}

func (e *Emit) frameMessage(frame *av.Frame) *pipeline.Message {
	e.message.Kind = pipeline.MessageFrame
	e.message.Packet = nil
	e.message.Frame = frame
	e.message.Event = nil
	return &e.message
}

func (e *Emit) eventMessage(event av.Event) *pipeline.Message {
	e.message.Kind = pipeline.MessageEvent
	e.message.Packet = nil
	e.message.Frame = nil
	e.message.Event = &event
	return &e.message
}

// packetDelivery/frameDelivery/eventDelivery are the source-push seam: they
// emit like Packet/Frame/Event but additionally report per-push delivery via
// the runner's optional pipeline.DeliveryEmitter capability. When the emitter
// lacks the capability, they fall back to Accepted = (err == nil).
func (e *Emit) packetDelivery(packet *av.Packet) (PushResult, error) {
	return e.emitDelivery(e.packetMessage(packet))
}

func (e *Emit) frameDelivery(frame *av.Frame) (PushResult, error) {
	return e.emitDelivery(e.frameMessage(frame))
}

func (e *Emit) eventDelivery(event av.Event) (PushResult, error) {
	return e.emitDelivery(e.eventMessage(event))
}

func (e *Emit) emitDelivery(msg *pipeline.Message) (PushResult, error) {
	if de, ok := e.emitter.(pipeline.DeliveryEmitter); ok {
		delivery, err := de.EmitDelivery(e.ctx, msg)
		return PushResult{Accepted: delivery.Delivered > 0, Dropped: delivery.Shed > 0}, err
	}
	err := e.emitter.Emit(e.ctx, msg)
	return PushResult{Accepted: err == nil}, err
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
	if fn == nil {
		return nil
	}
	return mediaFuncStage{name: name, packet: fn}
}

// FrameFunc wraps a function as a frame-processing stage for .Do(...): fn
// observes or transforms each frame and forwards output through emit. Packets
// and events pass through untouched.
func FrameFunc(name string, fn func(context.Context, *av.Frame, Emit) error) pipeline.Stage {
	if fn == nil {
		return nil
	}
	return mediaFuncStage{name: name, frame: fn}
}

// EventFunc wraps a function as an event-observing stage for .Do(...): fn
// sees each event, and every message (the event included) continues
// downstream.
func EventFunc(name string, fn func(context.Context, av.Event) error) pipeline.Stage {
	if fn == nil {
		return nil
	}
	return mediaFuncStage{name: name, event: fn}
}

// SinkFunc wraps a function as a terminal sink for goav.Sink(...): fn
// receives every delivered message. Returning an error fails the task.
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
