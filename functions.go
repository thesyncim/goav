package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

type sourceEmit struct {
	ctx     context.Context
	emitter pipeline.Emitter
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

// packetDelivery/frameDelivery/eventDelivery are the source-push seam: they
// emit like Packet/Frame/Event but additionally report per-push delivery via
// the runner's optional pipeline.DeliveryEmitter capability. When the emitter
// lacks the capability, they fall back to Accepted = (err == nil).
func (e *sourceEmit) packetDelivery(packet *av.Packet) (PushResult, error) {
	msg := packetMessage(packet)
	return e.emitDelivery(&msg)
}

func (e *sourceEmit) frameDelivery(frame *av.Frame) (PushResult, error) {
	msg := frameMessage(frame)
	return e.emitDelivery(&msg)
}

func (e *sourceEmit) eventDelivery(event av.Event) (PushResult, error) {
	msg := eventMessage(event)
	return e.emitDelivery(&msg)
}

func (e *sourceEmit) emitDelivery(msg *pipeline.Message) (PushResult, error) {
	if de, ok := e.emitter.(pipeline.DeliveryEmitter); ok {
		delivery, err := de.EmitDelivery(e.ctx, msg)
		return PushResult{Accepted: delivery.Delivered > 0, Dropped: delivery.Shed > 0}, err
	}
	err := e.emitter.Emit(e.ctx, msg)
	return PushResult{Accepted: err == nil}, err
}

// EOS emits end-of-stream for the given streams (or unscoped when none are
// listed), letting downstream nodes flush and finalize.
func (e *sourceEmit) EOS(streams ...av.StreamID) error {
	if len(streams) == 0 {
		return e.eventOnly(av.Event{Type: av.EventEndOfStream})
	}
	for i := range streams {
		if err := e.eventOnly(av.Event{Type: av.EventEndOfStream, StreamID: streams[i]}); err != nil {
			return err
		}
	}
	return nil
}

func (e *sourceEmit) eventOnly(event av.Event) error {
	msg := eventMessage(event)
	return e.emitter.Emit(e.ctx, &msg)
}

type componentValidator interface {
	ValidateComponent() error
}

func validateStageComponent(stage pipeline.Stage) error {
	if stage == nil {
		return ErrNilStage
	}
	if validator, ok := stage.(componentValidator); ok {
		if err := validator.ValidateComponent(); err != nil {
			return ErrNilStage
		}
	}
	return nil
}

func validateSinkComponent(sink pipeline.Sink) error {
	if sink == nil {
		return ErrNilSink
	}
	if validator, ok := sink.(componentValidator); ok {
		if err := validator.ValidateComponent(); err != nil {
			return ErrNilSink
		}
	}
	return nil
}
