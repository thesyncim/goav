// Package source contains helpers for application-owned media sources.
package source

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

// Func is the body of a custom source: push packets, frames, or events until
// the media ends (push.EOS) or the context is cancelled. The returned error
// stops the task; a clean EOS return ends the stream naturally.
type Func func(context.Context, Push) error

// Result reports what happened to one push across its matching downstream
// targets. On a fan-out a single push can be both Accepted and Dropped: one
// branch queued the message while another shed it.
type Result struct {
	// Accepted is true when at least one downstream target queued the message.
	Accepted bool
	// Dropped is true when at least one target deliberately shed the message: a
	// full queue under a dropping policy, an exhausted byte budget, or a paused
	// branch. Shedding is normal realtime behavior, not failure.
	Dropped bool
}

// Push is how a custom Source delivers packets, frames, and events into the
// pipeline. Packet/Frame/Event return a Result for per-push delivery visibility
// plus a flow-control error:
//
//   - err == nil: the push was handled. Result says what happened.
//   - errors.Is(err, goav.ErrBackpressure): strict paths refused the message.
//   - errors.Is(err, goav.ErrClosed): the task has stopped; return cleanly.
//
// On a fan-out, one slow branch never fails the push: its shed is reported on
// the Result while delivery continues to siblings. Any other error is fatal to
// the push.
type Push struct {
	ctx     context.Context
	emitter pipeline.Emitter
	stream  av.StreamID
}

// NewPush binds a pipeline emitter to one source stream. Runtime code uses it
// to call a Func; application code normally receives Push from goav.Source.
func NewPush(ctx context.Context, emitter pipeline.Emitter, stream av.StreamID) Push {
	return Push{ctx: ctx, emitter: emitter, stream: stream}
}

// Packet delivers one packet. Nil is a no-op. If packet.StreamID is empty, the
// source's declared stream id is applied before delivery.
func (p *Push) Packet(packet *av.Packet) (Result, error) {
	if packet == nil {
		return Result{}, nil
	}
	if packet.StreamID == "" {
		packet.StreamID = p.stream
	}
	msg := packetMessage(packet)
	return p.emitDelivery(&msg)
}

// Frame delivers one decoded frame. Nil is a no-op. If frame.StreamID is empty,
// the source's declared stream id is applied before delivery.
func (p *Push) Frame(frame *av.Frame) (Result, error) {
	if frame == nil {
		return Result{}, nil
	}
	if frame.StreamID == "" {
		frame.StreamID = p.stream
	}
	msg := frameMessage(frame)
	return p.emitDelivery(&msg)
}

// Event delivers one out-of-band event. It is also the dynamic stream announce
// seam: a source that discovers a stream mid-run pushes
// av.Event{Type: av.EventStreamAdded, Stream: &stream} before that stream's
// media, and av.Event{Type: av.EventStreamRemoved, StreamID: id} when it ends.
// Events with an empty StreamID inherit the source's declared stream id, so
// stream announces must set StreamID (or Stream.ID) explicitly.
func (p *Push) Event(event av.Event) (Result, error) {
	if event.StreamID == "" && event.Stream != nil {
		event.StreamID = event.Stream.ID
	}
	if event.StreamID == "" {
		event.StreamID = p.stream
	}
	msg := eventMessage(event)
	return p.emitDelivery(&msg)
}

// EOS ends the given streams (or the source's declared stream when none are
// listed): downstream nodes flush and destinations finalize naturally.
func (p *Push) EOS(streams ...av.StreamID) error {
	if len(streams) == 0 && p.stream != "" {
		streams = []av.StreamID{p.stream}
	}
	if len(streams) == 0 {
		return p.eventOnly(av.Event{Type: av.EventEndOfStream})
	}
	for i := range streams {
		if err := p.eventOnly(av.Event{Type: av.EventEndOfStream, StreamID: streams[i]}); err != nil {
			return err
		}
	}
	return nil
}

func (p *Push) emitDelivery(msg *pipeline.Message) (Result, error) {
	if de, ok := p.emitter.(pipeline.DeliveryEmitter); ok {
		delivery, err := de.EmitDelivery(p.ctx, msg)
		return Result{Accepted: delivery.Delivered > 0, Dropped: delivery.Shed > 0}, err
	}
	err := p.emitter.Emit(p.ctx, msg)
	return Result{Accepted: err == nil}, err
}

func (p *Push) eventOnly(event av.Event) error {
	msg := eventMessage(event)
	return p.emitter.Emit(p.ctx, &msg)
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
