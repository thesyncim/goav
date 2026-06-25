package goav

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

// selectorStage forwards messages from exactly one of N inputs to a single
// output and drops the rest — the runtime heart of a one-of-N Select switch
// (active-speaker camera, simulcast layer switch, failover). Each input arm is
// identified by its frame/packet StreamID.
//
// It is a normal pipeline.Stage: the buffered runner calls Handle serially per
// node, so the per-input EOS bookkeeping needs no locking (lock-free by design —
// the hot path takes no mutex). The active input lives in an atomic pointer so
// SetActive may be called concurrently with Handle without a mutex: the read on
// the forwarding hot path is a plain atomic load.
//
// EOS is forwarded like the mixer: one output EOS is emitted only after every
// input has ended. Non-EOS events are forwarded only from the currently active
// input so the downstream sees a single coherent stream.
//
// A switch is driven live through the control plane: an injected event carrying
// selectorActiveReason names the target input in Event.StreamID, calls SetActive,
// and is consumed (not forwarded). control.SelectActive builds that control.
type selectorStage struct {
	name        string
	inputs      []av.StreamID
	out         av.StreamID
	activeIndex atomic.Int32
	eos         map[av.StreamID]struct{}
	frame       av.Frame
	packet      av.Packet
	message     pipeline.Message
}

// selectorActiveReason marks an injected control event as a live switch request:
// the selector reads Event.StreamID as the input to make active and consumes the
// event instead of forwarding it. control.SelectActive stamps this reason.
const selectorActiveReason = "select.active"

func newSelectorStage(name string, inputs []av.StreamID, out av.StreamID) *selectorStage {
	return &selectorStage{
		name:   name,
		inputs: append([]av.StreamID(nil), inputs...),
		out:    out,
		eos:    make(map[av.StreamID]struct{}, len(inputs)),
	}
}

func (s *selectorStage) Name() string { return s.name }

// SetActive switches the forwarded input. It is safe to call concurrently with
// Handle: the new id is published with a single atomic store. An id that is not
// one of the configured inputs is rejected so a typo cannot silently mute the
// output.
func (s *selectorStage) SetActive(id av.StreamID) error {
	for i := range s.inputs {
		if s.inputs[i] == id {
			s.activeIndex.Store(int32(i))
			return nil
		}
	}
	return fmt.Errorf("goav: selector %q has no input %q (inputs: %s); switch with control.SelectActive to one of the configured arm ids", s.name, id, joinStreamIDs(s.inputs))
}

// joinStreamIDs renders the configured arm ids for the SetActive refusal.
func joinStreamIDs(ids []av.StreamID) string {
	out := ""
	for i := range ids {
		if i > 0 {
			out += ", "
		}
		out += string(ids[i])
	}
	return out
}

// activeID returns the currently active input id (empty if none configured).
func (s *selectorStage) activeID() av.StreamID {
	if len(s.inputs) == 0 {
		return ""
	}
	index := int(s.activeIndex.Load())
	if index < 0 || index >= len(s.inputs) {
		return ""
	}
	return s.inputs[index]
}

func (s *selectorStage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	active := s.activeID()
	switch msg.Kind {
	case pipeline.MessageFrame:
		if msg.Frame == nil || msg.Frame.StreamID != active {
			return nil
		}
		// Rewrite the StreamID on a shallow header copy — the payload buffer may
		// be borrowed/shared, so mutating the original frame's id could corrupt
		// another consumer.
		s.frame = *msg.Frame
		s.frame.StreamID = s.out
		s.message.Kind = pipeline.MessageFrame
		s.message.Packet = nil
		s.message.Frame = &s.frame
		s.message.Event = nil
		return emitter.Emit(ctx, &s.message)
	case pipeline.MessagePacket:
		if msg.Packet == nil || msg.Packet.StreamID != active {
			return nil
		}
		s.packet = *msg.Packet
		s.packet.StreamID = s.out
		s.message.Kind = pipeline.MessagePacket
		s.message.Packet = &s.packet
		s.message.Frame = nil
		s.message.Event = nil
		return emitter.Emit(ctx, &s.message)
	case pipeline.MessageEvent:
		if msg.Event == nil {
			return nil
		}
		// A live switch request (injected via the control plane) names the input to
		// make active in Event.StreamID. It is consumed here, never forwarded.
		if msg.Event.Reason == selectorActiveReason {
			return s.SetActive(msg.Event.StreamID)
		}
		if msg.Event.Type == av.EventEndOfStream {
			s.eos[msg.Event.StreamID] = struct{}{}
			if len(s.eos) >= len(s.inputs) {
				out := av.Event{Type: av.EventEndOfStream, StreamID: s.out}
				return emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageEvent, Event: &out})
			}
			return nil
		}
		// Non-EOS events forward only from the active input, rewritten to the
		// output id so downstream sees one coherent stream.
		if msg.Event.StreamID != active {
			return nil
		}
		out := *msg.Event
		out.StreamID = s.out
		return emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageEvent, Event: &out})
	default:
		return nil
	}
}

func (s *selectorStage) Close() error { return nil }
