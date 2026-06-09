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
type selectorStage struct {
	name   string
	inputs []av.StreamID
	out    av.StreamID
	active atomic.Pointer[av.StreamID]
	eos    map[av.StreamID]struct{}
}

func newSelectorStage(name string, inputs []av.StreamID, out av.StreamID) *selectorStage {
	s := &selectorStage{
		name:   name,
		inputs: append([]av.StreamID(nil), inputs...),
		out:    out,
		eos:    make(map[av.StreamID]struct{}, len(inputs)),
	}
	if len(s.inputs) > 0 {
		id := s.inputs[0]
		s.active.Store(&id)
	}
	return s
}

func (s *selectorStage) Name() string { return s.name }

// SetActive switches the forwarded input. It is safe to call concurrently with
// Handle: the new id is published with a single atomic store. An id that is not
// one of the configured inputs is rejected so a typo cannot silently mute the
// output.
func (s *selectorStage) SetActive(id av.StreamID) error {
	for i := range s.inputs {
		if s.inputs[i] == id {
			set := id
			s.active.Store(&set)
			return nil
		}
	}
	return fmt.Errorf("goav: selector %q has no input %q", s.name, id)
}

// activeID returns the currently active input id (empty if none configured).
func (s *selectorStage) activeID() av.StreamID {
	if p := s.active.Load(); p != nil {
		return *p
	}
	return ""
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
		clone := *msg.Frame
		clone.StreamID = s.out
		return emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: &clone})
	case pipeline.MessagePacket:
		if msg.Packet == nil || msg.Packet.StreamID != active {
			return nil
		}
		clone := *msg.Packet
		clone.StreamID = s.out
		return emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessagePacket, Packet: &clone})
	case pipeline.MessageEvent:
		if msg.Event == nil {
			return nil
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
