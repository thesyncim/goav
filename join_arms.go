// Join arms: what an arm may declare, its taps, and the restamp that
// republishes an arm under its id.

package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

// armRestampStage republishes an arm's stream under the arm's id — the
// runtime body of a tap arm and of an arm bound when its input opens. Join
// stages identify arms by stream id: a tap's own media keeps flowing down its
// declaring arm unchanged, and two files carry the same track ids, so the
// converged copy is re-stamped: a shallow per-message copy (payloads are
// shared exactly like every other fanout; downstream joins clone what they
// buffer). With in set, only that stream and stream-less events pass: a
// file's source carries all its streams.
type armRestampStage struct {
	name   string
	detail string
	in     av.StreamID
	out    av.StreamID
}

func newArmRestampStage(name string, detail string, in av.StreamID, out av.StreamID) *armRestampStage {
	return &armRestampStage{name: name, detail: detail, in: in, out: out}
}

func (s *armRestampStage) Name() string { return s.name }

// DescribeNode reports the same detail the join planner records on the
// planned restamp node, keeping Describe() ≡ Build().
func (s *armRestampStage) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: s.name, Kind: pipeline.NodeStage, Detail: s.detail}
}

func (s *armRestampStage) passes(id av.StreamID, event bool) bool {
	return s.in == "" || id == s.in || (event && id == "")
}

func (s *armRestampStage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	switch msg.Kind {
	case pipeline.MessageFrame:
		if msg.Frame == nil || !s.passes(msg.Frame.StreamID, false) {
			return nil
		}
		frame := *msg.Frame
		frame.StreamID = s.out
		return emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame})
	case pipeline.MessagePacket:
		if msg.Packet == nil || !s.passes(msg.Packet.StreamID, false) {
			return nil
		}
		packet := *msg.Packet
		packet.StreamID = s.out
		return emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet})
	case pipeline.MessageEvent:
		if msg.Event == nil {
			return nil
		}
		if msg.Event.Reason == selectorActiveReason {
			// Control-plane events ride the data path unchanged: a SelectActive
			// heading for a downstream selector carries its target arm in
			// Event.StreamID — re-stamping it would erase the target.
			return emitter.Emit(ctx, msg)
		}
		if !s.passes(msg.Event.StreamID, true) {
			return nil
		}
		event := *msg.Event
		event.StreamID = s.out
		if event.Stream != nil {
			stream := *event.Stream
			stream.ID = s.out
			event.Stream = &stream
		}
		return emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageEvent, Event: &event})
	default:
		return emitter.Emit(ctx, msg)
	}
}

func (s *armRestampStage) Close() error { return nil }
