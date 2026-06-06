package goav

import (
	"context"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

type streamSelectStage struct {
	name     string
	detail   string
	stream   av.Stream
	selector av.StreamSelector
	closed   bool
}

var _ pipeline.Stage = (*streamSelectStage)(nil)
var _ pipeline.NodeDescriber = (*streamSelectStage)(nil)

func newStreamSelectStage(name string, stream av.Stream, selector av.StreamSelector, detail string) *streamSelectStage {
	return &streamSelectStage{name: name, detail: detail, stream: stream, selector: selector}
}

func (s *streamSelectStage) Name() string {
	return s.name
}

func (s *streamSelectStage) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: s.name, Kind: pipeline.NodeStage, Detail: s.detail}
}

func (s *streamSelectStage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return pipeline.ErrClosed
	}
	if msg == nil {
		return nil
	}
	if !s.matches(msg) {
		return nil
	}
	return emitter.Emit(ctx, msg)
}

func (s *streamSelectStage) Close() error {
	s.closed = true
	return nil
}

func (s *streamSelectStage) matches(msg *pipeline.Message) bool {
	switch msg.Kind {
	case pipeline.MessagePacket:
		return msg.Packet != nil && msg.Packet.StreamID == s.stream.ID
	case pipeline.MessageFrame:
		return msg.Frame != nil && msg.Frame.StreamID == s.stream.ID
	case pipeline.MessageEvent:
		if msg.Event == nil {
			return false
		}
		return s.matchesEvent(msg.Event)
	default:
		return false
	}
}

func (s *streamSelectStage) matchesEvent(event *av.Event) bool {
	if event.Type != av.EventCodecChanged || event.Stream == nil {
		return event.StreamID == "" || event.StreamID == s.stream.ID
	}
	next := selectStreamWithCodecChangedEvent(s.stream, event)
	if event.StreamID == "" || event.StreamID == s.stream.ID {
		if streamMatchesSelector(next, s.selector) {
			s.stream = next
		}
		return true
	}
	if !streamMatchesSelector(next, s.selector) {
		return false
	}
	s.stream = next
	return true
}

func selectStreamWithCodecChangedEvent(stream av.Stream, event *av.Event) av.Stream {
	next := stream
	if event.Stream != nil {
		next = *event.Stream
	}
	if next.ID == "" {
		if event.StreamID != "" {
			next.ID = event.StreamID
		} else {
			next.ID = stream.ID
		}
	}
	if next.Type == "" {
		next.Type = stream.Type
	}
	if next.Codec.ID == "" {
		next.Codec = stream.Codec
	}
	if event.Codec != nil {
		next.Codec = *event.Codec
		if next.Codec.Type != "" {
			next.Type = next.Codec.Type
		}
	}
	if next.TimeBase == (av.TimeBase{}) {
		next.TimeBase = stream.TimeBase
	}
	if next.Epoch == 0 && event.Epoch != 0 {
		next.Epoch = event.Epoch
	}
	return next
}

func selectNodeName(selector av.StreamSelector) string {
	if selector.Name != "" {
		return "select-" + selector.Name
	}
	if selector.ID != "" {
		return "select-" + string(selector.ID)
	}
	if selector.Codec != "" {
		return "select-" + string(selector.Codec)
	}
	if selector.Type != "" {
		return "select-" + string(selector.Type)
	}
	if selector.Index != 0 {
		return "select-" + strconv.Itoa(selector.Index)
	}
	return "select"
}
