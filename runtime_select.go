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
	streamID av.StreamID
	closed   bool
}

var _ pipeline.Stage = (*streamSelectStage)(nil)
var _ pipeline.NodeDescriber = (*streamSelectStage)(nil)

func newStreamSelectStage(name string, streamID av.StreamID, detail string) *streamSelectStage {
	return &streamSelectStage{name: name, detail: detail, streamID: streamID}
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
		return msg.Packet != nil && msg.Packet.StreamID == s.streamID
	case pipeline.MessageFrame:
		return msg.Frame != nil && msg.Frame.StreamID == s.streamID
	case pipeline.MessageEvent:
		if msg.Event == nil {
			return false
		}
		return msg.Event.StreamID == "" || msg.Event.StreamID == s.streamID
	default:
		return false
	}
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
