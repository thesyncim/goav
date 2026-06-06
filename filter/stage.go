package filter

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

type StageConfig struct {
	Name string
	// Filter receives frames and events and writes transformed frames into Result.
	Filter FrameFilter
	// Result is caller-owned scratch. Its slice capacities define how many
	// frames and events can be emitted per input message.
	Result Result
	// DropInputEvents suppresses forwarding upstream events after the filter
	// has observed them. By default, events stay visible downstream.
	DropInputEvents bool
}

type Stage struct {
	name       string
	filter     FrameFilter
	result     Result
	message    pipeline.Message
	dropEvents bool
	closed     bool
}

var _ pipeline.Stage = (*Stage)(nil)

func NewStage(config StageConfig) (*Stage, error) {
	if config.Filter == nil {
		return nil, ErrNilFilter
	}
	name := config.Name
	if name == "" {
		name = config.Filter.Descriptor().Name
	}
	if name == "" {
		name = "filter"
	}
	return &Stage{
		name:       name,
		filter:     config.Filter,
		result:     config.Result,
		dropEvents: config.DropInputEvents,
	}, nil
}

func (s *Stage) Name() string {
	return s.name
}

func (s *Stage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return pipeline.ErrClosed
	}
	if msg == nil {
		return nil
	}
	switch msg.Kind {
	case pipeline.MessageFrame:
		if msg.Frame == nil {
			return nil
		}
		s.result.Reset()
		if err := s.filter.FilterInto(ctx, msg.Frame, &s.result); err != nil {
			return err
		}
		return s.emitResult(ctx, emitter)
	case pipeline.MessageEvent:
		return s.handleEvent(ctx, msg, emitter)
	case pipeline.MessagePacket:
		return emitter.Emit(ctx, msg)
	default:
		return nil
	}
}

func (s *Stage) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.filter.Close()
}

func (s *Stage) handleEvent(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	if msg.Event == nil {
		return nil
	}
	if err := s.filter.HandleEvent(ctx, msg.Event); err != nil {
		return err
	}

	if msg.Event.Type == av.EventEndOfStream {
		s.result.Reset()
		if err := s.filter.FlushInto(ctx, &s.result); err != nil {
			return err
		}
		if err := s.emitResult(ctx, emitter); err != nil {
			return err
		}
		return s.emitInputEvent(ctx, msg, emitter)
	}
	return s.emitInputEvent(ctx, msg, emitter)
}

func (s *Stage) emitInputEvent(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	if s.dropEvents {
		return nil
	}
	return emitter.Emit(ctx, msg)
}

func (s *Stage) emitResult(ctx context.Context, emitter pipeline.Emitter) error {
	for i := range s.result.Events {
		s.message.Kind = pipeline.MessageEvent
		s.message.Packet = nil
		s.message.Frame = nil
		s.message.Event = &s.result.Events[i]
		if err := emitter.Emit(ctx, &s.message); err != nil {
			return err
		}
	}
	for i := range s.result.Frames {
		s.message.Kind = pipeline.MessageFrame
		s.message.Packet = nil
		s.message.Frame = &s.result.Frames[i]
		s.message.Event = nil
		if err := emitter.Emit(ctx, &s.message); err != nil {
			return err
		}
	}
	return nil
}
