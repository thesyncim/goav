package engine

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

// Sink is the goav sink feeding an engine: one serial Handle receives both
// the audio and the video branch.
type Sink struct {
	Engine *Engine
}

var _ pipeline.Sink = Sink{}

// Name is the sink node's name.
func (Sink) Name() string { return "player" }

// Handle routes decoded frames to the engine's queues; a discontinuity (a
// seek landed) flushes them once per seek, so the new position re-prerolls.
func (s Sink) Handle(ctx context.Context, msg *pipeline.Message) error {
	switch {
	case msg == nil:
	case msg.Kind == pipeline.MessageEvent && msg.Event != nil:
		switch msg.Event.Type {
		case av.EventDiscontinuity:
			s.Engine.Discontinuity()
		case av.EventEndOfStream:
			// The source ended: what is queued plays out, and the last of
			// it running dry is the end, not an underrun.
			s.Engine.End()
		}
	case msg.Kind == pipeline.MessageFrame && msg.Frame != nil:
		switch {
		case msg.Frame.Audio != nil || msg.Frame.Type == av.MediaAudio:
			return s.Engine.PushAudio(ctx, msg.Frame)
		case msg.Frame.Video != nil || msg.Frame.Type == av.MediaVideo:
			return s.Engine.PushVideo(ctx, msg.Frame)
		}
	}
	return nil
}

// Close marks the input ended; the engine keeps playing out what it holds.
func (s Sink) Close() error {
	s.Engine.End()
	return nil
}
