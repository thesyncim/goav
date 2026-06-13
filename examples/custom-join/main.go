package main

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

func main() {
	frames, err := runCustomJoin(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println("joined:", frames)
}

func runCustomJoin(ctx context.Context) ([][]int16, error) {
	out := goavtest.NewCollector()
	err := goav.Join("interleave", newInterleaveStage("interleave", "left", "right"),
		goav.From(s16Source("left", []int16{1}, []int16{3})).Audio(),
		goav.From(s16Source("right", []int16{2}, []int16{4})).Audio(),
	).To(out.Sink()).
		UseRuntime(goavtest.Runtime()).
		Run(ctx)
	return out.S16(), err
}

type interleaveStage struct {
	name    string
	arms    []av.StreamID
	pending map[av.StreamID][]*av.Frame
	eos     map[av.StreamID]struct{}
}

func newInterleaveStage(name string, arms ...av.StreamID) *interleaveStage {
	return &interleaveStage{
		name:    name,
		arms:    arms,
		pending: make(map[av.StreamID][]*av.Frame, len(arms)),
		eos:     make(map[av.StreamID]struct{}, len(arms)),
	}
}

func (s *interleaveStage) Name() string { return s.name }

func (s *interleaveStage) InputShapes() shape.Set {
	return shape.Set{shape.Frame(av.MediaAudio, shape.Audio(8000, 1, av.SampleFormatS16))}
}

func (s *interleaveStage) OutputShapes(shape.Spec) shape.Set {
	return shape.Set{shape.Frame(av.MediaAudio, shape.Audio(8000, 1, av.SampleFormatS16))}
}

func (s *interleaveStage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	switch msg.Kind {
	case pipeline.MessageFrame:
		if msg.Frame != nil {
			s.pending[msg.Frame.StreamID] = append(s.pending[msg.Frame.StreamID], cloneFrame(msg.Frame))
		}
		return s.drain(ctx, emitter)
	case pipeline.MessageEvent:
		if msg.Event == nil || msg.Event.Type != av.EventEndOfStream {
			return nil
		}
		s.eos[msg.Event.StreamID] = struct{}{}
		if err := s.drain(ctx, emitter); err != nil {
			return err
		}
		if len(s.eos) >= len(s.arms) {
			event := av.Event{Type: av.EventEndOfStream, StreamID: av.StreamID(s.name)}
			return emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageEvent, Event: &event})
		}
	}
	return nil
}

func (s *interleaveStage) drain(ctx context.Context, emitter pipeline.Emitter) error {
	for s.roundReady() {
		for _, arm := range s.arms {
			queue := s.pending[arm]
			if len(queue) == 0 {
				continue
			}
			frame := queue[0]
			s.pending[arm] = queue[1:]
			frame.StreamID = av.StreamID(s.name)
			if err := emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: frame}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *interleaveStage) roundReady() bool {
	anyPending := false
	for _, arm := range s.arms {
		if len(s.pending[arm]) != 0 {
			anyPending = true
			continue
		}
		if _, ended := s.eos[arm]; !ended {
			return false
		}
	}
	return anyPending
}

func (s *interleaveStage) Close() error { return nil }

func s16Source(id av.StreamID, frames ...[]int16) goav.InputSpec {
	return goav.Source(string(id),
		shape.Frame(av.MediaAudio, shape.Audio(8000, 1, av.SampleFormatS16), shape.Stream(id)),
		func(_ context.Context, push goav.SourcePush) error {
			var elapsed int64
			for _, samples := range frames {
				frame := av.Frame{
					StreamID: id,
					Type:     av.MediaAudio,
					PTS:      av.Timestamp{Value: elapsed, Base: av.TimeBase{Num: 1, Den: 8000}},
					Duration: av.Duration{Value: int64(len(samples)), Base: av.TimeBase{Num: 1, Den: 8000}},
					Audio:    &av.AudioFrame{SampleRate: 8000, Channels: 1, SampleFormat: av.SampleFormatS16, Samples: len(samples)},
					Planes:   []av.Plane{{Buffer: av.Buffer{Bytes: encodeS16(samples), Ownership: av.BufferImmutable}}},
				}
				elapsed += int64(len(samples))
				if _, err := push.Frame(&frame); err != nil {
					return err
				}
			}
			return push.EOS()
		})
}

func cloneFrame(frame *av.Frame) *av.Frame {
	clone := *frame
	if frame.Audio != nil {
		audio := *frame.Audio
		clone.Audio = &audio
	}
	clone.Planes = make([]av.Plane, len(frame.Planes))
	for i := range frame.Planes {
		clone.Planes[i] = frame.Planes[i]
		clone.Planes[i].Buffer.Bytes = append([]byte(nil), frame.Planes[i].Buffer.Bytes...)
		clone.Planes[i].Buffer.Ownership = av.BufferOwned
	}
	return &clone
}

func encodeS16(samples []int16) []byte {
	out := make([]byte, len(samples)*2)
	for i, sample := range samples {
		binary.LittleEndian.PutUint16(out[i*2:], uint16(sample))
	}
	return out
}

var _ pipeline.Stage = (*interleaveStage)(nil)
