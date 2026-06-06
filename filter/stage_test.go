package filter

import (
	"context"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

type fakeFactory struct {
	filter *fakeFilter
	config Config
}

func (f *fakeFactory) NewFilter(_ context.Context, config Config) (FrameFilter, error) {
	f.config = config
	if f.filter == nil {
		f.filter = &fakeFilter{}
	}
	return f.filter, nil
}

type fakeFilter struct {
	frames  int
	events  int
	flushes int
	closed  bool
}

func (f *fakeFilter) Descriptor() Descriptor {
	return Descriptor{Name: FactoryResample, Input: av.MediaAudio, Output: av.MediaAudio}
}

func (f *fakeFilter) Open(context.Context, Config) error {
	return nil
}

func (f *fakeFilter) FilterInto(_ context.Context, frame *av.Frame, out *Result) error {
	if frame == nil {
		return nil
	}
	if len(out.Frames) == cap(out.Frames) {
		return ErrResultFull
	}
	index := len(out.Frames)
	out.Frames = out.Frames[:index+1]
	outFrame := &out.Frames[index]
	outFrame.Reset()
	*outFrame = *frame
	outFrame.StreamID = frame.StreamID
	f.frames++
	return nil
}

func (f *fakeFilter) FlushInto(_ context.Context, out *Result) error {
	f.flushes++
	if len(out.Frames) == cap(out.Frames) {
		return ErrResultFull
	}
	index := len(out.Frames)
	out.Frames = out.Frames[:index+1]
	out.Frames[index].Reset()
	out.Frames[index].StreamID = "flushed"
	return nil
}

func (f *fakeFilter) HandleEvent(context.Context, *av.Event) error {
	f.events++
	return nil
}

func (f *fakeFilter) Close() error {
	f.closed = true
	return nil
}

type stageEmitter struct {
	frames     int
	packets    int
	events     int
	lastFrame  av.StreamID
	lastEvent  av.EventType
	order      [2]pipeline.MessageKind
	orderCount int
}

func (e *stageEmitter) Emit(_ context.Context, msg *pipeline.Message) error {
	switch msg.Kind {
	case pipeline.MessageFrame:
		e.frames++
		if msg.Frame != nil {
			e.lastFrame = msg.Frame.StreamID
		}
	case pipeline.MessagePacket:
		e.packets++
	case pipeline.MessageEvent:
		e.events++
		if msg.Event != nil {
			e.lastEvent = msg.Event.Type
		}
	}
	if e.orderCount < len(e.order) {
		e.order[e.orderCount] = msg.Kind
		e.orderCount++
	}
	return nil
}

func (e *stageEmitter) Reset() {
	e.frames = 0
	e.packets = 0
	e.events = 0
	e.lastFrame = ""
	e.lastEvent = ""
	e.order = [2]pipeline.MessageKind{}
	e.orderCount = 0
}

func TestStageEmitsFilteredFrames(t *testing.T) {
	filter := &fakeFilter{}
	stage, err := NewStage(StageConfig{
		Filter: filter,
		Result: Result{Frames: make([]av.Frame, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	frame := av.Frame{StreamID: "audio"}
	message := pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame}
	emitter := &stageEmitter{}

	if err := stage.Handle(context.Background(), &message, emitter); err != nil {
		t.Fatal(err)
	}
	if filter.frames != 1 || emitter.frames != 1 || emitter.lastFrame != "audio" {
		t.Fatalf("filter frames=%d emitter frames=%d last=%s", filter.frames, emitter.frames, emitter.lastFrame)
	}
}

func TestStageFlushesBeforeEOS(t *testing.T) {
	filter := &fakeFilter{}
	stage, err := NewStage(StageConfig{
		Filter: filter,
		Result: Result{Frames: make([]av.Frame, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	event := av.Event{Type: av.EventEndOfStream, StreamID: "audio"}
	message := pipeline.Message{Kind: pipeline.MessageEvent, Event: &event}
	emitter := &stageEmitter{}

	if err := stage.Handle(context.Background(), &message, emitter); err != nil {
		t.Fatal(err)
	}
	if filter.events != 1 || filter.flushes != 1 {
		t.Fatalf("events=%d flushes=%d", filter.events, filter.flushes)
	}
	if emitter.frames != 1 || emitter.events != 1 {
		t.Fatalf("frames=%d events=%d", emitter.frames, emitter.events)
	}
	if emitter.order != [2]pipeline.MessageKind{pipeline.MessageFrame, pipeline.MessageEvent} {
		t.Fatalf("order = %+v", emitter.order)
	}
}

func TestStageCanDropInputEvents(t *testing.T) {
	stage, err := NewStage(StageConfig{
		Filter:          &fakeFilter{},
		Result:          Result{Frames: make([]av.Frame, 0, 1)},
		DropInputEvents: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	event := av.Event{Type: av.EventStats, StreamID: "audio"}
	message := pipeline.Message{Kind: pipeline.MessageEvent, Event: &event}
	emitter := &stageEmitter{}

	if err := stage.Handle(context.Background(), &message, emitter); err != nil {
		t.Fatal(err)
	}
	if emitter.events != 0 || emitter.frames != 0 {
		t.Fatalf("events=%d frames=%d", emitter.events, emitter.frames)
	}
}

func TestStagePassesPacketsThrough(t *testing.T) {
	stage, err := NewStage(StageConfig{Filter: &fakeFilter{}})
	if err != nil {
		t.Fatal(err)
	}
	packet := av.Packet{StreamID: "audio"}
	message := pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet}
	emitter := &stageEmitter{}

	if err := stage.Handle(context.Background(), &message, emitter); err != nil {
		t.Fatal(err)
	}
	if emitter.packets != 1 {
		t.Fatalf("packets = %d, want 1", emitter.packets)
	}
}

func TestStageClose(t *testing.T) {
	filter := &fakeFilter{}
	stage, err := NewStage(StageConfig{Filter: filter})
	if err != nil {
		t.Fatal(err)
	}
	if err := stage.Close(); err != nil {
		t.Fatal(err)
	}
	if !filter.closed {
		t.Fatal("filter not closed")
	}
	if err := stage.Handle(context.Background(), &pipeline.Message{}, &stageEmitter{}); err != pipeline.ErrClosed {
		t.Fatalf("handle after close = %v, want %v", err, pipeline.ErrClosed)
	}
}

func TestStageAllocs(t *testing.T) {
	stage, err := NewStage(StageConfig{
		Filter: &fakeFilter{},
		Result: Result{Frames: make([]av.Frame, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	frame := av.Frame{StreamID: "audio"}
	message := pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame}
	emitter := &stageEmitter{}

	if allocs := testing.AllocsPerRun(1000, func() {
		emitter.Reset()
		if err := stage.Handle(context.Background(), &message, emitter); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("stage allocs = %v, want 0", allocs)
	}
}
