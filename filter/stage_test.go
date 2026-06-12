package filter

import (
	"context"
	"errors"
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

type scriptedFilter struct {
	desc         Descriptor
	filterErr    error
	flushErr     error
	eventErr     error
	closeErr     error
	filterFrames []av.Frame
	filterEvents []av.Event
	flushFrames  []av.Frame
	flushEvents  []av.Event
	events       int
	closes       int
}

func (f *scriptedFilter) Descriptor() Descriptor {
	return f.desc
}

func (f *scriptedFilter) Open(context.Context, Config) error {
	return nil
}

func (f *scriptedFilter) FilterInto(_ context.Context, _ *av.Frame, out *Result) error {
	if f.filterErr != nil {
		return f.filterErr
	}
	out.Events = append(out.Events, f.filterEvents...)
	out.Frames = append(out.Frames, f.filterFrames...)
	return nil
}

func (f *scriptedFilter) FlushInto(_ context.Context, out *Result) error {
	if f.flushErr != nil {
		return f.flushErr
	}
	out.Events = append(out.Events, f.flushEvents...)
	out.Frames = append(out.Frames, f.flushFrames...)
	return nil
}

func (f *scriptedFilter) HandleEvent(context.Context, *av.Event) error {
	f.events++
	return f.eventErr
}

func (f *scriptedFilter) Close() error {
	f.closes++
	return f.closeErr
}

type stageEmitter struct {
	frames     int
	packets    int
	events     int
	lastFrame  av.StreamID
	lastEvent  av.EventType
	order      [2]pipeline.MessageKind
	orderCount int
	err        error
	failKind   pipeline.MessageKind
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
	if e.err != nil && (e.failKind == "" || e.failKind == msg.Kind) {
		return e.err
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
	e.err = nil
	e.failKind = ""
}

func TestResultResetClearsFramesAndEvents(t *testing.T) {
	result := Result{
		Frames: []av.Frame{{
			StreamID: "video",
			Planes:   make([]av.Plane, 1, 4),
			Metadata: av.Metadata{"color": "bt709"},
		}},
		Events: []av.Event{{
			Type:     av.EventStats,
			StreamID: "video",
			Metadata: av.Metadata{"frames": "1"},
		}},
	}
	frameCap := cap(result.Frames)
	eventCap := cap(result.Events)

	result.Reset()

	if len(result.Frames) != 0 || cap(result.Frames) != frameCap {
		t.Fatalf("frames len/cap = %d/%d, want 0/%d", len(result.Frames), cap(result.Frames), frameCap)
	}
	if len(result.Events) != 0 || cap(result.Events) != eventCap {
		t.Fatalf("events len/cap = %d/%d, want 0/%d", len(result.Events), cap(result.Events), eventCap)
	}
	if len(result.Frames[:1][0].Planes) != 0 || result.Frames[:1][0].Metadata != nil {
		t.Fatalf("frame scratch was not reset: %+v", result.Frames[:1][0])
	}
	if result.Events[:1][0].Type != "" || result.Events[:1][0].Metadata != nil {
		t.Fatalf("event scratch was not reset: %+v", result.Events[:1][0])
	}
}

func TestNewStageNamesAndDescription(t *testing.T) {
	if _, err := NewStage(StageConfig{}); err != ErrNilFilter {
		t.Fatalf("nil filter err = %v, want %v", err, ErrNilFilter)
	}

	stage, err := NewStage(StageConfig{
		Name:   "custom-resizer",
		Detail: "640x360 exact",
		Filter: &fakeFilter{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if stage.Name() != "custom-resizer" {
		t.Fatalf("name = %q", stage.Name())
	}
	spec := stage.DescribeNode()
	if spec.Name != "custom-resizer" || spec.Kind != pipeline.NodeStage || spec.Detail != "640x360 exact" {
		t.Fatalf("spec = %+v", spec)
	}

	fromDescriptor, err := NewStage(StageConfig{Filter: &fakeFilter{}})
	if err != nil {
		t.Fatal(err)
	}
	if fromDescriptor.Name() != FactoryResample {
		t.Fatalf("descriptor name = %q, want %q", fromDescriptor.Name(), FactoryResample)
	}

	fallback, err := NewStage(StageConfig{Filter: &scriptedFilter{}})
	if err != nil {
		t.Fatal(err)
	}
	if fallback.Name() != "filter" {
		t.Fatalf("fallback name = %q, want filter", fallback.Name())
	}
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

func TestStageEmitsResultEventsBeforeFrames(t *testing.T) {
	stage, err := NewStage(StageConfig{
		Filter: &scriptedFilter{
			filterEvents: []av.Event{{Type: av.EventStats, StreamID: "video"}},
			filterFrames: []av.Frame{{StreamID: "filtered"}},
		},
		Result: Result{
			Frames: make([]av.Frame, 0, 1),
			Events: make([]av.Event, 0, 1),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	frame := av.Frame{StreamID: "source"}
	message := pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame}
	emitter := &stageEmitter{}

	if err := stage.Handle(context.Background(), &message, emitter); err != nil {
		t.Fatal(err)
	}
	if emitter.events != 1 || emitter.frames != 1 {
		t.Fatalf("events=%d frames=%d", emitter.events, emitter.frames)
	}
	if emitter.lastEvent != av.EventStats || emitter.lastFrame != "filtered" {
		t.Fatalf("last event/frame = %s/%s", emitter.lastEvent, emitter.lastFrame)
	}
	if emitter.order != [2]pipeline.MessageKind{pipeline.MessageEvent, pipeline.MessageFrame} {
		t.Fatalf("order = %+v", emitter.order)
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

func TestStagePassesEventsThrough(t *testing.T) {
	stage, err := NewStage(StageConfig{
		Filter: &fakeFilter{},
		Result: Result{Frames: make([]av.Frame, 0, 1)},
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
	if emitter.events != 1 || emitter.lastEvent != av.EventStats || emitter.frames != 0 {
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

func TestStageNoopInputs(t *testing.T) {
	filter := &fakeFilter{}
	stage, err := NewStage(StageConfig{Filter: filter})
	if err != nil {
		t.Fatal(err)
	}
	emitter := &stageEmitter{}

	if err := stage.Handle(context.Background(), nil, emitter); err != nil {
		t.Fatal(err)
	}
	if err := stage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessageFrame}, emitter); err != nil {
		t.Fatal(err)
	}
	if err := stage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessageEvent}, emitter); err != nil {
		t.Fatal(err)
	}
	if err := stage.Handle(context.Background(), &pipeline.Message{}, emitter); err != nil {
		t.Fatal(err)
	}
	if filter.frames != 0 || filter.events != 0 || emitter.frames != 0 || emitter.events != 0 || emitter.packets != 0 {
		t.Fatalf("unexpected work: filter frames=%d events=%d emitter=%+v", filter.frames, filter.events, emitter)
	}
}

func TestStageReturnsContextError(t *testing.T) {
	stage, err := NewStage(StageConfig{Filter: &fakeFilter{}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := stage.Handle(ctx, &pipeline.Message{}, &stageEmitter{}); err != context.Canceled {
		t.Fatalf("err = %v, want %v", err, context.Canceled)
	}
}

func TestStageForwardsFilterAndEmitterErrors(t *testing.T) {
	errFilter := errors.New("filter failed")
	stage, err := NewStage(StageConfig{
		Filter: &scriptedFilter{filterErr: errFilter},
		Result: Result{Frames: make([]av.Frame, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	frame := av.Frame{StreamID: "video"}
	if err := stage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame}, &stageEmitter{}); err != errFilter {
		t.Fatalf("filter err = %v, want %v", err, errFilter)
	}

	errEmit := errors.New("emit failed")
	stage, err = NewStage(StageConfig{
		Filter: &fakeFilter{},
		Result: Result{Frames: make([]av.Frame, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := stage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame}, &stageEmitter{err: errEmit, failKind: pipeline.MessageFrame}); err != errEmit {
		t.Fatalf("emit frame err = %v, want %v", err, errEmit)
	}

	packet := av.Packet{StreamID: "audio"}
	if err := stage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet}, &stageEmitter{err: errEmit, failKind: pipeline.MessagePacket}); err != errEmit {
		t.Fatalf("emit packet err = %v, want %v", err, errEmit)
	}

	stage, err = NewStage(StageConfig{
		Filter: &scriptedFilter{filterEvents: []av.Event{{Type: av.EventStats}}},
		Result: Result{Events: make([]av.Event, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := stage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame}, &stageEmitter{err: errEmit, failKind: pipeline.MessageEvent}); err != errEmit {
		t.Fatalf("emit result event err = %v, want %v", err, errEmit)
	}
}

func TestStageForwardsEventErrors(t *testing.T) {
	errEvent := errors.New("event failed")
	stage, err := NewStage(StageConfig{Filter: &scriptedFilter{eventErr: errEvent}})
	if err != nil {
		t.Fatal(err)
	}
	event := av.Event{Type: av.EventStats}
	if err := stage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessageEvent, Event: &event}, &stageEmitter{}); err != errEvent {
		t.Fatalf("event err = %v, want %v", err, errEvent)
	}

	errFlush := errors.New("flush failed")
	stage, err = NewStage(StageConfig{Filter: &scriptedFilter{flushErr: errFlush}})
	if err != nil {
		t.Fatal(err)
	}
	eos := av.Event{Type: av.EventEndOfStream}
	if err := stage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessageEvent, Event: &eos}, &stageEmitter{}); err != errFlush {
		t.Fatalf("flush err = %v, want %v", err, errFlush)
	}

	errEmit := errors.New("emit event failed")
	stage, err = NewStage(StageConfig{
		Filter: &fakeFilter{},
		Result: Result{Frames: make([]av.Frame, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := stage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessageEvent, Event: &eos}, &stageEmitter{err: errEmit, failKind: pipeline.MessageFrame}); err != errEmit {
		t.Fatalf("emit flushed frame err = %v, want %v", err, errEmit)
	}

	stats := av.Event{Type: av.EventStats}
	if err := stage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessageEvent, Event: &stats}, &stageEmitter{err: errEmit, failKind: pipeline.MessageEvent}); err != errEmit {
		t.Fatalf("emit passthrough event err = %v, want %v", err, errEmit)
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
	if err := stage.Close(); err != nil {
		t.Fatalf("second close err = %v", err)
	}
}

func TestStageCloseForwardsError(t *testing.T) {
	errClose := errors.New("close failed")
	filter := &scriptedFilter{closeErr: errClose}
	stage, err := NewStage(StageConfig{Filter: filter})
	if err != nil {
		t.Fatal(err)
	}
	if err := stage.Close(); err != errClose {
		t.Fatalf("close err = %v, want %v", err, errClose)
	}
	if filter.closes != 1 {
		t.Fatalf("closes = %d, want 1", filter.closes)
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
