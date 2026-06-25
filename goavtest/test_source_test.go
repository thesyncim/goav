package goavtest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/goav/control"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

type recordingEmitter struct {
	failures int
	err      error
	calls    int
	messages []pipeline.Message
	onEmit   func()
}

func (e *recordingEmitter) Emit(_ context.Context, msg *pipeline.Message) error {
	e.calls++
	if e.failures > 0 {
		e.failures--
		return pipeline.ErrBackpressure
	}
	if e.err != nil {
		return e.err
	}
	e.messages = append(e.messages, clonePipelineMessage(msg))
	if e.onEmit != nil {
		e.onEmit()
	}
	return nil
}

func clonePipelineMessage(msg *pipeline.Message) pipeline.Message {
	if msg == nil {
		return pipeline.Message{}
	}
	switch msg.Kind {
	case pipeline.MessageFrame:
		return pipeline.Message{Kind: msg.Kind, Frame: cloneFramePtr(msg.Frame)}
	case pipeline.MessagePacket:
		return pipeline.Message{Kind: msg.Kind, Packet: clonePacketPtr(msg.Packet)}
	case pipeline.MessageEvent:
		return pipeline.Message{Kind: msg.Kind, Event: cloneEventPtr(msg.Event)}
	default:
		return pipeline.Message{Kind: msg.Kind}
	}
}

func TestTestSourceNilMethodsAreSafe(t *testing.T) {
	var source *TestSource
	if got := source.Name(); got != "" {
		t.Fatalf("nil Name() = %q, want empty", got)
	}
	if got := source.Detail(); got != "" {
		t.Fatalf("nil Detail() = %q, want empty", got)
	}
	if got := source.SourceShape(); got != (shape.Spec{}) {
		t.Fatalf("nil SourceShape() = %#v, want zero", got)
	}
	if _, _, err := source.OpenSource(context.Background()); err == nil {
		t.Fatal("nil OpenSource() succeeded")
	}
	if err := source.Start(context.Background(), &recordingEmitter{}); err == nil {
		t.Fatal("nil Start() succeeded")
	}
	if err := source.Control(context.Background(), &pipeline.Message{}); err == nil {
		t.Fatal("nil Control() succeeded")
	}
	if got := source.Controls(); got != nil {
		t.Fatalf("nil Controls() = %#v, want nil", got)
	}
	if _, err := source.WaitControl(context.Background(), av.EventRate); err == nil {
		t.Fatal("nil WaitControl() succeeded")
	}
	if got := source.String(); got != "goavtest.TestSource(<nil>)" {
		t.Fatalf("nil String() = %q", got)
	}
}

func TestTestSourceFramesStartAndClone(t *testing.T) {
	frame := s16Frame("", 16000, 2, []int16{1, 2}, 0)
	source := NewTestSource("",
		shape.Frame(av.MediaAudio, shape.Audio(16000, 2, av.SampleFormatS16)),
		TestSourceDetail("scripted fixture"),
		TestSourceFrames(nil, frame),
	)
	frame.Planes[0].Buffer.Bytes[0] = 99

	if source.Name() == "" {
		t.Fatal("NewTestSource should generate a stable name")
	}
	if got := source.Detail(); got != "scripted fixture" {
		t.Fatalf("Detail() = %q", got)
	}
	if got := source.SourceShape(); got.Domain != shape.DomainFrame || got.StreamID != av.StreamID(source.Name()) {
		t.Fatalf("SourceShape() = %#v, want frame domain and generated stream id", got)
	}
	opened, streams, err := source.OpenSource(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if opened != source {
		t.Fatalf("OpenSource() source = %T, want original TestSource", opened)
	}
	if len(streams) != 1 || streams[0].ID != av.StreamID(source.Name()) || streams[0].Codec.SampleRate != 16000 {
		t.Fatalf("OpenSource() streams = %#v", streams)
	}

	emitter := &recordingEmitter{failures: 1}
	if err := source.Start(context.Background(), emitter); err != nil {
		t.Fatal(err)
	}
	if emitter.calls != 3 {
		t.Fatalf("Emit calls = %d, want frame retry + frame + EOS", emitter.calls)
	}
	if len(emitter.messages) != 2 {
		t.Fatalf("messages = %d, want frame and EOS", len(emitter.messages))
	}
	first := emitter.messages[0]
	if first.Kind != pipeline.MessageFrame || first.Frame == nil {
		t.Fatalf("first message = %#v, want frame", first)
	}
	if first.Frame.StreamID != av.StreamID(source.Name()) || first.Frame.Type != av.MediaAudio {
		t.Fatalf("frame stream/type = %q/%q", first.Frame.StreamID, first.Frame.Type)
	}
	if got := first.Frame.Planes[0].Buffer.Bytes[0]; got == 99 {
		t.Fatal("TestSourceFrames did not clone the caller-owned frame")
	}
	if eos := emitter.messages[1]; eos.Kind != pipeline.MessageEvent || eos.Event == nil || eos.Event.Type != av.EventEndOfStream {
		t.Fatalf("second message = %#v, want EOS", eos)
	}
	if got := source.String(); got != "goavtest.TestSource("+source.Name()+")" {
		t.Fatalf("String() = %q", got)
	}
}

func TestTestSourceMixedScriptAndAppend(t *testing.T) {
	frame := s16Frame("", 16000, 1, []int16{1}, 0)
	packet := &av.Packet{Payload: av.Buffer{Bytes: []byte{2}, Ownership: av.BufferImmutable}}
	event := av.Event{Type: av.EventStats, Reason: "ready", Metadata: av.Metadata{"phase": "start"}}
	source := NewTestSource("script",
		shape.Frame(av.MediaAudio, shape.Audio(16000, 1, av.SampleFormatS16)),
		TestSourceScript(
			TestSourceFrame(frame),
			TestSourceEvent(event),
		),
		TestSourceAppend(
			TestSourcePacket(packet),
			TestSourceMessage{},
		),
	)
	frame.Planes[0].Buffer.Bytes[0] = 99
	packet.Payload.Bytes[0] = 99
	event.Metadata["phase"] = "mutated"

	emitter := &recordingEmitter{}
	if err := source.Start(context.Background(), emitter); err != nil {
		t.Fatal(err)
	}
	if len(emitter.messages) != 4 {
		t.Fatalf("messages = %d, want frame, event, packet, EOS", len(emitter.messages))
	}
	if got := emitter.messages[0].Frame.Planes[0].Buffer.Bytes[0]; got == 99 {
		t.Fatalf("script frame was not cloned, got payload byte %d", got)
	}
	if got := emitter.messages[1].Event.Metadata["phase"]; got != "start" {
		t.Fatalf("script event metadata = %q, want cloned start", got)
	}
	if got := emitter.messages[2].Packet.Payload.Bytes[0]; got != 2 {
		t.Fatalf("script packet payload = %d, want cloned 2", got)
	}
	if eos := emitter.messages[3]; eos.Kind != pipeline.MessageEvent || eos.Event == nil || eos.Event.Type != av.EventEndOfStream {
		t.Fatalf("last message = %#v, want EOS", eos)
	}

	emitter.messages[0].Frame.Planes[0].Buffer.Bytes[0] = 42
	second := &recordingEmitter{}
	if err := source.Start(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if got := second.messages[0].Frame.Planes[0].Buffer.Bytes[0]; got == 42 {
		t.Fatalf("delivered frame mutation leaked into next run")
	}
}

func TestTestSourceAppendComposesWithTypedScripts(t *testing.T) {
	frame := s16Frame("", 16000, 1, []int16{1}, 0)
	source := NewTestSource("append",
		shape.Frame(av.MediaAudio, shape.Audio(16000, 1, av.SampleFormatS16)),
		TestSourceFrames(frame),
		TestSourceAppend(TestSourceEvent(av.Event{Type: av.EventStats, Reason: "after-frame"})),
	)

	emitter := &recordingEmitter{}
	if err := source.Start(context.Background(), emitter); err != nil {
		t.Fatal(err)
	}
	if len(emitter.messages) != 3 ||
		emitter.messages[0].Kind != pipeline.MessageFrame ||
		emitter.messages[1].Kind != pipeline.MessageEvent ||
		emitter.messages[1].Event.Reason != "after-frame" {
		t.Fatalf("append messages = %#v", emitter.messages)
	}
}

func TestTestSourcePacketsEventsAndDefaults(t *testing.T) {
	packet := &av.Packet{Payload: av.Buffer{Bytes: []byte{7}, Ownership: av.BufferImmutable}}
	packetSource := NewTestSource("packets",
		shape.Packet(av.MediaVideo, av.CodecVP8),
		TestSourcePackets(nil, packet),
	)
	packet.Payload.Bytes[0] = 9
	packetEmitter := &recordingEmitter{}
	if err := packetSource.Start(context.Background(), packetEmitter); err != nil {
		t.Fatal(err)
	}
	if got := packetEmitter.messages[0].Packet.Payload.Bytes[0]; got != 7 {
		t.Fatalf("packet payload = %d, want cloned value 7", got)
	}
	if got := packetEmitter.messages[0].Packet.Type; got != av.MediaVideo {
		t.Fatalf("packet type = %q, want video", got)
	}

	eventSource := NewTestSource("events",
		shape.Event(shape.Stream("control")),
		TestSourceEvents(av.Event{Type: av.EventStats, Reason: "ready"}),
	)
	eventEmitter := &recordingEmitter{}
	if err := eventSource.Start(context.Background(), eventEmitter); err != nil {
		t.Fatal(err)
	}
	if got := eventEmitter.messages[0].Event.StreamID; got != av.StreamID("control") {
		t.Fatalf("event stream = %q, want control", got)
	}

	videoSource := NewTestSource("video", shape.Frame(av.MediaVideo))
	videoEmitter := &recordingEmitter{}
	if err := videoSource.Start(context.Background(), videoEmitter); err != nil {
		t.Fatal(err)
	}
	video := videoEmitter.messages[0].Frame.Video
	if video == nil || video.Width != 2 || video.Height != 2 {
		t.Fatalf("default video frame = %#v, want minimum 2x2", video)
	}

	packetDefault := NewTestSource("packet-default", shape.Packet(av.MediaAudio, av.CodecOpus))
	if msg := packetDefault.pipelineMessage(testSourceMessage{}); msg.Kind != pipeline.MessageEvent || msg.Event.Reason != "goavtest empty message" {
		t.Fatalf("empty script message = %#v", msg)
	}

	packetDefaultEmitter := &recordingEmitter{}
	if err := packetDefault.Start(context.Background(), packetDefaultEmitter); err != nil {
		t.Fatal(err)
	}
	if got := packetDefaultEmitter.messages[0].Packet.Payload.Bytes; len(got) != 1 || got[0] != 1 {
		t.Fatalf("default packet payload = %v, want [1]", got)
	}

	eventDefault := NewTestSource("event-default", shape.Event(), nil)
	eventDefaultEmitter := &recordingEmitter{}
	if err := eventDefault.Start(context.Background(), eventDefaultEmitter); err != nil {
		t.Fatal(err)
	}
	if got := eventDefaultEmitter.messages[0].Event.Reason; got != "goavtest" {
		t.Fatalf("default event reason = %q, want goavtest", got)
	}

	audioDefault := NewTestSource("audio-default", shape.Frame(av.MediaAudio))
	audioDefaultEmitter := &recordingEmitter{}
	if err := audioDefault.Start(context.Background(), audioDefaultEmitter); err != nil {
		t.Fatal(err)
	}
	audio := audioDefaultEmitter.messages[0].Frame.Audio
	if audio == nil || audio.SampleRate != 48000 || audio.Channels != 1 {
		t.Fatalf("default audio frame = %#v, want 48kHz mono", audio)
	}

	stream := streamFromTestSourceShape("fallback", shape.Spec{MediaKind: av.MediaVideo})
	if stream.ID != "fallback" || stream.Codec.ClockRate != 90000 {
		t.Fatalf("fallback stream = %#v, want named 90kHz video stream", stream)
	}
}

func TestTestSourceLiveStopsWithoutEOS(t *testing.T) {
	source := NewTestSource("live",
		shape.Event(shape.Stream("live")),
		TestSourceLive(),
		TestSourceEvents(av.Event{Type: av.EventStats}),
	)
	emitter := &recordingEmitter{}
	emitter.onEmit = func() {
		if len(emitter.messages) >= 3 {
			_ = source.Close()
		}
	}

	if err := source.Start(context.Background(), emitter); err != nil {
		t.Fatal(err)
	}
	if len(emitter.messages) != 3 {
		t.Fatalf("live messages = %d, want repeated script until Close", len(emitter.messages))
	}
	for i, msg := range emitter.messages {
		if msg.Event == nil || msg.Event.Type == av.EventEndOfStream {
			t.Fatalf("message %d = %#v, live source should not emit EOS on close", i, msg)
		}
	}
}

func TestTestSourceStartStopsBetweenScriptMessages(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	source := NewTestSource("cancel",
		shape.Event(shape.Stream("cancel")),
		TestSourceEvents(
			av.Event{Type: av.EventStats, Reason: "first"},
			av.Event{Type: av.EventStats, Reason: "second"},
		),
	)
	emitter := &recordingEmitter{onEmit: cancel}

	if err := source.Start(ctx, emitter); err != nil {
		t.Fatal(err)
	}
	if len(emitter.messages) != 1 || emitter.messages[0].Event.Reason != "first" {
		t.Fatalf("messages after mid-script cancel = %#v, want first only", emitter.messages)
	}
}

func TestTestSourceStartHonorsClosedContextAndEmitterErrors(t *testing.T) {
	source := NewTestSource("fixture", shape.Event(shape.Stream("fixture")), TestSourceEvents(av.Event{Type: av.EventStats}))

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	emitter := &recordingEmitter{}
	if err := source.Start(cancelled, emitter); err != nil {
		t.Fatal(err)
	}
	if emitter.calls != 0 {
		t.Fatalf("cancelled Start emitted %d messages, want 0", emitter.calls)
	}

	closed := NewTestSource("closed", shape.Event(shape.Stream("closed")), TestSourceEvents(av.Event{Type: av.EventStats}))
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	emitter = &recordingEmitter{}
	if err := closed.Start(context.Background(), emitter); err != nil {
		t.Fatal(err)
	}
	if emitter.calls != 0 {
		t.Fatalf("closed Start emitted %d messages, want 0", emitter.calls)
	}

	errClosedEmitter := &recordingEmitter{err: pipeline.ErrClosed}
	if err := source.emit(context.Background(), errClosedEmitter, testSourceMessage{event: &av.Event{Type: av.EventStats}}); err != nil {
		t.Fatalf("emit ErrClosed = %v, want nil", err)
	}

	boom := errors.New("boom")
	boomEmitter := &recordingEmitter{err: boom}
	if err := source.emit(context.Background(), boomEmitter, testSourceMessage{event: &av.Event{Type: av.EventStats}}); !errors.Is(err, boom) {
		t.Fatalf("emit custom error = %v, want boom", err)
	}

	backpressureCtx, cancelBackpressure := context.WithCancel(context.Background())
	cancelBackpressure()
	backpressureEmitter := &recordingEmitter{failures: 1}
	if err := source.emit(backpressureCtx, backpressureEmitter, testSourceMessage{event: &av.Event{Type: av.EventStats}}); err != nil {
		t.Fatalf("emit backpressure on cancelled context = %v, want nil", err)
	}

	eosFailure := errors.New("eos failure")
	eosSource := NewTestSource("eos", shape.Event(shape.Stream("eos")))
	eosSource.script = nil
	if err := eosSource.Start(context.Background(), &recordingEmitter{err: eosFailure}); !errors.Is(err, eosFailure) {
		t.Fatalf("Start() EOS error = %v, want eos failure", err)
	}
}

func TestTestSourceControlValidationAndCopies(t *testing.T) {
	source := NewTestSource("control", shape.Packet(av.MediaAudio, av.CodecOpus))
	if err := source.Control(context.Background(), nil); err == nil {
		t.Fatal("nil control message succeeded")
	}
	if err := source.Control(context.Background(), &pipeline.Message{Kind: pipeline.MessageFrame, Frame: &av.Frame{}}); err == nil {
		t.Fatal("frame control message succeeded")
	}
	if err := source.Control(context.Background(), &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventRate}}); err == nil {
		t.Fatal("malformed rate control succeeded")
	}
	if err := source.Control(context.Background(), &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventSegment}}); err == nil {
		t.Fatal("malformed segment control succeeded")
	}
	if err := source.Control(context.Background(), &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
		Type:      av.EventSegment,
		Timestamp: av.Timestamp{Value: int64(time.Second), Base: av.TimeBase{Num: 1, Den: int64(time.Second)}},
	}}); err == nil {
		t.Fatal("segment control without end succeeded")
	}
	if err := source.Control(context.Background(), &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventSeek}}); err == nil {
		t.Fatal("malformed seek control succeeded")
	}

	rate := av.Event{Type: av.EventRate, Metadata: av.RateMetadata(2)}
	segment := av.Event{
		Type:      av.EventSegment,
		Timestamp: av.Timestamp{Value: int64(time.Second), Base: av.TimeBase{Num: 1, Den: int64(time.Second)}},
		Metadata:  av.SegmentEndMetadata(3 * time.Second),
	}
	if err := source.Control(context.Background(), &pipeline.Message{Kind: pipeline.MessageEvent, Event: &rate}); err != nil {
		t.Fatal(err)
	}
	rate.Metadata[av.MetadataRate] = "99"
	if err := source.Control(context.Background(), &pipeline.Message{Kind: pipeline.MessageEvent, Event: &segment}); err != nil {
		t.Fatal(err)
	}
	if err := source.Control(context.Background(), &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventStats, StreamID: "control"}}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := source.WaitControl(ctx, av.EventSegment)
	if err != nil {
		t.Fatal(err)
	}
	if end, ok := av.EventSegmentEnd(&got); !ok || end != 3*time.Second {
		t.Fatalf("segment control = %+v, end=%s ok=%v", got, end, ok)
	}

	controls := source.Controls()
	if len(controls) != 3 {
		t.Fatalf("controls = %d, want 3", len(controls))
	}
	if controls[0].Metadata[av.MetadataRate] != "2" {
		t.Fatalf("stored rate metadata = %q, want cloned value 2", controls[0].Metadata[av.MetadataRate])
	}
	controls[0].Metadata[av.MetadataRate] = "mutated"
	if again := source.Controls(); again[0].Metadata[av.MetadataRate] != "2" {
		t.Fatalf("Controls() exposed internal metadata: %q", again[0].Metadata[av.MetadataRate])
	}

	expired, cancelExpired := context.WithCancel(context.Background())
	cancelExpired()
	if _, err := source.WaitControl(expired, av.EventRate); !errors.Is(err, context.Canceled) {
		t.Fatalf("expired WaitControl() = %v, want context.Canceled", err)
	}
}

func TestTestSourcePipelineMessagePreservesExplicitFields(t *testing.T) {
	source := NewTestSource("source", shape.Frame(av.MediaAudio, shape.Audio(48000, 1, av.SampleFormatS16)))

	frame := s16Frame("explicit-frame", 48000, 1, []int16{1}, 0)
	frameMsg := source.pipelineMessage(testSourceMessage{frame: frame})
	if frameMsg.Frame.StreamID != "explicit-frame" || frameMsg.Frame.Type != av.MediaAudio {
		t.Fatalf("frame message = %#v", frameMsg.Frame)
	}

	packet := &av.Packet{StreamID: "explicit-packet", Type: av.MediaData}
	packetMsg := source.pipelineMessage(testSourceMessage{packet: packet})
	if packetMsg.Packet.StreamID != "explicit-packet" || packetMsg.Packet.Type != av.MediaData {
		t.Fatalf("packet message = %#v", packetMsg.Packet)
	}

	event := &av.Event{Type: av.EventStats, StreamID: "explicit-event"}
	eventMsg := source.pipelineMessage(testSourceMessage{event: event})
	if eventMsg.Event.StreamID != "explicit-event" {
		t.Fatalf("event message = %#v", eventMsg.Event)
	}
}

func TestTestSourceRecordsSourceControlsThroughRealTask(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	source := NewTestSource("fixture",
		shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48000, 1, av.SampleFormatS16)),
	)
	task, err := goav.From(source.Input()).
		Audio().Copy().
		To(NewCollector().Sink()).
		UseRuntime(Runtime()).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	if err := task.Control(ctx, control.Rate(0.5).At("fixture")); err != nil {
		t.Fatal(err)
	}
	event, err := source.WaitControl(ctx, av.EventRate)
	if err != nil {
		t.Fatal(err)
	}
	if rate, ok := av.EventRateValue(&event); !ok || rate != 0.5 {
		t.Fatalf("rate control = %+v, parsed=%v ok=%v", event, rate, ok)
	}

	if err := task.Control(ctx, control.Seek(12*time.Second).At("fixture")); err != nil {
		t.Fatal(err)
	}
	event, err = source.WaitControl(ctx, av.EventSeek)
	if err != nil {
		t.Fatal(err)
	}
	if position, ok := event.Timestamp.ToDuration(); !ok || position != 12*time.Second {
		t.Fatalf("seek control = %+v, parsed=%v ok=%v", event, position, ok)
	}
}
