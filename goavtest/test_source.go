package goavtest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// TestSource is a provider-backed, controllable source fixture for integration
// tests. It opens through goav.Input, emits a deterministic script of frames,
// packets, and events, records every source-control event it receives, and can
// run once or loop as a live source.
type TestSource struct {
	name   string
	detail string
	spec   shape.Spec
	stream av.Stream
	script []testSourceMessage
	live   bool

	mu       sync.Mutex
	controls []av.Event
	controlC chan av.Event
	closed   atomic.Bool
}

type testSourceMessage struct {
	frame  *av.Frame
	packet *av.Packet
	event  *av.Event
}

// TestSourceMessage is one scripted emission for TestSource. Build values with
// TestSourceFrame, TestSourcePacket, or TestSourceEvent so caller-owned media
// is cloned before the source starts.
type TestSourceMessage struct {
	message testSourceMessage
}

// TestSourceOption configures a TestSource.
type TestSourceOption func(*TestSource)

// NewTestSource creates a reusable source fixture. If spec leaves Domain empty
// it defaults to frame-domain media; if spec leaves StreamID empty it uses name.
func NewTestSource(name string, spec shape.Spec, options ...TestSourceOption) *TestSource {
	if name == "" {
		name = nextName("test-source")
	}
	if spec.Domain == "" {
		spec.Domain = shape.DomainFrame
	}
	if spec.StreamID == "" {
		spec.StreamID = av.StreamID(name)
	}
	source := &TestSource{
		name:     name,
		spec:     spec,
		stream:   streamFromTestSourceShape(name, spec),
		controlC: make(chan av.Event, 64),
	}
	for _, option := range options {
		if option != nil {
			option(source)
		}
	}
	if len(source.script) == 0 {
		source.script = defaultTestSourceScript(source.stream, source.spec)
	}
	return source
}

// TestSourceDetail sets the Describe detail returned by the source node.
func TestSourceDetail(detail string) TestSourceOption {
	return func(source *TestSource) {
		source.detail = detail
	}
}

// TestSourceLive makes the source repeat its script until the task stops.
func TestSourceLive() TestSourceOption {
	return func(source *TestSource) {
		source.live = true
	}
}

// TestSourceFrame builds a scripted frame emission.
func TestSourceFrame(frame *av.Frame) TestSourceMessage {
	return TestSourceMessage{message: testSourceMessage{frame: cloneFramePtr(frame)}}
}

// TestSourcePacket builds a scripted packet emission.
func TestSourcePacket(packet *av.Packet) TestSourceMessage {
	return TestSourceMessage{message: testSourceMessage{packet: clonePacketPtr(packet)}}
}

// TestSourceEvent builds a scripted event emission.
func TestSourceEvent(event av.Event) TestSourceMessage {
	return TestSourceMessage{message: testSourceMessage{event: cloneEventPtr(&event)}}
}

// TestSourceScript replaces the source script with a mixed sequence of frames,
// packets, and events. Each message is cloned when the option is applied, and
// Start clones again on delivery, so tests can mutate inputs and collected
// outputs without corrupting future runs.
func TestSourceScript(messages ...TestSourceMessage) TestSourceOption {
	return func(source *TestSource) {
		source.script = source.script[:0]
		source.appendScript(messages...)
	}
}

// TestSourceAppend appends mixed frame, packet, and event emissions to the
// current script. It composes with TestSourceScript, TestSourceFrames,
// TestSourcePackets, and TestSourceEvents in option order.
func TestSourceAppend(messages ...TestSourceMessage) TestSourceOption {
	return func(source *TestSource) {
		source.appendScript(messages...)
	}
}

// TestSourceFrames sets the frame script emitted by the source.
func TestSourceFrames(frames ...*av.Frame) TestSourceOption {
	return func(source *TestSource) {
		source.script = source.script[:0]
		for _, frame := range frames {
			if frame == nil {
				continue
			}
			source.script = append(source.script, testSourceMessage{frame: cloneFrame(frame)})
		}
	}
}

// TestSourcePackets sets the packet script emitted by the source.
func TestSourcePackets(packets ...*av.Packet) TestSourceOption {
	return func(source *TestSource) {
		source.script = source.script[:0]
		for _, packet := range packets {
			if packet == nil {
				continue
			}
			source.script = append(source.script, testSourceMessage{packet: clonePacket(packet)})
		}
	}
}

// TestSourceEvents sets the event script emitted by the source.
func TestSourceEvents(events ...av.Event) TestSourceOption {
	return func(source *TestSource) {
		source.script = source.script[:0]
		for _, event := range events {
			clone := cloneEvent(event)
			source.script = append(source.script, testSourceMessage{event: &clone})
		}
	}
}

func (s *TestSource) appendScript(messages ...TestSourceMessage) {
	for _, message := range messages {
		if message.message.frame == nil && message.message.packet == nil && message.message.event == nil {
			continue
		}
		s.script = append(s.script, cloneTestSourceMessage(message.message))
	}
}

// Input adapts the source fixture into a goav recipe input.
func (s *TestSource) Input(options ...goav.InputOption) goav.InputSpec {
	return goav.Input(s, options...)
}

// Name returns the source node name.
func (s *TestSource) Name() string {
	if s == nil {
		return ""
	}
	return s.name
}

// Detail returns the source node detail for Describe output.
func (s *TestSource) Detail() string {
	if s == nil {
		return ""
	}
	return s.detail
}

// SourceShape declares the media shape the fixture produces.
func (s *TestSource) SourceShape() shape.Spec {
	if s == nil {
		return shape.Spec{}
	}
	return s.spec
}

// OpenSource opens the fixture as a running pipeline source.
func (s *TestSource) OpenSource(context.Context) (pipeline.Source, []av.Stream, error) {
	if s == nil {
		return nil, nil, errors.New("goavtest: nil TestSource")
	}
	return s, []av.Stream{cloneStream(s.stream)}, nil
}

// Start emits the configured script once, or repeats it while live mode is set.
func (s *TestSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	if s == nil {
		return errors.New("goavtest: nil TestSource")
	}
	for {
		if ctx.Err() != nil || s.closed.Load() {
			return nil
		}
		for _, message := range s.scriptSnapshot() {
			if ctx.Err() != nil || s.closed.Load() {
				return nil
			}
			if err := s.emit(ctx, emitter, message); err != nil {
				return err
			}
		}
		if !s.live {
			return s.emitEOS(ctx, emitter)
		}
	}
}

// Control records a source-control event. It validates that the message is an
// event message and rejects malformed source time controls instead of silently
// accepting them.
func (s *TestSource) Control(_ context.Context, msg *pipeline.Message) error {
	if s == nil {
		return errors.New("goavtest: nil TestSource")
	}
	if msg == nil || msg.Kind != pipeline.MessageEvent || msg.Event == nil {
		return errors.New("goavtest: TestSource control expects an event message")
	}
	event := cloneEvent(*msg.Event)
	switch event.Type {
	case av.EventRate:
		if _, ok := av.EventRateValue(&event); !ok {
			return errors.New("goavtest: malformed rate control")
		}
	case av.EventSegment:
		if _, ok := event.Timestamp.ToDuration(); !ok {
			return errors.New("goavtest: malformed segment start")
		}
		if _, ok := av.EventSegmentEnd(&event); !ok {
			return errors.New("goavtest: malformed segment end")
		}
	case av.EventSeek:
		if _, ok := event.Timestamp.ToDuration(); !ok {
			return errors.New("goavtest: malformed seek position")
		}
	}
	s.mu.Lock()
	s.controls = append(s.controls, event)
	s.mu.Unlock()
	select {
	case s.controlC <- event:
	default:
	}
	return nil
}

// Controls returns every control event recorded so far.
func (s *TestSource) Controls() []av.Event {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]av.Event, len(s.controls))
	for i := range s.controls {
		out[i] = cloneEvent(s.controls[i])
	}
	return out
}

// WaitControl blocks until a control of the requested type arrives. An empty
// type matches the next control of any type.
func (s *TestSource) WaitControl(ctx context.Context, typ av.EventType) (av.Event, error) {
	if s == nil {
		return av.Event{}, errors.New("goavtest: nil TestSource")
	}
	for {
		select {
		case event := <-s.controlC:
			if typ == "" || event.Type == typ {
				return cloneEvent(event), nil
			}
		case <-ctx.Done():
			return av.Event{}, ctx.Err()
		}
	}
}

// Close stops a live TestSource.
func (s *TestSource) Close() error {
	if s != nil {
		s.closed.Store(true)
	}
	return nil
}

func (s *TestSource) scriptSnapshot() []testSourceMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]testSourceMessage, len(s.script))
	for i := range s.script {
		out[i] = cloneTestSourceMessage(s.script[i])
	}
	return out
}

func (s *TestSource) emit(ctx context.Context, emitter pipeline.Emitter, message testSourceMessage) error {
	msg := s.pipelineMessage(message)
	for {
		err := emitter.Emit(ctx, &msg)
		if err == nil {
			return nil
		}
		if errors.Is(err, pipeline.ErrBackpressure) {
			if ctx.Err() != nil {
				return nil
			}
			continue
		}
		if errors.Is(err, pipeline.ErrClosed) {
			return nil
		}
		return err
	}
}

func (s *TestSource) pipelineMessage(message testSourceMessage) pipeline.Message {
	switch {
	case message.frame != nil:
		frame := cloneFrame(message.frame)
		if frame.StreamID == "" {
			frame.StreamID = s.stream.ID
		}
		if frame.Type == "" {
			frame.Type = s.stream.Type
		}
		return pipeline.Message{Kind: pipeline.MessageFrame, Frame: frame}
	case message.packet != nil:
		packet := clonePacket(message.packet)
		if packet.StreamID == "" {
			packet.StreamID = s.stream.ID
		}
		if packet.Type == "" {
			packet.Type = s.stream.Type
		}
		return pipeline.Message{Kind: pipeline.MessagePacket, Packet: packet}
	case message.event != nil:
		event := cloneEvent(*message.event)
		if event.StreamID == "" {
			event.StreamID = s.stream.ID
		}
		return pipeline.Message{Kind: pipeline.MessageEvent, Event: &event}
	default:
		event := av.Event{Type: av.EventStats, StreamID: s.stream.ID, Reason: "goavtest empty message"}
		return pipeline.Message{Kind: pipeline.MessageEvent, Event: &event}
	}
}

func (s *TestSource) emitEOS(ctx context.Context, emitter pipeline.Emitter) error {
	event := av.Event{Type: av.EventEndOfStream, StreamID: s.stream.ID}
	return s.emit(ctx, emitter, testSourceMessage{event: &event})
}

func streamFromTestSourceShape(name string, spec shape.Spec) av.Stream {
	stream := av.Stream{
		ID:   spec.StreamID,
		Type: spec.MediaKind,
		Codec: av.CodecParameters{
			ID:           spec.Codec,
			Type:         spec.MediaKind,
			ClockRate:    clockRateFromShape(spec),
			SampleRate:   spec.SampleRate,
			Channels:     spec.Channels,
			Width:        spec.Width,
			Height:       spec.Height,
			PixelFormat:  spec.PixelFormat,
			SampleFormat: spec.SampleFormat,
		},
		Name: name,
	}
	if stream.ID == "" {
		stream.ID = av.StreamID(name)
	}
	return stream
}

func clockRateFromShape(spec shape.Spec) uint32 {
	if spec.SampleRate > 0 {
		return uint32(spec.SampleRate)
	}
	switch spec.MediaKind {
	case av.MediaVideo:
		return 90000
	default:
		return 0
	}
}

func defaultTestSourceScript(stream av.Stream, spec shape.Spec) []testSourceMessage {
	switch spec.Domain {
	case shape.DomainPacket:
		packet := &av.Packet{
			StreamID: stream.ID,
			Type:     stream.Type,
			Payload:  av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable},
		}
		return []testSourceMessage{{packet: packet}}
	case shape.DomainEvent:
		event := &av.Event{Type: av.EventStats, StreamID: stream.ID, Reason: "goavtest"}
		return []testSourceMessage{{event: event}}
	default:
		frame := defaultTestSourceFrame(stream, spec)
		return []testSourceMessage{{frame: frame}}
	}
}

func defaultTestSourceFrame(stream av.Stream, spec shape.Spec) *av.Frame {
	switch spec.MediaKind {
	case av.MediaVideo:
		width := max(spec.Width, 2)
		height := max(spec.Height, 2)
		return i420Frame(string(stream.ID), width, height, 0)
	default:
		sampleRate := max(spec.SampleRate, 48000)
		channels := max(spec.Channels, 1)
		return s16Frame(string(stream.ID), sampleRate, channels, make([]int16, channels), 0)
	}
}

func cloneTestSourceMessage(message testSourceMessage) testSourceMessage {
	return testSourceMessage{
		frame:  cloneFramePtr(message.frame),
		packet: clonePacketPtr(message.packet),
		event:  cloneEventPtr(message.event),
	}
}

func cloneFramePtr(frame *av.Frame) *av.Frame {
	if frame == nil {
		return nil
	}
	return cloneFrame(frame)
}

func clonePacketPtr(packet *av.Packet) *av.Packet {
	if packet == nil {
		return nil
	}
	return clonePacket(packet)
}

func cloneEventPtr(event *av.Event) *av.Event {
	if event == nil {
		return nil
	}
	clone := cloneEvent(*event)
	return &clone
}

func cloneStream(stream av.Stream) av.Stream {
	clone := stream
	clone.Codec = cloneCodecParameters(stream.Codec)
	clone.Metadata = cloneMetadata(stream.Metadata)
	return clone
}

// String returns a compact diagnostic name for the source fixture.
func (s *TestSource) String() string {
	if s == nil {
		return "goavtest.TestSource(<nil>)"
	}
	return fmt.Sprintf("goavtest.TestSource(%s)", s.name)
}
