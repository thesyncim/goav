package playoutav

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// FormatPlayout names the scheduled playout framing in SourceShape. It is an
// open av.FormatID owned by this nested module, not by the root package.
const FormatPlayout av.FormatID = "playout"

var (
	// ErrNoStreams reports a playout input without any declared stream.
	ErrNoStreams = errors.New("playoutav: no streams")
	// ErrStreamIDRequired reports a declared stream or dynamic stream event
	// without a stream id.
	ErrStreamIDRequired = errors.New("playoutav: stream id required")
	// ErrInvalidMessage reports a schedule row that does not contain exactly
	// one packet, frame, or event.
	ErrInvalidMessage = errors.New("playoutav: message must contain exactly one packet, frame, or event")
	// ErrNegativeTime reports a schedule row before the start of the playout.
	ErrNegativeTime = errors.New("playoutav: schedule time must be non-negative")
	// ErrOutOfOrder reports a schedule whose times move backward.
	ErrOutOfOrder = errors.New("playoutav: schedule times must be non-decreasing")
	// ErrMixedDomains reports a schedule that mixes packet and frame media.
	ErrMixedDomains = errors.New("playoutav: schedule cannot mix packet and frame media")
	// ErrUnknownStream reports a packet, frame, or event for an undeclared
	// stream.
	ErrUnknownStream = errors.New("playoutav: message references unknown stream")
)

// Message is one scheduled emission. Build values with Packet, Frame, or
// Event so playout owns a defensive copy of caller-provided media.
type Message struct {
	At     time.Duration
	Packet *av.Packet
	Frame  *av.Frame
	Event  *av.Event
}

// Packet schedules an encoded packet at offset at.
func Packet(at time.Duration, packet *av.Packet) Message {
	return Message{At: at, Packet: clonePacketPtr(packet)}
}

// Frame schedules a decoded frame at offset at.
func Frame(at time.Duration, frame *av.Frame) Message {
	return Message{At: at, Frame: cloneFramePtr(frame)}
}

// Event schedules an out-of-band event at offset at.
func Event(at time.Duration, event av.Event) Message {
	return Message{At: at, Event: cloneEventPtr(&event)}
}

// Option configures a playout Input.
type Option func(*Input)

// WithName names the source node in Describe output.
func WithName(name string) Option {
	return func(input *Input) {
		input.name = strings.TrimSpace(name)
	}
}

// WithDetail overrides the source-node detail in Describe output.
func WithDetail(detail string) Option {
	return func(input *Input) {
		input.detail = strings.TrimSpace(detail)
	}
}

// WithRealtime controls the Realtime fact reported by SourceShape. Scheduled
// playout defaults to realtime.
func WithRealtime(realtime bool) Option {
	return func(input *Input) {
		input.realtime = realtime
	}
}

// WithSourceShape pins the SourceShape reported before OpenSource. Leave this
// unset for the module to infer the shape from the first media stream.
func WithSourceShape(spec shape.Spec) Option {
	return func(input *Input) {
		input.spec = spec
		input.hasSpec = true
	}
}

// Input is a scheduled playout provider. It implements provider.Source without
// importing or changing any root goav internals.
type Input struct {
	name     string
	detail   string
	realtime bool
	spec     shape.Spec
	hasSpec  bool

	streams  []av.Stream
	messages []Message
}

// New builds a scheduled playout provider. Streams are declared up front; media
// messages may omit StreamID when there is exactly one declared stream.
func New(streams []av.Stream, messages []Message, options ...Option) *Input {
	input := &Input{
		realtime: true,
		streams:  cloneStreams(streams),
	}
	input.messages = cloneMessages(messages)
	for i := range options {
		if options[i] != nil {
			options[i](input)
		}
	}
	return input
}

// Name returns the source node name.
func (in *Input) Name() string {
	if in == nil || in.name == "" {
		return "playout"
	}
	return in.name
}

// Detail returns the human-readable source detail.
func (in *Input) Detail() string {
	if in == nil || in.detail == "" {
		return "scheduled playout"
	}
	return in.detail
}

// SourceShape reports the media shape the schedule produces.
func (in *Input) SourceShape() shape.Spec {
	if in == nil {
		return shape.Spec{}
	}
	if in.hasSpec {
		spec := in.spec
		if spec.Format == "" {
			spec.Format = FormatPlayout
		}
		return spec
	}
	spec := inferShape(in.streams, in.messages)
	spec.Realtime = in.realtime
	if spec.Format == "" {
		spec.Format = FormatPlayout
	}
	return spec
}

// OpenSource validates the schedule and returns a running pipeline source.
func (in *Input) OpenSource(context.Context) (pipeline.Source, []av.Stream, error) {
	if in == nil {
		return nil, nil, ErrNoStreams
	}
	streams, err := normalizeStreams(in.streams)
	if err != nil {
		return nil, nil, err
	}
	messages, err := normalizeMessages(streams, in.messages)
	if err != nil {
		return nil, nil, err
	}
	return &source{
		name:     in.Name(),
		detail:   in.Detail(),
		streams:  streams,
		messages: messages,
		sleep:    sleepContext,
	}, cloneStreams(streams), nil
}

type source struct {
	name     string
	detail   string
	streams  []av.Stream
	messages []Message
	sleep    func(context.Context, time.Duration) error
	closed   atomic.Bool
}

func (s *source) Name() string { return s.name }

func (s *source) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: s.name, Kind: pipeline.NodeSource, Detail: s.detail}
}

func (s *source) Start(ctx context.Context, emitter pipeline.Emitter) error {
	if s.closed.Load() {
		return pipeline.ErrClosed
	}
	var previous time.Duration
	ended := make(map[av.StreamID]struct{}, len(s.streams))
	for i := range s.messages {
		if s.closed.Load() || ctx.Err() != nil {
			return nil
		}
		message := s.messages[i]
		if wait := message.At - previous; wait > 0 {
			if err := s.sleep(ctx, wait); err != nil {
				return nil
			}
		}
		previous = message.At
		if message.Event != nil && (message.Event.Type == av.EventEndOfStream || message.Event.Type == av.EventStreamRemoved) {
			ended[message.Event.StreamID] = struct{}{}
		}
		msg := pipelineMessage(message)
		if err := emit(ctx, emitter, &msg); err != nil {
			return err
		}
	}
	for i := range s.streams {
		id := s.streams[i].ID
		if _, ok := ended[id]; ok {
			continue
		}
		event := av.Event{Type: av.EventEndOfStream, StreamID: id}
		msg := pipeline.Message{Kind: pipeline.MessageEvent, Event: &event}
		if err := emit(ctx, emitter, &msg); err != nil {
			return err
		}
	}
	return nil
}

func (s *source) Close() error {
	if s != nil {
		s.closed.Store(true)
	}
	return nil
}

func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func emit(ctx context.Context, emitter pipeline.Emitter, msg *pipeline.Message) error {
	for {
		err := emitter.Emit(ctx, msg)
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

func pipelineMessage(message Message) pipeline.Message {
	switch {
	case message.Packet != nil:
		return pipeline.Message{Kind: pipeline.MessagePacket, Packet: clonePacket(message.Packet)}
	case message.Frame != nil:
		return pipeline.Message{Kind: pipeline.MessageFrame, Frame: cloneFrame(message.Frame)}
	case message.Event != nil:
		event := cloneEvent(*message.Event)
		return pipeline.Message{Kind: pipeline.MessageEvent, Event: &event}
	default:
		event := av.Event{Type: av.EventStats, Reason: "playoutav empty message"}
		return pipeline.Message{Kind: pipeline.MessageEvent, Event: &event}
	}
}

func normalizeStreams(streams []av.Stream) ([]av.Stream, error) {
	if len(streams) == 0 {
		return nil, ErrNoStreams
	}
	out := cloneStreams(streams)
	seen := make(map[av.StreamID]struct{}, len(out))
	for i := range out {
		if out[i].ID == "" {
			return nil, ErrStreamIDRequired
		}
		if _, ok := seen[out[i].ID]; ok {
			return nil, ErrUnknownStream
		}
		seen[out[i].ID] = struct{}{}
		if out[i].Codec.Type == "" {
			out[i].Codec.Type = out[i].Type
		}
	}
	return out, nil
}

func normalizeMessages(streams []av.Stream, messages []Message) ([]Message, error) {
	streamByID := make(map[av.StreamID]av.Stream, len(streams))
	for i := range streams {
		streamByID[streams[i].ID] = streams[i]
	}
	var previous time.Duration
	var mediaDomain shape.MediaDomain
	out := cloneMessages(messages)
	for i := range out {
		if out[i].At < 0 {
			return nil, ErrNegativeTime
		}
		if i > 0 && out[i].At < previous {
			return nil, ErrOutOfOrder
		}
		previous = out[i].At
		count := 0
		if out[i].Packet != nil {
			count++
			if mediaDomain == "" {
				mediaDomain = shape.DomainPacket
			} else if mediaDomain != shape.DomainPacket {
				return nil, ErrMixedDomains
			}
			packet := out[i].Packet
			stream, err := resolveStream(streamByID, len(streams), packet.StreamID)
			if err != nil {
				return nil, err
			}
			packet.StreamID = stream.ID
			if packet.Type == "" {
				packet.Type = stream.Type
			}
		}
		if out[i].Frame != nil {
			count++
			if mediaDomain == "" {
				mediaDomain = shape.DomainFrame
			} else if mediaDomain != shape.DomainFrame {
				return nil, ErrMixedDomains
			}
			frame := out[i].Frame
			stream, err := resolveStream(streamByID, len(streams), frame.StreamID)
			if err != nil {
				return nil, err
			}
			frame.StreamID = stream.ID
			if frame.Type == "" {
				frame.Type = stream.Type
			}
		}
		if out[i].Event != nil {
			count++
			event := out[i].Event
			if event.StreamID == "" && event.Stream != nil {
				event.StreamID = event.Stream.ID
			}
			if event.Type == av.EventStreamAdded && event.Stream != nil {
				if event.Stream.ID == "" {
					return nil, ErrStreamIDRequired
				}
				streamByID[event.Stream.ID] = cloneStream(*event.Stream)
			}
			if event.StreamID == "" && len(streams) == 1 {
				event.StreamID = streams[0].ID
			}
			if event.StreamID != "" {
				if _, ok := streamByID[event.StreamID]; !ok {
					return nil, ErrUnknownStream
				}
			}
		}
		if count != 1 {
			return nil, ErrInvalidMessage
		}
	}
	return out, nil
}

func resolveStream(streams map[av.StreamID]av.Stream, streamCount int, id av.StreamID) (av.Stream, error) {
	if id == "" && streamCount == 1 {
		for _, stream := range streams {
			return stream, nil
		}
	}
	stream, ok := streams[id]
	if !ok {
		return av.Stream{}, ErrUnknownStream
	}
	return stream, nil
}

func inferShape(streams []av.Stream, messages []Message) shape.Spec {
	spec := shape.Spec{Format: FormatPlayout}
	for i := range messages {
		switch {
		case messages[i].Packet != nil:
			spec.Domain = shape.DomainPacket
			return fillShapeFromStream(spec, firstStream(streams, messages[i].Packet.StreamID))
		case messages[i].Frame != nil:
			spec.Domain = shape.DomainFrame
			return fillShapeFromStream(spec, firstStream(streams, messages[i].Frame.StreamID))
		case messages[i].Event != nil && spec.Domain == "":
			spec.Domain = shape.DomainEvent
		}
	}
	if spec.Domain == "" {
		spec.Domain = shape.DomainEvent
	}
	if len(streams) == 1 {
		spec = fillShapeFromStream(spec, streams[0])
	}
	return spec
}

func firstStream(streams []av.Stream, id av.StreamID) av.Stream {
	if id != "" {
		for i := range streams {
			if streams[i].ID == id {
				return streams[i]
			}
		}
	}
	if len(streams) == 1 {
		return streams[0]
	}
	return av.Stream{}
}

func fillShapeFromStream(spec shape.Spec, stream av.Stream) shape.Spec {
	if stream.ID != "" {
		spec.StreamID = stream.ID
	}
	if stream.Type != "" {
		spec.MediaKind = stream.Type
	}
	params := stream.Codec
	if params.ID != "" {
		spec.Codec = params.ID
	}
	if params.Type != "" && spec.MediaKind == "" {
		spec.MediaKind = params.Type
	}
	spec.Width = params.Width
	spec.Height = params.Height
	spec.PixelFormat = params.PixelFormat
	spec.SampleRate = params.SampleRate
	spec.Channels = params.Channels
	spec.SampleFormat = params.SampleFormat
	return spec
}

func cloneMessages(messages []Message) []Message {
	if len(messages) == 0 {
		return nil
	}
	out := make([]Message, len(messages))
	for i := range messages {
		out[i] = Message{
			At:     messages[i].At,
			Packet: clonePacketPtr(messages[i].Packet),
			Frame:  cloneFramePtr(messages[i].Frame),
			Event:  cloneEventPtr(messages[i].Event),
		}
	}
	return out
}

func cloneStreams(streams []av.Stream) []av.Stream {
	if len(streams) == 0 {
		return nil
	}
	out := make([]av.Stream, len(streams))
	for i := range streams {
		out[i] = cloneStream(streams[i])
	}
	return out
}

func cloneStream(stream av.Stream) av.Stream {
	clone := stream
	clone.Codec = cloneCodecParameters(stream.Codec)
	clone.Metadata = cloneMetadata(stream.Metadata)
	return clone
}

func cloneCodecParameters(params av.CodecParameters) av.CodecParameters {
	clone := params
	clone.ExtraData = cloneBuffer(params.ExtraData)
	clone.Attributes = cloneMetadata(params.Attributes)
	return clone
}

func clonePacketPtr(packet *av.Packet) *av.Packet {
	if packet == nil {
		return nil
	}
	return clonePacket(packet)
}

func clonePacket(packet *av.Packet) *av.Packet {
	clone := *packet
	clone.Payload = cloneBuffer(packet.Payload)
	clone.Metadata = cloneMetadata(packet.Metadata)
	return &clone
}

func cloneFramePtr(frame *av.Frame) *av.Frame {
	if frame == nil {
		return nil
	}
	return cloneFrame(frame)
}

func cloneFrame(frame *av.Frame) *av.Frame {
	clone := *frame
	if frame.Video != nil {
		video := *frame.Video
		clone.Video = &video
	}
	if frame.Audio != nil {
		audio := *frame.Audio
		clone.Audio = &audio
	}
	clone.Planes = make([]av.Plane, len(frame.Planes))
	for i := range frame.Planes {
		clone.Planes[i] = frame.Planes[i]
		clone.Planes[i].Buffer = cloneBuffer(frame.Planes[i].Buffer)
	}
	clone.Metadata = cloneMetadata(frame.Metadata)
	return &clone
}

func cloneEventPtr(event *av.Event) *av.Event {
	if event == nil {
		return nil
	}
	clone := cloneEvent(*event)
	return &clone
}

func cloneEvent(event av.Event) av.Event {
	clone := event
	if event.Stream != nil {
		stream := cloneStream(*event.Stream)
		clone.Stream = &stream
	}
	if event.Codec != nil {
		codec := cloneCodecParameters(*event.Codec)
		clone.Codec = &codec
	}
	clone.Metadata = cloneMetadata(event.Metadata)
	return clone
}

func cloneBuffer(buffer av.Buffer) av.Buffer {
	if len(buffer.Bytes) == 0 {
		return av.Buffer{Ownership: av.BufferImmutable}
	}
	return av.Buffer{
		Bytes:     append([]byte(nil), buffer.Bytes...),
		Ownership: av.BufferImmutable,
	}
}

func cloneMetadata(metadata av.Metadata) av.Metadata {
	if metadata == nil {
		return nil
	}
	clone := make(av.Metadata, len(metadata))
	for key, value := range metadata {
		clone[key] = value
	}
	return clone
}
