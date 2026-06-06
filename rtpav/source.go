package rtpav

import (
	"context"
	"errors"
	"io"
	"strings"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

type SourceConfig struct {
	Name          string
	Detail        string
	Receiver      PacketReader
	Feedback      FeedbackWriter
	Jitter        JitterBuffer
	Depacketizers []Depacketizer
	Streams       []av.Stream

	MaxReady    int
	MaxEvents   int
	MaxFeedback int
	MaxPackets  int
}

type Source struct {
	name          string
	detail        string
	receiver      PacketReader
	feedback      FeedbackWriter
	payloads      PayloadMap
	jitter        JitterBuffer
	depacketizers []Depacketizer
	streams       []av.Stream
	jitterOut     JitterResult
	depacketOut   DepacketizeResult
	message       pipeline.Message
	eos           av.Event
	closed        bool
}

var _ pipeline.Source = (*Source)(nil)
var _ pipeline.NodeDescriber = (*Source)(nil)

func NewSource(config SourceConfig) (*Source, error) {
	if config.Receiver == nil {
		return nil, ErrNilReceiver
	}
	name := config.Name
	if name == "" {
		name = "rtp"
	}
	maxReady := positiveOrDefault(config.MaxReady, 16)
	maxEvents := positiveOrDefault(config.MaxEvents, 8)
	maxFeedback := positiveOrDefault(config.MaxFeedback, 4)
	maxPackets := positiveOrDefault(config.MaxPackets, 8)

	return &Source{
		name:          name,
		detail:        config.Detail,
		receiver:      config.Receiver,
		feedback:      feedbackWriter(config.Receiver, config.Feedback),
		payloads:      config.Receiver.PayloadMap(),
		jitter:        config.Jitter,
		depacketizers: config.Depacketizers,
		streams:       cloneStreams(config.Streams),
		jitterOut: JitterResult{
			Ready:    make([]*rtp.Packet, 0, maxReady),
			Events:   make([]av.Event, 0, maxEvents),
			Feedback: make([]rtcp.Packet, 0, maxFeedback),
		},
		depacketOut: DepacketizeResult{
			Packets:  make([]av.Packet, 0, maxPackets),
			Events:   make([]av.Event, 0, maxEvents),
			Feedback: make([]rtcp.Packet, 0, maxFeedback),
		},
	}, nil
}

func (s *Source) Name() string {
	return s.name
}

func (s *Source) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: s.name, Kind: pipeline.NodeSource, Detail: s.detail}
}

func (s *Source) Start(ctx context.Context, emitter pipeline.Emitter) error {
	if s.closed {
		return ErrClosed
	}
	for {
		if err := s.emitReceiverEvents(ctx, emitter); err != nil {
			return err
		}
		packet, err := s.receiver.ReadRTP(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if err := s.flush(ctx, emitter); err != nil {
					return err
				}
				s.eos = endOfStreamEvent(s.streams)
				return s.emitEvent(ctx, emitter, &s.eos)
			}
			return err
		}
		if err := s.handleRTP(ctx, emitter, packet); err != nil {
			return err
		}
	}
}

func (s *Source) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.receiver.Close()
}

func (s *Source) handleRTP(ctx context.Context, emitter pipeline.Emitter, packet *rtp.Packet) error {
	if s.jitter == nil {
		return s.depacketize(ctx, emitter, packet)
	}
	s.jitterOut.Reset()
	if err := s.jitter.PushInto(ctx, packet, &s.jitterOut); err != nil {
		return err
	}
	if err := s.emitEvents(ctx, emitter, s.jitterOut.Events); err != nil {
		return err
	}
	if err := s.writeFeedback(ctx, s.jitterOut.Feedback); err != nil {
		return err
	}
	for i := range s.jitterOut.Ready {
		if err := s.depacketize(ctx, emitter, s.jitterOut.Ready[i]); err != nil {
			return err
		}
	}
	return nil
}

func (s *Source) depacketize(ctx context.Context, emitter pipeline.Emitter, packet *rtp.Packet) error {
	if s.payloads == nil {
		return ErrPayloadNotFound
	}
	payload, ok := s.payloads.Lookup(packet.PayloadType)
	if !ok {
		return ErrPayloadNotFound
	}
	depacketizer := s.depacketizerFor(payload)
	if depacketizer == nil {
		return ErrDepacketizerNotFound
	}
	s.depacketOut.Reset()
	if err := depacketizer.PushInto(ctx, packet, payload, &s.depacketOut); err != nil {
		return err
	}
	if err := s.emitEvents(ctx, emitter, s.depacketOut.Events); err != nil {
		return err
	}
	if err := s.writeFeedback(ctx, s.depacketOut.Feedback); err != nil {
		return err
	}
	for i := range s.depacketOut.Packets {
		s.message.Kind = pipeline.MessagePacket
		s.message.Packet = &s.depacketOut.Packets[i]
		s.message.Frame = nil
		s.message.Event = nil
		if err := emitter.Emit(ctx, &s.message); err != nil {
			return err
		}
	}
	return nil
}

func (s *Source) flush(ctx context.Context, emitter pipeline.Emitter) error {
	if s.jitter != nil {
		s.jitterOut.Reset()
		if err := s.jitter.FlushInto(ctx, &s.jitterOut); err != nil {
			return err
		}
		if err := s.emitEvents(ctx, emitter, s.jitterOut.Events); err != nil {
			return err
		}
		for i := range s.jitterOut.Ready {
			if err := s.depacketize(ctx, emitter, s.jitterOut.Ready[i]); err != nil {
				return err
			}
		}
	}
	for i := range s.depacketizers {
		s.depacketOut.Reset()
		if err := s.depacketizers[i].FlushInto(ctx, &s.depacketOut); err != nil {
			return err
		}
		if err := s.emitEvents(ctx, emitter, s.depacketOut.Events); err != nil {
			return err
		}
		for j := range s.depacketOut.Packets {
			s.message.Kind = pipeline.MessagePacket
			s.message.Packet = &s.depacketOut.Packets[j]
			s.message.Frame = nil
			s.message.Event = nil
			if err := emitter.Emit(ctx, &s.message); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Source) emitReceiverEvents(ctx context.Context, emitter pipeline.Emitter) error {
	for {
		select {
		case event, ok := <-s.receiver.Events():
			if !ok {
				return nil
			}
			if err := s.emitEvent(ctx, emitter, &event); err != nil {
				return err
			}
		default:
			return nil
		}
	}
}

func (s *Source) emitEvents(ctx context.Context, emitter pipeline.Emitter, events []av.Event) error {
	for i := range events {
		if err := s.emitEvent(ctx, emitter, &events[i]); err != nil {
			return err
		}
	}
	return nil
}

func (s *Source) emitEvent(ctx context.Context, emitter pipeline.Emitter, event *av.Event) error {
	if err := s.handleEvent(ctx, event); err != nil {
		return err
	}
	s.message.Kind = pipeline.MessageEvent
	s.message.Packet = nil
	s.message.Frame = nil
	s.message.Event = event
	return emitter.Emit(ctx, &s.message)
}

func (s *Source) handleEvent(ctx context.Context, event *av.Event) error {
	if event != nil && event.Type == av.EventCodecChanged {
		s.payloads = s.receiver.PayloadMap()
	}
	for i := range s.depacketizers {
		if err := s.depacketizers[i].HandleEvent(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func (s *Source) writeFeedback(ctx context.Context, feedback []rtcp.Packet) error {
	if len(feedback) == 0 {
		return nil
	}
	if s.feedback == nil {
		return nil
	}
	return s.feedback.WriteRTCP(ctx, feedback)
}

func (s *Source) depacketizerFor(payload PayloadCodec) Depacketizer {
	id := payload.Parameters.ID
	if id == "" {
		id = codecIDFromMIME(payload.MIMEType)
	}
	for i := range s.depacketizers {
		if s.depacketizers[i].Codec() == id {
			return s.depacketizers[i]
		}
	}
	return nil
}

func codecIDFromMIME(mimeType string) av.CodecID {
	switch {
	case strings.EqualFold(mimeType, MIMEOpus):
		return av.CodecOpus
	case strings.EqualFold(mimeType, MIMEVP8):
		return av.CodecVP8
	case strings.EqualFold(mimeType, MIMEVP9):
		return av.CodecVP9
	case strings.EqualFold(mimeType, MIMEH264):
		return av.CodecH264
	case strings.EqualFold(mimeType, MIMEAV1):
		return av.CodecAV1
	default:
		return av.CodecUnknown
	}
}

func positiveOrDefault(value int, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

func cloneStreams(streams []av.Stream) []av.Stream {
	if len(streams) == 0 {
		return nil
	}
	out := make([]av.Stream, len(streams))
	copy(out, streams)
	return out
}

func endOfStreamEvent(streams []av.Stream) av.Event {
	event := av.Event{Type: av.EventEndOfStream}
	if len(streams) != 1 {
		return event
	}
	event.StreamID = streams[0].ID
	event.Epoch = streams[0].Epoch
	return event
}

func feedbackWriter(reader PacketReader, explicit FeedbackWriter) FeedbackWriter {
	if explicit != nil {
		return explicit
	}
	feedback, ok := reader.(FeedbackWriter)
	if !ok {
		return nil
	}
	return feedback
}
