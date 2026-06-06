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
	// MaxTimestampGap emits a discontinuity when a positive packet timestamp
	// delta exceeds this threshold. Backward timestamps always emit one.
	MaxTimestampGap av.Duration

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
	timestamps    []sourceTimestampState
	maxTSGap      av.Duration
	jitterOut     JitterResult
	depacketOut   DepacketizeResult
	message       pipeline.Message
	eos           av.Event
	discontinuity av.Event
	closed        bool
}

type sourceTimestampState struct {
	streamID    av.StreamID
	initialized bool
	epoch       av.Epoch
	timestamp   av.Timestamp
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
		timestamps:    makeTimestampStates(config.Streams),
		maxTSGap:      config.MaxTimestampGap,
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
		if err := s.observePacketTimestamp(ctx, emitter, &s.depacketOut.Packets[i]); err != nil {
			return err
		}
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
			if err := s.observePacketTimestamp(ctx, emitter, &s.depacketOut.Packets[j]); err != nil {
				return err
			}
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
		s.applyCodecChangedStream(event)
	}
	s.resetTimestampState(event)
	for i := range s.depacketizers {
		if err := s.depacketizers[i].HandleEvent(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func (s *Source) applyCodecChangedStream(event *av.Event) {
	if event == nil || event.Type != av.EventCodecChanged || len(s.streams) == 0 {
		return
	}
	for i := range s.streams {
		if !eventMatchesStream(s.streams[i], event) && !sourceCodecChangedReplacementMatches(s.streams[i], event, len(s.streams) == 1) {
			continue
		}
		s.streams[i] = streamWithCodecChangedEvent(s.streams[i], event)
		if i < len(s.timestamps) {
			s.timestamps[i].streamID = s.streams[i].ID
			s.timestamps[i].epoch = s.streams[i].Epoch
			s.timestamps[i].initialized = false
		}
		return
	}
}

func sourceCodecChangedReplacementMatches(stream av.Stream, event *av.Event, singleStream bool) bool {
	if !singleStream || event == nil || event.Type != av.EventCodecChanged || event.Stream == nil {
		return false
	}
	next := *event.Stream
	if next.ID == "" && event.StreamID != "" {
		next.ID = event.StreamID
	}
	if event.Codec != nil {
		next.Codec = *event.Codec
	}
	if next.Type != "" && stream.Type != "" && next.Type != stream.Type {
		return false
	}
	if next.Codec.Type != "" && stream.Type != "" && next.Codec.Type != stream.Type {
		return false
	}
	return next.ID != "" && next.ID != stream.ID
}

func streamWithCodecChangedEvent(stream av.Stream, event *av.Event) av.Stream {
	next := stream
	if event.Stream != nil {
		next = *event.Stream
	}
	if next.ID == "" {
		if event.StreamID != "" {
			next.ID = event.StreamID
		} else {
			next.ID = stream.ID
		}
	}
	if next.Type == "" {
		next.Type = stream.Type
	}
	if next.Codec.ID == "" {
		next.Codec = stream.Codec
	}
	if event.Codec != nil {
		next.Codec = *event.Codec
		if next.Codec.Type != "" {
			next.Type = next.Codec.Type
		}
	}
	if next.TimeBase == (av.TimeBase{}) {
		next.TimeBase = stream.TimeBase
	}
	if next.Epoch == 0 && event.Epoch != 0 {
		next.Epoch = event.Epoch
	}
	return next
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

func (s *Source) observePacketTimestamp(ctx context.Context, emitter pipeline.Emitter, packet *av.Packet) error {
	state := s.timestampState(packet)
	if state == nil || !packet.PTS.Base.Valid() {
		return nil
	}
	if !state.initialized || state.epoch != packet.CodecEpoch {
		state.init(packet)
		return nil
	}
	gap, ok := packet.PTS.Sub(state.timestamp)
	if !ok {
		state.init(packet)
		return nil
	}
	if timestampDiscontinuous(gap, s.maxTSGap) {
		packet.Discontinuous = true
		if err := s.emitTimestampDiscontinuity(ctx, emitter, packet, gap); err != nil {
			return err
		}
	}
	state.init(packet)
	return nil
}

func (s *Source) timestampState(packet *av.Packet) *sourceTimestampState {
	if packet == nil || len(s.timestamps) == 0 {
		return nil
	}
	for i := range s.timestamps {
		if s.timestamps[i].streamID == packet.StreamID || (packet.StreamID == "" && len(s.timestamps) == 1) {
			return &s.timestamps[i]
		}
	}
	return nil
}

func (s *Source) emitTimestampDiscontinuity(ctx context.Context, emitter pipeline.Emitter, packet *av.Packet, gap av.Duration) error {
	s.discontinuity.Reset()
	s.discontinuity.Type = av.EventDiscontinuity
	s.discontinuity.StreamID = packet.StreamID
	s.discontinuity.Epoch = packet.CodecEpoch
	s.discontinuity.Timestamp = packet.PTS
	if gap.Value < 0 {
		s.discontinuity.Reason = "timestamp moved backward"
	} else {
		s.discontinuity.Reason = "timestamp gap"
	}
	return s.emitEvent(ctx, emitter, &s.discontinuity)
}

func (s *Source) resetTimestampState(event *av.Event) {
	if event == nil {
		return
	}
	switch event.Type {
	case av.EventCodecChanged, av.EventDiscontinuity, av.EventEndOfStream, av.EventStreamRemoved:
	default:
		return
	}
	for i := range s.timestamps {
		if event.StreamID == "" || event.StreamID == s.timestamps[i].streamID {
			s.timestamps[i].initialized = false
		}
	}
}

func (s *sourceTimestampState) init(packet *av.Packet) {
	s.initialized = true
	s.epoch = packet.CodecEpoch
	s.timestamp = packet.PTS
}

func makeTimestampStates(streams []av.Stream) []sourceTimestampState {
	if len(streams) == 0 {
		return nil
	}
	states := make([]sourceTimestampState, len(streams))
	for i := range streams {
		states[i].streamID = streams[i].ID
	}
	return states
}

func timestampDiscontinuous(gap av.Duration, maxGap av.Duration) bool {
	if gap.Value < 0 {
		return true
	}
	if maxGap.Value <= 0 || !maxGap.Base.Valid() {
		return false
	}
	cmp, ok := gap.Compare(maxGap)
	return ok && cmp > 0
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
