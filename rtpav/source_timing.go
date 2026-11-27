package rtpav

import (
	"context"
	"maps"
	"strings"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

func (s *Source) observePacketTimestamp(ctx context.Context, emitter pipeline.Emitter, packet *av.Packet) error {
	state := s.timestampState(packet)
	if state == nil || !packet.PTS.Base.Valid() {
		return nil
	}
	if !state.initialized || state.epoch != packet.CodecEpoch {
		state.init(packet)
		return nil
	}
	// RTP timestamps are 32-bit and start anywhere, so a stream wraps in
	// practice. Extend each one by its signed 32-bit step from the previous
	// timestamp of this stream epoch; the step is exact across the wrap and
	// for small reorders, and already-extended timestamps pass unchanged.
	if packet.PTS.Base == state.timestamp.Base {
		packet.PTS.Value = state.timestamp.Value + int64(int32(uint32(packet.PTS.Value)-uint32(state.timestamp.Value)))
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
	if s.lastDepackValid &&
		s.lastDepackPT == payload.PayloadType &&
		s.lastDepackID == id &&
		s.lastDepackMIME == payload.MIMEType &&
		s.lastDepackSID == payload.StreamID {
		return s.lastDepack
	}
	var found Depacketizer
	matches := 0
	for i := range s.depacketizers {
		if s.depacketizers[i].Codec() == id {
			stream, known := depacketizerStream(s.depacketizers[i])
			if payload.StreamID != "" && known && stream.ID == payload.StreamID {
				s.cacheDepacketizer(payload, id, s.depacketizers[i])
				return s.depacketizers[i]
			}
			matches++
			found = s.depacketizers[i]
		}
	}
	if matches != 1 {
		return nil
	}
	if payload.StreamID != "" {
		matches := 0
		for _, stream := range s.streams {
			if stream.Codec.ID == id {
				matches++
			}
		}
		if matches > 1 {
			return nil
		}
	}
	if found != nil {
		s.cacheDepacketizer(payload, id, found)
	}
	return found
}

func (s *Source) cacheDepacketizer(payload PayloadCodec, id av.CodecID, d Depacketizer) {
	s.lastDepackValid = true
	s.lastDepackPT = payload.PayloadType
	s.lastDepackID = id
	s.lastDepackMIME = payload.MIMEType
	s.lastDepackSID = payload.StreamID
	s.lastDepack = d
}

// Untagged maps use a session-wide configuration generation. A tagged map
// belongs to one stream; other tracks can advance independently.
func recordEpochScope(packet *rtp.Packet, event *av.Event, payloads PayloadMap) (av.StreamID, error) {
	if packet != nil && !configurationEvent(event) {
		payload, _ := payloads.Lookup(packet.PayloadType)
		return payload.StreamID, nil
	}
	var scope av.StreamID
	seen := false
	for _, payload := range payloads.Codecs() {
		if seen && payload.StreamID != scope {
			return "", ErrInvalidConfiguration
		}
		scope, seen = payload.StreamID, true
	}
	if !seen && event != nil {
		// A removal has no remaining payload entry to tag. Its epoch still
		// belongs to that stream, not an aggregate session generation.
		scope = recordEventTarget(event)
		if event.Stream != nil && event.Stream.ID != "" {
			scope = event.Stream.ID
		}
	}
	return scope, nil
}

func (s *Source) eventScope(event *av.Event) (av.StreamID, error) {
	if event == nil {
		return "", nil
	}
	scope := event.StreamID
	if event.Stream == nil {
		return scope, nil
	}
	if scope != "" && event.Stream.ID != "" && scope != event.Stream.ID {
		// CodecChanged can target the old ID while declaring its replacement.
		// The new ID must not take over a sibling.
		if event.Type != av.EventCodecChanged {
			return "", ErrInvalidConfiguration
		}
		for _, stream := range s.streams {
			if stream.ID == event.Stream.ID {
				return "", ErrInvalidConfiguration
			}
		}
		return event.Stream.ID, nil
	}
	if scope == "" {
		return event.Stream.ID, nil
	}
	return scope, nil
}

func (s *Source) packetScope(packet *rtp.Packet, payloads PayloadMap, scope av.StreamID) (av.StreamID, error) {
	payload, ok := payloads.Lookup(packet.PayloadType)
	if !ok {
		return "", ErrPayloadNotFound
	}
	if payload.StreamID != "" {
		if scope != "" && scope != payload.StreamID {
			return "", ErrInvalidConfiguration
		}
		return payload.StreamID, nil
	}
	if scope != "" {
		return scope, nil
	}
	for _, stream := range s.streams {
		if stream.Codec.ID == payload.Parameters.ID {
			if scope != "" {
				return "", ErrAmbiguousStream
			}
			scope = stream.ID
		}
	}
	return scope, nil
}

func recordEventTarget(event *av.Event) av.StreamID {
	if event == nil {
		return ""
	}
	if event.StreamID != "" {
		return event.StreamID
	}
	if event.Stream != nil {
		return event.Stream.ID
	}
	return ""
}

func (s *Source) recordScope(packet *rtp.Packet, event *av.Event, payloads PayloadMap) (av.StreamID, error) {
	scope, err := s.eventScope(event)
	if err != nil {
		return "", err
	}
	if packet != nil {
		scope, err = s.packetScope(packet, payloads, scope)
		if err != nil {
			return "", err
		}
	}
	target := recordEventTarget(event)
	addsStream := event != nil && event.Type == av.EventStreamAdded && event.Stream != nil && target == scope
	if packet != nil && s.removed[scope] && !addsStream {
		return "", ErrUnknownStream
	}
	if scope == "" && len(s.streams) == 1 {
		scope = s.streams[0].ID
	}
	if scope == "" && configurationEvent(event) && len(s.streams) > 1 {
		return "", ErrAmbiguousStream
	}
	if event != nil && event.Type == av.EventCodecChanged && len(s.streams) > 1 {
		known := false
		for _, stream := range s.streams {
			known = known || stream.ID == target
		}
		if !known {
			return "", ErrUnknownStream
		}
	}
	if scope == "" && len(s.streams) == 0 {
		scope = s.scope
	}
	return scope, nil
}

func codecIDFromMIME(mimeType string) av.CodecID {
	switch {
	case strings.EqualFold(mimeType, av.MIMEOpus):
		return av.CodecOpus
	case strings.EqualFold(mimeType, av.MIMEVP8):
		return av.CodecVP8
	case strings.EqualFold(mimeType, av.MIMEVP9):
		return av.CodecVP9
	case strings.EqualFold(mimeType, av.MIMEH264):
		return av.CodecH264
	case strings.EqualFold(mimeType, av.MIMEAV1):
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
	for i, stream := range streams {
		out[i] = cloneRTPStream(stream)
	}
	return out
}

func cloneRTPStream(stream av.Stream) av.Stream {
	stream.Metadata = maps.Clone(stream.Metadata)
	stream.Codec = clonePayloadCodec(PayloadCodec{Parameters: stream.Codec}).Parameters
	return stream
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
