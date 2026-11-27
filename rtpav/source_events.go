package rtpav

import (
	"context"

	"github.com/pion/rtcp"
	"github.com/thesyncim/goav/av"
)

func (s *Source) handleEvent(ctx context.Context, event *av.Event) error {
	target := av.StreamID("")
	var uniqueTargetCodec av.CodecID
	if event != nil && event.Type == av.EventCodecChanged {
		for _, stream := range s.streams {
			if eventMatchesStream(stream, event) {
				target = stream.ID
				uniqueTargetCodec = stream.Codec.ID
				break
			}
		}
		if target == "" && len(s.streams) == 1 {
			target = s.streams[0].ID
			uniqueTargetCodec = s.streams[0].Codec.ID
		}
		// A codec-intent depacketizer can use the input's display name. Save
		// its unique transport scope before applying an ID replacement.
		if !s.uniqueCodecScope(target, uniqueTargetCodec) {
			uniqueTargetCodec = ""
		}
	}
	if event != nil && (configurationEvent(event) || event.Type == av.EventDiscontinuity) {
		for _, depacketizer := range s.depacketizers {
			partial, stream := partialDepacketizer(depacketizer)
			uniqueTarget := uniqueTargetCodec != "" && depacketizer.Codec() == uniqueTargetCodec
			if partial && (eventMatchesStream(stream, event) || uniqueTarget) {
				s.discardedPartial.Add(1)
			}
		}
	}
	var canonical av.Stream
	var canonicalize bool
	if event != nil && event.Type == av.EventCodecChanged {
		canonical, canonicalize = s.applyCodecChangedStream(event)
	}
	if event != nil && event.Type == av.EventStreamAdded {
		s.applyStreamAdded(event)
	}
	if event != nil && event.Type == av.EventStreamRemoved {
		s.applyStreamRemoved(event)
	}
	s.resetTimestampState(event)
	for i := range s.depacketizers {
		if event != nil && event.Type == av.EventCodecChanged && target != "" {
			codec := s.depacketizers[i].Codec()
			uniqueTarget := uniqueTargetCodec != "" && codec == uniqueTargetCodec
			uniqueReplacement := canonicalize && s.uniqueCodecScope(canonical.ID, codec)
			if stream, known := depacketizerStream(s.depacketizers[i]); known && stream.ID != target && !uniqueTarget && !uniqueReplacement {
				continue
			}
		}
		if event != nil && event.Type == av.EventStreamRemoved {
			reset := *event
			reset.Type = av.EventDiscontinuity
			if err := s.depacketizers[i].HandleEvent(ctx, &reset); err != nil {
				return err
			}
		}
		if err := s.depacketizers[i].HandleEvent(ctx, event); err != nil {
			return err
		}
	}
	if event != nil && event.Type == av.EventStreamRemoved {
		kept := s.depacketizers[:0]
		for _, depacketizer := range s.depacketizers {
			stream, known := depacketizerStream(depacketizer)
			if !known || !eventMatchesStream(stream, event) {
				kept = append(kept, depacketizer)
			}
		}
		clear(s.depacketizers[len(kept):])
		s.depacketizers = kept
	}
	if canonicalize {
		canonicalizeCodecChangedEvent(event, canonical)
	}
	return nil
}

func (s *Source) uniqueCodecScope(target av.StreamID, codec av.CodecID) bool {
	matches := 0
	found := false
	for _, stream := range s.streams {
		if stream.Codec.ID == codec {
			matches++
			found = found || stream.ID == target
		}
	}
	return found && matches == 1
}

func partialDepacketizer(depacketizer Depacketizer) (bool, av.Stream) {
	var assembler *videoFrameAssembler
	switch d := depacketizer.(type) {
	case *VP8Depacketizer:
		assembler = &d.assembler
	case *VP9Depacketizer:
		assembler = &d.assembler
	case *H264Depacketizer:
		assembler = &d.assembler
	case *AV1Depacketizer:
		assembler = &d.assembler
	}
	if assembler == nil {
		return false, av.Stream{}
	}
	return assembler.state == asmAssembling, assembler.stream
}

func (s *Source) applyCodecChangedStream(event *av.Event) (av.Stream, bool) {
	if event == nil || event.Type != av.EventCodecChanged || len(s.streams) == 0 {
		return av.Stream{}, false
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
		return s.streams[i], true
	}
	return av.Stream{}, false
}

// applyStreamAdded tracks a stream the receiver announced mid-run
// (av.EventStreamAdded with the full av.Stream): the stream and its timestamp
// state join the source's set. A default depacketizer is bound to each new
// stream, including streams using the same codec or RTP payload type.
// The event itself continues downstream regardless.
func (s *Source) applyStreamAdded(event *av.Event) {
	if event == nil || event.Stream == nil {
		return
	}
	stream := *event.Stream
	if stream.ID == "" {
		stream.ID = event.StreamID
	}
	if stream.ID == "" {
		return
	}
	delete(s.removed, stream.ID)
	for i := range s.streams {
		if s.streams[i].ID == stream.ID {
			// Known stream re-announced: refresh its facts, keep machinery.
			s.streams[i] = stream
			return
		}
	}
	s.streams = append(s.streams, stream)
	s.timestamps = append(s.timestamps, sourceTimestampState{streamID: stream.ID})
	if stream.Codec.ID == "" {
		return
	}
	for i := range s.depacketizers {
		if existing, ok := depacketizerStream(s.depacketizers[i]); ok && existing.ID == stream.ID {
			return
		}
	}
	if depacketizer := defaultDepacketizerFor(stream.Codec.ID, stream); depacketizer != nil {
		s.depacketizers = append(s.depacketizers, depacketizer)
	}
}

// applyStreamRemoved drops a removed stream from the source's set; its
// timestamp state goes with it (resetTimestampState already handles the
// in-flight reset for the matching id).
func (s *Source) applyStreamRemoved(event *av.Event) {
	if event == nil || event.StreamID == "" {
		return
	}
	delete(s.epochs, event.StreamID)
	s.removed[event.StreamID] = true
	streams := s.streams[:0]
	for i := range s.streams {
		if s.streams[i].ID == event.StreamID {
			continue
		}
		streams = append(streams, s.streams[i])
	}
	clear(s.streams[len(streams):])
	s.streams = streams
	timestamps := s.timestamps[:0]
	for i := range s.timestamps {
		if s.timestamps[i].streamID == event.StreamID {
			continue
		}
		timestamps = append(timestamps, s.timestamps[i])
	}
	s.timestamps = timestamps
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

func canonicalizeCodecChangedEvent(event *av.Event, stream av.Stream) {
	if event == nil || event.Type != av.EventCodecChanged {
		return
	}
	if stream.ID != "" {
		event.StreamID = stream.ID
	}
	if stream.Epoch != 0 {
		event.Epoch = stream.Epoch
	}
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
