package rtpav

import (
	"context"
	"errors"
	"io"
	"maps"
	"strings"
	"sync/atomic"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

// SourceConfig assembles an RTP Source: the transport reader, optional
// feedback writer and jitter buffer, the depacketizers per codec, and the
// per-read output bounds.
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

// Source adapts a PacketReader into a pipeline.Source: RTP is read,
// optionally reordered, depacketized into av.Packets, stamped with media
// timestamps, and emitted with loss/discontinuity events; RTCP feedback flows
// back through the FeedbackWriter.
type Source struct {
	name          string
	detail        string
	receiver      PacketReader
	feedback      FeedbackWriter
	payloads      PayloadMap
	scope         av.StreamID
	epochs        map[av.StreamID]av.Epoch
	removed       map[av.StreamID]bool
	jitter        JitterBuffer
	jitterScope   av.StreamID
	jitterScoped  bool
	depacketizers []Depacketizer
	streams       []av.Stream
	timestamps    []sourceTimestampState
	maxTSGap      av.Duration
	jitterOut     JitterResult
	depacketOut   DepacketizeResult
	message       pipeline.Message
	// packet is the header a depacketized packet is published under.
	packet           av.Packet
	eos              av.Event
	event            av.Event
	discontinuity    av.Event
	closed           bool
	discardedPartial atomic.Uint64
}

type sourceTimestampState struct {
	streamID    av.StreamID
	initialized bool
	epoch       av.Epoch
	// timestamp is the last extended (unwrapped) timestamp.
	timestamp av.Timestamp
}

var _ pipeline.Source = (*Source)(nil)
var _ pipeline.NodeDescriber = (*Source)(nil)

// NewSource builds an RTP source; the receiver is required and unset bounds
// take small defaults.
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
		epochs:        map[av.StreamID]av.Epoch{},
		removed:       map[av.StreamID]bool{},
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

// Name returns the source's node name.
func (s *Source) Name() string {
	return s.name
}

// DroppedMessages counts incomplete built-in access units discarded at a
// configuration, removal, or discontinuity boundary.
func (s *Source) DroppedMessages() uint64 { return s.discardedPartial.Load() }

// DescribeNode reports the source's spec entry for Describe.
func (s *Source) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: s.name, Kind: pipeline.NodeSource, Detail: s.detail}
}

// Start announces the declared streams, then reads, reorders, and
// depacketizes RTP until io.EOF, flushing partial state at end of stream.
func (s *Source) Start(ctx context.Context, emitter pipeline.Emitter) error {
	if s.closed {
		return ErrClosed
	}
	for {
		packet, event, payloads, err := s.receiver.ReadRTP(ctx)
		if err == nil || event != nil || packet != nil {
			if payloads == nil || (err == nil && packet == nil && event == nil) {
				return ErrInvalidConfiguration
			}
			scope, scopeErr := s.recordScope(packet, event, payloads)
			if scopeErr != nil {
				return scopeErr
			}
			target := recordEventTarget(event)
			replacesJitterStream := event != nil && event.Type == av.EventCodecChanged &&
				(len(s.streams) <= 1 || target == s.jitterScope)
			resetsJitter := event != nil && s.jitterScoped && (replacesJitterStream ||
				event.Type == av.EventStreamRemoved && target == s.jitterScope ||
				event.Type == av.EventDiscontinuity && (target == "" || target == s.jitterScope))
			if s.jitter != nil && packet != nil {
				if s.jitterScoped && s.jitterScope != scope && !s.removed[s.jitterScope] && !replacesJitterStream {
					return ErrJitterStreamConflict
				}
			}
			epoch := payloads.Epoch()
			epochScope, epochScopeErr := recordEpochScope(packet, event, payloads)
			if epochScopeErr != nil {
				return epochScopeErr
			}
			if event != nil && epochScope != "" && scope != "" && epochScope != scope {
				return ErrInvalidConfiguration
			}
			if epochScope != "" && configurationEvent(event) && event.Epoch != 0 && event.Epoch != epoch {
				return ErrInvalidConfiguration
			}
			if previous, seen := s.epochs[epochScope]; seen {
				if epoch < previous {
					return ErrEpochRegression
				}
				if epoch != previous && !configurationEvent(event) {
					return ErrInvalidConfiguration
				}
			}
			// The jitter window contains one stream/configuration at a time.
			// Drain using its retained map BEFORE switching. Complete old units
			// emit normally; configuration events discard incomplete assembly.
			boundary := s.payloads != nil && (scope != s.scope || epoch != s.payloads.Epoch())
			if boundary || configurationEvent(event) || event != nil && event.Type == av.EventDiscontinuity {
				if flushErr := s.flushJitter(ctx, emitter); flushErr != nil {
					return flushErr
				}
			}
			if resetsJitter && s.jitter != nil {
				s.jitter.Reset()
				if event.Type == av.EventStreamRemoved {
					s.jitterScoped = false
				}
			}
			if replacesJitterStream && s.jitterScoped {
				s.jitterScope = scope
			}
			if s.jitter != nil && packet != nil {
				s.jitterScope, s.jitterScoped = scope, true
			}
			s.payloads, s.scope = payloads, scope
			s.epochs[epochScope] = epoch
			if event != nil {
				if eventErr := s.emitEvent(ctx, emitter, event); eventErr != nil {
					return eventErr
				}
			}
		}
		if packet != nil {
			if handleErr := s.handleRTP(ctx, emitter, packet); handleErr != nil {
				return handleErr
			}
		}
		if err != nil {
			if isReadEOF(err) {
				if err := s.flush(ctx, emitter); err != nil {
					return err
				}
				s.eos = endOfStreamEvent(s.streams)
				return s.emitEvent(ctx, emitter, &s.eos)
			}
			return err
		}
	}
}

// A joined finalization/protocol failure is still a failure when one sibling
// cause is EOF. Only an error tree made entirely of EOF marks successful input
// completion; ordinary wrappers preserve that meaning.
func isReadEOF(err error) bool {
	switch cause := err.(type) {
	case interface{ Unwrap() []error }:
		leaves := cause.Unwrap()
		if len(leaves) == 0 {
			return false
		}
		for _, leaf := range leaves {
			if !isReadEOF(leaf) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		return isReadEOF(cause.Unwrap())
	default:
		return errors.Is(err, io.EOF)
	}
}

// Close closes the wrapped receiver once.
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
	payload, ok := s.lookupPayload(packet.PayloadType)
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
		if err := s.emitPacket(ctx, emitter, &s.depacketOut.Packets[i]); err != nil {
			return err
		}
	}
	return nil
}

func (s *Source) lookupPayload(payloadType uint8) (PayloadCodec, bool) {
	if s.payloads == nil {
		return PayloadCodec{}, false
	}
	return s.payloads.Lookup(payloadType)
}

func (s *Source) flushJitter(ctx context.Context, emitter pipeline.Emitter) error {
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
	return nil
}

func (s *Source) flush(ctx context.Context, emitter pipeline.Emitter) error {
	if err := s.flushJitter(ctx, emitter); err != nil {
		return err
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
			if err := s.emitPacket(ctx, emitter, &s.depacketOut.Packets[j]); err != nil {
				return err
			}
		}
	}
	return nil
}

// emitPacket publishes a depacketized packet. Its payload is a view into the
// received RTP packet or the depacketizer's reassembly scratch, both reused,
// so it is copied once into a pooled buffer every branch then shares.
func (s *Source) emitPacket(ctx context.Context, emitter pipeline.Emitter, packet *av.Packet) error {
	s.packet = *packet
	if packet.Payload.Owner == nil {
		s.packet.Payload = av.NewBuffer(len(packet.Payload.Bytes))
		copy(s.packet.Payload.Bytes, packet.Payload.Bytes)
	} else {
		packet.Payload = av.Buffer{}
	}
	s.message.Kind = pipeline.MessagePacket
	s.message.Packet = &s.packet
	s.message.Frame = nil
	s.message.Event = nil
	err := emitter.Emit(ctx, &s.message)
	// Downstream holds its own reference now.
	s.packet.Release()
	return err
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
	if event == nil {
		return nil
	}
	s.event = *event
	if event.Stream != nil {
		stream := cloneRTPStream(*event.Stream)
		s.event.Stream = &stream
		if s.event.StreamID == "" {
			s.event.StreamID = stream.ID
		}
	}
	if event.Codec != nil {
		codec := clonePayloadCodec(PayloadCodec{Parameters: *event.Codec}).Parameters
		s.event.Codec = &codec
	}
	event = &s.event
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
	target := av.StreamID("")
	if event != nil && event.Type == av.EventCodecChanged {
		for _, stream := range s.streams {
			if eventMatchesStream(stream, event) {
				target = stream.ID
				break
			}
		}
		if target == "" && len(s.streams) == 1 {
			target = s.streams[0].ID
		}
	}
	if event != nil && (configurationEvent(event) || event.Type == av.EventDiscontinuity) {
		for _, depacketizer := range s.depacketizers {
			if partial, stream := partialDepacketizer(depacketizer); partial && eventMatchesStream(stream, event) {
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
			if stream, known := depacketizerStream(s.depacketizers[i]); known && stream.ID != target && !s.uniqueCodecScope(target, s.depacketizers[i].Codec()) {
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
	return assembler.started, assembler.stream
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
	var found Depacketizer
	matches := 0
	for i := range s.depacketizers {
		if s.depacketizers[i].Codec() == id {
			stream, known := depacketizerStream(s.depacketizers[i])
			if payload.StreamID != "" && known && stream.ID == payload.StreamID {
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
	return found
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
	var scope av.StreamID
	var target av.StreamID
	if event != nil {
		scope = event.StreamID
		target = scope
		if event.Stream != nil {
			if scope != "" && event.Stream.ID != "" && scope != event.Stream.ID {
				// CodecChanged can target the old ID while declaring its
				// replacement. The new ID must not take over a sibling.
				if event.Type != av.EventCodecChanged {
					return "", ErrInvalidConfiguration
				}
				for _, stream := range s.streams {
					if stream.ID == event.Stream.ID {
						return "", ErrInvalidConfiguration
					}
				}
				scope = event.Stream.ID
			}
			if scope == "" {
				scope = event.Stream.ID
			}
		}
		if target == "" {
			target = scope
		}
	}
	if packet != nil {
		payload, ok := payloads.Lookup(packet.PayloadType)
		if !ok {
			return "", ErrPayloadNotFound
		}
		if payload.StreamID != "" {
			if scope != "" && scope != payload.StreamID {
				return "", ErrInvalidConfiguration
			}
			scope = payload.StreamID
		}
		if scope == "" {
			for _, stream := range s.streams {
				if stream.Codec.ID == payload.Parameters.ID {
					if scope != "" {
						return "", ErrAmbiguousStream
					}
					scope = stream.ID
				}
			}
		}
	}
	addsStream := event != nil && event.Type == av.EventStreamAdded && event.Stream != nil && recordEventTarget(event) == scope
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
