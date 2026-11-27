package rtpav

import (
	"context"
	"errors"
	"io"
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

type sourceState uint8

const (
	sourceIdle sourceState = iota
	sourceRunning
	sourceClosed
)

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
	state            atomic.Uint32
	discardedPartial atomic.Uint64

	// Steady-state depacketizer cache: avoids linearly scanning s.depacketizers
	// on every RTP packet when PT/stream/codec are unchanged.
	lastDepackValid bool
	lastDepackPT    uint8
	lastDepackID    av.CodecID
	lastDepackMIME  string
	lastDepackSID   av.StreamID
	lastDepack      Depacketizer
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
	if !s.state.CompareAndSwap(uint32(sourceIdle), uint32(sourceRunning)) {
		if sourceState(s.state.Load()) == sourceClosed {
			return ErrClosed
		}
		return errors.New("rtpav: source already started")
	}
	defer s.state.CompareAndSwap(uint32(sourceRunning), uint32(sourceIdle))
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
				s.lastDepackValid = false
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
	if s.state.Swap(uint32(sourceClosed)) == uint32(sourceClosed) {
		return nil
	}
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
	if s.jitter == nil {
		return nil
	}
	for {
		s.jitterOut.Reset()
		flushErr := s.jitter.FlushInto(ctx, &s.jitterOut)
		if flushErr != nil && !errors.Is(flushErr, ErrResultFull) {
			return flushErr
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
		if flushErr == nil {
			return nil
		}
		if len(s.jitterOut.Ready) == 0 && len(s.jitterOut.Events) == 0 && len(s.jitterOut.Feedback) == 0 {
			return flushErr
		}
	}
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
		if err := s.writeFeedback(ctx, s.depacketOut.Feedback); err != nil {
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
