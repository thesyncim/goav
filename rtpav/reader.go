package rtpav

import (
	"context"
	"errors"
	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
)

// StaticReader is the legacy transport interface, supported only when its
// configuration is fixed for its entire lifetime.
type StaticReader interface {
	Streams(context.Context) ([]av.Stream, error)
	PayloadMap() PayloadMap
	ReadRTP(context.Context) (*rtp.Packet, error)
	Events() <-chan av.Event
	Close() error
}

type staticReader struct {
	StaticReader
	payloads PayloadMap
}

// Static adapts a fixed-configuration legacy reader with an owned, frozen map.
// Dynamic configuration events fail explicitly. It cannot reconstruct atomic
// configuration from a legacy reader; dynamic transports must migrate ReadRTP.
func Static(reader StaticReader, payloads PayloadMap) PacketReader {
	var frozen PayloadMap
	if payloads == nil && reader != nil {
		payloads = reader.PayloadMap()
	}
	if payloads != nil {
		frozen = NewStaticPayloadMap(payloads.Epoch(), payloads.Codecs())
	}
	r := &staticReader{reader, frozen}
	if feedback, ok := reader.(FeedbackWriter); ok {
		return feedbackReader{PacketReader: r, FeedbackWriter: feedback}
	}
	return r
}

func (r *staticReader) PayloadMap() PayloadMap { return r.payloads }
func (r *staticReader) ReadRTP(ctx context.Context) (*rtp.Packet, *av.Event, PayloadMap, error) {
	if r.StaticReader == nil {
		return nil, nil, nil, ErrNilReceiver
	}
	if r.payloads == nil {
		return nil, nil, nil, ErrInvalidConfiguration
	}
	select {
	case event, ok := <-r.StaticReader.Events():
		if ok {
			if configurationEvent(&event) {
				return nil, nil, nil, ErrOrderedConfigurationRequired
			}
			return nil, &event, r.payloads, nil
		}
	default:
	}
	packet, err := r.StaticReader.ReadRTP(ctx)
	if current := r.StaticReader.PayloadMap(); current != nil && current.Epoch() != r.payloads.Epoch() {
		return nil, nil, nil, errors.Join(ErrOrderedConfigurationRequired, err)
	}
	select {
	case event, ok := <-r.StaticReader.Events():
		if ok {
			if configurationEvent(&event) {
				return nil, nil, nil, errors.Join(ErrOrderedConfigurationRequired, err)
			}
			return packet, &event, r.payloads, err
		}
	default:
	}
	return packet, nil, r.payloads, err
}

// ReadRTPFunc is the single atomic read operation, including event-only records.
type ReadRTPFunc func(context.Context) (*rtp.Packet, *av.Event, PayloadMap, error)
type decoratedReader struct {
	PacketReader
	read ReadRTPFunc
}

func (r decoratedReader) ReadRTP(ctx context.Context) (*rtp.Packet, *av.Event, PayloadMap, error) {
	return r.read(ctx)
}

type feedbackReader struct {
	PacketReader
	FeedbackWriter
}

var _ FeedbackWriter = feedbackReader{}

// DecorateReader intercepts the authoritative atomic read for logging, metrics,
// or tracing, while preserving the reader's other operations and optional RTCP.
// The decorator must preserve packet/event/map ordering and propagate errors.
func DecorateReader(reader PacketReader, wrap func(ReadRTPFunc) ReadRTPFunc) PacketReader {
	if wrap == nil {
		return reader
	}
	r := decoratedReader{PacketReader: reader, read: wrap(reader.ReadRTP)}
	if feedback, ok := reader.(FeedbackWriter); ok {
		return feedbackReader{PacketReader: r, FeedbackWriter: feedback}
	}
	return r
}

func configurationEvent(event *av.Event) bool {
	if event == nil {
		return false
	}
	switch event.Type {
	case av.EventCodecChanged, av.EventStreamAdded, av.EventStreamRemoved:
		return true
	}
	return false
}
