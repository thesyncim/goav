package consumer_test

import (
	"context"
	"io"
	"reflect"
	"testing"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/rtpav"
)

type publishedRecord struct {
	packet   *rtp.Packet
	event    *av.Event
	payloads rtpav.PayloadMap
}
type publishedReader struct {
	records []publishedRecord
	at      int
}

var _ rtpav.PacketReader = (*publishedReader)(nil)

func (r *publishedReader) Streams(context.Context) ([]av.Stream, error) { return nil, nil }
func (r *publishedReader) PayloadMap() rtpav.PayloadMap                 { return r.records[0].payloads }
func (r *publishedReader) Events() <-chan av.Event                      { return nil }
func (r *publishedReader) Close() error                                 { return nil }
func (r *publishedReader) ReadRTP(context.Context) (*rtp.Packet, *av.Event, rtpav.PayloadMap, error) {
	if r.at == len(r.records) {
		return nil, nil, nil, io.EOF
	}
	record := r.records[r.at]
	r.at++
	return record.packet, record.event, record.payloads, nil
}

type publishedEmitter func(context.Context, *pipeline.Message) error

func (e publishedEmitter) Emit(ctx context.Context, m *pipeline.Message) error { return e(ctx, m) }

func TestResolvedRTPRecordsPreserveIndependentTrackConfiguration(t *testing.T) {
	a := av.Stream{ID: "a", Type: av.MediaAudio, Epoch: 1, TimeBase: av.RTPTimeBase(48000), Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio, ClockRate: 48000, SampleRate: 48000, Channels: 1}}
	b := a
	b.ID = "b"
	c := a
	c.ID = "c"
	a2 := a
	a2.Epoch = 2
	payloads := func(s av.Stream) rtpav.PayloadMap {
		return rtpav.NewStaticPayloadMap(s.Epoch, []rtpav.PayloadCodec{{StreamID: s.ID, PayloadType: 111, Parameters: s.Codec, MIMEType: av.MIMEOpus, ClockRate: 48000, Channels: 1}})
	}
	packet := func(sequence uint16) *rtp.Packet {
		return &rtp.Packet{PayloadType: 111, SequenceNumber: sequence, Timestamp: uint32(sequence) * 960, Payload: []byte{0xf8}}
	}
	reader := &publishedReader{records: []publishedRecord{
		{event: &av.Event{Type: av.EventStreamAdded, StreamID: a.ID, Stream: &a}, payloads: payloads(a)},
		{event: &av.Event{Type: av.EventStreamAdded, StreamID: b.ID, Stream: &b}, payloads: payloads(b)},
		{event: &av.Event{Type: av.EventStreamAdded, StreamID: c.ID, Stream: &c}, payloads: payloads(c)},
		{packet: packet(1), payloads: payloads(a)},
		{event: &av.Event{Type: av.EventCodecChanged, StreamID: a.ID, Epoch: 2, Stream: &a2}, payloads: payloads(a2)},
		{packet: packet(2), payloads: payloads(b)},
		{packet: packet(3), payloads: payloads(c)},
		{packet: packet(4), payloads: payloads(a2)},
	}}
	reads := 0
	decorated := rtpav.DecorateReader(reader, func(next rtpav.ReadRTPFunc) rtpav.ReadRTPFunc {
		return func(ctx context.Context) (*rtp.Packet, *av.Event, rtpav.PayloadMap, error) { reads++; return next(ctx) }
	})
	source, _, err := rtpav.Receive(decorated).OpenSource(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	var streams []av.StreamID
	var epochs []av.Epoch
	if err := source.Start(t.Context(), publishedEmitter(func(_ context.Context, m *pipeline.Message) error {
		if m.Packet != nil {
			streams = append(streams, m.Packet.StreamID)
			epochs = append(epochs, m.Packet.CodecEpoch)
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(streams, []av.StreamID{"a", "b", "c", "a"}) || !reflect.DeepEqual(epochs, []av.Epoch{1, 1, 1, 2}) || reads != 9 {
		t.Fatalf("streams=%v epochs=%v reads=%d", streams, epochs, reads)
	}
}
