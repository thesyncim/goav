package rtpav

import (
	"context"
	"fmt"
	"io"
	"slices"
	"testing"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

type configuredTestReader struct {
	fakeReceiver
	stream av.Stream
}

func (r *configuredTestReader) ReadRTP(context.Context) (*rtp.Packet, *av.Event, PayloadMap, error) {
	if r.index == len(r.packets) {
		return nil, nil, r.payloads, io.EOF
	}
	packet := r.packets[r.index]
	var event *av.Event
	if r.index == 2 {
		r.stream.Epoch = 2
		r.payloads = NewStaticPayloadMap(2, []PayloadCodec{{PayloadType: 111, Parameters: r.stream.Codec, ClockRate: 48000}})
		event = &av.Event{Type: av.EventCodecChanged, Stream: &r.stream, Epoch: 2}
	}
	r.index++
	return packet, event, r.payloads, nil
}
func TestConfiguredSourceDrainsOldJitterBeforeChange(t *testing.T) {
	stream := av.Stream{ID: "audio", Epoch: 1, Type: av.MediaAudio, TimeBase: av.RTPTimeBase(48000), Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio, ClockRate: 48000, Channels: 2}}
	r := &configuredTestReader{stream: stream}
	r.payloads = NewStaticPayloadMap(1, []PayloadCodec{{PayloadType: 111, Parameters: stream.Codec, ClockRate: 48000}})
	for _, seq := range []uint16{1, 3, 4} {
		r.packets = append(r.packets, &rtp.Packet{Header: rtp.Header{PayloadType: 111, SequenceNumber: seq, Timestamp: uint32(seq) * 960}, Payload: []byte{1, 2}})
	}
	jitter, _ := NewJitterRing(JitterConfig{Capacity: 8})
	source, err := NewSource(SourceConfig{Receiver: r, Streams: []av.Stream{stream}, Jitter: jitter, Depacketizers: []Depacketizer{NewOpusDepacketizer(stream)}})
	if err != nil {
		t.Fatal(err)
	}
	var order []string
	if err := source.Start(t.Context(), testEmitter(func(_ context.Context, m *pipeline.Message) error {
		if m.Event != nil && m.Event.Type == av.EventCodecChanged {
			order = append(order, fmt.Sprintf("event:%d", m.Event.Epoch))
		}
		if m.Packet != nil {
			order = append(order, fmt.Sprintf("packet:%d:%d", m.Packet.PTS.Value, m.Packet.CodecEpoch))
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	want := []string{"packet:960:1", "packet:2880:1", "event:2", "packet:3840:2"}
	if !slices.Equal(order, want) {
		t.Fatalf("order=%v want %v", order, want)
	}
}
