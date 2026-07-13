package playoutav_test

import (
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/playoutav"
	"github.com/thesyncim/goav/provider"
	"github.com/thesyncim/goav/shape"
)

var _ provider.Source = (*playoutav.Input)(nil)

func TestScheduledPlayoutProviderRunsThroughGoavInput(t *testing.T) {
	stream := audioStream("mic")
	first := s16Frame("mic", 0, 1, 2)
	second := s16Frame("mic", time.Millisecond, 3, 4)
	input := playoutav.New(
		[]av.Stream{stream},
		[]playoutav.Message{
			playoutav.Frame(0, first),
			playoutav.Frame(0, second),
		},
		playoutav.WithName("playlist"),
	)
	first.Planes[0].Buffer.Bytes[0] = 99

	out := goavtest.NewCollector()
	if err := goav.From(goav.Input(input)).Audio().To(out.Sink()).Run(context.Background()); err != nil {
		t.Fatalf("run playout input: %v", err)
	}
	if got, want := out.S16(), [][]int16{{1, 2}, {3, 4}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("collected S16 = %v, want %v", got, want)
	}
	spec, err := goav.From(goav.Input(input)).Audio().To(out.Sink()).Describe()
	if err != nil {
		t.Fatalf("describe playout input: %v", err)
	}
	if !containsNode(spec, "playlist") {
		t.Fatalf("Describe() did not include playout source node: %+v", spec.Nodes)
	}
}

func TestScheduledPlayoutPacketProviderKeepsTransportOutOfCore(t *testing.T) {
	stream := packetStream("audio")
	packet := &av.Packet{
		StreamID: "audio",
		Type:     av.MediaAudio,
		Payload:  av.Buffer{Bytes: []byte{1, 2, 3}, Ownership: av.BufferBorrowed},
		PTS:      av.Timestamp{Value: 10, Base: stream.TimeBase},
	}
	input := playoutav.New([]av.Stream{stream}, []playoutav.Message{playoutav.Packet(0, packet)})
	packet.Payload.Bytes[0] = 9

	out := goavtest.NewCollector()
	if err := goav.From(goav.Input(input)).Audio().Copy().To(out.Sink()).Run(context.Background()); err != nil {
		t.Fatalf("run packet playout input: %v", err)
	}
	packets := out.Packets()
	if len(packets) != 1 {
		t.Fatalf("collected packets = %d, want 1", len(packets))
	}
	if got, want := packets[0].Payload.Bytes, []byte{1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("packet payload = %v, want %v", got, want)
	}
	spec := input.SourceShape()
	if spec.Domain != shape.DomainPacket ||
		spec.MediaKind != av.MediaAudio ||
		spec.Codec != av.CodecOpus ||
		spec.Format != playoutav.FormatPlayout ||
		!spec.Realtime {
		t.Fatalf("SourceShape() = %+v", spec)
	}
}

func TestScheduledPlayoutRejectsInvalidSchedule(t *testing.T) {
	input := playoutav.New(
		[]av.Stream{audioStream("mic")},
		[]playoutav.Message{
			playoutav.Frame(10*time.Millisecond, s16Frame("mic", 10*time.Millisecond, 1)),
			playoutav.Frame(time.Millisecond, s16Frame("mic", time.Millisecond, 2)),
		},
	)
	if _, _, err := input.OpenSource(context.Background()); !errors.Is(err, playoutav.ErrOutOfOrder) {
		t.Fatalf("OpenSource() err = %v, want %v", err, playoutav.ErrOutOfOrder)
	}
}

func audioStream(id av.StreamID) av.Stream {
	return av.Stream{
		ID:       id,
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:           av.CodecPCM,
			Type:         av.MediaAudio,
			SampleRate:   48000,
			Channels:     1,
			SampleFormat: av.SampleFormatS16,
		},
	}
}

func packetStream(id av.StreamID) av.Stream {
	stream := audioStream(id)
	stream.Codec.ID = av.CodecOpus
	return stream
}

func s16Frame(stream av.StreamID, pts time.Duration, samples ...int16) *av.Frame {
	payload := make([]byte, len(samples)*2)
	for i := range samples {
		binary.LittleEndian.PutUint16(payload[i*2:], uint16(samples[i]))
	}
	timestamp, _ := av.TimestampFromStdDuration(pts, av.TimeBase{Num: 1, Den: 48000})
	return &av.Frame{
		StreamID: stream,
		Type:     av.MediaAudio,
		PTS:      timestamp,
		Audio: &av.AudioFrame{
			SampleRate:   48000,
			Channels:     1,
			SampleFormat: av.SampleFormatS16,
			Samples:      len(samples),
		},
		Planes: []av.Plane{{
			Buffer: av.Buffer{Bytes: payload, Ownership: av.BufferBorrowed},
			Stride: len(payload),
		}},
	}
}

func containsNode(spec pipeline.Spec, name string) bool {
	for i := range spec.Nodes {
		if spec.Nodes[i].Name == name {
			return true
		}
	}
	return false
}
