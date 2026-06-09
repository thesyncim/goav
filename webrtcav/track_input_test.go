package webrtcav

import (
	"context"
	"errors"
	"testing"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/rtpav"
	"github.com/thesyncim/goav/shape"
)

func trackInputTestReader(packets ...*rtp.Packet) TrackReader {
	return newTrackReader(RemoteTrack{
		Codec: webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:  webrtc.MimeTypeOpus,
				ClockRate: 48000,
				Channels:  2,
			},
			PayloadType: 111,
		},
		Stream: av.Stream{ID: "audio"},
	}, &fakeTrackRTPReader{packets: packets})
}

func TestTrackInputDerivesCodecIntent(t *testing.T) {
	ctx := context.Background()
	input := trackInput(trackInputTestReader(&rtp.Packet{
		Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
		Payload: []byte{1, 2, 3},
	}))

	if input.Name() != "audio" {
		t.Fatalf("name = %q", input.Name())
	}
	if input.Detail() != "rtp receive, codec=opus" {
		t.Fatalf("detail = %q", input.Detail())
	}
	spec := input.SourceShape()
	want := shape.Spec{
		Domain:     shape.DomainPacket,
		MediaKind:  av.MediaAudio,
		Codec:      av.CodecOpus,
		SampleRate: 48000,
		Channels:   2,
		Realtime:   true,
	}
	if spec != want {
		t.Fatalf("shape = %+v, want %+v", spec, want)
	}

	source, streams, err := input.OpenSource(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(streams) != 1 || streams[0].ID != "audio" || streams[0].Codec.ID != av.CodecOpus {
		t.Fatalf("streams = %+v", streams)
	}
	if source.Name() != "audio" {
		t.Fatalf("source name = %q", source.Name())
	}

	var packets []av.Packet
	var events []av.Event
	emitter := testEmitter(func(_ context.Context, msg *pipeline.Message) error {
		switch msg.Kind {
		case pipeline.MessagePacket:
			packets = append(packets, *msg.Packet)
		case pipeline.MessageEvent:
			events = append(events, *msg.Event)
		}
		return nil
	})
	if err := source.Start(ctx, emitter); err != nil {
		t.Fatal(err)
	}
	if len(packets) != 1 || packets[0].StreamID != "audio" || string(packets[0].Payload.Bytes) != "\x01\x02\x03" {
		t.Fatalf("packets = %+v", packets)
	}
	if len(events) != 1 || events[0].Type != av.EventEndOfStream {
		t.Fatalf("events = %+v", events)
	}
}

func TestTrackInputOptionsLayerOverDerived(t *testing.T) {
	input := trackInput(trackInputTestReader(), rtpav.WithName("override"))
	if input.Name() != "override" {
		t.Fatalf("name = %q", input.Name())
	}
	if input.Detail() != "rtp receive, codec=opus" {
		t.Fatalf("detail = %q", input.Detail())
	}
}

func TestTrackNilTrackFailsAtOpen(t *testing.T) {
	input := Track(nil)
	if _, _, err := input.OpenSource(context.Background()); !errors.Is(err, ErrNilTrack) {
		t.Fatalf("err = %v, want ErrNilTrack", err)
	}
}

func TestTrackInputUnknownStreamFailsAtOpen(t *testing.T) {
	input := trackInput(emptyTrackReader{})
	if _, _, err := input.OpenSource(context.Background()); !errors.Is(err, ErrUnknownStream) {
		t.Fatalf("err = %v, want ErrUnknownStream", err)
	}
}

type emptyTrackReader struct{}

func (emptyTrackReader) Streams(context.Context) ([]av.Stream, error) {
	return nil, nil
}

func (emptyTrackReader) PayloadMap() rtpav.PayloadMap {
	return nil
}

func (emptyTrackReader) UpdateCodec(context.Context, TrackCodecUpdate) error {
	return nil
}

func (emptyTrackReader) UpdateTrack(context.Context, RemoteTrack) error {
	return nil
}

func (emptyTrackReader) ReadRTP(context.Context) (*rtp.Packet, error) {
	return nil, ErrUnknownStream
}

func (emptyTrackReader) Events() <-chan av.Event {
	return nil
}

func (emptyTrackReader) Close() error {
	return nil
}
