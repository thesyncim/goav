package webrtcav

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/rtpav"
)

type fakeTrackRTPReader struct {
	packets []*rtp.Packet
	err     error
	reads   int
}

type fakeRTCPWriter struct {
	packets []rtcp.Packet
}

type feedbackDepacketizer struct {
	packet rtcp.Packet
}

type testEmitter func(context.Context, *pipeline.Message) error

func (r *fakeTrackRTPReader) ReadRTP() (*rtp.Packet, interceptor.Attributes, error) {
	if r.reads < len(r.packets) {
		packet := r.packets[r.reads]
		r.reads++
		return packet, nil, nil
	}
	if r.err != nil {
		return nil, nil, r.err
	}
	return nil, nil, io.EOF
}

func (w *fakeRTCPWriter) WriteRTCP(_ context.Context, packets []rtcp.Packet) error {
	w.packets = append(w.packets, packets...)
	return nil
}

func (d feedbackDepacketizer) Codec() av.CodecID {
	return av.CodecOpus
}

func (d feedbackDepacketizer) PushInto(_ context.Context, _ *rtp.Packet, _ rtpav.PayloadCodec, out *rtpav.DepacketizeResult) error {
	if len(out.Feedback) == cap(out.Feedback) {
		return rtpav.ErrResultFull
	}
	out.Feedback = append(out.Feedback, d.packet)
	return nil
}

func (d feedbackDepacketizer) FlushInto(context.Context, *rtpav.DepacketizeResult) error {
	return nil
}

func (d feedbackDepacketizer) HandleEvent(context.Context, *av.Event) error {
	return nil
}

func (e testEmitter) Emit(ctx context.Context, msg *pipeline.Message) error {
	return e(ctx, msg)
}

func TestNewTrackReaderMapsStreamAndPayload(t *testing.T) {
	remote := RemoteTrack{
		Codec: webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:    webrtc.MimeTypeOpus,
				ClockRate:   48000,
				Channels:    2,
				SDPFmtpLine: "minptime=10;useinbandfec=1",
			},
			PayloadType: 111,
		},
		Stream: av.Stream{ID: "audio", Epoch: 7},
		Metadata: av.Metadata{
			"rid": "h",
		},
	}

	reader := newTrackReader(remote, &fakeTrackRTPReader{})

	streams, err := reader.Streams(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(streams) != 1 {
		t.Fatalf("streams = %d, want 1", len(streams))
	}
	stream := streams[0]
	if stream.Codec.ID != av.CodecOpus || stream.Type != av.MediaAudio {
		t.Fatalf("stream = %+v", stream)
	}
	if stream.Codec.ClockRate != 48000 || stream.Codec.Channels != 2 {
		t.Fatalf("codec = %+v", stream.Codec)
	}
	if stream.TimeBase.Num != 1 || stream.TimeBase.Den != 48000 {
		t.Fatalf("timebase = %+v", stream.TimeBase)
	}
	if stream.Metadata["rid"] != "h" {
		t.Fatalf("metadata = %+v", stream.Metadata)
	}

	payload, ok := reader.PayloadMap().Lookup(111)
	if !ok {
		t.Fatal("payload 111 not found")
	}
	if payload.Parameters.ID != av.CodecOpus || payload.MIMEType != webrtc.MimeTypeOpus {
		t.Fatalf("payload = %+v", payload)
	}
	if reader.PayloadMap().Epoch() != 7 {
		t.Fatalf("epoch = %d, want 7", reader.PayloadMap().Epoch())
	}
}

func TestTrackReaderReadRTPAndClose(t *testing.T) {
	packet := &rtp.Packet{Header: rtp.Header{SSRC: 1, SequenceNumber: 10}}
	reader := newTrackReader(RemoteTrack{Stream: av.Stream{ID: "video"}}, &fakeTrackRTPReader{
		packets: []*rtp.Packet{packet},
	})

	got, err := reader.ReadRTP(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != packet {
		t.Fatalf("packet = %p, want %p", got, packet)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ReadRTP(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}

func TestTrackReaderRoutesRTCPFeedback(t *testing.T) {
	writer := &fakeRTCPWriter{}
	reader := newTrackReader(RemoteTrack{
		Stream:   av.Stream{ID: "video"},
		Feedback: writer,
	}, &fakeTrackRTPReader{})
	packet := &rtcp.PictureLossIndication{MediaSSRC: 42}

	if err := reader.WriteRTCP(context.Background(), []rtcp.Packet{packet}); err != nil {
		t.Fatal(err)
	}
	if len(writer.packets) != 1 || writer.packets[0] != packet {
		t.Fatalf("packets = %+v", writer.packets)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reader.WriteRTCP(context.Background(), []rtcp.Packet{packet}); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}

func TestTrackReaderRoutesSourceFeedback(t *testing.T) {
	writer := &fakeRTCPWriter{}
	stream := av.Stream{
		ID:    "audio",
		Epoch: 2,
		Codec: av.CodecParameters{
			ID:        av.CodecOpus,
			Type:      av.MediaAudio,
			ClockRate: 48000,
		},
	}
	reader := newTrackReader(RemoteTrack{
		Stream: stream,
		Codec: webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:  webrtc.MimeTypeOpus,
				ClockRate: 48000,
			},
			PayloadType: 111,
		},
		Feedback: writer,
	}, &fakeTrackRTPReader{
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 1},
			Payload: []byte{1},
		}},
	})
	pli := &rtcp.PictureLossIndication{MediaSSRC: 42}
	source, err := rtpav.NewSource(rtpav.SourceConfig{
		Receiver:      reader,
		Depacketizers: []rtpav.Depacketizer{feedbackDepacketizer{packet: pli}},
		MaxFeedback:   1,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := source.Start(context.Background(), testEmitter(func(context.Context, *pipeline.Message) error {
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if len(writer.packets) != 1 || writer.packets[0] != pli {
		t.Fatalf("feedback = %+v", writer.packets)
	}
}

func TestTrackReaderEOFEmitsEndOfStream(t *testing.T) {
	reader := newTrackReader(RemoteTrack{Stream: av.Stream{ID: "audio", Epoch: 4}}, &fakeTrackRTPReader{})

	_, err := reader.ReadRTP(context.Background())
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF", err)
	}

	select {
	case event := <-reader.Events():
		if event.Type != av.EventEndOfStream || event.StreamID != "audio" || event.Epoch != 4 {
			t.Fatalf("event = %+v", event)
		}
	default:
		t.Fatal("missing EOS event")
	}
}

func TestTrackReaderContextCancellation(t *testing.T) {
	reader := newTrackReader(RemoteTrack{Stream: av.Stream{ID: "audio"}}, &fakeTrackRTPReader{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := reader.ReadRTP(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestCodecIDFromMIME(t *testing.T) {
	cases := []struct {
		mime  string
		codec av.CodecID
		media av.MediaType
	}{
		{webrtc.MimeTypeOpus, av.CodecOpus, av.MediaAudio},
		{webrtc.MimeTypeVP8, av.CodecVP8, av.MediaVideo},
		{webrtc.MimeTypeVP9, av.CodecVP9, av.MediaVideo},
		{webrtc.MimeTypeH264, av.CodecH264, av.MediaVideo},
		{webrtc.MimeTypeAV1, av.CodecAV1, av.MediaVideo},
	}
	for _, tc := range cases {
		codec, media := codecIDFromMIME(tc.mime)
		if codec != tc.codec || media != tc.media {
			t.Fatalf("%s -> %s/%s, want %s/%s", tc.mime, codec, media, tc.codec, tc.media)
		}
	}
}
