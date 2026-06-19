package webrtcav

import (
	"bytes"
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

func TestTrackReaderUpdateCodecEmitsCodecChanged(t *testing.T) {
	reader := newTrackReader(RemoteTrack{
		Codec: webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:  webrtc.MimeTypeOpus,
				ClockRate: 48000,
				Channels:  2,
			},
			PayloadType: 111,
		},
		Stream: av.Stream{ID: "audio", Epoch: 1},
	}, &fakeTrackRTPReader{})

	err := reader.UpdateCodec(context.Background(), TrackCodecUpdate{
		Codec: webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:    webrtc.MimeTypeOpus,
				ClockRate:   48000,
				Channels:    2,
				SDPFmtpLine: "minptime=20",
			},
			PayloadType: 112,
		},
		Metadata: av.Metadata{"rid": "f"},
	})
	if err != nil {
		t.Fatal(err)
	}

	streams, err := reader.Streams(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(streams) != 1 {
		t.Fatalf("streams = %d, want 1", len(streams))
	}
	stream := streams[0]
	if stream.ID != "audio" || stream.Epoch != 2 || stream.Codec.ID != av.CodecOpus {
		t.Fatalf("stream = %+v", stream)
	}
	if stream.Codec.Attributes["fmtp"] != "minptime=20" || stream.Metadata["rid"] != "f" {
		t.Fatalf("stream metadata/attrs = %+v %+v", stream.Metadata, stream.Codec.Attributes)
	}

	payload, ok := reader.PayloadMap().Lookup(112)
	if !ok {
		t.Fatal("payload 112 not found")
	}
	if reader.PayloadMap().Epoch() != 2 || payload.MIMEType != webrtc.MimeTypeOpus || payload.FMTP != "minptime=20" {
		t.Fatalf("payload map epoch=%d payload=%+v", reader.PayloadMap().Epoch(), payload)
	}

	select {
	case event := <-reader.Events():
		if event.Type != av.EventCodecChanged || event.StreamID != "audio" || event.Epoch != 2 {
			t.Fatalf("event = %+v", event)
		}
		if event.Stream == nil || event.Stream.Codec.ID != av.CodecOpus || event.Codec == nil || event.Codec.ID != av.CodecOpus {
			t.Fatalf("event stream/codec = %+v %+v", event.Stream, event.Codec)
		}
	default:
		t.Fatal("missing codec changed event")
	}
}

func TestTrackReaderUpdateCodecUsesCustomPayloadEpoch(t *testing.T) {
	reader := newTrackReader(RemoteTrack{
		Codec: webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:  webrtc.MimeTypeOpus,
				ClockRate: 48000,
				Channels:  2,
			},
			PayloadType: 111,
		},
		Stream: av.Stream{ID: "audio", Epoch: 1},
	}, &fakeTrackRTPReader{})
	payloads := rtpav.NewStaticPayloadMap(9, []rtpav.PayloadCodec{{
		PayloadType: 120,
		Parameters: av.CodecParameters{
			ID:        av.CodecOpus,
			Type:      av.MediaAudio,
			ClockRate: 48000,
			Channels:  2,
		},
		MIMEType:  webrtc.MimeTypeOpus,
		ClockRate: 48000,
		Channels:  2,
	}})

	if err := reader.UpdateCodec(context.Background(), TrackCodecUpdate{Payloads: payloads}); err != nil {
		t.Fatal(err)
	}

	streams, err := reader.Streams(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if streams[0].Epoch != 9 || reader.PayloadMap().Epoch() != 9 {
		t.Fatalf("stream epoch=%d payload epoch=%d", streams[0].Epoch, reader.PayloadMap().Epoch())
	}
	select {
	case event := <-reader.Events():
		if event.Epoch != 9 {
			t.Fatalf("event = %+v", event)
		}
	default:
		t.Fatal("missing codec changed event")
	}
}

func TestTrackReaderUpdateCodecFeedsRTPSource(t *testing.T) {
	initial := av.Stream{
		ID:       "audio",
		Epoch:    1,
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:        av.CodecOpus,
			Type:      av.MediaAudio,
			ClockRate: 48000,
			Channels:  2,
		},
	}
	reader := newTrackReader(RemoteTrack{
		Codec: webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:  webrtc.MimeTypeOpus,
				ClockRate: 48000,
				Channels:  2,
			},
			PayloadType: 111,
		},
		Stream: initial,
	}, &fakeTrackRTPReader{
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 112, Timestamp: 960},
			Payload: []byte{9, 8, 7},
		}},
	})
	if err := reader.UpdateCodec(context.Background(), TrackCodecUpdate{
		Codec: webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:  webrtc.MimeTypeOpus,
				ClockRate: 48000,
				Channels:  2,
			},
			PayloadType: 112,
		},
	}); err != nil {
		t.Fatal(err)
	}

	source, err := rtpav.NewSource(rtpav.SourceConfig{
		Receiver:      reader,
		Depacketizers: []rtpav.Depacketizer{rtpav.NewOpusDepacketizer(initial)},
		MaxPackets:    1,
		MaxEvents:     2,
	})
	if err != nil {
		t.Fatal(err)
	}
	var packets []av.Packet
	var events []av.Event
	if err := source.Start(context.Background(), testEmitter(func(_ context.Context, msg *pipeline.Message) error {
		switch msg.Kind {
		case pipeline.MessagePacket:
			packets = append(packets, *msg.Packet)
		case pipeline.MessageEvent:
			events = append(events, *msg.Event)
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}

	if len(events) != 2 || events[0].Type != av.EventCodecChanged || events[1].Type != av.EventEndOfStream {
		t.Fatalf("events = %+v", events)
	}
	if len(packets) != 1 {
		t.Fatalf("packets = %d, want 1", len(packets))
	}
	if packets[0].StreamID != "audio" || packets[0].CodecEpoch != 2 || packets[0].PTS.Value != 960 {
		t.Fatalf("packet = %+v", packets[0])
	}
	if len(packets[0].Payload.Bytes) != 3 || packets[0].Payload.Bytes[0] != 9 {
		t.Fatalf("payload = %v", packets[0].Payload.Bytes)
	}
}

func TestTrackReaderUpdateTrackReplacesReaderAndFeedsRTPSource(t *testing.T) {
	initial := av.Stream{
		ID:       "video",
		Epoch:    3,
		Type:     av.MediaVideo,
		TimeBase: av.TimeBase{Num: 1, Den: 90000},
		Codec: av.CodecParameters{
			ID:        av.CodecH264,
			Type:      av.MediaVideo,
			ClockRate: 90000,
		},
	}
	reader := newTrackReader(RemoteTrack{
		Codec: webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:  webrtc.MimeTypeH264,
				ClockRate: 90000,
			},
			PayloadType: 96,
		},
		Stream: initial,
	}, &fakeTrackRTPReader{
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 96, Timestamp: 1},
			Payload: []byte{0x65, 0x00},
		}},
	})
	replacement := &fakeTrackRTPReader{
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 97, Marker: true, Timestamp: 90},
			Payload: []byte{0x65, 0xaa},
		}},
	}
	if err := reader.updateTrack(context.Background(), RemoteTrack{
		Codec: webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:    webrtc.MimeTypeH264,
				ClockRate:   90000,
				SDPFmtpLine: "profile-level-id=42e01f",
			},
			PayloadType: 97,
		},
		Stream: av.Stream{ID: "video"},
	}, replacement); err != nil {
		t.Fatal(err)
	}

	source, err := rtpav.NewSource(rtpav.SourceConfig{
		Receiver:      reader,
		Depacketizers: []rtpav.Depacketizer{rtpav.NewH264Depacketizer(initial, rtpav.WithMaxVideoFrameSize(16))},
		MaxPackets:    1,
		MaxEvents:     2,
	})
	if err != nil {
		t.Fatal(err)
	}
	var packets []av.Packet
	var events []av.Event
	if err := source.Start(context.Background(), testEmitter(func(_ context.Context, msg *pipeline.Message) error {
		switch msg.Kind {
		case pipeline.MessagePacket:
			packets = append(packets, *msg.Packet)
		case pipeline.MessageEvent:
			events = append(events, *msg.Event)
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}

	if len(events) != 2 || events[0].Type != av.EventCodecChanged || events[1].Type != av.EventEndOfStream {
		t.Fatalf("events = %+v", events)
	}
	if events[0].Epoch != 4 {
		t.Fatalf("codec event = %+v", events[0])
	}
	if events[0].Stream == nil || events[0].Stream.TimeBase != av.RTPTimeBase(90000) {
		t.Fatalf("codec event stream timebase = %+v", events[0].Stream)
	}
	if len(packets) != 1 {
		t.Fatalf("packets = %d, want 1", len(packets))
	}
	if packets[0].StreamID != "video" || packets[0].CodecEpoch != 4 || !packets[0].Keyframe {
		t.Fatalf("packet = %+v", packets[0])
	}
	if packets[0].PTS.Value != 90 || packets[0].PTS.Base != av.RTPTimeBase(90000) {
		t.Fatalf("packet PTS = %+v, want RTP timestamp 90 at 90kHz", packets[0].PTS)
	}
	want := []byte{0x00, 0x00, 0x00, 0x01, 0x65, 0xaa}
	if !bytes.Equal(packets[0].Payload.Bytes, want) {
		t.Fatalf("payload = %v, want %v", packets[0].Payload.Bytes, want)
	}
	if replacement.reads != 1 {
		t.Fatalf("replacement reads = %d, want 1", replacement.reads)
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
