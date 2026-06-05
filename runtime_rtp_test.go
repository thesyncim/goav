package goav

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/pion/rtp"
	ivfadapter "github.com/thesyncim/goav/adapters/ivf"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/rtpav"
)

type runtimeRTPReceiver struct {
	streams []av.Stream
	payload rtpav.PayloadMap
	packets []*rtp.Packet
	events  chan av.Event
	index   int
	closed  bool
}

func (r *runtimeRTPReceiver) Streams(context.Context) ([]av.Stream, error) {
	streams := make([]av.Stream, len(r.streams))
	copy(streams, r.streams)
	return streams, nil
}

func (r *runtimeRTPReceiver) PayloadMap() rtpav.PayloadMap {
	return r.payload
}

func (r *runtimeRTPReceiver) ReadRTP(context.Context) (*rtp.Packet, error) {
	if r.index >= len(r.packets) {
		return nil, io.EOF
	}
	packet := r.packets[r.index]
	r.index++
	return packet, nil
}

func (r *runtimeRTPReceiver) Events() <-chan av.Event {
	return r.events
}

func (r *runtimeRTPReceiver) Close() error {
	r.closed = true
	return nil
}

func TestRuntimeBuilderRTPRecordFanout(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecOpus,
			Type:       av.MediaAudio,
			ClockRate:  48000,
			SampleRate: 48000,
			Channels:   2,
		},
	}
	events := make(chan av.Event, 1)
	events <- av.Event{Type: av.EventPacketLoss, StreamID: stream.ID}
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    rtpav.MIMEOpus,
			ClockRate:   48000,
			Channels:    2,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
			Payload: []byte{1, 2, 3},
		}},
		events: events,
	}
	muxers := &remuxTestMuxerFactory{}
	formats := format.NewRegistry(
		format.WithProber(format.DefaultProber()),
		format.WithMuxer(av.FormatOgg, muxers),
	)

	builder := New(WithFormatRegistry(formats)).New().
		RTP(receiver,
			WithRTPName("remote-audio"),
			WithRTPDepacketizer(rtpav.NewOpusDepacketizer(stream)),
			WithRTPBufferLimits(RTPBufferLimits{MaxPackets: 2, MaxEvents: 2}),
		).
		Output(Output{Name: "archive.ogg"}).
		Output(Output{Name: "preview.ogg"})
	planned, err := builder.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if len(planned.Nodes) != 3 || len(planned.Edges) != 2 {
		t.Fatalf("nodes=%d edges=%d", len(planned.Nodes), len(planned.Edges))
	}
	if !strings.Contains(planned.String(), "remote-audio:out -> archive.ogg:inout") ||
		!strings.Contains(planned.Mermaid(), "preview.ogg\\nstage") {
		t.Fatalf("planned:\n%s\nmermaid:\n%s", planned.String(), planned.Mermaid())
	}

	task, err := builder.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	spec := task.Describe()
	if planned.String() != spec.String() || planned.Mermaid() != spec.Mermaid() {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", planned.String(), spec.String())
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(muxers.muxers) != 2 {
		t.Fatalf("muxers = %d, want 2", len(muxers.muxers))
	}
	for i := range muxers.muxers {
		muxer := muxers.muxers[i]
		if !muxer.opened || muxer.writes != 1 || muxer.streamCount != 1 || muxer.lastStream != stream.ID {
			t.Fatalf("muxer[%d] opened=%v writes=%d streams=%d last=%s", i, muxer.opened, muxer.writes, muxer.streamCount, muxer.lastStream)
		}
	}

	gotEvents := drainTaskEvents(task)
	if len(gotEvents) != 2 {
		t.Fatalf("events = %+v, want packet loss and EOS", gotEvents)
	}
	if gotEvents[0].Type != av.EventPacketLoss || gotEvents[1].Type != av.EventEndOfStream {
		t.Fatalf("events = %+v", gotEvents)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !receiver.closed {
		t.Fatal("receiver not closed")
	}
	for i := range muxers.muxers {
		if !muxers.muxers[i].closed {
			t.Fatalf("muxer[%d] not closed", i)
		}
	}
}

func TestRuntimeBuilderRTPVP8RecordIVF(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "video",
		Type:     av.MediaVideo,
		TimeBase: av.TimeBase{Num: 1, Den: 90000},
		Codec: av.CodecParameters{
			ID:        av.CodecVP8,
			Type:      av.MediaVideo,
			ClockRate: 90000,
			Width:     640,
			Height:    360,
		},
	}
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 96,
			Parameters:  stream.Codec,
			MIMEType:    rtpav.MIMEVP8,
			ClockRate:   90000,
		}}),
		packets: []*rtp.Packet{
			{Header: rtp.Header{PayloadType: 96, Timestamp: 90}, Payload: []byte{0x10, 0xaa}},
			{Header: rtp.Header{PayloadType: 96, Marker: true, Timestamp: 90}, Payload: []byte{0x00, 0xbb}},
		},
		events: make(chan av.Event),
	}
	var recording bytes.Buffer

	task, err := New(WithFormatAdapter(ivfadapter.Register)).New().
		RTP(receiver, WithRTPDepacketizer(rtpav.NewVP8Depacketizer(stream))).
		Output(Output{Name: "recording.ivf", Writer: &recording}).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer := &ivfadapter.Demuxer{}
	if err := demuxer.Open(ctx, format.Input{Reader: bytes.NewReader(recording.Bytes())}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	read := format.ReadResult{
		Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 16)}},
	}
	if err := demuxer.ReadInto(ctx, &read); err != nil {
		t.Fatal(err)
	}
	if !read.PacketReady || read.Packet.StreamID != "video" || read.Packet.PTS.Value != 90 {
		t.Fatalf("packet = %+v", read.Packet)
	}
	if !bytes.Equal(read.Packet.Payload.Bytes, []byte{0xaa, 0xbb}) {
		t.Fatalf("payload = %v", read.Packet.Payload.Bytes)
	}
	if err := demuxer.ReadInto(ctx, &read); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}
}

func TestRuntimeBuilderRTPAV1RecordIVF(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "video",
		Type:     av.MediaVideo,
		TimeBase: av.TimeBase{Num: 1, Den: 90000},
		Codec: av.CodecParameters{
			ID:        av.CodecAV1,
			Type:      av.MediaVideo,
			ClockRate: 90000,
			Width:     640,
			Height:    360,
		},
	}
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 96,
			Parameters:  stream.Codec,
			MIMEType:    rtpav.MIMEAV1,
			ClockRate:   90000,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 96, Marker: true, Timestamp: 90},
			Payload: []byte{0x18, 0x30, 0xaa, 0xbb},
		}},
		events: make(chan av.Event),
	}
	var recording bytes.Buffer

	task, err := New(WithFormatAdapter(ivfadapter.Register)).New().
		RTP(receiver, WithRTPDepacketizer(rtpav.NewAV1Depacketizer(stream))).
		Output(Output{Name: "recording.ivf", Writer: &recording}).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer := &ivfadapter.Demuxer{}
	if err := demuxer.Open(ctx, format.Input{Reader: bytes.NewReader(recording.Bytes())}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	read := format.ReadResult{
		Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 16)}},
	}
	if err := demuxer.ReadInto(ctx, &read); err != nil {
		t.Fatal(err)
	}
	if !read.PacketReady || read.Packet.StreamID != "video" || read.Packet.PTS.Value != 90 {
		t.Fatalf("packet = %+v", read.Packet)
	}
	if !bytes.Equal(read.Packet.Payload.Bytes, []byte{0x32, 0x02, 0xaa, 0xbb}) {
		t.Fatalf("payload = %v", read.Packet.Payload.Bytes)
	}
	if err := demuxer.ReadInto(ctx, &read); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}
}

func TestRuntimeBuilderRTPRecordRequiresReceiver(t *testing.T) {
	_, err := New().New().
		RTP(nil).
		Output(Output{Name: "archive.ogg"}).
		Build(context.Background())
	if !errors.Is(err, ErrNilSource) {
		t.Fatalf("err = %v, want ErrNilSource", err)
	}
}

func drainTaskEvents(task Task) []av.Event {
	var events []av.Event
	for {
		select {
		case event := <-task.Events():
			events = append(events, event)
		default:
			return events
		}
	}
}
