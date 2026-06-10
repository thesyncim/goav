package goav

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/pion/rtp"
	annexbadapter "github.com/thesyncim/goav/adapters/annexb"
	ivfadapter "github.com/thesyncim/goav/adapters/ivf"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
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

type runtimeRTPSwitchReceiver struct {
	initial  av.Stream
	updated  av.Stream
	payload  rtpav.PayloadMap
	packets  []*rtp.Packet
	events   chan av.Event
	index    int
	switched bool
	closed   bool
}

func newRuntimeRTPSwitchReceiver(initial av.Stream, updated av.Stream, packets []*rtp.Packet) *runtimeRTPSwitchReceiver {
	return &runtimeRTPSwitchReceiver{
		initial: initial,
		updated: updated,
		payload: rtpav.NewStaticPayloadMap(1, []rtpav.PayloadCodec{{
			PayloadType: 96,
			Parameters:  initial.Codec,
			MIMEType:    rtpav.MIMEVP8,
			ClockRate:   90000,
		}}),
		packets: packets,
		events:  make(chan av.Event, 1),
	}
}

func (r *runtimeRTPSwitchReceiver) Streams(context.Context) ([]av.Stream, error) {
	return []av.Stream{r.initial}, nil
}

func (r *runtimeRTPSwitchReceiver) PayloadMap() rtpav.PayloadMap {
	return r.payload
}

func (r *runtimeRTPSwitchReceiver) ReadRTP(context.Context) (*rtp.Packet, error) {
	if r.index >= len(r.packets) {
		return nil, io.EOF
	}
	packet := r.packets[r.index]
	r.index++
	if r.index == 1 && !r.switched {
		r.payload = rtpav.NewStaticPayloadMap(2, []rtpav.PayloadCodec{{
			PayloadType: 97,
			Parameters:  r.updated.Codec,
			MIMEType:    rtpav.MIMEH264,
			ClockRate:   90000,
		}})
		r.events <- av.Event{
			Type:     av.EventCodecChanged,
			StreamID: r.initial.ID,
			Epoch:    r.updated.Epoch,
			Stream:   &r.updated,
			Codec:    &r.updated.Codec,
		}
		r.switched = true
	}
	return packet, nil
}

func (r *runtimeRTPSwitchReceiver) Events() <-chan av.Event {
	return r.events
}

func (r *runtimeRTPSwitchReceiver) Close() error {
	r.closed = true
	return nil
}

func TestRecipeRTPDecodeUsesProviderDecodeBounds(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "video",
		Type:     av.MediaVideo,
		TimeBase: av.RTPTimeBase(90000),
		Codec: av.CodecParameters{
			ID:        av.CodecVP8,
			Type:      av.MediaVideo,
			ClockRate: 90000,
			Width:     640,
			Height:    360,
		},
	}
	requested := codec.DecodeBounds{
		MaxFramesPerInput:   2,
		MaxEventsPerInput:   3,
		MaxRequestsPerInput: 4,
		MaxPayloadBytes:     4096,
		MaxRetainedBytes:    8192,
		MaxWidth:            1280,
		MaxHeight:           720,
	}
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 96,
			Parameters:  stream.Codec,
			MIMEType:    rtpav.MIMEVP8,
			ClockRate:   90000,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 96, Marker: true, Timestamp: 3000},
			Payload: []byte{0x10, 0x00, 0xaa},
		}},
		events: make(chan av.Event),
	}
	decoder := &decodeTestDecoder{}
	decoderFactory := &decodeTestDecoderFactory{decoder: decoder}
	codecs := withTestCodecs(testCodecDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, decoderFactory))
	sink := &runtimeTestSink{name: "frames"}

	job := From(Input(rtpav.Receive(receiver,
		rtpav.WithName("video-rtp"),
		rtpav.WithDepacketizers(rtpav.NewVP8Depacketizer(stream, rtpav.WithMaxVideoFrameSize(4096))),
		rtpav.WithDecodeBounds(requested),
	))).
		UseRuntime(New(codecs)).
		Video().
		To(Sink(sink))
	planned, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(specText(planned), "decode bounds") {
		t.Fatalf("planned:\n%s", specText(planned))
	}

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if decoderFactory.config.Bounds.MaxFramesPerInput != 2 ||
		decoderFactory.config.Bounds.MaxEventsPerInput != 3 ||
		decoderFactory.config.Bounds.MaxRequestsPerInput != 4 ||
		decoderFactory.config.Bounds.MaxPayloadBytes != 4096 ||
		decoderFactory.config.Bounds.MaxRetainedBytes != 8192 ||
		decoderFactory.config.Bounds.MaxWidth != 1280 ||
		decoderFactory.config.Bounds.MaxHeight != 720 {
		t.Fatalf("bounds = %+v", decoderFactory.config.Bounds)
	}
	if sink.frames != 1 || sink.lastFrame.StreamID != stream.ID {
		t.Fatalf("frames=%d last=%+v", sink.frames, sink.lastFrame)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !receiver.closed || !decoder.closed || !sink.closed {
		t.Fatalf("closed receiver=%v decoder=%v sink=%v", receiver.closed, decoder.closed, sink.closed)
	}
}

// testBoundsProvider is a minimal decode-bounds capability fixture: per-stream
// bounds keyed by stream id, like rtpav.Input.DecodeBounds after OpenSource.
type testBoundsProvider map[av.StreamID]codec.DecodeBounds

func (p testBoundsProvider) DecodeBounds(id av.StreamID) codec.DecodeBounds {
	return p[id]
}

func TestProviderDecodeBoundsForStreamUsesMatchingInput(t *testing.T) {
	video := av.Stream{ID: "video", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo}}
	bounds := providerDecodeBoundsForStream(video, []decodeBoundsProvider{
		testBoundsProvider{"audio": {MaxPayloadBytes: 1024}},
		testBoundsProvider{"video": {MaxFramesPerInput: 2, MaxPayloadBytes: 4096}},
	})
	if bounds.MaxFramesPerInput != 2 || bounds.MaxPayloadBytes != 4096 {
		t.Fatalf("bounds = %+v", bounds)
	}
}

func TestRecipeRTPDecodeRejectsDifferentCodecSwitch(t *testing.T) {
	ctx := context.Background()
	initial := av.Stream{
		ID:       "video",
		Type:     av.MediaVideo,
		Epoch:    1,
		TimeBase: av.RTPTimeBase(90000),
		Codec: av.CodecParameters{
			ID:        av.CodecVP8,
			Type:      av.MediaVideo,
			ClockRate: 90000,
			Width:     640,
			Height:    360,
		},
	}
	updated := initial
	updated.Epoch = 2
	updated.Codec = av.CodecParameters{
		ID:        av.CodecH264,
		Type:      av.MediaVideo,
		ClockRate: 90000,
		Width:     640,
		Height:    360,
	}
	receiver := newRuntimeRTPSwitchReceiver(initial, updated, []*rtp.Packet{
		{
			Header:  rtp.Header{PayloadType: 96, Marker: true, Timestamp: 3000},
			Payload: []byte{0x10, 0x00, 0xaa},
		},
		{
			Header:  rtp.Header{PayloadType: 97, Marker: true, Timestamp: 6000},
			Payload: []byte{0x65, 0xbb},
		},
	})
	decoder := &decodeTestDecoder{}
	codecs := withTestCodecs(testCodecDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, &decodeTestDecoderFactory{decoder: decoder}))
	sink := &runtimeTestSink{name: "frames"}

	task, err := From(Input(rtpav.Receive(receiver,
		rtpav.WithName("video-rtp"),
		rtpav.WithDepacketizers(
			rtpav.NewVP8Depacketizer(initial, rtpav.WithMaxVideoFrameSize(16)),
			rtpav.NewH264Depacketizer(initial, rtpav.WithMaxVideoFrameSize(16)),
		),
		rtpav.WithBufferLimits(rtpav.BufferLimits{MaxPackets: 1, MaxEvents: 2}),
	))).
		UseRuntime(New(codecs)).
		Video().
		To(Sink(sink)).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); !errors.Is(err, codec.ErrUnsupportedCodecSwitch) {
		t.Fatalf("err = %v, want ErrUnsupportedCodecSwitch", err)
	}
	if decoder.decodes != 1 || sink.frames != 1 || sink.lastFrame.StreamID != initial.ID {
		t.Fatalf("decodes=%d frames=%d last=%+v", decoder.decodes, sink.frames, sink.lastFrame)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !receiver.closed || !decoder.closed || !sink.closed {
		t.Fatalf("closed receiver=%v decoder=%v sink=%v", receiver.closed, decoder.closed, sink.closed)
	}
}

func TestRecipeRTPAV1RecordIVF(t *testing.T) {
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

	task, err := From(Input(rtpav.Receive(receiver, rtpav.WithDepacketizers(rtpav.NewAV1Depacketizer(stream))))).
		UseRuntime(New(WithFormatAdapter(ivfadapter.Register))).
		Copy().
		To(File("recording.ivf", &recording)).
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

func TestRecipeRTPH264RecordAnnexB(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "video",
		Type:     av.MediaVideo,
		TimeBase: av.TimeBase{Num: 1, Den: 90000},
		Codec: av.CodecParameters{
			ID:        av.CodecH264,
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
			MIMEType:    rtpav.MIMEH264,
			ClockRate:   90000,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 96, Marker: true, Timestamp: 90},
			Payload: []byte{0x65, 0xaa, 0xbb},
		}},
		events: make(chan av.Event),
	}
	var recording bytes.Buffer

	task, err := From(Input(rtpav.Receive(receiver, rtpav.WithDepacketizers(rtpav.NewH264Depacketizer(stream))))).
		UseRuntime(New(WithFormatAdapter(annexbadapter.Register))).
		Copy().
		To(File("recording.h264", &recording)).
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

	want := []byte{0x00, 0x00, 0x00, 0x01, 0x65, 0xaa, 0xbb}
	if !bytes.Equal(recording.Bytes(), want) {
		t.Fatalf("recording = %v, want %v", recording.Bytes(), want)
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

func streamIDsEqual(got []av.StreamID, want []av.StreamID) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func countEventsForStream(events []av.Event, eventType av.EventType, stream av.StreamID) int {
	count := 0
	for i := range events {
		if events[i].Type == eventType && events[i].StreamID == stream {
			count++
		}
	}
	return count
}
