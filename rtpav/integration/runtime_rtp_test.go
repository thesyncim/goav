package integration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav"
	annexbadapter "github.com/thesyncim/goav/adapters/annexb"
	ivfadapter "github.com/thesyncim/goav/adapters/ivf"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/rtpav"
)

func TestRecipeRTPDecodeUsesProviderDecodeBounds(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "video",
		Type:     av.MediaVideo,
		TimeBase: av.RTPTimeBase(90000),
		Codec: av.CodecParameters{
			ID:          av.CodecVP8,
			Type:        av.MediaVideo,
			ClockRate:   90000,
			Width:       640,
			Height:      360,
			PixelFormat: av.PixelFormatI420,
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
			MIMEType:    av.MIMEVP8,
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

	job := goav.From(goav.Input(rtpav.Receive(receiver,
		rtpav.WithName("video-rtp"),
		rtpav.WithDepacketizers(rtpav.NewVP8Depacketizer(stream, rtpav.WithMaxVideoFrameSize(4096))),
		rtpav.WithDecodeBounds(requested),
	))).
		UseRuntime(goav.MustNew(codecs)).
		Video().
		To(goav.Sink(sink))
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

func TestRecipeRTPDecodeRejectsDifferentCodecSwitch(t *testing.T) {
	ctx := context.Background()
	initial := av.Stream{
		ID:       "video",
		Type:     av.MediaVideo,
		Epoch:    1,
		TimeBase: av.RTPTimeBase(90000),
		Codec: av.CodecParameters{
			ID:          av.CodecVP8,
			Type:        av.MediaVideo,
			ClockRate:   90000,
			Width:       640,
			Height:      360,
			PixelFormat: av.PixelFormatI420,
		},
	}
	updated := initial
	updated.Epoch = 2
	updated.Codec = av.CodecParameters{
		ID:          av.CodecH264,
		Type:        av.MediaVideo,
		ClockRate:   90000,
		Width:       640,
		Height:      360,
		PixelFormat: av.PixelFormatI420,
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

	task, err := goav.From(goav.Input(rtpav.Receive(receiver,
		rtpav.WithName("video-rtp"),
		rtpav.WithDepacketizers(
			rtpav.NewVP8Depacketizer(initial, rtpav.WithMaxVideoFrameSize(16)),
			rtpav.NewH264Depacketizer(initial, rtpav.WithMaxVideoFrameSize(16)),
		),
		rtpav.WithBufferLimits(rtpav.BufferLimits{MaxPackets: 1, MaxEvents: 2}),
	))).
		UseRuntime(goav.MustNew(codecs)).
		Video().
		To(goav.Sink(sink)).
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
			MIMEType:    av.MIMEAV1,
			ClockRate:   90000,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 96, Marker: true, Timestamp: 90},
			Payload: []byte{0x18, 0x30, 0xaa, 0xbb},
		}},
		events: make(chan av.Event),
	}
	var recording bytes.Buffer

	task, err := goav.From(goav.Input(rtpav.Receive(receiver, rtpav.WithDepacketizers(rtpav.NewAV1Depacketizer(stream))))).
		UseRuntime(goav.MustNew(goav.WithFormatAdapter(ivfadapter.Register))).
		Copy().
		To(goav.File("recording.ivf", &recording)).
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
			MIMEType:    av.MIMEH264,
			ClockRate:   90000,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 96, Marker: true, Timestamp: 90},
			Payload: []byte{0x65, 0xaa, 0xbb},
		}},
		events: make(chan av.Event),
	}
	var recording bytes.Buffer

	task, err := goav.From(goav.Input(rtpav.Receive(receiver, rtpav.WithDepacketizers(rtpav.NewH264Depacketizer(stream))))).
		UseRuntime(goav.MustNew(goav.WithFormatAdapter(annexbadapter.Register))).
		Copy().
		To(goav.File("recording.h264", &recording)).
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
