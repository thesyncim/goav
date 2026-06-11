package integration

import (
	"context"
	"io"
	"reflect"
	"testing"

	"github.com/pion/rtp"
	gopusadapter "github.com/thesyncim/goav/adapters/gopus"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/rtpav"
)

func TestComponentRTPOpusDecodeGraph(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "audio",
		Type:     av.MediaAudio,
		Epoch:    1,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:           av.CodecOpus,
			Type:         av.MediaAudio,
			ClockRate:    48000,
			SampleRate:   48000,
			Channels:     1,
			SampleFormat: av.SampleFormatS16,
		},
	}
	reader := &componentRTPReader{
		streams: []av.Stream{stream},
		payloads: rtpav.NewStaticPayloadMap(1, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    av.MIMEOpus,
			ClockRate:   48000,
			Channels:    1,
		}}),
		packets: []*rtp.Packet{
			{
				Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
				Payload: componentCELTPacket(),
			},
		},
	}
	source, err := rtpav.NewSource(rtpav.SourceConfig{
		Name:          "rtp",
		Detail:        "component RTP Opus source",
		Receiver:      reader,
		Depacketizers: []rtpav.Depacketizer{rtpav.NewOpusDepacketizer(stream)},
		Streams:       []av.Stream{stream},
		MaxPackets:    1,
		MaxEvents:     1,
	})
	if err != nil {
		t.Fatal(err)
	}

	decoder, err := gopusadapter.NewDecoderFactory().NewDecoder(ctx, codec.DecodeConfig{Stream: stream})
	if err != nil {
		t.Fatal(err)
	}
	frames := make([]av.Frame, 1)
	frames[0].Planes = make([]av.Plane, 1)
	frames[0].Planes[0].Buffer.Bytes = make([]byte, 5760*2)
	result := codec.DecodeResult{Frames: frames}
	result.Reset()
	decode, err := codec.NewDecoderStage(codec.DecoderStageConfig{
		Name:        "decode",
		Detail:      "component Opus decoder",
		InputStream: stream,
		Decoder:     decoder,
		Result:      result,
	})
	if err != nil {
		t.Fatal(err)
	}
	sink := &componentMediaSink{name: "frames"}

	graph, err := pipeline.NewGraph(pipeline.GraphConfig{Name: "component-rtp-opus-decode"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddStage(decode, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(pipeline.Route{From: "rtp", To: []string{"decode"}, Policy: pipeline.RouteAll}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(pipeline.Route{From: "decode", To: []string{"frames"}, Policy: pipeline.RouteAll}); err != nil {
		t.Fatal(err)
	}

	spec := graph.Spec()
	if len(spec.Nodes) != 3 || len(spec.Edges) != 2 ||
		spec.Nodes[0].Detail != "component RTP Opus source" ||
		spec.Nodes[1].Detail != "component Opus decoder" {
		t.Fatalf("spec = %+v", spec)
	}

	if err := graph.Run(ctx); err != nil {
		t.Fatal(err)
	}
	assertComponentSpecStable(t, graph, spec)
	if reader.reads != 1 || sink.frames != 1 || sink.events != 1 || sink.lastEvent != av.EventEndOfStream {
		t.Fatalf("reads=%d frames=%d events=%d last=%s", reader.reads, sink.frames, sink.events, sink.lastEvent)
	}
	if sink.lastFrameStreamID != "audio" || sink.lastFrameEpoch != 1 ||
		sink.lastFramePTS.Value != 960 || sink.lastAudioSamples != 960 ||
		sink.lastAudioSampleRate != 48000 || sink.lastPlaneBytes == 0 ||
		sink.lastPlaneOwnership != av.BufferOwned {
		t.Fatalf("decoded frame stream=%s epoch=%d pts=%+v samples=%d rate=%d bytes=%d ownership=%s",
			sink.lastFrameStreamID, sink.lastFrameEpoch, sink.lastFramePTS,
			sink.lastAudioSamples, sink.lastAudioSampleRate, sink.lastPlaneBytes,
			sink.lastPlaneOwnership)
	}
	stats := graph.Stats()
	if stats.Packets != 1 || stats.Frames != 1 || stats.Events != 2 || stats.Delivered != 4 {
		t.Fatalf("stats = %+v", stats)
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
	if !reader.closed || !sink.closed {
		t.Fatalf("closed reader=%v sink=%v", reader.closed, sink.closed)
	}
}

type componentSpecGraph interface {
	Spec() pipeline.Spec
}

func assertComponentSpecStable(t *testing.T, graph componentSpecGraph, before pipeline.Spec) {
	t.Helper()
	if after := graph.Spec(); !reflect.DeepEqual(before, after) {
		t.Fatalf("component graph spec changed after run:\nbefore=%+v\nafter=%+v", before, after)
	}
}

type componentRTPReader struct {
	streams  []av.Stream
	payloads rtpav.PayloadMap
	packets  []*rtp.Packet
	reads    int
	closed   bool
}

func (r *componentRTPReader) Streams(context.Context) ([]av.Stream, error) {
	return append([]av.Stream(nil), r.streams...), nil
}

func (r *componentRTPReader) PayloadMap() rtpav.PayloadMap {
	return r.payloads
}

func (r *componentRTPReader) ReadRTP(context.Context) (*rtp.Packet, error) {
	if r.reads >= len(r.packets) {
		return nil, io.EOF
	}
	packet := r.packets[r.reads]
	r.reads++
	return packet, nil
}

func (r *componentRTPReader) Events() <-chan av.Event {
	return nil
}

func (r *componentRTPReader) Close() error {
	r.closed = true
	return nil
}

type componentMediaSink struct {
	name                string
	packets             int
	frames              int
	events              int
	lastEvent           av.EventType
	lastPacketStreamID  av.StreamID
	lastPacketEpoch     av.Epoch
	lastPacketPTS       av.Timestamp
	lastPacketBytes     int
	lastPacketOwnership av.BufferOwnership
	lastFrameStreamID   av.StreamID
	lastFrameEpoch      av.Epoch
	lastFramePTS        av.Timestamp
	lastAudioSamples    int
	lastAudioSampleRate int
	lastPlaneBytes      int
	lastPlaneOwnership  av.BufferOwnership
	order               [2]pipeline.MessageKind
	orderLen            int
	closed              bool
}

func (s *componentMediaSink) Name() string {
	return s.name
}

func (s *componentMediaSink) Handle(_ context.Context, msg *pipeline.Message) error {
	if msg == nil {
		return nil
	}
	switch msg.Kind {
	case pipeline.MessagePacket:
		s.packets++
		if msg.Packet != nil {
			s.lastPacketStreamID = msg.Packet.StreamID
			s.lastPacketEpoch = msg.Packet.CodecEpoch
			s.lastPacketPTS = msg.Packet.PTS
			s.lastPacketBytes = len(msg.Packet.Payload.Bytes)
			s.lastPacketOwnership = msg.Packet.Payload.Ownership
		}
	case pipeline.MessageFrame:
		s.frames++
		if msg.Frame != nil {
			s.lastFrameStreamID = msg.Frame.StreamID
			s.lastFrameEpoch = msg.Frame.CodecEpoch
			s.lastFramePTS = msg.Frame.PTS
			if msg.Frame.Audio != nil {
				s.lastAudioSamples = msg.Frame.Audio.Samples
				s.lastAudioSampleRate = msg.Frame.Audio.SampleRate
			}
			if len(msg.Frame.Planes) > 0 {
				s.lastPlaneBytes = len(msg.Frame.Planes[0].Buffer.Bytes)
				s.lastPlaneOwnership = msg.Frame.Planes[0].Buffer.Ownership
			}
		}
	case pipeline.MessageEvent:
		s.events++
		if msg.Event != nil {
			s.lastEvent = msg.Event.Type
		}
	}
	if s.orderLen < len(s.order) {
		s.order[s.orderLen] = msg.Kind
		s.orderLen++
	}
	return nil
}

func (s *componentMediaSink) Close() error {
	s.closed = true
	return nil
}

func componentCELTPacket() []byte {
	data := make([]byte, 50)
	data[0] = 0xf8
	for i := 1; i < len(data); i++ {
		data[i] = byte(i * 7)
	}
	return data
}
