package goav

import (
	"context"
	"io"
	"testing"

	"github.com/pion/rtp"
	gopusadapter "github.com/thesyncim/goav/adapters/gopus"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/rtpav"
)

func TestComponentFileRemuxFanout(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &remuxTestDemuxer{streams: streams}
	if err := demuxer.Open(ctx, format.Input{Name: "input.ogg"}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}

	sourcePacket := av.Packet{}
	source, err := format.NewDemuxSource(format.DemuxSourceConfig{
		Name:    "demux",
		Detail:  "component demux",
		Demuxer: demuxer,
		Result:  format.ReadResult{Packet: &sourcePacket, Events: make([]av.Event, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}

	archiveMuxer := &remuxTestMuxer{}
	if err := archiveMuxer.Open(ctx, format.Output{Name: "archive.ogg"}, streams, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	archive, err := format.NewMuxStage(format.MuxStageConfig{
		Name:   "archive",
		Detail: "component mux",
		Muxer:  archiveMuxer,
		Result: format.WriteResult{Events: make([]av.Event, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}

	previewMuxer := &remuxTestMuxer{}
	if err := previewMuxer.Open(ctx, format.Output{Name: "preview.ogg"}, streams, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	preview, err := format.NewMuxStage(format.MuxStageConfig{
		Name:   "preview",
		Detail: "component mux",
		Muxer:  previewMuxer,
		Result: format.WriteResult{Events: make([]av.Event, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}

	graph, err := pipeline.NewGraph(pipeline.GraphConfig{Name: "component-remux"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddStage(archive, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddStage(preview, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(pipeline.Route{
		From:   "demux",
		To:     []string{"archive", "preview"},
		Policy: pipeline.RouteAll,
	}); err != nil {
		t.Fatal(err)
	}

	spec := graph.Spec()
	if len(spec.Nodes) != 3 || len(spec.Edges) != 2 ||
		spec.Nodes[0].Detail != "component demux" ||
		spec.Nodes[1].Detail != "component mux" ||
		spec.Nodes[2].Detail != "component mux" {
		t.Fatalf("spec = %+v", spec)
	}

	if err := graph.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if archiveMuxer.writes != 1 || previewMuxer.writes != 1 ||
		!streamIDsEqual(archiveMuxer.writtenStreams, []av.StreamID{"audio"}) ||
		!streamIDsEqual(previewMuxer.writtenStreams, []av.StreamID{"audio"}) ||
		archiveMuxer.writtenPayloads[0] != 1 ||
		previewMuxer.writtenPayloads[0] != 1 {
		t.Fatalf("archive writes=%d streams=%+v payload=%+v preview writes=%d streams=%+v payload=%+v",
			archiveMuxer.writes, archiveMuxer.writtenStreams, archiveMuxer.writtenPayloads,
			previewMuxer.writes, previewMuxer.writtenStreams, previewMuxer.writtenPayloads)
	}

	stats := graph.Stats()
	if stats.Packets != 1 || stats.Events != 2 || stats.Delivered != 6 {
		t.Fatalf("stats = %+v", stats)
	}

	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
	if !demuxer.closed || !archiveMuxer.closed || !previewMuxer.closed {
		t.Fatalf("closed demux=%v archive=%v preview=%v", demuxer.closed, archiveMuxer.closed, previewMuxer.closed)
	}
}

func TestComponentCustomStageForwardsEvents(t *testing.T) {
	source := &componentEventSource{
		name: "events",
		events: []av.Event{
			{Type: av.EventStreamAdded, StreamID: "audio"},
			{Type: av.EventPacketLoss, StreamID: "audio"},
			{Type: av.EventEndOfStream, StreamID: "audio"},
		},
	}
	stage := &componentForwardStage{name: "forward"}
	sink := &componentEventSink{name: "sink"}

	graph, err := pipeline.NewGraph(pipeline.GraphConfig{Name: "component-custom-stage"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddStage(stage, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(pipeline.Route{From: "events", To: []string{"forward"}, Policy: pipeline.RouteAll}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(pipeline.Route{From: "forward", To: []string{"sink"}, Policy: pipeline.RouteAll}); err != nil {
		t.Fatal(err)
	}

	if err := graph.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if source.starts != 1 || stage.events != 3 || sink.events != 3 || sink.lastEvent != av.EventEndOfStream {
		t.Fatalf("starts=%d stage events=%d sink events=%d last=%s", source.starts, stage.events, sink.events, sink.lastEvent)
	}

	stats := graph.Stats()
	if stats.Events != 6 || stats.Delivered != 6 || stats.LastEvent.Type != av.EventEndOfStream {
		t.Fatalf("stats = %+v", stats)
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
	if !source.closed || !stage.closed || !sink.closed {
		t.Fatalf("closed source=%v stage=%v sink=%v", source.closed, stage.closed, sink.closed)
	}
}

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
			MIMEType:    rtpav.MIMEOpus,
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

func TestComponentCodecStageFlushesOnEOS(t *testing.T) {
	source := &componentEventSource{
		name:   "eos",
		events: []av.Event{{Type: av.EventEndOfStream, StreamID: "audio"}},
	}
	decoder := &componentFlushDecoder{}
	stage, err := codec.NewDecoderStage(codec.DecoderStageConfig{
		Name:    "decode",
		Detail:  "component decoder",
		Decoder: decoder,
		Result:  codec.DecodeResult{Frames: make([]av.Frame, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	sink := &componentMediaSink{name: "frames"}

	graph, err := pipeline.NewGraph(pipeline.GraphConfig{Name: "component-codec-eos"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddStage(stage, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(pipeline.Route{From: "eos", To: []string{"decode"}, Policy: pipeline.RouteAll}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(pipeline.Route{From: "decode", To: []string{"frames"}, Policy: pipeline.RouteAll}); err != nil {
		t.Fatal(err)
	}

	if err := graph.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if decoder.events != 1 || decoder.flushes != 1 {
		t.Fatalf("decoder events=%d flushes=%d", decoder.events, decoder.flushes)
	}
	if sink.frames != 1 || sink.events != 1 || sink.orderLen != 2 ||
		sink.order != [2]pipeline.MessageKind{pipeline.MessageFrame, pipeline.MessageEvent} {
		t.Fatalf("sink frames=%d events=%d order_len=%d order=%+v", sink.frames, sink.events, sink.orderLen, sink.order)
	}
	stats := graph.Stats()
	if stats.Frames != 1 || stats.Events != 2 || stats.Delivered != 3 {
		t.Fatalf("stats = %+v", stats)
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
	if !source.closed || !decoder.closed || !sink.closed {
		t.Fatalf("closed source=%v decoder=%v sink=%v", source.closed, decoder.closed, sink.closed)
	}
}

func TestComponentMuxStageEmitsWriteEvents(t *testing.T) {
	source := &componentPacketSource{
		name:   "packets",
		packet: av.Packet{StreamID: "audio", Payload: av.Buffer{Bytes: []byte{9}}},
	}
	muxer := &componentEventMuxer{}
	if err := muxer.Open(context.Background(), format.Output{Name: "events.ogg"}, []av.Stream{audioOpusTestStream()}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	stage, err := format.NewMuxStage(format.MuxStageConfig{
		Name:   "mux",
		Detail: "component mux",
		Muxer:  muxer,
		Result: format.WriteResult{Events: make([]av.Event, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	sink := &componentMediaSink{name: "events"}

	graph, err := pipeline.NewGraph(pipeline.GraphConfig{Name: "component-mux-events"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddStage(stage, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(pipeline.Route{From: "packets", To: []string{"mux"}, Policy: pipeline.RouteAll}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(pipeline.Route{From: "mux", To: []string{"events"}, Policy: pipeline.RouteAll}); err != nil {
		t.Fatal(err)
	}

	if err := graph.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if muxer.writes != 1 || muxer.lastPacket == nil || muxer.lastPacket.StreamID != "audio" {
		t.Fatalf("writes=%d packet=%+v", muxer.writes, muxer.lastPacket)
	}
	if sink.events != 1 || sink.lastEvent != av.EventStats || sink.frames != 0 || sink.packets != 0 {
		t.Fatalf("sink packets=%d frames=%d events=%d last=%s", sink.packets, sink.frames, sink.events, sink.lastEvent)
	}
	stats := graph.Stats()
	if stats.Packets != 1 || stats.Events != 1 || stats.Delivered != 2 {
		t.Fatalf("stats = %+v", stats)
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
	if !source.closed || !muxer.closed || !sink.closed {
		t.Fatalf("closed source=%v muxer=%v sink=%v", source.closed, muxer.closed, sink.closed)
	}
}

type componentEventSource struct {
	name   string
	events []av.Event
	msg    pipeline.Message
	starts int
	closed bool
}

func (s *componentEventSource) Name() string {
	return s.name
}

func (s *componentEventSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	s.starts++
	for i := range s.events {
		s.msg.Kind = pipeline.MessageEvent
		s.msg.Packet = nil
		s.msg.Frame = nil
		s.msg.Event = &s.events[i]
		if err := emitter.Emit(ctx, &s.msg); err != nil {
			return err
		}
	}
	return nil
}

func (s *componentEventSource) Close() error {
	s.closed = true
	return nil
}

type componentForwardStage struct {
	name   string
	event  av.Event
	msg    pipeline.Message
	events int
	closed bool
}

func (s *componentForwardStage) Name() string {
	return s.name
}

func (s *componentForwardStage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	if msg == nil {
		return nil
	}
	if msg.Kind != pipeline.MessageEvent || msg.Event == nil {
		return emitter.Emit(ctx, msg)
	}
	s.events++
	s.event = *msg.Event
	s.msg.Kind = pipeline.MessageEvent
	s.msg.Packet = nil
	s.msg.Frame = nil
	s.msg.Event = &s.event
	return emitter.Emit(ctx, &s.msg)
}

func (s *componentForwardStage) Close() error {
	s.closed = true
	return nil
}

type componentEventSink struct {
	name      string
	events    int
	lastEvent av.EventType
	closed    bool
}

func (s *componentEventSink) Name() string {
	return s.name
}

func (s *componentEventSink) Handle(_ context.Context, msg *pipeline.Message) error {
	if msg != nil && msg.Kind == pipeline.MessageEvent && msg.Event != nil {
		s.events++
		s.lastEvent = msg.Event.Type
	}
	return nil
}

func (s *componentEventSink) Close() error {
	s.closed = true
	return nil
}

type componentPacketSource struct {
	name   string
	packet av.Packet
	msg    pipeline.Message
	closed bool
}

func (s *componentPacketSource) Name() string {
	return s.name
}

func (s *componentPacketSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	s.msg.Kind = pipeline.MessagePacket
	s.msg.Packet = &s.packet
	s.msg.Frame = nil
	s.msg.Event = nil
	return emitter.Emit(ctx, &s.msg)
}

func (s *componentPacketSource) Close() error {
	s.closed = true
	return nil
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

type componentFlushDecoder struct {
	events  int
	flushes int
	closed  bool
}

func (d *componentFlushDecoder) Descriptor() codec.Descriptor {
	return codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}
}

func (d *componentFlushDecoder) Open(context.Context, codec.DecodeConfig) error {
	return nil
}

func (d *componentFlushDecoder) DecodeInto(context.Context, *av.Packet, *codec.DecodeResult) error {
	return nil
}

func (d *componentFlushDecoder) FlushInto(_ context.Context, out *codec.DecodeResult) error {
	d.flushes++
	if len(out.Frames) == cap(out.Frames) {
		return codec.ErrResultFull
	}
	index := len(out.Frames)
	out.Frames = out.Frames[:index+1]
	frame := &out.Frames[index]
	frame.Reset()
	frame.StreamID = "audio"
	frame.Type = av.MediaAudio
	return nil
}

func (d *componentFlushDecoder) HandleEvent(context.Context, *av.Event) error {
	d.events++
	return nil
}

func (d *componentFlushDecoder) Close() error {
	d.closed = true
	return nil
}

type componentEventMuxer struct {
	writes     int
	lastPacket *av.Packet
	closed     bool
}

func (m *componentEventMuxer) Format() av.FormatID {
	return av.FormatOgg
}

func (m *componentEventMuxer) Open(context.Context, format.Output, []av.Stream, format.OpenOptions) error {
	return nil
}

func (m *componentEventMuxer) Write(_ context.Context, packet *av.Packet, out *format.WriteResult) error {
	m.writes++
	m.lastPacket = packet
	return out.AddEvent(av.Event{Type: av.EventStats, StreamID: packet.StreamID})
}

func (m *componentEventMuxer) Close() error {
	m.closed = true
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
