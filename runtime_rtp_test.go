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
	"github.com/thesyncim/goav/pipeline"
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
	formats := withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, muxers),
	)

	builder := newTestBuilder(t, formats).
		RTP(receiver,
			WithRTPName("remote-audio"),
			WithRTPDepacketizers(rtpav.NewOpusDepacketizer(stream)),
			WithRTPMaxTimestampGap(av.SamplesDuration(960, 48000)),
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
	if !strings.Contains(specText(planned), "remote-audio -> archive.ogg") ||
		!strings.Contains(specText(planned), "timestamp gap") ||
		!strings.Contains(specMermaid(planned), "preview.ogg\\nstage") {
		t.Fatalf("planned:\n%s\nmermaid:\n%s", specText(planned), specMermaid(planned))
	}

	task, err := builder.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	spec := task.Describe()
	if specText(planned) != specText(spec) || specMermaid(planned) != specMermaid(spec) {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", specText(planned), specText(spec))
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

func TestRuntimeBuilderBufferedRTPRecordRequiresPacketCopyBound(t *testing.T) {
	builder, _, _ := newBufferedRTPRecordCopyFixture(pipeline.BufferPolicy{
		Capacity: 4,
		Drop:     pipeline.DropOldest,
	})

	task, err := builder.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(context.Background()); !errors.Is(err, pipeline.ErrBufferedMessageUnsafe) {
		t.Fatalf("err = %v, want ErrBufferedMessageUnsafe", err)
	}
}

func TestRuntimeBuilderBufferedRTPRecordCopiesPacketsToOutputs(t *testing.T) {
	builder, receiver, muxers := newBufferedRTPRecordCopyFixture(pipeline.BufferPolicy{
		Capacity:        4,
		Drop:            pipeline.DropOldest,
		CopyPacketBytes: 8,
	})
	planned, err := builder.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(specText(planned), "live-audio -> archive.ogg") ||
		!strings.Contains(specText(planned), "live-audio -> preview.ogg") {
		t.Fatalf("planned:\n%s", specText(planned))
	}

	task, err := builder.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if specText(planned) != specText(task.Describe()) || specMermaid(planned) != specMermaid(task.Describe()) {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", specText(planned), specText(task.Describe()))
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(muxers.muxers) != 2 {
		t.Fatalf("muxers = %d, want 2", len(muxers.muxers))
	}
	for i := range muxers.muxers {
		muxer := muxers.muxers[i]
		if !muxer.opened || muxer.streamCount != 1 || muxer.writes != 1 || muxer.lastStream != "audio" {
			t.Fatalf("muxer[%d] opened=%v streams=%d writes=%d last=%s", i, muxer.opened, muxer.streamCount, muxer.writes, muxer.lastStream)
		}
		if !bytes.Equal(muxer.writtenPayloads, []byte{1}) {
			t.Fatalf("muxer[%d] payloads = %+v, want copied Opus payload", i, muxer.writtenPayloads)
		}
	}
	gotEvents := drainTaskEvents(task)
	if countEvents(gotEvents, av.EventEndOfStream) != 1 {
		t.Fatalf("events = %+v, want EOS", gotEvents)
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

func TestRuntimeBuilderRTPDecodeSink(t *testing.T) {
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
		events: make(chan av.Event),
	}
	decoder := &decodeTestDecoder{}
	decoderFactory := &decodeTestDecoderFactory{decoder: decoder}
	codecs := withTestCodecs(testCodecDecoder(codec.Descriptor{
		ID:   av.CodecOpus,
		Type: av.MediaAudio,
	}, decoderFactory))
	sink := &runtimeTestSink{name: "frames"}

	builder := newTestBuilder(t, codecs).
		RTP(receiver,
			WithRTPName("live-audio"),
			WithRTPDepacketizers(rtpav.NewOpusDepacketizer(stream)),
		).
		Decode(testSelectAudio()).
		Sink(sink)
	planned, err := builder.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if len(planned.Nodes) != 4 || len(planned.Edges) != 3 {
		t.Fatalf("nodes=%d edges=%d", len(planned.Nodes), len(planned.Edges))
	}
	if !strings.Contains(specText(planned), "live-audio -> select-audio") ||
		!strings.Contains(specText(planned), "select-audio -> decode-audio") ||
		!strings.Contains(specText(planned), "decode-audio -> frames") {
		t.Fatalf("planned:\n%s", specText(planned))
	}

	task, err := builder.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	spec := task.Describe()
	if specText(planned) != specText(spec) || specMermaid(planned) != specMermaid(spec) {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", specText(planned), specText(spec))
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 || decoder.flushes != 1 || sink.frames != 1 || sink.lastFrame.StreamID != "audio" {
		t.Fatalf("decodes=%d flushes=%d frames=%d last=%+v", decoder.decodes, decoder.flushes, sink.frames, sink.lastFrame)
	}
	if decoderFactory.config.Stream.ID != "audio" || !decoderFactory.config.Realtime || !decoderFactory.config.LowLatency {
		t.Fatalf("decode config: %+v", decoderFactory.config)
	}
	gotEvents := drainTaskEvents(task)
	if countEvents(gotEvents, av.EventEndOfStream) != 2 || countEventsForStream(gotEvents, av.EventEndOfStream, "audio") != 2 {
		t.Fatalf("events = %+v", gotEvents)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !receiver.closed || !decoder.closed || !sink.closed {
		t.Fatalf("closed receiver=%v decoder=%v sink=%v", receiver.closed, decoder.closed, sink.closed)
	}
}

func TestRuntimeBuilderRTPDecodeUsesRTPDecodeBounds(t *testing.T) {
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

	builder := newTestBuilder(t, codecs).
		RTP(receiver,
			WithRTPName("video-rtp"),
			WithRTPDepacketizers(rtpav.NewVP8Depacketizer(stream, rtpav.WithMaxVideoFrameSize(4096))),
			WithRTPDecodeBounds(requested),
		).
		Decode(testSelectVideo()).
		Sink(sink)
	planned, err := builder.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(specText(planned), "decode bounds") {
		t.Fatalf("planned:\n%s", specText(planned))
	}

	task, err := builder.Build(ctx)
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

func TestRTPDecodeBoundsForStreamUsesMatchingInput(t *testing.T) {
	audio := av.Stream{ID: "audio", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}}
	video := av.Stream{ID: "video", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo}}
	bounds := rtpDecodeBoundsForStream(video, []rtpBuild{
		{
			streams:      []av.Stream{audio},
			decodeBounds: codec.DecodeBounds{MaxPayloadBytes: 1024},
		},
		{
			streams:      []av.Stream{video},
			decodeBounds: codec.DecodeBounds{MaxFramesPerInput: 2, MaxPayloadBytes: 4096},
		},
	})
	if bounds.MaxFramesPerInput != 2 || bounds.MaxPayloadBytes != 4096 {
		t.Fatalf("bounds = %+v", bounds)
	}
}

func TestRuntimeBuilderRTPDecodeRejectsDifferentCodecSwitch(t *testing.T) {
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

	task, err := newTestBuilder(t, codecs).
		RTP(receiver,
			WithRTPName("video-rtp"),
			WithRTPDepacketizers(
				rtpav.NewVP8Depacketizer(initial, rtpav.WithMaxVideoFrameSize(16)),
				rtpav.NewH264Depacketizer(initial, rtpav.WithMaxVideoFrameSize(16)),
			),
			WithRTPBufferLimits(RTPBufferLimits{MaxPackets: 1, MaxEvents: 2}),
		).
		Decode(testSelectVideo()).
		Sink(sink).
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

func TestRuntimeBuilderRTPDecodeFilterSink(t *testing.T) {
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
		events: make(chan av.Event),
	}
	decoder := &decodeTestDecoder{}
	codecs := withTestCodecs(testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: decoder}))
	filter := &runtimeTestStage{name: "meter"}
	sink := &runtimeTestSink{name: "frames"}

	builder := newTestBuilder(t, codecs).
		RTP(receiver,
			WithRTPName("live-audio"),
			WithRTPDepacketizers(rtpav.NewOpusDepacketizer(stream)),
		).
		Decode(testSelectAudio()).
		Filter(testSelectAudio(), filter).
		Sink(sink)
	planned, err := builder.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if len(planned.Nodes) != 5 || len(planned.Edges) != 4 {
		t.Fatalf("nodes=%d edges=%d", len(planned.Nodes), len(planned.Edges))
	}
	if !strings.Contains(specText(planned), "decode-audio -> meter") ||
		!strings.Contains(specText(planned), "meter -> frames") {
		t.Fatalf("planned:\n%s", specText(planned))
	}

	task, err := builder.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if specText(planned) != specText(task.Describe()) || specMermaid(planned) != specMermaid(task.Describe()) {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", specText(planned), specText(task.Describe()))
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 || decoder.flushes != 1 || filter.count != 1 || sink.frames != 1 {
		t.Fatalf("decodes=%d flushes=%d filter=%d frames=%d", decoder.decodes, decoder.flushes, filter.count, sink.frames)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !receiver.closed || !filter.closed || !sink.closed {
		t.Fatalf("closed receiver=%v filter=%v sink=%v", receiver.closed, filter.closed, sink.closed)
	}
}

func newBufferedRTPRecordCopyFixture(policy pipeline.BufferPolicy) (builderAPI, *runtimeRTPReceiver, *remuxTestMuxerFactory) {
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
		events: make(chan av.Event),
	}
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	builder := New(formats, WithBufferPolicy(policy)).(*runtime).New().
		RTP(receiver,
			WithRTPName("live-audio"),
			WithRTPDepacketizers(rtpav.NewOpusDepacketizer(stream)),
		).
		Output(Output{Name: "archive.ogg"}).
		Output(Output{Name: "preview.ogg"})
	return builder, receiver, muxers
}

func TestRuntimeBuilderMultiRTPDecodeSelectsOneStream(t *testing.T) {
	ctx := context.Background()
	audio := av.Stream{
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
	video := av.Stream{
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
	audioReceiver := &runtimeRTPReceiver{
		streams: []av.Stream{audio},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  audio.Codec,
			MIMEType:    rtpav.MIMEOpus,
			ClockRate:   48000,
			Channels:    2,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
			Payload: []byte{1, 2, 3},
		}},
		events: make(chan av.Event),
	}
	videoReceiver := &runtimeRTPReceiver{
		streams: []av.Stream{video},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 96,
			Parameters:  video.Codec,
			MIMEType:    rtpav.MIMEVP8,
			ClockRate:   90000,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 96, Marker: true, Timestamp: 90},
			Payload: []byte{0x10, 0x00, 0xaa},
		}},
		events: make(chan av.Event),
	}
	decoder := &decodeTestDecoder{}
	codecs := withTestCodecs(testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: decoder}))
	sink := &runtimeTestSink{name: "frames"}

	builder := newTestBuilder(t, codecs).
		RTP(audioReceiver,
			WithRTPName("audio-rtp"),
			WithRTPDepacketizers(rtpav.NewOpusDepacketizer(audio)),
		).
		RTP(videoReceiver,
			WithRTPName("video-rtp"),
			WithRTPDepacketizers(rtpav.NewVP8Depacketizer(video)),
		).
		Decode(testSelectAudio()).
		Sink(sink)
	planned, err := builder.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if len(planned.Nodes) != 5 || len(planned.Edges) != 4 {
		t.Fatalf("nodes=%d edges=%d", len(planned.Nodes), len(planned.Edges))
	}
	if !strings.Contains(specText(planned), "audio-rtp -> select-audio") ||
		!strings.Contains(specText(planned), "video-rtp -> select-audio") {
		t.Fatalf("planned:\n%s", specText(planned))
	}

	task, err := builder.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 || decoder.flushes != 1 || sink.frames != 1 {
		t.Fatalf("decodes=%d flushes=%d frames=%d", decoder.decodes, decoder.flushes, sink.frames)
	}
	gotEvents := drainTaskEvents(task)
	if countEvents(gotEvents, av.EventEndOfStream) != 3 ||
		countEventsForStream(gotEvents, av.EventEndOfStream, "audio") != 2 ||
		countEventsForStream(gotEvents, av.EventEndOfStream, "video") != 1 {
		t.Fatalf("events = %+v", gotEvents)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !audioReceiver.closed || !videoReceiver.closed {
		t.Fatalf("closed audio=%v video=%v", audioReceiver.closed, videoReceiver.closed)
	}
}

func TestRuntimeBuilderMultiRTPRecordFanout(t *testing.T) {
	ctx := context.Background()
	audio := av.Stream{
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
	video := av.Stream{
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
	audioReceiver := &runtimeRTPReceiver{
		streams: []av.Stream{audio},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  audio.Codec,
			MIMEType:    rtpav.MIMEOpus,
			ClockRate:   48000,
			Channels:    2,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
			Payload: []byte{1, 2, 3},
		}},
		events: make(chan av.Event),
	}
	videoReceiver := &runtimeRTPReceiver{
		streams: []av.Stream{video},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 96,
			Parameters:  video.Codec,
			MIMEType:    rtpav.MIMEVP8,
			ClockRate:   90000,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 96, Marker: true, Timestamp: 90},
			Payload: []byte{0x10, 0x00, 0xaa},
		}},
		events: make(chan av.Event),
	}
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, muxers),
	)

	builder := newTestBuilder(t, formats).
		RTP(audioReceiver,
			WithRTPName("audio-rtp"),
			WithRTPDepacketizers(rtpav.NewOpusDepacketizer(audio)),
		).
		RTP(videoReceiver,
			WithRTPName("video-rtp"),
			WithRTPDepacketizers(rtpav.NewVP8Depacketizer(video)),
		).
		Output(Output{Name: "archive.ogg"}).
		Output(Output{Name: "preview.ogg"})
	planned, err := builder.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if len(planned.Nodes) != 4 || len(planned.Edges) != 4 {
		t.Fatalf("nodes=%d edges=%d", len(planned.Nodes), len(planned.Edges))
	}
	if !strings.Contains(specText(planned), "audio-rtp -> archive.ogg") ||
		!strings.Contains(specText(planned), "video-rtp -> preview.ogg") {
		t.Fatalf("planned:\n%s", specText(planned))
	}

	task, err := builder.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	spec := task.Describe()
	if specText(planned) != specText(spec) || specMermaid(planned) != specMermaid(spec) {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", specText(planned), specText(spec))
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(muxers.muxers) != 2 {
		t.Fatalf("muxers = %d, want 2", len(muxers.muxers))
	}
	for i := range muxers.muxers {
		muxer := muxers.muxers[i]
		if !muxer.opened || muxer.streamCount != 2 || muxer.writes != 2 {
			t.Fatalf("muxer[%d] opened=%v streams=%d writes=%d", i, muxer.opened, muxer.streamCount, muxer.writes)
		}
		if !streamIDsEqual(muxer.openedStreams, []av.StreamID{"audio", "video"}) {
			t.Fatalf("muxer[%d] opened streams = %+v", i, muxer.openedStreams)
		}
		if !streamIDsEqual(muxer.writtenStreams, []av.StreamID{"audio", "video"}) {
			t.Fatalf("muxer[%d] written streams = %+v", i, muxer.writtenStreams)
		}
	}

	gotEvents := drainTaskEvents(task)
	if countEvents(gotEvents, av.EventEndOfStream) != 2 {
		t.Fatalf("events = %+v, want two EOS events", gotEvents)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !audioReceiver.closed || !videoReceiver.closed {
		t.Fatalf("closed audio=%v video=%v", audioReceiver.closed, videoReceiver.closed)
	}
	for i := range muxers.muxers {
		if !muxers.muxers[i].closed {
			t.Fatalf("muxer[%d] not closed", i)
		}
	}
}

func TestRuntimeBuilderMultiRTPRecordDefaultNames(t *testing.T) {
	spec, err := newTestBuilder(t).
		RTP(&runtimeRTPReceiver{}).
		RTP(&runtimeRTPReceiver{}).
		Output(Output{Name: "archive.ogg"}).
		Describe()
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Nodes) != 3 || len(spec.Edges) != 2 {
		t.Fatalf("nodes=%d edges=%d", len(spec.Nodes), len(spec.Edges))
	}
	if !strings.Contains(specText(spec), "rtp -> archive.ogg") ||
		!strings.Contains(specText(spec), "rtp-1 -> archive.ogg") {
		t.Fatalf("spec:\n%s", specText(spec))
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

	task, err := newTestBuilder(t, WithFormatAdapter(ivfadapter.Register)).
		RTP(receiver, WithRTPDepacketizers(rtpav.NewVP8Depacketizer(stream))).
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

	task, err := newTestBuilder(t, WithFormatAdapter(ivfadapter.Register)).
		RTP(receiver, WithRTPDepacketizers(rtpav.NewAV1Depacketizer(stream))).
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

func TestRuntimeBuilderRTPH264RecordAnnexB(t *testing.T) {
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

	task, err := newTestBuilder(t, WithFormatAdapter(annexbadapter.Register)).
		RTP(receiver, WithRTPDepacketizers(rtpav.NewH264Depacketizer(stream))).
		Output(Output{Name: "recording.h264", Writer: &recording}).
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

func TestRuntimeBuilderRTPRecordRequiresReceiver(t *testing.T) {
	_, err := newTestBuilder(t).
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

func countEvents(events []av.Event, eventType av.EventType) int {
	count := 0
	for i := range events {
		if events[i].Type == eventType {
			count++
		}
	}
	return count
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
