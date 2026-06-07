package rtpav

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

type fakeReceiver struct {
	packets  []*rtp.Packet
	payloads PayloadMap
	events   chan av.Event
	feedback []rtcp.Packet
	err      error
	index    int
	closed   bool
}

type fakeFeedbackWriter struct {
	packets []rtcp.Packet
}

func (w *fakeFeedbackWriter) WriteRTCP(_ context.Context, packets []rtcp.Packet) error {
	w.packets = append(w.packets, packets...)
	return nil
}

type feedbackDepacketizer struct {
	packet rtcp.Packet
}

type eventDepacketizer struct {
	events []av.Event
}

func (d feedbackDepacketizer) Codec() av.CodecID {
	return av.CodecOpus
}

func (d feedbackDepacketizer) PushInto(_ context.Context, _ *rtp.Packet, _ PayloadCodec, out *DepacketizeResult) error {
	if len(out.Feedback) == cap(out.Feedback) {
		return ErrResultFull
	}
	out.Feedback = append(out.Feedback, d.packet)
	return nil
}

func (d feedbackDepacketizer) FlushInto(context.Context, *DepacketizeResult) error {
	return nil
}

func (d feedbackDepacketizer) HandleEvent(context.Context, *av.Event) error {
	return nil
}

func (d *eventDepacketizer) Codec() av.CodecID {
	return av.CodecOpus
}

func (d *eventDepacketizer) PushInto(context.Context, *rtp.Packet, PayloadCodec, *DepacketizeResult) error {
	return nil
}

func (d *eventDepacketizer) FlushInto(context.Context, *DepacketizeResult) error {
	return nil
}

func (d *eventDepacketizer) HandleEvent(_ context.Context, event *av.Event) error {
	d.events = append(d.events, *event)
	return nil
}

func (r *fakeReceiver) Streams(context.Context) ([]av.Stream, error) {
	return []av.Stream{{ID: "audio"}}, nil
}

func (r *fakeReceiver) PayloadMap() PayloadMap {
	return r.payloads
}

func (r *fakeReceiver) ReadRTP(context.Context) (*rtp.Packet, error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.index >= len(r.packets) {
		return nil, io.EOF
	}
	packet := r.packets[r.index]
	r.index++
	return packet, nil
}

func (r *fakeReceiver) Events() <-chan av.Event {
	return r.events
}

func (r *fakeReceiver) WriteRTCP(_ context.Context, packets []rtcp.Packet) error {
	r.feedback = append(r.feedback, packets...)
	return nil
}

func (r *fakeReceiver) Close() error {
	r.closed = true
	return nil
}

func (r *fakeReceiver) Reset() {
	r.index = 0
	r.closed = false
	r.feedback = r.feedback[:0]
}

type packetSink struct {
	name    string
	packets []av.Packet
	events  []av.Event
}

func (s *packetSink) Name() string {
	return s.name
}

func (s *packetSink) Handle(_ context.Context, msg *pipeline.Message) error {
	switch msg.Kind {
	case pipeline.MessagePacket:
		s.packets = append(s.packets, *msg.Packet)
	case pipeline.MessageEvent:
		s.events = append(s.events, *msg.Event)
	}
	return nil
}

func (s *packetSink) Close() error {
	return nil
}

type testEmitter func(context.Context, *pipeline.Message) error

func (e testEmitter) Emit(ctx context.Context, msg *pipeline.Message) error {
	return e(ctx, msg)
}

type countEmitter struct {
	packets int
	events  int
}

func (e *countEmitter) Emit(_ context.Context, msg *pipeline.Message) error {
	switch msg.Kind {
	case pipeline.MessagePacket:
		e.packets++
	case pipeline.MessageEvent:
		e.events++
	}
	return nil
}

func (e *countEmitter) Reset() {
	e.packets = 0
	e.events = 0
}

func TestSourceDepacketizesRTPIntoPipelinePackets(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{ID: "audio", Epoch: 2, Codec: av.CodecParameters{ID: av.CodecOpus, ClockRate: 48000}}
	receiver := &fakeReceiver{
		payloads: NewStaticPayloadMap(2, []PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    MIMEOpus,
			ClockRate:   48000,
		}}),
		packets: []*rtp.Packet{
			{Header: rtp.Header{PayloadType: 111, Timestamp: 960}, Payload: []byte{1, 2, 3}},
		},
		events: make(chan av.Event),
	}
	source, err := NewSource(SourceConfig{
		Receiver:      receiver,
		Depacketizers: []Depacketizer{NewOpusDepacketizer(stream)},
		MaxPackets:    2,
	})
	if err != nil {
		t.Fatal(err)
	}
	sink := &packetSink{name: "sink"}
	graph, err := pipeline.NewGraph(pipeline.GraphConfig{Name: "rtp"})
	if err != nil {
		t.Fatal(err)
	}
	sourceRef, err := graph.AddSource(source, pipeline.BufferPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	sinkRef, err := graph.AddSink(sink, pipeline.BufferPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(pipeline.Route{
		From:   sourceRef.String(),
		To:     []string{sinkRef.String()},
		Policy: pipeline.RouteAll,
	}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Run(ctx); err != nil {
		t.Fatal(err)
	}

	if len(sink.packets) != 1 {
		t.Fatalf("packets = %d, want 1", len(sink.packets))
	}
	packet := sink.packets[0]
	if packet.StreamID != "audio" || packet.CodecEpoch != 2 {
		t.Fatalf("packet = %+v", packet)
	}
	if packet.Payload.Ownership != av.BufferBorrowed || packet.PTS.Value != 960 {
		t.Fatalf("packet = %+v", packet)
	}
	if len(sink.events) != 1 || sink.events[0].Type != av.EventEndOfStream {
		t.Fatalf("events = %+v", sink.events)
	}
}

func TestSourceCodecChangedReplacementUpdatesEOSStream(t *testing.T) {
	initial := av.Stream{
		ID:    "video-main",
		Type:  av.MediaVideo,
		Epoch: 1,
		Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo, ClockRate: 90000},
	}
	updated := initial
	updated.ID = "video-replaced"
	updated.Epoch = 2
	events := make(chan av.Event, 1)
	events <- av.Event{
		Type:     av.EventCodecChanged,
		StreamID: updated.ID,
		Epoch:    updated.Epoch,
		Stream:   &updated,
		Codec:    &updated.Codec,
	}
	source, err := NewSource(SourceConfig{
		Receiver: &fakeReceiver{events: events},
		Streams:  []av.Stream{initial},
	})
	if err != nil {
		t.Fatal(err)
	}

	var got []av.Event
	if err := source.Start(context.Background(), testEmitter(func(_ context.Context, msg *pipeline.Message) error {
		if msg.Kind == pipeline.MessageEvent && msg.Event != nil {
			got = append(got, *msg.Event)
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Type != av.EventCodecChanged || got[1].Type != av.EventEndOfStream {
		t.Fatalf("events = %+v", got)
	}
	if got[1].StreamID != updated.ID || got[1].Epoch != updated.Epoch {
		t.Fatalf("eos = %+v", got[1])
	}
}

func TestSourceCodecChangedUpdatesDestinedStreamInMultiStreamSource(t *testing.T) {
	audio := av.Stream{
		ID:    "audio",
		Type:  av.MediaAudio,
		Epoch: 1,
		Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio, ClockRate: 48000},
	}
	initial := av.Stream{
		ID:    "video-main",
		Type:  av.MediaVideo,
		Epoch: 1,
		Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo, ClockRate: 90000},
	}
	updated := initial
	updated.ID = "video-replaced"
	updated.Epoch = 2
	events := make(chan av.Event, 1)
	events <- av.Event{
		Type:     av.EventCodecChanged,
		StreamID: initial.ID,
		Epoch:    updated.Epoch,
		Stream:   &updated,
		Codec:    &updated.Codec,
	}
	receiver := &fakeReceiver{
		payloads: NewStaticPayloadMap(1, []PayloadCodec{{
			PayloadType: 96,
			Parameters:  initial.Codec,
			MIMEType:    MIMEVP8,
			ClockRate:   90000,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 97, Marker: true, Timestamp: 9000},
			Payload: []byte{0x10, 0x00, 0xcc},
		}},
		events: events,
	}
	source, err := NewSource(SourceConfig{
		Receiver:      receiver,
		Streams:       []av.Stream{audio, initial},
		Depacketizers: []Depacketizer{NewVP8Depacketizer(initial, WithMaxVideoFrameSize(16))},
		MaxPackets:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	receiver.payloads = NewStaticPayloadMap(2, []PayloadCodec{{
		PayloadType: 97,
		Parameters:  updated.Codec,
		MIMEType:    MIMEVP8,
		ClockRate:   90000,
	}})

	var packets []av.Packet
	var eventsOut []av.Event
	if err := source.Start(context.Background(), testEmitter(func(_ context.Context, msg *pipeline.Message) error {
		switch msg.Kind {
		case pipeline.MessagePacket:
			packets = append(packets, *msg.Packet)
		case pipeline.MessageEvent:
			eventsOut = append(eventsOut, *msg.Event)
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if len(eventsOut) != 2 || eventsOut[0].Type != av.EventCodecChanged || eventsOut[1].Type != av.EventEndOfStream {
		t.Fatalf("events = %+v", eventsOut)
	}
	if eventsOut[0].StreamID != updated.ID || eventsOut[0].Epoch != updated.Epoch {
		t.Fatalf("codec changed event = %+v", eventsOut[0])
	}
	if eventsOut[1].StreamID != "" {
		t.Fatalf("multi-stream eos should stay unscoped: %+v", eventsOut[1])
	}
	if len(packets) != 1 || packets[0].StreamID != updated.ID || packets[0].CodecEpoch != updated.Epoch {
		t.Fatalf("packets = %+v", packets)
	}
}

func TestSourceCodecChangedCanSwitchDepacketizerCodec(t *testing.T) {
	initial := av.Stream{
		ID:    "video",
		Type:  av.MediaVideo,
		Epoch: 1,
		Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo, ClockRate: 90000},
	}
	updated := initial
	updated.Epoch = 2
	updated.Codec = av.CodecParameters{ID: av.CodecH264, Type: av.MediaVideo, ClockRate: 90000}
	events := make(chan av.Event, 1)
	events <- av.Event{
		Type:     av.EventCodecChanged,
		StreamID: initial.ID,
		Epoch:    updated.Epoch,
		Stream:   &updated,
		Codec:    &updated.Codec,
	}
	receiver := &fakeReceiver{
		payloads: NewStaticPayloadMap(1, []PayloadCodec{{
			PayloadType: 96,
			Parameters:  initial.Codec,
			MIMEType:    MIMEVP8,
			ClockRate:   90000,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 97, Marker: true, Timestamp: 9000},
			Payload: []byte{0x65, 0xcc},
		}},
		events: events,
	}
	source, err := NewSource(SourceConfig{
		Receiver: receiver,
		Streams:  []av.Stream{initial},
		Depacketizers: []Depacketizer{
			NewVP8Depacketizer(initial, WithMaxVideoFrameSize(16)),
			NewH264Depacketizer(initial, WithMaxVideoFrameSize(16)),
		},
		MaxPackets: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	receiver.payloads = NewStaticPayloadMap(2, []PayloadCodec{{
		PayloadType: 97,
		Parameters:  updated.Codec,
		MIMEType:    MIMEH264,
		ClockRate:   90000,
	}})

	var packets []av.Packet
	var eventsOut []av.Event
	if err := source.Start(context.Background(), testEmitter(func(_ context.Context, msg *pipeline.Message) error {
		switch msg.Kind {
		case pipeline.MessagePacket:
			packets = append(packets, *msg.Packet)
		case pipeline.MessageEvent:
			eventsOut = append(eventsOut, *msg.Event)
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if len(eventsOut) != 2 || eventsOut[0].Type != av.EventCodecChanged || eventsOut[1].Type != av.EventEndOfStream {
		t.Fatalf("events = %+v", eventsOut)
	}
	if eventsOut[0].StreamID != updated.ID || eventsOut[0].Epoch != updated.Epoch ||
		eventsOut[1].StreamID != updated.ID || eventsOut[1].Epoch != updated.Epoch {
		t.Fatalf("events = %+v", eventsOut)
	}
	if len(packets) != 1 ||
		packets[0].StreamID != updated.ID ||
		packets[0].CodecEpoch != updated.Epoch ||
		!packets[0].Keyframe ||
		packets[0].Payload.Bytes[4] != 0x65 {
		t.Fatalf("packets = %+v", packets)
	}
}

func TestSourceEmitsTimestampDiscontinuityOnBackwardPTS(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{ID: "audio", Codec: av.CodecParameters{ID: av.CodecOpus, ClockRate: 48000}}
	receiver := &fakeReceiver{
		payloads: NewStaticPayloadMap(1, []PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    MIMEOpus,
			ClockRate:   48000,
		}}),
		packets: []*rtp.Packet{
			{Header: rtp.Header{PayloadType: 111, Timestamp: 960}, Payload: []byte{1}},
			{Header: rtp.Header{PayloadType: 111, Timestamp: 480}, Payload: []byte{2}},
		},
		events: make(chan av.Event),
	}
	source, err := NewSource(SourceConfig{
		Receiver:      receiver,
		Depacketizers: []Depacketizer{NewOpusDepacketizer(stream)},
		Streams:       []av.Stream{stream},
		MaxPackets:    2,
	})
	if err != nil {
		t.Fatal(err)
	}
	sink := &packetSink{name: "sink"}
	graph, err := pipeline.NewGraph(pipeline.GraphConfig{Name: "rtp"})
	if err != nil {
		t.Fatal(err)
	}
	sourceRef, err := graph.AddSource(source, pipeline.BufferPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	sinkRef, err := graph.AddSink(sink, pipeline.BufferPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(pipeline.Route{
		From:   sourceRef.String(),
		To:     []string{sinkRef.String()},
		Policy: pipeline.RouteAll,
	}); err != nil {
		t.Fatal(err)
	}

	if err := graph.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(sink.packets) != 2 || !sink.packets[1].Discontinuous {
		t.Fatalf("packets = %+v", sink.packets)
	}
	if len(sink.events) != 2 || sink.events[0].Type != av.EventDiscontinuity || sink.events[1].Type != av.EventEndOfStream {
		t.Fatalf("events = %+v", sink.events)
	}
	if sink.events[0].StreamID != stream.ID || sink.events[0].Timestamp.Value != 480 ||
		sink.events[0].Reason != "timestamp moved backward" {
		t.Fatalf("discontinuity = %+v", sink.events[0])
	}
}

func TestSourceEmitsTimestampDiscontinuityOnLargeGap(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{ID: "audio", Codec: av.CodecParameters{ID: av.CodecOpus, ClockRate: 48000}}
	receiver := &fakeReceiver{
		payloads: NewStaticPayloadMap(1, []PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    MIMEOpus,
			ClockRate:   48000,
		}}),
		packets: []*rtp.Packet{
			{Header: rtp.Header{PayloadType: 111, Timestamp: 0}, Payload: []byte{1}},
			{Header: rtp.Header{PayloadType: 111, Timestamp: 48000}, Payload: []byte{2}},
		},
		events: make(chan av.Event),
	}
	source, err := NewSource(SourceConfig{
		Receiver:        receiver,
		Depacketizers:   []Depacketizer{NewOpusDepacketizer(stream)},
		Streams:         []av.Stream{stream},
		MaxTimestampGap: av.SamplesDuration(960, 48000),
		MaxPackets:      2,
	})
	if err != nil {
		t.Fatal(err)
	}
	var packets []av.Packet
	var events []av.Event

	if err := source.Start(ctx, testEmitter(func(_ context.Context, msg *pipeline.Message) error {
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
	if len(packets) != 2 || !packets[1].Discontinuous {
		t.Fatalf("packets = %+v", packets)
	}
	if len(events) != 2 || events[0].Type != av.EventDiscontinuity || events[0].Reason != "timestamp gap" ||
		events[1].Type != av.EventEndOfStream {
		t.Fatalf("events = %+v", events)
	}
}

func TestSourceJitterOrdersPackets(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{ID: "audio", Codec: av.CodecParameters{ID: av.CodecOpus, ClockRate: 48000}}
	jitter, err := NewJitterRing(JitterConfig{Capacity: 8})
	if err != nil {
		t.Fatal(err)
	}
	receiver := &fakeReceiver{
		payloads: NewStaticPayloadMap(1, []PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    MIMEOpus,
			ClockRate:   48000,
		}}),
		packets: []*rtp.Packet{
			{Header: rtp.Header{SSRC: 1, PayloadType: 111, SequenceNumber: 10, Timestamp: 10}, Payload: []byte{10}},
			{Header: rtp.Header{SSRC: 1, PayloadType: 111, SequenceNumber: 12, Timestamp: 12}, Payload: []byte{12}},
			{Header: rtp.Header{SSRC: 1, PayloadType: 111, SequenceNumber: 11, Timestamp: 11}, Payload: []byte{11}},
		},
		events: make(chan av.Event),
	}
	source, err := NewSource(SourceConfig{
		Receiver:      receiver,
		Jitter:        jitter,
		Depacketizers: []Depacketizer{NewOpusDepacketizer(stream)},
		MaxReady:      4,
		MaxPackets:    4,
	})
	if err != nil {
		t.Fatal(err)
	}
	sink := &packetSink{name: "sink"}
	graph, err := pipeline.NewGraph(pipeline.GraphConfig{Name: "rtp"})
	if err != nil {
		t.Fatal(err)
	}
	sourceRef, err := graph.AddSource(source, pipeline.BufferPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	sinkRef, err := graph.AddSink(sink, pipeline.BufferPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(pipeline.Route{
		From:   sourceRef.String(),
		To:     []string{sinkRef.String()},
		Policy: pipeline.RouteAll,
	}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Run(ctx); err != nil {
		t.Fatal(err)
	}

	if len(sink.packets) != 3 {
		t.Fatalf("packets = %d, want 3", len(sink.packets))
	}
	for i, want := range []int64{10, 11, 12} {
		if got := sink.packets[i].PTS.Value; got != want {
			t.Fatalf("packet[%d] pts = %d, want %d", i, got, want)
		}
	}
}

func TestSourceRoutesFeedbackToExplicitWriter(t *testing.T) {
	receiver := &fakeReceiver{
		payloads: NewStaticPayloadMap(1, []PayloadCodec{{
			PayloadType: 111,
			Parameters:  av.CodecParameters{ID: av.CodecOpus},
			MIMEType:    MIMEOpus,
			ClockRate:   48000,
		}}),
		packets: []*rtp.Packet{{Header: rtp.Header{PayloadType: 111}}},
		events:  make(chan av.Event),
	}
	writer := &fakeFeedbackWriter{}
	pli := &rtcp.PictureLossIndication{SenderSSRC: 1, MediaSSRC: 2}
	source, err := NewSource(SourceConfig{
		Receiver:      receiver,
		Feedback:      writer,
		Depacketizers: []Depacketizer{feedbackDepacketizer{packet: pli}},
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
	if len(receiver.feedback) != 0 {
		t.Fatalf("receiver feedback should not be used when explicit writer is set: %+v", receiver.feedback)
	}
}

func TestSourceForwardsEventsToDepacketizers(t *testing.T) {
	events := make(chan av.Event, 1)
	events <- av.Event{Type: av.EventPacketLoss, StreamID: "audio"}
	receiver := &fakeReceiver{
		payloads: NewStaticPayloadMap(1, []PayloadCodec{{
			PayloadType: 111,
			Parameters:  av.CodecParameters{ID: av.CodecOpus},
			MIMEType:    MIMEOpus,
			ClockRate:   48000,
		}}),
		events: events,
	}
	depacketizer := &eventDepacketizer{}
	source, err := NewSource(SourceConfig{
		Receiver:      receiver,
		Depacketizers: []Depacketizer{depacketizer},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := source.Start(context.Background(), testEmitter(func(context.Context, *pipeline.Message) error {
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if len(depacketizer.events) != 2 {
		t.Fatalf("events = %+v, want packet loss and EOS", depacketizer.events)
	}
	if depacketizer.events[0].Type != av.EventPacketLoss || depacketizer.events[1].Type != av.EventEndOfStream {
		t.Fatalf("events = %+v", depacketizer.events)
	}
}

func TestSourceEOSUsesSingleConfiguredStream(t *testing.T) {
	stream := av.Stream{ID: "audio", Epoch: 7}
	receiver := &fakeReceiver{
		payloads: NewStaticPayloadMap(1, nil),
		events:   make(chan av.Event),
	}
	source, err := NewSource(SourceConfig{
		Receiver: receiver,
		Streams:  []av.Stream{stream},
	})
	if err != nil {
		t.Fatal(err)
	}
	var events []av.Event

	if err := source.Start(context.Background(), testEmitter(func(_ context.Context, msg *pipeline.Message) error {
		if msg.Kind == pipeline.MessageEvent {
			events = append(events, *msg.Event)
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Type != av.EventEndOfStream || events[0].StreamID != stream.ID || events[0].Epoch != stream.Epoch {
		t.Fatalf("events = %+v", events)
	}
}

func TestSourceCodecChangedRefreshesPayloadMapAndEpoch(t *testing.T) {
	initial := av.Stream{
		ID:    "audio",
		Epoch: 1,
		Codec: av.CodecParameters{
			ID:        av.CodecOpus,
			Type:      av.MediaAudio,
			ClockRate: 48000,
		},
	}
	updated := initial
	updated.Epoch = 2
	events := make(chan av.Event, 1)
	events <- av.Event{
		Type:     av.EventCodecChanged,
		StreamID: updated.ID,
		Epoch:    updated.Epoch,
		Stream:   &updated,
		Codec:    &updated.Codec,
	}
	receiver := &fakeReceiver{
		payloads: NewStaticPayloadMap(1, []PayloadCodec{{
			PayloadType: 111,
			Parameters:  initial.Codec,
			MIMEType:    MIMEOpus,
			ClockRate:   48000,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 112, Timestamp: 960},
			Payload: []byte{9, 8, 7},
		}},
		events: events,
	}
	source, err := NewSource(SourceConfig{
		Receiver:      receiver,
		Depacketizers: []Depacketizer{NewOpusDepacketizer(initial)},
		MaxPackets:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	receiver.payloads = NewStaticPayloadMap(2, []PayloadCodec{{
		PayloadType: 112,
		Parameters:  updated.Codec,
		MIMEType:    MIMEOpus,
		ClockRate:   48000,
	}})
	var packets []av.Packet
	var gotEvents []av.Event

	if err := source.Start(context.Background(), testEmitter(func(_ context.Context, msg *pipeline.Message) error {
		switch msg.Kind {
		case pipeline.MessagePacket:
			packets = append(packets, *msg.Packet)
		case pipeline.MessageEvent:
			gotEvents = append(gotEvents, *msg.Event)
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if len(gotEvents) != 2 || gotEvents[0].Type != av.EventCodecChanged || gotEvents[1].Type != av.EventEndOfStream {
		t.Fatalf("events = %+v", gotEvents)
	}
	if len(packets) != 1 {
		t.Fatalf("packets = %d, want 1", len(packets))
	}
	if packets[0].StreamID != updated.ID || packets[0].CodecEpoch != updated.Epoch {
		t.Fatalf("packet = %+v", packets[0])
	}
	if packets[0].PTS.Value != 960 || packets[0].Payload.Bytes[0] != 9 {
		t.Fatalf("packet = %+v", packets[0])
	}
}

func TestSourceErrors(t *testing.T) {
	if _, err := NewSource(SourceConfig{}); !errors.Is(err, ErrNilReceiver) {
		t.Fatalf("err = %v, want ErrNilReceiver", err)
	}

	receiver := &fakeReceiver{
		payloads: NewStaticPayloadMap(1, nil),
		packets:  []*rtp.Packet{{Header: rtp.Header{PayloadType: 111}}},
		events:   make(chan av.Event),
	}
	source, err := NewSource(SourceConfig{Receiver: receiver})
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Start(context.Background(), testEmitter(func(context.Context, *pipeline.Message) error {
		return nil
	})); !errors.Is(err, ErrPayloadNotFound) {
		t.Fatalf("err = %v, want ErrPayloadNotFound", err)
	}
}

func TestSourceStartAllocs(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{ID: "audio", Codec: av.CodecParameters{ID: av.CodecOpus, ClockRate: 48000}}
	receiver := &fakeReceiver{
		payloads: NewStaticPayloadMap(1, []PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    MIMEOpus,
			ClockRate:   48000,
		}}),
		packets: []*rtp.Packet{
			{Header: rtp.Header{PayloadType: 111, Timestamp: 960}, Payload: []byte{1, 2, 3}},
		},
		events: make(chan av.Event),
	}
	source, err := NewSource(SourceConfig{
		Receiver:        receiver,
		Depacketizers:   []Depacketizer{NewOpusDepacketizer(stream)},
		Streams:         []av.Stream{stream},
		MaxTimestampGap: av.SamplesDuration(960, 48000),
		MaxPackets:      1,
	})
	if err != nil {
		t.Fatal(err)
	}
	emitter := &countEmitter{}

	if allocs := testing.AllocsPerRun(1000, func() {
		receiver.Reset()
		emitter.Reset()
		if err := source.Start(ctx, emitter); err != nil {
			t.Fatal(err)
		}
		if emitter.packets != 1 || emitter.events != 1 {
			t.Fatalf("packets=%d events=%d", emitter.packets, emitter.events)
		}
	}); allocs != 0 {
		t.Fatalf("source start allocs = %v, want 0", allocs)
	}
}
