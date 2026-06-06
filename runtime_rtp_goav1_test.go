//go:build goav_goav1

package goav

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/pion/rtp"
	goav1adapter "github.com/thesyncim/goav/adapters/goav1"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/rtpav"
	av1backend "github.com/thesyncim/goav1"
)

func TestRuntimeBuilderRTPAV1DecodeSink(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "video",
		Type:     av.MediaVideo,
		TimeBase: av.RTPTimeBase(90000),
		Codec: av.CodecParameters{
			ID:          av.CodecAV1,
			Type:        av.MediaVideo,
			ClockRate:   90000,
			Width:       16,
			Height:      16,
			PixelFormat: av.PixelFormatGray8,
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
			Header:  rtp.Header{PayloadType: 96, Marker: true, Timestamp: 3000},
			Payload: runtimeAV1RTPPayload(),
		}},
		events: make(chan av.Event),
	}
	sink := &runtimeTestSink{name: "frames"}

	builder := newTestBuilder(t, WithCodecAdapter(goav1adapter.Register)).
		RTP(receiver,
			withRTPName("av1-rtp"),
			withRTPDepacketizers(rtpav.NewAV1Depacketizer(stream, rtpav.WithMaxVideoFrameSize(128))),
		).
		Decode(testSelectVideo()).
		Sink(sink)
	planned, err := builder.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(specText(planned), "av1-rtp -> select-video") ||
		!strings.Contains(specText(planned), "decode-video -> frames") {
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
	if sink.frames != 1 ||
		sink.lastFrame.StreamID != "video" ||
		sink.lastFrame.Video == nil ||
		sink.lastFrame.Video.Width != 16 ||
		sink.lastFrame.Video.Height != 16 ||
		sink.lastFrame.Video.PixelFormat != av.PixelFormatGray8 ||
		sink.lastFrame.PTS.Value != 3000 {
		t.Fatalf("frames=%d last=%+v video=%+v", sink.frames, sink.lastFrame, sink.lastFrame.Video)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !receiver.closed || !sink.closed {
		t.Fatalf("closed receiver=%v sink=%v", receiver.closed, sink.closed)
	}
}

func TestRuntimeBuilderRTPAV1DecodeSinkI420(t *testing.T) {
	testRuntimeBuilderRTPAV1DecodeSink420(t, av.PixelFormatI420)
}

func TestRuntimeBuilderRTPAV1DecodeSinkYUV420P(t *testing.T) {
	testRuntimeBuilderRTPAV1DecodeSink420(t, av.PixelFormatYUV420P)
}

func testRuntimeBuilderRTPAV1DecodeSink420(t *testing.T, pixelFormat string) {
	t.Helper()
	ctx := context.Background()
	stream := av.Stream{
		ID:       "video",
		Type:     av.MediaVideo,
		TimeBase: av.RTPTimeBase(90000),
		Codec: av.CodecParameters{
			ID:          av.CodecAV1,
			Type:        av.MediaVideo,
			ClockRate:   90000,
			Width:       16,
			Height:      16,
			PixelFormat: pixelFormat,
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
			Header:  rtp.Header{PayloadType: 96, Marker: true, Timestamp: 3000},
			Payload: runtimeAV1I420RTPPayload(),
		}},
		events: make(chan av.Event),
	}
	sink := &runtimeAV1FrameSink{name: "frames"}

	task, err := newTestBuilder(t, WithCodecAdapter(goav1adapter.Register)).
		RTP(receiver,
			withRTPName("av1-rtp"),
			withRTPDepacketizers(rtpav.NewAV1Depacketizer(stream, rtpav.WithMaxVideoFrameSize(128))),
		).
		Decode(testSelectVideo()).
		Sink(sink).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if sink.frames != 1 ||
		sink.lastVideo.Width != 16 ||
		sink.lastVideo.Height != 16 ||
		sink.lastVideo.PixelFormat != av.PixelFormatI420 ||
		sink.lastPlaneCount != 3 ||
		sink.lastPlaneBytes[0] == 0 ||
		sink.lastPlaneBytes[1] == 0 ||
		sink.lastPlaneBytes[2] == 0 {
		t.Fatalf("frames=%d video=%+v planes=%d bytes=%+v", sink.frames, sink.lastVideo, sink.lastPlaneCount, sink.lastPlaneBytes)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !receiver.closed || !sink.closed {
		t.Fatalf("closed receiver=%v sink=%v", receiver.closed, sink.closed)
	}
}

func TestRuntimeBuilderRTPAV1CodecChangedDropsUntilSync(t *testing.T) {
	ctx := context.Background()
	initial := av.Stream{
		ID:       "video",
		Type:     av.MediaVideo,
		Epoch:    1,
		TimeBase: av.RTPTimeBase(90000),
		Codec: av.CodecParameters{
			ID:          av.CodecAV1,
			Type:        av.MediaVideo,
			ClockRate:   90000,
			Width:       16,
			Height:      16,
			PixelFormat: av.PixelFormatGray8,
		},
	}
	updated := initial
	updated.Epoch = 2

	receiver := newRuntimeAV1SwitchReceiver(initial, updated, []*rtp.Packet{
		{
			Header:  rtp.Header{PayloadType: 96, Marker: true, Timestamp: 3000},
			Payload: runtimeAV1RTPPayload(),
		},
		{
			Header:  rtp.Header{PayloadType: 97, Marker: true, Timestamp: 6000},
			Payload: []byte{0x10, 0x30, 0xcc},
		},
		{
			Header:  rtp.Header{PayloadType: 97, Marker: true, Timestamp: 9000},
			Payload: runtimeAV1RTPPayload(),
		},
	})
	sink := &runtimeTestSink{name: "frames"}

	task, err := newTestBuilder(t, WithCodecAdapter(goav1adapter.Register)).
		RTP(receiver,
			withRTPName("av1-rtp"),
			withRTPDepacketizers(rtpav.NewAV1Depacketizer(initial, rtpav.WithMaxVideoFrameSize(128))),
			withRTPBufferLimits(RTPBufferLimits{MaxPackets: 1, MaxEvents: 2}),
		).
		Decode(testSelectVideo()).
		Sink(sink).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if sink.frames != 2 ||
		sink.lastFrame.StreamID != updated.ID ||
		sink.lastFrame.CodecEpoch != updated.Epoch ||
		sink.lastFrame.PTS.Value != 9000 ||
		sink.lastFrame.Video == nil ||
		sink.lastFrame.Video.Width != 16 ||
		sink.lastFrame.Video.Height != 16 ||
		sink.lastFrame.Video.PixelFormat != av.PixelFormatGray8 {
		t.Fatalf("frames=%d last=%+v video=%+v", sink.frames, sink.lastFrame, sink.lastFrame.Video)
	}
	gotEvents := drainTaskEvents(task)
	if countEventsForStream(gotEvents, av.EventCodecChanged, updated.ID) == 0 ||
		countEventsForStream(gotEvents, av.EventKeyframeRequired, updated.ID) == 0 ||
		countEventsForStream(gotEvents, av.EventEndOfStream, updated.ID) == 0 {
		t.Fatalf("events = %+v", gotEvents)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !receiver.closed || !sink.closed {
		t.Fatalf("closed receiver=%v sink=%v", receiver.closed, sink.closed)
	}
}

func TestRuntimeBuilderRTPAV1CodecChangedAdoptsReplacementStream(t *testing.T) {
	testRuntimeBuilderRTPAV1CodecChangedReplacementStream(t, false)
}

func TestRuntimeBuilderRTPAV1CodecChangedUsesOldIDReplacementTarget(t *testing.T) {
	testRuntimeBuilderRTPAV1CodecChangedReplacementStream(t, true)
}

func testRuntimeBuilderRTPAV1CodecChangedReplacementStream(t *testing.T, oldIDTarget bool) {
	t.Helper()
	ctx := context.Background()
	initial := av.Stream{
		ID:       "video-main",
		Type:     av.MediaVideo,
		Epoch:    1,
		TimeBase: av.RTPTimeBase(90000),
		Codec: av.CodecParameters{
			ID:          av.CodecAV1,
			Type:        av.MediaVideo,
			ClockRate:   90000,
			Width:       16,
			Height:      16,
			PixelFormat: av.PixelFormatGray8,
		},
	}
	updated := initial
	updated.ID = "video-replaced"
	updated.Epoch = 2
	eventStreamID := updated.ID
	if oldIDTarget {
		eventStreamID = initial.ID
	}

	receiver := newRuntimeAV1SwitchReceiverWithEventStreamID(initial, updated, eventStreamID, []*rtp.Packet{
		{
			Header:  rtp.Header{PayloadType: 96, Marker: true, Timestamp: 3000},
			Payload: runtimeAV1RTPPayload(),
		},
		{
			Header:  rtp.Header{PayloadType: 97, Marker: true, Timestamp: 6000},
			Payload: []byte{0x10, 0x30, 0xcc},
		},
		{
			Header:  rtp.Header{PayloadType: 97, Marker: true, Timestamp: 9000},
			Payload: runtimeAV1RTPPayload(),
		},
	})
	sink := &runtimeTestSink{name: "frames"}

	task, err := newTestBuilder(t, WithCodecAdapter(goav1adapter.Register)).
		RTP(receiver,
			withRTPName("av1-rtp"),
			withRTPDepacketizers(rtpav.NewAV1Depacketizer(initial, rtpav.WithMaxVideoFrameSize(128))),
			withRTPBufferLimits(RTPBufferLimits{MaxPackets: 1, MaxEvents: 2}),
		).
		Decode(testSelectVideo()).
		Sink(sink).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if sink.frames != 2 ||
		sink.lastFrame.StreamID != updated.ID ||
		sink.lastFrame.CodecEpoch != updated.Epoch ||
		sink.lastFrame.PTS.Value != 9000 ||
		sink.lastFrame.Video == nil ||
		sink.lastFrame.Video.PixelFormat != av.PixelFormatGray8 {
		t.Fatalf("frames=%d last=%+v video=%+v", sink.frames, sink.lastFrame, sink.lastFrame.Video)
	}
	gotEvents := drainTaskEvents(task)
	if countEventsForStream(gotEvents, av.EventCodecChanged, updated.ID) == 0 ||
		countEventsForStream(gotEvents, av.EventKeyframeRequired, updated.ID) == 0 ||
		countEventsForStream(gotEvents, av.EventEndOfStream, updated.ID) == 0 {
		t.Fatalf("events = %+v", gotEvents)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !receiver.closed || !sink.closed {
		t.Fatalf("closed receiver=%v sink=%v", receiver.closed, sink.closed)
	}
}

type runtimeAV1SwitchReceiver struct {
	initial       av.Stream
	updated       av.Stream
	eventStreamID av.StreamID
	payload       rtpav.PayloadMap
	packets       []*rtp.Packet
	events        chan av.Event
	index         int
	switched      bool
	closed        bool
}

type runtimeAV1FrameSink struct {
	name           string
	frames         int
	lastVideo      av.VideoFrame
	lastPlaneCount int
	lastPlaneBytes [3]int
	closed         bool
}

func (s *runtimeAV1FrameSink) Name() string {
	return s.name
}

func (s *runtimeAV1FrameSink) Handle(_ context.Context, msg *pipeline.Message) error {
	if msg == nil || msg.Kind != pipeline.MessageFrame || msg.Frame == nil {
		return nil
	}
	s.frames++
	if msg.Frame.Video != nil {
		s.lastVideo = *msg.Frame.Video
	}
	s.lastPlaneCount = len(msg.Frame.Planes)
	s.lastPlaneBytes = [3]int{}
	for i := range msg.Frame.Planes {
		if i >= len(s.lastPlaneBytes) {
			break
		}
		s.lastPlaneBytes[i] = len(msg.Frame.Planes[i].Buffer.Bytes)
	}
	return nil
}

func (s *runtimeAV1FrameSink) Close() error {
	s.closed = true
	return nil
}

func newRuntimeAV1SwitchReceiver(initial av.Stream, updated av.Stream, packets []*rtp.Packet) *runtimeAV1SwitchReceiver {
	return newRuntimeAV1SwitchReceiverWithEventStreamID(initial, updated, updated.ID, packets)
}

func newRuntimeAV1SwitchReceiverWithEventStreamID(initial av.Stream, updated av.Stream, eventStreamID av.StreamID, packets []*rtp.Packet) *runtimeAV1SwitchReceiver {
	return &runtimeAV1SwitchReceiver{
		initial:       initial,
		updated:       updated,
		eventStreamID: eventStreamID,
		payload:       runtimeAV1PayloadMap(1, 96, initial),
		packets:       packets,
		events:        make(chan av.Event, 1),
	}
}

func (r *runtimeAV1SwitchReceiver) Streams(context.Context) ([]av.Stream, error) {
	return []av.Stream{r.initial}, nil
}

func (r *runtimeAV1SwitchReceiver) PayloadMap() rtpav.PayloadMap {
	return r.payload
}

func (r *runtimeAV1SwitchReceiver) ReadRTP(context.Context) (*rtp.Packet, error) {
	if r.index >= len(r.packets) {
		return nil, io.EOF
	}
	packet := r.packets[r.index]
	r.index++
	if r.index == 1 && !r.switched {
		r.payload = runtimeAV1PayloadMap(2, 97, r.updated)
		r.events <- av.Event{
			Type:     av.EventCodecChanged,
			StreamID: r.eventStreamID,
			Epoch:    r.updated.Epoch,
			Stream:   &r.updated,
			Codec:    &r.updated.Codec,
		}
		r.switched = true
	}
	return packet, nil
}

func (r *runtimeAV1SwitchReceiver) Events() <-chan av.Event {
	return r.events
}

func (r *runtimeAV1SwitchReceiver) Close() error {
	r.closed = true
	return nil
}

func runtimeAV1PayloadMap(epoch av.Epoch, payloadType uint8, stream av.Stream) rtpav.PayloadMap {
	return rtpav.NewStaticPayloadMap(epoch, []rtpav.PayloadCodec{{
		PayloadType: payloadType,
		Parameters:  stream.Codec,
		MIMEType:    rtpav.MIMEAV1,
		ClockRate:   90000,
	}})
}

func runtimeAV1RTPPayload() []byte {
	return runtimeAV1RTPPayloadWithSequence(runtimeAV1SequenceHeaderPayload(), runtimeAV1FrameHeaderPayload())
}

func runtimeAV1I420RTPPayload() []byte {
	return runtimeAV1RTPPayloadWithSequence(runtimeAV1I420SequenceHeaderPayload(), runtimeAV1I420FrameHeaderPayload())
}

func runtimeAV1RTPPayloadWithSequence(sequenceHeader []byte, frameHeader []byte) []byte {
	seq := runtimeAV1OBUElement(av1backend.OBUSequenceHeader, sequenceHeader)
	frame := runtimeAV1OBUElement(av1backend.OBUFrameHeader, frameHeader)
	tile := runtimeAV1OBUElement(av1backend.OBUTileGroup, []byte{0x80})
	payload := []byte{0x38}
	payload = append(payload, byte(len(seq)))
	payload = append(payload, seq...)
	payload = append(payload, byte(len(frame)))
	payload = append(payload, frame...)
	payload = append(payload, tile...)
	return payload
}

func runtimeAV1OBUElement(typ av1backend.OBUType, payload []byte) []byte {
	var header [2]byte
	n, err := av1backend.PutOBUHeader(header[:], av1backend.OBUHeader{Type: typ})
	if err != nil {
		panic(err)
	}
	out := append([]byte{}, header[:n]...)
	return append(out, payload...)
}

func runtimeAV1SequenceHeaderPayload() []byte {
	return runtimeAV1SequenceHeaderPayloadForMonochrome(true)
}

func runtimeAV1I420SequenceHeaderPayload() []byte {
	return runtimeAV1SequenceHeaderPayloadForMonochrome(false)
}

func runtimeAV1SequenceHeaderPayloadForMonochrome(mono bool) []byte {
	var w runtimeAV1BitWriter
	w.writeBits(0, 3)
	w.writeBool(true)
	w.writeBool(true)
	w.writeBits(5, 5)
	w.writeBits(7, 4)
	w.writeBits(7, 4)
	w.writeBits(15, 8)
	w.writeBits(15, 8)
	w.writeBool(false)
	w.writeBool(true)
	w.writeBool(true)
	w.writeBool(false)
	w.writeBool(true)
	w.writeBool(false)
	w.writeBool(false)
	w.writeBool(mono)
	w.writeBool(false)
	w.writeBool(false)
	if !mono {
		w.writeBits(0, 2)
		w.writeBool(false)
	}
	w.writeBool(false)
	return w.trailingBits()
}

func runtimeAV1FrameHeaderPayload() []byte {
	return runtimeAV1FrameHeaderPayloadForMonochrome(true)
}

func runtimeAV1I420FrameHeaderPayload() []byte {
	return runtimeAV1FrameHeaderPayloadForMonochrome(false)
}

func runtimeAV1FrameHeaderPayloadForMonochrome(mono bool) []byte {
	var w runtimeAV1BitWriter
	w.writeBool(true)
	w.writeBool(false)
	w.writeBool(false)
	w.writeBits(0, 8)
	w.writeBool(false)
	if !mono {
		w.writeBool(false)
		w.writeBool(false)
	}
	w.writeBool(false)
	w.writeBool(false)
	w.writeBool(false)
	return w.bytes()
}

type runtimeAV1BitWriter struct {
	buf [128]byte
	bit int
}

func (w *runtimeAV1BitWriter) writeBits(value uint64, n uint8) {
	for i := int(n) - 1; i >= 0; i-- {
		if (value>>uint(i))&1 != 0 {
			w.buf[w.bit>>3] |= 1 << uint(7-(w.bit&7))
		}
		w.bit++
	}
}

func (w *runtimeAV1BitWriter) writeBool(value bool) {
	if value {
		w.writeBits(1, 1)
		return
	}
	w.writeBits(0, 1)
}

func (w *runtimeAV1BitWriter) bytes() []byte {
	return w.buf[:(w.bit+7)>>3]
}

func (w *runtimeAV1BitWriter) trailingBits() []byte {
	w.writeBits(1, 1)
	for w.bit&7 != 0 {
		w.writeBits(0, 1)
	}
	return w.buf[:w.bit>>3]
}
