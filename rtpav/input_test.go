package rtpav

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// inputTestReceiver ports the goav root runtime RTP test receiver: configured
// streams, a static payload map, scripted packets, and an event channel.
type inputTestReceiver struct {
	streams []av.Stream
	payload PayloadMap
	packets []*rtp.Packet
	events  chan av.Event
	index   int
	closed  bool
}

func (r *inputTestReceiver) Streams(context.Context) ([]av.Stream, error) {
	streams := make([]av.Stream, len(r.streams))
	copy(streams, r.streams)
	return streams, nil
}

func (r *inputTestReceiver) PayloadMap() PayloadMap {
	return r.payload
}

func (r *inputTestReceiver) ReadRTP(context.Context) (*rtp.Packet, error) {
	if r.index >= len(r.packets) {
		return nil, io.EOF
	}
	packet := r.packets[r.index]
	r.index++
	return packet, nil
}

func (r *inputTestReceiver) Events() <-chan av.Event {
	return r.events
}

func (r *inputTestReceiver) Close() error {
	r.closed = true
	return nil
}

func inputTestOpusStream() av.Stream {
	return av.Stream{
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
}

func inputTestVP8Stream() av.Stream {
	return av.Stream{
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
}

func inputTestOpusReceiver(stream av.Stream, packets ...*rtp.Packet) *inputTestReceiver {
	return &inputTestReceiver{
		streams: []av.Stream{stream},
		payload: NewStaticPayloadMap(0, []PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    MIMEOpus,
			ClockRate:   48000,
			Channels:    2,
		}}),
		packets: packets,
		events:  make(chan av.Event, 1),
	}
}

type collectEmitter struct {
	packets []av.Packet
	events  []av.Event
}

func (e *collectEmitter) Emit(_ context.Context, msg *pipeline.Message) error {
	switch msg.Kind {
	case pipeline.MessagePacket:
		e.packets = append(e.packets, *msg.Packet)
	case pipeline.MessageEvent:
		e.events = append(e.events, *msg.Event)
	}
	return nil
}

func TestReceiveOpenSourceFeedsPipeline(t *testing.T) {
	ctx := context.Background()
	stream := inputTestOpusStream()
	receiver := inputTestOpusReceiver(stream, &rtp.Packet{
		Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
		Payload: []byte{1, 2, 3},
	})
	receiver.events <- av.Event{Type: av.EventPacketLoss, StreamID: stream.ID}

	input := Receive(receiver,
		WithName("remote-audio"),
		WithDepacketizers(NewOpusDepacketizer(stream)),
		WithMaxTimestampGap(av.SamplesDuration(960, 48000)),
		WithBufferLimits(BufferLimits{MaxPackets: 2, MaxEvents: 2}),
	)
	source, streams, err := input.OpenSource(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(streams) != 1 || streams[0].ID != "audio" || streams[0].Codec.ID != av.CodecOpus {
		t.Fatalf("streams = %+v", streams)
	}
	if source.Name() != "remote-audio" {
		t.Fatalf("source name = %q", source.Name())
	}
	spec := source.(pipeline.NodeDescriber).DescribeNode()
	if spec.Detail != input.Detail() {
		t.Fatalf("node detail = %q, want %q", spec.Detail, input.Detail())
	}

	emitter := &collectEmitter{}
	if err := source.Start(ctx, emitter); err != nil {
		t.Fatal(err)
	}
	if len(emitter.packets) != 1 {
		t.Fatalf("packets = %d, want 1", len(emitter.packets))
	}
	packet := emitter.packets[0]
	if packet.StreamID != "audio" || string(packet.Payload.Bytes) != "\x01\x02\x03" {
		t.Fatalf("packet = %+v", packet)
	}
	if len(emitter.events) != 2 ||
		emitter.events[0].Type != av.EventPacketLoss ||
		emitter.events[1].Type != av.EventEndOfStream {
		t.Fatalf("events = %+v", emitter.events)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if !receiver.closed {
		t.Fatal("receiver not closed")
	}
}

func TestReceiveOpenSourceRequiresReceiver(t *testing.T) {
	_, _, err := Receive(nil).OpenSource(context.Background())
	if !errors.Is(err, ErrNilReceiver) {
		t.Fatalf("err = %v, want ErrNilReceiver", err)
	}
}

func TestReceiveCodecIntentDerivesDepacketizerForNamedStream(t *testing.T) {
	ctx := context.Background()
	video := inputTestVP8Stream()
	receiver := &inputTestReceiver{
		streams: []av.Stream{inputTestOpusStream(), video},
		payload: NewStaticPayloadMap(0, []PayloadCodec{{
			PayloadType: 96,
			Parameters:  video.Codec,
			MIMEType:    MIMEVP8,
			ClockRate:   90000,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 96, Marker: true, Timestamp: 3000},
			Payload: []byte{0x10, 0x00, 0xaa},
		}},
		events: make(chan av.Event),
	}
	input := Receive(receiver, WithCodec(codec.CodecSpec{ID: av.CodecVP8, Type: av.MediaVideo}))

	source, streams, err := input.OpenSource(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(streams) != 2 {
		t.Fatalf("streams = %+v", streams)
	}
	emitter := &collectEmitter{}
	if err := source.Start(ctx, emitter); err != nil {
		t.Fatal(err)
	}
	if len(emitter.packets) != 1 || emitter.packets[0].StreamID != "video" {
		t.Fatalf("packets = %+v", emitter.packets)
	}
}

// The codec-intent name renames the depacketizer stream when no receiver
// stream matches the name, mirroring the goav root codecStream derivation.
func TestReceiveCodecIntentRenamesUnmatchedStream(t *testing.T) {
	ctx := context.Background()
	stream := inputTestOpusStream()
	receiver := inputTestOpusReceiver(stream, &rtp.Packet{
		Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
		Payload: []byte{4, 5},
	})
	input := Receive(receiver,
		WithName("mic"),
		WithCodec(codec.CodecSpec{ID: av.CodecOpus, Type: av.MediaAudio}),
	)
	source, _, err := input.OpenSource(ctx)
	if err != nil {
		t.Fatal(err)
	}
	emitter := &collectEmitter{}
	if err := source.Start(ctx, emitter); err != nil {
		t.Fatal(err)
	}
	if len(emitter.packets) != 1 || emitter.packets[0].StreamID != "mic" {
		t.Fatalf("packets = %+v", emitter.packets)
	}
}

func TestReceiveExplicitDepacketizersAndTimestampGap(t *testing.T) {
	ctx := context.Background()
	stream := inputTestOpusStream()
	receiver := inputTestOpusReceiver(stream,
		&rtp.Packet{Header: rtp.Header{PayloadType: 111, Timestamp: 960}, Payload: []byte{1}},
		&rtp.Packet{Header: rtp.Header{PayloadType: 111, Timestamp: 960 * 4}, Payload: []byte{2}},
	)
	input := Receive(receiver,
		WithDepacketizers(NewOpusDepacketizer(stream), nil),
		WithMaxTimestampGap(av.SamplesDuration(960, 48000)),
	)
	source, _, err := input.OpenSource(ctx)
	if err != nil {
		t.Fatal(err)
	}
	emitter := &collectEmitter{}
	if err := source.Start(ctx, emitter); err != nil {
		t.Fatal(err)
	}
	if len(emitter.packets) != 2 {
		t.Fatalf("packets = %d, want 2", len(emitter.packets))
	}
	if !emitter.packets[1].Discontinuous {
		t.Fatalf("packet = %+v, want discontinuous", emitter.packets[1])
	}
	discontinuities := 0
	for i := range emitter.events {
		if emitter.events[i].Type == av.EventDiscontinuity {
			discontinuities++
		}
	}
	if discontinuities != 1 {
		t.Fatalf("events = %+v, want one discontinuity", emitter.events)
	}
}

func TestReceiveNameAndDetailMatchGoavRoot(t *testing.T) {
	stream := inputTestOpusStream()
	jitter, err := NewJitterRing(JitterConfig{Capacity: 4})
	if err != nil {
		t.Fatal(err)
	}

	plain := Receive(&inputTestReceiver{})
	if plain.Name() != "rtp" || plain.Detail() != "rtp receive" {
		t.Fatalf("name = %q detail = %q", plain.Name(), plain.Detail())
	}

	custom := Receive(&inputTestReceiver{}, WithDepacketizers(NewOpusDepacketizer(stream)))
	if custom.Detail() != "rtp receive, custom payloads" {
		t.Fatalf("detail = %q", custom.Detail())
	}

	full := Receive(&inputTestReceiver{},
		WithName("remote-audio"),
		WithJitter(jitter),
		WithCodec(codec.CodecSpec{ID: av.CodecOpus, Type: av.MediaAudio}),
		WithFeedback(&fakeFeedbackWriter{}),
		WithDecodeBounds(codec.DecodeBounds{MaxPayloadBytes: 4096}),
		WithMaxTimestampGap(av.SamplesDuration(960, 48000)),
	)
	if full.Name() != "remote-audio" {
		t.Fatalf("name = %q", full.Name())
	}
	want := "rtp receive, jitter, codec=opus, feedback, decode bounds, timestamp gap"
	if full.Detail() != want {
		t.Fatalf("detail = %q, want %q", full.Detail(), want)
	}
}

func TestReceiveSourceShape(t *testing.T) {
	plain := Receive(&inputTestReceiver{}).SourceShape()
	if plain != (shape.Spec{Domain: shape.DomainPacket, Realtime: true}) {
		t.Fatalf("shape = %+v", plain)
	}

	stream := inputTestOpusStream()
	spec := Receive(&inputTestReceiver{}, WithCodec(codec.CodecSpec{
		ID:         av.CodecOpus,
		Type:       av.MediaAudio,
		Parameters: stream.Codec,
	})).SourceShape()
	want := shape.Spec{
		Domain:     shape.DomainPacket,
		MediaKind:  av.MediaAudio,
		Codec:      av.CodecOpus,
		SampleRate: 48000,
		Channels:   2,
		Realtime:   true,
	}
	if spec != want {
		t.Fatalf("shape = %+v, want %+v", spec, want)
	}
}

func TestReceiveDecodeBoundsPerStream(t *testing.T) {
	ctx := context.Background()
	bounds := codec.DecodeBounds{
		MaxFramesPerInput:   2,
		MaxEventsPerInput:   3,
		MaxRequestsPerInput: 4,
		MaxPayloadBytes:     4096,
		MaxRetainedBytes:    8192,
		MaxWidth:            1280,
		MaxHeight:           720,
	}
	video := inputTestVP8Stream()
	receiver := &inputTestReceiver{
		streams: []av.Stream{video},
		payload: NewStaticPayloadMap(0, nil),
		events:  make(chan av.Event),
	}
	input := Receive(receiver,
		WithDepacketizers(NewVP8Depacketizer(video)),
		WithDecodeBounds(bounds),
	)
	if got := input.DecodeBounds("video"); got != (codec.DecodeBounds{}) {
		t.Fatalf("bounds before open = %+v, want zero", got)
	}
	if _, _, err := input.OpenSource(ctx); err != nil {
		t.Fatal(err)
	}
	if got := input.DecodeBounds("video"); got != bounds {
		t.Fatalf("bounds = %+v, want %+v", got, bounds)
	}
	if got := input.DecodeBounds("audio"); got != (codec.DecodeBounds{}) {
		t.Fatalf("bounds for other stream = %+v, want zero", got)
	}

	unbounded := Receive(receiver, WithDepacketizers(NewVP8Depacketizer(video)))
	if _, _, err := unbounded.OpenSource(ctx); err != nil {
		t.Fatal(err)
	}
	if got := unbounded.DecodeBounds("video"); got != (codec.DecodeBounds{}) {
		t.Fatalf("unconfigured bounds = %+v, want zero", got)
	}
}
