package gopus

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/rtpav"
)

func TestDescriptor(t *testing.T) {
	desc := Descriptor()
	if desc.ID != av.CodecOpus || !desc.Supports(codec.ModeDecode) {
		t.Fatalf("descriptor = %+v", desc)
	}
	if !desc.Supports(codec.ModeEncode) {
		t.Fatal("gopus adapter should claim encode support")
	}
}

func TestRegister(t *testing.T) {
	registry := codec.NewRegistry()
	Register(registry)

	if _, err := registry.DecoderFactory(av.CodecOpus); err != nil {
		t.Fatalf("decoder factory: %v", err)
	}
	if _, err := registry.EncoderFactory(av.CodecOpus); err != nil {
		t.Fatalf("encoder factory: %v", err)
	}
}

func TestEncodeOpusIntoCallerOwnedPacket(t *testing.T) {
	ctx := context.Background()
	encoder, err := NewEncoderFactory().NewEncoder(ctx, opusEncodeConfig())
	if err != nil {
		t.Fatal(err)
	}
	result := opusEncodeResult(1, 4096)
	frame := opusTestFrame()

	if err := encoder.EncodeInto(ctx, &frame, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Packets) != 1 {
		t.Fatalf("packets = %d, want 1", len(result.Packets))
	}
	packet := result.Packets[0]
	if packet.StreamID != "encoded-audio" || packet.CodecEpoch != 7 {
		t.Fatalf("packet identity = %+v", packet)
	}
	if len(packet.Payload.Bytes) == 0 || packet.Payload.Ownership != av.BufferOwned {
		t.Fatalf("payload = %+v", packet.Payload)
	}
	if packet.PTS != frame.PTS || packet.Duration != frame.Duration {
		t.Fatalf("timing packet=%+v frame=%+v", packet, frame)
	}
}

func TestEncodeOpusRequiresPacketCapacity(t *testing.T) {
	ctx := context.Background()
	encoder, err := NewEncoderFactory().NewEncoder(ctx, opusEncodeConfig())
	if err != nil {
		t.Fatal(err)
	}
	frame := opusTestFrame()

	if err := encoder.EncodeInto(ctx, &frame, &codec.EncodeResult{}); !errors.Is(err, codec.ErrResultFull) {
		t.Fatalf("err = %v, want ErrResultFull", err)
	}
}

func TestEncodeOpusRequiresPayloadCapacity(t *testing.T) {
	ctx := context.Background()
	encoder, err := NewEncoderFactory().NewEncoder(ctx, opusEncodeConfig())
	if err != nil {
		t.Fatal(err)
	}
	result := opusEncodeResult(1, 0)
	frame := opusTestFrame()

	if err := encoder.EncodeInto(ctx, &frame, &result); !errors.Is(err, codec.ErrOutputBufferTooSmall) {
		t.Fatalf("err = %v, want ErrOutputBufferTooSmall", err)
	}
}

func TestEncodeOpusCloseIsIdempotentAndStopsUse(t *testing.T) {
	ctx := context.Background()
	encoder, err := NewEncoderFactory().NewEncoder(ctx, opusEncodeConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	result := opusEncodeResult(1, 4096)
	frame := opusTestFrame()

	if err := encoder.EncodeInto(ctx, &frame, &result); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("encode after close = %v, want ErrClosed", err)
	}
}

func TestDecodePacketLossConcealmentIntoPreallocatedFrame(t *testing.T) {
	ctx := context.Background()
	decoder, result := newTestDecoder(t)
	event := av.Event{Type: av.EventPacketLoss}

	if err := decoder.HandleEvent(ctx, &event); err != nil {
		t.Fatal(err)
	}
	if err := decoder.DecodeInto(ctx, nil, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(result.Frames))
	}
	frame := result.Frames[0]
	if frame.Type != av.MediaAudio || frame.Audio == nil {
		t.Fatalf("frame = %+v", frame)
	}
	if frame.Audio.SampleRate != 48000 || frame.Audio.Channels != 1 || frame.Audio.SampleFormat != av.SampleFormatS16 {
		t.Fatalf("audio = %+v", frame.Audio)
	}
	if len(frame.Planes) != 1 || frame.Planes[0].Buffer.Ownership != av.BufferOwned {
		t.Fatalf("planes = %+v", frame.Planes)
	}
	if len(frame.Planes[0].Buffer.Bytes) == 0 {
		t.Fatal("empty PCM output")
	}
}

func TestDecodeRTPDepacketizedOpusIntoPreallocatedFrame(t *testing.T) {
	ctx := context.Background()
	decoder, result := newTestDecoder(t)
	depacketizer := rtpav.NewOpusDepacketizer(opusTestStream())
	depacketized := rtpav.DepacketizeResult{Packets: make([]av.Packet, 0, 1)}
	rtpPacket := rtp.Packet{
		Header:  rtp.Header{Timestamp: 960},
		Payload: testCELTPacket(),
	}

	if err := depacketizer.PushInto(ctx, &rtpPacket, rtpav.PayloadCodec{ClockRate: 48000}, &depacketized); err != nil {
		t.Fatal(err)
	}
	if err := decoder.DecodeInto(ctx, &depacketized.Packets[0], &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Frames) != 1 || result.Frames[0].Audio == nil {
		t.Fatalf("frames = %+v", result.Frames)
	}
	if result.Frames[0].Audio.Samples != 960 {
		t.Fatalf("samples = %d, want 960", result.Frames[0].Audio.Samples)
	}
	if result.Frames[0].PTS.Value != 960 {
		t.Fatalf("pts = %+v", result.Frames[0].PTS)
	}
}

func TestDecodeRequiresFrameCapacity(t *testing.T) {
	ctx := context.Background()
	factory := NewDecoderFactory()
	decoder, err := factory.NewDecoder(ctx, codec.DecodeConfig{Stream: opusTestStream()})
	if err != nil {
		t.Fatal(err)
	}
	event := av.Event{Type: av.EventPacketLoss}
	if err := decoder.HandleEvent(ctx, &event); err != nil {
		t.Fatal(err)
	}
	result := codec.DecodeResult{}
	if err := decoder.DecodeInto(ctx, nil, &result); !errors.Is(err, codec.ErrResultFull) {
		t.Fatalf("err = %v, want ErrResultFull", err)
	}
}

func TestDecodeRequiresPlaneBufferCapacity(t *testing.T) {
	ctx := context.Background()
	factory := NewDecoderFactory()
	decoder, err := factory.NewDecoder(ctx, codec.DecodeConfig{Stream: opusTestStream()})
	if err != nil {
		t.Fatal(err)
	}
	event := av.Event{Type: av.EventPacketLoss}
	if err := decoder.HandleEvent(ctx, &event); err != nil {
		t.Fatal(err)
	}
	result := codec.DecodeResult{Frames: make([]av.Frame, 0, 1)}
	if err := decoder.DecodeInto(ctx, nil, &result); !errors.Is(err, codec.ErrOutputBufferTooSmall) {
		t.Fatalf("err = %v, want ErrOutputBufferTooSmall", err)
	}
}

func TestDecodePacketLossConcealmentAllocs(t *testing.T) {
	ctx := context.Background()
	decoder, result := newTestDecoder(t)
	event := av.Event{Type: av.EventPacketLoss}

	if allocs := testing.AllocsPerRun(1000, func() {
		result.Reset()
		if err := decoder.HandleEvent(ctx, &event); err != nil {
			t.Fatal(err)
		}
		if err := decoder.DecodeInto(ctx, nil, &result); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("decode allocs = %v, want 0", allocs)
	}
}

func newTestDecoder(t *testing.T) (codec.Decoder, codec.DecodeResult) {
	t.Helper()
	ctx := context.Background()
	factory := NewDecoderFactory()
	decoder, err := factory.NewDecoder(ctx, codec.DecodeConfig{Stream: opusTestStream()})
	if err != nil {
		t.Fatal(err)
	}

	frames := make([]av.Frame, 1)
	frames[0].Planes = make([]av.Plane, 1)
	frames[0].Planes[0].Buffer.Bytes = make([]byte, 5760*2)
	result := codec.DecodeResult{Frames: frames}
	result.Reset()
	return decoder, result
}

func opusTestStream() av.Stream {
	return av.Stream{
		ID:    "audio",
		Type:  av.MediaAudio,
		Epoch: 1,
		Codec: av.CodecParameters{
			ID:           av.CodecOpus,
			Type:         av.MediaAudio,
			SampleRate:   48000,
			ClockRate:    48000,
			Channels:     1,
			SampleFormat: av.SampleFormatS16,
		},
	}
}

func opusEncodeConfig() codec.EncodeConfig {
	return codec.EncodeConfig{
		Stream: av.Stream{
			ID:       "encoded-audio",
			Epoch:    7,
			Type:     av.MediaAudio,
			TimeBase: av.TimeBase{Num: 1, Den: 48000},
			Codec: av.CodecParameters{
				ID:           av.CodecOpus,
				Type:         av.MediaAudio,
				SampleRate:   48000,
				ClockRate:    48000,
				Channels:     1,
				SampleFormat: av.SampleFormatS16,
			},
		},
		Parameters: av.CodecParameters{
			ID:           av.CodecOpus,
			Type:         av.MediaAudio,
			SampleRate:   48000,
			ClockRate:    48000,
			Channels:     1,
			SampleFormat: av.SampleFormatS16,
		},
		Bitrate:  64_000,
		Realtime: true,
	}
}

func opusEncodeResult(capacity int, payloadCapacity int) codec.EncodeResult {
	packets := make([]av.Packet, capacity)
	for i := range packets {
		packets[i].Payload.Bytes = make([]byte, 0, payloadCapacity)
	}
	result := codec.EncodeResult{
		Packets: packets,
		Events:  make([]av.Event, 0, 1),
	}
	result.Reset()
	return result
}

func opusTestFrame() av.Frame {
	const samples = 960
	pcm := make([]byte, samples*2)
	for i := 0; i < samples; i++ {
		v := int16((i%64 - 32) * 320)
		binary.LittleEndian.PutUint16(pcm[i*2:], uint16(v))
	}
	return av.Frame{
		StreamID:   "decoded-audio",
		CodecEpoch: 6,
		Type:       av.MediaAudio,
		PTS:        av.Timestamp{Value: 960, Base: av.TimeBase{Num: 1, Den: 48000}},
		Duration:   av.SamplesDuration(samples, 48000),
		Audio: &av.AudioFrame{
			SampleRate:   48000,
			Channels:     1,
			SampleFormat: av.SampleFormatS16,
			Samples:      samples,
		},
		Planes: []av.Plane{
			{Buffer: av.Buffer{Bytes: pcm, Ownership: av.BufferOwned}, Stride: 2},
		},
	}
}

func testCELTPacket() []byte {
	data := make([]byte, 50)
	data[0] = 0xf8
	for i := 1; i < len(data); i++ {
		data[i] = byte(i * 7)
	}
	return data
}
