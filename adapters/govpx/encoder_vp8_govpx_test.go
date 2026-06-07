package govpx

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	govpxlib "github.com/thesyncim/govpx"
)

func TestEncodeVP8IntoCallerOwnedPacket(t *testing.T) {
	ctx := context.Background()
	encoder, err := NewVP8EncoderFactory().NewEncoder(ctx, vp8EncodeConfig())
	if err != nil {
		t.Fatal(err)
	}
	result := vp8EncodeResult(1, 64*1024)
	frame := vp8TestFrame()

	if err := encoder.EncodeInto(ctx, &frame, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Packets) != 1 {
		t.Fatalf("packets = %d, want 1", len(result.Packets))
	}
	packet := result.Packets[0]
	if packet.StreamID != "encoded" || packet.CodecEpoch != 4 {
		t.Fatalf("packet identity = %+v", packet)
	}
	if len(packet.Payload.Bytes) == 0 || packet.Payload.Ownership != av.BufferOwned {
		t.Fatalf("payload = %+v", packet.Payload)
	}
	if !packet.Keyframe {
		t.Fatal("first encoded packet is not keyframe")
	}
	if packet.PTS != frame.PTS || packet.Duration != frame.Duration {
		t.Fatalf("timing packet=%+v frame=%+v", packet, frame)
	}
	info, err := govpxlib.PeekVP8StreamInfo(packet.Payload.Bytes)
	if err != nil {
		t.Fatalf("PeekVP8StreamInfo: %v", err)
	}
	if !info.KeyFrame || !info.ShowFrame || info.Width != 16 || info.Height != 16 {
		t.Fatalf("stream info = %+v", info)
	}
}

func TestEncodeVP8RequiresPacketCapacity(t *testing.T) {
	ctx := context.Background()
	encoder, err := NewVP8EncoderFactory().NewEncoder(ctx, vp8EncodeConfig())
	if err != nil {
		t.Fatal(err)
	}
	frame := vp8TestFrame()

	if err := encoder.EncodeInto(ctx, &frame, &codec.EncodeResult{}); !errors.Is(err, codec.ErrResultFull) {
		t.Fatalf("err = %v, want ErrResultFull", err)
	}
}

func TestEncodeVP8RequiresPayloadCapacity(t *testing.T) {
	ctx := context.Background()
	encoder, err := NewVP8EncoderFactory().NewEncoder(ctx, vp8EncodeConfig())
	if err != nil {
		t.Fatal(err)
	}
	result := vp8EncodeResult(1, 0)
	frame := vp8TestFrame()

	if err := encoder.EncodeInto(ctx, &frame, &result); !errors.Is(err, codec.ErrOutputBufferTooSmall) {
		t.Fatalf("err = %v, want ErrOutputBufferTooSmall", err)
	}
}

func TestEncodeVP8RejectsWrongGeometry(t *testing.T) {
	ctx := context.Background()
	encoder, err := NewVP8EncoderFactory().NewEncoder(ctx, vp8EncodeConfig())
	if err != nil {
		t.Fatal(err)
	}
	result := vp8EncodeResult(1, 64*1024)
	frame := vp8TestFrame()
	frame.Video.Width = 32

	if err := encoder.EncodeInto(ctx, &frame, &result); !errors.Is(err, codec.ErrUnsupportedFormat) {
		t.Fatalf("err = %v, want ErrUnsupportedFormat", err)
	}
}

func TestVP8EncoderKeyframeRequiredForcesNextPacket(t *testing.T) {
	ctx := context.Background()
	encoder, err := NewVP8EncoderFactory().NewEncoder(ctx, vp8EncodeConfig())
	if err != nil {
		t.Fatal(err)
	}
	result := vp8EncodeResult(1, 64*1024)
	frame := vp8TestFrame()
	if err := encoder.EncodeInto(ctx, &frame, &result); err != nil {
		t.Fatal(err)
	}

	result.Reset()
	event := av.Event{Type: av.EventKeyframeRequired, StreamID: "encoded"}
	if err := encoder.HandleEvent(ctx, &event); err != nil {
		t.Fatal(err)
	}
	frame.PTS.Value += 3000
	if err := encoder.EncodeInto(ctx, &frame, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Packets) != 1 {
		t.Fatalf("packets = %d, want 1", len(result.Packets))
	}
	info, err := govpxlib.PeekVP8StreamInfo(result.Packets[0].Payload.Bytes)
	if err != nil {
		t.Fatalf("PeekVP8StreamInfo: %v", err)
	}
	if !info.KeyFrame {
		t.Fatalf("forced packet info = %+v, want keyframe", info)
	}
}

func TestVP8EncodeFrameMappingAllocs(t *testing.T) {
	frame := vp8TestFrame()

	if allocs := testing.AllocsPerRun(1000, func() {
		if _, err := govpxImageFromFrame(&frame, 16, 16); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("frame mapping allocs = %v, want 0", allocs)
	}
}

func TestVP8EncoderCloseIsIdempotentAndStopsUse(t *testing.T) {
	ctx := context.Background()
	encoder, err := NewVP8EncoderFactory().NewEncoder(ctx, vp8EncodeConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}

	result := vp8EncodeResult(1, 64*1024)
	frame := vp8TestFrame()
	event := av.Event{Type: av.EventKeyframeRequired, StreamID: "encoded"}
	if err := encoder.EncodeInto(ctx, &frame, &result); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("encode after close = %v, want ErrClosed", err)
	}
	if err := encoder.FlushInto(ctx, &result); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("flush after close = %v, want ErrClosed", err)
	}
	if err := encoder.HandleEvent(ctx, &event); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("event after close = %v, want ErrClosed", err)
	}
}

func vp8EncodeConfig() codec.EncodeConfig {
	return codec.EncodeConfig{
		Stream: av.Stream{
			ID:       "encoded",
			Epoch:    4,
			Type:     av.MediaVideo,
			TimeBase: av.RTPTimeBase(90000),
			Codec: av.CodecParameters{
				ID:          av.CodecVP8,
				Type:        av.MediaVideo,
				ClockRate:   90000,
				Width:       16,
				Height:      16,
				PixelFormat: av.PixelFormatI420,
			},
		},
		Parameters: av.CodecParameters{
			ID:          av.CodecVP8,
			Type:        av.MediaVideo,
			ClockRate:   90000,
			Width:       16,
			Height:      16,
			PixelFormat: av.PixelFormatI420,
		},
		Bitrate:   300,
		Framerate: av.Duration{Value: 3000, Base: av.RTPTimeBase(90000)},
		Realtime:  true,
	}
}

func vp8EncodeResult(capacity int, payloadCapacity int) codec.EncodeResult {
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

func vp8TestFrame() av.Frame {
	img := patternedVP8Image(16, 16)
	return av.Frame{
		StreamID:   "decoded",
		CodecEpoch: 2,
		Type:       av.MediaVideo,
		PTS:        av.Timestamp{Value: 90, Base: av.RTPTimeBase(90000)},
		Duration:   av.Duration{Value: 3000, Base: av.RTPTimeBase(90000)},
		Video: &av.VideoFrame{
			Width:       img.Width,
			Height:      img.Height,
			PixelFormat: av.PixelFormatI420,
		},
		Planes: []av.Plane{
			{Buffer: av.Buffer{Bytes: img.Y, Ownership: av.BufferOwned}, Stride: img.YStride},
			{Buffer: av.Buffer{Bytes: img.U, Ownership: av.BufferOwned}, Stride: img.UStride},
			{Buffer: av.Buffer{Bytes: img.V, Ownership: av.BufferOwned}, Stride: img.VStride},
		},
	}
}
