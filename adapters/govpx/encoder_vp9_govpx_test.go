//go:build goav_govpx

package govpx

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	govpxlib "github.com/thesyncim/govpx"
)

func TestEncodeVP9IntoCallerOwnedPacket(t *testing.T) {
	ctx := context.Background()
	encoder, err := NewVP9EncoderFactory().NewEncoder(ctx, vp9EncodeConfig())
	if err != nil {
		t.Fatal(err)
	}
	result := vp9EncodeResult(1, 256*1024)
	frame := vp9TestFrame()

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
	info, err := govpxlib.PeekVP9StreamInfo(packet.Payload.Bytes)
	if err != nil {
		t.Fatalf("PeekVP9StreamInfo: %v", err)
	}
	if !info.KeyFrame || !info.ShowFrame || info.Width != 64 || info.Height != 64 {
		t.Fatalf("stream info = %+v", info)
	}
}

func TestEncodeVP9RequiresPacketCapacity(t *testing.T) {
	ctx := context.Background()
	encoder, err := NewVP9EncoderFactory().NewEncoder(ctx, vp9EncodeConfig())
	if err != nil {
		t.Fatal(err)
	}
	frame := vp9TestFrame()

	if err := encoder.EncodeInto(ctx, &frame, &codec.EncodeResult{}); !errors.Is(err, codec.ErrResultFull) {
		t.Fatalf("err = %v, want ErrResultFull", err)
	}
}

func TestEncodeVP9RequiresPayloadCapacity(t *testing.T) {
	ctx := context.Background()
	encoder, err := NewVP9EncoderFactory().NewEncoder(ctx, vp9EncodeConfig())
	if err != nil {
		t.Fatal(err)
	}
	result := vp9EncodeResult(1, 0)
	frame := vp9TestFrame()

	if err := encoder.EncodeInto(ctx, &frame, &result); !errors.Is(err, codec.ErrOutputBufferTooSmall) {
		t.Fatalf("err = %v, want ErrOutputBufferTooSmall", err)
	}
}

func TestEncodeVP9RejectsWrongGeometry(t *testing.T) {
	ctx := context.Background()
	encoder, err := NewVP9EncoderFactory().NewEncoder(ctx, vp9EncodeConfig())
	if err != nil {
		t.Fatal(err)
	}
	result := vp9EncodeResult(1, 256*1024)
	frame := vp9TestFrame()
	frame.Video.Width = 32

	if err := encoder.EncodeInto(ctx, &frame, &result); !errors.Is(err, codec.ErrUnsupportedFormat) {
		t.Fatalf("err = %v, want ErrUnsupportedFormat", err)
	}
}

func TestVP9EncoderKeyframeRequiredForcesNextPacket(t *testing.T) {
	ctx := context.Background()
	encoder, err := NewVP9EncoderFactory().NewEncoder(ctx, vp9EncodeConfig())
	if err != nil {
		t.Fatal(err)
	}
	result := vp9EncodeResult(1, 256*1024)
	frame := vp9TestFrame()
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
	info, err := govpxlib.PeekVP9StreamInfo(result.Packets[0].Payload.Bytes)
	if err != nil {
		t.Fatalf("PeekVP9StreamInfo: %v", err)
	}
	if !info.KeyFrame {
		t.Fatalf("forced packet info = %+v, want keyframe", info)
	}
}

func TestVP9EncodeFrameMappingAllocs(t *testing.T) {
	frame := vp9TestFrame()

	if allocs := testing.AllocsPerRun(1000, func() {
		if _, err := govpxYCbCrFromFrame(&frame, 64, 64); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("frame mapping allocs = %v, want 0", allocs)
	}
}

func TestVP9EncoderCloseIsIdempotentAndStopsUse(t *testing.T) {
	ctx := context.Background()
	encoder, err := NewVP9EncoderFactory().NewEncoder(ctx, vp9EncodeConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}

	result := vp9EncodeResult(1, 256*1024)
	frame := vp9TestFrame()
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

func TestVP9EncoderOptionsUseCommonAndNativeConfig(t *testing.T) {
	config := vp9EncodeConfig()
	config.Bitrate = 1_200_000
	config.Framerate = av.Duration{Value: 4500, Base: av.RTPTimeBase(90000)}
	config.KeyframeInterval = 90
	config.Config = govpxlib.VP9EncoderOptions{
		Threads:             2,
		MaxKeyframeInterval: 128,
	}

	options, err := vp9EncoderOptions(config, 64, 64)
	if err != nil {
		t.Fatal(err)
	}
	if options.Width != 64 || options.Height != 64 {
		t.Fatalf("size = %dx%d", options.Width, options.Height)
	}
	if options.FPS != 20 {
		t.Fatalf("fps = %d, want 20", options.FPS)
	}
	if options.TargetBitrateKbps != 1200 {
		t.Fatalf("bitrate = %d, want 1200", options.TargetBitrateKbps)
	}
	if options.MaxKeyframeInterval != 90 {
		t.Fatalf("max keyframe interval = %d, want 90", options.MaxKeyframeInterval)
	}
	if options.Threads != 2 {
		t.Fatalf("threads = %d, want 2", options.Threads)
	}
	if !options.RateControlModeSet || options.RateControlMode != govpxlib.RateControlCBR {
		t.Fatalf("rate control = set:%v mode:%v, want CBR", options.RateControlModeSet, options.RateControlMode)
	}
}

func TestVP9EncoderAppliesControls(t *testing.T) {
	ctx := context.Background()
	config := vp9EncodeConfig()
	called := false
	config.Controls = []any{
		func(encoder *govpxlib.VP9Encoder) error {
			called = true
			return encoder.SetKeyFrameInterval(3)
		},
	}

	encoder, err := NewVP9EncoderFactory().NewEncoder(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("control was not applied")
	}
	_ = encoder.Close()
}

func vp9EncodeConfig() codec.EncodeConfig {
	return codec.EncodeConfig{
		Stream: av.Stream{
			ID:       "encoded",
			Epoch:    4,
			Type:     av.MediaVideo,
			TimeBase: av.RTPTimeBase(90000),
			Codec: av.CodecParameters{
				ID:          av.CodecVP9,
				Type:        av.MediaVideo,
				ClockRate:   90000,
				Width:       64,
				Height:      64,
				PixelFormat: av.PixelFormatI420,
			},
		},
		Parameters: av.CodecParameters{
			ID:          av.CodecVP9,
			Type:        av.MediaVideo,
			ClockRate:   90000,
			Width:       64,
			Height:      64,
			PixelFormat: av.PixelFormatI420,
		},
		Bitrate:   600,
		Framerate: av.Duration{Value: 3000, Base: av.RTPTimeBase(90000)},
		Realtime:  true,
	}
}

func vp9EncodeResult(capacity int, payloadCapacity int) codec.EncodeResult {
	return vp8EncodeResult(capacity, payloadCapacity)
}

func vp9TestFrame() av.Frame {
	img := patternedVP9Image(64, 64)
	return av.Frame{
		StreamID:   "decoded",
		CodecEpoch: 2,
		Type:       av.MediaVideo,
		PTS:        av.Timestamp{Value: 90, Base: av.RTPTimeBase(90000)},
		Duration:   av.Duration{Value: 3000, Base: av.RTPTimeBase(90000)},
		Video: &av.VideoFrame{
			Width:       img.Rect.Dx(),
			Height:      img.Rect.Dy(),
			PixelFormat: av.PixelFormatI420,
		},
		Planes: []av.Plane{
			{Buffer: av.Buffer{Bytes: img.Y, Ownership: av.BufferOwned}, Stride: img.YStride},
			{Buffer: av.Buffer{Bytes: img.Cb, Ownership: av.BufferOwned}, Stride: img.CStride},
			{Buffer: av.Buffer{Bytes: img.Cr, Ownership: av.BufferOwned}, Stride: img.CStride},
		},
	}
}
