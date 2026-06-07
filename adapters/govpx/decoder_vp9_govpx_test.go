package govpx

import (
	"context"
	"errors"
	"image"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	govpxlib "github.com/thesyncim/govpx"
)

func TestDecodeVP9IntoCallerOwnedI420Frame(t *testing.T) {
	ctx := context.Background()
	decoder, err := NewVP9DecoderFactory().NewDecoder(ctx, codec.DecodeConfig{
		Stream: vp9TestStream(),
		Resilience: codec.ResiliencePolicy{
			AcceptLoss:       true,
			DropDamagedVideo: true,
			RequestKeyframes: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded := encodedVP9TestFrame(t)
	result := vp9DecodeResult(1, 64, 64)
	packet := av.Packet{
		StreamID:   "video",
		CodecEpoch: 3,
		Payload:    av.Buffer{Bytes: encoded, Ownership: av.BufferImmutable},
		PTS:        av.Timestamp{Value: 90, Base: av.RTPTimeBase(90000)},
		Duration:   av.Duration{Value: 3000, Base: av.RTPTimeBase(90000)},
		Keyframe:   true,
	}

	if err := decoder.DecodeInto(ctx, &packet, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(result.Frames))
	}
	frame := result.Frames[0]
	if frame.StreamID != "video" || frame.CodecEpoch != 3 || frame.Type != av.MediaVideo {
		t.Fatalf("frame identity = %+v", frame)
	}
	if frame.Video == nil || frame.Video.Width != 64 || frame.Video.Height != 64 || frame.Video.PixelFormat != av.PixelFormatI420 {
		t.Fatalf("video = %+v", frame.Video)
	}
	if frame.PTS.Value != 90 || frame.PTS.Base != av.RTPTimeBase(90000) {
		t.Fatalf("pts = %+v", frame.PTS)
	}
	if frame.Duration.Value != 3000 || frame.Duration.Base != av.RTPTimeBase(90000) {
		t.Fatalf("duration = %+v", frame.Duration)
	}
	wantLengths := [...]int{4096, 1024, 1024}
	for i := range frame.Planes {
		if frame.Planes[i].Buffer.Ownership != av.BufferOwned {
			t.Fatalf("plane[%d] ownership = %q, want owned", i, frame.Planes[i].Buffer.Ownership)
		}
		if len(frame.Planes[i].Buffer.Bytes) != wantLengths[i] {
			t.Fatalf("plane[%d] len = %d, want %d", i, len(frame.Planes[i].Buffer.Bytes), wantLengths[i])
		}
		if len(frame.Planes[i].Buffer.Bytes) == 0 || allZero(frame.Planes[i].Buffer.Bytes) {
			t.Fatalf("plane[%d] is empty", i)
		}
	}
}

func TestDecodeVP9RequiresFrameCapacity(t *testing.T) {
	ctx := context.Background()
	decoder, err := NewVP9DecoderFactory().NewDecoder(ctx, codec.DecodeConfig{Stream: vp9TestStream()})
	if err != nil {
		t.Fatal(err)
	}
	packet := av.Packet{StreamID: "video", Payload: av.Buffer{Bytes: encodedVP9TestFrame(t)}, Keyframe: true}

	if err := decoder.DecodeInto(ctx, &packet, &codec.DecodeResult{}); !errors.Is(err, codec.ErrResultFull) {
		t.Fatalf("err = %v, want ErrResultFull", err)
	}
}

func TestDecodeVP9RequiresPlaneCapacity(t *testing.T) {
	ctx := context.Background()
	decoder, err := NewVP9DecoderFactory().NewDecoder(ctx, codec.DecodeConfig{Stream: vp9TestStream()})
	if err != nil {
		t.Fatal(err)
	}
	result := codec.DecodeResult{Frames: make([]av.Frame, 1)}
	result.Reset()
	packet := av.Packet{StreamID: "video", Payload: av.Buffer{Bytes: encodedVP9TestFrame(t)}, Keyframe: true}

	if err := decoder.DecodeInto(ctx, &packet, &result); !errors.Is(err, codec.ErrOutputBufferTooSmall) {
		t.Fatalf("err = %v, want ErrOutputBufferTooSmall", err)
	}
}

func TestVP9PacketLossRequestsKeyframe(t *testing.T) {
	ctx := context.Background()
	decoder, err := NewVP9DecoderFactory().NewDecoder(ctx, codec.DecodeConfig{
		Stream: vp9TestStream(),
		Resilience: codec.ResiliencePolicy{
			RequestKeyframes: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := vp9DecodeResult(1, 64, 64)
	event := av.Event{Type: av.EventPacketLoss, StreamID: "video"}

	if err := decoder.HandleEvent(ctx, &event); err != nil {
		t.Fatal(err)
	}
	if err := decoder.DecodeInto(ctx, nil, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Requests) != 1 || result.Requests[0].Type != codec.ControlRequestKeyframe || result.Requests[0].StreamID != "video" {
		t.Fatalf("requests = %+v", result.Requests)
	}
}

func TestVP9InterFrameBeforeKeyframeRequestsKeyframe(t *testing.T) {
	ctx := context.Background()
	decoder, err := NewVP9DecoderFactory().NewDecoder(ctx, codec.DecodeConfig{
		Stream: vp9TestStream(),
		Resilience: codec.ResiliencePolicy{
			RequestKeyframes: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, inter := encodedVP9KeyAndInterFrames(t)
	result := vp9DecodeResult(1, 64, 64)
	packet := av.Packet{StreamID: "video", Payload: av.Buffer{Bytes: inter}}

	if err := decoder.DecodeInto(ctx, &packet, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Frames) != 0 {
		t.Fatalf("frames = %d, want 0", len(result.Frames))
	}
	if len(result.Requests) != 1 || result.Requests[0].Type != codec.ControlRequestKeyframe {
		t.Fatalf("requests = %+v", result.Requests)
	}
}

func TestVP9CodecChangedUpdatesStreamIdentity(t *testing.T) {
	ctx := context.Background()
	decoder, err := NewVP9DecoderFactory().NewDecoder(ctx, codec.DecodeConfig{Stream: vp9TestStream()})
	if err != nil {
		t.Fatal(err)
	}
	codecParams := av.CodecParameters{
		ID:          av.CodecVP9,
		Type:        av.MediaVideo,
		ClockRate:   90000,
		Width:       64,
		Height:      64,
		PixelFormat: av.PixelFormatI420,
	}
	event := av.Event{
		Type:     av.EventCodecChanged,
		StreamID: "replacement",
		Epoch:    7,
		Codec:    &codecParams,
	}
	if err := decoder.HandleEvent(ctx, &event); err != nil {
		t.Fatal(err)
	}
	result := vp9DecodeResult(1, 64, 64)
	packet := av.Packet{Payload: av.Buffer{Bytes: encodedVP9TestFrame(t)}, Keyframe: true}

	if err := decoder.DecodeInto(ctx, &packet, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(result.Frames))
	}
	frame := result.Frames[0]
	if frame.StreamID != "replacement" || frame.CodecEpoch != 7 {
		t.Fatalf("frame identity = %+v", frame)
	}
}

func TestVP9DecoderCloseIsIdempotentAndStopsUse(t *testing.T) {
	ctx := context.Background()
	decoder, err := NewVP9DecoderFactory().NewDecoder(ctx, codec.DecodeConfig{Stream: vp9TestStream()})
	if err != nil {
		t.Fatal(err)
	}
	if err := decoder.Close(); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}

	result := vp9DecodeResult(1, 64, 64)
	packet := av.Packet{StreamID: "video", Payload: av.Buffer{Bytes: encodedVP9TestFrame(t)}, Keyframe: true}
	event := av.Event{Type: av.EventCodecChanged, StreamID: "video"}
	if err := decoder.DecodeInto(ctx, &packet, &result); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("decode after close = %v, want ErrClosed", err)
	}
	if err := decoder.FlushInto(ctx, &result); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("flush after close = %v, want ErrClosed", err)
	}
	if err := decoder.HandleEvent(ctx, &event); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("event after close = %v, want ErrClosed", err)
	}
}

func TestVP9PacketLossKeyframeRequestAllocs(t *testing.T) {
	ctx := context.Background()
	decoder, err := NewVP9DecoderFactory().NewDecoder(ctx, codec.DecodeConfig{
		Stream: vp9TestStream(),
		Resilience: codec.ResiliencePolicy{
			RequestKeyframes: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	event := av.Event{Type: av.EventPacketLoss, StreamID: "video"}
	result := vp9DecodeResult(1, 64, 64)

	if allocs := testing.AllocsPerRun(1000, func() {
		result.Reset()
		if err := decoder.HandleEvent(ctx, &event); err != nil {
			t.Fatal(err)
		}
		if err := decoder.DecodeInto(ctx, nil, &result); err != nil {
			t.Fatal(err)
		}
		if len(result.Requests) != 1 {
			t.Fatalf("requests = %d, want 1", len(result.Requests))
		}
	}); allocs != 0 {
		t.Fatalf("keyframe request allocs = %v, want 0", allocs)
	}
}

func vp9DecodeResult(capacity int, width int, height int) codec.DecodeResult {
	return vp8DecodeResult(capacity, width, height)
}

func vp9TestStream() av.Stream {
	return av.Stream{
		ID:       "video",
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
	}
}

func encodedVP9TestFrame(t *testing.T) []byte {
	t.Helper()
	key, _ := encodedVP9KeyAndInterFrames(t)
	return key
}

func encodedVP9KeyAndInterFrames(t *testing.T) ([]byte, []byte) {
	t.Helper()
	encoder, err := govpxlib.NewVP9Encoder(govpxlib.VP9EncoderOptions{
		Width:  64,
		Height: 64,
		FPS:    30,
	})
	if err != nil {
		t.Fatalf("NewVP9Encoder: %v", err)
	}
	src := patternedVP9Image(64, 64)
	key, err := encoder.Encode(src)
	if err != nil {
		t.Fatalf("Encode VP9 keyframe: %v", err)
	}
	if len(key) == 0 {
		t.Fatal("empty VP9 keyframe")
	}
	inter, err := encoder.Encode(src)
	if err != nil {
		t.Fatalf("Encode VP9 inter frame: %v", err)
	}
	if len(inter) == 0 {
		t.Fatal("empty VP9 inter frame")
	}
	return append([]byte(nil), key...), append([]byte(nil), inter...)
}

func patternedVP9Image(width, height int) *image.YCbCr {
	img := image.NewYCbCr(image.Rect(0, 0, width, height), image.YCbCrSubsampleRatio420)
	chromaWidth := (width + 1) / 2
	chromaHeight := (height + 1) / 2
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			img.Y[y*img.YStride+x] = byte((x*11 + y*17 + 3) & 0xff)
		}
	}
	for y := 0; y < chromaHeight; y++ {
		for x := 0; x < chromaWidth; x++ {
			img.Cb[y*img.CStride+x] = byte((x*19 + y*7 + 41) & 0xff)
			img.Cr[y*img.CStride+x] = byte((x*5 + y*23 + 109) & 0xff)
		}
	}
	return img
}
