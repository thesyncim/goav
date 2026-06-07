package govpx

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	govpxlib "github.com/thesyncim/govpx"
)

func TestRegisterProvidesVP8AndVP9Factories(t *testing.T) {
	registry := codec.NewRegistry()
	Register(registry)

	if _, err := registry.Find(av.CodecVP8, codec.ModeDecode); err != nil {
		t.Fatalf("find VP8: %v", err)
	}
	if _, err := registry.DecoderFactory(av.CodecVP8); err != nil {
		t.Fatalf("VP8 decoder factory: %v", err)
	}
	if _, err := registry.Find(av.CodecVP9, codec.ModeDecode); err != nil {
		t.Fatalf("find VP9: %v", err)
	}
	if _, err := registry.DecoderFactory(av.CodecVP9); err != nil {
		t.Fatalf("VP9 decoder factory: %v", err)
	}
	if _, err := registry.EncoderFactory(av.CodecVP8); err != nil {
		t.Fatalf("VP8 encoder factory: %v", err)
	}
	if _, err := registry.EncoderFactory(av.CodecVP9); err != nil {
		t.Fatalf("VP9 encoder factory: %v", err)
	}
	if descriptors := registry.Descriptors(); len(descriptors) != 2 {
		t.Fatalf("descriptors = %d, want merged VP8 and VP9 descriptors", len(descriptors))
	}
}

func TestDecodeVP8IntoCallerOwnedI420Frame(t *testing.T) {
	ctx := context.Background()
	decoder, err := NewDecoderFactory().NewDecoder(ctx, codec.DecodeConfig{
		Stream: vp8TestStream(),
		Resilience: codec.ResiliencePolicy{
			AcceptLoss:       true,
			DropDamagedVideo: true,
			RequestKeyframes: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded := encodedVP8TestFrame(t)
	result := vp8DecodeResult(1, 16, 16)
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
	if frame.Video == nil || frame.Video.Width != 16 || frame.Video.Height != 16 || frame.Video.PixelFormat != av.PixelFormatI420 {
		t.Fatalf("video = %+v", frame.Video)
	}
	if frame.PTS.Value != 90 || frame.PTS.Base != av.RTPTimeBase(90000) {
		t.Fatalf("pts = %+v", frame.PTS)
	}
	if frame.Duration.Value != 3000 || frame.Duration.Base != av.RTPTimeBase(90000) {
		t.Fatalf("duration = %+v", frame.Duration)
	}
	wantLengths := [...]int{256, 64, 64}
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

func TestDecodeRequiresFrameCapacity(t *testing.T) {
	ctx := context.Background()
	decoder, err := NewDecoderFactory().NewDecoder(ctx, codec.DecodeConfig{Stream: vp8TestStream()})
	if err != nil {
		t.Fatal(err)
	}
	packet := av.Packet{StreamID: "video", Payload: av.Buffer{Bytes: encodedVP8TestFrame(t)}, Keyframe: true}

	if err := decoder.DecodeInto(ctx, &packet, &codec.DecodeResult{}); !errors.Is(err, codec.ErrResultFull) {
		t.Fatalf("err = %v, want ErrResultFull", err)
	}
}

func TestDecodeRequiresPlaneCapacity(t *testing.T) {
	ctx := context.Background()
	decoder, err := NewDecoderFactory().NewDecoder(ctx, codec.DecodeConfig{Stream: vp8TestStream()})
	if err != nil {
		t.Fatal(err)
	}
	result := codec.DecodeResult{Frames: make([]av.Frame, 1)}
	result.Reset()
	packet := av.Packet{StreamID: "video", Payload: av.Buffer{Bytes: encodedVP8TestFrame(t)}, Keyframe: true}

	if err := decoder.DecodeInto(ctx, &packet, &result); !errors.Is(err, codec.ErrOutputBufferTooSmall) {
		t.Fatalf("err = %v, want ErrOutputBufferTooSmall", err)
	}
}

func TestPacketLossRequestsKeyframe(t *testing.T) {
	ctx := context.Background()
	decoder, err := NewDecoderFactory().NewDecoder(ctx, codec.DecodeConfig{
		Stream: vp8TestStream(),
		Resilience: codec.ResiliencePolicy{
			RequestKeyframes: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := vp8DecodeResult(1, 16, 16)
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

func TestInterFrameBeforeKeyframeRequestsKeyframe(t *testing.T) {
	ctx := context.Background()
	decoder, err := NewDecoderFactory().NewDecoder(ctx, codec.DecodeConfig{
		Stream: vp8TestStream(),
		Resilience: codec.ResiliencePolicy{
			RequestKeyframes: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded := encodedVP8TestFrame(t)
	encoded[0] |= 1
	result := vp8DecodeResult(1, 16, 16)
	packet := av.Packet{StreamID: "video", Payload: av.Buffer{Bytes: encoded}}

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

func TestPacketLossInterFrameRequestsOneKeyframe(t *testing.T) {
	ctx := context.Background()
	decoder, err := NewDecoderFactory().NewDecoder(ctx, codec.DecodeConfig{
		Stream: vp8TestStream(),
		Resilience: codec.ResiliencePolicy{
			RequestKeyframes: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded := encodedVP8TestFrame(t)
	encoded[0] |= 1
	result := vp8DecodeResult(1, 16, 16)
	packet := av.Packet{StreamID: "video", Payload: av.Buffer{Bytes: encoded}, LossBefore: true}

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

func TestPrepareI420FrameAllocs(t *testing.T) {
	frame := av.Frame{
		Planes: []av.Plane{
			{Buffer: av.Buffer{Bytes: make([]byte, 0, 256)}},
			{Buffer: av.Buffer{Bytes: make([]byte, 0, 64)}},
			{Buffer: av.Buffer{Bytes: make([]byte, 0, 64)}},
		},
	}

	if allocs := testing.AllocsPerRun(1000, func() {
		frame.Reset()
		if err := prepareI420Frame(&frame, 16, 16); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("prepare I420 frame allocs = %v, want 0", allocs)
	}
}

func TestPacketLossKeyframeRequestAllocs(t *testing.T) {
	ctx := context.Background()
	decoder, err := NewDecoderFactory().NewDecoder(ctx, codec.DecodeConfig{
		Stream: vp8TestStream(),
		Resilience: codec.ResiliencePolicy{
			RequestKeyframes: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	event := av.Event{Type: av.EventPacketLoss, StreamID: "video"}
	result := vp8DecodeResult(1, 16, 16)

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

func TestCodecChangedUpdatesStreamIdentity(t *testing.T) {
	ctx := context.Background()
	decoder, err := NewDecoderFactory().NewDecoder(ctx, codec.DecodeConfig{Stream: vp8TestStream()})
	if err != nil {
		t.Fatal(err)
	}
	codecParams := av.CodecParameters{
		ID:          av.CodecVP8,
		Type:        av.MediaVideo,
		ClockRate:   90000,
		Width:       16,
		Height:      16,
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
	result := vp8DecodeResult(1, 16, 16)
	packet := av.Packet{Payload: av.Buffer{Bytes: encodedVP8TestFrame(t)}, Keyframe: true}

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

func TestDecoderCloseIsIdempotentAndStopsUse(t *testing.T) {
	ctx := context.Background()
	decoder, err := NewDecoderFactory().NewDecoder(ctx, codec.DecodeConfig{Stream: vp8TestStream()})
	if err != nil {
		t.Fatal(err)
	}
	if err := decoder.Close(); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}

	result := vp8DecodeResult(1, 16, 16)
	packet := av.Packet{StreamID: "video", Payload: av.Buffer{Bytes: encodedVP8TestFrame(t)}, Keyframe: true}
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

func vp8DecodeResult(capacity int, width int, height int) codec.DecodeResult {
	frames := make([]av.Frame, capacity)
	chromaWidth := (width + 1) / 2
	chromaHeight := (height + 1) / 2
	for i := range frames {
		frames[i].Planes = []av.Plane{
			{Buffer: av.Buffer{Bytes: make([]byte, 0, width*height)}},
			{Buffer: av.Buffer{Bytes: make([]byte, 0, chromaWidth*chromaHeight)}},
			{Buffer: av.Buffer{Bytes: make([]byte, 0, chromaWidth*chromaHeight)}},
		}
	}
	result := codec.DecodeResult{
		Frames:   frames,
		Requests: make([]codec.ControlRequest, 0, 1),
	}
	result.Reset()
	return result
}

func vp8TestStream() av.Stream {
	return av.Stream{
		ID:       "video",
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
	}
}

func encodedVP8TestFrame(t *testing.T) []byte {
	t.Helper()
	encoder, err := govpxlib.NewVP8Encoder(govpxlib.EncoderOptions{
		Width:             16,
		Height:            16,
		FPS:               30,
		TargetBitrateKbps: 200,
	})
	if err != nil {
		t.Fatalf("NewVP8Encoder: %v", err)
	}
	src := patternedVP8Image(16, 16)
	result, err := encoder.EncodeInto(make([]byte, 4096), src, 90, 3000, 0)
	if err != nil {
		t.Fatalf("EncodeInto: %v", err)
	}
	if len(result.Data) == 0 || !result.KeyFrame {
		t.Fatalf("encoded result = %+v", result)
	}
	return append([]byte(nil), result.Data...)
}

func patternedVP8Image(width, height int) govpxlib.Image {
	chromaWidth := (width + 1) / 2
	chromaHeight := (height + 1) / 2
	img := govpxlib.Image{
		Width:   width,
		Height:  height,
		Y:       make([]byte, width*height),
		U:       make([]byte, chromaWidth*chromaHeight),
		V:       make([]byte, chromaWidth*chromaHeight),
		YStride: width,
		UStride: chromaWidth,
		VStride: chromaWidth,
	}
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			img.Y[y*img.YStride+x] = byte((x*11 + y*17 + 3) & 0xff)
		}
	}
	for y := 0; y < chromaHeight; y++ {
		for x := 0; x < chromaWidth; x++ {
			img.U[y*img.UStride+x] = byte((x*19 + y*7 + 41) & 0xff)
			img.V[y*img.VStride+x] = byte((x*5 + y*23 + 109) & 0xff)
		}
	}
	return img
}

func allZero(buf []byte) bool {
	for i := range buf {
		if buf[i] != 0 {
			return false
		}
	}
	return true
}
