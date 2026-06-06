//go:build goav_goh264

package goh264

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	goh264lib "github.com/thesyncim/goh264"
)

func TestRegisterWithBuildTagProvidesFactory(t *testing.T) {
	registry := codec.NewRegistry()
	Register(registry)

	if _, err := registry.Find(av.CodecH264, codec.ModeDecode); err != nil {
		t.Fatalf("find H264: %v", err)
	}
	if _, err := registry.DecoderFactory(av.CodecH264); err != nil {
		t.Fatalf("decoder factory: %v", err)
	}
}

func TestDecodeAnnexBIntoBorrowedVideoFrame(t *testing.T) {
	ctx := context.Background()
	decoder, err := NewDecoderFactory().NewDecoder(ctx, codec.DecodeConfig{
		Stream: h264TestStream(),
		Resilience: codec.ResiliencePolicy{
			DropDamagedVideo: true,
			RequestKeyframes: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded, want := encodedAnnexBTestFrame(t)
	result := h264DecodeResult(1)
	packet := av.Packet{
		StreamID:   "video",
		CodecEpoch: 3,
		Payload:    av.Buffer{Bytes: encoded, Ownership: av.BufferImmutable},
		PTS:        av.Timestamp{Value: 90, Base: av.RTPTimeBase(90000)},
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
	if frame.Video == nil || frame.Video.Width != 16 || frame.Video.Height != 16 || frame.Video.PixelFormat != "yuv420p" {
		t.Fatalf("video = %+v", frame.Video)
	}
	if frame.PTS.Value != 90 || frame.PTS.Base != av.RTPTimeBase(90000) {
		t.Fatalf("pts = %+v", frame.PTS)
	}
	if len(frame.Planes) != 3 {
		t.Fatalf("planes = %d, want 3", len(frame.Planes))
	}
	for i := range frame.Planes {
		if frame.Planes[i].Buffer.Ownership != av.BufferBorrowed || len(frame.Planes[i].Buffer.Bytes) == 0 {
			t.Fatalf("plane[%d] = %+v", i, frame.Planes[i])
		}
	}

	got := appendI420FromFrame(nil, frame)
	if !bytes.Equal(got, want) {
		t.Fatalf("decoded raw md5 = %x, want %x", md5.Sum(got), md5.Sum(want))
	}
}

func TestDecodeRequiresFrameCapacity(t *testing.T) {
	ctx := context.Background()
	decoder, err := NewDecoderFactory().NewDecoder(ctx, codec.DecodeConfig{Stream: h264TestStream()})
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := encodedAnnexBTestFrame(t)
	result := codec.DecodeResult{}
	packet := av.Packet{StreamID: "video", Payload: av.Buffer{Bytes: encoded}, Keyframe: true}

	if err := decoder.DecodeInto(ctx, &packet, &result); !errors.Is(err, codec.ErrResultFull) {
		t.Fatalf("err = %v, want ErrResultFull", err)
	}
}

func TestDecodeRequiresPlaneCapacity(t *testing.T) {
	ctx := context.Background()
	decoder, err := NewDecoderFactory().NewDecoder(ctx, codec.DecodeConfig{Stream: h264TestStream()})
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := encodedAnnexBTestFrame(t)
	result := codec.DecodeResult{Frames: make([]av.Frame, 0, 1)}
	packet := av.Packet{StreamID: "video", Payload: av.Buffer{Bytes: encoded}, Keyframe: true}

	if err := decoder.DecodeInto(ctx, &packet, &result); !errors.Is(err, codec.ErrOutputBufferTooSmall) {
		t.Fatalf("err = %v, want ErrOutputBufferTooSmall", err)
	}
}

func TestPacketLossRequestsKeyframe(t *testing.T) {
	ctx := context.Background()
	decoder, err := NewDecoderFactory().NewDecoder(ctx, codec.DecodeConfig{
		Stream: h264TestStream(),
		Resilience: codec.ResiliencePolicy{
			RequestKeyframes: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := h264DecodeResult(1)
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

func h264DecodeResult(capacity int) codec.DecodeResult {
	frames := make([]av.Frame, capacity)
	for i := range frames {
		frames[i].Planes = make([]av.Plane, 3)
	}
	result := codec.DecodeResult{
		Frames:   frames,
		Requests: make([]codec.ControlRequest, 0, 1),
	}
	result.Reset()
	return result
}

func h264TestStream() av.Stream {
	return av.Stream{
		ID:       "video",
		Type:     av.MediaVideo,
		TimeBase: av.RTPTimeBase(90000),
		Codec: av.CodecParameters{
			ID:          av.CodecH264,
			Type:        av.MediaVideo,
			ClockRate:   90000,
			Width:       16,
			Height:      16,
			PixelFormat: "yuv420p",
		},
	}
}

func encodedAnnexBTestFrame(t *testing.T) ([]byte, []byte) {
	t.Helper()
	cfg := goh264lib.DefaultEncoderConfig(16, 16)
	cfg.OutputFormat = goh264lib.EncoderOutputAnnexB
	cfg.DeblockMode = goh264lib.EncoderDeblockDisabled
	cfg.FrameDrop = goh264lib.EncoderFrameDropDisabled
	cfg.RTPMaxPayloadSize = 0
	encoder, err := goh264lib.NewEncoder(cfg)
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}
	frame := patternedI420EncoderFrame(16, 16)
	encoded, err := encoder.Encode(frame)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded.Data) == 0 {
		t.Fatal("empty encoded data")
	}
	return encoded.Data, appendI420EncoderFrame(nil, frame)
}

func patternedI420EncoderFrame(width, height int) goh264lib.EncoderFrame {
	chromaWidth := width / 2
	chromaHeight := height / 2
	frame := goh264lib.EncoderFrame{
		Y:        make([]byte, width*height),
		Cb:       make([]byte, chromaWidth*chromaHeight),
		Cr:       make([]byte, chromaWidth*chromaHeight),
		StrideY:  width,
		StrideCb: chromaWidth,
		StrideCr: chromaWidth,
		Width:    width,
		Height:   height,
		PTS:      3000,
		Duration: 3000,
	}
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			frame.Y[y*frame.StrideY+x] = byte((x*11 + y*17 + 3) & 0xff)
		}
	}
	for y := 0; y < chromaHeight; y++ {
		for x := 0; x < chromaWidth; x++ {
			frame.Cb[y*frame.StrideCb+x] = byte((x*19 + y*7 + 41) & 0xff)
			frame.Cr[y*frame.StrideCr+x] = byte((x*5 + y*23 + 109) & 0xff)
		}
	}
	return frame
}

func appendI420EncoderFrame(dst []byte, frame goh264lib.EncoderFrame) []byte {
	for y := 0; y < frame.Height; y++ {
		dst = append(dst, frame.Y[y*frame.StrideY:y*frame.StrideY+frame.Width]...)
	}
	chromaWidth := frame.Width / 2
	chromaHeight := frame.Height / 2
	for y := 0; y < chromaHeight; y++ {
		dst = append(dst, frame.Cb[y*frame.StrideCb:y*frame.StrideCb+chromaWidth]...)
	}
	for y := 0; y < chromaHeight; y++ {
		dst = append(dst, frame.Cr[y*frame.StrideCr:y*frame.StrideCr+chromaWidth]...)
	}
	return dst
}

func appendI420FromFrame(dst []byte, frame av.Frame) []byte {
	width := frame.Video.Width
	height := frame.Video.Height
	for y := 0; y < height; y++ {
		offset := y * frame.Planes[0].Stride
		dst = append(dst, frame.Planes[0].Buffer.Bytes[offset:offset+width]...)
	}
	chromaWidth := width / 2
	chromaHeight := height / 2
	for y := 0; y < chromaHeight; y++ {
		offset := y * frame.Planes[1].Stride
		dst = append(dst, frame.Planes[1].Buffer.Bytes[offset:offset+chromaWidth]...)
	}
	for y := 0; y < chromaHeight; y++ {
		offset := y * frame.Planes[2].Stride
		dst = append(dst, frame.Planes[2].Buffer.Bytes[offset:offset+chromaWidth]...)
	}
	return dst
}
