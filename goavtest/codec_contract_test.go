package goavtest

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

func TestPassthroughCodecFactoryDescriptorAndOpenContracts(t *testing.T) {
	desc := codecDescriptor(av.CodecVP8)
	factory := passthroughCodecFactory{desc: desc}

	encoder, err := factory.NewEncoder(context.Background(), codec.EncodeConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if got := encoder.Descriptor(); got.ID != av.CodecVP8 || got.Backend.Name != "goavtest" {
		t.Fatalf("encoder descriptor = %+v", got)
	}
	if err := encoder.Open(context.Background(), codec.EncodeConfig{}); err != nil {
		t.Fatal(err)
	}
	if err := encoder.FlushInto(context.Background(), &codec.EncodeResult{}); err != nil {
		t.Fatal(err)
	}
	if err := encoder.HandleEvent(context.Background(), &av.Event{Type: av.EventKeyframeRequired}); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}

	stream := av.Stream{
		ID:   "video",
		Type: av.MediaVideo,
		Codec: av.CodecParameters{
			ID:          av.CodecVP8,
			Type:        av.MediaVideo,
			Width:       5,
			Height:      3,
			PixelFormat: av.PixelFormatI420,
		},
	}
	decoder, err := factory.NewDecoder(context.Background(), codec.DecodeConfig{Stream: stream})
	if err != nil {
		t.Fatal(err)
	}
	if got := decoder.Descriptor(); got.ID != av.CodecVP8 || got.Backend.Package != "goavtest" {
		t.Fatalf("decoder descriptor = %+v", got)
	}
	if err := decoder.Open(context.Background(), codec.DecodeConfig{Stream: stream}); err != nil {
		t.Fatal(err)
	}
	if err := decoder.FlushInto(context.Background(), &codec.DecodeResult{}); err != nil {
		t.Fatal(err)
	}
	if err := decoder.HandleEvent(context.Background(), &av.Event{Type: av.EventDiscontinuity}); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPassthroughDecoderFallbackVideoFrame(t *testing.T) {
	decoder := &passthroughDecoder{}
	stream := av.Stream{
		ID:   "video",
		Type: av.MediaVideo,
		Codec: av.CodecParameters{
			Type:        av.MediaVideo,
			Width:       5,
			Height:      3,
			PixelFormat: av.PixelFormatI420,
		},
	}
	if err := decoder.Open(context.Background(), codec.DecodeConfig{Stream: stream}); err != nil {
		t.Fatal(err)
	}

	pts := av.Timestamp{Value: 42, Base: av.TimeBase{Num: 1, Den: 90000}}
	duration := av.Duration{Value: 3000, Base: av.TimeBase{Num: 1, Den: 90000}}
	result := codec.DecodeResult{Frames: make([]av.Frame, 0, 1)}
	err := decoder.DecodeInto(context.Background(), &av.Packet{
		StreamID:   "video",
		Type:       av.MediaVideo,
		CodecEpoch: 7,
		PTS:        pts,
		Duration:   duration,
		Payload:    av.Buffer{Bytes: []byte("foreign-vp8")},
	}, &result)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(result.Frames))
	}
	frame := result.Frames[0]
	if frame.StreamID != "video" || frame.CodecEpoch != 7 || frame.PTS != pts || frame.Duration != duration {
		t.Fatalf("frame identity/timing = %+v", frame)
	}
	if frame.Type != av.MediaVideo || frame.Video == nil ||
		frame.Video.Width != 5 || frame.Video.Height != 3 || frame.Video.PixelFormat != av.PixelFormatI420 {
		t.Fatalf("video fallback = %+v", frame.Video)
	}
	wantPlaneLens := []int{15, 6, 6}
	wantStrides := []int{5, 3, 3}
	if len(frame.Planes) != 3 {
		t.Fatalf("planes = %d, want 3", len(frame.Planes))
	}
	for i := range frame.Planes {
		if len(frame.Planes[i].Buffer.Bytes) != wantPlaneLens[i] || frame.Planes[i].Stride != wantStrides[i] {
			t.Fatalf("plane %d = len %d stride %d, want len %d stride %d", i, len(frame.Planes[i].Buffer.Bytes), frame.Planes[i].Stride, wantPlaneLens[i], wantStrides[i])
		}
		for _, b := range frame.Planes[i].Buffer.Bytes {
			if b != 0 {
				t.Fatalf("plane %d contains non-black byte %d", i, b)
			}
		}
	}
}

func TestPassthroughDecoderFallbackAudioFrames(t *testing.T) {
	decoder := &passthroughDecoder{}
	stream := av.Stream{
		ID:   "audio",
		Type: av.MediaAudio,
		Codec: av.CodecParameters{
			Type:       av.MediaAudio,
			SampleRate: 16000,
			Channels:   2,
		},
	}
	if err := decoder.Open(context.Background(), codec.DecodeConfig{Stream: stream}); err != nil {
		t.Fatal(err)
	}

	result := codec.DecodeResult{Frames: make([]av.Frame, 0, 1)}
	err := decoder.DecodeInto(context.Background(), &av.Packet{
		StreamID: "audio",
		Payload:  av.Buffer{Bytes: []byte("foreign-opus")},
	}, &result)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(result.Frames))
	}
	assertSilentAudioFrame(t, result.Frames[0], 16000, 2, 1920, 4)

	defaultDecoder := &passthroughDecoder{}
	defaults := codec.DecodeResult{Frames: make([]av.Frame, 0, 1)}
	err = defaultDecoder.DecodeInto(context.Background(), &av.Packet{
		StreamID: "unknown",
		Payload:  av.Buffer{Bytes: []byte("foreign-unknown")},
	}, &defaults)
	if err != nil {
		t.Fatal(err)
	}
	assertSilentAudioFrame(t, defaults.Frames[0], 48000, 1, 960, 2)
}

func TestPassthroughDecoderFallbackRefusalsAndFullResult(t *testing.T) {
	decoder := &passthroughDecoder{}
	full := codec.DecodeResult{Frames: make([]av.Frame, 0)}
	err := decoder.DecodeInto(context.Background(), &av.Packet{
		Type:    av.MediaAudio,
		Payload: av.Buffer{Bytes: []byte("foreign")},
	}, &full)
	if !errors.Is(err, codec.ErrResultFull) {
		t.Fatalf("full decode err = %v, want ErrResultFull", err)
	}

	refused := codec.DecodeResult{Frames: make([]av.Frame, 0, 1)}
	err = decoder.DecodeInto(context.Background(), &av.Packet{
		StreamID: "metadata",
		Type:     av.MediaData,
		Payload:  av.Buffer{Bytes: []byte("foreign-data")},
	}, &refused)
	if err == nil || !strings.Contains(err.Error(), "cannot fabricate a fallback data frame") {
		t.Fatalf("unsupported fallback err = %v", err)
	}
	if len(refused.Frames) != 0 {
		t.Fatalf("frames after refusal = %d, want 0", len(refused.Frames))
	}
}

func TestPassthroughPayloadParserRejectsMalformedPayloads(t *testing.T) {
	var frame av.Frame
	var audio av.AudioFrame
	var video av.VideoFrame
	if parseFramePayload(nil, &frame, &audio, &video) {
		t.Fatal("empty payload parsed")
	}
	if parseFramePayload([]byte(framePayloadMagic), &frame, &audio, &video) {
		t.Fatal("missing frame kind parsed")
	}
	if parseFramePayload(append([]byte(framePayloadMagic), 99), &frame, &audio, &video) {
		t.Fatal("unknown frame kind parsed")
	}
	if frame.Type != "" || frame.Audio != nil || frame.Video != nil || len(frame.Planes) != 0 {
		t.Fatalf("malformed payload left frame dirty: %+v", frame)
	}
}

func assertSilentAudioFrame(t *testing.T, frame av.Frame, sampleRate int, channels int, bytesLen int, stride int) {
	t.Helper()
	if frame.Type != av.MediaAudio || frame.Audio == nil {
		t.Fatalf("audio fallback = %+v", frame)
	}
	if frame.Audio.SampleRate != sampleRate ||
		frame.Audio.Channels != channels ||
		frame.Audio.SampleFormat != av.SampleFormatS16 ||
		frame.Audio.Samples != 480 {
		t.Fatalf("audio header = %+v, want rate=%d channels=%d s16 480", frame.Audio, sampleRate, channels)
	}
	if len(frame.Planes) != 1 {
		t.Fatalf("planes = %d, want 1", len(frame.Planes))
	}
	plane := frame.Planes[0]
	if len(plane.Buffer.Bytes) != bytesLen || plane.Stride != stride {
		t.Fatalf("plane = len %d stride %d, want len %d stride %d", len(plane.Buffer.Bytes), plane.Stride, bytesLen, stride)
	}
	for _, b := range plane.Buffer.Bytes {
		if b != 0 {
			t.Fatalf("audio plane contains non-silent byte %d", b)
		}
	}
}
