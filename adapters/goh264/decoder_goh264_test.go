//go:build goav_goh264

package goh264

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"os"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/container/mp4"
	"github.com/thesyncim/goav/internal/h264"
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
	if frame.Video == nil || frame.Video.Width != 64 || frame.Video.Height != 48 || frame.Video.PixelFormat != av.PixelFormatYUV420P {
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
	if sum := md5.Sum(got); sum != want {
		t.Fatalf("decoded raw md5 = %x, want %x", sum, want)
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

func TestFillDecodedFrameAllocs(t *testing.T) {
	decoder := &Decoder{}
	if err := decoder.Open(context.Background(), codec.DecodeConfig{Stream: h264TestStream()}); err != nil {
		t.Fatal(err)
	}
	src := decodedH264TestFrame(t)
	packet := av.Packet{
		StreamID:   "video",
		CodecEpoch: 4,
		PTS:        av.Timestamp{Value: 180, Base: av.RTPTimeBase(90000)},
		Duration:   av.Duration{Value: 3000, Base: av.RTPTimeBase(90000)},
	}
	frame := av.Frame{Planes: make([]av.Plane, 3)}

	if allocs := testing.AllocsPerRun(1000, func() {
		frame.Reset()
		if err := decoder.fillDecodedFrame(&packet, src, &frame); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("fill decoded frame allocs = %v, want 0", allocs)
	}
}

func TestPacketLossKeyframeRequestAllocs(t *testing.T) {
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
	event := av.Event{Type: av.EventPacketLoss, StreamID: "video"}
	result := h264DecodeResult(1)

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
	decoder := &Decoder{}
	if err := decoder.Open(context.Background(), codec.DecodeConfig{Stream: h264TestStream()}); err != nil {
		t.Fatal(err)
	}
	codecParams := av.CodecParameters{
		ID:          av.CodecH264,
		Type:        av.MediaVideo,
		ClockRate:   90000,
		Width:       16,
		Height:      16,
		PixelFormat: av.PixelFormatYUV420P,
	}
	event := av.Event{
		Type:     av.EventCodecChanged,
		StreamID: "replacement",
		Epoch:    7,
		Codec:    &codecParams,
	}
	if err := decoder.HandleEvent(context.Background(), &event); err != nil {
		t.Fatal(err)
	}

	frame := av.Frame{Planes: make([]av.Plane, 3)}
	if err := decoder.fillDecodedFrame(nil, decodedH264TestFrame(t), &frame); err != nil {
		t.Fatal(err)
	}
	if frame.StreamID != "replacement" || frame.CodecEpoch != 7 {
		t.Fatalf("frame identity = %+v", frame)
	}
}

func TestDecoderCloseIsIdempotentAndStopsUse(t *testing.T) {
	ctx := context.Background()
	decoder, err := NewDecoderFactory().NewDecoder(ctx, codec.DecodeConfig{Stream: h264TestStream()})
	if err != nil {
		t.Fatal(err)
	}
	if err := decoder.Close(); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}

	result := h264DecodeResult(1)
	packet := av.Packet{StreamID: "video", Payload: av.Buffer{Bytes: []byte{0}}, Keyframe: true}
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
			Width:       64,
			Height:      48,
			PixelFormat: av.PixelFormatYUV420P,
		},
	}
}

func decodedH264TestFrame(t *testing.T) *goh264lib.Frame {
	t.Helper()
	encoded, _ := encodedAnnexBTestFrame(t)
	decoder := goh264lib.NewDecoder()
	frames, err := decoder.DecodeFrames(encoded)
	if err != nil {
		t.Fatalf("DecodeFrames: %v", err)
	}
	for i := range frames {
		if frames[i] != nil {
			return frames[i]
		}
	}
	t.Fatal("no decoded frame")
	return nil
}

// h264FixtureDecodedMD5 pins the decoded I420 bytes of the fixture's first
// access unit. The fixture is a real encoder's output, so the decoder's own
// output is the oracle: a change here is a decoder behavior change.
var h264FixtureDecodedMD5 = [16]byte{0xdf, 0x81, 0x68, 0xe3, 0xf4, 0x92, 0xce, 0xdd, 0x3a, 0x33, 0xeb, 0x79, 0x2f, 0xf6, 0x0b, 0x26}

// encodedAnnexBTestFrame returns the first H.264 access unit of the repo's MP4
// fixture as Annex B — the avcC parameter sets, then the IDR sample — and the
// MD5 its decode is pinned to.
func encodedAnnexBTestFrame(t *testing.T) ([]byte, [16]byte) {
	t.Helper()
	data, err := os.ReadFile("../../container/mp4/testdata/h264_aac.mp4")
	if err != nil {
		t.Fatal(err)
	}
	demuxer, err := mp4.NewDemuxer(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	video := -1
	var config h264.Config
	for i, track := range demuxer.Tracks() {
		if track.Codec.ID == av.CodecH264 {
			video = i
			if config, err = h264.ParseConfig(track.Codec.ExtraData.Bytes); err != nil {
				t.Fatalf("fixture avcC: %v", err)
			}
			break
		}
	}
	if video < 0 {
		t.Fatal("fixture has no H.264 track")
	}
	var sample mp4.Sample
	for {
		if err := demuxer.ReadInto(nil, &sample); err != nil {
			t.Fatalf("no H.264 sample in fixture: %v", err)
		}
		if sample.TrackIndex == video {
			break
		}
	}
	if !sample.Keyframe {
		t.Fatal("fixture's first H.264 sample is not a keyframe")
	}
	annexB, err := h264.AppendAnnexB(config.AppendParameterSets(nil), sample.Data, config.LengthSize)
	if err != nil {
		t.Fatal(err)
	}
	return annexB, h264FixtureDecodedMD5
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
