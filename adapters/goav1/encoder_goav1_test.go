package goav1

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	backend "github.com/thesyncim/goav1"
)

func TestEncoderProducesDecodableAV1Packet(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:   "video",
		Type: av.MediaVideo,
		Codec: av.CodecParameters{
			ID:          av.CodecAV1,
			Type:        av.MediaVideo,
			Width:       16,
			Height:      16,
			PixelFormat: av.PixelFormatI420,
			ClockRate:   90000,
		},
		TimeBase: av.RTPTimeBase(90000),
	}
	encoder, err := NewEncoderFactory().NewEncoder(ctx, codec.EncodeConfig{
		Stream:     stream,
		Parameters: stream.Codec,
		Realtime:   true,
		Settings: codec.CodecSettings{
			Framerate:        av.Duration{Value: 1, Base: av.TimeBase{Num: 1, Den: 30}},
			KeyframeInterval: 1,
			Custom:           av.Metadata{"qindex": "96"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()

	encoded := codec.EncodeResult{
		Packets: []av.Packet{{
			Payload: av.Buffer{Bytes: make([]byte, 0, 1<<20), Ownership: av.BufferOwned},
		}}[:0],
		Events: make([]av.Event, 0, 1),
	}
	if err := encoder.EncodeInto(ctx, patternedAV1Frame(16, 16, 0), &encoded); err != nil {
		t.Fatal(err)
	}
	if len(encoded.Packets) != 1 {
		t.Fatalf("packets = %d, want 1", len(encoded.Packets))
	}
	packet := encoded.Packets[0]
	if !packet.Keyframe || packet.Type != av.MediaVideo || len(packet.Payload.Bytes) == 0 {
		t.Fatalf("packet = %+v payload=%d", packet, len(packet.Payload.Bytes))
	}
	if bytes.HasPrefix(packet.Payload.Bytes, []byte("gAVf")) {
		t.Fatal("packet payload uses the test passthrough frame marker, not AV1")
	}

	decodeState, err := NewDecoderFactory().NewDecodeState(ctx, codec.DecodeConfig{
		Stream: stream,
		Bounds: codec.DecodeBounds{
			MaxFramesPerInput:   2,
			MaxEventsPerInput:   2,
			MaxRequestsPerInput: 2,
			MaxPayloadBytes:     1 << 20,
			MaxWidth:            16,
			MaxHeight:           16,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := NewDecoderFactory().NewDecoder(ctx, codec.DecodeConfig{
		Stream:      stream,
		OpaqueState: decodeState,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()

	frameScratch := make([]av.Frame, 2)
	for i := range frameScratch {
		frameScratch[i].Planes = make([]av.Plane, 0, 3)
	}
	decoded := codec.DecodeResult{
		Frames:   frameScratch[:0],
		Events:   make([]av.Event, 0, 2),
		Requests: make([]codec.ControlRequest, 0, 2),
	}
	if err := decoder.DecodeInto(ctx, &packet, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Frames) != 1 || decoded.Frames[0].Video == nil {
		t.Fatalf("decoded frames = %+v", decoded.Frames)
	}
	if decoded.Frames[0].Video.Width != 16 || decoded.Frames[0].Video.Height != 16 {
		t.Fatalf("decoded video = %+v", decoded.Frames[0].Video)
	}

	encoded.Reset()
	if err := encoder.EncodeInto(ctx, patternedAV1Frame(16, 16, 1), &encoded); err != nil {
		t.Fatal(err)
	}
	if len(encoded.Packets) != 1 {
		t.Fatalf("second packets = %d, want 1", len(encoded.Packets))
	}
	if got := encoded.Packets[0].PTS.Value; got != 3000 {
		t.Fatalf("second packet PTS = %d, want 3000 in 90 kHz timebase", got)
	}
	if got := encoded.Packets[0].Duration.Value; got != 3000 {
		t.Fatalf("second packet duration = %d, want 3000 in 90 kHz timebase", got)
	}
}

func TestEncoderAppliesCustomSettingsToNativeConfig(t *testing.T) {
	ctx := context.Background()
	seen := false
	encoder, err := NewEncoderFactory().NewEncoder(ctx, codec.EncodeConfig{
		Stream: av.Stream{
			ID:   "video",
			Type: av.MediaVideo,
			Codec: av.CodecParameters{
				ID:          av.CodecAV1,
				Type:        av.MediaVideo,
				Width:       32,
				Height:      32,
				PixelFormat: av.PixelFormatI420,
			},
		},
		Parameters: av.CodecParameters{ID: av.CodecAV1, Type: av.MediaVideo, Width: 32, Height: 32},
		Realtime:   true,
		Settings: codec.CodecSettings{
			Bitrate:   400_000,
			Framerate: av.Duration{Value: 1, Base: av.TimeBase{Num: 1, Den: 30}},
			Custom: av.Metadata{
				"min_qindex":      "20",
				"max_qindex":      "180",
				"temporal_layers": "2",
				"tile_columns":    "1",
				"golden_interval": "-1",
			},
			Control: func(native any) error {
				config, ok := native.(*backend.VideoEncoderConfig)
				if !ok {
					t.Fatalf("control native = %T, want *goav1.VideoEncoderConfig", native)
				}
				seen = true
				if config.TargetBitrate != 400_000 || config.Framerate != 30 ||
					config.MinQIndex != 20 || config.MaxQIndex != 180 ||
					config.TemporalLayers != 2 || config.TileColumns != 1 ||
					config.GoldenInterval != -1 {
					t.Fatalf("native config = %+v", config)
				}
				return nil
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	if !seen {
		t.Fatal("native control was not called")
	}
}

func TestEncoderRejectsUnsupportedCustomSetting(t *testing.T) {
	_, err := NewEncoderFactory().NewEncoder(context.Background(), codec.EncodeConfig{
		Stream: av.Stream{
			Type: av.MediaVideo,
			Codec: av.CodecParameters{
				ID:          av.CodecAV1,
				Type:        av.MediaVideo,
				Width:       16,
				Height:      16,
				PixelFormat: av.PixelFormatI420,
			},
		},
		Parameters: av.CodecParameters{ID: av.CodecAV1, Type: av.MediaVideo, Width: 16, Height: 16},
		Settings: codec.CodecSettings{
			Custom: av.Metadata{"speed": "8"},
		},
	})
	if !errors.Is(err, codec.ErrUnsupportedFormat) {
		t.Fatalf("err = %v, want ErrUnsupportedFormat", err)
	}
}

func patternedAV1Frame(width, height, n int) *av.Frame {
	chromaW := width / 2
	chromaH := height / 2
	y := make([]byte, width*height)
	for row := 0; row < height; row++ {
		for col := 0; col < width; col++ {
			y[row*width+col] = byte((row*7 + col*5 + n) % 256)
		}
	}
	u := bytes.Repeat([]byte{128}, chromaW*chromaH)
	v := bytes.Repeat([]byte{128}, chromaW*chromaH)
	base := av.TimeBase{Num: 1, Den: 30}
	return &av.Frame{
		StreamID: "video",
		Type:     av.MediaVideo,
		PTS:      av.Timestamp{Value: int64(n), Base: base},
		Duration: av.Duration{Value: 1, Base: base},
		Video:    &av.VideoFrame{Width: width, Height: height, PixelFormat: av.PixelFormatI420},
		Planes: []av.Plane{
			{Buffer: av.Buffer{Bytes: y, Ownership: av.BufferImmutable}, Stride: width},
			{Buffer: av.Buffer{Bytes: u, Ownership: av.BufferImmutable}, Stride: chromaW},
			{Buffer: av.Buffer{Bytes: v, Ownership: av.BufferImmutable}, Stride: chromaW},
		},
	}
}
