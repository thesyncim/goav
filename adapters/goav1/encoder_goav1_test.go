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

func TestEncoderLifecycleAndEvents(t *testing.T) {
	ctx := context.Background()
	encoder := newTestAV1Encoder(t, codec.CodecSettings{
		Bitrate:   400_000,
		Framerate: av.Duration{Value: 1, Base: av.TimeBase{Num: 1, Den: 30}},
	})
	if !encoder.Descriptor().Supports(codec.ModeEncode) {
		t.Fatalf("descriptor = %+v", encoder.Descriptor())
	}
	if err := encoder.FlushInto(ctx, &codec.EncodeResult{}); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := encoder.FlushInto(ctx, nil); !errors.Is(err, codec.ErrNilResult) {
		t.Fatalf("nil flush err = %v, want ErrNilResult", err)
	}
	if err := encoder.EncodeInto(ctx, nil, &codec.EncodeResult{}); err != nil {
		t.Fatalf("nil frame encode: %v", err)
	}
	if err := encoder.EncodeInto(ctx, patternedAV1Frame(16, 16, 0), nil); !errors.Is(err, codec.ErrNilResult) {
		t.Fatalf("nil encode result err = %v, want ErrNilResult", err)
	}

	encoded := newAV1EncodeResult()
	if err := encoder.EncodeInto(ctx, patternedAV1Frame(16, 16, 0), &encoded); err != nil {
		t.Fatal(err)
	}
	if len(encoded.Packets) != 1 || !encoded.Packets[0].Keyframe {
		t.Fatalf("first encoded packets = %+v", encoded.Packets)
	}
	if err := encoder.HandleEvent(ctx, &av.Event{Type: av.EventKeyframeRequired}); err != nil {
		t.Fatalf("keyframe event: %v", err)
	}
	encoded.Reset()
	if err := encoder.EncodeInto(ctx, patternedAV1Frame(16, 16, 1), &encoded); err != nil {
		t.Fatal(err)
	}
	if len(encoded.Packets) != 1 || !encoded.Packets[0].Keyframe {
		t.Fatalf("keyframe-required packets = %+v", encoded.Packets)
	}
	if err := encoder.HandleEvent(ctx, &av.Event{Type: av.EventBitrateChanged, Metadata: codec.BitrateMetadata(250_000)}); err != nil {
		t.Fatalf("bitrate event: %v", err)
	}
	encoded.Reset()
	if err := encoder.EncodeInto(ctx, patternedAV1Frame(16, 16, 2), &encoded); err != nil {
		t.Fatal(err)
	}
	if len(encoded.Packets) != 1 || !encoded.Packets[0].Keyframe {
		t.Fatalf("post-bitrate packets = %+v", encoded.Packets)
	}
	if err := encoder.HandleEvent(ctx, &av.Event{Type: av.EventBitrateChanged}); err == nil {
		t.Fatal("malformed bitrate event err = nil")
	}
	if err := encoder.HandleEvent(ctx, &av.Event{Type: av.EventDiscontinuity}); err != nil {
		t.Fatalf("discontinuity event: %v", err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if err := encoder.EncodeInto(ctx, patternedAV1Frame(16, 16, 3), &encoded); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("closed encode err = %v, want ErrClosed", err)
	}
	if err := encoder.FlushInto(ctx, &encoded); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("closed flush err = %v, want ErrClosed", err)
	}
	if err := encoder.HandleEvent(ctx, &av.Event{Type: av.EventKeyframeRequired}); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("closed event err = %v, want ErrClosed", err)
	}
}

func TestEncoderRejectsInvalidFrames(t *testing.T) {
	encoder := newTestAV1Encoder(t, codec.CodecSettings{Custom: av.Metadata{"qindex": "96"}})
	defer encoder.Close()

	for _, tc := range []struct {
		name string
		mut  func(*av.Frame)
		want error
	}{
		{
			name: "audio frame",
			mut: func(frame *av.Frame) {
				frame.Type = av.MediaAudio
			},
			want: codec.ErrUnsupportedFormat,
		},
		{
			name: "nil video",
			mut: func(frame *av.Frame) {
				frame.Video = nil
			},
			want: codec.ErrUnsupportedFormat,
		},
		{
			name: "wrong size",
			mut: func(frame *av.Frame) {
				frame.Video.Width = 32
			},
			want: codec.ErrUnsupportedFormat,
		},
		{
			name: "wrong pixel format",
			mut: func(frame *av.Frame) {
				frame.Video.PixelFormat = av.PixelFormatGray8
			},
			want: codec.ErrUnsupportedFormat,
		},
		{
			name: "too few planes",
			mut: func(frame *av.Frame) {
				frame.Planes = frame.Planes[:2]
			},
			want: codec.ErrOutputBufferTooSmall,
		},
		{
			name: "mismatched chroma stride",
			mut: func(frame *av.Frame) {
				frame.Planes[2].Stride++
			},
			want: codec.ErrUnsupportedFormat,
		},
		{
			name: "bad plane offset",
			mut: func(frame *av.Frame) {
				frame.Planes[0].Offset = len(frame.Planes[0].Buffer.Bytes) + 1
			},
			want: codec.ErrOutputBufferTooSmall,
		},
		{
			name: "short luma plane",
			mut: func(frame *av.Frame) {
				frame.Planes[0].Buffer.Bytes = frame.Planes[0].Buffer.Bytes[:1]
			},
			want: codec.ErrOutputBufferTooSmall,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frame := patternedAV1Frame(16, 16, 0)
			tc.mut(frame)
			err := encoder.EncodeInto(context.Background(), frame, &codec.EncodeResult{
				Packets: make([]av.Packet, 0, 1),
			})
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestAV1EncoderSettingsValidation(t *testing.T) {
	cfg, err := av1EncoderConfig(codec.EncodeConfig{
		Settings: codec.CodecSettings{
			Bitrate: 200_000,
			Custom: av.Metadata{
				"q-index": "80",
			},
		},
	}, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TargetBitrate != 0 || cfg.QIndex != 80 {
		t.Fatalf("qindex config = %+v", cfg)
	}

	for _, tc := range []struct {
		name     string
		settings av.Metadata
	}{
		{name: "bad q", settings: av.Metadata{"qindex": "0"}},
		{name: "bad integer", settings: av.Metadata{"temporal_layers": "fast"}},
		{name: "bad temporal layers", settings: av.Metadata{"temporal_layers": "4"}},
		{name: "bad tile columns", settings: av.Metadata{"tile_columns": "-1"}},
		{name: "bad tune", settings: av.Metadata{"tune": "film"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := av1EncoderConfig(codec.EncodeConfig{Settings: codec.CodecSettings{Custom: tc.settings}}, 16, 16)
			if !errors.Is(err, codec.ErrUnsupportedFormat) {
				t.Fatalf("err = %v, want ErrUnsupportedFormat", err)
			}
		})
	}
}

func TestAV1EncoderHelpers(t *testing.T) {
	if got := rescaleAV1Timestamp(av.Timestamp{Value: 1}, av.RTPTimeBase(90000)); got.Value != 1 || got.Base.Den != 90000 {
		t.Fatalf("timestamp with missing base = %+v", got)
	}
	if got := rescaleAV1Timestamp(av.Timestamp{Value: 1, Base: av.TimeBase{Num: 1, Den: 30}}, av.RTPTimeBase(90000)); got.Value != 3000 {
		t.Fatalf("rescaled timestamp = %+v", got)
	}
	if got := rescaleAV1Duration(av.Duration{Value: 1}, av.RTPTimeBase(90000)); got.Value != 1 || got.Base.Den != 90000 {
		t.Fatalf("duration with missing base = %+v", got)
	}
	if got := rescaleAV1Duration(av.Duration{Value: 1, Base: av.TimeBase{Num: 1, Den: 30}}, av.RTPTimeBase(90000)); got.Value != 3000 {
		t.Fatalf("rescaled duration = %+v", got)
	}
	if got := encodeAV1FPS(codec.EncodeConfig{}); got != 30 {
		t.Fatalf("default fps = %d, want 30", got)
	}
	if got := mapGoav1EncodeError(backend.ErrEncoderInvalidConfig); !errors.Is(got, codec.ErrUnsupportedFormat) {
		t.Fatalf("mapped err = %v, want ErrUnsupportedFormat", got)
	}
	boom := errors.New("boom")
	if got := mapGoav1EncodeError(boom); !errors.Is(got, boom) {
		t.Fatalf("mapped err = %v, want boom", got)
	}
	if got := mapGoav1EncodeError(nil); got != nil {
		t.Fatalf("nil mapped err = %v", got)
	}
}

func newTestAV1Encoder(t *testing.T, settings codec.CodecSettings) *Encoder {
	t.Helper()
	encoder, err := NewEncoderFactory().NewEncoder(context.Background(), codec.EncodeConfig{
		Stream: av.Stream{
			ID:       "video",
			Type:     av.MediaVideo,
			TimeBase: av.RTPTimeBase(90000),
			Codec: av.CodecParameters{
				ID:          av.CodecAV1,
				Type:        av.MediaVideo,
				Width:       16,
				Height:      16,
				PixelFormat: av.PixelFormatI420,
				ClockRate:   90000,
			},
		},
		Parameters: av.CodecParameters{ID: av.CodecAV1, Type: av.MediaVideo, Width: 16, Height: 16},
		Settings:   settings,
	})
	if err != nil {
		t.Fatal(err)
	}
	concrete, ok := encoder.(*Encoder)
	if !ok {
		t.Fatalf("encoder = %T, want *Encoder", encoder)
	}
	return concrete
}

func newAV1EncodeResult() codec.EncodeResult {
	return codec.EncodeResult{
		Packets: []av.Packet{{
			Payload: av.Buffer{Bytes: make([]byte, 0, 1<<20), Ownership: av.BufferOwned},
		}}[:0],
		Events: make([]av.Event, 0, 1),
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
