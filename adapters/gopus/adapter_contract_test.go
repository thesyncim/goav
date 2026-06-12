package gopus

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	gopuslib "github.com/thesyncim/gopus"
)

func TestConcreteDescriptorAndLifecycleNoops(t *testing.T) {
	ctx := context.Background()
	decoder := &Decoder{}
	if err := decoder.Open(ctx, codec.DecodeConfig{Stream: opusTestStream()}); err != nil {
		t.Fatal(err)
	}
	if desc := decoder.Descriptor(); desc.ID != av.CodecOpus || !desc.Supports(codec.ModeDecode) {
		t.Fatalf("decoder descriptor = %+v", desc)
	}
	if err := decoder.FlushInto(ctx, nil); err != nil {
		t.Fatalf("decoder flush err = %v", err)
	}
	if err := decoder.HandleEvent(ctx, nil); err != nil {
		t.Fatalf("decoder nil event err = %v", err)
	}
	if err := decoder.Close(); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Close(); err != nil {
		t.Fatalf("decoder second close err = %v", err)
	}

	encoder := &Encoder{}
	if err := encoder.Open(ctx, opusEncodeConfig()); err != nil {
		t.Fatal(err)
	}
	if desc := encoder.Descriptor(); desc.ID != av.CodecOpus || !desc.Supports(codec.ModeEncode) {
		t.Fatalf("encoder descriptor = %+v", desc)
	}
	if err := encoder.FlushInto(ctx, &codec.EncodeResult{}); err != nil {
		t.Fatalf("encoder flush err = %v", err)
	}
	if err := encoder.HandleEvent(ctx, nil); err != nil {
		t.Fatalf("encoder nil event err = %v", err)
	}
	if err := encoder.HandleEvent(ctx, &av.Event{Type: av.EventDiscontinuity}); err != nil {
		t.Fatalf("encoder discontinuity err = %v", err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	if err := encoder.FlushInto(ctx, &codec.EncodeResult{}); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("flush after close err = %v, want %v", err, codec.ErrClosed)
	}
	if err := encoder.HandleEvent(ctx, &av.Event{Type: av.EventCodecChanged}); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("event after close err = %v, want %v", err, codec.ErrClosed)
	}
}

func TestOpenHonorsContextAndValidation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewDecoderFactory().NewDecoder(ctx, codec.DecodeConfig{Stream: opusTestStream()}); !errors.Is(err, context.Canceled) {
		t.Fatalf("decoder canceled err = %v, want context.Canceled", err)
	}
	if _, err := NewEncoderFactory().NewEncoder(ctx, opusEncodeConfig()); !errors.Is(err, context.Canceled) {
		t.Fatalf("encoder canceled err = %v, want context.Canceled", err)
	}

	for _, tc := range []struct {
		name   string
		config codec.EncodeConfig
	}{
		{name: "stream codec", config: func() codec.EncodeConfig {
			config := opusEncodeConfig()
			config.Stream.Codec.ID = av.CodecAAC
			return config
		}()},
		{name: "parameters codec", config: func() codec.EncodeConfig {
			config := opusEncodeConfig()
			config.Parameters.ID = av.CodecAAC
			return config
		}()},
		{name: "channels", config: func() codec.EncodeConfig {
			config := opusEncodeConfig()
			config.Parameters.Channels = 3
			return config
		}()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, _, err := normalizeEncodeStream(tc.config); !errors.Is(err, codec.ErrUnsupportedFormat) {
				t.Fatalf("normalize err = %v, want %v", err, codec.ErrUnsupportedFormat)
			}
		})
	}

	config := codec.EncodeConfig{
		Stream:     av.Stream{Codec: av.CodecParameters{ClockRate: 16000}},
		Parameters: av.CodecParameters{Channels: 1},
	}
	stream, sampleRate, channels, err := normalizeEncodeStream(config)
	if err != nil {
		t.Fatal(err)
	}
	if sampleRate != 16000 || channels != 1 || stream.Codec.ChannelLayout != "mono" || stream.TimeBase.Den != 16000 {
		t.Fatalf("normalized stream=%+v sampleRate=%d channels=%d", stream, sampleRate, channels)
	}

	stream, sampleRate, channels, err = normalizeEncodeStream(codec.EncodeConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if sampleRate != 48000 || channels != 2 || stream.Codec.ChannelLayout != "stereo" {
		t.Fatalf("default stream=%+v sampleRate=%d channels=%d", stream, sampleRate, channels)
	}
}

func TestEncoderControlHookAndErrorMapping(t *testing.T) {
	hookErr := errors.New("hook failed")
	config := opusEncodeConfig()
	config.Settings.Control = func(any) error { return hookErr }
	if _, err := NewEncoderFactory().NewEncoder(context.Background(), config); !errors.Is(err, hookErr) {
		t.Fatalf("control hook err = %v, want %v", err, hookErr)
	}

	if err := mapEncodeError(nil); err != nil {
		t.Fatalf("nil map err = %v", err)
	}
	if err := mapEncodeError(gopuslib.ErrBufferTooSmall); !errors.Is(err, codec.ErrOutputBufferTooSmall) {
		t.Fatalf("buffer err = %v, want %v", err, codec.ErrOutputBufferTooSmall)
	}
	for _, err := range []error{
		gopuslib.ErrInvalidSampleRate,
		gopuslib.ErrInvalidChannels,
		gopuslib.ErrInvalidSampleFormat,
		gopuslib.ErrInvalidFrameSize,
		gopuslib.ErrInvalidBitrate,
		gopuslib.ErrInvalidApplication,
		gopuslib.ErrInvalidArgument,
	} {
		if got := mapEncodeError(err); !errors.Is(got, codec.ErrUnsupportedFormat) {
			t.Fatalf("mapEncodeError(%v) = %v, want %v", err, got, codec.ErrUnsupportedFormat)
		}
	}
	other := errors.New("other")
	if err := mapEncodeError(other); !errors.Is(err, other) {
		t.Fatalf("other err = %v, want %v", err, other)
	}
	if got := firstPositive(0, -1, 0); got != 0 {
		t.Fatalf("firstPositive = %d, want 0", got)
	}
}

func TestEncodeInputValidationAndNoops(t *testing.T) {
	ctx := context.Background()
	encoder := &Encoder{}
	if err := encoder.Open(ctx, opusEncodeConfig()); err != nil {
		t.Fatal(err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := encoder.EncodeInto(canceled, opusTestFramePtr(), &codec.EncodeResult{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("encode canceled err = %v, want context.Canceled", err)
	}
	if err := encoder.FlushInto(canceled, &codec.EncodeResult{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("flush canceled err = %v, want context.Canceled", err)
	}
	if err := encoder.HandleEvent(canceled, &av.Event{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("event canceled err = %v, want context.Canceled", err)
	}
	if err := encoder.EncodeInto(ctx, opusTestFramePtr(), nil); !errors.Is(err, codec.ErrNilResult) {
		t.Fatalf("nil result err = %v, want %v", err, codec.ErrNilResult)
	}
	if err := encoder.FlushInto(ctx, nil); !errors.Is(err, codec.ErrNilResult) {
		t.Fatalf("nil flush err = %v, want %v", err, codec.ErrNilResult)
	}
	result := opusEncodeResult(1, 4096)
	if err := encoder.EncodeInto(ctx, nil, &result); err != nil {
		t.Fatalf("nil frame err = %v", err)
	}
	if len(result.Packets) != 0 {
		t.Fatalf("nil frame packets = %+v", result.Packets)
	}

	frame := opusTestFrame()
	frame.Duration = av.Duration{}
	result = opusEncodeResult(1, 4096)
	if err := encoder.EncodeInto(ctx, &frame, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Packets) != 1 || result.Packets[0].Duration.Value == 0 {
		t.Fatalf("duration fallback packets = %+v", result.Packets)
	}
}

func TestPCMFromFrameRejectsInvalidShapes(t *testing.T) {
	encoder := &Encoder{channels: 1}
	for _, tc := range []struct {
		name  string
		frame av.Frame
	}{
		{name: "non audio", frame: func() av.Frame {
			frame := opusTestFrame()
			frame.Type = av.MediaVideo
			return frame
		}()},
		{name: "missing audio", frame: av.Frame{Type: av.MediaAudio}},
		{name: "bad sample format", frame: func() av.Frame {
			frame := opusTestFrame()
			frame.Audio.SampleFormat = av.SampleFormatF32
			return frame
		}()},
		{name: "bad channels", frame: func() av.Frame {
			frame := opusTestFrame()
			frame.Audio.Channels = 2
			return frame
		}()},
		{name: "odd bytes", frame: func() av.Frame {
			frame := opusTestFrame()
			frame.Planes[0].Buffer.Bytes = frame.Planes[0].Buffer.Bytes[:len(frame.Planes[0].Buffer.Bytes)-1]
			return frame
		}()},
		{name: "bad sample count", frame: func() av.Frame {
			frame := opusTestFrame()
			frame.Audio.Samples++
			return frame
		}()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := encoder.pcmFromFrame(&tc.frame); !errors.Is(err, codec.ErrUnsupportedFormat) {
				t.Fatalf("err = %v, want %v", err, codec.ErrUnsupportedFormat)
			}
		})
	}

	frame := opusTestFrame()
	frame.Audio.Samples = 0
	pcm, samples, err := encoder.pcmFromFrame(&frame)
	if err != nil {
		t.Fatal(err)
	}
	if len(pcm) != 960 || samples != 960 {
		t.Fatalf("pcm len=%d samples=%d", len(pcm), samples)
	}
}

func TestDecoderInputValidationAndResetEvents(t *testing.T) {
	ctx := context.Background()
	decoder := &Decoder{}
	if err := decoder.Open(ctx, codec.DecodeConfig{Stream: av.Stream{Codec: av.CodecParameters{ClockRate: 16000}}}); err != nil {
		t.Fatal(err)
	}
	if decoder.sampleRate != 16000 || decoder.channels != 1 || decoder.stream.TimeBase.Den != 16000 {
		t.Fatalf("decoder defaults = %+v sampleRate=%d channels=%d", decoder.stream, decoder.sampleRate, decoder.channels)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := decoder.DecodeInto(canceled, nil, &codec.DecodeResult{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("decode canceled err = %v, want context.Canceled", err)
	}
	if err := decoder.HandleEvent(canceled, &av.Event{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("event canceled err = %v, want context.Canceled", err)
	}
	if err := decoder.DecodeInto(ctx, nil, nil); !errors.Is(err, codec.ErrNilResult) {
		t.Fatalf("nil result err = %v, want %v", err, codec.ErrNilResult)
	}
	result := codec.DecodeResult{Frames: make([]av.Frame, 0, 1)}
	if err := decoder.DecodeInto(ctx, nil, &result); err != nil {
		t.Fatalf("nil packet err = %v", err)
	}
	if len(result.Frames) != 0 {
		t.Fatalf("nil packet frames = %+v", result.Frames)
	}

	if err := decoder.HandleEvent(ctx, &av.Event{Type: av.EventPacketLoss}); err != nil {
		t.Fatal(err)
	}
	if decoder.pendingPLC != 1 {
		t.Fatalf("pending plc = %d, want 1", decoder.pendingPLC)
	}
	if err := decoder.HandleEvent(ctx, &av.Event{Type: av.EventCodecChanged}); err != nil {
		t.Fatal(err)
	}
	if decoder.pendingPLC != 0 {
		t.Fatalf("pending plc after reset = %d, want 0", decoder.pendingPLC)
	}
	if err := decoder.Close(); err != nil {
		t.Fatal(err)
	}
	if err := decoder.DecodeInto(ctx, &av.Packet{}, &result); !errors.Is(err, codec.ErrUnsupportedFormat) {
		t.Fatalf("decode after close err = %v, want %v", err, codec.ErrUnsupportedFormat)
	}
	if got := bytesAsInt16(nil); got != nil {
		t.Fatalf("empty bytesAsInt16 = %v, want nil", got)
	}
}

func opusTestFramePtr() *av.Frame {
	frame := opusTestFrame()
	return &frame
}
