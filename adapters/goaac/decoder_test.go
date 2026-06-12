package goaac

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	aaclib "github.com/thesyncim/goaac"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

func TestDescriptorAndRegister(t *testing.T) {
	desc := Descriptor()
	if desc.ID != av.CodecAAC || desc.Backend.Module != "github.com/thesyncim/goaac" {
		t.Fatalf("descriptor = %+v", desc)
	}
	if !desc.Supports(codec.ModeDecode) || desc.Supports(codec.ModeEncode) {
		t.Fatalf("descriptor modes = %v", desc.Modes)
	}
	if desc.Backend.Status != "active" {
		t.Fatalf("status = %q", desc.Backend.Status)
	}

	registry := codec.NewRegistry()
	Register(registry)
	if _, err := registry.DecoderFactory(av.CodecAAC); err != nil {
		t.Fatalf("decoder factory: %v", err)
	}
	if _, err := registry.EncoderFactory(av.CodecAAC); !errors.Is(err, codec.ErrNotFound) {
		t.Fatalf("encoder factory err = %v, want ErrNotFound", err)
	}
}

func TestNormalizeDecodeConfigSelectsRawFromExtraData(t *testing.T) {
	private := audioSpecificConfig(t, 48000, 2)
	stream, options, err := normalizeDecodeConfig(codec.DecodeConfig{
		Stream: av.Stream{Codec: av.CodecParameters{
			ID:        av.CodecAAC,
			Type:      av.MediaAudio,
			ExtraData: av.Buffer{Bytes: private},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if options.Transport != aaclib.TransportRaw {
		t.Fatalf("transport = %s, want raw", options.Transport)
	}
	if stream.Codec.SampleRate != 48000 || stream.Codec.Channels != 2 || stream.Codec.SampleFormat != av.SampleFormatS16 {
		t.Fatalf("stream codec = %+v", stream.Codec)
	}
}

func TestNormalizeDecodeConfigSupportsTransportOverride(t *testing.T) {
	private := audioSpecificConfig(t, 44100, 2)
	_, options, err := normalizeDecodeConfig(codec.DecodeConfig{
		Stream: av.Stream{Codec: av.CodecParameters{
			ID:        av.CodecAAC,
			Type:      av.MediaAudio,
			ExtraData: av.Buffer{Bytes: private},
		}},
		Settings: codec.CodecSettings{Custom: av.Metadata{aacTransportKey: aacTransportADTS}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if options.Transport != aaclib.TransportADTS {
		t.Fatalf("transport = %s, want adts", options.Transport)
	}

	stream, options, err := normalizeDecodeConfig(codec.DecodeConfig{
		Stream: av.Stream{Codec: av.CodecParameters{
			ID:        av.CodecAAC,
			Type:      av.MediaAudio,
			ExtraData: av.Buffer{Bytes: private},
		}},
		Settings: codec.CodecSettings{Custom: av.Metadata{aacTransportKey: aacTransportAuto}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if options.Transport != aaclib.TransportAuto || len(options.Config.ExtraData) == 0 || stream.Codec.SampleRate != 44100 {
		t.Fatalf("auto options=%+v stream=%+v", options, stream.Codec)
	}

	_, _, err = normalizeDecodeConfig(codec.DecodeConfig{
		Stream:   av.Stream{Codec: av.CodecParameters{ID: av.CodecAAC}},
		Settings: codec.CodecSettings{Custom: av.Metadata{aacTransportKey: "latm"}},
	})
	if !errors.Is(err, codec.ErrUnsupportedFormat) {
		t.Fatalf("err = %v, want ErrUnsupportedFormat", err)
	}
}

func TestOpenHonorsContextAndValidation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewDecoderFactory().NewDecoder(ctx, codec.DecodeConfig{Stream: aacADTSStream()}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled err = %v, want context.Canceled", err)
	}
	if _, err := NewDecoderFactory().NewDecoder(context.Background(), codec.DecodeConfig{
		Stream: av.Stream{Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecAAC}},
	}); !errors.Is(err, codec.ErrUnsupportedFormat) {
		t.Fatalf("video stream err = %v, want ErrUnsupportedFormat", err)
	}
	if _, err := NewDecoderFactory().NewDecoder(context.Background(), codec.DecodeConfig{
		Stream: av.Stream{Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus}},
	}); !errors.Is(err, codec.ErrUnsupportedFormat) {
		t.Fatalf("wrong codec err = %v, want ErrUnsupportedFormat", err)
	}
}

func TestDecodeADTSIntoCallerOwnedFrame(t *testing.T) {
	packets := ffmpegADTSFrames(t)
	decoder := newTestDecoder(t, codec.DecodeConfig{Stream: aacADTSStream()})
	result := decodeResult(1, 4096)
	var packet av.Packet
	for i, packetData := range packets {
		result.Reset()
		packet = av.Packet{
			StreamID:   "audio",
			Type:       av.MediaAudio,
			CodecEpoch: 9,
			Payload:    av.Buffer{Bytes: packetData, Ownership: av.BufferBorrowed},
			PTS:        av.Timestamp{Value: int64(i * 1024), Base: av.TimeBase{Num: 1, Den: 44100}},
			Metadata:   av.Metadata{"track": "main"},
		}
		if err := decoder.DecodeInto(context.Background(), &packet, &result); err != nil {
			t.Fatal(err)
		}
		if len(result.Frames) != 0 {
			break
		}
	}
	if len(result.Frames) != 1 {
		t.Fatalf("frames = %d, want first decoded AAC frame", len(result.Frames))
	}
	frame := result.Frames[0]
	if frame.StreamID != "audio" || frame.CodecEpoch != 9 || frame.Type != av.MediaAudio {
		t.Fatalf("frame identity = %+v", frame)
	}
	if frame.Audio == nil ||
		frame.Audio.SampleRate != 44100 ||
		frame.Audio.Channels != 2 ||
		frame.Audio.SampleFormat != av.SampleFormatS16 ||
		frame.Audio.Samples != 1024 {
		t.Fatalf("audio = %+v", frame.Audio)
	}
	if len(frame.Planes) != 1 ||
		len(frame.Planes[0].Buffer.Bytes) != 1024*2*2 ||
		frame.Planes[0].Buffer.Ownership != av.BufferOwned ||
		frame.Planes[0].Stride != 4 {
		t.Fatalf("planes = %+v", frame.Planes)
	}
	if frame.PTS != packet.PTS || frame.Duration.Value != 1024 || frame.Duration.Base.Den != 44100 || frame.Metadata["track"] != "main" {
		t.Fatalf("timing/metadata frame=%+v packet=%+v", frame, packet)
	}
}

func TestDecodeRawAACWithAudioSpecificConfig(t *testing.T) {
	adts := ffmpegADTSFrames(t)
	header, err := aaclib.ParseADTSHeader(adts[0])
	if err != nil {
		t.Fatal(err)
	}
	private := audioSpecificConfig(t, header.SampleRate, header.ChannelConfig)
	decoder := newTestDecoder(t, codec.DecodeConfig{Stream: av.Stream{
		ID:   "raw",
		Type: av.MediaAudio,
		Codec: av.CodecParameters{
			ID:        av.CodecAAC,
			Type:      av.MediaAudio,
			ExtraData: av.Buffer{Bytes: private},
		},
	}})
	result := decodeResult(1, 4096)
	for _, frame := range adts {
		header, err := aaclib.ParseADTSHeader(frame)
		if err != nil {
			t.Fatal(err)
		}
		result.Reset()
		packet := av.Packet{Payload: av.Buffer{Bytes: frame[header.HeaderLength:header.FrameLength]}}
		if err := decoder.DecodeInto(context.Background(), &packet, &result); err != nil {
			t.Fatal(err)
		}
		if len(result.Frames) != 0 {
			break
		}
	}
	if len(result.Frames) != 1 || result.Frames[0].Audio == nil || result.Frames[0].Audio.Samples != 1024 {
		t.Fatalf("frames = %+v", result.Frames)
	}
}

func TestDecodeRejectsInsufficientCallerBuffers(t *testing.T) {
	packetData := ffmpegADTSFrames(t)[0]
	decoder := newTestDecoder(t, codec.DecodeConfig{Stream: aacADTSStream()})
	packet := av.Packet{Payload: av.Buffer{Bytes: packetData}}

	full := codec.DecodeResult{}
	if err := decoder.DecodeInto(context.Background(), &packet, &full); !errors.Is(err, codec.ErrResultFull) {
		t.Fatalf("full result err = %v, want ErrResultFull", err)
	}

	tiny := decodeResult(1, 2)
	if err := decoder.DecodeInto(context.Background(), &packet, &tiny); !errors.Is(err, codec.ErrOutputBufferTooSmall) {
		t.Fatalf("tiny buffer err = %v, want ErrOutputBufferTooSmall", err)
	}
}

func TestLifecycleControlAndEvents(t *testing.T) {
	hookErr := errors.New("control failed")
	_, err := NewDecoderFactory().NewDecoder(context.Background(), codec.DecodeConfig{
		Stream: aacADTSStream(),
		Settings: codec.CodecSettings{Control: func(native any) error {
			if _, ok := native.(*aaclib.Decoder); !ok {
				t.Fatalf("native = %T, want *aac.Decoder", native)
			}
			return hookErr
		}},
	})
	if errors.Is(err, codec.ErrUnavailable) {
		t.Skip("goaac backend unavailable on this target")
	}
	if !errors.Is(err, hookErr) {
		t.Fatalf("control err = %v, want hook error", err)
	}

	decoder := newTestDecoder(t, codec.DecodeConfig{Stream: aacADTSStream()})
	if err := decoder.HandleEvent(context.Background(), nil); err != nil {
		t.Fatalf("nil event err = %v", err)
	}
	if err := decoder.HandleEvent(context.Background(), &av.Event{Type: av.EventDiscontinuity}); err != nil {
		t.Fatalf("discontinuity err = %v", err)
	}
	if err := decoder.HandleEvent(context.Background(), &av.Event{
		Type:  av.EventCodecChanged,
		Codec: &av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio},
	}); !errors.Is(err, codec.ErrUnsupportedCodecSwitch) {
		t.Fatalf("codec switch err = %v, want ErrUnsupportedCodecSwitch", err)
	}
	if err := decoder.Close(); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Close(); err != nil {
		t.Fatalf("second close err = %v", err)
	}
	if err := decoder.FlushInto(context.Background(), &codec.DecodeResult{}); !errors.Is(err, codec.ErrClosed) {
		t.Fatalf("flush after close err = %v, want ErrClosed", err)
	}
}

func newTestDecoder(t *testing.T, config codec.DecodeConfig) codec.Decoder {
	t.Helper()
	decoder, err := NewDecoderFactory().NewDecoder(context.Background(), config)
	if errors.Is(err, codec.ErrUnavailable) {
		t.Skip("goaac backend unavailable on this target")
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = decoder.Close() })
	return decoder
}

func aacADTSStream() av.Stream {
	return av.Stream{
		ID:   "audio",
		Type: av.MediaAudio,
		Codec: av.CodecParameters{
			ID:   av.CodecAAC,
			Type: av.MediaAudio,
		},
	}
}

func decodeResult(frameCapacity int, planeCapacity int) codec.DecodeResult {
	frames := make([]av.Frame, frameCapacity)
	for i := range frames {
		frames[i].Planes = []av.Plane{{Buffer: av.Buffer{Bytes: make([]byte, 0, planeCapacity)}}}
	}
	result := codec.DecodeResult{Frames: frames}
	result.Reset()
	return result
}

func audioSpecificConfig(t *testing.T, sampleRate int, channelConfig int) []byte {
	t.Helper()
	private, err := (aaclib.Config{
		ObjectType:    aaclib.AOTAACLC,
		SampleRate:    sampleRate,
		ChannelConfig: channelConfig,
	}).AudioSpecificConfig()
	if err != nil {
		t.Fatal(err)
	}
	return private
}

func ffmpegADTSFrames(t *testing.T) [][]byte {
	t.Helper()
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg CLI not available")
	}
	file := filepath.Join(t.TempDir(), "tone.aac")
	cmd := exec.Command(ffmpeg,
		"-hide_banner", "-loglevel", "error",
		"-f", "lavfi",
		"-i", "sine=frequency=997:duration=0.12:sample_rate=44100",
		"-ac", "2",
		"-c:a", "aac",
		"-profile:a", "aac_low",
		"-b:a", "96k",
		"-f", "adts",
		file,
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("ffmpeg failed: %v\n%s", err, out)
	}
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	frames, err := aaclib.SplitADTSFrames(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) == 0 {
		t.Fatal("ffmpeg produced no ADTS frames")
	}
	out := make([][]byte, len(frames))
	for i := range frames {
		out[i] = append([]byte(nil), frames[i].Data...)
	}
	return out
}

func TestMapDecodeError(t *testing.T) {
	for _, err := range []error{
		aaclib.ErrNeedMoreData,
		aaclib.ErrInvalidConfig,
		aaclib.ErrInvalidADTS,
		aaclib.ErrUnsupportedProfile,
	} {
		if got := mapDecodeError(err); !errors.Is(got, codec.ErrUnsupportedFormat) {
			t.Fatalf("mapDecodeError(%v) = %v, want ErrUnsupportedFormat", err, got)
		}
	}
	if got := mapDecodeError(aaclib.ErrClosed); !errors.Is(got, codec.ErrClosed) {
		t.Fatalf("closed = %v, want ErrClosed", got)
	}
	if got := mapDecodeError(aaclib.ErrNativeUnavailable); !errors.Is(got, codec.ErrUnavailable) {
		t.Fatalf("unavailable = %v, want ErrUnavailable", got)
	}
	other := errors.New("other")
	if got := mapDecodeError(other); !errors.Is(got, other) {
		t.Fatalf("other = %v, want original", got)
	}
	if got := mapDecodeError(nil); got != nil {
		t.Fatalf("nil = %v", got)
	}
}

func TestDecodeADTSMatchesRawPayloadPCM(t *testing.T) {
	adts := ffmpegADTSFrames(t)
	header, err := aaclib.ParseADTSHeader(adts[0])
	if err != nil {
		t.Fatal(err)
	}
	private := audioSpecificConfig(t, header.SampleRate, header.ChannelConfig)

	adtsDecoder := newTestDecoder(t, codec.DecodeConfig{Stream: aacADTSStream()})
	rawDecoder := newTestDecoder(t, codec.DecodeConfig{Stream: av.Stream{
		ID:   "raw",
		Type: av.MediaAudio,
		Codec: av.CodecParameters{
			ID:        av.CodecAAC,
			Type:      av.MediaAudio,
			ExtraData: av.Buffer{Bytes: private},
		},
	}})
	raw := make([][]byte, 0, len(adts))
	for _, frame := range adts {
		header, err := aaclib.ParseADTSHeader(frame)
		if err != nil {
			t.Fatal(err)
		}
		raw = append(raw, frame[header.HeaderLength:header.FrameLength])
	}
	adtsPCM := decodeFirstPCM(t, adtsDecoder, adts)
	rawPCM := decodeFirstPCM(t, rawDecoder, raw)
	if !bytes.Equal(adtsPCM, rawPCM) {
		t.Fatal("ADTS and raw AAC decode produced different PCM")
	}
}

func decodeFirstPCM(t *testing.T, decoder codec.Decoder, payloads [][]byte) []byte {
	t.Helper()
	result := decodeResult(1, 4096)
	for _, payload := range payloads {
		result.Reset()
		if err := decoder.DecodeInto(context.Background(), &av.Packet{Payload: av.Buffer{Bytes: payload}}, &result); err != nil {
			t.Fatal(err)
		}
		if len(result.Frames) != 0 {
			return append([]byte(nil), result.Frames[0].Planes[0].Buffer.Bytes...)
		}
	}
	t.Fatal("decoder produced no PCM")
	return nil
}
