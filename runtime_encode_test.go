package goav

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

type encodeTestEncoderFactory struct {
	encoder  *encodeTestEncoder
	encoders []*encodeTestEncoder
	config   codec.EncodeConfig
	configs  []codec.EncodeConfig
}

func (f *encodeTestEncoderFactory) NewEncoder(_ context.Context, config codec.EncodeConfig) (codec.Encoder, error) {
	f.config = config
	f.configs = append(f.configs, config)
	if f.encoder == nil {
		encoder := &encodeTestEncoder{}
		f.encoders = append(f.encoders, encoder)
		return encoder, nil
	}
	return f.encoder, nil
}

type encodeTestEncoder struct {
	encodes int
	flushes int
	closed  bool
}

func (e *encodeTestEncoder) Descriptor() codec.Descriptor {
	return codec.Descriptor{ID: av.CodecPCM}
}

func (e *encodeTestEncoder) Open(context.Context, codec.EncodeConfig) error {
	return nil
}

func (e *encodeTestEncoder) EncodeInto(_ context.Context, frame *av.Frame, out *codec.EncodeResult) error {
	if frame == nil {
		return nil
	}
	if len(out.Packets) == cap(out.Packets) {
		return codec.ErrResultFull
	}
	index := len(out.Packets)
	out.Packets = out.Packets[:index+1]
	packet := &out.Packets[index]
	packet.Reset()
	packet.StreamID = frame.StreamID
	packet.CodecEpoch = frame.CodecEpoch
	packet.PTS = frame.PTS
	packet.Duration = frame.Duration
	packet.Payload.Bytes = append(packet.Payload.Bytes, 7)
	e.encodes++
	return nil
}

func (e *encodeTestEncoder) FlushInto(context.Context, *codec.EncodeResult) error {
	e.flushes++
	return nil
}

func (e *encodeTestEncoder) HandleEvent(context.Context, *av.Event) error {
	return nil
}

func (e *encodeTestEncoder) Close() error {
	e.closed = true
	return nil
}

func TestEncodePacketBufferSizeScalesWithVideoFrame(t *testing.T) {
	audio := encodePacketBufferSize(av.Stream{
		Type:  av.MediaAudio,
		Codec: av.CodecParameters{Type: av.MediaAudio},
	})
	if audio != 4096 {
		t.Fatalf("audio buffer = %d, want 4096", audio)
	}

	fallbackVideo := encodePacketBufferSize(av.Stream{
		Type:  av.MediaVideo,
		Codec: av.CodecParameters{Type: av.MediaVideo},
	})
	if fallbackVideo < 4*1024*1024 {
		t.Fatalf("fallback video buffer = %d, want at least 4 MiB", fallbackVideo)
	}

	video := encodePacketBufferSize(av.Stream{
		Type: av.MediaVideo,
		Codec: av.CodecParameters{
			Type:   av.MediaVideo,
			Width:  960,
			Height: 540,
		},
	})
	if video < 960*540*3 {
		t.Fatalf("video buffer = %d, want sized for frame dimensions", video)
	}
}

func TestPrepareEncodeConfigRequiresMatchingStream(t *testing.T) {
	request := encodeRequest{selector: testSelectVideo(), config: pcmEncodeConfig()}

	_, _, err := prepareEncodeConfig(audioOpusTestStream(), request, false)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_stream_mismatch" || !errors.Is(err, errUnsupportedBuild) {
		t.Fatalf("err = %v, want encode_stream_mismatch with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "selected: audio[0]") || !strings.Contains(err.Error(), "requested: type=video") {
		t.Fatalf("err = %v, want selected and requested stream details", err)
	}
}

func TestPrepareEncodeConfigRequiresTargetCodec(t *testing.T) {
	request := encodeRequest{selector: testSelectAudio()}

	_, _, err := prepareEncodeConfig(audioOpusTestStream(), request, false)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_destination_missing" || !errors.Is(err, errUnsupportedBuild) {
		t.Fatalf("err = %v, want encode_destination_missing with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "no target codec") || !strings.Contains(err.Error(), "codec.EncodeConfig.Parameters.ID") {
		t.Fatalf("err = %v, want target codec guidance", err)
	}
}

func audioOpusTestStream() av.Stream {
	return av.Stream{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:           av.CodecOpus,
			Type:         av.MediaAudio,
			ClockRate:    48000,
			SampleRate:   48000,
			Channels:     2,
			SampleFormat: av.SampleFormatS16,
		},
	}
}

func pcmEncodeConfig() codec.EncodeConfig {
	return codec.EncodeConfig{
		Parameters: av.CodecParameters{
			ID:           av.CodecPCM,
			Type:         av.MediaAudio,
			ClockRate:    48000,
			SampleRate:   48000,
			Channels:     2,
			SampleFormat: av.SampleFormatS16,
		},
	}
}
