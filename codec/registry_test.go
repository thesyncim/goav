package codec

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
)

type testDecoderFactory struct{}

func (testDecoderFactory) NewDecoder(context.Context, DecodeConfig) (Decoder, error) {
	return nil, nil
}

type testEncoderFactory struct{}

func (testEncoderFactory) NewEncoder(context.Context, EncodeConfig) (Encoder, error) {
	return nil, nil
}

func TestRegistryFindsExplicitFactories(t *testing.T) {
	registry := NewRegistry(
		WithDecoder(Descriptor{ID: av.CodecOpus, Name: "opus"}, testDecoderFactory{}),
		WithEncoder(Descriptor{ID: av.CodecOpus, Name: "opus"}, testEncoderFactory{}),
	)

	if _, err := registry.DecoderFactory(av.CodecOpus); err != nil {
		t.Fatalf("decoder factory: %v", err)
	}
	if _, err := registry.EncoderFactory(av.CodecOpus); err != nil {
		t.Fatalf("encoder factory: %v", err)
	}

	decoders, err := registry.Find(av.CodecOpus, ModeDecode)
	if err != nil {
		t.Fatalf("find decode: %v", err)
	}
	if len(decoders) != 1 || !decoders[0].Supports(ModeDecode) {
		t.Fatalf("decode descriptors = %+v", decoders)
	}
	encoders, err := registry.Find(av.CodecOpus, ModeEncode)
	if err != nil {
		t.Fatalf("find encode: %v", err)
	}
	if len(encoders) != 1 || !encoders[0].Supports(ModeEncode) {
		t.Fatalf("encode descriptors = %+v", encoders)
	}
	if descriptors := registry.Descriptors(); len(descriptors) != 1 {
		t.Fatalf("descriptors = %d, want merged descriptor", len(descriptors))
	}

	if _, err := registry.DecoderFactory(av.CodecAV1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing decoder error = %v, want ErrNotFound", err)
	}
	if _, err := registry.DecoderFactory(""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("empty decoder error = %v, want ErrNotFound", err)
	}
}

func TestRegistryReportsDescriptorOnlyFactoriesUnavailable(t *testing.T) {
	registry := NewRegistry(
		WithDescriptor(Descriptor{ID: av.CodecH264, Modes: []Mode{ModeDecode}}),
		WithDescriptor(Descriptor{ID: av.CodecVP8, Modes: []Mode{ModeEncode}}),
	)

	if _, err := registry.DecoderFactory(av.CodecH264); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("decoder factory err = %v, want ErrUnavailable", err)
	}
	if _, err := registry.EncoderFactory(av.CodecVP8); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("encoder factory err = %v, want ErrUnavailable", err)
	}
	if _, err := registry.DecoderFactory(av.CodecAV1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing decoder error = %v, want ErrNotFound", err)
	}
}

func TestRegistryDescriptorsAreCopied(t *testing.T) {
	registry := NewRegistry(
		WithDecoder(Descriptor{ID: av.CodecOpus, Name: "original"}, testDecoderFactory{}),
	)

	descriptors := registry.Descriptors()
	descriptors[0].Name = "mutated"

	again := registry.Descriptors()
	if again[0].Name != "original" {
		t.Fatalf("descriptor mutation leaked: %+v", again[0])
	}
}

func TestRegistryNormalizesCapabilityCodecIDForFactories(t *testing.T) {
	registry := NewRegistry(
		WithDecoder(Descriptor{Capabilities: Capabilities{CodecID: av.CodecVP8}}, testDecoderFactory{}),
	)

	if _, err := registry.DecoderFactory(av.CodecVP8); err != nil {
		t.Fatalf("decoder factory: %v", err)
	}
	if descriptors := registry.Descriptors(); len(descriptors) != 1 || descriptors[0].ID != av.CodecVP8 {
		t.Fatalf("descriptors = %+v, want normalized VP8 descriptor", descriptors)
	}
}
