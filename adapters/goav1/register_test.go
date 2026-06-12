package goav1

import (
	"context"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

func TestRegisterDecoderFactory(t *testing.T) {
	registry := codec.NewRegistry()
	Register(registry)

	descriptors, err := registry.Find(av.CodecAV1, codec.ModeDecode)
	if err != nil {
		t.Fatalf("find AV1: %v", err)
	}
	if len(descriptors) != 1 || descriptors[0].Backend.Status != "active" {
		t.Fatalf("descriptors = %+v", descriptors)
	}
	if _, err := registry.DecoderFactory(av.CodecAV1); err != nil {
		t.Fatalf("decoder factory: %v", err)
	}
	encoders, err := registry.Find(av.CodecAV1, codec.ModeEncode)
	if err != nil {
		t.Fatalf("find AV1 encode: %v", err)
	}
	if len(encoders) != 1 || !encoders[0].Supports(codec.ModeEncode) {
		t.Fatalf("encoders = %+v", encoders)
	}
	if _, err := registry.EncoderFactory(av.CodecAV1); err != nil {
		t.Fatalf("encoder factory: %v", err)
	}
}

func TestDecoderFactoryRequiresCallerOwnedState(t *testing.T) {
	factory := NewDecoderFactory()
	_, err := factory.NewDecoder(context.Background(), codec.DecodeConfig{
		Stream: av.Stream{Codec: av.CodecParameters{ID: av.CodecAV1}},
	})
	if err != codec.ErrUnsupportedFormat {
		t.Fatalf("err = %v, want ErrUnsupportedFormat", err)
	}
}

func TestDecoderFactoryProvidesRuntimeState(t *testing.T) {
	factory := NewDecoderFactory()
	state, err := factory.NewDecodeState(context.Background(), codec.DecodeConfig{
		Stream: av.Stream{
			Codec: av.CodecParameters{
				ID:          av.CodecAV1,
				Width:       16,
				Height:      16,
				PixelFormat: av.PixelFormatGray8,
			},
		},
		Bounds: codec.DecodeBounds{
			MaxFramesPerInput:   1,
			MaxEventsPerInput:   1,
			MaxRequestsPerInput: 1,
			MaxWidth:            16,
			MaxHeight:           16,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	decoderState, ok := state.(*DecoderState)
	if !ok {
		t.Fatalf("state = %T", state)
	}
	if decoderState.Format().MonoChrome != true ||
		decoderState.ScratchSize().Events < defaultRuntimeEvents ||
		decoderState.ScratchSize().Event.Plan.JobCount < 1 {
		t.Fatalf("state format=%+v scratch=%+v", decoderState.Format(), decoderState.ScratchSize())
	}
	decoderState.Close()
}
