//go:build goav_goav1

package goav1

import (
	"context"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

func TestTaggedRegisterDecoderFactory(t *testing.T) {
	registry := codec.NewRegistry()
	Register(registry)

	descriptors, err := registry.Find(av.CodecAV1, codec.ModeDecode)
	if err != nil {
		t.Fatalf("find AV1: %v", err)
	}
	if len(descriptors) != 1 || descriptors[0].Backend.Status != "active-build-tagged" {
		t.Fatalf("descriptors = %+v", descriptors)
	}
	if _, err := registry.DecoderFactory(av.CodecAV1); err != nil {
		t.Fatalf("decoder factory: %v", err)
	}
}

func TestTaggedDecoderFactoryRequiresCallerOwnedState(t *testing.T) {
	factory := NewDecoderFactory()
	_, err := factory.NewDecoder(context.Background(), codec.DecodeConfig{
		Stream: av.Stream{Codec: av.CodecParameters{ID: av.CodecAV1}},
	})
	if err != codec.ErrUnsupportedFormat {
		t.Fatalf("err = %v, want ErrUnsupportedFormat", err)
	}
}
