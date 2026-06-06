//go:build !goav_goav1

package goav1

import (
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

func TestRegisterDescriptorOnly(t *testing.T) {
	registry := codec.NewRegistry()
	Register(registry)

	if _, err := registry.Find(av.CodecAV1, codec.ModeDecode); err != nil {
		t.Fatalf("find AV1: %v", err)
	}
	if _, err := registry.DecoderFactory(av.CodecAV1); !errors.Is(err, codec.ErrUnavailable) {
		t.Fatalf("factory err = %v, want ErrUnavailable", err)
	}
}
