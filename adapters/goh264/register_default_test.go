//go:build !goav_goh264

package goh264

import (
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

func TestRegisterDescriptorOnly(t *testing.T) {
	registry := codec.NewRegistry()
	Register(registry)

	if _, err := registry.Find(av.CodecH264, codec.ModeDecode); err != nil {
		t.Fatalf("find H264: %v", err)
	}
	if _, err := registry.DecoderFactory(av.CodecH264); !errors.Is(err, codec.ErrUnavailable) {
		t.Fatalf("factory err = %v, want ErrUnavailable", err)
	}
}
