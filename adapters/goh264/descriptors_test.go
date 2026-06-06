package goh264

import (
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

func TestDescriptor(t *testing.T) {
	desc := Descriptor()
	if desc.ID != av.CodecH264 || desc.Backend.Module != "github.com/thesyncim/goh264" {
		t.Fatalf("descriptor = %+v", desc)
	}
	if len(desc.Capabilities.BuildTags) != 1 || desc.Capabilities.BuildTags[0] != "goav_goh264" {
		t.Fatalf("build tags = %+v", desc.Capabilities.BuildTags)
	}
}

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
