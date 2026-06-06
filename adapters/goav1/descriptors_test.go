package goav1

import (
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

func TestDescriptor(t *testing.T) {
	desc := Descriptor()
	if desc.ID != av.CodecAV1 || desc.Backend.Module != "github.com/thesyncim/goav1" {
		t.Fatalf("descriptor = %+v", desc)
	}
	if desc.Capabilities.Encode {
		t.Fatal("goav1 descriptor should not claim encode support yet")
	}
	if len(desc.Capabilities.BuildTags) != 1 || desc.Capabilities.BuildTags[0] != "goav_goav1" {
		t.Fatalf("build tags = %v", desc.Capabilities.BuildTags)
	}
	if desc.Backend.Status != "planned-build-tagged" {
		t.Fatalf("backend status = %q", desc.Backend.Status)
	}
}

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
