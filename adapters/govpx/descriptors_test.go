package govpx

import (
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

func TestDescriptors(t *testing.T) {
	descriptors := Descriptors()
	if len(descriptors) != 2 {
		t.Fatalf("descriptors = %d, want 2", len(descriptors))
	}
	for i := range descriptors {
		if descriptors[i].Backend.Module != "github.com/thesyncim/govpx" {
			t.Fatalf("descriptor = %+v", descriptors[i])
		}
	}
}

func TestRegisterDescriptorsOnly(t *testing.T) {
	registry := codec.NewRegistry()
	Register(registry)

	found, err := registry.Find(av.CodecVP8, codec.ModeDecode)
	if err != nil {
		t.Fatalf("find VP8: %v", err)
	}
	if len(found) != 1 {
		t.Fatalf("found = %d, want 1", len(found))
	}
	if _, err := registry.DecoderFactory(av.CodecVP8); !errors.Is(err, codec.ErrUnavailable) {
		t.Fatalf("factory err = %v, want ErrUnavailable", err)
	}
}
