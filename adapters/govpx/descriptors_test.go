package govpx

import (
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
		if len(descriptors[i].Capabilities.BuildTags) != 0 {
			t.Fatalf("build tags = %v", descriptors[i].Capabilities.BuildTags)
		}
		if descriptors[i].Backend.Status != "active" {
			t.Fatalf("backend status = %q", descriptors[i].Backend.Status)
		}
	}
}

func TestRegisterProvidesFactories(t *testing.T) {
	registry := codec.NewRegistry()
	Register(registry)

	for _, id := range []av.CodecID{av.CodecVP8, av.CodecVP9} {
		if _, err := registry.Find(id, codec.ModeDecode); err != nil {
			t.Fatalf("find %s decode: %v", id, err)
		}
		if _, err := registry.Find(id, codec.ModeEncode); err != nil {
			t.Fatalf("find %s encode: %v", id, err)
		}
		if _, err := registry.DecoderFactory(id); err != nil {
			t.Fatalf("%s decoder factory: %v", id, err)
		}
		if _, err := registry.EncoderFactory(id); err != nil {
			t.Fatalf("%s encoder factory: %v", id, err)
		}
	}
}
