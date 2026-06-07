package filter

import (
	"testing"

	"github.com/thesyncim/goav/av"
)

func TestRegistryFactory(t *testing.T) {
	factory := &fakeFactory{}
	registry := NewRegistry()
	registry.RegisterFactory(Descriptor{
		Name:          FactoryResize,
		Input:         av.MediaVideo,
		Output:        av.MediaVideo,
		PixelFormats:  []string{av.PixelFormatI420},
		SampleFormats: []string{av.SampleFormatS16},
		ResizeModes:   []ResizeMode{ResizeExact},
		Realtime:      true,
		Stateless:     true,
		Metadata:      av.Metadata{"backend": "test"},
	}, factory)

	got, err := registry.Factory(FactoryResize)
	if err != nil {
		t.Fatal(err)
	}
	if got != factory {
		t.Fatalf("factory = %p, want %p", got, factory)
	}
	desc, err := registry.Descriptor(FactoryResize)
	if err != nil {
		t.Fatal(err)
	}
	if desc.Name != FactoryResize ||
		desc.Input != av.MediaVideo ||
		desc.Output != av.MediaVideo ||
		len(desc.PixelFormats) != 1 ||
		desc.PixelFormats[0] != av.PixelFormatI420 ||
		len(desc.SampleFormats) != 1 ||
		desc.SampleFormats[0] != av.SampleFormatS16 ||
		len(desc.ResizeModes) != 1 ||
		desc.ResizeModes[0] != ResizeExact ||
		!desc.Realtime ||
		!desc.Stateless ||
		desc.Metadata["backend"] != "test" {
		t.Fatalf("descriptor = %+v", desc)
	}
	desc.PixelFormats[0] = av.PixelFormatYUV420P
	desc.SampleFormats[0] = av.SampleFormatF32
	desc.ResizeModes[0] = ResizeFill
	desc.Metadata["backend"] = "mutated"
	again, err := registry.Descriptor(FactoryResize)
	if err != nil {
		t.Fatal(err)
	}
	if again.PixelFormats[0] != av.PixelFormatI420 ||
		again.SampleFormats[0] != av.SampleFormatS16 ||
		again.ResizeModes[0] != ResizeExact ||
		again.Metadata["backend"] != "test" {
		t.Fatalf("descriptor was not cloned: %+v", again)
	}
}

func TestRegistryFactoryNotFound(t *testing.T) {
	_, err := NewRegistry().Factory(FactoryResize)
	if err != ErrNotFound {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	_, err = NewRegistry().Descriptor(FactoryResize)
	if err != ErrNotFound {
		t.Fatalf("descriptor err = %v, want ErrNotFound", err)
	}
}
