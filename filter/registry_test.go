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

func TestRegistryDescriptorsSortedAndCloned(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterFactory(Descriptor{
		Name:        FactoryResample,
		Input:       av.MediaAudio,
		Output:      av.MediaAudio,
		ResizeModes: []ResizeMode{ResizePassthrough},
		Realtime:    true,
		Stateless:   true,
		Metadata:    av.Metadata{"backend": "audio"},
	}, &fakeFactory{})
	registry.RegisterFactory(Descriptor{
		Name:         FactoryResize,
		Input:        av.MediaVideo,
		Output:       av.MediaVideo,
		PixelFormats: []string{av.PixelFormatYUV420P},
	}, &fakeFactory{})

	descriptors := registry.Descriptors()
	if len(descriptors) != 2 {
		t.Fatalf("len = %d, want 2", len(descriptors))
	}
	if descriptors[0].Name != FactoryResample || descriptors[1].Name != FactoryResize {
		t.Fatalf("descriptors not sorted by name: %+v", descriptors)
	}

	descriptors[0].ResizeModes[0] = ResizeFill
	descriptors[0].Metadata["backend"] = "mutated"
	descriptors[1].PixelFormats[0] = av.PixelFormatI420
	again := registry.Descriptors()
	if again[0].ResizeModes[0] != ResizePassthrough ||
		again[0].Metadata["backend"] != "audio" ||
		again[1].PixelFormats[0] != av.PixelFormatYUV420P {
		t.Fatalf("descriptor list was not cloned: %+v", again)
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
