package filter

import (
	"testing"

	"github.com/thesyncim/goav/av"
)

func TestRegistryFactory(t *testing.T) {
	factory := &fakeFactory{}
	registry := NewRegistry()
	registry.RegisterFactory(Descriptor{
		Name:      FactoryResize,
		Input:     av.MediaVideo,
		Output:    av.MediaVideo,
		Realtime:  true,
		Stateless: true,
		Metadata:  av.Metadata{"backend": "test"},
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
		!desc.Realtime ||
		!desc.Stateless ||
		desc.Metadata["backend"] != "test" {
		t.Fatalf("descriptor = %+v", desc)
	}
	desc.Metadata["backend"] = "mutated"
	again, err := registry.Descriptor(FactoryResize)
	if err != nil {
		t.Fatal(err)
	}
	if again.Metadata["backend"] != "test" {
		t.Fatalf("descriptor metadata was not cloned: %+v", again.Metadata)
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
