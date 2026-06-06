package filter

import "testing"

func TestRegistryFactory(t *testing.T) {
	factory := &fakeFactory{}
	registry := NewRegistry(WithFactory(Descriptor{Name: FactoryResize}, factory))

	got, err := registry.Factory(FactoryResize)
	if err != nil {
		t.Fatal(err)
	}
	if got != factory {
		t.Fatalf("factory = %p, want %p", got, factory)
	}
	if len(registry.Descriptors()) != 1 {
		t.Fatalf("descriptors = %d, want 1", len(registry.Descriptors()))
	}
}

func TestRegistryFactoryNotFound(t *testing.T) {
	_, err := NewRegistry().Factory(FactoryResize)
	if err != ErrNotFound {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}
