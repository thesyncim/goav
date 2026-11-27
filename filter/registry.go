package filter

import (
	"cmp"
	"maps"
	"slices"
)

// Registry is the per-runtime filter registry: factories and
// capability descriptors keyed by filter name. Registration is last-wins; it
// is populated at runtime construction and read-only afterwards.
type Registry struct {
	factories   map[string]Factory
	descriptors map[string]Descriptor
}

// NewRegistry returns an empty filter registry.
func NewRegistry() *Registry {
	return &Registry{
		factories:   make(map[string]Factory),
		descriptors: make(map[string]Descriptor),
	}
}

// RegisterFactory records a filter factory under its descriptor's name; last
// registration wins.
func (r *Registry) RegisterFactory(desc Descriptor, factory Factory) {
	if factory != nil {
		r.factories[desc.Name] = factory
		r.descriptors[desc.Name] = cloneDescriptor(desc)
	}
}

// Factory returns the factory registered under name, or ErrNotFound.
func (r *Registry) Factory(name string) (Factory, error) {
	factory, ok := r.factories[name]
	if !ok {
		return nil, ErrNotFound
	}
	return factory, nil
}

// Descriptors lists every registered filter descriptor sorted by name, so
// capability-driven selection (the shape solver) is deterministic.
func (r *Registry) Descriptors() []Descriptor {
	out := make([]Descriptor, 0, len(r.descriptors))
	for _, desc := range r.descriptors {
		out = append(out, cloneDescriptor(desc))
	}
	slices.SortFunc(out, func(a, b Descriptor) int { return cmp.Compare(a.Name, b.Name) })
	return out
}

// Descriptor returns the capability descriptor registered under name, or
// ErrNotFound.
func (r *Registry) Descriptor(name string) (Descriptor, error) {
	desc, ok := r.descriptors[name]
	if !ok {
		return Descriptor{}, ErrNotFound
	}
	return cloneDescriptor(desc), nil
}

func cloneDescriptor(desc Descriptor) Descriptor {
	cloned := desc
	cloned.PixelFormats = slices.Clone(desc.PixelFormats)
	cloned.SampleFormats = slices.Clone(desc.SampleFormats)
	cloned.ResizeModes = slices.Clone(desc.ResizeModes)
	cloned.Metadata = maps.Clone(desc.Metadata)
	return cloned
}
