package filter

import "sort"

type SimpleRegistry struct {
	factories   map[string]Factory
	descriptors map[string]Descriptor
}

func NewRegistry() *SimpleRegistry {
	return &SimpleRegistry{
		factories:   make(map[string]Factory),
		descriptors: make(map[string]Descriptor),
	}
}

func (r *SimpleRegistry) RegisterFactory(desc Descriptor, factory Factory) {
	if factory != nil {
		r.factories[desc.Name] = factory
		r.descriptors[desc.Name] = cloneDescriptor(desc)
	}
}

func (r *SimpleRegistry) Factory(name string) (Factory, error) {
	factory, ok := r.factories[name]
	if !ok {
		return nil, ErrNotFound
	}
	return factory, nil
}

// Descriptors lists every registered filter descriptor sorted by name, so
// capability-driven selection (the shape solver) is deterministic.
func (r *SimpleRegistry) Descriptors() []Descriptor {
	out := make([]Descriptor, 0, len(r.descriptors))
	for name := range r.descriptors {
		out = append(out, cloneDescriptor(r.descriptors[name]))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (r *SimpleRegistry) Descriptor(name string) (Descriptor, error) {
	desc, ok := r.descriptors[name]
	if !ok {
		return Descriptor{}, ErrNotFound
	}
	return cloneDescriptor(desc), nil
}

func cloneDescriptor(desc Descriptor) Descriptor {
	cloned := desc
	if desc.PixelFormats != nil {
		cloned.PixelFormats = append([]string(nil), desc.PixelFormats...)
	}
	if desc.SampleFormats != nil {
		cloned.SampleFormats = append([]string(nil), desc.SampleFormats...)
	}
	if desc.ResizeModes != nil {
		cloned.ResizeModes = append([]ResizeMode(nil), desc.ResizeModes...)
	}
	if desc.Metadata == nil {
		return cloned
	}
	cloned.Metadata = make(map[string]string, len(desc.Metadata))
	for key, value := range desc.Metadata {
		cloned.Metadata[key] = value
	}
	return cloned
}
