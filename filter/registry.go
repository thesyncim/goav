package filter

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
