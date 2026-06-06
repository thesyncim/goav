package filter

type RegistryOption func(*SimpleRegistry)

type SimpleRegistry struct {
	descriptors []Descriptor
	factories   map[string]Factory
}

func NewRegistry(options ...RegistryOption) *SimpleRegistry {
	registry := &SimpleRegistry{
		factories: make(map[string]Factory),
	}
	for _, option := range options {
		option(registry)
	}
	return registry
}

func WithFactory(desc Descriptor, factory Factory) RegistryOption {
	return func(registry *SimpleRegistry) {
		registry.RegisterFactory(desc, factory)
	}
}

func WithDescriptor(desc Descriptor) RegistryOption {
	return func(registry *SimpleRegistry) {
		registry.RegisterDescriptor(desc)
	}
}

func (r *SimpleRegistry) RegisterDescriptor(desc Descriptor) {
	r.descriptors = append(r.descriptors, desc)
}

func (r *SimpleRegistry) RegisterFactory(desc Descriptor, factory Factory) {
	r.descriptors = append(r.descriptors, desc)
	if factory != nil {
		r.factories[desc.Name] = factory
	}
}

func (r *SimpleRegistry) Descriptors() []Descriptor {
	out := make([]Descriptor, len(r.descriptors))
	copy(out, r.descriptors)
	return out
}

func (r *SimpleRegistry) Factory(name string) (Factory, error) {
	factory, ok := r.factories[name]
	if !ok {
		return nil, ErrNotFound
	}
	return factory, nil
}
