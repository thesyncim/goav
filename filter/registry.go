package filter

type SimpleRegistry struct {
	factories map[string]Factory
}

func NewRegistry() *SimpleRegistry {
	return &SimpleRegistry{
		factories: make(map[string]Factory),
	}
}

func (r *SimpleRegistry) RegisterFactory(desc Descriptor, factory Factory) {
	if factory != nil {
		r.factories[desc.Name] = factory
	}
}

func (r *SimpleRegistry) Factory(name string) (Factory, error) {
	factory, ok := r.factories[name]
	if !ok {
		return nil, ErrNotFound
	}
	return factory, nil
}
