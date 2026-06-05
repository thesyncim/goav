package codec

import (
	"errors"

	"github.com/thesyncim/goav/av"
)

var ErrNotFound = errors.New("codec: not found")

type RegistryOption func(*SimpleRegistry)

type SimpleRegistry struct {
	descriptors []Descriptor
	decoders    map[av.CodecID]DecoderFactory
	encoders    map[av.CodecID]EncoderFactory
}

func NewRegistry(options ...RegistryOption) *SimpleRegistry {
	r := &SimpleRegistry{
		decoders: make(map[av.CodecID]DecoderFactory),
		encoders: make(map[av.CodecID]EncoderFactory),
	}
	for _, option := range options {
		option(r)
	}
	return r
}

func WithDecoder(desc Descriptor, factory DecoderFactory) RegistryOption {
	return func(r *SimpleRegistry) {
		r.RegisterDecoder(desc, factory)
	}
}

func WithEncoder(desc Descriptor, factory EncoderFactory) RegistryOption {
	return func(r *SimpleRegistry) {
		r.RegisterEncoder(desc, factory)
	}
}

func WithDescriptor(desc Descriptor) RegistryOption {
	return func(r *SimpleRegistry) {
		r.RegisterDescriptor(desc)
	}
}

func (r *SimpleRegistry) RegisterDescriptor(desc Descriptor) {
	desc.ID = pickCodecID(desc.ID, desc.Capabilities.CodecID)
	if desc.Capabilities.CodecID == "" {
		desc.Capabilities.CodecID = desc.ID
	}
	r.descriptors = append(r.descriptors, desc)
}

func (r *SimpleRegistry) RegisterDecoder(desc Descriptor, factory DecoderFactory) {
	desc.Capabilities.Decode = true
	desc.ID = pickCodecID(desc.ID, desc.Capabilities.CodecID)
	if desc.Capabilities.CodecID == "" {
		desc.Capabilities.CodecID = desc.ID
	}
	r.descriptors = append(r.descriptors, desc)
	if factory != nil {
		r.decoders[desc.ID] = factory
	}
}

func (r *SimpleRegistry) RegisterEncoder(desc Descriptor, factory EncoderFactory) {
	desc.Capabilities.Encode = true
	desc.ID = pickCodecID(desc.ID, desc.Capabilities.CodecID)
	if desc.Capabilities.CodecID == "" {
		desc.Capabilities.CodecID = desc.ID
	}
	r.descriptors = append(r.descriptors, desc)
	if factory != nil {
		r.encoders[desc.ID] = factory
	}
}

func (r *SimpleRegistry) Descriptors() []Descriptor {
	out := make([]Descriptor, len(r.descriptors))
	copy(out, r.descriptors)
	return out
}

func (r *SimpleRegistry) Find(id av.CodecID, mode Mode) ([]Descriptor, error) {
	var out []Descriptor
	for i := range r.descriptors {
		desc := r.descriptors[i]
		if id != "" && desc.ID != id {
			continue
		}
		if !desc.Supports(mode) {
			continue
		}
		out = append(out, desc)
	}
	if len(out) == 0 {
		return nil, ErrNotFound
	}
	return out, nil
}

func (r *SimpleRegistry) DecoderFactory(id av.CodecID) (DecoderFactory, error) {
	factory, ok := r.decoders[id]
	if !ok {
		return nil, ErrNotFound
	}
	return factory, nil
}

func (r *SimpleRegistry) EncoderFactory(id av.CodecID) (EncoderFactory, error) {
	factory, ok := r.encoders[id]
	if !ok {
		return nil, ErrNotFound
	}
	return factory, nil
}

func (d Descriptor) Supports(mode Mode) bool {
	switch mode {
	case ModeDecode:
		if d.Capabilities.Decode {
			return true
		}
	case ModeEncode:
		if d.Capabilities.Encode {
			return true
		}
	}
	for _, candidate := range d.Modes {
		if candidate == mode {
			return true
		}
	}
	return false
}

func pickCodecID(a av.CodecID, b av.CodecID) av.CodecID {
	if a != "" {
		return a
	}
	return b
}
