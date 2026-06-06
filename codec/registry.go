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
	desc = normalizeDescriptor(desc)
	r.upsertDescriptor(desc)
}

func (r *SimpleRegistry) RegisterDecoder(desc Descriptor, factory DecoderFactory) {
	desc.Capabilities.Decode = true
	desc = normalizeDescriptor(desc)
	r.upsertDescriptor(desc)
	if factory != nil {
		r.decoders[desc.ID] = factory
	}
}

func (r *SimpleRegistry) RegisterEncoder(desc Descriptor, factory EncoderFactory) {
	desc.Capabilities.Encode = true
	desc = normalizeDescriptor(desc)
	r.upsertDescriptor(desc)
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
		if r.hasDescriptor(id, ModeDecode) {
			return nil, ErrUnavailable
		}
		return nil, ErrNotFound
	}
	return factory, nil
}

func (r *SimpleRegistry) EncoderFactory(id av.CodecID) (EncoderFactory, error) {
	factory, ok := r.encoders[id]
	if !ok {
		if r.hasDescriptor(id, ModeEncode) {
			return nil, ErrUnavailable
		}
		return nil, ErrNotFound
	}
	return factory, nil
}

func (r *SimpleRegistry) hasDescriptor(id av.CodecID, mode Mode) bool {
	if id == "" {
		return false
	}
	for i := range r.descriptors {
		desc := r.descriptors[i]
		if desc.ID != id {
			continue
		}
		if desc.Supports(mode) {
			return true
		}
	}
	return false
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

func normalizeDescriptor(desc Descriptor) Descriptor {
	desc.ID = pickCodecID(desc.ID, desc.Capabilities.CodecID)
	if desc.Capabilities.CodecID == "" {
		desc.Capabilities.CodecID = desc.ID
	}
	return desc
}

func (r *SimpleRegistry) upsertDescriptor(desc Descriptor) {
	for i := range r.descriptors {
		if !sameDescriptorSlot(r.descriptors[i], desc) {
			continue
		}
		r.descriptors[i] = mergeDescriptors(r.descriptors[i], desc)
		return
	}
	r.descriptors = append(r.descriptors, desc)
}

func sameDescriptorSlot(a Descriptor, b Descriptor) bool {
	return a.ID == b.ID &&
		a.Backend.Name == b.Backend.Name &&
		a.Backend.Module == b.Backend.Module &&
		a.Backend.Package == b.Backend.Package
}

func mergeDescriptors(existing Descriptor, next Descriptor) Descriptor {
	merged := existing
	if next.Name != "" {
		merged.Name = next.Name
	}
	if next.Type != "" {
		merged.Type = next.Type
	}
	merged.Modes = mergeModes(merged.Modes, next.Modes)
	merged.Profiles = mergeStrings(merged.Profiles, next.Profiles)
	merged.Realtime = merged.Realtime || next.Realtime
	merged.Experimental = merged.Experimental || next.Experimental
	merged.Capabilities = mergeCapabilities(merged.Capabilities, next.Capabilities)
	merged.Backend = mergeBackend(merged.Backend, next.Backend)
	return merged
}

func mergeCapabilities(existing Capabilities, next Capabilities) Capabilities {
	merged := existing
	if next.CodecID != "" {
		merged.CodecID = next.CodecID
	}
	if next.Type != "" {
		merged.Type = next.Type
	}
	merged.Decode = merged.Decode || next.Decode
	merged.Encode = merged.Encode || next.Encode
	merged.Realtime = merged.Realtime || next.Realtime
	merged.SampleFormats = mergeStrings(merged.SampleFormats, next.SampleFormats)
	merged.PixelFormats = mergeStrings(merged.PixelFormats, next.PixelFormats)
	merged.RTPPayloads = mergeStrings(merged.RTPPayloads, next.RTPPayloads)
	merged.BuildTags = mergeStrings(merged.BuildTags, next.BuildTags)
	merged.Experimental = merged.Experimental || next.Experimental
	return merged
}

func mergeBackend(existing Backend, next Backend) Backend {
	merged := existing
	if next.Name != "" {
		merged.Name = next.Name
	}
	if next.Module != "" {
		merged.Module = next.Module
	}
	if next.Package != "" {
		merged.Package = next.Package
	}
	if next.Status != "" {
		merged.Status = next.Status
	}
	return merged
}

func mergeModes(existing []Mode, next []Mode) []Mode {
	for _, candidate := range next {
		found := false
		for _, current := range existing {
			if current == candidate {
				found = true
				break
			}
		}
		if !found {
			existing = append(existing, candidate)
		}
	}
	return existing
}

func mergeStrings(existing []string, next []string) []string {
	for _, candidate := range next {
		found := false
		for _, current := range existing {
			if current == candidate {
				found = true
				break
			}
		}
		if !found {
			existing = append(existing, candidate)
		}
	}
	return existing
}
