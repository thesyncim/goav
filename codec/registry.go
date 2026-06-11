package codec

import (
	"errors"

	"github.com/thesyncim/goav/av"
)

// ErrNotFound reports a codec id (or id+mode) with no registered descriptor.
var ErrNotFound = errors.New("codec: not found")

// SimpleRegistry is the per-runtime codec registry: descriptors for
// capability checks plus decoder/encoder factories keyed by codec id.
// Registration is last-wins, so layering options can override defaults. It is
// populated at runtime construction and read-only afterwards.
type SimpleRegistry struct {
	descriptors []Descriptor
	decoders    map[av.CodecID]DecoderFactory
	encoders    map[av.CodecID]EncoderFactory
}

// NewRegistry returns an empty codec registry.
func NewRegistry() *SimpleRegistry {
	return &SimpleRegistry{
		decoders: make(map[av.CodecID]DecoderFactory),
		encoders: make(map[av.CodecID]EncoderFactory),
	}
}

// RegisterDescriptor records a codec's capabilities without an
// implementation, so validation recognizes the codec.
func (r *SimpleRegistry) RegisterDescriptor(desc Descriptor) {
	r.upsertDescriptor(desc)
}

// RegisterDecoder records the descriptor (decode mode added) and the decoder
// factory for its codec id; last registration wins.
func (r *SimpleRegistry) RegisterDecoder(desc Descriptor, factory DecoderFactory) {
	desc.Modes = mergeModes(desc.Modes, []Mode{ModeDecode})
	r.upsertDescriptor(desc)
	if factory != nil {
		r.decoders[desc.ID] = factory
	}
}

// RegisterEncoder records the descriptor (encode mode added) and the encoder
// factory for its codec id; last registration wins.
func (r *SimpleRegistry) RegisterEncoder(desc Descriptor, factory EncoderFactory) {
	desc.Modes = mergeModes(desc.Modes, []Mode{ModeEncode})
	r.upsertDescriptor(desc)
	if factory != nil {
		r.encoders[desc.ID] = factory
	}
}

// Descriptors returns a copy of every registered descriptor.
func (r *SimpleRegistry) Descriptors() []Descriptor {
	out := make([]Descriptor, len(r.descriptors))
	for i := range r.descriptors {
		out[i] = cloneDescriptor(r.descriptors[i])
	}
	return out
}

// Find returns the descriptors matching the codec id (empty matches all)
// that support mode, or ErrNotFound when none do.
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
		out = append(out, cloneDescriptor(desc))
	}
	if len(out) == 0 {
		return nil, ErrNotFound
	}
	return out, nil
}

// DecoderFactory returns the decoder factory for a codec id: ErrUnavailable
// when only the descriptor is registered, ErrNotFound when nothing is.
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

// EncoderFactory returns the encoder factory for a codec id: ErrUnavailable
// when only the descriptor is registered, ErrNotFound when nothing is.
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

// Supports reports whether the descriptor lists the given mode.
func (d Descriptor) Supports(mode Mode) bool {
	for _, candidate := range d.Modes {
		if candidate == mode {
			return true
		}
	}
	return false
}

func (r *SimpleRegistry) upsertDescriptor(desc Descriptor) {
	desc = cloneDescriptor(desc)
	for i := range r.descriptors {
		if !sameDescriptorSlot(r.descriptors[i], desc) {
			continue
		}
		r.descriptors[i] = mergeDescriptors(r.descriptors[i], desc)
		return
	}
	r.descriptors = append(r.descriptors, desc)
}

func cloneDescriptor(desc Descriptor) Descriptor {
	cloned := desc
	cloned.Modes = append([]Mode(nil), desc.Modes...)
	cloned.Profiles = append([]string(nil), desc.Profiles...)
	cloned.Capabilities = cloneCapabilities(desc.Capabilities)
	return cloned
}

func cloneCapabilities(capabilities Capabilities) Capabilities {
	return Capabilities{
		SampleFormats: append([]string(nil), capabilities.SampleFormats...),
		PixelFormats:  append([]string(nil), capabilities.PixelFormats...),
		RTPPayloads:   append([]string(nil), capabilities.RTPPayloads...),
		BuildTags:     append([]string(nil), capabilities.BuildTags...),
	}
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
	merged.SampleFormats = mergeStrings(merged.SampleFormats, next.SampleFormats)
	merged.PixelFormats = mergeStrings(merged.PixelFormats, next.PixelFormats)
	merged.RTPPayloads = mergeStrings(merged.RTPPayloads, next.RTPPayloads)
	merged.BuildTags = mergeStrings(merged.BuildTags, next.BuildTags)
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
