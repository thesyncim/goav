package codec

import (
	"errors"
	"slices"
	"strings"

	"github.com/thesyncim/goav/av"
)

// ErrNotFound reports a codec id (or id+mode) with no registered descriptor.
var ErrNotFound = errors.New("codec: not found")

// SimpleRegistry is the per-runtime codec registry: descriptors for
// capability checks plus the decoder and encoder backends serving each codec.
// It holds many backends per codec, keyed by codec, backend name, and mode;
// registering the same identity again replaces it. Select picks the backend
// for one request. It is populated at runtime construction and read-only
// afterwards.
type SimpleRegistry struct {
	descriptors []Descriptor
	backends    []registered
	preference  map[preferenceKey][]string
	sequence    int
}

// registered is one backend: the descriptor it registered with, its
// factory, and when it registered (later registrations rank first among
// equals, so layering options overrides defaults).
type registered struct {
	desc     Descriptor
	mode     Mode
	decoder  DecoderFactory
	encoder  EncoderFactory
	sequence int
}

// NewRegistry returns an empty codec registry.
func NewRegistry() *SimpleRegistry {
	return &SimpleRegistry{}
}

// RegisterDescriptor records a codec's capabilities without an
// implementation, so validation recognizes the codec.
func (r *SimpleRegistry) RegisterDescriptor(desc Descriptor) {
	r.upsertDescriptor(desc)
}

// RegisterDecoder records the descriptor (decode mode added) and adds its
// backend as a decoder for the codec, replacing a decoder registered under
// the same backend name.
func (r *SimpleRegistry) RegisterDecoder(desc Descriptor, factory DecoderFactory) {
	desc.Modes = mergeModes(desc.Modes, []Mode{ModeDecode})
	r.upsertDescriptor(desc)
	if factory != nil {
		r.addBackend(registered{desc: cloneDescriptor(desc), mode: ModeDecode, decoder: factory})
	}
}

// RegisterEncoder records the descriptor (encode mode added) and adds its
// backend as an encoder for the codec, replacing an encoder registered under
// the same backend name.
func (r *SimpleRegistry) RegisterEncoder(desc Descriptor, factory EncoderFactory) {
	desc.Modes = mergeModes(desc.Modes, []Mode{ModeEncode})
	r.upsertDescriptor(desc)
	if factory != nil {
		r.addBackend(registered{desc: cloneDescriptor(desc), mode: ModeEncode, encoder: factory})
	}
}

func (r *SimpleRegistry) addBackend(backend registered) {
	r.sequence++
	backend.sequence = r.sequence
	r.backends = slices.DeleteFunc(r.backends, func(existing registered) bool {
		return existing.mode == backend.mode && existing.desc.ID == backend.desc.ID && strings.EqualFold(existing.desc.Backend.Name, backend.desc.Backend.Name)
	})
	r.backends = append(r.backends, backend)
}

// Prefer sets the order Select tries the backends of codec id in mode when a
// request names none: backend names (openh264) or kinds (hardware, native,
// go), most preferred first. An empty id or mode applies to every codec or
// mode; the most specific order wins. Backends the order leaves out follow
// in the default order: no kind, hardware, native, go, later registrations
// first within a kind.
func (r *SimpleRegistry) Prefer(id av.CodecID, mode Mode, order ...string) {
	if r.preference == nil {
		r.preference = map[preferenceKey][]string{}
	}
	r.preference[preferenceKey{id, mode}] = slices.Clone(order)
}

type preferenceKey struct {
	id   av.CodecID
	mode Mode
}

// preferenceFor is the most specific order set for id in mode.
func (r *SimpleRegistry) preferenceFor(id av.CodecID, mode Mode) []string {
	for _, key := range []preferenceKey{{id, mode}, {id, ""}, {"", mode}, {"", ""}} {
		if order, ok := r.preference[key]; ok {
			return order
		}
	}
	return nil
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

// DecoderFactory returns the decoder factory Select picks for a codec id with
// no other requirement: ErrUnavailable when only descriptors are registered,
// ErrNotFound when nothing is.
func (r *SimpleRegistry) DecoderFactory(id av.CodecID) (DecoderFactory, error) {
	choice, err := r.Select(Need{ID: id, Mode: ModeDecode})
	if err != nil {
		return nil, errors.Unwrap(err)
	}
	return choice.Decoder, nil
}

// EncoderFactory returns the encoder factory Select picks for a codec id with
// no other requirement: ErrUnavailable when only descriptors are registered,
// ErrNotFound when nothing is.
func (r *SimpleRegistry) EncoderFactory(id av.CodecID) (EncoderFactory, error) {
	choice, err := r.Select(Need{ID: id, Mode: ModeEncode})
	if err != nil {
		return nil, errors.Unwrap(err)
	}
	return choice.Encoder, nil
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
	return slices.Contains(d.Modes, mode)
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
	cloned.Knobs = slices.Clone(desc.Knobs)
	cloned.RawKnobs = slices.Clone(desc.RawKnobs)
	return cloned
}

// cloneCapabilities copies the struct first, so a field added later is
// carried even before it gets a deep copy here.
func cloneCapabilities(capabilities Capabilities) Capabilities {
	cloned := capabilities
	cloned.SampleFormats = append([]string(nil), capabilities.SampleFormats...)
	cloned.PixelFormats = append([]string(nil), capabilities.PixelFormats...)
	cloned.SampleRates = append([]int(nil), capabilities.SampleRates...)
	cloned.BitDepths = append([]int(nil), capabilities.BitDepths...)
	cloned.RTPPayloads = append([]string(nil), capabilities.RTPPayloads...)
	cloned.BuildTags = append([]string(nil), capabilities.BuildTags...)
	return cloned
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
	if next.Knobs != nil {
		merged.Knobs = slices.Clone(next.Knobs)
	}
	if next.RawKnobs != nil {
		merged.RawKnobs = slices.Clone(next.RawKnobs)
	}
	return merged
}

func mergeCapabilities(existing Capabilities, next Capabilities) Capabilities {
	merged := existing
	merged.SampleFormats = mergeStrings(merged.SampleFormats, next.SampleFormats)
	merged.PixelFormats = mergeStrings(merged.PixelFormats, next.PixelFormats)
	merged.BitDepths = mergeInts(merged.BitDepths, next.BitDepths)
	if next.MaxWidth != 0 {
		merged.MaxWidth = next.MaxWidth
	}
	if next.MaxHeight != 0 {
		merged.MaxHeight = next.MaxHeight
	}
	merged.RTPPayloads = mergeStrings(merged.RTPPayloads, next.RTPPayloads)
	merged.BuildTags = mergeStrings(merged.BuildTags, next.BuildTags)
	return merged
}

func mergeBackend(existing BackendInfo, next BackendInfo) BackendInfo {
	merged := existing
	if next.Name != "" {
		merged.Name = next.Name
	}
	if next.Kind != "" {
		merged.Kind = next.Kind
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
		found := slices.Contains(existing, candidate)
		if !found {
			existing = append(existing, candidate)
		}
	}
	return existing
}

func mergeStrings(existing []string, next []string) []string {
	for _, candidate := range next {
		found := slices.Contains(existing, candidate)
		if !found {
			existing = append(existing, candidate)
		}
	}
	return existing
}

func mergeInts(existing []int, next []int) []int {
	for _, candidate := range next {
		if !slices.Contains(existing, candidate) {
			existing = append(existing, candidate)
		}
	}
	return existing
}
