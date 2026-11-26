package format

import (
	"cmp"
	"context"
	"errors"
	"maps"
	"slices"

	"github.com/thesyncim/goav/av"
)

// Format sentinels, matched with errors.Is.
var (
	// ErrNotFound reports a format with no registered adapter, or a probe with
	// no matching rule.
	ErrNotFound = errors.New("format: not found")
	// ErrNilDemuxer reports a nil demuxer where one is required.
	ErrNilDemuxer = errors.New("format: nil demuxer")
	// ErrNilMuxer reports a nil muxer where one is required.
	ErrNilMuxer = errors.New("format: nil muxer")
	// ErrNilPacket reports a nil packet handed to a mux write.
	ErrNilPacket = errors.New("format: nil packet")
	// ErrResultFull reports a result buffer at capacity (AddEvent).
	ErrResultFull = errors.New("format: result capacity full")
)

type factoryStore[F any] struct {
	factories   map[av.FormatID]F
	descriptors map[av.FormatID]Descriptor
}

func newFactoryStore[F any]() factoryStore[F] {
	return factoryStore[F]{
		factories:   make(map[av.FormatID]F),
		descriptors: make(map[av.FormatID]Descriptor),
	}
}

func (s *factoryStore[F]) register(desc Descriptor, factory F) {
	if desc.Format != "" {
		s.factories[desc.Format] = factory
		s.descriptors[desc.Format] = cloneDescriptor(desc)
	}
}

func (s *factoryStore[F]) factory(format av.FormatID) (F, error) {
	factory, ok := s.factories[format]
	if !ok {
		var zero F
		return zero, ErrNotFound
	}
	return factory, nil
}

func (s *factoryStore[F]) descriptor(format av.FormatID) (Descriptor, error) {
	desc, ok := s.descriptors[format]
	if !ok {
		return Descriptor{}, ErrNotFound
	}
	return cloneDescriptor(desc), nil
}

func (s *factoryStore[F]) allDescriptors() []Descriptor {
	out := make([]Descriptor, 0, len(s.descriptors))
	for _, desc := range s.descriptors {
		out = append(out, cloneDescriptor(desc))
	}
	slices.SortFunc(out, func(a, b Descriptor) int { return cmp.Compare(a.Format, b.Format) })
	return out
}

// SimpleRegistry is the per-runtime format registry: probers for detection
// plus demuxer/muxer factories and capability descriptors keyed by format id.
// Registration is last-wins; it is populated at runtime construction and
// read-only afterwards.
type SimpleRegistry struct {
	probers  []Prober
	demuxers factoryStore[DemuxerFactory]
	muxers   factoryStore[MuxerFactory]
}

// NewRegistry returns an empty format registry.
func NewRegistry() *SimpleRegistry {
	return &SimpleRegistry{
		demuxers: newFactoryStore[DemuxerFactory](),
		muxers:   newFactoryStore[MuxerFactory](),
	}
}

// RegisterProber adds a format prober; Probe consults every registered one.
func (r *SimpleRegistry) RegisterProber(prober Prober) {
	if prober != nil {
		r.probers = append(r.probers, prober)
	}
}

// RegisterDemuxer records a demuxer factory for a format id; last
// registration wins.
func (r *SimpleRegistry) RegisterDemuxer(format av.FormatID, factory DemuxerFactory) {
	if factory != nil {
		r.RegisterDemuxerDescriptor(Descriptor{Format: format}, factory)
	}
}

// RegisterDemuxerDescriptor records a demuxer factory together with the
// capability descriptor validation reads.
func (r *SimpleRegistry) RegisterDemuxerDescriptor(desc Descriptor, factory DemuxerFactory) {
	if factory != nil {
		r.demuxers.register(desc, factory)
	}
}

// RegisterMuxer records a muxer factory for a format id; last registration
// wins.
func (r *SimpleRegistry) RegisterMuxer(format av.FormatID, factory MuxerFactory) {
	if factory != nil {
		r.RegisterMuxerDescriptor(Descriptor{Format: format}, factory)
	}
}

// RegisterMuxerDescriptor records a muxer factory together with the
// capability descriptor destination validation reads.
func (r *SimpleRegistry) RegisterMuxerDescriptor(desc Descriptor, factory MuxerFactory) {
	if factory != nil {
		r.muxers.register(desc, factory)
	}
}

// Probe runs every registered prober and returns the highest-scoring result,
// or ErrNotFound when none match.
func (r *SimpleRegistry) Probe(ctx context.Context, request ProbeRequest) (ProbeResult, error) {
	var best ProbeResult
	var found bool
	var firstErr error
	for i := range r.probers {
		result, err := r.probers[i].Probe(ctx, request)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if !found || result.Score > best.Score {
			best = result
			found = true
		}
	}
	if found {
		return best, nil
	}
	if firstErr != nil {
		return ProbeResult{}, firstErr
	}
	return ProbeResult{}, ErrNotFound
}

// DemuxerFactory returns the demuxer factory registered for a format id, or
// ErrNotFound.
func (r *SimpleRegistry) DemuxerFactory(format av.FormatID) (DemuxerFactory, error) {
	return r.demuxers.factory(format)
}

// DemuxerDescriptor returns the capability descriptor registered for a
// format's demuxer, or ErrNotFound.
func (r *SimpleRegistry) DemuxerDescriptor(format av.FormatID) (Descriptor, error) {
	return r.demuxers.descriptor(format)
}

// DemuxerDescriptors lists every registered demuxer descriptor sorted by format.
func (r *SimpleRegistry) DemuxerDescriptors() []Descriptor {
	return r.demuxers.allDescriptors()
}

// MuxerFactory returns the muxer factory registered for a format id, or
// ErrNotFound.
func (r *SimpleRegistry) MuxerFactory(format av.FormatID) (MuxerFactory, error) {
	return r.muxers.factory(format)
}

// MuxerDescriptor returns the capability descriptor registered for a
// format's muxer, or ErrNotFound.
func (r *SimpleRegistry) MuxerDescriptor(format av.FormatID) (Descriptor, error) {
	return r.muxers.descriptor(format)
}

// MuxerDescriptors lists every registered muxer descriptor sorted by format.
func (r *SimpleRegistry) MuxerDescriptors() []Descriptor {
	return r.muxers.allDescriptors()
}

func cloneDescriptor(desc Descriptor) Descriptor {
	cloned := desc
	cloned.Media = slices.Clone(desc.Media)
	cloned.Codecs = slices.Clone(desc.Codecs)
	cloned.Metadata = maps.Clone(desc.Metadata)
	return cloned
}
