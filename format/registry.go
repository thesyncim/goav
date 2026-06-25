package format

import (
	"context"
	"errors"
	"sort"

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

// SimpleRegistry is the per-runtime format registry: probers for detection
// plus demuxer/muxer factories and capability descriptors keyed by format id.
// Registration is last-wins; it is populated at runtime construction and
// read-only afterwards.
type SimpleRegistry struct {
	probers            []Prober
	demuxers           map[av.FormatID]DemuxerFactory
	demuxerDescriptors map[av.FormatID]Descriptor
	muxers             map[av.FormatID]MuxerFactory
	muxerDescriptors   map[av.FormatID]Descriptor
}

// NewRegistry returns an empty format registry.
func NewRegistry() *SimpleRegistry {
	return &SimpleRegistry{
		demuxers:           make(map[av.FormatID]DemuxerFactory),
		demuxerDescriptors: make(map[av.FormatID]Descriptor),
		muxers:             make(map[av.FormatID]MuxerFactory),
		muxerDescriptors:   make(map[av.FormatID]Descriptor),
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
	if factory != nil && desc.Format != "" {
		r.demuxers[desc.Format] = factory
		r.demuxerDescriptors[desc.Format] = cloneDescriptor(desc)
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
	if factory != nil && desc.Format != "" {
		r.muxers[desc.Format] = factory
		r.muxerDescriptors[desc.Format] = cloneDescriptor(desc)
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
	factory, ok := r.demuxers[format]
	if !ok {
		return nil, ErrNotFound
	}
	return factory, nil
}

// DemuxerDescriptor returns the capability descriptor registered for a
// format's demuxer, or ErrNotFound.
func (r *SimpleRegistry) DemuxerDescriptor(format av.FormatID) (Descriptor, error) {
	desc, ok := r.demuxerDescriptors[format]
	if !ok {
		return Descriptor{}, ErrNotFound
	}
	return cloneDescriptor(desc), nil
}

// DemuxerDescriptors lists every registered demuxer descriptor sorted by format.
func (r *SimpleRegistry) DemuxerDescriptors() []Descriptor {
	out := make([]Descriptor, 0, len(r.demuxerDescriptors))
	for id := range r.demuxerDescriptors {
		out = append(out, cloneDescriptor(r.demuxerDescriptors[id]))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Format < out[j].Format })
	return out
}

// MuxerFactory returns the muxer factory registered for a format id, or
// ErrNotFound.
func (r *SimpleRegistry) MuxerFactory(format av.FormatID) (MuxerFactory, error) {
	factory, ok := r.muxers[format]
	if !ok {
		return nil, ErrNotFound
	}
	return factory, nil
}

// MuxerDescriptor returns the capability descriptor registered for a
// format's muxer, or ErrNotFound.
func (r *SimpleRegistry) MuxerDescriptor(format av.FormatID) (Descriptor, error) {
	desc, ok := r.muxerDescriptors[format]
	if !ok {
		return Descriptor{}, ErrNotFound
	}
	return cloneDescriptor(desc), nil
}

// MuxerDescriptors lists every registered muxer descriptor sorted by format.
func (r *SimpleRegistry) MuxerDescriptors() []Descriptor {
	out := make([]Descriptor, 0, len(r.muxerDescriptors))
	for id := range r.muxerDescriptors {
		out = append(out, cloneDescriptor(r.muxerDescriptors[id]))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Format < out[j].Format })
	return out
}

func cloneDescriptor(desc Descriptor) Descriptor {
	cloned := desc
	if desc.Media != nil {
		cloned.Media = append([]av.MediaType(nil), desc.Media...)
	}
	if desc.Codecs != nil {
		cloned.Codecs = append([]av.CodecID(nil), desc.Codecs...)
	}
	if desc.Metadata != nil {
		cloned.Metadata = make(av.Metadata, len(desc.Metadata))
		for key, value := range desc.Metadata {
			cloned.Metadata[key] = value
		}
	}
	return cloned
}
