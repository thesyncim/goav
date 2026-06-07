package format

import (
	"context"
	"errors"

	"github.com/thesyncim/goav/av"
)

var (
	ErrNotFound   = errors.New("format: not found")
	ErrNilDemuxer = errors.New("format: nil demuxer")
	ErrNilMuxer   = errors.New("format: nil muxer")
	ErrNilPacket  = errors.New("format: nil packet")
	ErrResultFull = errors.New("format: result capacity full")
)

type SimpleRegistry struct {
	probers            []Prober
	demuxers           map[av.FormatID]DemuxerFactory
	demuxerDescriptors map[av.FormatID]Descriptor
	muxers             map[av.FormatID]MuxerFactory
	muxerDescriptors   map[av.FormatID]Descriptor
}

func NewRegistry() *SimpleRegistry {
	return &SimpleRegistry{
		demuxers:           make(map[av.FormatID]DemuxerFactory),
		demuxerDescriptors: make(map[av.FormatID]Descriptor),
		muxers:             make(map[av.FormatID]MuxerFactory),
		muxerDescriptors:   make(map[av.FormatID]Descriptor),
	}
}

func (r *SimpleRegistry) RegisterProber(prober Prober) {
	if prober != nil {
		r.probers = append(r.probers, prober)
	}
}

func (r *SimpleRegistry) RegisterDemuxer(format av.FormatID, factory DemuxerFactory) {
	if factory != nil {
		r.RegisterDemuxerDescriptor(Descriptor{Format: format}, factory)
	}
}

func (r *SimpleRegistry) RegisterDemuxerDescriptor(desc Descriptor, factory DemuxerFactory) {
	if factory != nil && desc.Format != "" {
		r.demuxers[desc.Format] = factory
		r.demuxerDescriptors[desc.Format] = cloneDescriptor(desc)
	}
}

func (r *SimpleRegistry) RegisterMuxer(format av.FormatID, factory MuxerFactory) {
	if factory != nil {
		r.RegisterMuxerDescriptor(Descriptor{Format: format}, factory)
	}
}

func (r *SimpleRegistry) RegisterMuxerDescriptor(desc Descriptor, factory MuxerFactory) {
	if factory != nil && desc.Format != "" {
		r.muxers[desc.Format] = factory
		r.muxerDescriptors[desc.Format] = cloneDescriptor(desc)
	}
}

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

func (r *SimpleRegistry) DemuxerFactory(format av.FormatID) (DemuxerFactory, error) {
	factory, ok := r.demuxers[format]
	if !ok {
		return nil, ErrNotFound
	}
	return factory, nil
}

func (r *SimpleRegistry) DemuxerDescriptor(format av.FormatID) (Descriptor, error) {
	desc, ok := r.demuxerDescriptors[format]
	if !ok {
		return Descriptor{}, ErrNotFound
	}
	return cloneDescriptor(desc), nil
}

func (r *SimpleRegistry) MuxerFactory(format av.FormatID) (MuxerFactory, error) {
	factory, ok := r.muxers[format]
	if !ok {
		return nil, ErrNotFound
	}
	return factory, nil
}

func (r *SimpleRegistry) MuxerDescriptor(format av.FormatID) (Descriptor, error) {
	desc, ok := r.muxerDescriptors[format]
	if !ok {
		return Descriptor{}, ErrNotFound
	}
	return cloneDescriptor(desc), nil
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
