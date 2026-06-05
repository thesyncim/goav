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

type RegistryOption func(*SimpleRegistry)

type SimpleRegistry struct {
	probers  []Prober
	demuxers map[av.FormatID]DemuxerFactory
	muxers   map[av.FormatID]MuxerFactory
}

func NewRegistry(options ...RegistryOption) *SimpleRegistry {
	registry := &SimpleRegistry{
		demuxers: make(map[av.FormatID]DemuxerFactory),
		muxers:   make(map[av.FormatID]MuxerFactory),
	}
	for _, option := range options {
		option(registry)
	}
	return registry
}

func WithProber(prober Prober) RegistryOption {
	return func(registry *SimpleRegistry) {
		registry.RegisterProber(prober)
	}
}

func WithDemuxer(format av.FormatID, factory DemuxerFactory) RegistryOption {
	return func(registry *SimpleRegistry) {
		registry.RegisterDemuxer(format, factory)
	}
}

func WithMuxer(format av.FormatID, factory MuxerFactory) RegistryOption {
	return func(registry *SimpleRegistry) {
		registry.RegisterMuxer(format, factory)
	}
}

func (r *SimpleRegistry) RegisterProber(prober Prober) {
	if prober != nil {
		r.probers = append(r.probers, prober)
	}
}

func (r *SimpleRegistry) RegisterDemuxer(format av.FormatID, factory DemuxerFactory) {
	if factory != nil {
		r.demuxers[format] = factory
	}
}

func (r *SimpleRegistry) RegisterMuxer(format av.FormatID, factory MuxerFactory) {
	if factory != nil {
		r.muxers[format] = factory
	}
}

func (r *SimpleRegistry) Probers() []Prober {
	out := make([]Prober, len(r.probers))
	copy(out, r.probers)
	return out
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

func (r *SimpleRegistry) MuxerFactory(format av.FormatID) (MuxerFactory, error) {
	factory, ok := r.muxers[format]
	if !ok {
		return nil, ErrNotFound
	}
	return factory, nil
}
