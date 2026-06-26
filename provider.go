package goav

import (
	"context"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/provider"
	"github.com/thesyncim/goav/shape"
)

// Optional provider.Source capabilities, discovered by type assertion.
type (
	sourceProviderNamer    interface{ Name() string }
	sourceProviderDetailer interface{ Detail() string }
	// decodeBoundsProvider reports the decode bounds a provider declares for
	// one of its streams (zero value when none); resolved after OpenSource.
	decodeBoundsProvider interface {
		DecodeBounds(av.StreamID) codec.DecodeBounds
	}
)

// Input adapts a provider.Source into a recipe input. The provider's declared
// shape seeds the input's codec intent and realtime policy, so the recipe
// planner can select streams and pick decoders before the source is opened:
//
//	goav.From(goav.Input(rtpav.Receive(reader, rtpav.WithCodec(codec.Opus())))).
//		Copy().To(out)
func Input(src provider.Source, opts ...InputOption) InputSpec {
	if src == nil {
		return inputSpecHandle(InputSpec{err: ErrNilSource})
	}
	spec := src.SourceShape()
	return applyInputOptions(inputSpecHandle(InputSpec{
		input:    format.Input{Realtime: spec.Realtime},
		provider: src,
		codec:    codecSpecFromSourceShape(spec),
		realtime: spec.Realtime,
	}), opts)
}

// providerNodeName returns the provider's node name capability, defaulting to
// "source".
func providerNodeName(src provider.Source) string {
	if named, ok := src.(sourceProviderNamer); ok {
		if name := named.Name(); name != "" {
			return name
		}
	}
	return "source"
}

// providerNodeDetail returns the provider's node detail capability; providers
// without one get an empty detail so planned and built graphs agree.
func providerNodeDetail(src provider.Source) string {
	if detailed, ok := src.(sourceProviderDetailer); ok {
		return detailed.Detail()
	}
	return ""
}

// disambiguateSourceNodeName applies the index suffix that keeps repeated
// provider node names unique across the inputs of one job ("rtp", "rtp-1",
// ...), recording the resolved name in seen.
func disambiguateSourceNodeName(seen map[string]struct{}, name string, provider bool, index int) string {
	if _, dup := seen[name]; dup && provider && index > 0 {
		name += "-" + strconv.Itoa(index)
	}
	seen[name] = struct{}{}
	return name
}

// openProviderSource opens a provider input through the unified source seam:
// the running source (renamed to the resolved node name when disambiguation or
// a job-level name applies), its streams, the declared shape domain and
// realtime policy, and the optional decode-bounds capability.
func openProviderSource(ctx context.Context, src provider.Source, name string) (graphSourceBuild, error) {
	if src == nil {
		return graphSourceBuild{}, ErrNilSource
	}
	source, streams, err := src.OpenSource(ctx)
	if err != nil {
		return graphSourceBuild{}, err
	}
	spec := src.SourceShape()
	domain := spec.Domain
	if domain == "" {
		domain = shape.DomainPacket
	}
	build := graphSourceBuild{
		source:   resolvedProviderSource(source, name, providerNodeDetail(src)),
		streams:  streams,
		domain:   domain,
		realtime: spec.Realtime,
	}
	if bounds, ok := src.(decodeBoundsProvider); ok {
		build.bounds = bounds
	}
	return build, nil
}

// providerDecodeBoundsForStream merges the decode bounds every opened provider
// input declares for this stream.
func providerDecodeBoundsForStream(stream av.Stream, providers []decodeBoundsProvider) codec.DecodeBounds {
	var bounds codec.DecodeBounds
	for i := range providers {
		bounds = maxDecodeBounds(bounds, providers[i].DecodeBounds(stream.ID))
	}
	return bounds
}

func maxDecodeBounds(a codec.DecodeBounds, b codec.DecodeBounds) codec.DecodeBounds {
	if b.MaxFramesPerInput > a.MaxFramesPerInput {
		a.MaxFramesPerInput = b.MaxFramesPerInput
	}
	if b.MaxEventsPerInput > a.MaxEventsPerInput {
		a.MaxEventsPerInput = b.MaxEventsPerInput
	}
	if b.MaxRequestsPerInput > a.MaxRequestsPerInput {
		a.MaxRequestsPerInput = b.MaxRequestsPerInput
	}
	if b.MaxPayloadBytes > a.MaxPayloadBytes {
		a.MaxPayloadBytes = b.MaxPayloadBytes
	}
	if b.MaxRetainedBytes > a.MaxRetainedBytes {
		a.MaxRetainedBytes = b.MaxRetainedBytes
	}
	if b.MaxWidth > a.MaxWidth {
		a.MaxWidth = b.MaxWidth
	}
	if b.MaxHeight > a.MaxHeight {
		a.MaxHeight = b.MaxHeight
	}
	return a
}

// resolvedProviderSource renames an opened provider source to the resolved
// planner node name and detail, so Describe of the planned and built graphs
// agree even when the job applies an index suffix or a job-level name. Sources
// already matching are passed through untouched (preserving every optional
// capability they implement).
func resolvedProviderSource(source pipeline.Source, name string, detail string) pipeline.Source {
	if source.Name() == name && describedNodeDetail(source) == detail {
		return source
	}
	renamed := renamedSource{source: source, name: name, detail: detail}
	if _, ok := source.(pipeline.ControllableSource); ok {
		return &renamedControllableSource{renamedSource: renamed}
	}
	return &renamed
}

type renamedSource struct {
	source pipeline.Source
	name   string
	detail string
}

func (s *renamedSource) Name() string {
	return s.name
}

func (s *renamedSource) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: s.name, Kind: pipeline.NodeSource, Detail: s.detail}
}

func (s *renamedSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	return s.source.Start(ctx, emitter)
}

func (s *renamedSource) Close() error {
	return s.source.Close()
}

type renamedControllableSource struct {
	renamedSource
}

func (s *renamedControllableSource) Control(ctx context.Context, msg *pipeline.Message) error {
	return s.source.(pipeline.ControllableSource).Control(ctx, msg)
}
