package goav

import (
	"context"
	"errors"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/internal/compile"
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

// Input adapts a provider input into a recipe input: a provider.Source,
// whose media the provider produces, or a provider.ByteSource, whose
// container bytes the runtime probes (from goav.Name, goav.MIME, or
// SourceShape().Format) and demuxes. The declared shape seeds the input's
// codec intent and realtime policy, so the recipe planner can select streams
// and pick decoders before the input is opened:
//
//	in := goav.Input(rtpav.Receive(reader, rtpav.WithCodec(codec.Opus())))
//	err := rt.Run(ctx, in.Audio().To(out))
func Input(src provider.Input, opts ...InputOption) InputSpec {
	return applyInputOptions(inputSpecHandle(InputSpec{}.withProvider(src)), opts)
}

var errProviderInputKind = errors.New("goav: provider input implements neither provider.Source (OpenSource) nor provider.ByteSource (OpenBytes)")

// withProvider binds src as the spec's input, keeping the naming facts a
// URIInput already carries. A value that is both a Source and a ByteSource
// opens as a Source.
func (s InputSpec) withProvider(src provider.Input) InputSpec {
	if src == nil {
		s.err = errNilSource
		return s
	}
	spec := src.SourceShape()
	switch src := src.(type) {
	case provider.Source:
		s.provider = src
		s.codec = codecSpecFromSourceShape(spec)
	case provider.ByteSource:
		s.bytes = src
		if named, ok := src.(sourceProviderNamer); ok && s.input.Name == "" {
			s.input.Name = named.Name()
			s.name = s.input.Name
		}
	default:
		s.err = errProviderInputKind
		return s
	}
	s.input.Realtime = spec.Realtime
	s.realtime = spec.Realtime
	return s
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
//
// Clock-aware sources — those implementing `UseClock(av.Clock)` — receive the
// task clock (the shared timeline) right after opening, before any wrapping,
// so scheduled emission paces on the task timeline: fake clocks make it
// deterministic in tests and task-wide rate changes scale it live. The seam is
// structural (a type assertion), so providers stay free of goav internals.
func openProviderSource(ctx context.Context, src provider.Source, name string, detail string, clock av.Clock) (graphSourceBuild, error) {
	if src == nil {
		return graphSourceBuild{}, errNilSource
	}
	source, streams, err := src.OpenSource(ctx)
	if err != nil {
		return graphSourceBuild{}, err
	}
	if aware, ok := source.(interface{ UseClock(av.Clock) }); ok && clock != nil {
		aware.UseClock(clock)
		markTimelinePaced(clock)
	}
	spec := src.SourceShape()
	domain := spec.Domain
	if domain == "" {
		domain = shape.DomainPacket
	}
	build := graphSourceBuild{
		source:   resolvedProviderSource(source, name, detail),
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
	if source.Name() == name && compile.DescribedNodeDetail(source) == detail {
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

// applySourceWraps decorates an opened source with the input's WrapSource
// chain, then re-pins the node's name and described detail to the opened
// source's values so Describe() ≡ Build() holds no matter what the decorator
// reports. A decorator that delegates Name() and DescribeNode passes through
// untouched, keeping every optional capability it implements visible.
func (s InputSpec) applySourceWraps(source pipeline.Source) pipeline.Source {
	if len(s.wraps) == 0 || source == nil {
		return source
	}
	name, detail := source.Name(), compile.DescribedNodeDetail(source)
	wrapped := source
	for _, wrap := range s.wraps {
		if wrap == nil {
			continue
		}
		if next := wrap(wrapped); next != nil {
			wrapped = next
		}
	}
	return resolvedProviderSource(wrapped, name, detail)
}

// reconnecting wraps an opened live input in its reconnect policy, which
// reopens it through the same open path.
func (s InputSpec) reconnecting(build graphSourceBuild, reopen func(context.Context) (graphSourceBuild, error), clock av.Clock, report func(av.Event)) graphSourceBuild {
	if s.reconnect == nil {
		return build
	}
	build.source = newReconnectingSource(build.source, compile.DescribedNodeDetail(build.source), func(ctx context.Context) (pipeline.Source, error) {
		next, err := reopen(ctx)
		return next.source, err
	}, *s.reconnect, clock, report)
	return build
}
