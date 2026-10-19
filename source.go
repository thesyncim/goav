package goav

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
	sourcepkg "github.com/thesyncim/goav/source"
)

type sourceInputSpec struct {
	shape shape.Spec
	fn    sourcepkg.Func
}

// inputStreamAnchor is the runtime-attach anchor for one stream an InputSpec
// produces or will produce: what task.Track(input, stream) compiles to, so an
// application that owns its track lifecycle attaches a per-track branch before
// the source pushes that track's media. The av.Stream supplies the branch's
// source shape facts, which makes it the deterministic sibling of OnStream.
type inputStreamAnchor struct {
	input  InputSpec
	stream av.Stream
}

// Source declares a custom input the application pushes media into: spec
// states the media facts the planner needs before the source opens
// (shape.Packet, shape.Frame, or shape.Event plus format facts), and fn runs
// on the task pushing through source.Push. Custom sources participate in
// streams, branches, taps, explain, and runtime attach exactly like built-in
// inputs.
func Source(name string, spec shape.Spec, fn sourcepkg.Func, opts ...InputOption) InputSpec {
	spec = recipeir.NormalizeSourceShape(name, spec)
	return applyInputOptions(inputSpecHandle(InputSpec{
		input: format.Input{
			Name:     name,
			Protocol: av.ProtocolCustom,
			Realtime: spec.Realtime,
		},
		source: &sourceInputSpec{shape: spec, fn: fn},
		codec:  codecSpecFromSourceShape(spec),
		name:   name,
	}), opts)
}

// track returns the attach anchor for one stream of this input.
func (s InputSpec) track(stream av.Stream) inputStreamAnchor {
	return inputStreamAnchor{input: s, stream: stream}
}

// name reports the source node name this stream anchor attaches from.
func (s inputStreamAnchor) name() string {
	return s.input.graphSourceNodeName()
}

func (s inputStreamAnchor) branchSource() branchSourceBinding {
	stream := s.stream
	return branchSourceBinding{
		from:         s.input.graphSourceNodeName(),
		policy:       pipeline.RouteByStream,
		label:        string(stream.ID),
		stream:       &stream,
		streamDomain: s.input.sourceEventDomain(),
	}
}

func codecSpecFromSourceShape(shape shape.Spec) codec.Spec {
	return codec.Spec{
		ID:   shape.Codec,
		Type: shape.MediaKind,
		Parameters: av.CodecParameters{
			ID:            shape.Codec,
			Type:          shape.MediaKind,
			ClockRate:     sourceShapeClockRate(shape),
			SampleRate:    shape.SampleRate,
			Channels:      shape.Channels,
			ChannelLayout: sourceShapeChannelLayout(shape),
			Width:         shape.Width,
			Height:        shape.Height,
			PixelFormat:   shape.PixelFormat,
			SampleFormat:  shape.SampleFormat,
		},
	}
}

func sourceShapeClockRate(spec shape.Spec) uint32 {
	if spec.SampleRate > 0 {
		return uint32(spec.SampleRate)
	}
	switch spec.MediaKind {
	case av.MediaVideo:
		return 90000
	default:
		return 0
	}
}

func sourceShapeChannelLayout(spec shape.Spec) string {
	switch spec.Channels {
	case codec.Mono:
		return "mono"
	case codec.Stereo:
		return "stereo"
	default:
		return ""
	}
}

func customSourceShape(input InputSpec) (shape.Spec, bool) {
	if input.source == nil {
		return shape.Spec{}, false
	}
	return recipeir.NormalizeSourceShape(input.inputName("source"), input.source.shape), true
}

func declaredSourceShape(input InputSpec) (shape.Spec, bool) {
	if spec, ok := customSourceShape(input); ok {
		return spec, true
	}
	if input.provider == nil {
		return shape.Spec{}, false
	}
	return recipeir.NormalizeSourceShape(input.inputName("source"), input.provider.SourceShape()), true
}

type compileSourceShapeState interface {
	customSourceShapeForCompile() (shape.Spec, bool)
}

func compileStateCustomSourceShape(state compileSourceShapeState) (shape.Spec, bool) {
	if state == nil {
		return shape.Spec{}, false
	}
	return state.customSourceShapeForCompile()
}

func customSourceStreams(input InputSpec) []av.Stream {
	if input.source == nil {
		return nil
	}
	return []av.Stream{declaredSourceStream(input, input.source.shape)}
}

func customSourceProbeResult(input InputSpec) format.ProbeResult {
	return format.ProbeResult{
		Score:   100,
		Streams: customSourceStreams(input),
		Reason:  "declared custom source shape",
	}
}

func declaredSourceStream(input InputSpec, spec shape.Spec) av.Stream {
	shape := recipeir.NormalizeSourceShape(input.inputName("source"), spec)
	stream := av.Stream{
		ID:   shape.StreamID,
		Type: shape.MediaKind,
		Codec: av.CodecParameters{
			ID:           shape.Codec,
			Type:         shape.MediaKind,
			SampleRate:   shape.SampleRate,
			Channels:     shape.Channels,
			Width:        shape.Width,
			Height:       shape.Height,
			PixelFormat:  shape.PixelFormat,
			SampleFormat: shape.SampleFormat,
		},
		Name: string(shape.StreamID),
	}
	recipeir.FillStreamCodecParameters(&stream, input.codec)
	return stream
}

// graphSourceBuild is the unified result of opening one input as a graph source:
// the running pipeline source, the streams it carries, its media domain, the
// realtime contribution of that input, and the optional decode-bounds capability
// that provider inputs may declare. Carrying everything in one value lets the
// recipe build path iterate inputs without branching on the input kind.
type graphSourceBuild struct {
	source pipeline.Source
	// streams keep the input's own ids, the ids selection matches; the
	// running source stamps qualifier on them (qualifyStreamID).
	streams   []av.Stream
	qualifier string
	domain    shape.MediaDomain
	realtime  bool
	bounds    decodeBoundsProvider
}

// openGraphSourceBuild is the single source-opening seam: every input kind (custom
// Source, file/URI, source provider) resolves to a running pipeline source + its
// streams + media domain + realtime + optional decode bounds through here, so
// callers never branch on the input kind. The node name is resolved by the caller
// (graphSourceNodeNames) so repeated provider names stay disambiguated. Returning
// all streams keeps it composable — the caller selects what it needs. WrapSource
// decorators apply here, after the open, so they see every input kind uniformly.
// With qualify, a demuxed input stamps its stream ids with its node name, which
// keeps several file inputs of one run apart; declared ids (custom sources,
// providers) are the application's and stay as they are.
func (s InputSpec) openGraphSourceBuild(ctx context.Context, service *builder, name string, qualify bool) (graphSourceBuild, error) {
	build, err := s.openGraphSourceBuildKind(ctx, service, name, qualify)
	if err != nil {
		return graphSourceBuild{}, err
	}
	build.source = s.applySourceWraps(build.source)
	return build, nil
}

// openGraphSourceBuildKind opens the input by kind; openGraphSourceBuild
// layers the WrapSource decoration on top.
func (s InputSpec) openGraphSourceBuildKind(ctx context.Context, service *builder, name string, qualify bool) (graphSourceBuild, error) {
	switch {
	case s.source != nil:
		source, streams, err := newCustomSource(s)
		if err != nil {
			return graphSourceBuild{}, err
		}
		shapeSpec, _ := customSourceShape(s)
		return graphSourceBuild{
			source:   source,
			streams:  streams,
			domain:   shapeSpec.Domain,
			realtime: shapeSpec.Realtime,
		}, nil
	case s.provider != nil:
		open := func(ctx context.Context) (graphSourceBuild, error) {
			return openProviderSource(ctx, s.provider, name, s.providerDetail(), service.taskClock())
		}
		build, err := open(ctx)
		if err != nil {
			return graphSourceBuild{}, err
		}
		return s.reconnecting(build, open, service.taskClock(), service.runtime.qosReportFunc()), nil
	default:
		qualifier := ""
		if qualify {
			qualifier = "@" + name
		}
		open := func(ctx context.Context) (graphSourceBuild, error) {
			return s.openDemuxed(ctx, service, qualifier)
		}
		build, err := open(ctx)
		if err != nil {
			return graphSourceBuild{}, err
		}
		return s.reconnecting(build, open, service.taskClock(), service.runtime.qosReportFunc()), nil
	}
}

// openDemuxed opens a file-like input or a byte provider's bytes and demuxes
// them.
func (s InputSpec) openDemuxed(ctx context.Context, service *builder, qualifier string) (graphSourceBuild, error) {
	input := s.formatInput()
	var reader io.ReadCloser
	if s.bytes != nil {
		var err error
		if input, reader, err = openByteInput(ctx, s.bytes, input); err != nil {
			return graphSourceBuild{}, err
		}
	}
	build, err := service.openDemuxSource(ctx, input, qualifier, s.pinnedFormat(), reader)
	if err != nil {
		return graphSourceBuild{}, err
	}
	return graphSourceBuild{
		source:    build.source,
		streams:   build.streams,
		qualifier: qualifier,
		domain:    shape.DomainPacket,
		realtime:  input.Realtime,
	}, nil
}

// graphSourceNodeName returns the base planner node name for this input. Callers
// iterating several inputs resolve names through graphSourceNodeNames so repeated
// provider names get the index-suffix disambiguation.
func (s InputSpec) graphSourceNodeName() string {
	switch {
	case s.source != nil:
		return customSourceNodeName(s)
	case s.provider != nil:
		return firstNonEmpty(s.name, providerNodeName(s.provider))
	default:
		return demuxNodeName(s.formatInput())
	}
}

func demuxNodeName(input format.Input) string {
	if input.Name != "" {
		return input.Name
	}
	if input.URI != "" {
		return input.URI
	}
	return "input"
}

func customSourceNodeName(input InputSpec) string {
	if input.source == nil {
		return firstNonEmpty(input.name, input.input.Name, "source")
	}
	return firstNonEmpty(input.name, input.input.Name, string(input.source.shape.StreamID), "source")
}

// graphSourceNodeNames resolves one planner/build node name per input, matching
// the running source's node name for every input kind so describe and build
// agree. Provider inputs repeating an earlier input's name get an index suffix
// ("rtp", "rtp-1", ...).
func graphSourceNodeNames(inputs []InputSpec) []string {
	names := make([]string, len(inputs))
	seen := make(map[string]struct{}, len(inputs))
	for i := range inputs {
		names[i] = disambiguateSourceNodeName(seen, inputs[i].graphSourceNodeName(), inputs[i].provider != nil, i)
	}
	return names
}

// graphSourceNodeDetail returns the planner node detail for this input, matching
// the running source's detail for every input kind.
func (s InputSpec) graphSourceNodeDetail() string {
	switch {
	case s.source != nil:
		return customSourceDetail(s)
	case s.provider != nil:
		return s.providerDetail()
	default:
		return inputNodeDetail(s.formatInput())
	}
}

func newCustomSource(input InputSpec) (pipeline.Source, []av.Stream, error) {
	if input.source == nil {
		return nil, nil, errNilSource
	}
	streams := customSourceStreams(input)
	if len(streams) == 0 {
		return nil, nil, errNilSource
	}
	source := &customSource{
		name:   customSourceNodeName(input),
		detail: customSourceDetail(input),
		stream: streams[0].ID,
		fn:     input.source.fn,
	}
	return source, streams, nil
}

type customSource struct {
	name   string
	detail string
	stream av.StreamID
	fn     sourcepkg.Func
	closed atomic.Bool
}

func (s *customSource) Name() string {
	return s.name
}

func (s *customSource) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: s.name, Kind: pipeline.NodeSource, Detail: s.detail}
}

func (s *customSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	if s.closed.Load() {
		return pipeline.ErrClosed
	}
	if s.fn == nil {
		return errNilSource
	}
	push := sourcepkg.NewPush(ctx, emitter, s.stream)
	return s.fn(ctx, push)
}

func (s *customSource) Close() error {
	s.closed.Store(true)
	return nil
}

// inputStartsFrameDomain reports an input that delivers decoded frames.
func inputStartsFrameDomain(input InputSpec) bool {
	spec, ok := declaredSourceShape(input)
	return ok && spec.Domain == shape.DomainFrame
}
