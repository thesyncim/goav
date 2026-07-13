package goav

import (
	"context"
	"sync/atomic"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
	sourcepkg "github.com/thesyncim/goav/source"
)

type sourceInputSpec struct {
	shape shape.Spec
	fn    sourcepkg.Func
}

// inputStreamAnchor is a runtime-attach anchor for one stream produced by an
// InputSpec. Applications with an explicit track registry use
// input.Stream(stream) with Branch(...).From(...) when they need to attach a
// per-track branch before the source starts pushing that track's media.
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
	spec = normalizeCustomSourceShape(name, spec)
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

// Stream returns a runtime branch anchor for a stream that this input produces
// or will produce. It keeps applications with explicit dynamic tracks on the
// main grammar:
//
//	track := av.Stream{ID: "host", Type: av.MediaAudio, Codec: ...}
//	task.Attach(ctx, goav.Branch("track-host").From(input.Stream(track)).To(out))
//
// The av.Stream supplies the branch's source shape facts, so this is the
// deterministic sibling of OnStream: use it when the application already owns
// the track lifecycle and wants to attach before sending the first frame or
// packet for that stream.
func (s InputSpec) Stream(stream av.Stream) inputStreamAnchor {
	return inputStreamAnchor{input: s, stream: stream}
}

// Name reports the source node name this stream anchor attaches from.
func (s inputStreamAnchor) Name() string {
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

func normalizeCustomSourceShape(name string, spec shape.Spec) shape.Spec {
	if spec.Domain == "" {
		spec.Domain = shape.DomainPacket
	}
	if spec.StreamID == "" {
		spec.StreamID = av.StreamID(firstNonEmpty(name, string(spec.MediaKind), "stream"))
	}
	return spec
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
	return normalizeCustomSourceShape(input.inputName("source"), input.source.shape), true
}

func declaredSourceShape(input InputSpec) (shape.Spec, bool) {
	if spec, ok := customSourceShape(input); ok {
		return spec, true
	}
	if input.provider == nil {
		return shape.Spec{}, false
	}
	return normalizeCustomSourceShape(input.inputName("source"), input.provider.SourceShape()), true
}

func declaredSourceStreams(input InputSpec) []av.Stream {
	spec, ok := declaredSourceShape(input)
	if !ok {
		return nil
	}
	return []av.Stream{declaredSourceStream(input, spec)}
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
	shape := normalizeCustomSourceShape(input.inputName("source"), spec)
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
	fillStreamCodecParameters(&stream, input.codec)
	return stream
}

// fillStreamCodecParameters overlays shape-derived codec parameters onto a
// declared source stream: the declared stream stays authoritative, and the
// source shape fills only facts the stream left open.
func fillStreamCodecParameters(stream *av.Stream, spec codec.Spec) {
	if spec.ID != "" && stream.Codec.ID != "" && spec.ID != stream.Codec.ID {
		return
	}
	params := spec.Parameters
	if stream.Codec.ID == "" {
		stream.Codec.ID = firstCodecID(spec.ID, params.ID)
	}
	if stream.Codec.SampleRate == 0 {
		stream.Codec.SampleRate = params.SampleRate
	}
	if stream.Codec.Channels == 0 {
		stream.Codec.Channels = params.Channels
	}
	if stream.Codec.SampleFormat == "" {
		stream.Codec.SampleFormat = params.SampleFormat
	}
	if stream.Codec.ChannelLayout == "" {
		stream.Codec.ChannelLayout = params.ChannelLayout
	}
	if stream.Codec.Width == 0 {
		stream.Codec.Width = params.Width
	}
	if stream.Codec.Height == 0 {
		stream.Codec.Height = params.Height
	}
	if stream.Codec.PixelFormat == "" {
		stream.Codec.PixelFormat = params.PixelFormat
	}
	if stream.Codec.ClockRate == 0 {
		stream.Codec.ClockRate = params.ClockRate
	}
}

// firstCodecID returns the first non-empty codec id.
func firstCodecID(ids ...av.CodecID) av.CodecID {
	for _, id := range ids {
		if id != "" {
			return id
		}
	}
	return ""
}

// graphSourceBuild is the unified result of opening one input as a graph source:
// the running pipeline source, the streams it carries, its media domain, the
// realtime contribution of that input, and the optional decode-bounds capability
// that provider inputs may declare. Carrying everything in one value lets the
// recipe build path iterate inputs without branching on the input kind.
type graphSourceBuild struct {
	source   pipeline.Source
	streams  []av.Stream
	domain   shape.MediaDomain
	realtime bool
	bounds   decodeBoundsProvider
}

// openGraphSourceBuild is the single source-opening seam: every input kind (custom
// Source, file/URI, source provider) resolves to a running pipeline source + its
// streams + media domain + realtime + optional decode bounds through here, so
// callers never branch on the input kind. The node name is resolved by the caller
// (graphSourceNodeNames) so repeated provider names stay disambiguated. Returning
// all streams keeps it composable — the caller selects what it needs. WrapSource
// decorators apply here, after the open, so they see every input kind uniformly.
func (s InputSpec) openGraphSourceBuild(ctx context.Context, service *builder, name string) (graphSourceBuild, error) {
	build, err := s.openGraphSourceBuildKind(ctx, service, name)
	if err != nil {
		return graphSourceBuild{}, err
	}
	build.source = s.applySourceWraps(build.source)
	return build, nil
}

// openGraphSourceBuildKind opens the input by kind; openGraphSourceBuild
// layers the WrapSource decoration on top.
func (s InputSpec) openGraphSourceBuildKind(ctx context.Context, service *builder, name string) (graphSourceBuild, error) {
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
		return openProviderSource(ctx, s.provider, name)
	default:
		input := s.formatInput()
		build, err := service.openDemuxSource(ctx, input)
		if err != nil {
			return graphSourceBuild{}, err
		}
		return graphSourceBuild{
			source:   build.source,
			streams:  build.streams,
			domain:   shape.DomainPacket,
			realtime: input.Realtime,
		}, nil
	}
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
	name, detail := source.Name(), describedNodeDetail(source)
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

// openGraphSource keeps the streams/domain 4-tuple shape used by the Mix and
// Composite arms; it delegates to openGraphSourceBuild so all opening goes through
// one seam.
func (s InputSpec) openGraphSource(ctx context.Context, service *builder, name string) (pipeline.Source, []av.Stream, shape.MediaDomain, error) {
	build, err := s.openGraphSourceBuild(ctx, service, name)
	if err != nil {
		return nil, nil, "", err
	}
	return build.source, build.streams, build.domain, nil
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
		return providerNodeDetail(s.provider)
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
