package goav

import (
	"context"
	"sync/atomic"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// SourceFunc is the body of a custom source: push packets, frames, or events
// until the media ends (push.EOS) or the context is cancelled. The returned
// error stops the task; a clean EOS return ends the stream naturally.
type SourceFunc func(context.Context, SourcePush) error

// PushResult reports what happened to one push across its matching downstream
// targets. On a fan-out a single push can be both Accepted and Dropped: one
// branch queued the message while another shed it.
type PushResult struct {
	// Accepted is true when at least one downstream target queued the message.
	Accepted bool
	// Dropped is true when at least one target deliberately shed the message:
	// a full queue under a dropping policy (DropNewest/DropOldest/Latest), an
	// exhausted byte budget, or a paused branch. Shedding is normal realtime
	// behavior, not failure — the push error stays nil.
	Dropped bool
}

// SourcePush is how a custom Source delivers packets, frames, and events into
// the pipeline. The Packet/Frame/Event methods return a PushResult for
// per-push delivery visibility plus a flow-control error:
//
//   - err == nil: the push was handled. The PushResult says what happened:
//     Accepted means at least one downstream target queued the message; Dropped
//     means at least one target deliberately shed it. A dropping policy sheds
//     without error — the shed is reported on the PushResult and counted in the
//     branch's drop counters (Stats).
//   - errors.Is(err, ErrBackpressure): the strict paths refused the message
//     (a Blocking buffer that could not pace, DropNever). A source that can
//     pace itself should slow down.
//   - errors.Is(err, ErrClosed): the task has stopped; return cleanly.
//
// On a fan-out, one slow branch never fails the push: its shed is reported on
// the PushResult (and counted on that branch) while delivery continues to
// siblings. Any other error is fatal to the push.
type SourcePush struct {
	emit   Emit
	stream av.StreamID
}

// Packet delivers one packet. See SourcePush for the PushResult and
// ErrBackpressure/ErrClosed flow-control contract.
func (p *SourcePush) Packet(packet *av.Packet) (PushResult, error) {
	if packet == nil {
		return PushResult{}, nil
	}
	if packet.StreamID == "" {
		packet.StreamID = p.stream
	}
	return p.emit.packetDelivery(packet)
}

// Frame delivers one decoded frame. See SourcePush for the PushResult and
// ErrBackpressure/ErrClosed flow-control contract.
func (p *SourcePush) Frame(frame *av.Frame) (PushResult, error) {
	if frame == nil {
		return PushResult{}, nil
	}
	if frame.StreamID == "" {
		frame.StreamID = p.stream
	}
	return p.emit.frameDelivery(frame)
}

// Event delivers one out-of-band event. It is also the dynamic stream
// announce seam: a source that discovers a stream mid-run pushes
// av.Event{Type: av.EventStreamAdded, Stream: &stream} (the full av.Stream
// rides the typed Stream field) before that stream's media, and
// av.Event{Type: av.EventStreamRemoved, StreamID: id} when it ends. The
// running task surfaces both on Events()/Watch and reacts to declared
// OnStream rules without a rebuild. Events with an empty StreamID inherit
// the source's declared stream id, so stream announces must set StreamID
// (or Stream.ID) explicitly.
func (p *SourcePush) Event(event av.Event) (PushResult, error) {
	if event.StreamID == "" && event.Stream != nil {
		event.StreamID = event.Stream.ID
	}
	if event.StreamID == "" {
		event.StreamID = p.stream
	}
	return p.emit.eventDelivery(event)
}

// EOS ends the given streams (or the source's declared stream when none are
// listed): downstream nodes flush and destinations finalize naturally.
func (p *SourcePush) EOS(streams ...av.StreamID) error {
	if len(streams) == 0 && p.stream != "" {
		streams = []av.StreamID{p.stream}
	}
	return p.emit.EOS(streams...)
}

type sourceInputSpec struct {
	shape shape.Spec
	fn    SourceFunc
}

// Source declares a custom input the application pushes media into: spec
// states the media facts the planner needs before the source opens
// (shape.Packet, shape.Frame, or shape.Event plus format facts), and fn runs
// on the task pushing through the SourcePush. Custom sources participate in
// streams, branches, taps, explain, and runtime attach exactly like built-in
// inputs.
func Source(name string, spec shape.Spec, fn SourceFunc, opts ...InputOption) InputSpec {
	spec = normalizeCustomSourceShape(name, spec)
	return applyInputOptions(InputSpec{
		input: format.Input{
			Name:     name,
			Protocol: av.ProtocolCustom,
			Realtime: spec.Realtime,
		},
		source: &sourceInputSpec{shape: spec, fn: fn},
		codec:  codecSpecFromSourceShape(spec),
		name:   name,
	}, opts)
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

func codecSpecFromSourceShape(shape shape.Spec) codec.CodecSpec {
	return codec.CodecSpec{
		ID:   shape.Codec,
		Type: shape.MediaKind,
		Parameters: av.CodecParameters{
			ID:           shape.Codec,
			Type:         shape.MediaKind,
			SampleRate:   shape.SampleRate,
			Channels:     shape.Channels,
			Width:        shape.Width,
			Height:       shape.Height,
			PixelFormat:  shape.PixelFormat,
			SampleFormat: shape.SampleFormat,
		},
	}
}

func customSourceShape(input InputSpec) (shape.Spec, bool) {
	if input.source == nil {
		return shape.Spec{}, false
	}
	return normalizeCustomSourceShape(input.inputName("source"), input.source.shape), true
}

func compileStateCustomSourceShape(state *recipeCompileState) (shape.Spec, bool) {
	if state == nil {
		return shape.Spec{}, false
	}
	if state.branchCompositionPresent {
		return customSourceShape(state.branchInputAttachment)
	}
	if len(state.inputAttachments) != 1 {
		return shape.Spec{}, false
	}
	return customSourceShape(state.inputAttachments[0])
}

func customSourceStreams(input InputSpec) []av.Stream {
	if input.source == nil {
		return nil
	}
	return []av.Stream{customSourceStream(input)}
}

func customSourceProbeResult(input InputSpec) format.ProbeResult {
	return format.ProbeResult{
		Score:   100,
		Streams: customSourceStreams(input),
		Reason:  "declared custom source shape",
	}
}

func customSourceStream(input InputSpec) av.Stream {
	shape := normalizeCustomSourceShape(input.inputName("source"), input.source.shape)
	return av.Stream{
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
// all streams keeps it composable — the caller selects what it needs.
func (s InputSpec) openGraphSourceBuild(ctx context.Context, service *builder, name string) (graphSourceBuild, error) {
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
		return nil, nil, ErrNilSource
	}
	streams := customSourceStreams(input)
	if len(streams) == 0 {
		return nil, nil, ErrNilSource
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
	fn     SourceFunc
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
		return ErrNilSource
	}
	push := SourcePush{
		emit:   Emit{ctx: ctx, emitter: emitter},
		stream: s.stream,
	}
	return s.fn(ctx, push)
}

func (s *customSource) Close() error {
	s.closed.Store(true)
	return nil
}
