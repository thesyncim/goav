package goav

import (
	"context"
	"errors"
	"sync"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/info"
	"github.com/thesyncim/goav/pipeline"
)

var (
	ErrUnsupportedBuild = errors.New("goav: unsupported builder graph")
	ErrNilSource        = errors.New("goav: nil source")
	ErrNilStage         = errors.New("goav: nil stage")
	ErrNilSink          = errors.New("goav: nil sink")
	ErrNilWriter        = errors.New("goav: nil writer")
)

// Runtime flow-control sentinels, surfaced on the front door so a SourceFunc,
// PacketFunc, FrameFunc, or SinkFunc can react to backpressure and shutdown with
// errors.Is — without importing the pipeline package. They share identity with
// the values the runtime returns, so errors.Is matches either name.
var (
	// ErrBackpressure is returned by SourcePush/Emit when a downstream buffer is
	// full and the message was not delivered. On a Blocking branch the producer
	// is paced instead; on a dropping branch the message is shed and the producer
	// may keep going. A source that can pace itself should slow down on this.
	ErrBackpressure = pipeline.ErrBackpressure
	// ErrClosed is returned when emitting into a graph or stage that has shut
	// down (the task stopped, or its context was cancelled). A source should
	// treat it as a signal to return cleanly.
	ErrClosed = pipeline.ErrClosed
)

type Option func(*runtime)

func New(options ...Option) Runtime {
	formats := format.NewRegistry()
	formats.RegisterProber(format.DefaultProber())
	runtime := &runtime{
		codecs:   codec.NewRegistry(),
		filters:  filter.NewRegistry(),
		formats:  formats,
		realtime: true,
	}
	for _, option := range options {
		option(runtime)
	}
	return runtime
}

func WithCodecAdapter(register func(*codec.SimpleRegistry)) Option {
	return func(runtime *runtime) {
		if register != nil {
			register(runtime.codecs)
		}
	}
}

func WithCodecDescriptor(desc codec.Descriptor) Option {
	return WithCodecAdapter(func(registry *codec.SimpleRegistry) {
		registry.RegisterDescriptor(desc)
	})
}

func WithDecoder(desc codec.Descriptor, factory codec.DecoderFactory) Option {
	return WithCodecAdapter(func(registry *codec.SimpleRegistry) {
		registry.RegisterDecoder(desc, factory)
	})
}

func WithEncoder(desc codec.Descriptor, factory codec.EncoderFactory) Option {
	return WithCodecAdapter(func(registry *codec.SimpleRegistry) {
		registry.RegisterEncoder(desc, factory)
	})
}

// WithFilter adds one filter factory by descriptor — the direct value form,
// mirroring WithDecoder/WithEncoder, so an external filter plugs in by passing the
// implementation, never by touching a registry. (WithFilterAdapter remains for
// registering a whole bundle at once.)
func WithFilter(desc filter.Descriptor, factory filter.Factory) Option {
	return WithFilterAdapter(func(registry *filter.SimpleRegistry) {
		registry.RegisterFactory(desc, factory)
	})
}

// WithMuxer adds one muxer factory for a container format — pass the muxer
// directly, no registry callback.
func WithMuxer(id av.FormatID, factory format.MuxerFactory) Option {
	return WithFormatAdapter(func(registry *format.SimpleRegistry) {
		registry.RegisterMuxer(id, factory)
	})
}

// WithDemuxer adds one demuxer factory for a container format — pass the demuxer
// directly, no registry callback.
func WithDemuxer(id av.FormatID, factory format.DemuxerFactory) Option {
	return WithFormatAdapter(func(registry *format.SimpleRegistry) {
		registry.RegisterDemuxer(id, factory)
	})
}

// WithProber adds one format prober (content sniffing) directly.
func WithProber(prober format.Prober) Option {
	return WithFormatAdapter(func(registry *format.SimpleRegistry) {
		registry.RegisterProber(prober)
	})
}

func WithFormatAdapter(register func(*format.SimpleRegistry)) Option {
	return func(runtime *runtime) {
		if register != nil {
			register(runtime.formats)
		}
	}
}

func WithFilterAdapter(register func(*filter.SimpleRegistry)) Option {
	return func(runtime *runtime) {
		if register != nil {
			register(runtime.filters)
		}
	}
}

func WithBufferPolicy(policy pipeline.BufferPolicy) Option {
	return func(runtime *runtime) {
		runtime.buffer = policy
	}
}

func WithEventCapacity(capacity int) Option {
	return func(runtime *runtime) {
		runtime.eventCapacity = capacity
	}
}

func WithRealtime(realtime bool) Option {
	return func(runtime *runtime) {
		runtime.realtime = realtime
	}
}

// WithClock sets the time source the runtime's realtime pacing runs on — a
// realtime task playing a file delivers each packet when its media time is due
// on this clock (and goav.Rate scales that pace). Nil or unset defaults to
// av.MonotonicClock(); tests and simulations inject a fake so nothing sleeps
// for real. Offline runtimes (WithRealtime(false)) never consult it.
func WithClock(clock av.Clock) Option {
	return func(runtime *runtime) {
		runtime.clock = clock
	}
}

type runtime struct {
	codecs        *codec.SimpleRegistry
	filters       *filter.SimpleRegistry
	formats       *format.SimpleRegistry
	buffer        pipeline.BufferPolicy
	realtime      bool
	clock         av.Clock
	eventCapacity int
}

type builderAPI interface {
	Input(format.Input) builderAPI
	Provider(SourceProvider) builderAPI
	Mux(format.Output) builderAPI
	Decode(av.StreamSelector) builderAPI
	Encode(av.StreamSelector, codec.EncodeConfig) builderAPI
	Filter(av.StreamSelector, pipeline.Stage) builderAPI
	Source(pipeline.Source) builderAPI
	Stage(pipeline.Stage) builderAPI
	Sink(pipeline.Sink) builderAPI
	Routes(...pipeline.Route) builderAPI
	Describe() (pipeline.Spec, error)
	Build(context.Context) (Task, error)
}

func (r *runtime) Probe(ctx context.Context, request format.ProbeRequest) (format.ProbeResult, error) {
	return r.formats.Probe(ctx, request)
}

func (r *runtime) New() builderAPI {
	return &builder{runtime: r}
}

type builder struct {
	runtime         *runtime
	inputs          []builderInput
	outputs         []format.Output
	outputFmts      []av.FormatID
	outputGraphFmts []av.FormatID
	destinationTxs  []*destinationTransaction
	requireRunOK    bool
	decodes         []decodeRequest
	encodes         []encodeRequest
	filters         []filterRequest
	sources         []pipeline.Source
	stages          []pipeline.Stage
	sinks           []pipeline.Sink
	routes          []pipeline.Route
}

// builderInput is the single builder input model: exactly one of demux (file/URI)
// or provider (an external source provider, e.g. rtpav.Receive) is set. It lets
// the runtime compilers iterate every input through one loop and one
// source-opening seam, with provider-only build metadata (decode bounds) surfaced
// through graphSourceBuild only when present.
type builderInput struct {
	demux    *format.Input
	provider SourceProvider
}

// open resolves this input to a running pipeline source plus its streams, domain,
// realtime contribution, and (for provider inputs) the decode-bounds capability,
// without the caller branching on the input kind. The node name comes from the
// caller (builder.inputNodeNames) so repeated provider names stay disambiguated.
func (in builderInput) open(ctx context.Context, b *builder, name string) (graphSourceBuild, error) {
	if in.provider != nil {
		return openProviderSource(ctx, in.provider, name)
	}
	build, err := b.openDemuxSource(ctx, in.demuxInput())
	if err != nil {
		return graphSourceBuild{}, err
	}
	return graphSourceBuild{
		source:   build.source,
		streams:  build.streams,
		realtime: in.demuxInput().Realtime,
	}, nil
}

func (in builderInput) demuxInput() format.Input {
	if in.demux == nil {
		return format.Input{}
	}
	return *in.demux
}

// nodeName returns the base planner/source node name for this input; the
// builder resolves the final names through inputNodeNames so describe and build
// agree.
func (in builderInput) nodeName() string {
	if in.provider != nil {
		return providerNodeName(in.provider)
	}
	return demuxNodeName(in.demuxInput())
}

// detail returns the planner node detail for this input, matching the running
// source's detail for both input kinds.
func (in builderInput) detail() string {
	if in.provider != nil {
		return providerNodeDetail(in.provider)
	}
	return inputNodeDetail(in.demuxInput())
}

// inputNodeNames resolves one node name per builder input, applying the index
// suffix that disambiguates repeated provider names ("rtp", "rtp-1", ...).
func (b *builder) inputNodeNames() []string {
	names := make([]string, len(b.inputs))
	seen := make(map[string]struct{}, len(b.inputs))
	for i := range b.inputs {
		names[i] = disambiguateSourceNodeName(seen, b.inputs[i].nodeName(), b.inputs[i].provider != nil, i)
	}
	return names
}

type encodeRequest struct {
	name     string
	selector av.StreamSelector
	config   codec.EncodeConfig
}

type decodeRequest struct {
	selector    av.StreamSelector
	codecChange CodecChangePolicy
	config      codec.CodecSpec
}

type filterRequest struct {
	selector  av.StreamSelector
	stage     pipeline.Stage
	transform *mediaTransform
}

func (b *builder) Input(input format.Input) builderAPI {
	in := input
	b.inputs = append(b.inputs, builderInput{demux: &in})
	return b
}

// Provider adds an external source provider (e.g. rtpav.Receive) as a builder
// input, opened through the same seam as file/URI inputs.
func (b *builder) Provider(provider SourceProvider) builderAPI {
	b.inputs = append(b.inputs, builderInput{provider: provider})
	return b
}

func (b *builder) Mux(output format.Output) builderAPI {
	return b.outputWithFormats(output, "", "")
}

func (b *builder) outputWithFormats(output format.Output, openFormat av.FormatID, detailFormat av.FormatID) builderAPI {
	b.outputs = append(b.outputs, output)
	b.outputFmts = append(b.outputFmts, openFormat)
	b.outputGraphFmts = append(b.outputGraphFmts, detailFormat)
	return b
}

func (b *builder) outputOpenFormat(index int) av.FormatID {
	if index < 0 || index >= len(b.outputFmts) {
		return ""
	}
	return b.outputFmts[index]
}

func (b *builder) outputFormat(index int) av.FormatID {
	if index < 0 || index >= len(b.outputGraphFmts) {
		return ""
	}
	return b.outputGraphFmts[index]
}

func (b *builder) Decode(selector av.StreamSelector) builderAPI {
	return b.decodeWithPolicy(selector, CodecChangePolicy{})
}

func (b *builder) decodeWithPolicy(selector av.StreamSelector, policy CodecChangePolicy) builderAPI {
	b.decodes = append(b.decodes, decodeRequest{selector: selector, codecChange: policy})
	return b
}

func (b *builder) Encode(selector av.StreamSelector, config codec.EncodeConfig) builderAPI {
	b.encodes = append(b.encodes, encodeRequest{selector: selector, config: config})
	return b
}

func (b *builder) Filter(selector av.StreamSelector, stage pipeline.Stage) builderAPI {
	b.filters = append(b.filters, filterRequest{selector: selector, stage: stage})
	return b
}

func (b *builder) Source(source pipeline.Source) builderAPI {
	b.sources = append(b.sources, source)
	return b
}

func (b *builder) Stage(stage pipeline.Stage) builderAPI {
	b.stages = append(b.stages, stage)
	return b
}

func (b *builder) Sink(sink pipeline.Sink) builderAPI {
	b.sinks = append(b.sinks, sink)
	return b
}

func (b *builder) Routes(routes ...pipeline.Route) builderAPI {
	b.routes = append(b.routes, routes...)
	return b
}

func (b *builder) Build(ctx context.Context) (Task, error) {
	compiler, err := b.selectCompiler()
	if err != nil {
		return nil, err
	}
	b.destinationTxs = nil
	b.requireRunOK = true
	return compiler.build(ctx, b)
}

func (b *builder) hasHighLevelRequests() bool {
	return len(b.inputs) != 0 || len(b.outputs) != 0 || len(b.decodes) != 0 ||
		len(b.encodes) != 0 || len(b.filters) != 0
}

func (b *builder) hasExplicitGraph() bool {
	return len(b.sources) != 0 || len(b.stages) != 0 || len(b.sinks) != 0 ||
		len(b.routes) != 0
}

func (b *builder) compileExplicitGraph(graph pipeline.Graph) error {
	if len(b.sources) == 0 {
		return explicitGraphMissingSourceError()
	}

	sourceRefs := make([]pipeline.NodeRef, len(b.sources))
	stageRefs := make([]pipeline.NodeRef, len(b.stages))
	sinkRefs := make([]pipeline.NodeRef, len(b.sinks))

	for i := range b.sources {
		if b.sources[i] == nil {
			return ErrNilSource
		}
		ref, err := graph.AddSource(b.sources[i], b.runtime.buffer)
		if err != nil {
			return err
		}
		sourceRefs[i] = ref
	}
	for i := range b.stages {
		if b.stages[i] == nil {
			return ErrNilStage
		}
		ref, err := graph.AddStage(b.stages[i], b.runtime.buffer)
		if err != nil {
			return err
		}
		stageRefs[i] = ref
	}
	for i := range b.sinks {
		if b.sinks[i] == nil {
			return ErrNilSink
		}
		ref, err := graph.AddSink(b.sinks[i], b.runtime.buffer)
		if err != nil {
			return err
		}
		sinkRefs[i] = ref
	}

	if len(b.routes) != 0 {
		nodes := make(map[string]pipeline.NodeRef, len(sourceRefs)+len(stageRefs)+len(sinkRefs))
		addRuntimeNodes(nodes, sourceRefs)
		addRuntimeNodes(nodes, stageRefs)
		addRuntimeNodes(nodes, sinkRefs)
		return b.compileExplicitRoutes(graph, nodes)
	}

	if len(stageRefs) == 0 {
		return linkMany(graph, sourceRefs, sinkRefs)
	}
	if err := linkMany(graph, sourceRefs, stageRefs[:1]); err != nil {
		return err
	}
	for i := 0; i < len(stageRefs)-1; i++ {
		if err := connectRefs(graph, stageRefs[i], stageRefs[i+1]); err != nil {
			return err
		}
	}
	return linkMany(graph, stageRefs[len(stageRefs)-1:], sinkRefs)
}

func (b *builder) compileExplicitRoutes(graph pipeline.Graph, nodes map[string]pipeline.NodeRef) error {
	for i := range b.routes {
		route, err := resolveRuntimeRoute(nodes, b.routes[i])
		if err != nil {
			return err
		}
		if err := graph.Connect(route); err != nil {
			return err
		}
	}
	return nil
}

func resolveRuntimeRoute(nodes map[string]pipeline.NodeRef, route pipeline.Route) (pipeline.Route, error) {
	from, err := resolveRuntimeNode(nodes, pipeline.NodeRef(route.From))
	if err != nil {
		return pipeline.Route{}, err
	}
	to := make([]string, len(route.To))
	for i := range route.To {
		ref, err := resolveRuntimeNode(nodes, pipeline.NodeRef(route.To[i]))
		if err != nil {
			return pipeline.Route{}, err
		}
		to[i] = ref.String()
	}
	route.From = from.String()
	route.To = to
	return route, nil
}

func addRuntimeNodes(nodes map[string]pipeline.NodeRef, refs []pipeline.NodeRef) {
	for i := range refs {
		nodes[refs[i].String()] = refs[i]
	}
}

func resolveRuntimeNode(nodes map[string]pipeline.NodeRef, ref pipeline.NodeRef) (pipeline.NodeRef, error) {
	node, ok := nodes[ref.String()]
	if !ok {
		return "", pipeline.ErrUnknownNode
	}
	return node, nil
}

func linkMany(graph pipeline.Graph, from []pipeline.NodeRef, to []pipeline.NodeRef) error {
	for i := range from {
		for j := range to {
			if err := connectRefs(graph, from[i], to[j]); err != nil {
				return err
			}
		}
	}
	return nil
}

func connectRefs(graph pipeline.Graph, from pipeline.NodeRef, to pipeline.NodeRef) error {
	return graph.Connect(pipeline.Route{
		From:   from.String(),
		To:     []string{to.String()},
		Policy: pipeline.RouteAll,
	})
}

type task struct {
	graph        pipeline.Graph
	runtime      *runtime
	destinations []*destinationTransaction
	taps         []info.Tap
	branchTaps   []info.Tap
	attachMu     sync.Mutex
	attachments  map[*runtimeAttachment]struct{}

	// watch fans the graph's event stream out to filtered Watch subscribers.
	watch eventWatch

	// rules is the dynamic-stream rule engine (OnStream grammar), installed at
	// build before the task escapes; nil when the job declared no rules.
	rules *taskStreamRules

	// lifecycleMu guards the recorded run/close progress below, which exists
	// only so Snapshot can report typed lifecycle states.
	lifecycleMu sync.Mutex
	started     bool
	finished    bool
	runErr      error
	closed      bool
}

func newTask(graph pipeline.Graph, runtime *runtime, destinations ...*destinationTransaction) *task {
	return &task{graph: graph, runtime: runtime, destinations: destinations}
}

func (t *task) Describe() pipeline.Spec {
	return t.graph.Spec()
}

func (t *task) Explain(context.Context) (info.Plan, error) {
	return info.Plan{
		Summary: "running media task",
		Graph:   t.Describe(),
		Taps:    t.Taps(),
	}, nil
}

func (t *task) Run(ctx context.Context) error {
	t.lifecycleMu.Lock()
	t.started = true
	t.lifecycleMu.Unlock()
	err := t.graph.Run(ctx)
	t.finishDestinations(err == nil)
	t.lifecycleMu.Lock()
	t.finished = true
	t.runErr = err
	t.lifecycleMu.Unlock()
	return err
}

func (t *task) Events() <-chan av.Event {
	if t.rules != nil {
		// The rule engine holds an internal Watch subscription, so the raw
		// graph channel is already being drained by the watch distributor.
		// Hand every Events caller its own unfiltered subscription — the
		// documented remedy once Watch is in use — so no consumer competes
		// with the engine for events.
		return t.Watch()
	}
	return t.graph.Events()
}

func (t *task) Stats() pipeline.GraphStats {
	return t.graph.Stats()
}

func (t *task) Taps() []info.Tap {
	t.attachMu.Lock()
	defer t.attachMu.Unlock()
	return t.tapsLocked()
}

func (t *task) Snapshot() info.TaskSnapshot {
	if t == nil {
		return info.TaskSnapshot{}
	}
	state, _ := t.lifecycleStates()
	stats := t.Stats()
	t.attachMu.Lock()
	defer t.attachMu.Unlock()
	branches := make([]info.BranchSnapshot, 0, len(t.attachments))
	for attachment := range t.attachments {
		state := attachment.branchSnapshotLocked(stats)
		if state.ID == "" && state.Name == "" {
			continue
		}
		branches = append(branches, state)
	}
	return info.TaskSnapshot{
		State:        state,
		Spec:         t.Describe(),
		Stats:        stats,
		Taps:         t.tapsLocked(),
		Branches:     branches,
		Destinations: taskSnapshotDestinations(branches),
	}
}

// lifecycleStates derives the typed task state and the matching state of the
// task's still-open destinations from the recorded run/close progress. The
// destination outcome mirrors finishDestinations: a run that completes without
// error commits destinations, a failed run aborts them.
func (t *task) lifecycleStates() (info.TaskState, info.DestinationState) {
	t.lifecycleMu.Lock()
	defer t.lifecycleMu.Unlock()
	switch {
	case t.finished && t.runErr != nil:
		return info.TaskFailed, info.DestinationAborted
	case t.finished:
		return info.TaskClosed, info.DestinationCommitted
	case t.closed:
		return info.TaskClosed, info.DestinationClosed
	case t.started:
		return info.TaskRunning, info.DestinationOpen
	default:
		return info.TaskBuilt, info.DestinationOpen
	}
}

func taskSnapshotDestinations(branches []info.BranchSnapshot) []info.DestinationSnapshot {
	if len(branches) == 0 {
		return nil
	}
	out := make([]info.DestinationSnapshot, 0)
	seen := make(map[string]struct{})
	for i := range branches {
		for j := range branches[i].Destinations {
			destination := branches[i].Destinations[j]
			key := destination.Name + "\x00" + string(destination.Operation) + "\x00" + destination.Component
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, destination)
		}
	}
	return out
}

func (t *task) Detach(ctx context.Context, attachment Attachment) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if attachment == nil {
		return nil
	}
	if runtimeAttachment, ok := attachment.(*runtimeAttachment); ok {
		return t.stopAttachment(ctx, runtimeAttachment)
	}
	return attachment.Close(ctx)
}

func (t *task) stopAttachments(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t.attachMu.Lock()
	defer t.attachMu.Unlock()
	var first error
	for attachment := range t.attachments {
		if err := t.stopAttachmentLocked(ctx, attachment); first == nil && err != nil {
			first = err
		}
	}
	return first
}

func (t *task) Close() error {
	t.lifecycleMu.Lock()
	t.closed = true
	t.lifecycleMu.Unlock()
	first := t.stopAttachments(context.Background())
	if err := t.graph.Close(); first == nil && err != nil {
		first = err
	}
	return first
}

func (t *task) finishDestinations(ok bool) {
	for i := range t.destinations {
		if t.destinations[i] == nil {
			continue
		}
		if ok {
			t.destinations[i].Succeed()
		} else {
			t.destinations[i].Fail()
		}
	}
}

type destinationTransaction struct {
	mu             sync.Mutex
	requireSuccess bool
	succeeded      bool
	failed         bool
}

func (t *destinationTransaction) Succeed() {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.succeeded = true
	t.mu.Unlock()
}

func (t *destinationTransaction) Fail() {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.failed = true
	t.mu.Unlock()
}

func (t *destinationTransaction) ShouldAbort() bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.failed || (t.requireSuccess && !t.succeeded)
}
