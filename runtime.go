package goav

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/lifecycle"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/snapshot"
)

// Build-refusal sentinels. Build-time BuildErrors wrap one of these as their
// Cause, so errors.Is classifies a refusal while errors.As + the Code field
// identify it precisely. Runtime safety wrappers may instead carry a pipeline
// sentinel when preserving low-level errors.Is compatibility matters.
var (
	// ErrUnsupportedBuild is the cause behind every build-shape refusal: the
	// declared recipe or graph cannot be lowered as written.
	ErrUnsupportedBuild = errors.New("goav: unsupported builder graph")
	// ErrNilSource reports a nil source handed to a builder or constructor.
	ErrNilSource = errors.New("goav: nil source")
	// ErrNilStage reports a nil stage handed to a builder or .Do(...).
	ErrNilStage = errors.New("goav: nil stage")
	// ErrNilSink reports a nil sink handed to a builder or goav.Sink(...).
	ErrNilSink = errors.New("goav: nil sink")
	// ErrNilWriter reports a nil writer handed to a byte destination.
	ErrNilWriter = errors.New("goav: nil writer")
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

// Option configures a runtime under construction: registries (codecs,
// formats, filters), pacing (WithRealtime, WithClock), and graph policy
// (WithBufferPolicy, WithEventCapacity). Registration is last-wins, so an
// option layered over Default() can override a standard adapter.
type Option func(*runtime)

// New builds a bare runtime: per-runtime registries with no adapters beyond
// content sniffing, realtime pacing on. Use Default(opts...) for a runtime
// with the standard adapters already registered; use New when the application
// controls every codec, format, and filter explicitly.
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

// WithCodecAdapter registers a whole codec bundle at once: the callback
// receives the runtime's codec registry. Use WithDecoder/WithEncoder for the
// direct single-value form.
func WithCodecAdapter(register func(*codec.SimpleRegistry)) Option {
	return func(runtime *runtime) {
		if register != nil {
			register(runtime.codecs)
		}
	}
}

// WithCodecDescriptor registers a codec descriptor without an implementation,
// so capability checks (Explain, validation) recognize the codec even when
// encode/decode factories come from elsewhere.
func WithCodecDescriptor(desc codec.Descriptor) Option {
	return WithCodecAdapter(func(registry *codec.SimpleRegistry) {
		registry.RegisterDescriptor(desc)
	})
}

// WithDecoder adds one decoder factory by descriptor — pass the
// implementation directly, no registry callback. The descriptor's
// capabilities drive compatibility checks before anything opens.
func WithDecoder(desc codec.Descriptor, factory codec.DecoderFactory) Option {
	return WithCodecAdapter(func(registry *codec.SimpleRegistry) {
		registry.RegisterDecoder(desc, factory)
	})
}

// WithEncoder adds one encoder factory by descriptor — pass the
// implementation directly, no registry callback. The descriptor's
// capabilities drive compatibility checks before anything opens.
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

// WithFormatAdapter registers a whole container-format bundle at once: the
// callback receives the runtime's format registry. Use
// WithMuxer/WithDemuxer/WithProber for the direct single-value form.
func WithFormatAdapter(register func(*format.SimpleRegistry)) Option {
	return func(runtime *runtime) {
		if register != nil {
			register(runtime.formats)
		}
	}
}

// WithFilterAdapter registers a whole filter bundle at once: the callback
// receives the runtime's filter registry. Use WithFilter for the direct
// single-value form.
func WithFilterAdapter(register func(*filter.SimpleRegistry)) Option {
	return func(runtime *runtime) {
		if register != nil {
			register(runtime.filters)
		}
	}
}

// WithBufferPolicy sets the default node buffer policy for graphs this
// runtime builds — the queue depth and overflow behavior between nodes.
// Branch-local .Buffer(flow....) declarations override it per branch.
func WithBufferPolicy(policy pipeline.BufferPolicy) Option {
	return func(runtime *runtime) {
		runtime.buffer = policy
	}
}

// WithEventCapacity sets the buffered capacity of the task event stream (and
// of each Watch subscriber's channel). A watcher that falls behind by more
// than this sheds events for itself only.
func WithEventCapacity(capacity int) Option {
	return func(runtime *runtime) {
		runtime.eventCapacity = capacity
	}
}

// WithRealtime selects pacing: true (the default) delivers file media when
// its media time is due on the runtime clock, so Rate works as a live pacing
// multiplier; false pumps at full speed (offline transcode) and rejects Rate
// with format.ErrRateUnsupported.
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

func (r *runtime) Probe(ctx context.Context, request format.ProbeRequest) (format.ProbeResult, error) {
	return r.formats.Probe(ctx, request)
}

// New returns the explicit-graph builder behind expert.Graph(runtime). The
// recipe compiler lowers high-level jobs straight onto pipeline graphs; the
// builder only assembles caller-provided Source/Stage/Sink nodes and routes.
func (r *runtime) New() *builder {
	return &builder{runtime: r}
}

type builder struct {
	runtime        *runtime
	destinationTxs []*destinationTransaction
	requireRunOK   bool
	sources        []pipeline.Source
	stages         []pipeline.Stage
	sinks          []pipeline.Sink
	routes         []pipeline.Route
}

func (b *builder) newGraph(_ context.Context) (pipeline.Graph, error) {
	return pipeline.NewGraph(pipeline.GraphConfig{
		Name:          "goav",
		Realtime:      b.runtime.realtime,
		Buffer:        b.runtime.buffer,
		EventCapacity: b.runtime.eventCapacity,
	})
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

func (b *builder) Source(source pipeline.Source) *builder {
	b.sources = append(b.sources, source)
	return b
}

func (b *builder) Stage(stage pipeline.Stage) *builder {
	b.stages = append(b.stages, stage)
	return b
}

func (b *builder) Sink(sink pipeline.Sink) *builder {
	b.sinks = append(b.sinks, sink)
	return b
}

func (b *builder) Routes(routes ...pipeline.Route) *builder {
	b.routes = append(b.routes, routes...)
	return b
}

func (b *builder) Build(ctx context.Context) (Task, error) {
	b.destinationTxs = nil
	b.requireRunOK = true
	graph, err := b.newGraph(ctx)
	if err != nil {
		return nil, err
	}
	if b.hasExplicitGraph() {
		if err := b.compileExplicitGraph(graph); err != nil {
			graph.Close()
			return nil, err
		}
	}
	return newTask(graph, b.runtime, b.destinationTxs...), nil
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
	taps         []snapshot.Tap
	branchTaps   []snapshot.Tap
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

func (t *task) Explain(context.Context) (plan.Report, error) {
	return plan.Report{
		Summary: "running media task",
		Graph:   t.Describe(),
		Taps:    planTapRows(t.Taps()),
	}, nil
}

// EncoderDescriptors lists encoder factories registered on the task runtime.
// It is intentionally outside the public Task interface: control-plane helpers
// use it through an optional interface when a task can describe its runtime.
func (t *task) EncoderDescriptors() []codec.Descriptor {
	if t == nil || t.runtime == nil || t.runtime.codecs == nil {
		return nil
	}
	descriptors, err := t.runtime.codecs.Find("", codec.ModeEncode)
	if err != nil {
		return nil
	}
	out := make([]codec.Descriptor, 0, len(descriptors))
	seen := make(map[av.CodecID]struct{}, len(descriptors))
	for _, desc := range descriptors {
		if desc.ID == "" {
			continue
		}
		if _, ok := seen[desc.ID]; ok {
			continue
		}
		if _, err := t.runtime.codecs.EncoderFactory(desc.ID); err != nil {
			continue
		}
		out = append(out, desc)
		seen[desc.ID] = struct{}{}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ID != out[j].ID {
			return out[i].ID < out[j].ID
		}
		return out[i].Type < out[j].Type
	})
	return out
}

// MuxerDescriptors lists muxer factories registered on the task runtime.
func (t *task) MuxerDescriptors() []format.Descriptor {
	if t == nil || t.runtime == nil || t.runtime.formats == nil {
		return nil
	}
	return t.runtime.formats.MuxerDescriptors()
}

// planTapRows projects live task taps onto the plan report's tap rows.
func planTapRows(taps []snapshot.Tap) []plan.Tap {
	rows := make([]plan.Tap, 0, len(taps))
	for i := range taps {
		rows = append(rows, plan.Tap{
			Name:      taps[i].Name,
			MediaKind: taps[i].MediaKind,
			Domain:    taps[i].Domain,
			Shape:     taps[i].Shape,
			Node:      taps[i].Node,
		})
	}
	return rows
}

func (t *task) Run(ctx context.Context) error {
	t.lifecycleMu.Lock()
	t.started = true
	t.lifecycleMu.Unlock()
	err := t.structuredRunError(t.graph.Run(ctx))
	t.finishDestinations(err == nil)
	t.lifecycleMu.Lock()
	t.finished = true
	t.runErr = err
	t.lifecycleMu.Unlock()
	return err
}

func (t *task) structuredRunError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, pipeline.ErrBufferedMessageUnsafe):
		return t.bufferedPayloadRunError(err, errcode.BufferPayloadUnsafe,
			"a buffered edge refused a mutable payload that cannot be shared safely")
	case errors.Is(err, pipeline.ErrMessageTooLarge):
		return t.bufferedPayloadRunError(err, errcode.BufferPayloadTooLarge,
			"a buffered edge received a payload larger than its copy bounds")
	default:
		return err
	}
}

func (t *task) bufferedPayloadRunError(cause error, code errcode.Code, reason string) error {
	details := []string{"cause=" + bufferedPayloadCauseName(cause)}
	if branches := t.copyNeverBranchNames(); len(branches) != 0 {
		details = append(details, "copy_never_branches="+strings.Join(branches, ","))
	}
	return &BuildError{
		Code:      code,
		Operation: "run task",
		Node:      "buffered graph",
		Reason:    reason,
		Details:   details,
		Suggestions: []string{
			"for branch buffers, use flow.BufferCopyBounds(packetBytes, frameBytes) with bounds large enough for the payload",
			"when using flow.CopyNever, emit av.BufferImmutable payloads only or switch to flow.CopyIfMutable/flow.CopyAlways",
			"for runtime-level buffers, set goav.WithBufferPolicy(pipeline.BufferPolicy{Capacity: ..., Drop: pipeline.DropBlock, CopyPacketBytes: ..., CopyFrameBytes: ...})",
		},
		Cause: cause,
	}
}

func bufferedPayloadCauseName(err error) string {
	switch {
	case errors.Is(err, pipeline.ErrBufferedMessageUnsafe):
		return "pipeline.ErrBufferedMessageUnsafe"
	case errors.Is(err, pipeline.ErrMessageTooLarge):
		return "pipeline.ErrMessageTooLarge"
	default:
		return err.Error()
	}
}

func (t *task) copyNeverBranchNames() []string {
	if t == nil {
		return nil
	}
	t.attachMu.Lock()
	defer t.attachMu.Unlock()
	var out []string
	for attachment := range t.attachments {
		if attachment == nil || attachment.stopped {
			continue
		}
		out = append(out, attachment.copyNever...)
	}
	return uniqueStrings(out)
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

func (t *task) Taps() []snapshot.Tap {
	t.attachMu.Lock()
	defer t.attachMu.Unlock()
	return t.tapsLocked()
}

func (t *task) Snapshot() snapshot.Task {
	if t == nil {
		return snapshot.Task{}
	}
	state, _ := t.lifecycleStates()
	stats := t.Stats()
	t.attachMu.Lock()
	defer t.attachMu.Unlock()
	branches := make([]snapshot.Branch, 0, len(t.attachments))
	for attachment := range t.attachments {
		state := attachment.branchSnapshotLocked(stats)
		if state.ID == "" && state.Name == "" {
			continue
		}
		branches = append(branches, state)
	}
	return snapshot.Task{
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
func (t *task) lifecycleStates() (lifecycle.TaskState, lifecycle.DestinationState) {
	t.lifecycleMu.Lock()
	defer t.lifecycleMu.Unlock()
	switch {
	case t.finished && t.runErr != nil:
		return lifecycle.TaskFailed, lifecycle.DestinationAborted
	case t.finished:
		return lifecycle.TaskClosed, lifecycle.DestinationCommitted
	case t.closed:
		return lifecycle.TaskClosed, lifecycle.DestinationClosed
	case t.started:
		return lifecycle.TaskRunning, lifecycle.DestinationOpen
	default:
		return lifecycle.TaskBuilt, lifecycle.DestinationOpen
	}
}

func taskSnapshotDestinations(branches []snapshot.Branch) []snapshot.Destination {
	if len(branches) == 0 {
		return nil
	}
	out := make([]snapshot.Destination, 0)
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
