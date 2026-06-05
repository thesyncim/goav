package goav

import (
	"context"
	"errors"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/transcode"
)

var (
	ErrUnsupportedBuild = errors.New("goav: unsupported builder graph")
	ErrNilSource        = errors.New("goav: nil source")
	ErrNilStage         = errors.New("goav: nil stage")
	ErrNilSink          = errors.New("goav: nil sink")
)

type Logger interface {
	Log(context.Context, string, ...any)
}

type Metrics interface {
	Count(context.Context, string, int64)
}

type Option func(*runtime)

func New(options ...Option) Runtime {
	runtime := &runtime{
		codecs:    codec.NewRegistry(),
		formats:   format.NewRegistry(format.WithProber(format.DefaultProber())),
		pipelines: pipeline.NewDirectFactory(),
		realtime:  true,
	}
	for _, option := range options {
		option(runtime)
	}
	return runtime
}

func WithCodecRegistry(registry codec.Registry) Option {
	return func(runtime *runtime) {
		if registry != nil {
			runtime.codecs = registry
		}
	}
}

func WithFormatRegistry(registry format.Registry) Option {
	return func(runtime *runtime) {
		if registry != nil {
			runtime.formats = registry
		}
	}
}

func WithPipelineFactory(factory pipeline.Factory) Option {
	return func(runtime *runtime) {
		if factory != nil {
			runtime.pipelines = factory
		}
	}
}

func WithCodecAdapter(register func(*codec.SimpleRegistry)) Option {
	return func(runtime *runtime) {
		registry, ok := runtime.codecs.(*codec.SimpleRegistry)
		if ok && register != nil {
			register(registry)
		}
	}
}

func WithBufferPolicy(policy pipeline.BufferPolicy) Option {
	return func(runtime *runtime) {
		runtime.buffer = policy
	}
}

func WithRealtime(realtime bool) Option {
	return func(runtime *runtime) {
		runtime.realtime = realtime
	}
}

func WithLogger(logger Logger) Option {
	return func(runtime *runtime) {
		runtime.logger = logger
	}
}

func WithMetrics(metrics Metrics) Option {
	return func(runtime *runtime) {
		runtime.metrics = metrics
	}
}

type runtime struct {
	codecs    codec.Registry
	formats   format.Registry
	pipelines pipeline.Factory
	buffer    pipeline.BufferPolicy
	realtime  bool
	logger    Logger
	metrics   Metrics
}

func (r *runtime) Codecs() codec.Registry {
	return r.codecs
}

func (r *runtime) Formats() format.Registry {
	return r.formats
}

func (r *runtime) Pipelines() pipeline.Factory {
	return r.pipelines
}

func (r *runtime) Probe(ctx context.Context, request ProbeRequest) (ProbeResult, error) {
	return r.formats.Probe(ctx, request)
}

func (r *runtime) New() Builder {
	return &builder{runtime: r}
}

type builder struct {
	runtime    *runtime
	inputs     []Input
	outputs    []Output
	decodes    []av.StreamSelector
	encodes    []encodeRequest
	filters    []filterRequest
	transcodes []transcode.Plan
	sources    []pipeline.Source
	stages     []pipeline.Stage
	sinks      []pipeline.Sink
	links      []pipeline.Link
	routes     []pipeline.Route
}

type encodeRequest struct {
	selector av.StreamSelector
	config   codec.EncodeConfig
}

type filterRequest struct {
	selector av.StreamSelector
	stage    pipeline.Stage
}

func (b *builder) Input(input Input) Builder {
	b.inputs = append(b.inputs, input)
	return b
}

func (b *builder) Output(output Output) Builder {
	b.outputs = append(b.outputs, output)
	return b
}

func (b *builder) Decode(selector av.StreamSelector) Builder {
	b.decodes = append(b.decodes, selector)
	return b
}

func (b *builder) Encode(selector av.StreamSelector, config codec.EncodeConfig) Builder {
	b.encodes = append(b.encodes, encodeRequest{selector: selector, config: config})
	return b
}

func (b *builder) Filter(selector av.StreamSelector, stage pipeline.Stage) Builder {
	b.filters = append(b.filters, filterRequest{selector: selector, stage: stage})
	return b
}

func (b *builder) Transcode(plan transcode.Plan) Builder {
	b.transcodes = append(b.transcodes, plan)
	return b
}

func (b *builder) Source(source pipeline.Source) Builder {
	b.sources = append(b.sources, source)
	return b
}

func (b *builder) Stage(stage pipeline.Stage) Builder {
	b.stages = append(b.stages, stage)
	return b
}

func (b *builder) Sink(sink pipeline.Sink) Builder {
	b.sinks = append(b.sinks, sink)
	return b
}

func (b *builder) Connect(from string, to string) Builder {
	b.links = append(b.links, pipeline.Link{
		From: pipeline.PadRef{Node: from},
		To:   pipeline.PadRef{Node: to},
	})
	return b
}

func (b *builder) ConnectStream(from string, to string, stream av.StreamID) Builder {
	b.routes = append(b.routes, pipeline.Route{
		From:   pipeline.PadRef{Node: from},
		To:     []pipeline.PadRef{{Node: to}},
		Policy: pipeline.RouteByStream,
		Label:  string(stream),
	})
	return b
}

func (b *builder) ConnectEvent(from string, to string, event av.EventType) Builder {
	b.routes = append(b.routes, pipeline.Route{
		From:   pipeline.PadRef{Node: from},
		To:     []pipeline.PadRef{{Node: to}},
		Policy: pipeline.RouteByEvent,
		Label:  string(event),
	})
	return b
}

func (b *builder) Link(link pipeline.Link) Builder {
	b.links = append(b.links, link)
	return b
}

func (b *builder) Route(route pipeline.Route) Builder {
	b.routes = append(b.routes, route)
	return b
}

func (b *builder) Build(ctx context.Context) (Task, error) {
	compiler, err := b.selectCompiler()
	if err != nil {
		return nil, err
	}
	return compiler.build(ctx, b)
}

func (b *builder) hasHighLevelRequests() bool {
	return len(b.inputs) != 0 || len(b.outputs) != 0 || len(b.decodes) != 0 ||
		len(b.encodes) != 0 || len(b.filters) != 0 || len(b.transcodes) != 0
}

func (b *builder) hasExplicitGraph() bool {
	return len(b.sources) != 0 || len(b.stages) != 0 || len(b.sinks) != 0 ||
		len(b.links) != 0 || len(b.routes) != 0
}

func (b *builder) compileExplicitGraph(graph pipeline.Graph) error {
	if len(b.sources) == 0 {
		return ErrUnsupportedBuild
	}

	sourcePads := make([]pipeline.PadRef, len(b.sources))
	stagePads := make([]pipeline.PadRef, len(b.stages))
	sinkPads := make([]pipeline.PadRef, len(b.sinks))

	for i := range b.sources {
		if b.sources[i] == nil {
			return ErrNilSource
		}
		pad, err := graph.AddSource(b.sources[i], b.runtime.buffer)
		if err != nil {
			return err
		}
		sourcePads[i] = pad
	}
	for i := range b.stages {
		if b.stages[i] == nil {
			return ErrNilStage
		}
		pad, err := graph.AddStage(b.stages[i], b.runtime.buffer)
		if err != nil {
			return err
		}
		stagePads[i] = pad
	}
	for i := range b.sinks {
		if b.sinks[i] == nil {
			return ErrNilSink
		}
		pad, err := graph.AddSink(b.sinks[i], b.runtime.buffer)
		if err != nil {
			return err
		}
		sinkPads[i] = pad
	}

	if len(b.links) != 0 || len(b.routes) != 0 {
		pads := make(map[string]pipeline.PadRef, len(sourcePads)+len(stagePads)+len(sinkPads))
		addRuntimePads(pads, sourcePads)
		addRuntimePads(pads, stagePads)
		addRuntimePads(pads, sinkPads)
		return b.compileExplicitEdges(graph, pads)
	}

	if len(stagePads) == 0 {
		return linkMany(graph, sourcePads, sinkPads)
	}
	if err := linkMany(graph, sourcePads, stagePads[:1]); err != nil {
		return err
	}
	for i := 0; i < len(stagePads)-1; i++ {
		if err := graph.Link(pipeline.Link{From: stagePads[i], To: stagePads[i+1]}); err != nil {
			return err
		}
	}
	return linkMany(graph, stagePads[len(stagePads)-1:], sinkPads)
}

func (b *builder) compileExplicitEdges(graph pipeline.Graph, pads map[string]pipeline.PadRef) error {
	for i := range b.links {
		link, err := resolveRuntimeLink(pads, b.links[i])
		if err != nil {
			return err
		}
		if err := graph.Link(link); err != nil {
			return err
		}
	}
	for i := range b.routes {
		route, err := resolveRuntimeRoute(pads, b.routes[i])
		if err != nil {
			return err
		}
		if err := graph.Route(route); err != nil {
			return err
		}
	}
	return nil
}

func addRuntimePads(pads map[string]pipeline.PadRef, refs []pipeline.PadRef) {
	for i := range refs {
		pads[refs[i].Node] = refs[i]
	}
}

func resolveRuntimeLink(pads map[string]pipeline.PadRef, link pipeline.Link) (pipeline.Link, error) {
	from, err := resolveRuntimePad(pads, link.From)
	if err != nil {
		return pipeline.Link{}, err
	}
	to, err := resolveRuntimePad(pads, link.To)
	if err != nil {
		return pipeline.Link{}, err
	}
	link.From = from
	link.To = to
	return link, nil
}

func resolveRuntimeRoute(pads map[string]pipeline.PadRef, route pipeline.Route) (pipeline.Route, error) {
	from, err := resolveRuntimePad(pads, route.From)
	if err != nil {
		return pipeline.Route{}, err
	}
	route.From = from
	toRefs := make([]pipeline.PadRef, len(route.To))
	for i := range route.To {
		to, err := resolveRuntimePad(pads, route.To[i])
		if err != nil {
			return pipeline.Route{}, err
		}
		toRefs[i] = to
	}
	route.To = toRefs
	return route, nil
}

func resolveRuntimePad(pads map[string]pipeline.PadRef, ref pipeline.PadRef) (pipeline.PadRef, error) {
	if ref.Pad != "" {
		return ref, nil
	}
	pad, ok := pads[ref.Node]
	if !ok {
		return pipeline.PadRef{}, pipeline.ErrUnknownNode
	}
	return pad, nil
}

func linkMany(graph pipeline.Graph, from []pipeline.PadRef, to []pipeline.PadRef) error {
	for i := range from {
		for j := range to {
			if err := graph.Link(pipeline.Link{From: from[i], To: to[j]}); err != nil {
				return err
			}
		}
	}
	return nil
}

type task struct {
	graph pipeline.Graph
}

func (t *task) Describe() pipeline.Spec {
	return t.graph.Spec()
}

func (t *task) Run(ctx context.Context) error {
	return t.graph.Run(ctx)
}

func (t *task) Events() <-chan av.Event {
	return t.graph.Events()
}

func (t *task) Close() error {
	return t.graph.Close()
}
