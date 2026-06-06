package goav

import (
	"context"
	"errors"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/rtpav"
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
		codecs:   codec.NewRegistry(),
		filters:  filter.NewRegistry(),
		formats:  format.NewRegistry(format.WithProber(format.DefaultProber())),
		realtime: true,
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

func WithFilterRegistry(registry filter.Registry) Option {
	return func(runtime *runtime) {
		if registry != nil {
			runtime.filters = registry
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

func WithFormatAdapter(register func(*format.SimpleRegistry)) Option {
	return func(runtime *runtime) {
		registry, ok := runtime.formats.(*format.SimpleRegistry)
		if ok && register != nil {
			register(registry)
		}
	}
}

func WithFilterAdapter(register func(*filter.SimpleRegistry)) Option {
	return func(runtime *runtime) {
		registry, ok := runtime.filters.(*filter.SimpleRegistry)
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
	codecs   codec.Registry
	filters  filter.Registry
	formats  format.Registry
	buffer   pipeline.BufferPolicy
	realtime bool
	logger   Logger
	metrics  Metrics
}

func (r *runtime) Codecs() codec.Registry {
	return r.codecs
}

func (r *runtime) Formats() format.Registry {
	return r.formats
}

func (r *runtime) Filters() filter.Registry {
	return r.filters
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
	rtpInputs  []rtpInput
	outputs    []Output
	decodes    []av.StreamSelector
	encodes    []encodeRequest
	filters    []filterRequest
	transcodes []transcode.Plan
	sources    []pipeline.Source
	stages     []pipeline.Stage
	sinks      []pipeline.Sink
	routes     []pipeline.Route
}

type encodeRequest struct {
	name     string
	selector av.StreamSelector
	config   codec.EncodeConfig
}

type filterRequest struct {
	selector av.StreamSelector
	stage    pipeline.Stage
}

type RTPOption func(*rtpInput)

type RTPBufferLimits struct {
	MaxReady    int
	MaxEvents   int
	MaxFeedback int
	MaxPackets  int
}

type rtpInput struct {
	name          string
	receiver      rtpav.PacketReader
	feedback      rtpav.FeedbackWriter
	jitter        rtpav.JitterBuffer
	depacketizers []rtpav.Depacketizer
	limits        RTPBufferLimits
	decodeBounds  codec.DecodeBounds
	maxTSGap      av.Duration
}

func (b *builder) Input(input Input) Builder {
	b.inputs = append(b.inputs, input)
	return b
}

func (b *builder) RTP(receiver rtpav.PacketReader, options ...RTPOption) Builder {
	input := rtpInput{receiver: receiver}
	for i := range options {
		if options[i] != nil {
			options[i](&input)
		}
	}
	b.rtpInputs = append(b.rtpInputs, input)
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

func (b *builder) Routes(routes ...pipeline.Route) Builder {
	b.routes = append(b.routes, routes...)
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
	return len(b.inputs) != 0 || len(b.rtpInputs) != 0 || len(b.outputs) != 0 || len(b.decodes) != 0 ||
		len(b.encodes) != 0 || len(b.filters) != 0 || len(b.transcodes) != 0
}

func (b *builder) hasExplicitGraph() bool {
	return len(b.sources) != 0 || len(b.stages) != 0 || len(b.sinks) != 0 ||
		len(b.routes) != 0
}

func (b *builder) compileExplicitGraph(graph pipeline.Graph) error {
	if len(b.sources) == 0 {
		return ErrUnsupportedBuild
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
