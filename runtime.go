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

func (b *builder) Build(ctx context.Context) (Task, error) {
	if b.hasHighLevelRequests() {
		return nil, ErrUnsupportedBuild
	}
	graph, err := b.runtime.pipelines.NewGraph(ctx, pipeline.GraphConfig{
		Name:     "goav",
		Realtime: b.runtime.realtime,
		Buffer:   b.runtime.buffer,
	})
	if err != nil {
		return nil, err
	}
	if b.hasExplicitGraph() {
		if err := b.compileExplicitGraph(graph); err != nil {
			graph.Close()
			return nil, err
		}
	}
	return &task{graph: graph}, nil
}

func (b *builder) hasHighLevelRequests() bool {
	return len(b.inputs) != 0 || len(b.outputs) != 0 || len(b.decodes) != 0 ||
		len(b.encodes) != 0 || len(b.filters) != 0 || len(b.transcodes) != 0
}

func (b *builder) hasExplicitGraph() bool {
	return len(b.sources) != 0 || len(b.stages) != 0 || len(b.sinks) != 0
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
