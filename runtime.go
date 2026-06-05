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

var ErrUnsupportedBuild = errors.New("goav: unsupported builder graph")

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

func (b *builder) Build(ctx context.Context) (Task, error) {
	if len(b.inputs) != 0 || len(b.outputs) != 0 || len(b.decodes) != 0 ||
		len(b.encodes) != 0 || len(b.filters) != 0 || len(b.transcodes) != 0 {
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
	return &task{graph: graph}, nil
}

type task struct {
	graph pipeline.Graph
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
