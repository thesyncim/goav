package goav

import (
	"context"
	"errors"
	"sync"

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

func WithCodecDescriptor(desc CodecDescriptor) Option {
	return WithCodecAdapter(func(registry *codec.SimpleRegistry) {
		registry.RegisterDescriptor(desc)
	})
}

func WithDecoder(desc CodecDescriptor, factory DecoderFactory) Option {
	return WithCodecAdapter(func(registry *codec.SimpleRegistry) {
		registry.RegisterDecoder(desc, factory)
	})
}

func WithEncoder(desc CodecDescriptor, factory EncoderFactory) Option {
	return WithCodecAdapter(func(registry *codec.SimpleRegistry) {
		registry.RegisterEncoder(desc, factory)
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

func WithRealtime(realtime bool) Option {
	return func(runtime *runtime) {
		runtime.realtime = realtime
	}
}

type runtime struct {
	codecs   *codec.SimpleRegistry
	filters  *filter.SimpleRegistry
	formats  *format.SimpleRegistry
	buffer   pipeline.BufferPolicy
	realtime bool
}

type builderAPI interface {
	Input(format.Input) builderAPI
	RTP(rtpav.PacketReader, ...rtpOption) builderAPI
	Mux(format.Output) builderAPI
	Decode(av.StreamSelector) builderAPI
	Encode(av.StreamSelector, codec.EncodeConfig) builderAPI
	Filter(av.StreamSelector, pipeline.Stage) builderAPI
	Transcode(transcode.Plan) builderAPI
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

func (r *runtime) Graph() GraphBuilder {
	return &graphBuilder{builder: r.New()}
}

type builder struct {
	runtime         *runtime
	inputs          []format.Input
	rtpInputs       []rtpInput
	outputs         []format.Output
	outputFmts      []av.FormatID
	outputGraphFmts []av.FormatID
	decodes         []decodeRequest
	encodes         []encodeRequest
	filters         []filterRequest
	transcodes      []transcode.Plan
	sources         []pipeline.Source
	stages          []pipeline.Stage
	sinks           []pipeline.Sink
	routes          []pipeline.Route
}

type encodeRequest struct {
	name     string
	selector av.StreamSelector
	config   codec.EncodeConfig
}

type decodeRequest struct {
	selector    av.StreamSelector
	codecChange CodecChangePolicy
	config      CodecSpec
}

type filterRequest struct {
	selector  av.StreamSelector
	stage     pipeline.Stage
	transform *mediaTransform
}

type rtpOption func(*rtpInput)

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
	codec         CodecSpec
	limits        RTPBufferLimits
	decodeBounds  codec.DecodeBounds
	maxTSGap      av.Duration
}

func (b *builder) Input(input format.Input) builderAPI {
	b.inputs = append(b.inputs, input)
	return b
}

func (b *builder) RTP(receiver rtpav.PacketReader, options ...rtpOption) builderAPI {
	input := rtpInput{receiver: receiver}
	for i := range options {
		if options[i] != nil {
			options[i](&input)
		}
	}
	b.rtpInputs = append(b.rtpInputs, input)
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

func (b *builder) transform(selector av.StreamSelector, transform mediaTransform) builderAPI {
	b.filters = append(b.filters, filterRequest{selector: selector, transform: &transform})
	return b
}

func (b *builder) Transcode(plan transcode.Plan) builderAPI {
	b.transcodes = append(b.transcodes, plan)
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
	graph       pipeline.Graph
	runtime     *runtime
	taps        []TapInfo
	branchTaps  []TapInfo
	attachMu    sync.Mutex
	attachments map[*runtimeAttachment]struct{}
}

func newTask(graph pipeline.Graph, runtime *runtime) *task {
	return &task{graph: graph, runtime: runtime}
}

func (t *task) Describe() pipeline.Spec {
	return t.graph.Spec()
}

func (t *task) Explain(context.Context) (PlanReport, error) {
	return PlanReport{
		Summary: "running media task",
		Graph:   t.Describe(),
		Taps:    tapReports(t.Taps()),
	}, nil
}

func (t *task) Run(ctx context.Context) error {
	return t.graph.Run(ctx)
}

func (t *task) Events() <-chan av.Event {
	return t.graph.Events()
}

func (t *task) Stats() TaskStats {
	return t.graph.Stats()
}

func (t *task) Taps() []TapInfo {
	t.attachMu.Lock()
	defer t.attachMu.Unlock()
	return t.tapsLocked()
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
	first := t.stopAttachments(context.Background())
	if err := t.graph.Close(); first == nil && err != nil {
		first = err
	}
	return first
}
