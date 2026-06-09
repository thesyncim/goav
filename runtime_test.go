package goav

import (
	"context"
	"errors"
	"strings"
	"testing"

	gopusadapter "github.com/thesyncim/goav/adapters/gopus"
	ivfadapter "github.com/thesyncim/goav/adapters/ivf"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/graphrender"
	"github.com/thesyncim/goav/pipeline"
)

type runtimeTestSource struct {
	name    string
	message pipeline.Message
	closed  bool
}

func (s *runtimeTestSource) Name() string {
	return s.name
}

func (s *runtimeTestSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	return emitter.Emit(ctx, &s.message)
}

func (s *runtimeTestSource) Close() error {
	s.closed = true
	return nil
}

type runtimeEventBurstSource struct {
	name   string
	count  int
	closed bool
}

func (s *runtimeEventBurstSource) Name() string {
	return s.name
}

func (s *runtimeEventBurstSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	for i := 0; i < s.count; i++ {
		event := av.Event{Type: av.EventStats}
		if err := emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageEvent, Event: &event}); err != nil {
			return err
		}
	}
	return nil
}

func (s *runtimeEventBurstSource) Close() error {
	s.closed = true
	return nil
}

type runtimeTestStage struct {
	name   string
	count  int
	closed bool
}

func (s *runtimeTestStage) Name() string {
	return s.name
}

func (s *runtimeTestStage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	s.count++
	return emitter.Emit(ctx, msg)
}

func (s *runtimeTestStage) Close() error {
	s.closed = true
	return nil
}

type runtimeTestSink struct {
	name            string
	count           int
	frames          int
	lastPacket      *av.Packet
	lastPacketValue av.Packet
	lastFrame       av.Frame
	closed          bool
}

func (s *runtimeTestSink) Name() string {
	return s.name
}

func (s *runtimeTestSink) Handle(_ context.Context, msg *pipeline.Message) error {
	s.count++
	if msg.Kind == pipeline.MessagePacket {
		s.lastPacket = msg.Packet
		if msg.Packet != nil {
			s.lastPacketValue = *msg.Packet
		}
	}
	if msg.Kind == pipeline.MessageFrame {
		s.frames++
		if msg.Frame != nil {
			s.lastFrame = *msg.Frame
		}
	}
	return nil
}

func (s *runtimeTestSink) Close() error {
	s.closed = true
	return nil
}

func runtimeValue(t *testing.T, rt Runtime) *runtime {
	t.Helper()
	r, ok := rt.(*runtime)
	if !ok {
		t.Fatalf("runtime = %T, want *runtime", rt)
	}
	return r
}

func newTestBuilder(t *testing.T, options ...Option) builderAPI {
	t.Helper()
	return runtimeValue(t, New(options...)).New()
}

func testSelectAudio() av.StreamSelector {
	return av.StreamSelector{Type: av.MediaAudio}
}

func testSelectVideo() av.StreamSelector {
	return av.StreamSelector{Type: av.MediaVideo}
}

func testRoute(from string, to ...string) pipeline.Route {
	return pipeline.Route{
		From:   from,
		To:     append([]string(nil), to...),
		Policy: pipeline.RouteAll,
	}
}

func specDOT(spec pipeline.Spec) string {
	return specRenderURI(spec, "goav://graph/dot")
}

func specText(spec pipeline.Spec) string {
	return specRenderURI(spec, "goav:graph")
}

func specMermaid(spec pipeline.Spec) string {
	return specRenderURI(spec, "goav:graph?format=mermaid")
}

func specRenderURI(spec pipeline.Spec, target string) string {
	out, err := graphrender.RenderURI(spec, target)
	if err != nil {
		return err.Error()
	}
	return out
}

func withTestCodecs(configure ...func(*codec.SimpleRegistry)) Option {
	return WithCodecAdapter(func(registry *codec.SimpleRegistry) {
		for i := range configure {
			configure[i](registry)
		}
	})
}

func withTestFormats(configure ...func(*format.SimpleRegistry)) Option {
	return WithFormatAdapter(func(registry *format.SimpleRegistry) {
		for i := range configure {
			configure[i](registry)
		}
	})
}

func withTestFilters(configure ...func(*filter.SimpleRegistry)) Option {
	return WithFilterAdapter(func(registry *filter.SimpleRegistry) {
		for i := range configure {
			configure[i](registry)
		}
	})
}

func withTestCodecDescriptor(desc codec.Descriptor) Option {
	return WithCodecAdapter(func(registry *codec.SimpleRegistry) {
		registry.RegisterDescriptor(desc)
	})
}

func testCodecDecoder(desc codec.Descriptor, factory codec.DecoderFactory) func(*codec.SimpleRegistry) {
	return func(registry *codec.SimpleRegistry) {
		registry.RegisterDecoder(desc, factory)
	}
}

func testCodecEncoder(desc codec.Descriptor, factory codec.EncoderFactory) func(*codec.SimpleRegistry) {
	return func(registry *codec.SimpleRegistry) {
		registry.RegisterEncoder(desc, factory)
	}
}

func testFormatProber(prober format.Prober) func(*format.SimpleRegistry) {
	return func(registry *format.SimpleRegistry) {
		registry.RegisterProber(prober)
	}
}

func testFormatDemuxer(id av.FormatID, factory format.DemuxerFactory) func(*format.SimpleRegistry) {
	return func(registry *format.SimpleRegistry) {
		registry.RegisterDemuxer(id, factory)
	}
}

func testFormatMuxer(id av.FormatID, factory format.MuxerFactory) func(*format.SimpleRegistry) {
	return func(registry *format.SimpleRegistry) {
		registry.RegisterMuxer(id, factory)
	}
}

func testFilterFactory(desc filter.Descriptor, factory filter.Factory) func(*filter.SimpleRegistry) {
	return func(registry *filter.SimpleRegistry) {
		registry.RegisterFactory(desc, factory)
	}
}

func TestNewRuntimeDefaults(t *testing.T) {
	rt := runtimeValue(t, New())
	if rt.codecs == nil || rt.formats == nil || rt.filters == nil {
		t.Fatalf("runtime defaults incomplete: %+v", rt)
	}
	if _, err := rt.Probe(context.Background(), format.ProbeRequest{}); !errors.Is(err, format.ErrNotFound) {
		t.Fatalf("probe err = %v, want format.ErrNotFound", err)
	}
	result, err := rt.Probe(context.Background(), format.ProbeRequest{Name: "audio.opus"})
	if err != nil {
		t.Fatalf("probe opus: %v", err)
	}
	if result.Format != av.FormatOgg {
		t.Fatalf("format = %s, want ogg", result.Format)
	}
}

func TestRuntimeWithCodecAdapter(t *testing.T) {
	rt := runtimeValue(t, New(WithCodecAdapter(gopusadapter.Register)))

	if _, err := rt.codecs.DecoderFactory(av.CodecOpus); err != nil {
		t.Fatalf("decoder factory: %v", err)
	}
}

func TestRuntimeWithCustomCodecHooks(t *testing.T) {
	desc := CodecDescriptor{
		ID:    av.CodecID("x_pcm"),
		Name:  "X PCM",
		Type:  av.MediaAudio,
		Modes: []codec.Mode{codec.ModeDecode, codec.ModeEncode},
	}

	descriptorOnly := runtimeValue(t, New(WithCodecDescriptor(desc)))
	if _, err := descriptorOnly.codecs.DecoderFactory(desc.ID); !errors.Is(err, codec.ErrUnavailable) {
		t.Fatalf("descriptor-only decoder err = %v, want codec.ErrUnavailable", err)
	}
	if _, err := descriptorOnly.codecs.EncoderFactory(desc.ID); !errors.Is(err, codec.ErrUnavailable) {
		t.Fatalf("descriptor-only encoder err = %v, want codec.ErrUnavailable", err)
	}

	decoderFactory := &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}
	encoderFactory := &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}
	rt := runtimeValue(t, New(
		WithDecoder(desc, decoderFactory),
		WithEncoder(desc, encoderFactory),
	))
	if got, err := rt.codecs.DecoderFactory(desc.ID); err != nil || got != decoderFactory {
		t.Fatalf("decoder factory = %T err=%v, want registered custom factory", got, err)
	}
	if got, err := rt.codecs.EncoderFactory(desc.ID); err != nil || got != encoderFactory {
		t.Fatalf("encoder factory = %T err=%v, want registered custom factory", got, err)
	}

	spec := codec.Codec(desc.ID, av.MediaAudio, codec.SampleRate(16_000), codec.Channels(Mono), codec.ClockRate(48_000))
	if spec.ID != desc.ID || spec.Type != av.MediaAudio || spec.Parameters.ID != desc.ID ||
		spec.Parameters.Type != av.MediaAudio || spec.Parameters.SampleRate != 16_000 ||
		spec.Parameters.Channels != Mono || spec.Parameters.ClockRate != 48_000 {
		t.Fatalf("codec spec = %+v, want custom audio codec parameters", spec)
	}
}

func TestRuntimeWithFormatAdapter(t *testing.T) {
	rt := runtimeValue(t, New(WithFormatAdapter(ivfadapter.Register)))

	if _, err := rt.formats.DemuxerFactory(av.FormatIVF); err != nil {
		t.Fatalf("demuxer factory: %v", err)
	}
	if _, err := rt.formats.MuxerFactory(av.FormatIVF); err != nil {
		t.Fatalf("muxer factory: %v", err)
	}
}

func TestRuntimeWithFilterAdapter(t *testing.T) {
	rt := runtimeValue(t, New(WithFilterAdapter(func(registry *filter.SimpleRegistry) {
		registry.RegisterFactory(filter.Descriptor{Name: filter.FactoryResample}, &transcodeTestFilterFactory{})
	})))

	if _, err := rt.filters.Factory(filter.FactoryResample); err != nil {
		t.Fatalf("filter factory: %v", err)
	}
}

func TestRuntimeBuilderEmptyTask(t *testing.T) {
	task, err := newTestBuilder(t).Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeBuilderExplicitGraph(t *testing.T) {
	packet := av.Packet{StreamID: "audio"}
	source := &runtimeTestSource{
		name: "source",
		message: pipeline.Message{
			Kind:   pipeline.MessagePacket,
			Packet: &packet,
		},
	}
	stage := &runtimeTestStage{name: "stage"}
	left := &runtimeTestSink{name: "left"}
	right := &runtimeTestSink{name: "right"}

	task, err := newTestBuilder(t).
		Source(source).
		Stage(stage).
		Sink(left).
		Sink(right).
		Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	spec := task.Describe()
	if len(spec.Nodes) != 4 || len(spec.Edges) != 3 {
		t.Fatalf("nodes=%d edges=%d", len(spec.Nodes), len(spec.Edges))
	}
	if !strings.Contains(specText(spec), "stage -> left") ||
		!strings.Contains(specDOT(spec), "\"stage\" -> \"right\"") {
		t.Fatalf("spec text:\n%s\ndot:\n%s", specText(spec), specDOT(spec))
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stage.count != 1 {
		t.Fatalf("stage count = %d, want 1", stage.count)
	}
	if left.count != 1 || right.count != 1 {
		t.Fatalf("left=%d right=%d", left.count, right.count)
	}
	if left.lastPacket != &packet || right.lastPacket != &packet {
		t.Fatalf("packet fanout copied pointers: left=%p right=%p want=%p", left.lastPacket, right.lastPacket, &packet)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !source.closed || !stage.closed || !left.closed || !right.closed {
		t.Fatalf("closed source=%v stage=%v left=%v right=%v", source.closed, stage.closed, left.closed, right.closed)
	}
}

func TestRuntimeBuilderExplicitGraphWithBufferPolicy(t *testing.T) {
	packet := av.Packet{StreamID: "audio"}
	source := &runtimeTestSource{
		name: "source",
		message: pipeline.Message{
			Kind:   pipeline.MessagePacket,
			Packet: &packet,
		},
	}
	stage := &runtimeTestStage{name: "stage"}
	sink := &runtimeTestSink{name: "sink"}

	builder := newTestBuilder(t, WithBufferPolicy(pipeline.BufferPolicy{Capacity: 2, Drop: pipeline.DropOldest})).
		Source(source).
		Stage(stage).
		Sink(sink)
	planned, err := builder.Describe()
	if err != nil {
		t.Fatal(err)
	}

	task, err := builder.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if specText(planned) != specText(task.Describe()) {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", specText(planned), specText(task.Describe()))
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stage.count != 1 || sink.count != 1 || sink.lastPacketValue.StreamID != "audio" {
		t.Fatalf("stage=%d sink=%d packet=%+v", stage.count, sink.count, sink.lastPacketValue)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeBuilderExplicitGraphWithEventCapacity(t *testing.T) {
	source := &runtimeEventBurstSource{name: "events", count: 2}
	task, err := newTestBuilder(t, WithEventCapacity(2)).
		Source(source).
		Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := drainTaskEvents(task); len(got) != 2 {
		t.Fatalf("events = %+v, want 2", got)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !source.closed {
		t.Fatal("source not closed")
	}
}

func TestRuntimeBuilderDescribeBeforeBuild(t *testing.T) {
	packet := av.Packet{StreamID: "audio"}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}
	stage := &runtimeTestStage{name: "stage"}
	sink := &runtimeTestSink{name: "sink"}

	builder := newTestBuilder(t).
		Source(source).
		Stage(stage).
		Sink(sink)

	planned, err := builder.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if source.closed || stage.closed || sink.closed {
		t.Fatalf("describe closed nodes: source=%v stage=%v sink=%v", source.closed, stage.closed, sink.closed)
	}
	if len(planned.Nodes) != 3 || len(planned.Edges) != 2 {
		t.Fatalf("nodes=%d edges=%d", len(planned.Nodes), len(planned.Edges))
	}

	task, err := builder.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	built := task.Describe()
	if specText(planned) != specText(built) || specMermaid(planned) != specMermaid(built) {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", specText(planned), specText(built))
	}
}

func TestRuntimeBuilderExplicitSourceToSink(t *testing.T) {
	packet := av.Packet{StreamID: "audio"}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}
	sink := &runtimeTestSink{name: "sink"}

	task, err := newTestBuilder(t).
		Source(source).
		Sink(sink).
		Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sink.count != 1 || sink.lastPacket != &packet {
		t.Fatalf("sink count=%d packet=%p want=%p", sink.count, sink.lastPacket, &packet)
	}
}

func TestRouteHelpers(t *testing.T) {
	route := testRoute("source", "decode", "stats")
	if route.From != "source" || len(route.To) != 2 || route.To[0] != "decode" || route.To[1] != "stats" ||
		route.Policy != pipeline.RouteAll {
		t.Fatalf("route = %+v", route)
	}

	stream := testRoute("decode", "record").ByStream("video")
	if stream.Policy != pipeline.RouteByStream || stream.Label != "video" || len(stream.To) != 1 ||
		stream.To[0] != "record" {
		t.Fatalf("stream route = %+v", stream)
	}

	event := testRoute("source", "feedback").ByEvent(av.EventPacketLoss)
	if event.Policy != pipeline.RouteByEvent || event.Label != string(av.EventPacketLoss) || len(event.To) != 1 ||
		event.To[0] != "feedback" {
		t.Fatalf("event route = %+v", event)
	}
}

func TestRuntimeBuilderExplicitRoutes(t *testing.T) {
	packet := av.Packet{StreamID: "audio"}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}
	audio := &runtimeTestSink{name: "audio"}
	video := &runtimeTestSink{name: "video"}

	task, err := newTestBuilder(t).
		Source(source).
		Sink(audio).
		Sink(video).
		Routes(
			testRoute("source", "audio").ByStream("audio"),
			testRoute("source", "video").ByStream("video"),
		).
		Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	spec := task.Describe()
	if len(spec.Edges) != 2 {
		t.Fatalf("edges=%d, want 2", len(spec.Edges))
	}
	if !strings.Contains(specText(spec), "source -> audio [stream=audio]") ||
		!strings.Contains(specMermaid(spec), "-- \"stream=video\" -->") {
		t.Fatalf("spec:\n%s\nmermaid:\n%s", specText(spec), specMermaid(spec))
	}

	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if audio.count != 1 || video.count != 0 {
		t.Fatalf("audio=%d video=%d", audio.count, video.count)
	}
}

func TestRuntimeBuilderExplicitFanout(t *testing.T) {
	packet := av.Packet{StreamID: "video"}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}
	record := &runtimeTestSink{name: "record"}
	preview := &runtimeTestSink{name: "preview"}
	stats := &runtimeTestSink{name: "stats"}

	task, err := newTestBuilder(t).
		Source(source).
		Sink(record).
		Sink(preview).
		Sink(stats).
		Routes(testRoute("source", "record", "preview", "stats")).
		Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	spec := task.Describe()
	if len(spec.Edges) != 3 {
		t.Fatalf("edges=%d, want 3", len(spec.Edges))
	}
	if !strings.Contains(specText(spec), "source -> record") ||
		!strings.Contains(specText(spec), "source -> preview") ||
		!strings.Contains(specText(spec), "source -> stats") {
		t.Fatalf("spec:\n%s", specText(spec))
	}

	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if record.count != 1 || preview.count != 1 || stats.count != 1 {
		t.Fatalf("record=%d preview=%d stats=%d", record.count, preview.count, stats.count)
	}
}

func TestRuntimeBuilderExplicitStreamFanout(t *testing.T) {
	packet := av.Packet{StreamID: "video"}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}
	record := &runtimeTestSink{name: "record"}
	preview := &runtimeTestSink{name: "preview"}
	audio := &runtimeTestSink{name: "audio"}

	task, err := newTestBuilder(t).
		Source(source).
		Sink(record).
		Sink(preview).
		Sink(audio).
		Routes(
			testRoute("source", "record", "preview").ByStream("video"),
			testRoute("source", "audio").ByStream("audio"),
		).
		Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	spec := task.Describe()
	if !strings.Contains(specText(spec), "source -> record [stream=video]") ||
		!strings.Contains(specText(spec), "source -> preview [stream=video]") {
		t.Fatalf("spec:\n%s", specText(spec))
	}

	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if record.count != 1 || preview.count != 1 || audio.count != 0 {
		t.Fatalf("record=%d preview=%d audio=%d", record.count, preview.count, audio.count)
	}
}

func TestRuntimeBuilderExplicitRoutesHelper(t *testing.T) {
	packet := av.Packet{StreamID: "video"}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}
	record := &runtimeTestSink{name: "record"}
	preview := &runtimeTestSink{name: "preview"}
	audio := &runtimeTestSink{name: "audio"}

	task, err := newTestBuilder(t).
		Source(source).
		Sink(record).
		Sink(preview).
		Sink(audio).
		Routes(
			testRoute("source", "record", "preview").ByStream("video"),
			testRoute("source", "audio").ByStream("audio"),
		).
		Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	spec := task.Describe()
	if !strings.Contains(specText(spec), "source -> record [stream=video]") ||
		!strings.Contains(specText(spec), "source -> preview [stream=video]") {
		t.Fatalf("spec:\n%s", specText(spec))
	}

	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if record.count != 1 || preview.count != 1 || audio.count != 0 {
		t.Fatalf("record=%d preview=%d audio=%d", record.count, preview.count, audio.count)
	}
}

func TestRuntimeBuilderDescribeRoutesBeforeBuild(t *testing.T) {
	packet := av.Packet{StreamID: "audio"}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}
	audio := &runtimeTestSink{name: "audio"}
	video := &runtimeTestSink{name: "video"}

	spec, err := newTestBuilder(t).
		Source(source).
		Sink(audio).
		Sink(video).
		Routes(
			testRoute("source", "audio").ByStream("audio"),
			testRoute("source", "video").ByStream("video"),
		).
		Describe()
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Edges) != 2 {
		t.Fatalf("edges=%d, want 2", len(spec.Edges))
	}
	if !strings.Contains(specText(spec), "source -> audio [stream=audio]") ||
		!strings.Contains(specMermaid(spec), "-- \"stream=video\" -->") {
		t.Fatalf("spec:\n%s\nmermaid:\n%s", specText(spec), specMermaid(spec))
	}
}

func TestRuntimeBuilderExplicitRouteByEvent(t *testing.T) {
	event := av.Event{Type: av.EventStats}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessageEvent, Event: &event},
	}
	stats := &runtimeTestSink{name: "stats"}
	loss := &runtimeTestSink{name: "loss"}

	task, err := newTestBuilder(t).
		Source(source).
		Sink(stats).
		Sink(loss).
		Routes(
			testRoute("source", "stats").ByEvent(av.EventStats),
			testRoute("source", "loss").ByEvent(av.EventPacketLoss),
		).
		Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	spec := task.Describe()
	if !strings.Contains(specText(spec), "source -> stats [event=stats]") {
		t.Fatalf("spec:\n%s", specText(spec))
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stats.count != 1 || loss.count != 0 {
		t.Fatalf("stats=%d loss=%d", stats.count, loss.count)
	}
}

func TestRuntimeBuilderExplicitLinksOverrideLinearDefault(t *testing.T) {
	packet := av.Packet{StreamID: "audio"}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}
	unused := &runtimeTestStage{name: "unused"}
	sink := &runtimeTestSink{name: "sink"}

	task, err := newTestBuilder(t).
		Source(source).
		Stage(unused).
		Sink(sink).
		Routes(testRoute("source", "sink")).
		Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if unused.count != 0 || sink.count != 1 {
		t.Fatalf("stage=%d sink=%d", unused.count, sink.count)
	}
}

func TestRuntimeBuilderRefusesUnimplementedGraph(t *testing.T) {
	_, err := newTestBuilder(t).
		Input(format.Input{Name: "input"}).
		Decode(testSelectAudio()).
		Mux(format.Output{Name: "output"}).
		Build(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "runtime_graph_unsupported" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want runtime_graph_unsupported wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "decodes: 1") ||
		!strings.Contains(err.Error(), "outputs: 1") ||
		!strings.Contains(err.Error(), "Sink") {
		t.Fatalf("err = %v, want unsupported shape guidance", err)
	}
}

func TestRuntimeBuilderRefusesMixedGraph(t *testing.T) {
	packet := av.Packet{StreamID: "audio"}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}

	_, err := newTestBuilder(t).
		Input(format.Input{Name: "input"}).
		Source(source).
		Build(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "runtime_graph_unsupported" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want runtime_graph_unsupported wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "inputs: 1") ||
		!strings.Contains(err.Error(), "sources: 1") ||
		!strings.Contains(err.Error(), "avoid mixing explicit Source/Stage/Sink") {
		t.Fatalf("err = %v, want mixed graph guidance", err)
	}
}

func TestRuntimeBuilderDescribeValidation(t *testing.T) {
	if _, err := newTestBuilder(t).Input(format.Input{Name: "input"}).Describe(); !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("high-level err = %v, want ErrUnsupportedBuild", err)
	}
	if _, err := newTestBuilder(t).Source(nil).Describe(); !errors.Is(err, ErrNilSource) {
		t.Fatalf("source err = %v, want ErrNilSource", err)
	}

	packet := av.Packet{StreamID: "audio"}
	source := &runtimeTestSource{
		name:    "same",
		message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}
	sink := &runtimeTestSink{name: "same"}
	if _, err := newTestBuilder(t).Source(source).Sink(sink).Describe(); !errors.Is(err, pipeline.ErrNodeExists) {
		t.Fatalf("duplicate err = %v, want ErrNodeExists", err)
	}

	validSource := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}
	validSink := &runtimeTestSink{name: "sink"}
	if _, err := newTestBuilder(t).
		Source(validSource).
		Sink(validSink).
		Routes(testRoute("missing", "sink")).
		Describe(); !errors.Is(err, pipeline.ErrUnknownNode) {
		t.Fatalf("unknown err = %v, want ErrUnknownNode", err)
	}
	if _, err := newTestBuilder(t).
		Source(validSource).
		Sink(validSink).
		Routes(pipeline.Route{
			From:   "source",
			To:     []string{"sink"},
			Policy: pipeline.RoutePolicy("unsupported"),
		}).
		Describe(); !errors.Is(err, pipeline.ErrUnsupportedRoute) {
		t.Fatalf("route err = %v, want ErrUnsupportedRoute", err)
	}
}

func TestRuntimeBuilderExplicitGraphValidation(t *testing.T) {
	_, err := newTestBuilder(t).Source(nil).Build(context.Background())
	if !errors.Is(err, ErrNilSource) {
		t.Fatalf("source err = %v, want ErrNilSource", err)
	}

	_, err = newTestBuilder(t).Stage(&runtimeTestStage{name: "stage"}).Build(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "explicit_graph_source_missing" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("stage err = %v, want explicit_graph_source_missing wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "no source node") ||
		!strings.Contains(err.Error(), "Source(...)") {
		t.Fatalf("stage err = %v, want missing source guidance", err)
	}

	packet := av.Packet{StreamID: "audio"}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}
	_, err = newTestBuilder(t).Source(source).Stage(nil).Build(context.Background())
	if !errors.Is(err, ErrNilStage) {
		t.Fatalf("stage err = %v, want ErrNilStage", err)
	}

	_, err = newTestBuilder(t).Source(source).Sink(nil).Build(context.Background())
	if !errors.Is(err, ErrNilSink) {
		t.Fatalf("sink err = %v, want ErrNilSink", err)
	}

	sink := &runtimeTestSink{name: "sink"}
	_, err = newTestBuilder(t).
		Source(source).
		Sink(sink).
		Routes(testRoute("missing", "sink")).
		Build(context.Background())
	if !errors.Is(err, pipeline.ErrUnknownNode) {
		t.Fatalf("route err = %v, want ErrUnknownNode", err)
	}
}

func TestRuntimeWithCodecDescriptorAdapter(t *testing.T) {
	rt := runtimeValue(t, New(withTestCodecDescriptor(codec.Descriptor{
		ID:    av.CodecAV1,
		Modes: []codec.Mode{codec.ModeDecode},
	})))

	found, err := rt.codecs.Find(av.CodecAV1, codec.ModeDecode)
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 1 {
		t.Fatalf("found = %d, want 1", len(found))
	}
}
