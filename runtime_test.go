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

func TestNewRuntimeDefaults(t *testing.T) {
	runtime := New()
	if runtime.Codecs() == nil || runtime.Formats() == nil || runtime.Filters() == nil || runtime.Pipelines() == nil {
		t.Fatalf("runtime defaults incomplete: %+v", runtime)
	}
	if _, err := runtime.Probe(context.Background(), ProbeRequest{}); !errors.Is(err, format.ErrNotFound) {
		t.Fatalf("probe err = %v, want format.ErrNotFound", err)
	}
	result, err := runtime.Probe(context.Background(), ProbeRequest{Name: "audio.opus"})
	if err != nil {
		t.Fatalf("probe opus: %v", err)
	}
	if result.Format != av.FormatOgg {
		t.Fatalf("format = %s, want ogg", result.Format)
	}
}

func TestRuntimeWithCodecAdapter(t *testing.T) {
	runtime := New(WithCodecAdapter(gopusadapter.Register))

	if _, err := runtime.Codecs().DecoderFactory(av.CodecOpus); err != nil {
		t.Fatalf("decoder factory: %v", err)
	}
}

func TestRuntimeWithFormatAdapter(t *testing.T) {
	runtime := New(WithFormatAdapter(ivfadapter.Register))

	if _, err := runtime.Formats().DemuxerFactory(av.FormatIVF); err != nil {
		t.Fatalf("demuxer factory: %v", err)
	}
	if _, err := runtime.Formats().MuxerFactory(av.FormatIVF); err != nil {
		t.Fatalf("muxer factory: %v", err)
	}
}

func TestRuntimeWithFilterAdapter(t *testing.T) {
	runtime := New(WithFilterAdapter(func(registry *filter.SimpleRegistry) {
		registry.RegisterFactory(filter.Descriptor{Name: filter.FactoryResample}, &transcodeTestFilterFactory{})
	}))

	if _, err := runtime.Filters().Factory(filter.FactoryResample); err != nil {
		t.Fatalf("filter factory: %v", err)
	}
}

func TestRuntimeBuilderEmptyTask(t *testing.T) {
	task, err := New().New().Build(context.Background())
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

	task, err := New().New().
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
	if !strings.Contains(spec.String(), "stage -> left") ||
		!strings.Contains(spec.DOT(), "\"stage\" -> \"right\"") {
		t.Fatalf("spec text:\n%s\ndot:\n%s", spec.String(), spec.DOT())
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

	builder := New(WithBufferPolicy(pipeline.BufferPolicy{Capacity: 2, Drop: pipeline.DropOldest})).New().
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
	if planned.String() != task.Describe().String() {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", planned.String(), task.Describe().String())
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

func TestRuntimeBuilderDescribeBeforeBuild(t *testing.T) {
	packet := av.Packet{StreamID: "audio"}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}
	stage := &runtimeTestStage{name: "stage"}
	sink := &runtimeTestSink{name: "sink"}

	builder := New().New().
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
	if planned.String() != built.String() || planned.Mermaid() != built.Mermaid() {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", planned.String(), built.String())
	}
}

func TestRuntimeBuilderExplicitSourceToSink(t *testing.T) {
	packet := av.Packet{StreamID: "audio"}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}
	sink := &runtimeTestSink{name: "sink"}

	task, err := New().New().
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

func TestRuntimeBuilderExplicitRoutes(t *testing.T) {
	packet := av.Packet{StreamID: "audio"}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}
	audio := &runtimeTestSink{name: "audio"}
	video := &runtimeTestSink{name: "video"}

	task, err := New().New().
		Source(source).
		Sink(audio).
		Sink(video).
		ConnectStream("source", "audio", "audio").
		ConnectStream("source", "video", "video").
		Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	spec := task.Describe()
	if len(spec.Edges) != 2 {
		t.Fatalf("edges=%d, want 2", len(spec.Edges))
	}
	if !strings.Contains(spec.String(), "source -> audio [stream=audio]") ||
		!strings.Contains(spec.Mermaid(), "-- \"stream=video\" -->") {
		t.Fatalf("spec:\n%s\nmermaid:\n%s", spec.String(), spec.Mermaid())
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

	task, err := New().New().
		Source(source).
		Sink(record).
		Sink(preview).
		Sink(stats).
		Connect("source", "record", "preview", "stats").
		Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	spec := task.Describe()
	if len(spec.Edges) != 3 {
		t.Fatalf("edges=%d, want 3", len(spec.Edges))
	}
	if !strings.Contains(spec.String(), "source -> record") ||
		!strings.Contains(spec.String(), "source -> preview") ||
		!strings.Contains(spec.String(), "source -> stats") {
		t.Fatalf("spec:\n%s", spec.String())
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

	task, err := New().New().
		Source(source).
		Sink(record).
		Sink(preview).
		Sink(audio).
		ConnectStream("source", "video", "record", "preview").
		ConnectStream("source", "audio", "audio").
		Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	spec := task.Describe()
	if !strings.Contains(spec.String(), "source -> record [stream=video]") ||
		!strings.Contains(spec.String(), "source -> preview [stream=video]") {
		t.Fatalf("spec:\n%s", spec.String())
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

	task, err := New().New().
		Source(source).
		Sink(record).
		Sink(preview).
		Sink(audio).
		Routes(
			From("source").Stream("video", "record", "preview"),
			From("source").Stream("audio", "audio"),
		).
		Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	spec := task.Describe()
	if !strings.Contains(spec.String(), "source -> record [stream=video]") ||
		!strings.Contains(spec.String(), "source -> preview [stream=video]") {
		t.Fatalf("spec:\n%s", spec.String())
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

	spec, err := New().New().
		Source(source).
		Sink(audio).
		Sink(video).
		ConnectStream("source", "audio", "audio").
		ConnectStream("source", "video", "video").
		Describe()
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Edges) != 2 {
		t.Fatalf("edges=%d, want 2", len(spec.Edges))
	}
	if !strings.Contains(spec.String(), "source -> audio [stream=audio]") ||
		!strings.Contains(spec.Mermaid(), "-- \"stream=video\" -->") {
		t.Fatalf("spec:\n%s\nmermaid:\n%s", spec.String(), spec.Mermaid())
	}
}

func TestRuntimeBuilderExplicitEventRoute(t *testing.T) {
	event := av.Event{Type: av.EventStats}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessageEvent, Event: &event},
	}
	stats := &runtimeTestSink{name: "stats"}
	loss := &runtimeTestSink{name: "loss"}

	task, err := New().New().
		Source(source).
		Sink(stats).
		Sink(loss).
		ConnectEvent("source", av.EventStats, "stats").
		ConnectEvent("source", av.EventPacketLoss, "loss").
		Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	spec := task.Describe()
	if !strings.Contains(spec.String(), "source -> stats [event=stats]") {
		t.Fatalf("spec:\n%s", spec.String())
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

	task, err := New().New().
		Source(source).
		Stage(unused).
		Sink(sink).
		Connect("source", "sink").
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
	_, err := New().New().
		Input(Input{Name: "input"}).
		Decode(SelectAudio()).
		Output(Output{Name: "output"}).
		Build(context.Background())
	if !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want ErrUnsupportedBuild", err)
	}
}

func TestRuntimeBuilderRefusesMixedGraph(t *testing.T) {
	packet := av.Packet{StreamID: "audio"}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}

	_, err := New().New().
		Input(Input{Name: "input"}).
		Source(source).
		Build(context.Background())
	if !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want ErrUnsupportedBuild", err)
	}
}

func TestRuntimeBuilderDescribeValidation(t *testing.T) {
	if _, err := New().New().Input(Input{Name: "input"}).Describe(); !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("high-level err = %v, want ErrUnsupportedBuild", err)
	}
	if _, err := New().New().Source(nil).Describe(); !errors.Is(err, ErrNilSource) {
		t.Fatalf("source err = %v, want ErrNilSource", err)
	}

	packet := av.Packet{StreamID: "audio"}
	source := &runtimeTestSource{
		name:    "same",
		message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}
	sink := &runtimeTestSink{name: "same"}
	if _, err := New().New().Source(source).Sink(sink).Describe(); !errors.Is(err, pipeline.ErrNodeExists) {
		t.Fatalf("duplicate err = %v, want ErrNodeExists", err)
	}

	validSource := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}
	validSink := &runtimeTestSink{name: "sink"}
	if _, err := New().New().
		Source(validSource).
		Sink(validSink).
		Connect("missing", "sink").
		Describe(); !errors.Is(err, pipeline.ErrUnknownNode) {
		t.Fatalf("unknown err = %v, want ErrUnknownNode", err)
	}
	if _, err := New().New().
		Source(validSource).
		Sink(validSink).
		Connection(pipeline.Connection{
			From:   "source",
			To:     []string{"sink"},
			Policy: pipeline.RoutePolicy("unsupported"),
		}).
		Describe(); !errors.Is(err, pipeline.ErrUnsupportedRoute) {
		t.Fatalf("connection err = %v, want ErrUnsupportedRoute", err)
	}
}

func TestRuntimeBuilderExplicitGraphValidation(t *testing.T) {
	_, err := New().New().Source(nil).Build(context.Background())
	if !errors.Is(err, ErrNilSource) {
		t.Fatalf("source err = %v, want ErrNilSource", err)
	}

	_, err = New().New().Stage(&runtimeTestStage{name: "stage"}).Build(context.Background())
	if !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("stage err = %v, want ErrUnsupportedBuild", err)
	}

	packet := av.Packet{StreamID: "audio"}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}
	_, err = New().New().Source(source).Stage(nil).Build(context.Background())
	if !errors.Is(err, ErrNilStage) {
		t.Fatalf("stage err = %v, want ErrNilStage", err)
	}

	_, err = New().New().Source(source).Sink(nil).Build(context.Background())
	if !errors.Is(err, ErrNilSink) {
		t.Fatalf("sink err = %v, want ErrNilSink", err)
	}

	sink := &runtimeTestSink{name: "sink"}
	_, err = New().New().
		Source(source).
		Sink(sink).
		Connect("missing", "sink").
		Build(context.Background())
	if !errors.Is(err, pipeline.ErrUnknownNode) {
		t.Fatalf("connect err = %v, want ErrUnknownNode", err)
	}
}

func TestRuntimeWithCustomCodecRegistry(t *testing.T) {
	registry := codec.NewRegistry(codec.WithDescriptor(codec.Descriptor{
		ID:    av.CodecAV1,
		Modes: []codec.Mode{codec.ModeDecode},
	}))
	runtime := New(WithCodecRegistry(registry))

	found, err := runtime.Codecs().Find(av.CodecAV1, codec.ModeDecode)
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 1 {
		t.Fatalf("found = %d, want 1", len(found))
	}
}
