package goav_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/expert"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/graphrender"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/provider"
	goavruntime "github.com/thesyncim/goav/runtime"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/source"
)

type recipeAPISource struct {
	name string
}

func (s recipeAPISource) Name() string {
	return s.name
}

func (s recipeAPISource) Start(context.Context, pipeline.Emitter) error {
	return nil
}

func (s recipeAPISource) Close() error {
	return nil
}

type recipeAPIStreamProber struct {
	streams []av.Stream
}

func (p recipeAPIStreamProber) Probe(context.Context, format.ProbeRequest) (format.ProbeResult, error) {
	return format.ProbeResult{
		Format:  av.FormatOgg,
		Score:   100,
		Streams: p.streams,
	}, nil
}

type recipeAPIDemuxerFactory struct {
	called *bool
}

func (f recipeAPIDemuxerFactory) NewDemuxer(context.Context, format.ProbeResult) (format.Demuxer, error) {
	if f.called != nil {
		*f.called = true
	}
	return nil, errors.New("demuxer should not open during stream selection preflight")
}

type recipeAPIMuxerFactory struct{}

func (recipeAPIMuxerFactory) NewMuxer(context.Context, av.FormatID) (format.Muxer, error) {
	return nil, errors.New("muxer should not open during explain")
}

type recipeAPIDecoderFactory struct{}

func (recipeAPIDecoderFactory) NewDecoder(context.Context, codec.DecodeConfig) (codec.Decoder, error) {
	return nil, errors.New("decoder should not open during explain")
}

type recipeAPIEncoderFactory struct{}

func (recipeAPIEncoderFactory) NewEncoder(context.Context, codec.EncodeConfig) (codec.Encoder, error) {
	return nil, errors.New("encoder should not open during explain")
}

type recipeAPIFilterFactory struct{}

func (recipeAPIFilterFactory) NewFilter(context.Context, filter.Config) (filter.FrameFilter, error) {
	return nil, errors.New("filter should not open during explain")
}

type recipeAPICustomDestination struct{}

func (recipeAPICustomDestination) Name() string {
	return "custom"
}

func (recipeAPICustomDestination) Contract() provider.Contract {
	return provider.Contract{
		ByteStream: true,
		Formats:    []av.FormatID{av.FormatIVF},
		MIMETypes:  []string{"video/ivf"},
	}
}

func (recipeAPICustomDestination) Open(context.Context, provider.Info) (provider.Writer, error) {
	return recipeAPICustomWriter{}, nil
}

type recipeAPICustomWriter struct{}

func (recipeAPICustomWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func (recipeAPICustomWriter) Close() error {
	return nil
}

func specText(spec pipeline.Spec) string {
	out, err := graphrender.RenderURI(spec, "goav:graph")
	if err != nil {
		return err.Error()
	}
	return out
}

func adapterRequirementByKind(requirements []plan.AdapterRequirement, kind string, name string) (plan.AdapterRequirement, bool) {
	for i := range requirements {
		requirement := requirements[i]
		if requirement.Kind != kind {
			continue
		}
		if name != "" && requirement.Name != name && string(requirement.Format) != name && string(requirement.Codec) != name && requirement.Transform != name {
			continue
		}
		return requirement, true
	}
	return plan.AdapterRequirement{}, false
}

func adapterRequirementByKindAndOwner(requirements []plan.AdapterRequirement, kind string, name string, requiredBy string) (plan.AdapterRequirement, bool) {
	for i := range requirements {
		requirement := requirements[i]
		if requirement.RequiredBy != requiredBy {
			continue
		}
		if requirement.Kind != kind {
			continue
		}
		if name != "" && requirement.Name != name && string(requirement.Format) != name && string(requirement.Codec) != name && requirement.Transform != name {
			continue
		}
		return requirement, true
	}
	return plan.AdapterRequirement{}, false
}

func hasPlanWarning(warnings []plan.Diagnostic, code string) bool {
	for i := range warnings {
		if warnings[i].Code == code {
			return true
		}
	}
	return false
}

func operationKinds(operations []plan.Operation) []plan.OperationKind {
	kinds := make([]plan.OperationKind, 0, len(operations))
	for i := range operations {
		kinds = append(kinds, operations[i].Kind)
	}
	return kinds
}

func operationSpecKinds(operations any) []plan.OperationKind {
	return goav.OperationSpecKindsForTest(operations)
}

func equalOperationKinds(a []plan.OperationKind, b []plan.OperationKind) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func transformOperationsForTest(operations any) []goav.TransformViewForTest {
	return goav.TransformOperationsForTest(operations)
}

func equalStrings(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestMediaShapePublicContract(t *testing.T) {
	packet := shape.Packet(
		av.MediaAudio,
		av.CodecOpus,
		shape.Stream("audio"),
		shape.Audio(48_000, codec.Stereo, av.SampleFormatS16),
		shape.Realtime(true),
	)
	if packet.Domain != shape.DomainPacket ||
		packet.MediaKind != av.MediaAudio ||
		packet.Codec != av.CodecOpus ||
		packet.SampleRate != 48_000 ||
		packet.Channels != codec.Stereo ||
		!packet.Realtime {
		t.Fatalf("packet shape=%+v, want opus audio packet shape", packet)
	}
	if !packet.CompatibleWith(shape.New(shape.Domain(shape.DomainPacket), shape.Media(av.MediaAudio))) {
		t.Fatalf("packet shape should satisfy packet audio contract: %s", packet)
	}
	if (shape.Set{shape.Frame(av.MediaAudio)}).Accepts(packet) {
		t.Fatalf("frame shape set accepted packet shape: %s", packet)
	}

	var resizeContract shape.Contract = goav.Resize(1280, 720)
	if !resizeContract.InputShapes().Accepts(shape.Frame(av.MediaVideo)) {
		t.Fatalf("resize input shapes=%+v, want video frame", resizeContract.InputShapes())
	}
	resized := resizeContract.OutputShapes(shape.Frame(
		av.MediaVideo,
		shape.Video(1920, 1080, av.PixelFormatYUV420P),
	))[0]
	if resized.Domain != shape.DomainFrame ||
		resized.MediaKind != av.MediaVideo ||
		resized.Width != 1280 ||
		resized.Height != 720 ||
		resized.PixelFormat != av.PixelFormatYUV420P {
		t.Fatalf("resized shape=%+v, want 1280x720 video frame", resized)
	}

	copyContract := goav.CopyOperationContractForTest()
	if !copyContract.InputShapes().Accepts(packet) {
		t.Fatalf("copy input shapes=%+v, want packet domain", copyContract.InputShapes())
	}
	copied := copyContract.OutputShapes(packet)[0]
	if copied != packet {
		t.Fatalf("copied shape=%+v, want preserved packet %+v", copied, packet)
	}

	operationContract := goav.TransformOperationContractForTest(goav.Resample(16_000, codec.Mono))
	resampled := operationContract.OutputShapes(shape.Frame(
		av.MediaAudio,
		shape.Audio(48_000, codec.Stereo, av.SampleFormatS16),
	))[0]
	if resampled.Domain != shape.DomainFrame ||
		resampled.MediaKind != av.MediaAudio ||
		resampled.SampleRate != 16_000 ||
		resampled.Channels != codec.Mono ||
		resampled.SampleFormat != av.SampleFormatS16 {
		t.Fatalf("resampled shape=%+v, want 16k mono audio frame", resampled)
	}
}

func TestSourceInputIntentUsesCustomProtocol(t *testing.T) {
	input := goav.Source("generated",
		shape.Packet(av.MediaAudio, av.CodecOpus,
			shape.Audio(48_000, codec.Stereo, av.SampleFormatS16),
		),
		func(context.Context, source.Push) error {
			return nil
		},
	)
	job := goav.From(input).
		Audio().
		Copy().
		To(goav.Sink(component.SinkFunc("packets", func(context.Context, component.Message) error {
			return nil
		})))

	intent := goav.JobPlanForTest(job)
	if len(intent.Inputs) != 1 ||
		intent.Inputs[0].Name != "generated" ||
		intent.Inputs[0].Protocol != av.ProtocolCustom ||
		intent.Inputs[0].Codec.ID != av.CodecOpus ||
		intent.Inputs[0].Codec.Parameters.SampleRate != 48_000 ||
		intent.Inputs[0].Codec.Parameters.Channels != codec.Stereo {
		t.Fatalf("intent inputs: %+v", intent.Inputs)
	}
}

func TestFlowReportsShapeContractAndTaps(t *testing.T) {
	flow := goav.Flow("voice").Audio().
		Decode().
		Resample(16_000, codec.Mono).
		Tap(goav.FrameTap("voice.frames"))

	inputs := flow.InputShapes()
	if len(inputs) != 1 || !inputs.Accepts(shape.Packet(av.MediaAudio, av.CodecOpus)) {
		t.Fatalf("flow input shapes=%+v, want audio packet", inputs)
	}
	outputs := flow.OutputShapes(shape.Packet(
		av.MediaAudio,
		av.CodecOpus,
		shape.Audio(48_000, codec.Stereo, av.SampleFormatS16),
	))
	if len(outputs) != 1 ||
		outputs[0].Domain != shape.DomainFrame ||
		outputs[0].MediaKind != av.MediaAudio ||
		outputs[0].SampleRate != 16_000 ||
		outputs[0].Channels != codec.Mono ||
		outputs[0].SampleFormat != av.SampleFormatS16 {
		t.Fatalf("flow output shapes=%+v, want 16k mono audio frame", outputs)
	}
	taps := flow.Taps()
	if len(taps) != 1 || taps[0].Name() != "voice.frames" || taps[0].Domain() != shape.DomainFrame {
		t.Fatalf("flow taps=%+v, want frame tap voice.frames", taps)
	}
}

func branchByName(branches []plan.Branch, name string) (plan.Branch, bool) {
	for i := range branches {
		if branches[i].Name == name {
			return branches[i], true
		}
	}
	return plan.Branch{}, false
}

func tapReportByName(taps []plan.Tap, name string) (plan.Tap, bool) {
	for i := range taps {
		if taps[i].Name == name {
			return taps[i], true
		}
	}
	return plan.Tap{}, false
}

func operationReportByKind(operations []plan.Operation, kind plan.OperationKind) (plan.Operation, bool) {
	for i := range operations {
		if operations[i].Kind == kind {
			return operations[i], true
		}
	}
	return plan.Operation{}, false
}

func countOperationReports(operations []plan.Operation, kind plan.OperationKind, shared bool) int {
	count := 0
	for i := range operations {
		if operations[i].Kind == kind && operations[i].Shared == shared {
			count++
		}
	}
	return count
}

func recordJob(input goav.InputSpec, outputs ...goav.Destination) *goav.Job {
	return goav.From(input).Copy().To(outputs...)
}

func decodeJob(input goav.InputSpec, output goav.Destination) *goav.Job {
	return goav.From(input).Stream().Decode().To(output)
}

type testBranchJob struct {
	input    goav.InputSpec
	runtime  *goav.Runtime
	branches []testTranscodeBranch
}

type testBranchStream interface {
	Branches(...goav.BranchSpec) *goav.Job
}

type testTranscodeBranch struct {
	name         string
	media        av.MediaType
	transforms   []testTransform
	encode       codec.CodecSpec
	destinations []goav.Destination
}

type testTransform struct {
	kind       string
	width      int
	height     int
	sampleRate int
	channels   int
}

type testTranscodeBranchBuilder struct {
	job   *testBranchJob
	index int
}

func branchJob(input goav.InputSpec) *testBranchJob {
	return &testBranchJob{input: input}
}

func (j *testBranchJob) UseRuntime(runtime *goav.Runtime) *testBranchJob {
	j.runtime = runtime
	return j
}

func (j *testBranchJob) Audio(name string) *testTranscodeBranchBuilder {
	return j.stream(name, av.MediaAudio)
}

func (j *testBranchJob) Video(name string) *testTranscodeBranchBuilder {
	return j.stream(name, av.MediaVideo)
}

func (j *testBranchJob) stream(name string, media av.MediaType) *testTranscodeBranchBuilder {
	j.branches = append(j.branches, testTranscodeBranch{name: name, media: media})
	return &testTranscodeBranchBuilder{job: j, index: len(j.branches) - 1}
}

func (j *testBranchJob) materialize() *goav.Job {
	job := goav.From(j.input)
	if j.runtime != nil {
		job.UseRuntime(j.runtime)
	}
	for i := range j.branches {
		branch := j.branches[i]
		var stream testBranchStream
		switch branch.media {
		case av.MediaAudio:
			stream = job.Audio().Decode().Tap(goav.FrameTap("audio.decoded"))
		case av.MediaVideo:
			stream = job.Video().Decode().Tap(goav.FrameTap("video.decoded"))
		default:
			stream = job.Stream().Decode().Tap(goav.FrameTap("stream.decoded"))
		}
		builder := goav.Branch(branch.name)
		for _, transform := range branch.transforms {
			switch transform.kind {
			case "resize":
				builder = builder.Resize(transform.width, transform.height)
			case "resample":
				builder = builder.Resample(transform.sampleRate, transform.channels)
			}
		}
		if branch.encode.ID != "" {
			builder = builder.Encode(branch.encode)
		}
		destinations := append([]goav.Destination(nil), branch.destinations...)
		job = stream.Branches(builder.To(destinations...))
	}
	return job
}

func (j *testBranchJob) Explain(ctx context.Context) (plan.Report, error) {
	return j.materialize().Explain(ctx)
}

func (j *testBranchJob) Describe() (pipeline.Spec, error) {
	return j.materialize().Describe()
}

func (j *testBranchJob) Build(ctx context.Context) (goav.Task, error) {
	return j.materialize().Build(ctx)
}

func (j *testBranchJob) Run(ctx context.Context) error {
	return j.materialize().Run(ctx)
}

func (b *testTranscodeBranchBuilder) Resize(width int, height int) *testTranscodeBranchBuilder {
	b.current().transforms = append(b.current().transforms, testTransform{kind: "resize", width: width, height: height})
	return b
}

func (b *testTranscodeBranchBuilder) Resample(sampleRate int, channels int) *testTranscodeBranchBuilder {
	b.current().transforms = append(b.current().transforms, testTransform{kind: "resample", sampleRate: sampleRate, channels: channels})
	return b
}

func (b *testTranscodeBranchBuilder) Encode(spec codec.CodecSpec) *testTranscodeBranchBuilder {
	b.current().encode = spec
	return b
}

func (b *testTranscodeBranchBuilder) To(destinations ...goav.Destination) *testBranchJob {
	b.current().destinations = append([]goav.Destination(nil), destinations...)
	return b.job
}

func (b *testTranscodeBranchBuilder) current() *testTranscodeBranch {
	return &b.job.branches[b.index]
}

func TestRuntimeConcreteKeepsLegacyBuilderOutOfFrontDoor(t *testing.T) {
	runtime, err := goav.New()
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	if runtime == nil {
		t.Fatal("New() returned nil runtime")
	}
	runtimeType := reflect.TypeOf(runtime)
	if _, ok := runtimeType.MethodByName("Probe"); !ok {
		t.Fatal("Runtime should expose Probe")
	}
	if _, ok := runtimeType.MethodByName("New"); ok {
		t.Fatal("Runtime exposes legacy New builder; use expert.Graph(runtime) for expert graph wiring")
	}
	if _, ok := runtimeType.MethodByName("Graph"); ok {
		t.Fatal("Runtime exposes Graph; use expert.Graph(runtime) for expert graph wiring")
	}
	if reflect.TypeOf(expert.Graph).Kind() != reflect.Func {
		t.Fatal("expert.Graph should expose the expert graph entry point")
	}
}

func TestExpertGraphRequiresRuntime(t *testing.T) {
	graph := expert.Graph(nil)
	if _, err := graph.Describe(); !errors.Is(err, expert.ErrRuntimeRequired) {
		t.Fatalf("Describe err = %v, want ErrRuntimeRequired", err)
	}
	if _, err := graph.Build(context.Background()); !errors.Is(err, expert.ErrRuntimeRequired) {
		t.Fatalf("Build err = %v, want ErrRuntimeRequired", err)
	}
}

func TestRecipesExposeStructuredExplain(t *testing.T) {
	jobType := reflect.TypeOf((*goav.Job)(nil))
	if _, ok := jobType.MethodByName("Explain"); !ok {
		t.Fatal("Job should expose Explain for structured workflow reports")
	}
	reportType := reflect.TypeOf(plan.Report{})
	for _, method := range []string{"Text", "Mermaid", "DOT", "Render"} {
		if _, ok := reportType.MethodByName(method); ok {
			t.Fatalf("plan.Report exposes renderer method %s; keep rendering outside core", method)
		}
	}
}

func TestZeroJobRejectsPublicConstruction(t *testing.T) {
	typ := reflect.TypeOf(goav.Job{})
	for i := 0; i < typ.NumField(); i++ {
		if typ.Field(i).IsExported() {
			t.Fatalf("Job field %s is exported; use goav.From instead", typ.Field(i).Name)
		}
	}

	var zero goav.Job
	_, err := zero.Copy().
		To(goav.Write("out.ogg", io.Discard)).
		Describe()
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != errcode.JobInvalid {
		t.Fatalf("err = %v, want job_invalid with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "empty job") ||
		!strings.Contains(err.Error(), "goav.From(input)") {
		t.Fatalf("err = %v, want From constructor guidance", err)
	}

	_, err = goav.From().
		Copy().
		To(goav.Write("out.ogg", io.Discard)).
		Describe()
	if !errors.As(err, &buildErr) || buildErr.Code != errcode.Code("input_missing") {
		t.Fatalf("From() err = %v, want input_missing", err)
	}
}

func TestExplainReturnsPartialReportForMissingMuxer(t *testing.T) {
	report, err := recordJob(
		goav.FileInput("input.ivf", bytes.NewReader(tinyIVF())),
		goav.Write("recording.mp4", io.Discard),
	).UseRuntime(mustRuntime()).Explain(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_muxer_missing" {
		t.Fatalf("err = %v, want destination_muxer_missing", err)
	}
	if buildErr.Operation != "open destination" {
		t.Fatalf("operation = %q, want open destination", buildErr.Operation)
	}
	requirement, ok := adapterRequirementByKind(report.RequiredAdapters, "muxer", string(av.FormatMP4))
	if !ok || requirement.Status != "missing" || requirement.Format != av.FormatMP4 {
		t.Fatalf("requirements=%+v, want missing mp4 muxer", report.RequiredAdapters)
	}
	if len(report.Graph.Nodes) == 0 || report.Summary == "" {
		t.Fatalf("partial report not populated: %+v", report)
	}
	if len(report.Warnings) != 1 || report.Warnings[0].Code != "destination_muxer_missing" {
		t.Fatalf("warnings=%+v", report.Warnings)
	}
	if len(report.Missing) != 1 || report.Missing[0].Kind != "muxer" || report.Missing[0].Status != "missing" {
		t.Fatalf("missing=%+v", report.Missing)
	}
}

func TestExplainReturnsPartialReportForMissingTransformAdapter(t *testing.T) {
	report, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(bundle.MustNewCodecs()).
		Audio().
		Decode().
		Resample(16_000, codec.Mono).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))).
		Explain(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "transform_adapter_missing" {
		t.Fatalf("err = %v, want transform_adapter_missing", err)
	}
	requirement, ok := adapterRequirementByKind(report.RequiredAdapters, "filter", "resample")
	if !ok || requirement.Status != "missing" || requirement.Transform != "resample" {
		t.Fatalf("requirements=%+v, want missing resample filter", report.RequiredAdapters)
	}
	if !hasPlanWarning(report.Warnings, "transform_adapter_missing") {
		t.Fatalf("warnings=%+v", report.Warnings)
	}
	if len(report.Missing) != 1 || report.Missing[0].Name != "resample" {
		t.Fatalf("missing=%+v", report.Missing)
	}
}

func TestTranscodeExplainReportsGenericMediaPlanBranches(t *testing.T) {
	rt := mustRuntime(
		goavruntime.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{
				{Index: 0, ID: "audio", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}},
				{Index: 1, ID: "video", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo}},
			}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
			registry.RegisterMuxer(av.FormatOgg, recipeAPIMuxerFactory{})
		}),
		goavruntime.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, recipeAPIDecoderFactory{})
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, recipeAPIEncoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP9, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
		}),
		goavruntime.WithFilterAdapter(func(registry *filter.SimpleRegistry) {
			registry.RegisterFactory(filter.Descriptor{Name: filter.FactoryResize, Input: av.MediaVideo, Output: av.MediaVideo}, recipeAPIFilterFactory{})
			registry.RegisterFactory(filter.Descriptor{Name: filter.FactoryResample, Input: av.MediaAudio, Output: av.MediaAudio}, recipeAPIFilterFactory{})
		}),
	)

	web := goav.Mux("web", goav.Write("web.ogg", io.Discard))
	report, err := branchJob(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Video("v").Resize(1280, 720).Encode(codec.VP9(codec.Bitrate(2_000_000))).To(web).
		Audio("a").Resample(48_000, codec.Stereo).Encode(codec.Opus(codec.Bitrate(96_000))).To(web).
		Explain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Branches) != 2 {
		t.Fatalf("branches=%+v", report.Branches)
	}
	if len(report.Destinations) != 1 ||
		report.Destinations[0].Name != "web.ogg" ||
		report.Destinations[0].Format != av.FormatOgg ||
		!equalStrings(report.Destinations[0].Branches, []string{"v", "a"}) {
		t.Fatalf("destinations=%+v", report.Destinations)
	}
	want := []plan.OperationKind{plan.OpDemux, plan.OpSelect, plan.OpDecode, plan.OpTap, plan.OpTransform, plan.OpEncode}
	for _, name := range []string{"v", "a"} {
		branch, ok := branchByName(report.Branches, name)
		if !ok {
			t.Fatalf("missing branch %q in %+v", name, report.Branches)
		}
		if !equalOperationKinds(operationKinds(branch.Operations), want) {
			t.Fatalf("%s operations=%+v", name, branch.Operations)
		}
		if len(branch.Destinations) != 1 || branch.Destinations[0] != "web.ogg" {
			t.Fatalf("%s destinations=%+v", name, branch.Destinations)
		}
		for _, operation := range branch.Operations {
			if operation.Kind == plan.OperationKind("trans"+"code") {
				t.Fatalf("branch %s has special transcode operation: %+v", name, branch.Operations)
			}
		}
	}
	if len(report.Decisions) < 4 {
		t.Fatalf("decisions=%+v, want decode and encode decisions per branch", report.Decisions)
	}
}

func TestExplainReportsBranchShapeFromProbedInput(t *testing.T) {
	rt := mustRuntime(
		goavruntime.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{{
				Index: 0,
				ID:    "audio",
				Type:  av.MediaAudio,
				Codec: av.CodecParameters{
					ID:           av.CodecOpus,
					Type:         av.MediaAudio,
					SampleRate:   48000,
					Channels:     codec.Stereo,
					SampleFormat: av.SampleFormatS16,
				},
			}}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
		}),
		goavruntime.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, recipeAPIDecoderFactory{})
		}),
	)

	report, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Audio().
		Decode().
		Tap(goav.FrameTap("audio.decoded")).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))).
		Explain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	branch, ok := branchByName(report.Branches, "audio")
	if !ok {
		t.Fatalf("branches=%+v, want audio", report.Branches)
	}
	if branch.Shape.Domain != shape.DomainPacket ||
		branch.Shape.MediaKind != av.MediaAudio ||
		branch.Shape.StreamID != "audio" ||
		branch.Shape.Codec != av.CodecOpus ||
		branch.Shape.SampleRate != 48000 ||
		branch.Shape.Channels != codec.Stereo ||
		branch.Shape.SampleFormat != av.SampleFormatS16 {
		t.Fatalf("branch shape=%+v, want probed audio packet shape", branch.Shape)
	}
	tap, ok := tapReportByName(report.Taps, "audio.decoded")
	if !ok {
		t.Fatalf("taps=%+v, want audio.decoded", report.Taps)
	}
	if tap.Shape.Domain != shape.DomainFrame ||
		tap.Shape.MediaKind != av.MediaAudio ||
		tap.Shape.StreamID != "audio" ||
		tap.Shape.Codec != av.CodecOpus ||
		tap.Shape.SampleRate != 48000 ||
		tap.Shape.Channels != codec.Stereo ||
		tap.Shape.SampleFormat != av.SampleFormatS16 {
		t.Fatalf("tap shape=%+v, want decoded audio frame shape", tap.Shape)
	}
}

func TestExplainReportsOperationShapeThroughResizeAndEncode(t *testing.T) {
	rt := mustRuntime(
		goavruntime.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{{
				Index: 0,
				ID:    "video",
				Type:  av.MediaVideo,
				Codec: av.CodecParameters{
					ID:          av.CodecVP8,
					Type:        av.MediaVideo,
					Width:       1920,
					Height:      1080,
					PixelFormat: av.PixelFormatYUV420P,
				},
			}}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
			registry.RegisterMuxer(av.FormatOgg, recipeAPIMuxerFactory{})
		}),
		goavruntime.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP9, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
		}),
		goavruntime.WithFilterAdapter(func(registry *filter.SimpleRegistry) {
			registry.RegisterFactory(filter.Descriptor{Name: filter.FactoryResize, Input: av.MediaVideo, Output: av.MediaVideo}, recipeAPIFilterFactory{})
		}),
	)

	web := goav.Write("web.ogg", io.Discard)
	report, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Video().
		Decode().
		Branches(
			goav.Branch("preview").
				Resize(1280, 720).
				Encode(codec.VP9(codec.Bitrate(2_000_000), codec.FPS(30))).
				Tap(goav.PacketTap("video.encoded")).
				To(web),
		).
		Explain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	branch, ok := branchByName(report.Branches, "preview")
	if !ok {
		t.Fatalf("branches=%+v, want preview", report.Branches)
	}
	if branch.Shape.Domain != shape.DomainPacket ||
		branch.Shape.MediaKind != av.MediaVideo ||
		branch.Shape.Codec != av.CodecVP8 ||
		branch.Shape.Width != 1920 ||
		branch.Shape.Height != 1080 ||
		branch.Shape.PixelFormat != av.PixelFormatYUV420P {
		t.Fatalf("branch shape=%+v, want probed VP8 1920x1080 packet shape", branch.Shape)
	}
	resize, ok := operationReportByKind(branch.Operations, plan.OpTransform)
	if !ok {
		t.Fatalf("operations=%+v, want resize operation", branch.Operations)
	}
	if resize.Shape.Domain != shape.DomainFrame ||
		resize.Shape.MediaKind != av.MediaVideo ||
		resize.Shape.Codec != av.CodecVP8 ||
		resize.Shape.Width != 1280 ||
		resize.Shape.Height != 720 ||
		resize.Shape.PixelFormat != av.PixelFormatYUV420P {
		t.Fatalf("resize shape=%+v, want frame VP8 1280x720 shape", resize.Shape)
	}
	encode, ok := operationReportByKind(branch.Operations, plan.OpEncode)
	if !ok {
		t.Fatalf("operations=%+v, want encode operation", branch.Operations)
	}
	if encode.Shape.Domain != shape.DomainPacket ||
		encode.Shape.MediaKind != av.MediaVideo ||
		encode.Shape.StreamID != "preview" ||
		encode.Shape.Codec != av.CodecVP9 ||
		encode.Shape.Width != 1280 ||
		encode.Shape.Height != 720 ||
		encode.Shape.PixelFormat != av.PixelFormatYUV420P {
		t.Fatalf("encode shape=%+v, want packet VP9 1280x720 shape", encode.Shape)
	}
	tap, ok := tapReportByName(report.Taps, "video.encoded")
	if !ok {
		t.Fatalf("taps=%+v, want video.encoded", report.Taps)
	}
	if tap.Shape != encode.Shape {
		t.Fatalf("tap shape=%+v, want encode shape %+v", tap.Shape, encode.Shape)
	}
}

func TestShapeAnnotationCannotBreakOperationContract(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ivf", strings.NewReader(""))).
		UseRuntime(bundle.MustNew()).
		Video().
		Decode().
		Shape(shape.New(shape.Domain(shape.DomainPacket), shape.Media(av.MediaVideo))).
		Encode(codec.VP9(codec.Bitrate(2_000_000))).
		To(goav.Write("preview.ivf", io.Discard)).
		Describe()
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "operation_shape_mismatch" {
		t.Fatalf("err = %v, want operation_shape_mismatch", err)
	}
	for _, want := range []string{
		"vp9 cannot consume the current media shape",
		"expected_shape=domain=frame media=video",
		"actual_shape=domain=packet media=video",
		"keep .Shape(...) annotations in the frame domain before encoders",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want %q", err, want)
		}
	}
}

func TestExplainRequirementsFollowOrderedBranchOperations(t *testing.T) {
	rt := mustRuntime(
		goavruntime.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{
				{Index: 0, ID: "video", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo}},
			}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
			registry.RegisterMuxer(av.FormatOgg, recipeAPIMuxerFactory{})
		}),
		goavruntime.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP9, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
		}),
		goavruntime.WithFilterAdapter(func(registry *filter.SimpleRegistry) {
			registry.RegisterFactory(filter.Descriptor{Name: filter.FactoryResize, Input: av.MediaVideo, Output: av.MediaVideo}, recipeAPIFilterFactory{})
		}),
	)
	meter := component.FrameFunc("meter", func(ctx context.Context, frame *av.Frame, emit component.Emit) error {
		return emit.Frame(frame)
	})

	web := goav.Write("web.ogg", io.Discard)
	report, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Video().
		Decode().
		Branches(
			goav.Branch("v360").
				Resize(640, 360).
				Do(meter).
				Encode(codec.VP9(codec.Bitrate(600_000))).
				To(web),
		).
		Explain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	branch, ok := branchByName(report.Branches, "v360")
	if !ok {
		t.Fatalf("branches=%+v, want v360", report.Branches)
	}
	wantOps := []plan.OperationKind{plan.OpDemux, plan.OpSelect, plan.OpDecode, plan.OpTransform, plan.OpStage, plan.OpEncode}
	if !equalOperationKinds(operationKinds(branch.Operations), wantOps) {
		t.Fatalf("operations=%+v, want %+v", branch.Operations, wantOps)
	}
	for _, want := range []struct {
		kind       string
		name       string
		requiredBy string
		input      av.MediaType
		output     av.MediaType
	}{
		{kind: "demuxer", name: string(av.FormatOgg), requiredBy: "input.ogg"},
		{kind: "decoder", name: string(av.CodecVP8), requiredBy: "v360"},
		{kind: "filter", name: filter.FactoryResize, requiredBy: "v360", input: av.MediaVideo, output: av.MediaVideo},
		{kind: "encoder", name: string(av.CodecVP9), requiredBy: "v360"},
		{kind: "muxer", name: string(av.FormatOgg), requiredBy: "web.ogg"},
	} {
		requirement, ok := adapterRequirementByKindAndOwner(report.RequiredAdapters, want.kind, want.name, want.requiredBy)
		if !ok || requirement.Status != "available" {
			t.Fatalf("requirements=%+v, want available %s %s required by %s", report.RequiredAdapters, want.kind, want.name, want.requiredBy)
		}
		if want.kind == "filter" &&
			(requirement.Input != want.input ||
				requirement.Output != want.output) {
			t.Fatalf("filter requirement = %+v, want input/output capability details", requirement)
		}
	}
	if len(report.Warnings) != 0 {
		t.Fatalf("warnings=%+v", report.Warnings)
	}
}

func TestExplainReportsFilterDescriptorCapabilities(t *testing.T) {
	rt := mustRuntime(
		goavruntime.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{
				{Index: 0, ID: "audio", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}},
			}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
		}),
		goavruntime.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, recipeAPIDecoderFactory{})
		}),
		goavruntime.WithFilterAdapter(func(registry *filter.SimpleRegistry) {
			registry.RegisterFactory(filter.Descriptor{
				Name:          filter.FactoryResample,
				Input:         av.MediaAudio,
				Output:        av.MediaAudio,
				SampleFormats: []string{av.SampleFormatS16},
				Realtime:      true,
				Stateless:     true,
				Metadata:      av.Metadata{"sample_format": av.SampleFormatS16},
			}, recipeAPIFilterFactory{})
		}),
	)

	report, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Audio().
		Decode().
		Resample(16_000, codec.Mono).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))).
		Explain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	requirement, ok := adapterRequirementByKindAndOwner(report.RequiredAdapters, "filter", filter.FactoryResample, "audio")
	if !ok {
		t.Fatalf("requirements=%+v, want resample filter requirement", report.RequiredAdapters)
	}
	if requirement.Status != "available" ||
		requirement.Input != av.MediaAudio ||
		requirement.Output != av.MediaAudio ||
		len(requirement.SampleFormats) != 1 ||
		requirement.SampleFormats[0] != av.SampleFormatS16 ||
		!requirement.Realtime ||
		!requirement.Stateless ||
		requirement.Metadata["sample_format"] != av.SampleFormatS16 {
		t.Fatalf("filter requirement = %+v", requirement)
	}
	requirement.SampleFormats[0] = "mutated"
	requirement.Metadata["sample_format"] = "mutated"
	report, err = goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Audio().
		Decode().
		Resample(16_000, codec.Mono).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))).
		Explain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	requirement, ok = adapterRequirementByKindAndOwner(report.RequiredAdapters, "filter", filter.FactoryResample, "audio")
	if !ok ||
		len(requirement.SampleFormats) != 1 ||
		requirement.SampleFormats[0] != av.SampleFormatS16 ||
		requirement.Metadata["sample_format"] != av.SampleFormatS16 {
		t.Fatalf("filter requirement capabilities were not cloned: %+v", requirement)
	}
}

func TestExplainReportsIncompatibleFilterDescriptor(t *testing.T) {
	rt := mustRuntime(
		goavruntime.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{
				{Index: 0, ID: "audio", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}},
			}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
		}),
		goavruntime.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, recipeAPIDecoderFactory{})
		}),
		goavruntime.WithFilterAdapter(func(registry *filter.SimpleRegistry) {
			registry.RegisterFactory(filter.Descriptor{
				Name:   filter.FactoryResample,
				Input:  av.MediaVideo,
				Output: av.MediaVideo,
			}, recipeAPIFilterFactory{})
		}),
	)

	report, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Audio().
		Decode().
		Resample(16_000, codec.Mono).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))).
		Explain(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "transform_adapter_incompatible" {
		t.Fatalf("err = %v, want transform_adapter_incompatible", err)
	}
	requirement, ok := adapterRequirementByKindAndOwner(report.RequiredAdapters, "filter", filter.FactoryResample, "audio")
	if !ok {
		t.Fatalf("requirements=%+v, want resample filter requirement", report.RequiredAdapters)
	}
	if requirement.Status != "incompatible" ||
		requirement.Input != av.MediaVideo ||
		requirement.Output != av.MediaVideo {
		t.Fatalf("filter requirement = %+v", requirement)
	}
	if !hasPlanWarning(report.Warnings, "transform_adapter_incompatible") {
		t.Fatalf("warnings=%+v, want incompatible transform warning", report.Warnings)
	}
	if len(report.Missing) != 1 ||
		report.Missing[0].Kind != "filter" ||
		report.Missing[0].Name != filter.FactoryResample ||
		report.Missing[0].Status != "incompatible" {
		t.Fatalf("missing=%+v, want incompatible filter requirement", report.Missing)
	}
}

func TestExplainReportsIncompatibleEncodeDescriptor(t *testing.T) {
	custom := av.CodecID("x_audio")
	rt := mustRuntime(goavruntime.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
		registry.RegisterEncoder(codec.Descriptor{
			ID:   custom,
			Type: av.MediaVideo,
			Capabilities: codec.Capabilities{
				PixelFormats: []string{av.PixelFormatI420},
			},
		}, recipeAPIEncoderFactory{})
	}))

	report, err := goav.From(goav.FileInput("input.raw", strings.NewReader(""))).
		UseRuntime(rt).
		Audio().
		Decode().
		Encode(codec.Codec(custom, av.MediaAudio)).
		To(goav.Sink(component.SinkFunc("packets", func(context.Context, component.Message) error {
			return nil
		}))).
		Explain(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_adapter_incompatible" {
		t.Fatalf("err = %v, want encode_adapter_incompatible", err)
	}
	requirement, ok := adapterRequirementByKindAndOwner(report.RequiredAdapters, "encoder", string(custom), "audio")
	if !ok {
		t.Fatalf("requirements=%+v, want incompatible encoder requirement", report.RequiredAdapters)
	}
	if requirement.Status != "incompatible" ||
		len(requirement.Media) != 1 ||
		requirement.Media[0] != av.MediaVideo ||
		len(requirement.PixelFormats) != 1 ||
		requirement.PixelFormats[0] != av.PixelFormatI420 {
		t.Fatalf("encoder requirement = %+v", requirement)
	}
	if !hasPlanWarning(report.Warnings, "encode_adapter_incompatible") {
		t.Fatalf("warnings=%+v, want incompatible encoder warning", report.Warnings)
	}
	if len(report.Missing) != 1 ||
		report.Missing[0].Kind != "encoder" ||
		report.Missing[0].Name != string(custom) ||
		report.Missing[0].Status != "incompatible" {
		t.Fatalf("missing=%+v, want incompatible encoder requirement", report.Missing)
	}
}

func TestExplainReportsIncompatibleDecodeDescriptor(t *testing.T) {
	custom := av.CodecID("x_audio")
	rt := mustRuntime(
		goavruntime.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{{
				Index: 0,
				ID:    "audio",
				Type:  av.MediaAudio,
				Codec: av.CodecParameters{
					ID:           custom,
					Type:         av.MediaAudio,
					SampleFormat: av.SampleFormatF32,
				},
			}}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
		}),
		goavruntime.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{
				ID:   custom,
				Type: av.MediaAudio,
				Capabilities: codec.Capabilities{
					SampleFormats: []string{av.SampleFormatS16},
				},
			}, recipeAPIDecoderFactory{})
		}),
	)

	report, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Audio().
		Decode().
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))).
		Explain(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "decode_adapter_incompatible" {
		t.Fatalf("err = %v, want decode_adapter_incompatible", err)
	}
	requirement, ok := adapterRequirementByKindAndOwner(report.RequiredAdapters, "decoder", string(custom), "audio")
	if !ok {
		t.Fatalf("requirements=%+v, want incompatible decoder requirement", report.RequiredAdapters)
	}
	if requirement.Status != "incompatible" ||
		len(requirement.Media) != 1 ||
		requirement.Media[0] != av.MediaAudio ||
		len(requirement.SampleFormats) != 1 ||
		requirement.SampleFormats[0] != av.SampleFormatS16 {
		t.Fatalf("decoder requirement = %+v", requirement)
	}
	if !hasPlanWarning(report.Warnings, "decode_adapter_incompatible") {
		t.Fatalf("warnings=%+v, want incompatible decoder warning", report.Warnings)
	}
	if len(report.Missing) != 1 ||
		report.Missing[0].Kind != "decoder" ||
		report.Missing[0].Name != string(custom) ||
		report.Missing[0].Status != "incompatible" {
		t.Fatalf("missing=%+v, want incompatible decoder requirement", report.Missing)
	}
}

func TestBuildRejectsIncompatibleIVFMuxGroupBeforeOpeningMuxer(t *testing.T) {
	rt := mustRuntime(
		goavruntime.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{
				{Index: 0, ID: "video", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo}},
			}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
			registry.RegisterMuxer(av.FormatIVF, recipeAPIMuxerFactory{})
		}),
		goavruntime.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP9, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
		}),
	)

	web := goav.Mux("web", goav.Write("web.ivf", io.Discard, goav.Format(av.FormatIVF)))
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Video().
		Decode().
		Branches(
			goav.Branch("v8").Encode(codec.VP8(codec.Bitrate(600_000))).To(web),
			goav.Branch("v9").Encode(codec.VP9(codec.Bitrate(900_000))).To(web),
		).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_mux_incompatible" {
		t.Fatalf("err = %v, want destination_mux_incompatible", err)
	}
	for _, want := range []string{"destination=web", "format=ivf", "branch=v8 codec=vp8 media=video", "branch=v9 codec=vp9 media=video"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want detail %q", err, want)
		}
	}
	if !strings.Contains(err.Error(), "route exactly one VP8, VP9, or AV1 video branch") {
		t.Fatalf("err = %v, want IVF guidance", err)
	}
}

func TestBuildRejectsDescriptorBackedMuxIncompatibility(t *testing.T) {
	rt := mustRuntime(
		goavruntime.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{
				{Index: 0, ID: "video", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo}},
			}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
			registry.RegisterMuxerDescriptor(format.Descriptor{
				Format:     av.FormatMatroska,
				Media:      []av.MediaType{av.MediaAudio},
				Codecs:     []av.CodecID{av.CodecOpus},
				MinStreams: 1,
				MaxStreams: 1,
				Metadata: av.Metadata{
					"summary": "test matroska target accepts one Opus audio stream",
				},
			}, recipeAPIMuxerFactory{})
		}),
		goavruntime.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
		}),
	)

	target := goav.Write("out.mkv", io.Discard, goav.Format(av.FormatMatroska))
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Video().
		Decode().
		Branches(goav.Branch("video").Encode(codec.VP8(codec.Bitrate(600_000))).To(target)).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_mux_incompatible" {
		t.Fatalf("err = %v, want destination_mux_incompatible", err)
	}
	for _, want := range []string{
		"test matroska target accepts one Opus audio stream",
		"destination=out.mkv",
		"format=matroska",
		"branch=video codec=vp8 media=video",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want detail %q", err, want)
		}
	}
}

func TestExplainReportsMuxCompatibilityWarning(t *testing.T) {
	rt := mustRuntime(
		goavruntime.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{
				{Index: 0, ID: "video", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo}},
			}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
			registry.RegisterMuxer(av.FormatIVF, recipeAPIMuxerFactory{})
		}),
		goavruntime.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP9, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
		}),
	)

	web := goav.Mux("web", goav.Write("web.ivf", io.Discard, goav.Format(av.FormatIVF)))
	report, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Video().
		Decode().
		Branches(
			goav.Branch("v8").Encode(codec.VP8(codec.Bitrate(600_000))).To(web),
			goav.Branch("v9").Encode(codec.VP9(codec.Bitrate(900_000))).To(web),
		).
		Explain(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_mux_incompatible" {
		t.Fatalf("err = %v, want destination_mux_incompatible", err)
	}
	if !hasPlanWarning(report.Warnings, "destination_mux_incompatible") {
		t.Fatalf("warnings=%+v, want mux compatibility warning", report.Warnings)
	}
	if len(report.Missing) != 0 {
		t.Fatalf("missing=%+v, want none for mux compatibility", report.Missing)
	}
	if len(report.Destinations) != 1 || !equalStrings(report.Destinations[0].Branches, []string{"v8", "v9"}) {
		t.Fatalf("destinations=%+v", report.Destinations)
	}
}

func TestBuildRejectsIncompatibleAnnexBMuxGroup(t *testing.T) {
	rt := mustRuntime(
		goavruntime.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{
				{Index: 0, ID: "video", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo}},
			}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
			registry.RegisterMuxer(av.FormatAnnexB, recipeAPIMuxerFactory{})
		}),
		goavruntime.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
		}),
	)

	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Video().
		Decode().
		Encode(codec.VP8(codec.Bitrate(600_000))).
		To(goav.Write("out.h264", io.Discard, goav.Format(av.FormatAnnexB))).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_mux_incompatible" {
		t.Fatalf("err = %v, want destination_mux_incompatible", err)
	}
	if !strings.Contains(err.Error(), "Annex B destinations support one H264 video stream") ||
		!strings.Contains(err.Error(), "destination=out.h264") ||
		!strings.Contains(err.Error(), "branch=video codec=vp8 media=video") {
		t.Fatalf("err = %v, want Annex B codec guidance", err)
	}
}

// parsePackageSourceFiles parses every non-test .go file in the package
// directory, keyed by filename. The guards below only inspect declared names,
// so build tags are irrelevant and per-file parsing replaces the deprecated
// parser.ParseDir.
func parsePackageSourceFiles(t *testing.T) map[string]*ast.File {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	files := make(map[string]*ast.File)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files[name] = file
	}
	return files
}

func TestPackageKeepsLegacyHelpersOutOfFrontDoor(t *testing.T) {
	files := parsePackageSourceFiles(t)
	legacyFuncs := map[string]bool{
		"SelectAudio":            true,
		"SelectVideo":            true,
		"Route":                  true,
		"WithRTPName":            true,
		"WithRTPFeedback":        true,
		"WithRTPJitter":          true,
		"WithRTPDepacketizers":   true,
		"WithRTPBufferLimits":    true,
		"WithRTPDecodeBounds":    true,
		"WithRTPMaxTimestampGap": true,
		"WithTrackCodec":         true,
		"WithTrackStream":        true,
		"WithTrackPayloads":      true,
		"WithTrackFeedback":      true,
		"WithTrackMetadata":      true,
		"UseRuntime":             true,
		"WebRTCRemote":           true,
		"FrameSink":              true,
		"SinkEndpoint":           true,
		"FileOutput":             true,
		"URIOutput":              true,
		"AudioFlow":              true,
		"VideoFlow":              true,
		"OpusVoice":              true,
		"OpusMusic":              true,
		"Output":                 true,
		"Outputs":                true,
		"Destinations":           true,
		"Path":                   true,
		"Paths":                  true,
	}
	legacyTypes := map[string]bool{
		"Builder":                 true,
		"BranchBuilder":           true,
		"JobStreamBuilder":        true,
		"FlowBuilder":             true,
		"AudioFlowBuilder":        true,
		"VideoFlowBuilder":        true,
		"Input":                   true,
		"DestinationSpec":         true,
		"Output":                  true,
		"OutputIntent":            true,
		"OutputReport":            true,
		"OutputSpec":              true,
		"TargetIntent":            true,
		"TargetReport":            true,
		"Path":                    true,
		"PathSpec":                true,
		"PathBuilder":             true,
		"AudioOption":             true,
		"RecordOption":            true,
		"ProbeRequest":            true,
		"ProbeResult":             true,
		"ResizeOption":            true,
		"Source":                  true,
		"Stage":                   true,
		"Sink":                    true,
		"Metadata":                true,
		"CodecParameters":         true,
		"ConfigurableDestination": true,
		"JobOption":               true,
		"RTPOption":               true,
		"RTPInputOption":          true,
		"StreamBuilder":           true,
		"StreamOption":            true,
		"TargetRef":               true,
		"TargetSpec":              true,
		"TrackOption":             true,
		"TranscodeJob":            true,
		"TargetOrEndpoint":        true,
	}
	for _, name := range []string{
		"Destination" + "Cap" + "s",
		"Stream" + "Cap" + "s",
		"Cap" + "s",
	} {
		legacyTypes[name] = true
	}
	for filename, file := range files {
		for _, decl := range file.Decls {
			switch decl := decl.(type) {
			case *ast.FuncDecl:
				if decl.Recv != nil {
					continue
				}
				if legacyFuncs[decl.Name.Name] {
					t.Fatalf("goav.%s keeps a legacy helper on the front door in %s", decl.Name.Name, filename)
				}
			case *ast.GenDecl:
				for _, spec := range decl.Specs {
					typeSpec, ok := spec.(*ast.TypeSpec)
					if ok && legacyTypes[typeSpec.Name.Name] {
						t.Fatalf("goav.%s keeps a legacy type on the front door in %s", typeSpec.Name.Name, filename)
					}
				}
			default:
				continue
			}
		}
	}
}

func TestInputSpecKeepsManualDepacketizersOutOfRecipeFrontDoor(t *testing.T) {
	inputType := reflect.TypeOf(goav.InputSpec{})
	if _, ok := inputType.MethodByName("Depacketize"); ok {
		t.Fatal("InputSpec exposes manual depacketizer wiring; RTP recipes should use codec intent")
	}
}

func TestBranchesIsTheOnlyPublicPlannedSplitVerb(t *testing.T) {
	streamType := reflect.TypeOf(goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).Audio())
	if _, ok := streamType.MethodByName("Branches"); !ok {
		t.Fatal("stream chain should expose Branches for planned stream splits")
	}
	if _, ok := streamType.MethodByName("Fork"); ok {
		t.Fatal("stream chain should not expose Fork; Branches is the public planned split verb")
	}
	if _, ok := streamType.MethodByName("Tee"); ok {
		t.Fatal("stream chain should not expose Tee; flows apply to branches")
	}
	if _, ok := streamType.MethodByName("Branch"); ok {
		t.Fatal("stream chain should not expose build-time Branch; use Branches")
	}
}

func TestExpertGraphNodeNamesDoNotCreateTapAnchors(t *testing.T) {
	graph := expert.Graph(bundle.MustNew())
	source := graph.Source("source", recipeAPISource{name: "source"})
	decode := graph.Stage("decode-audio", component.PacketFunc("decode-audio", func(ctx context.Context, packet *av.Packet, emit component.Emit) error {
		return emit.Packet(packet)
	}))
	base := graph.Sink("base", component.SinkFunc("base", func(context.Context, component.Message) error {
		return nil
	}))
	graph.Connect(source.Out(), decode.In())
	graph.Connect(decode.Out(), base.In())
	task, err := graph.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if taps := task.Taps(); len(taps) != 0 {
		t.Fatalf("Taps() = %+v, want no taps inferred from expert graph node names", taps)
	}
	_, err = task.Attach(context.Background(),
		goav.Branch("implicit-tap").
			From(goav.FrameTap("audio.decoded")).
			To(goav.Sink(component.SinkFunc("implicit-tap", func(context.Context, component.Message) error {
				return nil
			}))),
	)
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != errcode.Code("runtime_branch_tap_missing") {
		t.Fatalf("err = %v, want runtime_branch_tap_missing", err)
	}
	attachment, err := task.Attach(context.Background(),
		goav.Branch("levels").
			From(decode).
			To(goav.Sink(component.SinkFunc("levels", func(context.Context, component.Message) error {
				return nil
			}))),
	)
	if err != nil {
		t.Fatal(err)
	}
	if attachment.Name() != "levels" {
		t.Fatalf("attachment name = %q", attachment.Name())
	}
	if err := attachment.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestTypedTapsDriveStreamIntent(t *testing.T) {
	decoded := goav.FrameTap("audio.decoded")
	encoded := goav.PacketTap("audio.encoded")

	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		Tap(decoded).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		Tap(encoded).
		To(goav.Write("encoded.ogg", io.Discard))

	intent := goav.JobPlanForTest(job)
	if len(intent.Streams) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
	taps := goav.StreamTapsForTest(intent.Streams[0])
	if len(taps) != 2 ||
		taps[0].Name != decoded.Name() ||
		taps[0].Domain != shape.DomainFrame ||
		taps[0].After != plan.OpDecode ||
		taps[1].Name != encoded.Name() ||
		taps[1].Domain != shape.DomainPacket ||
		taps[1].After != plan.OpEncode {
		t.Fatalf("taps: %+v", taps)
	}
}

func TestTypedTapDomainMismatchIsActionable(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Copy().
		Tap(goav.FrameTap("audio.packets")).
		To(goav.Write("copy.ogg", io.Discard)).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "tap_domain_mismatch" {
		t.Fatalf("err = %v, want tap_domain_mismatch with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "FrameTap") ||
		!strings.Contains(err.Error(), "PacketTap") {
		t.Fatalf("err = %v, want typed tap guidance", err)
	}
}

func TestTypedBranchTapDomainMismatchIsActionable(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		Branches(
			goav.Branch("levels").
				Tap(goav.PacketTap("audio.levels")).
				To(goav.Sink(component.SinkFunc("levels", func(context.Context, component.Message) error {
					return nil
				}))),
		).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "tap_domain_mismatch" {
		t.Fatalf("err = %v, want tap_domain_mismatch with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "FrameTap") ||
		!strings.Contains(err.Error(), "PacketTap") {
		t.Fatalf("err = %v, want typed tap guidance", err)
	}
}

func TestReadmeKeepsAdvancedRuntimeKnobsOutOfFrontDoor(t *testing.T) {
	body, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	// UseRuntime is deliberately absent from this list: the "Testing Your
	// Pipeline" section front-doors it as the seam that injects the
	// deterministic goavtest.Runtime() into a job under test.
	for _, advanced := range []string{
		"WithFormatAdapter",
		"RTPBuffer",
		"MaxTimestampGap",
		"Runtime.Graph",
		"LiveTask.Attach",
		"Expert(",
		"expert.Graph",
		"GraphBuilder",
		"graph.Source",
		"graph.Connect",
	} {
		if strings.Contains(text, advanced) {
			t.Fatalf("README exposes %s in the front-door guide", advanced)
		}
	}
}

func TestReadmeUsesBranchDestinationVocabulary(t *testing.T) {
	body, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, forbidden := range []string{
		"goav.Path(",
		".Paths(",
		"goav.Output(",
		".Outputs(",
		".From(\"",
		".To(\"",
		"PathSpec",
		"PathBuilder",
		"AudioFlow(",
		"VideoFlow(",
		"EndpointSpec",
		"FromTap(",
		"FileOutput",
		"URIOutput",
		"SinkEndpoint",
		"SinkDestination",
		"TapName(",
		"goav.Target(",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("README keeps old branch/destination vocabulary %q", forbidden)
		}
	}
	for _, required := range []string{
		"goav.Branch(",
		"Branches(",
		"goav.Writer(",
		"goav.Sink(",
		"goav.Write(",
		"goav.Mux(",
		"Use `goav.Mux(name, destination)`",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("README should show %s in the public composition grammar", required)
		}
	}
	for _, advanced := range []string{
		"Runtime attach point",
		"Reusable operation list",
		"goav.FrameTap(",
		"goav.PacketTap(",
		"goav.Flow(",
	} {
		if strings.Contains(text, advanced) {
			t.Fatalf("README should keep %s out of the beginner recipe table", advanced)
		}
	}
	if strings.Contains(text, "compatibility sugar") {
		t.Fatalf("README should not keep same-handle grouping compatibility wording")
	}
}

func TestAPISurfaceFrontDoorGrammarAvoidsAdvancedSurfaces(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("docs", "API_SURFACE.md"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	start := strings.Index(text, "## A. Front-door Grammar")
	end := strings.Index(text, "## B. Extension Points")
	if start < 0 || end < 0 || end <= start {
		t.Fatalf("API surface grammar section not found")
	}
	section := text[start:end]
	for _, required := range []string{
		"goav.From(input)",
		".Decode() or .Copy()",
		".Branches(goav.Branch(\"x\")...To(dst))",
		".To(Write|URI|Writer|Custom|Sink|Mux)",
		"Task: Run, Close",
		"*goav.BuildError",
	} {
		if !strings.Contains(section, required) {
			t.Fatalf("API surface front-door grammar should include %q", required)
		}
	}
	for _, advanced := range []string{
		"goav.Mix/Composite/Select",
		"Join(name, stage, arms)",
		"goav.Flow(\"name\")",
		"input.Stream(av.Stream",
		".OnStream(",
		"BranchSpec also drives task.Attach",
		"LiveTask",
		"Mutable.Attach",
		"Attachment.Rebranch",
		"Watch(inspect.EventFilter)",
		"SelectActive",
		"Snapshot ->",
	} {
		if strings.Contains(section, advanced) {
			t.Fatalf("API surface front-door grammar should keep %s in advanced docs", advanced)
		}
	}
}

func TestFrontDoorDocsPreferCopyVerb(t *testing.T) {
	for _, file := range []string{"docs/API_SURFACE.md", "docs/OPERATIONS.md"} {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		text := string(body)
		for _, forbidden := range []string{
			"Copy vs Encode(codec.Copy())",
			"two spellings",
			"lowers to `Encode(codec.Copy())`",
		} {
			if strings.Contains(text, forbidden) {
				t.Fatalf("%s should prefer .Copy() as the recipe spelling, found %q", file, forbidden)
			}
		}
	}
}

func TestPublicDiagnosticsUseDestinationVocabulary(t *testing.T) {
	files := []string{
		"branch.go",
		"recipe.go",
		"intent.go",
		"input.go",
		"destination.go",
		"chain.go",
		"transform.go",
		"codec_spec.go",
		"validate_codec.go",
		"branch_compose_plan.go",
		"recipe_compile.go",
		"recipe_mux_compat.go",
		"runtime_attach.go",
		"runtime_encode.go",
		"runtime_format_error.go",
		"branch_compose_build.go",
	}
	forbidden := []string{
		"target_muxer_missing",
		"target_format_unknown",
		"target_mux_incompatible",
		"target_shape_mismatch",
		"target_duplicate",
		"target_missing",
		"target_invalid",
		"branch_target_unmatched",
		"output_target_missing",
		"open target",
		"sink target",
		"mux target",
		"byte target",
	}
	for _, file := range files {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		text := string(body)
		for _, term := range forbidden {
			if strings.Contains(text, term) {
				t.Fatalf("%s should use destination vocabulary, found %q", file, term)
			}
		}
	}
}

func TestDocsShowCustomDestinations(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("docs", "EXTENSION_COOKBOOK.md"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, required := range []string{
		"## Custom Destination",
		"goav.Writer(",
		"goav.Custom(",
		"provider.Info",
		"provider.TransactionalWriter",
		"goav.Format(",
		"goav.MIME(",
		"goav.Metadata(",
		"goav.Mux(",
		"Use `goav.Mux(name, destination)`",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("extension cookbook should keep custom destination text %q", required)
		}
	}
	if strings.Contains(text, "compatibility sugar") {
		t.Fatalf("extension cookbook should not keep same-handle grouping compatibility wording")
	}
}

func TestDocsShowCustomSources(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("docs", "EXTENSION_COOKBOOK.md"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, required := range []string{
		"goav.Source(",
		"shape.Packet(",
		"shape.Event(",
		"source.Push",
		"push.Packet(",
		"push.Frame(",
		"push.Event(",
		"push.EOS()",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("extension cookbook should keep custom source text %q", required)
		}
	}
}

func TestDocsLinkCompiledBootstrapExamples(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("docs", "EXTENSION_COOKBOOK.md"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, required := range []string{
		"ExampleSource_pushAccounting",
		"ExampleWriter_transactionalUpload",
		"ExampleWithEncoder_customSettings",
		"ExampleTask_flowchart",
		"graphrender.RenderTaskFlowchart(task)",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("extension cookbook should keep compiled bootstrap example %q", required)
		}
	}
}

func TestDocsShowDebugDiagnosticsWorkflow(t *testing.T) {
	for _, file := range []string{"docs/USE_CASES.md"} {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		text := string(body)
		for _, required := range []string{
			"Debug And Diagnostics",
			"job.Explain(ctx)",
			"task.Watch().Events()",
			"task.Attach(ctx",
			"Attachment.Snapshot()",
			"Inspectable.Snapshot()",
			"task.Snapshot()",
			"component.FrameFunc(\"rms\"",
		} {
			if !strings.Contains(text, required) {
				t.Fatalf("%s should show debug diagnostics workflow %q", file, required)
			}
		}
	}
}

func TestDocsShowCodecControlsAndDeclarativePerformanceGoal(t *testing.T) {
	controlPlane, err := os.ReadFile(filepath.Join("docs", "CONTROL_PLANE.md"))
	if err != nil {
		t.Fatal(err)
	}
	controlPlaneText := strings.Join(strings.Fields(string(controlPlane)), " ")
	for _, required := range []string{
		"codec.Control(",
		"Opus, VP8, VP9, and AV1 are full encode/decode recipe verticals",
		"one typed settings contract everywhere",
		"Custom runtime encoders work through the generic `encode codec=<id> media=<kind>`",
		"`CodecSettings.Custom` for the adapter to validate",
		"The generated reference is the running host itself",
		"`goav ctl capabilities` is machine-readable",
		"public grammar stays Input, Stream, Tap, Branch, Destination, Flow,",
		"workflows should be expressible through declarative recipes",
	} {
		if !strings.Contains(controlPlaneText, required) {
			t.Fatalf("control-plane docs should keep codec/declarative goal text %q", required)
		}
	}
	for _, forbidden := range []string{
		"ShapeFramerate",
		"framerate from shape",
		"framerate/FPS",
	} {
		if strings.Contains(controlPlaneText, forbidden) {
			t.Fatalf("control-plane docs should not reintroduce shape-side encode tuning %q", forbidden)
		}
	}

	performance, err := os.ReadFile("docs/PERFORMANCE.md")
	if err != nil {
		t.Fatal(err)
	}
	performanceText := string(performance)
	for _, required := range []string{
		"Hot paths must keep allocation explicit and bounded",
		"per `source.Push` delivery, which currently allocates one independent",
		"Keep recipe, flow, branch, tap, destination, and codec abstractions cold-path",
		"do not dispatch through them for each packet or frame",
		"one cold-path executable `WorkPlan` and runtime `WorkPatch`",
		"must not route",
		"workflow-specific compiler dispatch",
	} {
		if !strings.Contains(performanceText, required) {
			t.Fatalf("performance docs should keep bounded-cost goal text %q", required)
		}
	}

	roadmap, err := os.ReadFile("docs/ROADMAP.md")
	if err != nil {
		t.Fatal(err)
	}
	roadmapText := string(roadmap)
	for _, required := range []string{
		"`input -> stream -> operations -> tap -> branch -> destination` lowers into",
		"`WorkPlan -> pipeline.Graph -> Task`",
		"Make runtime attachment a patch of the same plan model",
		"`WorkPatch`",
		"Collapse `Target` into `Destination`",
	} {
		if !strings.Contains(roadmapText, required) {
			t.Fatalf("roadmap should keep graph-plan goal text %q", required)
		}
	}

	progress, err := os.ReadFile("docs/PROGRESS.md")
	if err != nil {
		t.Fatal(err)
	}
	progressText := string(progress)
	for _, required := range []string{
		"normal workflows lower from `input -> stream -> operations -> tap -> branch -> destination` into `WorkPlan -> pipeline.Graph -> Task`",
		"runtime attach lowers the same branch model into `WorkPatch`",
		"direct streams are syntax sugar for an implicit `Branch(\"main\")`",
		"`Mux(name, destination)` groups branches into one sink or mux destination",
		"reusing the same ungrouped `Destination` value is rejected",
		"provider.Destination` is the extension point",
		"Direct `.To(...)` streams are only ergonomic syntax",
		"`branchComposePlan`, `runtimeBranch`, `destinationNames`",
		"normal composition does not import `goav/transcode`",
	} {
		if !strings.Contains(progressText, required) {
			t.Fatalf("progress should keep graph-plan goal text %q", required)
		}
	}
	for _, forbidden := range []string{
		"ShapeFramerate",
		"framerate from shape",
		"framerate/FPS",
	} {
		if strings.Contains(progressText, forbidden) {
			t.Fatalf("progress should not reintroduce shape-side encode tuning %q", forbidden)
		}
	}
}

func TestCodecSettingsOwnTuningAndAdapterOptions(t *testing.T) {
	codecSpec := reflect.TypeOf(codec.CodecSpec{})
	settingsType := reflect.TypeOf(codec.CodecSettings{})
	if field, ok := codecSpec.FieldByName("Settings"); !ok || field.Type != settingsType {
		t.Fatalf("CodecSpec.Settings = %v %v, want codec.CodecSettings", field.Type, ok)
	}
	for _, field := range []string{
		"Bitrate",
		"Framerate",
		"KeyframeInterval",
		"Profile",
		"Level",
		"Config",
		"Opaque",
		"Controls",
	} {
		if _, ok := codecSpec.FieldByName(field); ok {
			t.Fatalf("CodecSpec should not duplicate codec setting field %s", field)
		}
	}

	for _, tt := range []struct {
		name string
		typ  reflect.Type
	}{
		{name: "decode", typ: reflect.TypeOf(codec.DecodeConfig{})},
		{name: "encode", typ: reflect.TypeOf(codec.EncodeConfig{})},
	} {
		if field, ok := tt.typ.FieldByName("Settings"); !ok || field.Type != settingsType {
			t.Fatalf("%s config Settings = %v %v, want codec.CodecSettings", tt.name, field.Type, ok)
		}
		for _, field := range []string{
			"Bitrate",
			"Framerate",
			"KeyframeInterval",
			"Profile",
			"Level",
			"Config",
			"Opaque",
			"Controls",
		} {
			if _, ok := tt.typ.FieldByName(field); ok {
				t.Fatalf("%s config should not duplicate codec setting field %s", tt.name, field)
			}
		}
	}
	spec := codec.VP9(codec.Profile("0"), codec.Level("3.1"))
	if spec.Parameters.Profile != "" || spec.Parameters.Level != "" ||
		spec.Settings.Profile != "0" || spec.Settings.Level != "3.1" {
		t.Fatalf("profile/level should be codec settings, got parameters=%+v settings=%+v", spec.Parameters, spec.Settings)
	}
}

func TestArchitectureDocsUseSmallCompositionVocabulary(t *testing.T) {
	var body strings.Builder
	for _, file := range []string{
		"docs/ARCHITECTURE.md",
		"docs/ROADMAP.md",
	} {
		fileBody, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		body.Write(fileBody)
	}
	progressBody, err := os.ReadFile("docs/PROGRESS.md")
	if err != nil {
		t.Fatal(err)
	}
	body.WriteString(currentProgressSections(string(progressBody)))
	text := body.String()
	for _, forbidden := range []string{
		"Recipes: From, stream chains",
		"Intent graph: inputs, streams, transforms",
		"media-plan build kind",
		"media-plan build kinds",
		"Stream recipe transforms",
		"stream chains, not as",
		"apply to stream chains",
		"Simple high-level API | recipes, stream chains",
		"surface is small: `From`, stream chains",
		"direct stream chains",
		"AudioFlow",
		"VideoFlow",
		"SinkEndpoint",
		"FromTap",
		"From(node)",
		"node names from `Inspectable.Describe()`",
		"`Target`, destination constructors",
		"URIOut",
		"WriteCloser(",
		"`Object`",
		"TargetRef",
		"Recipes: From, chains, taps, branches, destinations",
		"Intent graph: inputs, selected media, chain operations, destinations, policies",
		"`Target`, `Destination`, and exported chain composition",
		"named `Target` refs for shared mux/sink groups",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("architecture docs keep stale composition vocabulary %q", forbidden)
		}
	}
	for _, required := range []string{
		"Recipes: From, stream selection, operations, taps, branches, destinations",
		"Intent graph: inputs, selected streams, ordered operations, destinations, policies",
		"work-plan lowerers",
		"Operation transforms such as",
		"Simple high-level API | `From`, stream selection, ordered operations",
		"surface is small: `From`, stream selection, ordered operations",
		"`Branch`, `Destination`, and operation composition",
		"direct `File`/`URI`/`Sink` destinations",
		"custom `Writer` destinations with `provider.Info`",
		"`Mux(name, destination)` for shared mux/sink groups",
		"preferred\nfirst-class grouping model",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("architecture docs should keep current composition vocabulary %q", required)
		}
	}
}

func currentProgressSections(text string) string {
	var current strings.Builder
	if before, _, ok := strings.Cut(text, "\n## Implementation Order"); ok {
		current.WriteString(before)
	}
	if _, done, ok := strings.Cut(text, "\n## Done Criteria"); ok {
		current.WriteString(done)
	}
	return current.String()
}

func TestBranchFromUsesTypedSources(t *testing.T) {
	method, ok := reflect.TypeOf(goav.Branch("typed")).MethodByName("From")
	if !ok {
		t.Fatal("Branch should expose From")
	}
	source := method.Type.In(1)
	if source.Kind() != reflect.Interface || source.String() == "interface {}" {
		t.Fatalf("Branch.From source type = %s, want sealed typed source interface", source)
	}

	_ = goav.Branch("tap").From(goav.FrameTap("audio.decoded"))
	graph := expert.Graph(bundle.MustNew())
	node := graph.Source("source", recipeAPISource{name: "source"})
	_ = goav.Branch("node").From(node)
	_ = goav.Branch("stream").From(node.Stream("audio"))
}

func TestUseCasesFlowExampleUsesDistinctBranches(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("docs", "USE_CASES.md"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	if strings.Contains(text, "Apply(voice).\n    Apply(voice)") {
		t.Fatal("use-case docs should not show repeated direct flow application when branches are the intended split")
	}
	if got := strings.Count(text, `goav.Branch("voice").Apply(voice).To(voiceOut)`); got != 1 {
		t.Fatalf("use-case docs voice flow branch count = %d, want 1", got)
	}
	if got := strings.Count(text, `goav.Branch("archive").Apply(archive).To(archiveOut)`); got != 1 {
		t.Fatalf("use-case docs archive flow branch count = %d, want 1", got)
	}
}

func TestDocsExplainFlowVersusBranchRule(t *testing.T) {
	for _, file := range []string{"docs/USE_CASES.md"} {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		text := strings.Join(strings.Fields(string(body)), " ")
		if !strings.Contains(text, "Use a direct stream when one reusable flow feeds one destination") ||
			!strings.Contains(text, "media point needs several downstream operation sequences") {
			t.Fatalf("%s should explain when to use a direct flow chain versus branches", file)
		}
	}
}

func TestDocsKeepGoAVNativeGoal(t *testing.T) {
	var body strings.Builder
	for _, file := range []string{"docs/PROGRESS.md", "docs/ROADMAP.md", "docs/ARCHITECTURE.md"} {
		fileBody, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		body.Write(fileBody)
	}
	text := strings.Join(strings.Fields(body.String()), " ")
	for _, required := range []string{
		"From(input) -> chain -> operations -> Tap -> Branch -> Destination -> Task",
		"shape.Spec",
		"BranchBuffer",
		"Branch + Do + Sink",
		"Events",
		"Snapshot",
		"custom source",
		"WorkPlan",
		"WorkPatch",
		"A flow is reusable operations",
		"shape validation is central",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("goal docs should include %q", required)
		}
	}
}

func TestDocsShowGoAVBranchBuffers(t *testing.T) {
	for _, file := range []string{"docs/USE_CASES.md"} {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		text := string(body)
		for _, required := range []string{
			"Branch buffers",
			"flow.Blocking(",
			"flow.DropOldest(",
			"flow.Latest()",
		} {
			if !strings.Contains(text, required) {
				t.Fatalf("%s should show branch buffer API %q", file, required)
			}
		}
	}
}

func TestFrontDoorDocsAvoidGStreamerVocabulary(t *testing.T) {
	var body strings.Builder
	for _, file := range []string{"README.md", "docs/USE_CASES.md"} {
		fileBody, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		body.Write(fileBody)
	}
	text := body.String()
	for _, forbidden := range []string{
		"Element",
		"Pad",
		"Bin",
		"Bus",
		"Pipeline State",
		"Flow.To(",
		"To(\"",
		"Record(",
		"Transcode(",
		"Runtime.Graph",
		"pipeline.BufferPolicy",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("front-door docs should not teach %q", forbidden)
		}
	}
}

func TestReadmeThirtySecondExamplesUseDefaultFormats(t *testing.T) {
	body, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	start := strings.Index(text, "## 30-Second Examples")
	end := strings.Index(text, "## Common Recipes")
	if start < 0 || end < 0 || end <= start {
		t.Fatalf("README sections not found")
	}
	frontDoor := text[start:end]
	for _, unsupported := range []string{
		".webm",
		".ogg",
		"Transcode(",
	} {
		if strings.Contains(frontDoor, unsupported) {
			t.Fatalf("30-second README examples include %s without default adapter support", unsupported)
		}
	}
}

func TestDocsUseDestinationOptions(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("docs", "EXTENSION_COOKBOOK.md"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, required := range []string{
		"goav.Write(\"\", out, goav.Format(av.FormatIVF))",
		"goav.Writer(",
		"goav.Format(",
		"goav.MIME(",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("extension cookbook should keep destination option example %q", required)
		}
	}
	for _, forbidden := range []string{
		").Format(",
		").MIME(",
		"ConfigurableDestination",
		"Adapter-Backed Workflows",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("extension cookbook should not teach stale destination/API spelling %q", forbidden)
		}
	}
}

func TestReusableComponentCatalogIsDocumented(t *testing.T) {
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(readme), "docs/COMPONENTS.md") {
		t.Fatal("README should link the reusable component catalog")
	}

	body, err := os.ReadFile("docs/COMPONENTS.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, want := range []string{
		"recipes express intent",
		"components do the media work",
		"expert graphs compose those components directly",
		"codec.DecoderStage",
		"format.DemuxSource",
		"format.MuxStage",
		"filter.Stage",
		"rtpav.Source",
		"rtpav.SequenceDetector",
		"webrtcav.TrackSet",
		"component.PacketFunc or component.FrameFunc meter",
		"stable",
		"experimental",
		"descriptor-only",
		"internal scaffold",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("docs/COMPONENTS.md missing %q", want)
		}
	}
}

func TestReusableComponentCatalogNamesAllocationProofs(t *testing.T) {
	body, err := os.ReadFile("docs/COMPONENTS.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, want := range []string{
		"## Allocation Proofs",
		"TestCoreResetAllocs",
		"TestMessageAndScratchResetAllocs",
		"TestGraphDirectRunAllocs",
		"TestDropControllerDecideAllocs",
		"TestSourceStartAllocs",
		"TestSequenceDetectorAllocs",
		"TestOpusDepacketizerAllocs",
		"TestVP8DepacketizerAllocs",
		"TestVP9DepacketizerAllocs",
		"TestH264DepacketizerAllocs",
		"TestAV1DepacketizerAllocs",
		"TestJitterRingAllocs",
		"TestFeedbackResultAllocs",
		"TestDecoderStageAllocs",
		"TestEncoderStageAllocs",
		"TestFormatResultResetAllocs",
		"TestDemuxSourceAllocs",
		"TestMuxStageAllocs",
		"TestStageAllocs",
		"TestDecodePacketLossConcealmentAllocs",
		"TestMuxerWriteAllocs",
		"TestDemuxerReadIntoAllocs",
		"TestFilterAllocs",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("docs/COMPONENTS.md allocation proofs missing %q", want)
		}
	}
}

func TestRootAPIUsesFromCompositionInsteadOfWorkflowHelpers(t *testing.T) {
	files := parsePackageSourceFiles(t)
	for filename, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil {
				continue
			}
			switch fn.Name.Name {
			case "Record", "Decode", "Transcode":
				t.Fatalf("goav.%s remains public in %s; compose with goav.From instead", fn.Name.Name, filename)
			}
		}
	}
}

func TestReportsUseDestinations(t *testing.T) {
	for _, tt := range []struct {
		name string
		typ  reflect.Type
	}{
		{name: "plan.Report", typ: reflect.TypeOf(plan.Report{})},
		{name: "plan.Branch", typ: reflect.TypeOf(plan.Branch{})},
	} {
		if _, ok := tt.typ.FieldByName("Targets"); ok {
			t.Fatalf("%s exposes Targets; use Destinations as the public routing field", tt.name)
		}
		if _, ok := tt.typ.FieldByName("Destinations"); !ok {
			t.Fatalf("%s should expose Destinations", tt.name)
		}
	}
}

func TestHighLevelCompositionInternalsAvoidEndpointVocabulary(t *testing.T) {
	files := []string{
		"branch.go",
		"flow.go",
		"recipe.go",
		"intent.go",
		"input.go",
		"destination.go",
		"chain.go",
		"transform.go",
		"codec_spec.go",
		"validate_codec.go",
		"branch_compose_plan.go",
		"recipe_compile.go",
		"media_plan_spec.go",
		"media_plan_build.go",
		"runtime_attach.go",
		"runtime_encode.go",
	}
	for _, file := range files {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(strings.ToLower(string(body)), "endpoint") {
			t.Fatalf("%s uses endpoint vocabulary; use destination naming in high-level composition code", file)
		}
	}
}

func TestRecipeConstructorsDoNotExposeRuntimeOptions(t *testing.T) {
	inputType := reflect.TypeOf(goav.InputSpec{})
	jobType := reflect.TypeOf((*goav.Job)(nil))

	cases := []struct {
		name string
		fn   any
		in   []reflect.Type
		out  reflect.Type
	}{
		{name: "From", fn: goav.From, in: []reflect.Type{reflect.SliceOf(inputType)}, out: jobType},
	}
	for _, tc := range cases {
		typ := reflect.TypeOf(tc.fn)
		if !typ.IsVariadic() || typ.NumIn() != len(tc.in) || typ.NumOut() != 1 || typ.Out(0) != tc.out {
			t.Fatalf("%s type = %s", tc.name, typ)
		}
		for i := range tc.in {
			if typ.In(i) != tc.in[i] {
				t.Fatalf("%s type = %s", tc.name, typ)
			}
		}
	}
}

func TestRecipeReportsNilRuntime(t *testing.T) {
	_, err := recordJob(
		goav.FileInput("input.ivf", strings.NewReader("")),
		goav.Write("recording.ivf", io.Discard),
	).UseRuntime(nil).Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "runtime_missing" {
		t.Fatalf("err = %v, want runtime_missing", err)
	}
	if !strings.Contains(err.Error(), "no runtime is configured") ||
		!strings.Contains(err.Error(), "goav.New") ||
		!strings.Contains(err.Error(), "bundle.MustNew") {
		t.Fatalf("err = %v, want runtime guidance", err)
	}
}

func TestRecipeReportsOmittedRuntime(t *testing.T) {
	_, err := recordJob(
		goav.FileInput("input.ivf", strings.NewReader("")),
		goav.Write("recording.ivf", io.Discard),
	).Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "runtime_missing" {
		t.Fatalf("err = %v, want runtime_missing", err)
	}
	if !strings.Contains(err.Error(), "bundle.Run") {
		t.Fatalf("err = %v, want bundle helper guidance", err)
	}
}

func TestReadmeRecordRecipeIsSmall(t *testing.T) {
	job := recordJob(
		goav.FileInput("input.ogg", strings.NewReader("")),
		goav.Write("recording.ogg", io.Discard),
	)

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(specText(spec), "input.ogg -> recording.ogg") {
		t.Fatalf("spec:\n%s", specText(spec))
	}
	intent := goav.JobPlanForTest(job)
	if intent.Name != "from" || len(intent.Inputs) != 1 || len(intent.Destinations) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestRecordRecipeCanWriteToTypedDestination(t *testing.T) {
	target := goav.Write("recording.ivf", io.Discard)
	job := goav.From(goav.FileInput("input.ivf", strings.NewReader(""))).
		Copy().
		To(target)

	intent := goav.JobPlanForTest(job)
	if len(intent.Destinations) != 1 || intent.Destinations[0].Name != "recording.ivf" {
		t.Fatalf("intent: %+v", intent)
	}

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "input.ivf -> recording.ivf") {
		t.Fatalf("spec:\n%s", text)
	}
}

func TestReadmeRecordFanoutRecipeIsSmall(t *testing.T) {
	job := recordJob(
		goav.FileInput("input.ivf", strings.NewReader("")),
		goav.Write("archive.ivf", io.Discard),
		goav.Write("preview.ivf", io.Discard),
	)

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "input.ivf -> archive.ivf") ||
		!strings.Contains(text, "input.ivf -> preview.ivf") {
		t.Fatalf("spec:\n%s", text)
	}
	intent := goav.JobPlanForTest(job)
	if intent.Name != "from" || len(intent.Inputs) != 1 || len(intent.Destinations) != 2 {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestReadmeAudioDecodeRecipeIsSmall(t *testing.T) {
	sink := component.SinkFunc("frames", func(context.Context, component.Message) error {
		return nil
	})
	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		To(goav.Sink(sink))

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "input.ogg -> select-audio") ||
		!strings.Contains(text, "select-audio -> decode-audio") ||
		!strings.Contains(text, "decode-audio -> frames") {
		t.Fatalf("spec:\n%s", text)
	}
	intent := goav.JobPlanForTest(job)
	if len(intent.Streams) != 1 || intent.Streams[0].Select.Type != "audio" || !goav.StreamHasDecodeForTest(intent.Streams[0]) {
		t.Fatalf("intent: %+v", intent)
	}
}

// TestTypedTapsCarryDeclaredDomain proves public tap constructors carry explicit
// frame/packet domains through stream and branch planning.
func TestTypedTapsCarryDeclaredDomain(t *testing.T) {
	frameJob := goav.From(goav.FileInput("in.webm", strings.NewReader(""))).
		Video().
		Decode().
		Resize(640, 360).
		Tap(goav.FrameTap("preview")).
		Encode(codec.VP9(codec.Bitrate(600_000))).
		To(goav.Write("out.webm", io.Discard))
	frameDomain := shape.MediaDomain("")
	for _, tap := range goav.StreamTapsForTest(goav.JobPlanForTest(frameJob).Streams[0]) {
		if tap.Name == "preview" {
			frameDomain = tap.Domain
			break
		}
	}
	if frameDomain != shape.DomainFrame {
		t.Fatalf("preview tap domain = %q, want frame", frameDomain)
	}

	pktJob := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		Branches(
			goav.Branch("archive").
				Encode(codec.Opus(codec.Bitrate(96_000))).
				Tap(goav.PacketTap("encoded")).
				To(goav.Write("archive.ogg", io.Discard)),
		)
	pktDomain := shape.MediaDomain("")
	for _, tap := range goav.StreamTapsForTest(goav.JobPlanForTest(pktJob).Streams[0]) {
		if tap.Name == "encoded" {
			pktDomain = tap.Domain
			break
		}
	}
	if pktDomain != shape.DomainPacket {
		t.Fatalf("encoded tap domain = %q, want packet", pktDomain)
	}
}

func TestReadmeCustomStageToCustomSinkRecipeIsSmall(t *testing.T) {
	meter := component.FrameFunc("meter", func(ctx context.Context, frame *av.Frame, emit component.Emit) error {
		return emit.Frame(frame)
	})
	sink := component.SinkFunc("levels", func(context.Context, component.Message) error {
		return nil
	})

	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		Do(meter).
		To(goav.Sink(sink))

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "decode-audio -> meter") ||
		!strings.Contains(text, "meter -> levels") {
		t.Fatalf("spec:\n%s", text)
	}
	intent := goav.JobPlanForTest(job)
	if len(intent.Streams) != 1 || !goav.StreamHasDecodeForTest(intent.Streams[0]) || goav.StreamEncodeForTest(intent.Streams[0]).ID != "" ||
		len(intent.Destinations) != 1 || intent.Destinations[0].Name != "levels" {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestAudioChainAppliesToStreamRecipeIntent(t *testing.T) {
	voice := goav.Flow("voice").
		Audio().
		Resample(16_000, codec.Mono).
		Encode(codec.Opus(codec.Bitrate(32_000), codec.Channels(codec.Mono)))

	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		Apply(voice).
		To(goav.Write("voice.ogg", io.Discard))

	intent := goav.JobPlanForTest(job)
	if len(intent.Streams) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
	stream := intent.Streams[0]
	if stream.Select.Type != av.MediaAudio || !goav.StreamHasDecodeForTest(stream) ||
		len(transformOperationsForTest(stream.Operations)) != 1 || transformOperationsForTest(stream.Operations)[0].Resample == nil ||
		transformOperationsForTest(stream.Operations)[0].Resample.SampleRate != 16_000 ||
		transformOperationsForTest(stream.Operations)[0].Resample.Channels != codec.Mono ||
		goav.StreamEncodeForTest(stream).ID != av.CodecOpus || goav.StreamEncodeForTest(stream).Settings.Bitrate != 32_000 ||
		len(stream.Destinations) != 1 || stream.Destinations[0] != "voice.ogg" {
		t.Fatalf("stream intent: %+v", stream)
	}
}

func TestStreamRecipeCanWriteToTypedDestination(t *testing.T) {
	voice := goav.Flow("voice").
		Audio().
		Resample(16_000, codec.Mono).
		Encode(codec.Opus(codec.Bitrate(32_000), codec.Channels(codec.Mono)))
	voiceOut := goav.Write("voice.ogg", io.Discard)

	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		Apply(voice).
		To(voiceOut)

	intent := goav.JobPlanForTest(job)
	if len(intent.Streams) != 1 || len(intent.Destinations) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
	stream := intent.Streams[0]
	if stream.Select.Type != av.MediaAudio ||
		!goav.StreamHasDecodeForTest(stream) ||
		goav.StreamEncodeForTest(stream).ID != av.CodecOpus ||
		!equalStrings(stream.Destinations, []string{"voice.ogg"}) ||
		intent.Destinations[0].Name != "voice.ogg" {
		t.Fatalf("intent: %+v", intent)
	}

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "encode-audio -> voice.ogg") {
		t.Fatalf("spec:\n%s", text)
	}
}

func TestToAcceptsDestinationSlices(t *testing.T) {
	destinations := []goav.Destination{
		goav.Write("archive.ogg", io.Discard),
		goav.Sink(component.SinkFunc("stats", func(context.Context, component.Message) error {
			return nil
		})),
	}

	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(destinations...)

	intent := goav.JobPlanForTest(job)
	if len(intent.Streams) != 1 ||
		!equalStrings(intent.Streams[0].Destinations, []string{"archive.ogg", "stats"}) ||
		len(intent.Destinations) != 2 ||
		intent.Destinations[0].Name != "archive.ogg" ||
		intent.Destinations[1].Name != "stats" {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestMuxGroupsBranches(t *testing.T) {
	web := goav.Mux("web", goav.Write("web.ivf", io.Discard, goav.Format(av.FormatIVF)))

	job := goav.From(goav.FileInput("input.ivf", strings.NewReader(""))).
		Video().
		Decode().
		Branches(
			goav.Branch("v720").Encode(codec.VP9(codec.Bitrate(2_000_000))).To(web),
			goav.Branch("v360").Resize(640, 360).Encode(codec.VP8(codec.Bitrate(600_000))).To(web),
		)

	intent := goav.JobPlanForTest(job)
	if len(intent.Streams) != 2 || len(intent.Destinations) != 1 || intent.Destinations[0].Name != "web.ivf" {
		t.Fatalf("intent: %+v", intent)
	}
	if !equalStrings(intent.Streams[0].Destinations, []string{"web.ivf"}) ||
		!equalStrings(intent.Streams[1].Destinations, []string{"web.ivf"}) {
		t.Fatalf("streams: %+v", intent.Streams)
	}
}

func TestDuplicateDestinationNameRequiresMux(t *testing.T) {
	left := goav.Write("web.ivf", io.Discard, goav.Format(av.FormatIVF))
	right := goav.Write("web.ivf", io.Discard, goav.Format(av.FormatIVF))

	_, err := goav.From(goav.FileInput("input.ivf", strings.NewReader(""))).
		Video().
		Decode().
		Branches(
			goav.Branch("v720").Encode(codec.VP9(codec.Bitrate(2_000_000))).To(left),
			goav.Branch("v360").Encode(codec.VP8(codec.Bitrate(600_000))).To(right),
		).
		Describe()
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_duplicate" {
		t.Fatalf("err = %v, want destination_duplicate", err)
	}
}

func TestDestinationConstructorsReturnDestination(t *testing.T) {
	destinationType := reflect.TypeOf((*goav.Destination)(nil)).Elem()
	for name, fn := range map[string]any{
		"Custom": goav.Custom,
		"URI":    goav.URI,
		"Write":  goav.Write,
		"Writer": goav.Writer,
	} {
		fnType := reflect.TypeOf(fn)
		if fnType.NumOut() != 1 || fnType.Out(0) != destinationType {
			t.Fatalf("%s return = %v, want Destination", name, fnType.Out(0))
		}
	}
	sinkFn := reflect.TypeOf(goav.Sink)
	if sinkFn.NumOut() != 1 || sinkFn.Out(0) != destinationType {
		t.Fatalf("Sink return = %v, want Destination", sinkFn.Out(0))
	}
}

func TestDestinationProviderIsWrappedByCustomHandle(t *testing.T) {
	providerType := reflect.TypeOf((*provider.Destination)(nil)).Elem()
	impl := reflect.TypeOf(recipeAPICustomDestination{})
	if !impl.Implements(providerType) {
		t.Fatalf("%v should implement provider.Destination", impl)
	}
	destinationType := reflect.TypeOf(goav.Custom("custom", recipeAPICustomDestination{}))
	if destinationType.Name() != "Destination" || destinationType.Kind() != reflect.Struct {
		t.Fatalf("Custom return type = %v, want concrete Destination handle", destinationType)
	}
	if impl.AssignableTo(destinationType) {
		t.Fatalf("provider %v should not be assignable to Destination handle %v", impl, destinationType)
	}
}

func TestRootAPIDoesNotExportTarget(t *testing.T) {
	body, err := os.ReadFile("branch.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "type Destination struct") ||
		strings.Contains(string(body), "type Destination interface") {
		t.Fatal("root API should expose Destination as a concrete handle, not an interface")
	}
	if strings.Contains(string(body), "func Target(") {
		t.Fatal("root API should not export Target")
	}
}

func TestFlowCarriesOrderedCustomStageAndTap(t *testing.T) {
	meter := component.FrameFunc("meter", func(context.Context, *av.Frame, component.Emit) error {
		return nil
	})
	voice := goav.Flow("voice").
		Audio().
		Do(meter).
		Tap(goav.FrameTap("audio.after-meter")).
		Resample(16_000, codec.Mono).
		Encode(codec.Opus(codec.Bitrate(32_000), codec.Channels(codec.Mono)))

	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		Apply(voice).
		To(goav.Write("voice.ogg", io.Discard))

	intent := goav.JobPlanForTest(job)
	if len(intent.Streams) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
	operations := intent.Streams[0].Operations
	if len(operations) != 5 {
		t.Fatalf("operations: %+v", operations)
	}
	transform := goav.TransformViewForTestFrom(operations[3].Transform)
	if operations[0].Kind != plan.OpDecode ||
		operations[1].Kind != plan.OpStage || operations[1].Component != "meter" ||
		operations[2].Kind != plan.OpTap || operations[2].Tap.Name != "audio.after-meter" ||
		operations[3].Kind != plan.OpTransform || transform.Resample == nil ||
		operations[4].Kind != plan.OpEncode || operations[4].Encode.ID != av.CodecOpus {
		t.Fatalf("operations: %+v", operations)
	}
	if len(goav.StreamTapsForTest(intent.Streams[0])) != 1 || goav.StreamTapsForTest(intent.Streams[0])[0].Name != "audio.after-meter" {
		t.Fatalf("taps: %+v", goav.StreamTapsForTest(intent.Streams[0]))
	}
}

func TestFlowTapAfterEncodeIsPacketTap(t *testing.T) {
	voice := goav.Flow("voice").
		Audio().
		Encode(codec.Opus(codec.Bitrate(32_000), codec.Channels(codec.Mono))).
		Tap(goav.PacketTap("audio.voice.packets"))

	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		Apply(voice).
		To(goav.Write("voice.ogg", io.Discard))

	intent := goav.JobPlanForTest(job)
	if len(intent.Streams) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
	operations := intent.Streams[0].Operations
	if len(operations) != 3 ||
		operations[0].Kind != plan.OpDecode ||
		operations[1].Kind != plan.OpEncode ||
		operations[2].Kind != plan.OpTap ||
		operations[2].Tap.Name != "audio.voice.packets" ||
		operations[2].Tap.Domain != shape.DomainPacket ||
		operations[2].Tap.After != plan.OpEncode {
		t.Fatalf("operations: %+v", operations)
	}
	if len(goav.StreamTapsForTest(intent.Streams[0])) != 1 ||
		goav.StreamTapsForTest(intent.Streams[0])[0].Name != "audio.voice.packets" ||
		goav.StreamTapsForTest(intent.Streams[0])[0].Domain != shape.DomainPacket ||
		goav.StreamTapsForTest(intent.Streams[0])[0].After != plan.OpEncode {
		t.Fatalf("taps: %+v", goav.StreamTapsForTest(intent.Streams[0]))
	}
}

func TestFlowCopyAppliesToStreamRecipeIntent(t *testing.T) {
	packets := goav.Flow("packets").
		Audio().
		Copy().
		Tap(goav.PacketTap("audio.copied"))

	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Apply(packets).
		To(goav.Write("copy.ogg", io.Discard))

	intent := goav.JobPlanForTest(job)
	if len(intent.Streams) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
	stream := intent.Streams[0]
	if goav.StreamHasDecodeForTest(stream) ||
		!goav.StreamEncodeForTest(stream).Copy ||
		len(stream.Operations) != 2 ||
		stream.Operations[0].Kind != plan.OpCopy ||
		stream.Operations[1].Kind != plan.OpTap ||
		stream.Operations[1].Tap.Name != "audio.copied" ||
		stream.Operations[1].Tap.Domain != shape.DomainPacket ||
		stream.Operations[1].Tap.After != plan.OpCopy {
		t.Fatalf("stream intent: %+v", stream)
	}
	if len(goav.StreamTapsForTest(stream)) != 1 ||
		goav.StreamTapsForTest(stream)[0].Name != "audio.copied" ||
		goav.StreamTapsForTest(stream)[0].Domain != shape.DomainPacket ||
		goav.StreamTapsForTest(stream)[0].After != plan.OpCopy {
		t.Fatalf("taps: %+v", goav.StreamTapsForTest(stream))
	}
}

func TestFlowBranchesStayOnJobAndBuildIntent(t *testing.T) {
	voice := goav.Flow("voice").
		Audio().
		Resample(16_000, codec.Mono).
		Encode(codec.Opus(codec.Bitrate(32_000), codec.Channels(codec.Mono)))
	archive := goav.Flow("archive").
		Audio().
		Resample(48_000, codec.Stereo).
		Encode(codec.Opus(codec.Bitrate(128_000), codec.Channels(codec.Stereo)))
	voiceOut := goav.Write("voice.ogg", io.Discard)
	archiveOut := goav.Write("archive.ogg", io.Discard)

	job := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Audio(goav.StreamIndex(0)).
		Branches(
			goav.Branch("voice").Apply(voice).To(voiceOut),
			goav.Branch("archive").Apply(archive).To(archiveOut),
		)

	if reflect.TypeOf(job) != reflect.TypeOf((*goav.Job)(nil)) {
		t.Fatalf("Branches returned %T, want *goav.Job", job)
	}
	intent := goav.JobPlanForTest(job)
	if len(intent.Streams) != 2 || len(intent.Destinations) != 2 {
		t.Fatalf("intent: %+v", intent)
	}
	if intent.Streams[0].Name != "voice" || intent.Streams[1].Name != "archive" ||
		!intent.Streams[0].Select.UseIndex || intent.Streams[0].Select.Index != 0 ||
		goav.StreamEncodeForTest(intent.Streams[0]).ID != av.CodecOpus || goav.StreamEncodeForTest(intent.Streams[1]).ID != av.CodecOpus ||
		transformOperationsForTest(intent.Streams[0].Operations)[0].Resample.SampleRate != 16_000 ||
		transformOperationsForTest(intent.Streams[1].Operations)[0].Resample.SampleRate != 48_000 {
		t.Fatalf("streams: %+v", intent.Streams)
	}

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	for _, want := range []string{
		"decode-audio -> resample-voice",
		"resample-voice -> encode-voice",
		"decode-audio -> resample-archive",
		"resample-archive -> encode-archive",
		"encode-voice -> voice.ogg",
		"encode-archive -> archive.ogg",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("spec missing %q:\n%s", want, text)
		}
	}
}

func TestFlowAppliesToTranscodeBranch(t *testing.T) {
	preview := goav.Flow("preview").
		Video().
		Resize(640, 360).
		Tap(goav.FrameTap("preview.frames")).
		Encode(codec.VP9(codec.Bitrate(600_000)))
	web := goav.Write("preview.webm", io.Discard)

	job := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Decode().
		Tap(goav.FrameTap("video.decoded")).
		Branches(goav.Branch("preview").Apply(preview).To(web))

	intent := goav.JobPlanForTest(job)
	if len(intent.Streams) != 1 || intent.Streams[0].Name != "preview" ||
		len(transformOperationsForTest(intent.Streams[0].Operations)) != 1 ||
		transformOperationsForTest(intent.Streams[0].Operations)[0].Resize.Width != 640 ||
		transformOperationsForTest(intent.Streams[0].Operations)[0].Resize.Height != 360 ||
		goav.StreamEncodeForTest(intent.Streams[0]).ID != av.CodecVP9 ||
		goav.StreamEncodeForTest(intent.Streams[0]).Settings.Bitrate != 600_000 {
		t.Fatalf("intent: %+v", intent)
	}
	foundPreview := false
	for _, tap := range goav.StreamTapsForTest(intent.Streams[0]) {
		if tap.Name == "preview.frames" && tap.After == plan.OpTransform {
			foundPreview = true
			break
		}
	}
	if !foundPreview {
		t.Fatalf("taps: %+v, want preview.frames after transform", goav.StreamTapsForTest(intent.Streams[0]))
	}
	operations := intent.Streams[0].Operations
	if len(operations) != 5 ||
		operations[0].Kind != plan.OpDecode ||
		operations[1].Kind != plan.OpTap || operations[1].Tap.Name != "video.decoded" ||
		operations[2].Kind != plan.OpTransform ||
		operations[3].Kind != plan.OpTap || operations[3].Tap.Name != "preview.frames" ||
		operations[4].Kind != plan.OpEncode {
		t.Fatalf("operations: %+v", operations)
	}
}

func TestBranchesGroupSelectedStreams(t *testing.T) {
	watch := goav.Mux("watch", goav.Write("watch.webm", io.Discard))
	mobile := goav.Mux("mobile", goav.Write("mobile.webm", io.Discard))
	job := goav.From(goav.FileInput("source.webm", strings.NewReader(""))).
		Video().
		Decode().
		Tap(goav.FrameTap("video.decoded")).
		Branches(
			goav.Branch("v1080").
				Resize(1920, 1080).
				Encode(codec.VP9(codec.Bitrate(4_000_000))).
				To(watch),
			goav.Branch("v360").
				Resize(640, 360).
				Encode(codec.VP8(codec.Bitrate(600_000))).
				To(mobile),
		).
		Audio().
		Decode().
		Tap(goav.FrameTap("audio.decoded")).
		Branches(
			goav.Branch("a96").
				Resample(48_000, codec.Stereo).
				Encode(codec.Opus(codec.Bitrate(96_000))).
				To(watch, mobile),
		)

	intent := goav.JobPlanForTest(job)
	if len(intent.Streams) != 3 || len(intent.Destinations) != 2 {
		t.Fatalf("intent: %+v", intent)
	}
	tests := []struct {
		name       string
		from       string
		fromDomain shape.MediaDomain
		codec      av.CodecID
		outputs    []string
	}{
		{name: "v1080", from: "video.decoded", fromDomain: shape.DomainFrame, codec: av.CodecVP9, outputs: []string{"watch.webm"}},
		{name: "v360", from: "video.decoded", fromDomain: shape.DomainFrame, codec: av.CodecVP8, outputs: []string{"mobile.webm"}},
		{name: "a96", from: "audio.decoded", fromDomain: shape.DomainFrame, codec: av.CodecOpus, outputs: []string{"watch.webm", "mobile.webm"}},
	}
	for i := range tests {
		stream := intent.Streams[i]
		if stream.Name != tests[i].name || stream.From.Name() != tests[i].from || stream.From.Domain() != tests[i].fromDomain ||
			goav.StreamEncodeForTest(stream).ID != tests[i].codec || !equalStrings(stream.Destinations, tests[i].outputs) {
			t.Fatalf("stream[%d]=%+v, want %+v", i, stream, tests[i])
		}
	}
	if transformOperationsForTest(intent.Streams[0].Operations)[0].Resize.Width != 1920 ||
		transformOperationsForTest(intent.Streams[1].Operations)[0].Resize.Width != 640 ||
		transformOperationsForTest(intent.Streams[2].Operations)[0].Resample.SampleRate != 48_000 {
		t.Fatalf("branch transforms: %+v", intent.Streams)
	}

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	for _, want := range []string{
		"decode-video -> resize-v1080",
		"decode-video -> resize-v360",
		"decode-audio -> resample-a96",
		"encode-v1080 -> watch.webm",
		"encode-v360 -> mobile.webm",
		"encode-a96 -> watch.webm",
		"encode-a96 -> mobile.webm",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("spec missing %q:\n%s", want, text)
		}
	}
}

func TestBranchAfterDecodeCustomStageUsesOrderedOperations(t *testing.T) {
	meter := component.FrameFunc("meter", func(ctx context.Context, frame *av.Frame, emit component.Emit) error {
		return emit.Frame(frame)
	})
	web := goav.Write("web.webm", io.Discard)
	job := goav.From(goav.FileInput("source.webm", strings.NewReader(""))).
		Video().
		Decode().
		Tap(goav.FrameTap("video.decoded")).
		Branches(
			goav.Branch("v360").
				Do(meter).
				Resize(640, 360).
				Encode(codec.VP9(codec.Bitrate(600_000))).
				To(web),
		)

	intent := goav.JobPlanForTest(job)
	if len(intent.Streams) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
	want := []plan.OperationKind{plan.OpDecode, plan.OpTap, plan.OpStage, plan.OpTransform, plan.OpEncode}
	if !equalOperationKinds(operationSpecKinds(intent.Streams[0].Operations), want) {
		t.Fatalf("operations=%+v, want %+v", intent.Streams[0].Operations, want)
	}
	if intent.Streams[0].Operations[2].Component != "meter" {
		t.Fatalf("stage operation: %+v", intent.Streams[0].Operations[2])
	}

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	for _, want := range []string{
		"decode-video -> meter",
		"meter -> resize-v360",
		"resize-v360 -> encode-v360",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("spec missing %q:\n%s", want, text)
		}
	}
}

func TestBranchTapAfterEncodeIsPacketTap(t *testing.T) {
	archive := goav.Write("archive.ogg", io.Discard)
	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		Tap(goav.FrameTap("audio.decoded")).
		Branches(
			goav.Branch("archive").
				Encode(codec.Opus(codec.Bitrate(96_000))).
				Tap(goav.PacketTap("audio.encoded")).
				To(archive),
		)

	intent := goav.JobPlanForTest(job)
	if len(intent.Streams) != 1 {
		t.Fatalf("streams: %+v", intent.Streams)
	}
	foundEncoded := false
	for _, tap := range goav.StreamTapsForTest(intent.Streams[0]) {
		if tap.Name == "audio.encoded" {
			foundEncoded = tap.Domain == shape.DomainPacket &&
				tap.MediaKind == av.MediaAudio &&
				tap.After == plan.OpEncode
			break
		}
	}
	if !foundEncoded {
		t.Fatalf("encoded tap, want packet tap after encode in %+v", goav.StreamTapsForTest(intent.Streams[0]))
	}
	operations := intent.Streams[0].Operations
	if len(operations) != 4 ||
		operations[0].Kind != plan.OpDecode ||
		operations[1].Kind != plan.OpTap || operations[1].Tap.Name != "audio.decoded" ||
		operations[2].Kind != plan.OpEncode ||
		operations[3].Kind != plan.OpTap || operations[3].Tap.Name != "audio.encoded" ||
		operations[3].Tap.Domain != shape.DomainPacket ||
		operations[3].Tap.After != plan.OpEncode {
		t.Fatalf("operations: %+v", operations)
	}
}

func TestBranchCustomStageUsesOrderedOperations(t *testing.T) {
	meter := component.FrameFunc("meter", func(ctx context.Context, frame *av.Frame, emit component.Emit) error {
		return emit.Frame(frame)
	})
	web := goav.Write("web.webm", io.Discard)
	job := goav.From(goav.FileInput("source.webm", strings.NewReader(""))).
		Video().
		Decode().
		Tap(goav.FrameTap("video.decoded")).
		Branches(
			goav.Branch("v360").
				Resize(640, 360).
				Do(meter).
				Encode(codec.VP9(codec.Bitrate(600_000))).
				To(web),
		)

	intent := goav.JobPlanForTest(job)
	if len(intent.Streams) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
	want := []plan.OperationKind{plan.OpDecode, plan.OpTap, plan.OpTransform, plan.OpStage, plan.OpEncode}
	if !equalOperationKinds(operationSpecKinds(intent.Streams[0].Operations), want) {
		t.Fatalf("operations=%+v, want %+v", intent.Streams[0].Operations, want)
	}

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	for _, want := range []string{
		"decode-video -> resize-v360",
		"resize-v360 -> meter",
		"meter -> encode-v360",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("spec missing %q:\n%s", want, text)
		}
	}
}

func TestFlowAppliesFlowAtDeclarationPosition(t *testing.T) {
	meter := component.FrameFunc("meter", func(ctx context.Context, frame *av.Frame, emit component.Emit) error {
		return emit.Frame(frame)
	})
	inner := goav.Flow("inner").Audio().
		Do(meter).
		Tap(goav.FrameTap("audio.inner-meter")).
		Require(shape.Frame(av.MediaAudio)).
		Auto(shape.AllowResample())
	outer := goav.Flow("outer").Audio().
		Apply(inner).
		Resample(16_000, codec.Mono).
		Encode(codec.Opus(codec.Bitrate(32_000), codec.Channels(codec.Mono)))

	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		Apply(outer).
		To(goav.Write("voice.ogg", io.Discard))

	intent := goav.JobPlanForTest(job)
	if len(intent.Streams) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
	// The inner flow's operations splice at its Apply position inside the
	// outer flow, in declaration order: the explicit stream decode, the inner
	// stage/tap/require/auto, then the outer resample and encoder.
	operations := intent.Streams[0].Operations
	if len(operations) != 7 {
		t.Fatalf("operations: %+v", operations)
	}
	transform := goav.TransformViewForTestFrom(operations[5].Transform)
	if operations[0].Kind != plan.OpDecode ||
		operations[1].Kind != plan.OpStage || operations[1].Component != "meter" ||
		operations[2].Kind != plan.OpTap || operations[2].Tap.Name != "audio.inner-meter" ||
		operations[3].Kind != plan.OpShape || operations[3].Require == nil ||
		operations[4].Kind != plan.OpShape || operations[4].Auto == nil ||
		operations[5].Kind != plan.OpTransform || transform.Resample == nil ||
		operations[6].Kind != plan.OpEncode || operations[6].Encode.ID != av.CodecOpus {
		t.Fatalf("operations: %+v", operations)
	}
	taps := goav.StreamTapsForTest(intent.Streams[0])
	if len(taps) != 1 || taps[0].Name != "audio.inner-meter" {
		t.Fatalf("taps: %+v, want the nested flow's tap to survive the splice", taps)
	}
}

func TestFlowApplyMediaMismatchIsActionable(t *testing.T) {
	video := goav.Flow("thumbs").Video().Resize(320, 180)
	composed := goav.Flow("voice").Audio().Apply(video)

	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Apply(composed).
		To(goav.Write("voice.ogg", io.Discard)).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "flow_media_mismatch" {
		t.Fatalf("err = %v, want flow_media_mismatch with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "video flow cannot be applied to audio stream") ||
		!strings.Contains(err.Error(), "Flow(name).Audio") ||
		!strings.Contains(err.Error(), "Flow(name).Video") {
		t.Fatalf("err = %v, want flow-on-flow media guidance", err)
	}
}

func TestFlowMediaMismatchIsActionable(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Apply(goav.Flow("voice").Audio().Encode(codec.Opus(codec.Bitrate(32_000), codec.Channels(codec.Mono)))).
		To(goav.Write("voice.webm", io.Discard)).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "flow_media_mismatch" {
		t.Fatalf("err = %v, want flow_media_mismatch", err)
	}
	if !strings.Contains(err.Error(), "Flow(name).Audio") || !strings.Contains(err.Error(), "Flow(name).Video") {
		t.Fatalf("err = %v, want flow guidance", err)
	}
}

func TestFlowBranchMediaMismatchIsActionable(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Branches(
			goav.Branch("voice").
				Apply(goav.Flow("voice").Audio().Encode(codec.Opus(codec.Bitrate(32_000), codec.Channels(codec.Mono)))).
				To(goav.Write("voice.webm", io.Discard)),
		).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "flow_media_mismatch" {
		t.Fatalf("err = %v, want flow_media_mismatch with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "audio flow cannot be applied to video stream") ||
		!strings.Contains(err.Error(), "Flow(name).Audio") ||
		!strings.Contains(err.Error(), "Flow(name).Video") {
		t.Fatalf("err = %v, want branch flow media guidance", err)
	}
}

func TestBranchRejectsConflictingFlowMedia(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Audio().
		Branches(
			goav.Branch("mixed").
				Apply(goav.Flow("voice").Audio().Resample(16_000, codec.Mono)).
				Apply(goav.Flow("preview").Video().Resize(640, 360)).
				To(goav.Write("mixed.webm", io.Discard)),
		).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "flow_media_mismatch" {
		t.Fatalf("err = %v, want flow_media_mismatch with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "video flow cannot be applied to audio stream") ||
		!strings.Contains(err.Error(), "Flow(name).Audio") ||
		!strings.Contains(err.Error(), "Flow(name).Video") {
		t.Fatalf("err = %v, want conflicting flow media guidance", err)
	}
}

func TestFlowBranchSnapshotsBuilderState(t *testing.T) {
	flow := goav.Flow("voice").
		Audio().
		Resample(16_000, codec.Mono).
		Encode(codec.Opus(codec.Bitrate(32_000), codec.Channels(codec.Mono)))
	branch := goav.Branch("voice").
		Apply(flow).
		To(goav.Write("voice.ogg", io.Discard))

	flow.Resample(8_000, codec.Mono)
	job := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Audio().
		Branches(branch)

	intent := goav.JobPlanForTest(job)
	if len(intent.Streams) != 1 ||
		len(transformOperationsForTest(intent.Streams[0].Operations)) != 1 ||
		transformOperationsForTest(intent.Streams[0].Operations)[0].Resample.SampleRate != 16_000 {
		t.Fatalf("intent after mutating flow: %+v", intent)
	}
}

func TestFlowDecodeAppliesToPacketBranchIntent(t *testing.T) {
	flow := goav.Flow("voice").
		Audio().
		Decode().
		Resample(16_000, codec.Mono).
		Encode(codec.Opus(codec.Bitrate(64_000)))
	target := goav.Sink(component.SinkFunc("voice", func(context.Context, component.Message) error {
		return nil
	}))

	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Copy().
		Branches(goav.Branch("voice").Apply(flow).To(target))

	intent := goav.JobPlanForTest(job)
	if len(intent.Streams) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
	stream := intent.Streams[0]
	if stream.Name != "voice" ||
		!goav.StreamHasDecodeForTest(stream) ||
		len(transformOperationsForTest(stream.Operations)) != 1 ||
		transformOperationsForTest(stream.Operations)[0].Resample.SampleRate != 16_000 ||
		goav.StreamEncodeForTest(stream).ID != av.CodecOpus ||
		goav.StreamEncodeForTest(stream).Settings.Bitrate != 64_000 ||
		len(stream.Destinations) != 1 ||
		stream.Destinations[0] != "voice" {
		t.Fatalf("stream intent: %+v", stream)
	}
	want := []plan.OperationKind{plan.OpDecode, plan.OpTransform, plan.OpEncode}
	if !equalOperationKinds(operationSpecKinds(stream.Operations), want) {
		t.Fatalf("operations=%+v, want %+v", stream.Operations, want)
	}
}

func TestFlowDecodeAppliesToStreamRecipeIntent(t *testing.T) {
	flow := goav.Flow("preview").
		Audio().
		Decode().
		Tap(goav.FrameTap("audio.flow.decoded"))

	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Apply(flow).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		})))

	intent := goav.JobPlanForTest(job)
	if len(intent.Streams) != 1 || len(intent.Destinations) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
	stream := intent.Streams[0]
	if stream.Name != "audio" ||
		!goav.StreamHasDecodeForTest(stream) ||
		goav.StreamEncodeForTest(stream).ID != "" ||
		len(transformOperationsForTest(stream.Operations)) != 0 ||
		len(stream.Destinations) != 1 ||
		stream.Destinations[0] != "frames" {
		t.Fatalf("stream intent: %+v", stream)
	}
	want := []plan.OperationKind{plan.OpDecode, plan.OpTap}
	if !equalOperationKinds(operationSpecKinds(stream.Operations), want) {
		t.Fatalf("operations=%+v, want %+v", stream.Operations, want)
	}
	if len(goav.StreamTapsForTest(stream)) != 1 ||
		goav.StreamTapsForTest(stream)[0].Name != "audio.flow.decoded" ||
		goav.StreamTapsForTest(stream)[0].Domain != shape.DomainFrame ||
		goav.StreamTapsForTest(stream)[0].MediaKind != av.MediaAudio ||
		goav.StreamTapsForTest(stream)[0].After != plan.OpDecode {
		t.Fatalf("taps: %+v", goav.StreamTapsForTest(stream))
	}
}

func TestFlowDecodeRejectsAfterStreamDecode(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		Apply(goav.Flow("voice").Audio().Decode()).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "flow_decode_domain_mismatch" {
		t.Fatalf("err = %v, want flow_decode_domain_mismatch with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "requires a packet-domain stream point") ||
		!strings.Contains(err.Error(), "after stream decode") {
		t.Fatalf("err = %v, want flow decode domain guidance", err)
	}
}

func TestFlowDecodeMustBeFirstOperation(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Apply(goav.Flow("voice").
			Audio().
			Resample(16_000, codec.Mono).
			Decode()).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "flow_decode_order_invalid" {
		t.Fatalf("err = %v, want flow_decode_order_invalid with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "decode must be the first flow operation") ||
		!strings.Contains(err.Error(), ".Decode().Resample") {
		t.Fatalf("err = %v, want flow decode order guidance", err)
	}
}

func TestFlowCopyRequiresPacketDomain(t *testing.T) {
	tests := []struct {
		name string
		job  *goav.Job
	}{
		{
			name: "after flow frame operation",
			job: goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
				Audio().
				Apply(goav.Flow("packets").
					Audio().
					Resample(16_000, codec.Mono).
					Copy()).
				To(goav.Write("copy.ogg", io.Discard)),
		},
		{
			name: "after stream decode",
			job: goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
				Audio().
				Decode().
				Apply(goav.Flow("packets").Audio().Copy()).
				To(goav.Write("copy.ogg", io.Discard)),
		},
		{
			name: "after branch frame operation",
			job: goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
				Audio().
				Branches(goav.Branch("copy").
					Decode().
					Apply(goav.Flow("packets").Audio().Copy()).
					To(goav.Write("copy.ogg", io.Discard))),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.job.Describe()
			var buildErr *goav.BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != "flow_copy_domain_mismatch" {
				t.Fatalf("err = %v, want flow_copy_domain_mismatch with matching BuildError code", err)
			}
			if !strings.Contains(err.Error(), "requires a packet-domain stream point") ||
				!strings.Contains(err.Error(), ".Copy().Tap") {
				t.Fatalf("err = %v, want flow copy domain guidance", err)
			}
		})
	}
}

func TestNilFlowIsActionable(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Apply(nil).
		To(goav.Write("voice.ogg", io.Discard)).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "flow_invalid" {
		t.Fatalf("err = %v, want flow_invalid", err)
	}
}

func TestNilFlowBranchIsActionable(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Branches(goav.Branch("voice").Apply(nil).To(goav.Write("voice.ogg", io.Discard))).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "flow_invalid" {
		t.Fatalf("err = %v, want flow_invalid", err)
	}
}

func TestBranchesRejectOuterOutputsAndDuplicateDestinations(t *testing.T) {
	voice := goav.Flow("voice").Audio().Encode(codec.Opus(codec.Bitrate(32_000), codec.Channels(codec.Mono)))
	voiceOut := goav.Write("voice.ogg", io.Discard)

	_, err := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Audio().
		Branches(goav.Branch("voice").Apply(voice).To(voiceOut)).
		To(goav.Write("ignored.ogg", io.Discard)).
		Describe()
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_scope_mixed" {
		t.Fatalf("err = %v, want output_scope_mixed", err)
	}

	_, err = goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Audio().
		Branches(goav.Branch("voice").Apply(voice).To(voiceOut, voiceOut)).
		Describe()
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_duplicate" {
		t.Fatalf("err = %v, want destination_duplicate", err)
	}
}

func TestFlowRejectsNonTapOperationsAfterEncode(t *testing.T) {
	tests := []struct {
		name  string
		build func() (pipeline.Spec, error)
		want  string
	}{
		{
			name: "transform",
			build: func() (pipeline.Spec, error) {
				return goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
					Audio().
					Apply(goav.Flow("voice").
						Audio().
						Encode(codec.Opus(codec.Bitrate(32_000), codec.Channels(codec.Mono))).
						Resample(16_000, codec.Mono)).
					To(goav.Write("voice.ogg", io.Discard)).
					Describe()
			},
			want: "resample",
		},
		{
			name: "stage",
			build: func() (pipeline.Spec, error) {
				return goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
					Audio().
					Apply(goav.Flow("voice").
						Audio().
						Encode(codec.Opus(codec.Bitrate(32_000), codec.Channels(codec.Mono))).
						Do(component.FrameFunc("meter", func(context.Context, *av.Frame, component.Emit) error {
							return nil
						}))).
					To(goav.Write("voice.ogg", io.Discard)).
					Describe()
			},
			want: "custom stage",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.build()

			var buildErr *goav.BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != "stream_step_after_encode" {
				t.Fatalf("err = %v, want stream_step_after_encode", err)
			}
			if buildErr.Operation != "build flow" || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want flow %s guidance", err, tt.want)
			}
		})
	}
}

func TestRecipeAndRejectsMultipleFileInputs(t *testing.T) {
	_, err := goav.From(goav.FileInput("a.ivf", strings.NewReader(""))).
		And(goav.FileInput("b.ivf", strings.NewReader(""))).
		To(goav.Write("out.ivf", io.Discard)).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "multi_input_unsupported" {
		t.Fatalf("err = %v, want multi_input_unsupported with matching BuildError code", err)
	}
}

// TestMediaOptionsConfigureBothDirections pins the direction-agnostic option
// vocabulary: the same goav.MIME / goav.Name / goav.Metadata values configure
// input and destination constructors, while Format stays destination-typed.
func TestMediaOptionsConfigureBothDirections(t *testing.T) {
	job := goav.From(goav.FileInput("in", strings.NewReader(""),
		goav.Name("mic"),
		goav.MIME("audio/ogg"),
		goav.Metadata(av.Metadata{"session": "demo"}),
	)).Copy().To(goav.Write("out", io.Discard,
		goav.Name("recording.ogg"),
		goav.MIME("audio/ogg"),
		goav.Metadata(av.Metadata{"session": "demo"}),
		goav.Format(av.FormatOgg),
	))

	intent := goav.JobPlanForTest(job)
	if len(intent.Inputs) != 1 || len(intent.Destinations) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
	if intent.Inputs[0].Name != "mic" || intent.Inputs[0].MIMEType != "audio/ogg" {
		t.Fatalf("input intent: %+v", intent.Inputs[0])
	}
	if intent.Destinations[0].Name != "recording.ogg" || intent.Destinations[0].MIMEType != "audio/ogg" || intent.Destinations[0].Format != av.FormatOgg {
		t.Fatalf("destination intent: %+v", intent.Destinations[0])
	}
}

// TestWithLayersOptionsOntoConstructedValues pins the late-config seam: With
// applies the same option vocabulary to an already-constructed input or
// destination value and returns a configured copy.
func TestWithLayersOptionsOntoConstructedValues(t *testing.T) {
	input := goav.FileInput("in.ogg", strings.NewReader("")).With(goav.Name("mic"))
	output := goav.Write("out.ogg", io.Discard).With(goav.MIME("audio/ogg"))

	intent := goav.JobPlanForTest(goav.From(input).Copy().To(output))
	if len(intent.Inputs) != 1 || intent.Inputs[0].Name != "mic" {
		t.Fatalf("input intent: %+v", intent.Inputs)
	}
	if len(intent.Destinations) != 1 || intent.Destinations[0].MIMEType != "audio/ogg" {
		t.Fatalf("destination intent: %+v", intent.Destinations)
	}
}

func TestJobCopyRecordsExplicitIntent(t *testing.T) {
	input := goav.Source("packets",
		shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48_000, codec.Stereo, "")),
		func(context.Context, source.Push) error { return nil },
	)
	output := goav.Sink(component.SinkFunc("out-packets", func(context.Context, component.Message) error { return nil }))

	explicit := goav.JobPlanForTest(goav.From(input).Copy().To(output))
	if !explicit.Copy {
		t.Fatalf("explicit copy intent = %+v, want Copy marker", explicit)
	}
	implicit := goav.JobPlanForTest(goav.From(input).To(output))
	if implicit.Copy {
		t.Fatalf("implicit copy intent = %+v, Copy marker should only follow Job.Copy", implicit)
	}
}

func TestJobCopyAppearsInExplain(t *testing.T) {
	input := goav.Source("packets",
		shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48_000, codec.Stereo, "")),
		func(context.Context, source.Push) error { return nil },
	)
	output := goav.Sink(component.SinkFunc("out-packets", func(context.Context, component.Message) error { return nil }))

	explicit, err := goav.From(input).Copy().To(output).Explain(context.Background())
	if err != nil {
		t.Fatalf("explicit Explain(): %v", err)
	}
	if !reportHasOperationDetail(explicit, "explicit packet copy") {
		t.Fatalf("explicit copy report should expose explicit packet copy detail: %+v", explicit.Branches)
	}

	implicit, err := goav.From(input).To(output).Explain(context.Background())
	if err != nil {
		t.Fatalf("implicit Explain(): %v", err)
	}
	if reportHasOperationDetail(implicit, "explicit packet copy") {
		t.Fatalf("implicit copy report should not look like Job.Copy was called: %+v", implicit.Branches)
	}
}

func reportHasOperationDetail(report plan.Report, detail string) bool {
	for i := range report.Streams {
		for j := range report.Streams[i].Operations {
			if report.Streams[i].Operations[j].Detail == detail {
				return true
			}
		}
	}
	for i := range report.Branches {
		for j := range report.Branches[i].Operations {
			if report.Branches[i].Operations[j].Detail == detail {
				return true
			}
		}
	}
	return false
}

func TestRecordRecipeRejectsEmptyInputSpec(t *testing.T) {
	_, err := recordJob(
		goav.InputSpec{},
		goav.Write("recording.ogg", io.Discard),
	).Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "input_invalid" {
		t.Fatalf("err = %v, want input_invalid with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "empty input spec") ||
		!strings.Contains(err.Error(), "goav.FileInput") {
		t.Fatalf("err = %v, want input constructor guidance", err)
	}
}

func TestDecodeRecipeRejectsNilSinkDestination(t *testing.T) {
	_, err := decodeJob(
		goav.FileInput("input.ogg", strings.NewReader("")),
		goav.Sink(nil),
	).Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_invalid" {
		t.Fatalf("err = %v, want output_invalid wrapping errNilSink", err)
	}
	if !strings.Contains(err.Error(), "non-nil sink") {
		t.Fatalf("err = %v, want sink destination guidance", err)
	}
}

func TestDecodeRecipeRejectsNilSinkFuncCallback(t *testing.T) {
	_, err := decodeJob(
		goav.FileInput("input.ogg", strings.NewReader("")),
		goav.Sink(component.SinkFunc("frames", nil)),
	).Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_invalid" {
		t.Fatalf("err = %v, want output_invalid wrapping errNilSink", err)
	}
	if !strings.Contains(err.Error(), "non-nil sink") {
		t.Fatalf("err = %v, want sink guidance", err)
	}
}

func TestDecodeRecipeRejectsMuxOutput(t *testing.T) {
	_, err := decodeJob(
		goav.FileInput("input.ogg", strings.NewReader("")),
		goav.Write("frames.ogg", io.Discard),
	).Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_missing" {
		t.Fatalf("err = %v, want encode_missing with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "goav.Sink") ||
		!strings.Contains(err.Error(), ".Copy().To(output)") {
		t.Fatalf("err = %v, want decode output guidance", err)
	}
}

func TestRecordRecipeRejectsMissingOutput(t *testing.T) {
	_, err := recordJob(
		goav.FileInput("input.ogg", strings.NewReader("")),
	).Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_missing" {
		t.Fatalf("err = %v, want output_missing", err)
	}
}

func TestRecordRecipeRejectsEmptyDestination(t *testing.T) {
	var target goav.Destination
	_, err := recordJob(
		goav.FileInput("input.ogg", strings.NewReader("")),
		target,
	).Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_invalid" {
		t.Fatalf("err = %v, want destination_invalid with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "destination is empty") ||
		!strings.Contains(err.Error(), "goav.Write") ||
		!strings.Contains(err.Error(), "goav.Sink") {
		t.Fatalf("err = %v, want destination constructor guidance", err)
	}
}

func TestRecordRecipeRejectsFileWithoutWriter(t *testing.T) {
	_, err := recordJob(
		goav.FileInput("input.ogg", strings.NewReader("")),
		goav.Write("recording.ogg", nil),
	).Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_writer_missing" {
		t.Fatalf("err = %v, want output_writer_missing with matching BuildError code", err)
	}
}

func TestRecordRecipeRejectsUnnamedFileWithoutFormat(t *testing.T) {
	_, err := recordJob(
		goav.FileInput("input.ivf", strings.NewReader("")),
		goav.Write("", io.Discard),
	).Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_format_missing" {
		t.Fatalf("err = %v, want output_format_missing with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "explicit format") ||
		!strings.Contains(err.Error(), "container extension") {
		t.Fatalf("err = %v, want format guidance", err)
	}
}

func TestRecordRecipeRejectsFormatOnlyDestination(t *testing.T) {
	_, err := recordJob(
		goav.FileInput("input.ivf", strings.NewReader("")),
		goav.URI("", goav.Format(av.FormatIVF)),
	).Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_destination_missing" {
		t.Fatalf("err = %v, want output_destination_missing with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "no URI, writer, or sink") ||
		!strings.Contains(err.Error(), "goav.Write") {
		t.Fatalf("err = %v, want output destination guidance", err)
	}
}

func TestRecordRecipeReportsMissingInputDemuxer(t *testing.T) {
	_, err := recordJob(
		goav.FileInput("input.ogg", strings.NewReader("")),
		goav.Sink(component.SinkFunc("packets", func(context.Context, component.Message) error { return nil })),
	).UseRuntime(mustRuntime()).Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "input_demuxer_missing" {
		t.Fatalf("err = %v, want input_demuxer_missing", err)
	}
	if !strings.Contains(err.Error(), `format "ogg"`) ||
		!strings.Contains(err.Error(), "no demuxer is registered") ||
		!strings.Contains(err.Error(), "WithFormatAdapter") {
		t.Fatalf("err = %v, want demuxer adapter guidance", err)
	}
}

func TestRecordRecipeReportsMissingDestinationMuxer(t *testing.T) {
	_, err := recordJob(
		goav.FileInput("input.ivf", bytes.NewReader(tinyIVF())),
		goav.Write("recording.mp4", io.Discard),
	).UseRuntime(mustRuntime()).Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_muxer_missing" {
		t.Fatalf("err = %v, want destination_muxer_missing", err)
	}
	if buildErr.Operation != "open destination" {
		t.Fatalf("operation = %q, want open destination", buildErr.Operation)
	}
	if !strings.Contains(err.Error(), `format "mp4"`) ||
		!strings.Contains(err.Error(), "no muxer is registered") ||
		!strings.Contains(err.Error(), ".ivf") {
		t.Fatalf("err = %v, want muxer adapter guidance", err)
	}
}

func TestRecordRecipeRejectsDuplicateOutputs(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ivf", strings.NewReader(""))).
		To(
			goav.Write("recording.ivf", io.Discard),
			goav.Write("recording.ivf", io.Discard),
		).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_duplicate" {
		t.Fatalf("err = %v, want output_duplicate with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), `output name "recording.ivf"`) ||
		!strings.Contains(err.Error(), "unique output name") {
		t.Fatalf("err = %v, want duplicate output guidance", err)
	}
}

func TestStreamRecipeRejectsDuplicateSinkDestinations(t *testing.T) {
	sink := func(context.Context, component.Message) error { return nil }
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		To(
			goav.Sink(component.SinkFunc("frames", sink)),
			goav.Sink(component.SinkFunc("frames", sink)),
		).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_duplicate" {
		t.Fatalf("err = %v, want output_duplicate with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), `output name "frames"`) ||
		!strings.Contains(err.Error(), ".Name") {
		t.Fatalf("err = %v, want duplicate sink guidance", err)
	}
}

func TestStreamRecipeRejectsDuplicateTypedDestinations(t *testing.T) {
	target := goav.Write("voice.ogg", io.Discard)
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(mustRuntime()).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(target, target).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_duplicate" {
		t.Fatalf("err = %v, want output_duplicate with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), `output name "voice.ogg"`) {
		t.Fatalf("err = %v, want duplicate destination guidance", err)
	}
}

func TestProviderRecipeRejectsNilProvider(t *testing.T) {
	_, err := recordJob(
		goav.Input(nil),
		goav.Write("recording.ogg", io.Discard),
	).Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "input_invalid" {
		t.Fatalf("err = %v, want input_invalid wrapping errNilSource", err)
	}
}

func TestReadmeAudioEncodeRecipeIsSmall(t *testing.T) {
	meter := component.FrameFunc("meter", func(ctx context.Context, frame *av.Frame, emit component.Emit) error {
		return emit.Frame(frame)
	})
	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		Do(meter).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(goav.Write("archive.ogg", io.Discard))

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "decode-audio -> meter") ||
		!strings.Contains(text, "meter -> encode-audio") ||
		!strings.Contains(text, "encode-audio -> archive.ogg") {
		t.Fatalf("spec:\n%s", text)
	}
}

func TestReadmeAudioResampleEncodeRecipeIsSmall(t *testing.T) {
	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		Resample(16_000, codec.Mono).
		Encode(codec.Opus(codec.Bitrate(48_000))).
		To(goav.Write("preview.ogg", io.Discard))

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "decode-audio -> resample-audio") ||
		!strings.Contains(text, "resample-audio -> encode-audio") ||
		!strings.Contains(text, "16000 Hz") ||
		!strings.Contains(text, "1 ch") {
		t.Fatalf("spec:\n%s", text)
	}
	intent := goav.JobPlanForTest(job)
	if len(intent.Streams) != 1 || len(transformOperationsForTest(intent.Streams[0].Operations)) != 1 || transformOperationsForTest(intent.Streams[0].Operations)[0].Resample == nil {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestReadmeVideoResizeEncodeRecipeIsSmall(t *testing.T) {
	job := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Decode().
		Resize(1280, 720).
		Encode(codec.VP9(codec.Bitrate(2_000_000))).
		To(goav.Write("preview.webm", io.Discard))

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "decode-video -> resize-video") ||
		!strings.Contains(text, "resize-video -> encode-video") ||
		!strings.Contains(text, "1280x720") {
		t.Fatalf("spec:\n%s", text)
	}
	intent := goav.JobPlanForTest(job)
	if len(intent.Streams) != 1 || len(transformOperationsForTest(intent.Streams[0].Operations)) != 1 || transformOperationsForTest(intent.Streams[0].Operations)[0].Resize == nil {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestStreamRecipeExplicitDecodeOperationsReachFrameDomain(t *testing.T) {
	sink := component.SinkFunc("frames", func(context.Context, component.Message) error {
		return nil
	})
	meter := component.FrameFunc("meter", func(ctx context.Context, frame *av.Frame, emit component.Emit) error {
		return emit.Frame(frame)
	})
	tests := []struct {
		name string
		job  *goav.Job
	}{
		{
			name: "sink destination",
			job: goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
				Audio().
				Decode().
				To(goav.Sink(sink)),
		},
		{
			name: "custom stage",
			job: goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
				Audio().
				Decode().
				Do(meter).
				Encode(codec.Opus(codec.Bitrate(96_000))).
				To(goav.Write("archive.ogg", io.Discard)),
		},
		{
			name: "resample",
			job: goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
				Audio().
				Decode().
				Resample(16_000, codec.Mono).
				Encode(codec.Opus(codec.Bitrate(48_000))).
				To(goav.Write("preview.ogg", io.Discard)),
		},
		{
			name: "resize",
			job: goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
				Video().
				Decode().
				Resize(1280, 720).
				Encode(codec.VP9(codec.Bitrate(2_000_000))).
				To(goav.Write("preview.webm", io.Discard)),
		},
		{
			name: "encoder",
			job: goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
				Audio().
				Decode().
				Encode(codec.Opus(codec.Bitrate(96_000))).
				To(goav.Write("archive.ogg", io.Discard)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			intent := goav.JobPlanForTest(tt.job)
			if len(intent.Streams) != 1 || !goav.StreamHasDecodeForTest(intent.Streams[0]) {
				t.Fatalf("intent: %+v", intent)
			}
		})
	}
}

func TestStreamRecipeRequiresOperationForMuxOutput(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		To(goav.Write("archive.ogg", io.Discard)).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_operation_missing" {
		t.Fatalf("err = %v, want stream_operation_missing with matching BuildError code", err)
	}
}

func TestStreamRecipeRequiresExplicitDecodeBeforeFrameConsumers(t *testing.T) {
	meter := component.FrameFunc("meter", func(ctx context.Context, frame *av.Frame, emit component.Emit) error {
		return emit.Frame(frame)
	})
	voiceFlow := goav.Flow("voice").Audio().
		Resample(16_000, codec.Mono).
		Encode(codec.Opus(codec.Bitrate(32_000), codec.Channels(codec.Mono)))
	tests := []struct {
		name string
		job  *goav.Job
		fix  string
	}{
		{
			name: "custom stage",
			job: goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
				Audio().
				Do(meter).
				To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error { return nil }))),
			fix: ".Decode().Do(...)",
		},
		{
			name: "flow",
			job: goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
				Audio().
				Apply(voiceFlow).
				To(goav.Write("voice.ogg", io.Discard)),
			fix: ".Decode().Apply(...)",
		},
		{
			name: "resize",
			job: goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
				Video().
				Resize(1280, 720).
				To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error { return nil }))),
			fix: ".Decode().Resize(...)",
		},
		{
			name: "resample",
			job: goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
				Audio().
				Resample(16_000, codec.Mono).
				To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error { return nil }))),
			fix: ".Decode().Resample(...)",
		},
		{
			name: "encode",
			job: goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
				Audio().
				Encode(codec.Opus(codec.Bitrate(96_000))).
				To(goav.Write("archive.ogg", io.Discard)),
			fix: ".Decode().Encode(...)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.job.Build(context.Background())
			var buildErr *goav.BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != "operation_shape_mismatch" {
				t.Fatalf("err = %v, want operation_shape_mismatch with matching BuildError code", err)
			}
			if !strings.Contains(err.Error(), "needs decoded frames") || !strings.Contains(err.Error(), tt.fix) {
				t.Fatalf("err = %v, want explicit decode guidance %q", err, tt.fix)
			}
		})
	}
}

func TestStreamRecipeRequiresExplicitDomainForPacketStreamSink(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error { return nil }))).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "operation_shape_mismatch" {
		t.Fatalf("err = %v, want operation_shape_mismatch with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), ".Decode().To(goav.Sink(...))") ||
		!strings.Contains(err.Error(), ".Copy().To(goav.Sink(...))") {
		t.Fatalf("err = %v, want explicit sink-domain guidance", err)
	}
}

func TestStreamRecipeRejectsGenericAndStreamOutputs(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		To(goav.Write("archive.ogg", io.Discard)).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(goav.Write("preview.ogg", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_scope_mixed" {
		t.Fatalf("err = %v, want output_scope_mixed with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "stream recipes use stream-local outputs") ||
		!strings.Contains(err.Error(), ".Copy().To") ||
		!strings.Contains(err.Error(), ".Branches") {
		t.Fatalf("err = %v, want output scope guidance", err)
	}
}

func TestStreamRecipeRejectsJobLevelOutput(t *testing.T) {
	job := goav.From(goav.FileInput("input.ogg", strings.NewReader("")))
	job.Audio()
	job.To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
		return nil
	})))
	_, err := job.Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_scope_mixed" {
		t.Fatalf("err = %v, want output_scope_mixed with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), ".Audio()...To(...)") {
		t.Fatalf("err = %v, want stream-local To guidance", err)
	}
}

func TestStreamRecipeRejectsSecondStreamSelectionBeforeRouting(t *testing.T) {
	// A new chain may only start once the previous one is routed with .To(...);
	// routed chains may stack (multi-stream jobs share one destination).
	job := goav.From(goav.FileInput("input.webm", strings.NewReader("")))
	job.Audio()
	job.Video()
	_, err := job.Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_duplicate" {
		t.Fatalf("err = %v, want stream_duplicate with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "first stream: audio") ||
		!strings.Contains(err.Error(), "second stream: video") ||
		!strings.Contains(err.Error(), ".Branches") {
		t.Fatalf("err = %v, want duplicate stream guidance", err)
	}
}

func TestStreamRecipeRejectsNegativeStreamIndex(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio(goav.StreamIndex(-1)).
		Decode().
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_selector_invalid" {
		t.Fatalf("err = %v, want stream_selector_invalid with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "stream index must be non-negative") ||
		!strings.Contains(err.Error(), "index=-1") ||
		!strings.Contains(err.Error(), "goav.StreamIndex(0)") {
		t.Fatalf("err = %v, want stream index guidance", err)
	}
}

func TestStreamRecipeRejectsNilCustomStage(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Do(nil).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stage_missing" {
		t.Fatalf("err = %v, want stage_missing wrapping errNilStage", err)
	}
	if !strings.Contains(err.Error(), ".Do(stage)") ||
		!strings.Contains(err.Error(), "component.FrameFunc") {
		t.Fatalf("err = %v, want custom stage guidance", err)
	}
}

func TestNilPacketFuncDoesNotBecomeSilentNilStage(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Do(component.PacketFunc("packets", nil)).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stage_missing" {
		t.Fatalf("err = %v, want stage_missing wrapping errNilStage", err)
	}
	if !strings.Contains(err.Error(), "component.PacketFunc") ||
		!strings.Contains(err.Error(), "non-nil stage") {
		t.Fatalf("err = %v, want PacketFunc nil-callback guidance", err)
	}
}

func TestNilSinkFuncDoesNotBecomeSilentNilSink(t *testing.T) {
	_, err := decodeJob(
		goav.FileInput("input.ogg", strings.NewReader("")),
		goav.Sink(component.SinkFunc("frames", nil)),
	).Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_invalid" {
		t.Fatalf("err = %v, want output_invalid wrapping errNilSink", err)
	}
	if !strings.Contains(err.Error(), "SinkFunc") ||
		!strings.Contains(err.Error(), "non-nil sink") {
		t.Fatalf("err = %v, want SinkFunc nil-callback guidance", err)
	}
}

func TestStreamRecipeRejectsWrongMediaTransform(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		Resize(320, 180).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "transform_media_mismatch" {
		t.Fatalf("err = %v, want transform_media_mismatch", err)
	}
}

func TestStreamRecipeRejectsInvalidResize(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Decode().
		Resize(0, 720).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "transform_invalid" {
		t.Fatalf("err = %v, want transform_invalid with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "positive width and height") ||
		!strings.Contains(err.Error(), "width=0") {
		t.Fatalf("err = %v, want resize value guidance", err)
	}
}

func TestStreamRecipeRequiresEncoderForFile(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		Resample(48_000, codec.Stereo).
		To(goav.Write("archive.ogg", io.Discard)).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_missing" {
		t.Fatalf("err = %v, want encode_missing with matching BuildError code", err)
	}
}

func TestStreamRecipeRejectsMixedSinkAndFile(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		To(
			goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
				return nil
			})),
			goav.Write("archive.ogg", io.Discard),
		).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_kind_mixed" {
		t.Fatalf("err = %v, want output_kind_mixed with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "cannot mix sinks and muxed outputs") ||
		!strings.Contains(err.Error(), ".Branches") {
		t.Fatalf("err = %v, want mixed output guidance", err)
	}
}

func TestStreamRecipeAllowsEncodedMuxAndSinkDestinations(t *testing.T) {
	rt := mustRuntime(
		goavruntime.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{
				{Index: 0, ID: "audio", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}},
			}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
			registry.RegisterMuxer(av.FormatOgg, recipeAPIMuxerFactory{})
		}),
		goavruntime.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, recipeAPIEncoderFactory{})
		}),
	)

	spec, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(
			goav.Write("archive.ogg", io.Discard),
			goav.Sink(component.SinkFunc("packets", func(context.Context, component.Message) error {
				return nil
			})),
		).
		Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	for _, want := range []string{
		"encode-audio -> archive.ogg",
		"encode-audio -> packets",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("spec:\n%s\nwant %q", text, want)
		}
	}
}

func TestStreamRecipeRejectsProcessingAfterEncoder(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		Resample(16_000, codec.Mono).
		To(goav.Write("archive.ogg", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_step_after_encode" {
		t.Fatalf("err = %v, want stream_step_after_encode with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "step: resample") ||
		!strings.Contains(err.Error(), "encoder: opus") ||
		!strings.Contains(err.Error(), "before .Encode") {
		t.Fatalf("err = %v, want terminal encoder guidance", err)
	}
}

func TestStreamRecipeRejectsDuplicateEncoder(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		Encode(codec.VP9(codec.Bitrate(600_000))).
		To(goav.Write("archive.ogg", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_duplicate" {
		t.Fatalf("err = %v, want encode_duplicate with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "first encoder: opus") ||
		!strings.Contains(err.Error(), "second encoder: vp9") ||
		!strings.Contains(err.Error(), "one terminal encoder") {
		t.Fatalf("err = %v, want duplicate encoder guidance", err)
	}
}

func TestStreamRecipeAllowsRuntimeRegisteredRecipeEncoders(t *testing.T) {
	tests := []struct {
		name  string
		codec codec.CodecSpec
	}{
		{name: "h264", codec: codec.H264(codec.Bitrate(2_000_000))},
		{name: "av1", codec: codec.AV1(codec.Bitrate(2_000_000))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := goav.Source("frames",
				shape.Frame(av.MediaVideo, shape.Video(16, 16, av.PixelFormatI420)),
				func(_ context.Context, push source.Push) error { return push.EOS() },
			)
			rt := mustRuntime(
				goavruntime.WithMuxer(av.FormatIVF, recipeAPIMuxerFactory{}),
				goavruntime.WithEncoder(codec.Descriptor{ID: tt.codec.ID, Type: av.MediaVideo}, recipeAPIEncoderFactory{}),
			)
			_, err := goav.From(source).
				UseRuntime(rt).
				Video().
				Encode(tt.codec).
				To(goav.Write("archive.ivf", io.Discard, goav.Format(av.FormatIVF))).
				Describe()
			if err != nil {
				t.Fatalf("Describe() err = %v", err)
			}
		})
	}
}

func TestStreamRecipeReportsMissingCustomEncoder(t *testing.T) {
	rt := mustRuntime(goavruntime.WithFormatAdapter(func(registry *format.SimpleRegistry) {
		registry.RegisterMuxer(av.FormatOgg, recipeAPIMuxerFactory{})
	}))
	_, err := goav.From(goav.FileInput("input.wav", strings.NewReader(""))).
		UseRuntime(rt).
		Audio().
		Decode().
		Encode(codec.Codec("pcm", av.MediaAudio)).
		To(goav.Write("archive.ogg", io.Discard)).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_adapter_missing" || !errors.Is(err, codec.ErrNotFound) {
		t.Fatalf("err = %v, want encode_adapter_missing wrapping codec.ErrNotFound", err)
	}
	if !strings.Contains(err.Error(), "no encoder adapter") ||
		!strings.Contains(err.Error(), "codec=pcm") {
		t.Fatalf("err = %v, want custom encoder adapter guidance", err)
	}
}

func TestStreamRecipeRejectsNegativeEncodeBitrate(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(-1))).
		To(goav.Write("archive.ogg", io.Discard)).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_parameter_invalid" {
		t.Fatalf("err = %v, want encode_parameter_invalid with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "bitrate must be non-negative") ||
		!strings.Contains(err.Error(), "bitrate=-1") {
		t.Fatalf("err = %v, want bitrate guidance", err)
	}
}

func TestStreamRecipeRejectsInvalidCodecTimingOptions(t *testing.T) {
	tests := []struct {
		name string
		job  *goav.Job
		want string
	}{
		{
			name: "fps",
			job: goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
				Video().
				Decode().
				Encode(codec.VP9(codec.FPS(0))).
				To(goav.Write("preview.webm", io.Discard)),
			want: "encode FPS must be positive",
		},
		{
			name: "keyframe interval",
			job: goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
				Video().
				Decode().
				Encode(codec.VP9(codec.KeyframeInterval(-1))).
				To(goav.Write("preview.webm", io.Discard)),
			want: "encode keyframe interval must be non-negative",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.job.Build(context.Background())
			var buildErr *goav.BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != "encode_parameter_invalid" {
				t.Fatalf("err = %v, want encode_parameter_invalid with matching BuildError code", err)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestStreamRecipeRejectsInvalidEncodeSampleRate(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		Encode(codec.Opus(codec.SampleRate(0))).
		To(goav.Write("archive.ogg", io.Discard)).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_parameter_invalid" {
		t.Fatalf("err = %v, want encode_parameter_invalid with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "sample rate must be positive") ||
		!strings.Contains(err.Error(), "sample_rate=0") {
		t.Fatalf("err = %v, want sample-rate guidance", err)
	}
}

func TestStreamRecipeReportsMissingEncodeAdapterBeforeOpeningInput(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(bundle.MustNewFormats()).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(goav.Write("archive.ivf", io.Discard)).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_adapter_missing" || !errors.Is(err, codec.ErrNotFound) {
		t.Fatalf("err = %v, want encode_adapter_missing wrapping codec.ErrNotFound", err)
	}
	if !strings.Contains(err.Error(), "codec=opus") ||
		!strings.Contains(err.Error(), "Sink") ||
		strings.Contains(err.Error(), "cannot open input") ||
		strings.Contains(err.Error(), "input_demuxer_missing") {
		t.Fatalf("err = %v, want encode adapter guidance before input diagnostics", err)
	}
}

func TestStreamRecipeReportsProbedFileSelectionBeforeOpeningInput(t *testing.T) {
	streams := []av.Stream{
		{
			Index: 0,
			ID:    "eng",
			Name:  "English",
			Type:  av.MediaAudio,
			Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio},
		},
		{
			Index: 1,
			ID:    "spa",
			Name:  "Spanish",
			Type:  av.MediaAudio,
			Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio},
		},
	}
	demuxerOpened := false
	rt := mustRuntime(goavruntime.WithFormatAdapter(func(registry *format.SimpleRegistry) {
		registry.RegisterProber(recipeAPIStreamProber{streams: streams})
		registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{called: &demuxerOpened})
	}))

	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Audio().
		Decode().
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_ambiguous" {
		t.Fatalf("err = %v, want stream_ambiguous with matching BuildError code", err)
	}
	if demuxerOpened {
		t.Fatal("demuxer opened before known stream selection failed")
	}
	if !strings.Contains(err.Error(), "id=eng") ||
		!strings.Contains(err.Error(), "id=spa") ||
		!strings.Contains(err.Error(), `.Audio(goav.StreamID("eng"))`) ||
		strings.Contains(err.Error(), "cannot open input") {
		t.Fatalf("err = %v, want probed stream-selection guidance before input diagnostics", err)
	}
}

func TestStreamRecipeReportsProbedFileMissingDecoderBeforeOpeningInput(t *testing.T) {
	streams := []av.Stream{{
		Index: 0,
		ID:    "audio",
		Type:  av.MediaAudio,
		Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio},
	}}
	demuxerOpened := false
	rt := mustRuntime(goavruntime.WithFormatAdapter(func(registry *format.SimpleRegistry) {
		registry.RegisterProber(recipeAPIStreamProber{streams: streams})
		registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{called: &demuxerOpened})
	}))

	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Audio().
		Decode().
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "decode_adapter_missing" || !errors.Is(err, codec.ErrNotFound) {
		t.Fatalf("err = %v, want decode_adapter_missing wrapping codec.ErrNotFound", err)
	}
	if demuxerOpened {
		t.Fatal("demuxer opened before known decoder preflight failed")
	}
	if !strings.Contains(err.Error(), "codec=opus") ||
		!strings.Contains(err.Error(), "goav.From") ||
		strings.Contains(err.Error(), "cannot open input") ||
		strings.Contains(err.Error(), "stream_ambiguous") {
		t.Fatalf("err = %v, want probed decoder guidance before input diagnostics", err)
	}
}

func TestStreamRecipeReportsMissingTransformAdapterBeforeOpeningInput(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(bundle.MustNewCodecs()).
		Audio().
		Decode().
		Resample(16_000, codec.Mono).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "transform_adapter_missing" || !errors.Is(err, filter.ErrNotFound) {
		t.Fatalf("err = %v, want transform_adapter_missing wrapping filter.ErrNotFound", err)
	}
	if !strings.Contains(err.Error(), "transform=resample") ||
		!strings.Contains(err.Error(), "bundle.MustNewFilters") ||
		strings.Contains(err.Error(), "input_demuxer_missing") ||
		strings.Contains(err.Error(), "cannot open input") {
		t.Fatalf("err = %v, want transform adapter guidance before input diagnostics", err)
	}
}

func TestStreamRecipeReportsIncompatibleTransformAdapterBeforeOpeningInput(t *testing.T) {
	streams := []av.Stream{{
		Index: 0,
		ID:    "audio",
		Type:  av.MediaAudio,
		Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio},
	}}
	demuxerOpened := false
	rt := mustRuntime(
		goavruntime.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: streams})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{called: &demuxerOpened})
		}),
		goavruntime.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, recipeAPIDecoderFactory{})
		}),
		goavruntime.WithFilterAdapter(func(registry *filter.SimpleRegistry) {
			registry.RegisterFactory(filter.Descriptor{
				Name:   filter.FactoryResample,
				Input:  av.MediaVideo,
				Output: av.MediaVideo,
			}, recipeAPIFilterFactory{})
		}),
	)
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Audio().
		Decode().
		Resample(16_000, codec.Mono).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "transform_adapter_incompatible" {
		t.Fatalf("err = %v, want transform_adapter_incompatible with matching BuildError code", err)
	}
	if demuxerOpened {
		t.Fatal("demuxer opened before transform adapter preflight failed")
	}
	if !strings.Contains(err.Error(), "transform=resample") ||
		!strings.Contains(err.Error(), "expected_input=audio") ||
		!strings.Contains(err.Error(), "actual_input=video") ||
		strings.Contains(err.Error(), "cannot open input") {
		t.Fatalf("err = %v, want transform adapter compatibility guidance before input diagnostics", err)
	}
}

func TestStreamRecipeRejectsUnresolvedEncodeIntents(t *testing.T) {
	tests := []struct {
		name string
		spec codec.CodecSpec
		code errcode.Code
	}{
		{name: "auto", spec: codec.Auto(), code: "encode_auto_unresolved"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
				Audio().
				Decode().
				Encode(tt.spec).
				To(goav.Write("archive.ogg", io.Discard)).
				Build(context.Background())
			var buildErr *goav.BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code {
				t.Fatalf("err = %v, want %s with matching BuildError code", err, tt.code)
			}
		})
	}
}

func TestDefaultRecordIVFRecipeRunShortcutRuns(t *testing.T) {
	var out bytes.Buffer
	if err := recordJob(
		goav.FileInput("input.ivf", bytes.NewReader(tinyIVF())),
		goav.Write("preview.ivf", &out),
	).UseRuntime(bundle.MustNew()).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if out.Len() == 0 {
		t.Fatal("empty output")
	}
}

func TestRecordRecipeDescribeMatchesBuiltGraph(t *testing.T) {
	var out bytes.Buffer
	job := recordJob(
		goav.FileInput("input.ivf", bytes.NewReader(tinyIVF())),
		goav.Write("preview.ivf", &out),
	).UseRuntime(bundle.MustNew())

	planned, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	task, err := job.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
}

func TestDefaultFromFanoutRecipeRunShortcutRuns(t *testing.T) {
	var recording bytes.Buffer
	var preview bytes.Buffer
	if err := goav.From(goav.FileInput("input.ivf", bytes.NewReader(tinyIVF()))).
		To(
			goav.Write("recording.ivf", &recording),
			goav.Write("preview.ivf", &preview),
		).
		UseRuntime(bundle.MustNew()).
		Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if recording.Len() == 0 || preview.Len() == 0 {
		t.Fatalf("recording=%d preview=%d, want both non-empty", recording.Len(), preview.Len())
	}
}

func TestFromFanoutRecipeDescribeMatchesBuiltGraph(t *testing.T) {
	var recording bytes.Buffer
	var preview bytes.Buffer
	job := goav.From(goav.FileInput("input.ivf", bytes.NewReader(tinyIVF()))).
		To(
			goav.Write("recording.ivf", &recording),
			goav.Write("preview.ivf", &preview),
		).
		UseRuntime(bundle.MustNew())

	planned, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	task, err := job.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
}

func TestDefaultRecordRecipeRunsWithExplicitUnnamedOutputFormat(t *testing.T) {
	var out bytes.Buffer
	job := recordJob(
		goav.FileInput("input.ivf", bytes.NewReader(tinyIVF())),
		goav.Write("", &out, goav.Format(av.FormatIVF)),
	).UseRuntime(bundle.MustNew())

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "input.ivf -> output") ||
		!strings.Contains(text, "format=ivf") {
		t.Fatalf("spec:\n%s", text)
	}

	task, err := job.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if out.Len() == 0 {
		t.Fatal("empty output")
	}
}

func TestReadmeTranscodeLadderRecipeIsSmall(t *testing.T) {
	web := goav.Write("web.webm", io.Discard)
	preview := goav.Write("preview.webm", io.Discard)
	job := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("720p").Resize(1280, 720).Encode(codec.VP9(codec.Bitrate(2_000_000))).To(web).
		Video("360p").Resize(640, 360).Encode(codec.VP9(codec.Bitrate(600_000))).To(preview)

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "resize, 1280x720") ||
		!strings.Contains(text, "codec=vp9") ||
		!strings.Contains(text, "web.webm") ||
		!strings.Contains(text, "preview.webm") {
		t.Fatalf("spec:\n%s", text)
	}
	if strings.Contains(text, "encode-720p -> preview.webm") ||
		strings.Contains(text, "encode-360p -> web.webm") {
		t.Fatalf("branch labels leaked:\n%s", text)
	}
	intent := goav.JobPlanForTest(job.materialize())
	if len(intent.Streams) != 2 || !goav.StreamHasDecodeForTest(intent.Streams[0]) || !goav.StreamHasDecodeForTest(intent.Streams[1]) {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestBranchRecipeComposesAudioAndVideoIntoSharedOutput(t *testing.T) {
	web := goav.Mux("out", goav.Write("out.webm", io.Discard))
	job := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("v360").Resize(640, 360).Encode(codec.VP9(codec.Bitrate(600_000))).To(web).
		Audio("a96").Resample(48_000, codec.Stereo).Encode(codec.Opus(codec.Bitrate(96_000))).To(web)

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	for _, want := range []string{
		"decode-video -> resize-v360",
		"resize-v360 -> encode-v360",
		"decode-audio -> resample-a96",
		"resample-a96 -> encode-a96",
		"encode-v360 -> out.webm",
		"encode-a96 -> out.webm",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("spec missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "decode-video -> resample-a96") ||
		strings.Contains(text, "decode-audio -> resize-v360") {
		t.Fatalf("audio/video decode paths crossed:\n%s", text)
	}
	intent := goav.JobPlanForTest(job.materialize())
	if len(intent.Streams) != 2 ||
		len(intent.Streams[0].Destinations) != 1 || intent.Streams[0].Destinations[0] != "out.webm" ||
		len(intent.Streams[1].Destinations) != 1 || intent.Streams[1].Destinations[0] != "out.webm" ||
		len(intent.Destinations) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestBranchRecipeSingleBranchUsesDestination(t *testing.T) {
	preview := goav.Write("preview.webm", io.Discard)
	job := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("360p").Resize(640, 360).Encode(codec.VP9(codec.Bitrate(600_000))).
		To(preview)

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "encode-360p -> preview.webm") {
		t.Fatalf("spec:\n%s", text)
	}
	intent := goav.JobPlanForTest(job.materialize())
	if len(intent.Streams) != 1 || len(intent.Streams[0].Destinations) != 1 || len(intent.Destinations) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestBranchRecipeRejectsDuplicateDestinations(t *testing.T) {
	web := goav.Write("web.webm", io.Discard)
	web2 := goav.Write("web.webm", io.Discard)
	_, err := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("720p").Encode(codec.VP9(codec.Bitrate(2_000_000))).To(web).
		Video("360p").Encode(codec.VP9(codec.Bitrate(600_000))).To(web2).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_duplicate" {
		t.Fatalf("err = %v, want destination_duplicate with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), `destination "web.webm"`) ||
		!strings.Contains(err.Error(), "goav.Mux(name, destination)") {
		t.Fatalf("err = %v, want duplicate destination guidance", err)
	}
}

func TestBranchRecipeRejectsDuplicateBranchDestinations(t *testing.T) {
	web := goav.Write("web.webm", io.Discard)
	_, err := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("720p").Encode(codec.VP9(codec.Bitrate(2_000_000))).To(web, web).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_duplicate" {
		t.Fatalf("err = %v, want destination_duplicate with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), `branch routes to destination "web.webm" more than once`) ||
		!strings.Contains(err.Error(), "second destination index: 1") ||
		!strings.Contains(err.Error(), "list each destination once") {
		t.Fatalf("err = %v, want duplicate branch destination guidance", err)
	}
}

func TestBranchRecipeRejectsDuplicateBranchNames(t *testing.T) {
	archive := goav.Write("archive.webm", io.Discard)
	preview := goav.Write("preview.webm", io.Discard)
	_, err := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("720p").Encode(codec.VP9(codec.Bitrate(2_000_000))).To(archive).
		Video("720p").Encode(codec.VP9(codec.Bitrate(1_000_000))).To(preview).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_duplicate" {
		t.Fatalf("err = %v, want stream_duplicate with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), `branch name "720p"`) ||
		!strings.Contains(err.Error(), "first branch index: 0") ||
		!strings.Contains(err.Error(), `.Video("360p")`) {
		t.Fatalf("err = %v, want duplicate branch guidance", err)
	}
}

func TestBranchRecipeRejectsMissingBranchName(t *testing.T) {
	web := goav.Write("web.webm", io.Discard)
	_, err := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("").Encode(codec.VP9(codec.Bitrate(2_000_000))).To(web).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_name_missing" {
		t.Fatalf("err = %v, want stream_name_missing with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "branches need stable names") ||
		!strings.Contains(err.Error(), `.Video("720p")`) ||
		!strings.Contains(err.Error(), "media type: video") {
		t.Fatalf("err = %v, want branch name guidance", err)
	}
}

func TestBranchRecipeRejectsInvalidDestination(t *testing.T) {
	_, err := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("360p").Encode(codec.VP9(codec.Bitrate(600_000))).
		To(goav.Write("preview.webm", nil)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_writer_missing" {
		t.Fatalf("err = %v, want output_writer_missing with matching BuildError code", err)
	}
}

func TestBranchCompositionAcceptsSinkDestination(t *testing.T) {
	job := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Decode().
		Branches(
			goav.Branch("preview").
				To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
					return nil
				}))),
		)

	intent := goav.JobPlanForTest(job)
	if len(intent.Streams) != 1 ||
		intent.Streams[0].Name != "preview" ||
		goav.StreamEncodeForTest(intent.Streams[0]).ID != "" ||
		!equalStrings(intent.Streams[0].Destinations, []string{"frames"}) {
		t.Fatalf("intent: %+v", intent)
	}

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "decode-video -> frames") ||
		strings.Contains(text, "encode-preview") {
		t.Fatalf("spec:\n%s", text)
	}
}

func TestBranchCompositionAcceptsSinkAfterResize(t *testing.T) {
	job := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Decode().
		Branches(
			goav.Branch("thumb").
				Resize(320, 180).
				To(goav.Sink(component.SinkFunc("thumbnail", func(context.Context, component.Message) error {
					return nil
				}))),
		)

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "decode-video -> resize-thumb") ||
		!strings.Contains(text, "resize-thumb -> thumbnail") ||
		strings.Contains(text, "encode-thumb") {
		t.Fatalf("spec:\n%s", text)
	}
}

func TestBranchCompositionSharesParentOperationBeforeBranches(t *testing.T) {
	web := goav.Write("web.webm", io.Discard)
	thumbnail := goav.Sink(component.SinkFunc("thumbnail", func(context.Context, component.Message) error {
		return nil
	}))
	job := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Decode().
		Resize(1280, 720).
		Tap(goav.FrameTap("video.720p.frames")).
		Branches(
			goav.Branch("web").
				Encode(codec.VP9(codec.Bitrate(2_000_000))).
				To(web),
			goav.Branch("thumb").
				Resize(320, 180).
				To(thumbnail),
		)

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	for _, want := range []string{
		"decode-video -> resize-video",
		"resize-video -> encode-web",
		"resize-video -> resize-thumb",
		"resize-thumb -> thumbnail",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("spec missing %q:\n%s", want, text)
		}
	}
	for _, duplicate := range []string{
		"decode-video -> resize-web",
		"decode-video -> resize-thumb",
	} {
		if strings.Contains(text, duplicate) {
			t.Fatalf("shared resize was duplicated as %q:\n%s", duplicate, text)
		}
	}
}

func TestExplainMarksSharedBranchOperations(t *testing.T) {
	rt := mustRuntime(
		goavruntime.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{{
				Index: 0,
				ID:    "video",
				Type:  av.MediaVideo,
				Codec: av.CodecParameters{
					ID:          av.CodecVP8,
					Type:        av.MediaVideo,
					Width:       1920,
					Height:      1080,
					PixelFormat: av.PixelFormatYUV420P,
				},
			}}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
			registry.RegisterMuxer(av.FormatOgg, recipeAPIMuxerFactory{})
		}),
		goavruntime.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP9, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
		}),
		goavruntime.WithFilterAdapter(func(registry *filter.SimpleRegistry) {
			registry.RegisterFactory(filter.Descriptor{Name: filter.FactoryResize, Input: av.MediaVideo, Output: av.MediaVideo}, recipeAPIFilterFactory{})
		}),
	)

	web := goav.Write("web.ogg", io.Discard)
	thumbnail := goav.Sink(component.SinkFunc("thumbnail", func(context.Context, component.Message) error {
		return nil
	}))
	report, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Video().
		Decode().
		Resize(1280, 720).
		Tap(goav.FrameTap("video.720p.frames")).
		Branches(
			goav.Branch("web").
				Encode(codec.VP9(codec.Bitrate(2_000_000))).
				To(web),
			goav.Branch("thumb").
				Resize(320, 180).
				To(thumbnail),
		).
		Explain(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	webBranch, ok := branchByName(report.Branches, "web")
	if !ok {
		t.Fatalf("branches=%+v, want web", report.Branches)
	}
	if countOperationReports(webBranch.Operations, plan.OpDecode, true) != 1 ||
		countOperationReports(webBranch.Operations, plan.OpTransform, true) != 1 ||
		countOperationReports(webBranch.Operations, plan.OpTap, true) != 1 ||
		countOperationReports(webBranch.Operations, plan.OpEncode, false) != 1 {
		t.Fatalf("web operations=%+v, want shared decode/resize/tap and private encode", webBranch.Operations)
	}

	thumbBranch, ok := branchByName(report.Branches, "thumb")
	if !ok {
		t.Fatalf("branches=%+v, want thumb", report.Branches)
	}
	if countOperationReports(thumbBranch.Operations, plan.OpDecode, true) != 1 ||
		countOperationReports(thumbBranch.Operations, plan.OpTransform, true) != 1 ||
		countOperationReports(thumbBranch.Operations, plan.OpTap, true) != 1 ||
		countOperationReports(thumbBranch.Operations, plan.OpTransform, false) != 1 {
		t.Fatalf("thumb operations=%+v, want shared parent work and private thumbnail resize", thumbBranch.Operations)
	}
}

func TestBranchCompositionCanSplitFromEarlierTap(t *testing.T) {
	web := goav.Write("web.webm", io.Discard)
	thumbnail := goav.Sink(component.SinkFunc("thumbnail", func(context.Context, component.Message) error {
		return nil
	}))
	job := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Decode().
		Tap(goav.FrameTap("video.decoded")).
		Resize(1280, 720).
		Tap(goav.FrameTap("video.720p.frames")).
		Branches(
			goav.Branch("raw-preview").
				From(goav.FrameTap("video.decoded")).
				Resize(320, 180).
				To(thumbnail),
			goav.Branch("web").
				From(goav.FrameTap("video.720p.frames")).
				Encode(codec.VP9(codec.Bitrate(2_000_000))).
				To(web),
		)

	intent := goav.JobPlanForTest(job)
	if len(intent.Streams) != 2 {
		t.Fatalf("intent streams = %+v, want 2", intent.Streams)
	}
	if intent.Streams[0].From.Name() != "video.decoded" ||
		intent.Streams[0].From.Domain() != shape.DomainFrame ||
		len(transformOperationsForTest(intent.Streams[0].Operations)) != 1 ||
		transformOperationsForTest(intent.Streams[0].Operations)[0].Resize.Width != 320 {
		t.Fatalf("raw branch intent = %+v, want branch from decoded tap with only thumbnail resize", intent.Streams[0])
	}
	if intent.Streams[1].From.Name() != "video.720p.frames" ||
		intent.Streams[1].From.Domain() != shape.DomainFrame ||
		len(transformOperationsForTest(intent.Streams[1].Operations)) != 1 ||
		transformOperationsForTest(intent.Streams[1].Operations)[0].Resize.Width != 1280 {
		t.Fatalf("web branch intent = %+v, want branch from 720p tap with shared resize", intent.Streams[1])
	}

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	for _, want := range []string{
		"decode-video -> resize-raw-preview",
		"decode-video -> resize-video",
		"resize-video -> encode-web",
		"resize-raw-preview -> thumbnail",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("spec missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "resize-video -> resize-raw-preview") ||
		strings.Contains(text, "decode-video -> encode-web") {
		t.Fatalf("branches ignored explicit typed tap anchors:\n%s", text)
	}
}

func TestBranchCompositionRejectsMissingPlannedTap(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Decode().
		Resize(1280, 720).
		Branches(
			goav.Branch("preview").
				From(goav.FrameTap("video.missing")).
				Resize(320, 180).
				To(goav.Sink(component.SinkFunc("preview", func(context.Context, component.Message) error {
					return nil
				}))),
		).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "branch_tap_missing" {
		t.Fatalf("err = %v, want branch_tap_missing with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), `Tap(goav.FrameTap("video.missing"))`) ||
		!strings.Contains(err.Error(), "current stream point") {
		t.Fatalf("err = %v, want planned tap guidance", err)
	}
}

func TestBranchCompositionRejectsGraphNodeSource(t *testing.T) {
	graphNode := expert.Graph(bundle.MustNew()).Stage("decode-video", component.PacketFunc("decode-video", func(ctx context.Context, packet *av.Packet, emit component.Emit) error {
		return emit.Packet(packet)
	}))
	_, err := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Decode().
		Branches(
			goav.Branch("preview").
				From(graphNode).
				Resize(320, 180).
				To(goav.Sink(component.SinkFunc("preview", func(context.Context, component.Message) error {
					return nil
				}))),
		).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "branch_source_invalid" {
		t.Fatalf("err = %v, want branch_source_invalid with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "graph handles") ||
		!strings.Contains(err.Error(), "From(goav.FrameTap") {
		t.Fatalf("err = %v, want planned branch source guidance", err)
	}
}

func TestBranchCompositionRejectsStreamEncodeBeforeBranches(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Decode().
		Encode(codec.VP9(codec.Bitrate(2_000_000))).
		Tap(goav.PacketTap("video.encoded")).
		Branches(
			goav.Branch("packets").
				To(goav.Sink(component.SinkFunc("packets", func(context.Context, component.Message) error {
					return nil
				}))),
		).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_branch_source_invalid" {
		t.Fatalf("err = %v, want encode_branch_source_invalid with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "stream encoders are terminal") ||
		!strings.Contains(err.Error(), "encoder: vp9") ||
		!strings.Contains(err.Error(), "Mutable.Attach") {
		t.Fatalf("err = %v, want planned parent encode guidance", err)
	}
}

func TestBranchCompositionSharesCurrentPointWithoutExplicitTap(t *testing.T) {
	web := goav.Write("web.webm", io.Discard)
	thumbnail := goav.Sink(component.SinkFunc("thumbnail", func(context.Context, component.Message) error {
		return nil
	}))
	job := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Decode().
		Resize(1280, 720).
		Branches(
			goav.Branch("web").
				Encode(codec.VP9(codec.Bitrate(2_000_000))).
				To(web),
			goav.Branch("thumb").
				Resize(320, 180).
				To(thumbnail),
		)

	intent := goav.JobPlanForTest(job)
	if len(intent.Streams) != 2 || intent.Streams[0].From.Name() != "" || intent.Streams[1].From.Name() != "" {
		t.Fatalf("intent streams = %+v, want unnamed current-point branch split", intent.Streams)
	}
	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	for _, want := range []string{
		"decode-video -> resize-video",
		"resize-video -> encode-web",
		"resize-video -> resize-thumb",
		"resize-thumb -> thumbnail",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("spec missing %q:\n%s", want, text)
		}
	}
	for _, duplicate := range []string{
		"decode-video -> resize-web",
		"decode-video -> resize-thumb",
	} {
		if strings.Contains(text, duplicate) {
			t.Fatalf("current-point resize was duplicated as %q:\n%s", duplicate, text)
		}
	}
}

func TestBranchCompositionSharesCustomStageCurrentPoint(t *testing.T) {
	meter := component.FrameFunc("meter", func(ctx context.Context, frame *av.Frame, emit component.Emit) error {
		return emit.Frame(frame)
	})
	packets := goav.Sink(component.SinkFunc("packets", func(context.Context, component.Message) error {
		return nil
	}))
	preview := goav.Sink(component.SinkFunc("preview", func(context.Context, component.Message) error {
		return nil
	}))
	job := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Decode().
		Do(meter).
		Branches(
			goav.Branch("web").
				Encode(codec.VP9(codec.Bitrate(2_000_000))).
				To(packets),
			goav.Branch("preview").
				Resize(320, 180).
				To(preview),
		)

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	for _, want := range []string{
		"decode-video -> meter",
		"meter -> encode-web",
		"meter -> resize-preview",
		"resize-preview -> preview",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("spec missing %q:\n%s", want, text)
		}
	}
	for _, duplicate := range []string{
		"decode-video -> encode-web",
		"decode-video -> resize-preview",
	} {
		if strings.Contains(text, duplicate) {
			t.Fatalf("custom stage split was duplicated as %q:\n%s", duplicate, text)
		}
	}
}

func TestBranchCompositionAllowsPacketCopyBranches(t *testing.T) {
	archive := goav.Write("archive.ivf", io.Discard)
	packets := goav.Sink(component.SinkFunc("packets", func(context.Context, component.Message) error {
		return nil
	}))
	job := goav.From(goav.FileInput("input.ivf", strings.NewReader(""))).
		Video().
		Copy().
		Tap(goav.PacketTap("video.packets")).
		Branches(
			goav.Branch("archive").To(archive),
			goav.Branch("packets").To(packets),
		)

	intent := goav.JobPlanForTest(job)
	if len(intent.Streams) != 2 ||
		intent.Streams[0].From.Name() != "video.packets" ||
		intent.Streams[0].From.Domain() != shape.DomainPacket ||
		intent.Streams[1].From.Name() != "video.packets" ||
		intent.Streams[1].From.Domain() != shape.DomainPacket {
		t.Fatalf("intent streams = %+v, want packet branches from video.packets", intent.Streams)
	}

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	for _, want := range []string{
		"input.ivf -> select-video",
		"select-video -> archive.ivf",
		"select-video -> packets",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("spec missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "decode-video") || strings.Contains(text, "encode-archive") {
		t.Fatalf("packet copy branches should not decode or encode:\n%s", text)
	}
}

func TestBranchCompositionRejectsDecodeAfterBranchOperation(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ivf", strings.NewReader(""))).
		Video().
		Copy().
		Branches(
			goav.Branch("bad").
				Resize(320, 180).
				Decode().
				To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
					return nil
				}))),
		).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "branch_decode_order_invalid" {
		t.Fatalf("err = %v, want branch_decode_order_invalid with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "decode must be the first branch operation") ||
		!strings.Contains(err.Error(), ".Decode().Resample") {
		t.Fatalf("err = %v, want decode order guidance", err)
	}
}

func TestBranchCompositionRejectsDecodeThenCopy(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ivf", strings.NewReader(""))).
		Video().
		Copy().
		Branches(
			goav.Branch("bad").
				Decode().
				Copy().
				To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
					return nil
				}))),
		).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "branch_decode_copy_invalid" {
		t.Fatalf("err = %v, want branch_decode_copy_invalid with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "cannot decode packets and then copy") ||
		!strings.Contains(err.Error(), ".Copy() for packet-preserving branches") {
		t.Fatalf("err = %v, want decode/copy guidance", err)
	}
}

func TestBranchCompositionRejectsDecodeFromFrameBranchPoint(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ivf", strings.NewReader(""))).
		Video().
		Decode().
		Branches(
			goav.Branch("bad").
				Decode().
				To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
					return nil
				}))),
		).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "branch_decode_domain_mismatch" {
		t.Fatalf("err = %v, want branch_decode_domain_mismatch with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "requires a packet-domain stream point") ||
		!strings.Contains(err.Error(), "already starts after stream decode") {
		t.Fatalf("err = %v, want decode domain guidance", err)
	}
}

func TestBranchRecipeRequiresBranch(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_missing" {
		t.Fatalf("err = %v, want output_missing with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "no output is configured") {
		t.Fatalf("err = %v, want output guidance", err)
	}
}

func TestBranchRecipeRequiresBranchDestination(t *testing.T) {
	job := branchJob(goav.FileInput("input.webm", strings.NewReader("")))
	job.Video("360p").Encode(codec.VP9(codec.Bitrate(600_000)))
	_, err := job.Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_missing" {
		t.Fatalf("err = %v, want destination_missing with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "branch has no destination") ||
		!strings.Contains(err.Error(), "goav.Write") {
		t.Fatalf("err = %v, want destination guidance", err)
	}
}

func TestBranchRecipeRejectsNegativeStreamIndex(t *testing.T) {
	_, err := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		materialize().
		Audio(goav.StreamIndex(-1)).
		Decode().
		Tap(goav.FrameTap("audio.decoded")).
		Branches(goav.Branch("bad").Encode(codec.Opus(codec.Bitrate(64_000))).To(goav.Write("bad.ogg", io.Discard))).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_selector_invalid" {
		t.Fatalf("err = %v, want stream_selector_invalid with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "stream index must be non-negative") ||
		!strings.Contains(err.Error(), "index=-1") {
		t.Fatalf("err = %v, want stream index guidance", err)
	}
}

func TestBranchRecipeRejectsWrongMediaTransform(t *testing.T) {
	_, err := goav.From(goavtest.Packets(av.CodecOpus, av.Packet{Payload: av.Buffer{Bytes: []byte{1}}})).
		UseRuntime(goavtest.Runtime()).
		Audio().
		Decode().
		Branches(
			goav.Branch("bad").
				Resize(640, 360).
				To(goavtest.NewCollector().Sink()),
		).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "operation_shape_mismatch" {
		t.Fatalf("err = %v, want operation_shape_mismatch with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "resize cannot consume the current media shape") ||
		!strings.Contains(err.Error(), ".Video().Resize(...)") {
		t.Fatalf("err = %v, want operation-shape transform guidance", err)
	}
}

func TestBranchRecipeRejectsInvalidResample(t *testing.T) {
	_, err := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Audio("bad").Resample(0, codec.Mono).Encode(codec.Opus(codec.Bitrate(64_000))).
		To(goav.Write("bad.ogg", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "transform_invalid" {
		t.Fatalf("err = %v, want transform_invalid with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "positive sample rate and channels") ||
		!strings.Contains(err.Error(), "sample_rate=0") {
		t.Fatalf("err = %v, want resample value guidance", err)
	}
}

func TestBranchRecipeRejectsProcessingAfterEncoder(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Decode().
		Tap(goav.FrameTap("video.decoded")).
		Branches(
			goav.Branch("360p").
				Encode(codec.VP9(codec.Bitrate(600_000))).
				Resize(640, 360).
				To(goav.Write("preview.webm", io.Discard)),
		).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_step_after_encode" {
		t.Fatalf("err = %v, want stream_step_after_encode with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "step: resize") ||
		!strings.Contains(err.Error(), "encoder: vp9") ||
		!strings.Contains(err.Error(), ".To(...) after the encoder") {
		t.Fatalf("err = %v, want transcode terminal encoder guidance", err)
	}
}

func TestBranchRecipeRejectsDuplicateEncoder(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Decode().
		Tap(goav.FrameTap("video.decoded")).
		Branches(
			goav.Branch("360p").
				Encode(codec.VP9(codec.Bitrate(600_000))).
				Encode(codec.VP8(codec.Bitrate(400_000))).
				To(goav.Write("preview.webm", io.Discard)),
		).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_duplicate" {
		t.Fatalf("err = %v, want encode_duplicate with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "first encoder: vp9") ||
		!strings.Contains(err.Error(), "second encoder: vp8") ||
		!strings.Contains(err.Error(), "multiple encoded branches") {
		t.Fatalf("err = %v, want duplicate transcode encoder guidance", err)
	}
}

func TestBranchRecipeRejectsNegativeEncodeBitrate(t *testing.T) {
	_, err := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("bad").Encode(codec.VP9(codec.Bitrate(-1))).
		To(goav.Write("bad.webm", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_parameter_invalid" {
		t.Fatalf("err = %v, want encode_parameter_invalid with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "bitrate must be non-negative") ||
		!strings.Contains(err.Error(), "bitrate=-1") {
		t.Fatalf("err = %v, want bitrate guidance", err)
	}
}

func TestBranchRecipeDescribesTransformChain(t *testing.T) {
	spec, err := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("360p").Resize(1280, 720).Resize(640, 360).Encode(codec.VP9(codec.Bitrate(600_000))).
		To(goav.Write("preview.webm", io.Discard)).
		Describe()
	if err != nil {
		t.Fatalf("Describe() error = %v", err)
	}
	text := specText(spec)
	for _, want := range []string{"resize-360p", "resize-360p-2", "resize-360p-2 -> encode-360p"} {
		if !strings.Contains(text, want) {
			t.Fatalf("spec missing %q:\n%s", want, text)
		}
	}
}

func tinyIVF() []byte {
	var data bytes.Buffer
	var header [32]byte
	copy(header[:4], "DKIF")
	binary.LittleEndian.PutUint16(header[6:8], 32)
	copy(header[8:12], "VP80")
	binary.LittleEndian.PutUint16(header[12:14], 16)
	binary.LittleEndian.PutUint16(header[14:16], 16)
	binary.LittleEndian.PutUint32(header[16:20], 1000)
	binary.LittleEndian.PutUint32(header[20:24], 1)
	data.Write(header[:])

	payload := []byte{0x10, 0x20, 0x30}
	var frame [12]byte
	binary.LittleEndian.PutUint32(frame[:4], uint32(len(payload)))
	data.Write(frame[:])
	data.Write(payload)
	return data.Bytes()
}
