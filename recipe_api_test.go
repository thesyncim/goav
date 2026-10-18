package goav_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
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
	runconfig "github.com/thesyncim/goav/runconfig"
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

	var resizeContract shape.Contract = goav.ResizeContractForTest(1280, 720)
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

	operationContract := goav.ResampleContractForTest(16_000, codec.Mono)
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
	report, err := recipeOf(input.
		Audio().
		To(goav.Sink(component.SinkFunc("packets", func(context.Context, component.Message) error {
			return nil
		})))).Explain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Inputs) != 1 ||
		report.Inputs[0].Name != "generated" ||
		report.Inputs[0].Protocol != av.ProtocolCustom ||
		report.Inputs[0].Codec != av.CodecOpus {
		t.Fatalf("inputs: %+v", report.Inputs)
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

// testRecipe runs Outputs the way these tests read: a recipe value whose
// runtime is picked late; without one it runs on a bare runtime.
type testRecipe struct {
	outs    []goav.Output
	runtime *goav.Runtime
	set     bool
}

func recipeOf(outs ...goav.Output) *testRecipe { return &testRecipe{outs: outs} }

func (r *testRecipe) UseRuntime(runtime *goav.Runtime) *testRecipe {
	r.runtime, r.set = runtime, true
	return r
}

func (r *testRecipe) rt() *goav.Runtime {
	if r.set {
		return r.runtime
	}
	return mustRuntime()
}

func (r *testRecipe) Describe() (pipeline.Spec, error) { return r.rt().Describe(r.outs...) }

func (r *testRecipe) Explain(ctx context.Context) (plan.Report, error) {
	return r.rt().Explain(ctx, r.outs...)
}

func (r *testRecipe) Build(ctx context.Context) (goav.LiveTask, error) {
	return r.rt().Build(ctx, r.outs...)
}

func (r *testRecipe) BuildLive(ctx context.Context) (goav.LiveTask, error) { return r.Build(ctx) }

func (r *testRecipe) Run(ctx context.Context) error { return r.rt().Run(ctx, r.outs...) }

func recordJob(input goav.InputSpec, outputs ...goav.Destination) *testRecipe {
	return recipeOf(input.To(outputs...))
}

func decodeJob(input goav.InputSpec, output goav.Destination) *testRecipe {
	return recipeOf(input.Stream().Decode().To(output))
}

type testBranchJob struct {
	input    goav.InputSpec
	runtime  *goav.Runtime
	set      bool
	branches []testTranscodeBranch
}

type testTranscodeBranch struct {
	name         string
	media        av.MediaType
	transforms   []testTransform
	encode       codec.Spec
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

// branchJob declares named decoded forks of one input, one per media call.
func branchJob(input goav.InputSpec) *testBranchJob {
	return &testBranchJob{input: input}
}

func (j *testBranchJob) UseRuntime(runtime *goav.Runtime) *testBranchJob {
	j.runtime, j.set = runtime, true
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

func (j *testBranchJob) materialize() []goav.Output {
	outs := make([]goav.Output, 0, len(j.branches))
	for _, branch := range j.branches {
		var stream goav.Stream
		switch branch.media {
		case av.MediaAudio:
			stream = j.input.Audio().Decode()
		case av.MediaVideo:
			stream = j.input.Video().Decode()
		default:
			stream = j.input.Stream().Decode()
		}
		for _, transform := range branch.transforms {
			switch transform.kind {
			case "resize":
				stream = stream.Resize(transform.width, transform.height)
			case "resample":
				stream = stream.Resample(transform.sampleRate, transform.channels)
			}
		}
		if branch.encode.ID != "" {
			stream = stream.Encode(branch.encode)
		}
		outs = append(outs, stream.As(branch.name).To(branch.destinations...))
	}
	return outs
}

func (j *testBranchJob) recipe() *testRecipe {
	r := recipeOf(j.materialize()...)
	if j.set {
		r.UseRuntime(j.runtime)
	}
	return r
}

func (j *testBranchJob) Explain(ctx context.Context) (plan.Report, error) {
	return j.recipe().Explain(ctx)
}

func (j *testBranchJob) Describe() (pipeline.Spec, error) { return j.recipe().Describe() }

func (j *testBranchJob) Build(ctx context.Context) (goav.LiveTask, error) {
	return j.recipe().Build(ctx)
}

func (j *testBranchJob) BuildLive(ctx context.Context) (goav.LiveTask, error) {
	return j.recipe().Build(ctx)
}

func (j *testBranchJob) Run(ctx context.Context) error { return j.recipe().Run(ctx) }

func (b *testTranscodeBranchBuilder) Resize(width int, height int) *testTranscodeBranchBuilder {
	b.current().transforms = append(b.current().transforms, testTransform{kind: "resize", width: width, height: height})
	return b
}

func (b *testTranscodeBranchBuilder) Resample(sampleRate int, channels int) *testTranscodeBranchBuilder {
	b.current().transforms = append(b.current().transforms, testTransform{kind: "resample", sampleRate: sampleRate, channels: channels})
	return b
}

func (b *testTranscodeBranchBuilder) Encode(spec codec.Spec) *testTranscodeBranchBuilder {
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
	runtimeType := reflect.TypeFor[*goav.Runtime]()
	if _, ok := runtimeType.MethodByName("Explain"); !ok {
		t.Fatal("Runtime should expose Explain for structured workflow reports")
	}
	reportType := reflect.TypeFor[plan.Report]()
	for _, method := range []string{"Text", "Mermaid", "DOT", "Render"} {
		if _, ok := reportType.MethodByName(method); ok {
			t.Fatalf("plan.Report exposes renderer method %s; keep rendering outside core", method)
		}
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
	report, err := mustRuntime(bundle.Codecs()).Explain(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Resample(16_000, codec.Mono).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))))
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
		runconfig.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{
				{Index: 0, ID: "audio", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}},
				{Index: 1, ID: "video", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo}},
			}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
			registry.RegisterMuxer(av.FormatOgg, recipeAPIMuxerFactory{})
		}),
		runconfig.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, recipeAPIDecoderFactory{})
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, recipeAPIEncoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP9, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
		}),
		runconfig.WithFilterAdapter(func(registry *filter.SimpleRegistry) {
			registry.RegisterFactory(filter.Descriptor{Name: filter.FactoryResize, Input: av.MediaVideo, Output: av.MediaVideo}, recipeAPIFilterFactory{})
			registry.RegisterFactory(filter.Descriptor{Name: filter.FactoryResample, Input: av.MediaAudio, Output: av.MediaAudio}, recipeAPIFilterFactory{})
		}),
	)

	web := goav.Write("web.ogg", io.Discard)
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
	want := []plan.OperationKind{plan.OpDemux, plan.OpSelect, plan.OpDecode, plan.OpTransform, plan.OpEncode}
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
		runconfig.WithFormatAdapter(func(registry *format.SimpleRegistry) {
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
		runconfig.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, recipeAPIDecoderFactory{})
		}),
	)

	report, err := rt.Explain(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Tap("audio.decoded").
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))))
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
		runconfig.WithFormatAdapter(func(registry *format.SimpleRegistry) {
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
		runconfig.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP9, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
		}),
		runconfig.WithFilterAdapter(func(registry *filter.SimpleRegistry) {
			registry.RegisterFactory(filter.Descriptor{Name: filter.FactoryResize, Input: av.MediaVideo, Output: av.MediaVideo}, recipeAPIFilterFactory{})
		}),
	)

	web := goav.Write("web.ogg", io.Discard)
	report, err := rt.Explain(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Video().
		Decode().
		Resize(1280, 720).
		Encode(codec.VP9(codec.Bitrate(2_000_000), codec.FPS(30))).
		Tap("video.encoded").
		As("preview").
		To(web))
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
	tests := []struct {
		name string
		job  *testRecipe
		want []string
	}{
		{
			name: "frame to packet",
			job: recipeOf(goav.FileInput("input.ivf", strings.NewReader("")).
				Video().
				Decode().
				Shape(shape.New(shape.Domain(shape.DomainPacket), shape.Media(av.MediaVideo))).
				Encode(codec.VP9(codec.Bitrate(2_000_000))).
				To(goav.Write("preview.ivf", io.Discard))).
				UseRuntime(bundle.MustNew()),
			want: []string{
				"shape annotation cannot change packet/frame domain",
				"operation_index=1",
				"expected_shape=domain=frame media=video",
				"actual_shape=domain=packet media=video",
				"keep .Shape(...) annotations in the current packet/frame domain",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.job.Describe()
			var buildErr *goav.BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != "operation_shape_mismatch" {
				t.Fatalf("err = %v, want operation_shape_mismatch", err)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestExplainRequirementsFollowOrderedBranchOperations(t *testing.T) {
	rt := mustRuntime(
		runconfig.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{
				{Index: 0, ID: "video", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo}},
			}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
			registry.RegisterMuxer(av.FormatOgg, recipeAPIMuxerFactory{})
		}),
		runconfig.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP9, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
		}),
		runconfig.WithFilterAdapter(func(registry *filter.SimpleRegistry) {
			registry.RegisterFactory(filter.Descriptor{Name: filter.FactoryResize, Input: av.MediaVideo, Output: av.MediaVideo}, recipeAPIFilterFactory{})
		}),
	)
	meter := component.FrameFunc("meter", func(ctx context.Context, frame *av.Frame, emit component.Emit) error {
		return emit.Frame(frame)
	})

	web := goav.Write("web.ogg", io.Discard)
	report, err := rt.Explain(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Video().
		Decode().
		Resize(640, 360).
		Do(meter).
		Encode(codec.VP9(codec.Bitrate(600_000))).
		As("v360").
		To(web))
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
		runconfig.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{
				{Index: 0, ID: "audio", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}},
			}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
		}),
		runconfig.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, recipeAPIDecoderFactory{})
		}),
		runconfig.WithFilterAdapter(func(registry *filter.SimpleRegistry) {
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

	report, err := rt.Explain(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Resample(16_000, codec.Mono).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))))
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
	report, err = rt.Explain(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Resample(16_000, codec.Mono).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))))
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
		runconfig.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{
				{Index: 0, ID: "audio", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}},
			}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
		}),
		runconfig.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, recipeAPIDecoderFactory{})
		}),
		runconfig.WithFilterAdapter(func(registry *filter.SimpleRegistry) {
			registry.RegisterFactory(filter.Descriptor{
				Name:   filter.FactoryResample,
				Input:  av.MediaVideo,
				Output: av.MediaVideo,
			}, recipeAPIFilterFactory{})
		}),
	)

	report, err := rt.Explain(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Resample(16_000, codec.Mono).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))))
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
	rt := mustRuntime(runconfig.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
		registry.RegisterEncoder(codec.Descriptor{
			ID:   custom,
			Type: av.MediaVideo,
			Capabilities: codec.Capabilities{
				PixelFormats: []string{av.PixelFormatI420},
			},
		}, recipeAPIEncoderFactory{})
	}))

	report, err := rt.Explain(context.Background(), goav.FileInput("input.raw", strings.NewReader("")).
		Audio().
		Decode().
		Encode(codec.Codec(custom, av.MediaAudio)).
		To(goav.Sink(component.SinkFunc("packets", func(context.Context, component.Message) error {
			return nil
		}))))
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
		runconfig.WithFormatAdapter(func(registry *format.SimpleRegistry) {
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
		runconfig.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{
				ID:   custom,
				Type: av.MediaAudio,
				Capabilities: codec.Capabilities{
					SampleFormats: []string{av.SampleFormatS16},
				},
			}, recipeAPIDecoderFactory{})
		}),
	)

	report, err := rt.Explain(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))))
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
		runconfig.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{
				{Index: 0, ID: "video", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo}},
			}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
			registry.RegisterMuxer(av.FormatIVF, recipeAPIMuxerFactory{})
		}),
		runconfig.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP9, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
		}),
	)

	web := goav.Write("web.ivf", io.Discard, goav.Format(av.FormatIVF))
	in := goav.FileInput("input.ogg", strings.NewReader(""))
	trunk := in.
		Video().
		Decode()
	_, err := rt.Build(context.Background(),
		trunk.
			Encode(codec.VP8(codec.Bitrate(600_000))).
			As("v8").
			To(web),
		trunk.
			Encode(codec.VP9(codec.Bitrate(900_000))).
			As("v9").
			To(web),
	)
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
		runconfig.WithFormatAdapter(func(registry *format.SimpleRegistry) {
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
		runconfig.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
		}),
	)

	target := goav.Write("out.mkv", io.Discard, goav.Format(av.FormatMatroska))
	_, err := rt.Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Video().
		Decode().
		Encode(codec.VP8(codec.Bitrate(600_000))).
		As("video").
		To(target))
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
		runconfig.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{
				{Index: 0, ID: "video", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo}},
			}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
			registry.RegisterMuxer(av.FormatIVF, recipeAPIMuxerFactory{})
		}),
		runconfig.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP9, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
		}),
	)

	web := goav.Write("web.ivf", io.Discard, goav.Format(av.FormatIVF))
	in := goav.FileInput("input.ogg", strings.NewReader(""))
	trunk := in.
		Video().
		Decode()
	report, err := rt.Explain(context.Background(),
		trunk.
			Encode(codec.VP8(codec.Bitrate(600_000))).
			As("v8").
			To(web),
		trunk.
			Encode(codec.VP9(codec.Bitrate(900_000))).
			As("v9").
			To(web),
	)
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
		runconfig.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{
				{Index: 0, ID: "video", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo}},
			}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
			registry.RegisterMuxer(av.FormatAnnexB, recipeAPIMuxerFactory{})
		}),
		runconfig.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
		}),
	)

	_, err := rt.Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Video().
		Decode().
		Encode(codec.VP8(codec.Bitrate(600_000))).
		To(goav.Write("out.h264", io.Discard, goav.Format(av.FormatAnnexB))))
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

func TestInputSpecKeepsManualDepacketizersOutOfRecipeFrontDoor(t *testing.T) {
	inputType := reflect.TypeFor[goav.InputSpec]()
	if _, ok := inputType.MethodByName("Depacketize"); ok {
		t.Fatal("InputSpec exposes manual depacketizer wiring; RTP recipes should use codec intent")
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
		task.Tap("audio.decoded").
			As("implicit-tap").
			To(goav.Sink(component.SinkFunc("implicit-tap", func(context.Context, component.Message) error {
				return nil
			}))),
	)
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != errcode.Code("runtime_branch_tap_missing") {
		t.Fatalf("err = %v, want runtime_branch_tap_missing", err)
	}
	attachment, err := task.Attach(context.Background(),
		task.Route(decode.Route()).
			As("levels").
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

func TestCodecSettingsOwnTuningAndAdapterOptions(t *testing.T) {
	codecSpec := reflect.TypeFor[codec.Spec]()
	settingsType := reflect.TypeFor[codec.CodecSettings]()
	if field, ok := codecSpec.FieldByName("Settings"); !ok || field.Type != settingsType {
		t.Fatalf("Spec.Settings = %v %v, want codec.CodecSettings", field.Type, ok)
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
			t.Fatalf("Spec should not duplicate codec setting field %s", field)
		}
	}

	for _, tt := range []struct {
		name string
		typ  reflect.Type
	}{
		{name: "decode", typ: reflect.TypeFor[codec.DecodeConfig]()},
		{name: "encode", typ: reflect.TypeFor[codec.EncodeConfig]()},
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

func TestReportsUseDestinations(t *testing.T) {
	for _, tt := range []struct {
		name string
		typ  reflect.Type
	}{
		{name: "plan.Report", typ: reflect.TypeFor[plan.Report]()},
		{name: "plan.Branch", typ: reflect.TypeFor[plan.Branch]()},
	} {
		if _, ok := tt.typ.FieldByName("Targets"); ok {
			t.Fatalf("%s exposes Targets; use Destinations as the public routing field", tt.name)
		}
		if _, ok := tt.typ.FieldByName("Destinations"); !ok {
			t.Fatalf("%s should expose Destinations", tt.name)
		}
	}
}

func TestRecipeReportsNilRuntime(t *testing.T) {
	_, err := recordJob(
		goav.FileInput("input.ivf", strings.NewReader("")),
		goav.Write("recording.ivf", io.Discard),
	).UseRuntime(nil).BuildLive(context.Background())

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
	).UseRuntime(nil).BuildLive(context.Background())

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
}

func TestRecordRecipeCanWriteToTypedDestination(t *testing.T) {
	target := goav.Write("recording.ivf", io.Discard)
	job := recipeOf(goav.FileInput("input.ivf", strings.NewReader("")).
		To(target))

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
}

func TestReadmeAudioDecodeRecipeIsSmall(t *testing.T) {
	sink := component.SinkFunc("frames", func(context.Context, component.Message) error {
		return nil
	})
	job := recipeOf(goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		To(goav.Sink(sink)))

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
}

func TestReadmeCustomStageToCustomSinkRecipeIsSmall(t *testing.T) {
	meter := component.FrameFunc("meter", func(ctx context.Context, frame *av.Frame, emit component.Emit) error {
		return emit.Frame(frame)
	})
	sink := component.SinkFunc("levels", func(context.Context, component.Message) error {
		return nil
	})

	job := recipeOf(goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Do(meter).
		To(goav.Sink(sink)))

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "decode-audio -> meter") ||
		!strings.Contains(text, "meter -> levels") {
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

	job := recipeOf(goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(destinations...))
	report, _ := job.Explain(context.Background())
	if len(report.Destinations) != 2 || report.Destinations[0].Name != "archive.ogg" || report.Destinations[1].Name != "stats" {
		t.Fatalf("destinations: %+v", report.Destinations)
	}
}

func TestDuplicateDestinationNameRequiresMux(t *testing.T) {
	left := goav.Write("web.ivf", io.Discard, goav.Format(av.FormatIVF))
	right := goav.Write("web.ivf", io.Discard, goav.Format(av.FormatIVF))

	in := goav.FileInput("input.ivf", strings.NewReader(""))
	trunk := in.
		Video().
		Decode()
	_, err := mustRuntime().Describe(
		trunk.
			Encode(codec.VP9(codec.Bitrate(2_000_000))).
			As("v720").
			To(left),
		trunk.
			Encode(codec.VP8(codec.Bitrate(600_000))).
			As("v360").
			To(right),
	)
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_duplicate" {
		t.Fatalf("err = %v, want destination_duplicate", err)
	}
}

func TestDestinationConstructorsReturnDestination(t *testing.T) {
	destinationType := reflect.TypeFor[goav.Destination]()
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
	providerType := reflect.TypeFor[provider.Destination]()
	impl := reflect.TypeFor[recipeAPICustomDestination]()
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

// TestMediaOptionsConfigureBothDirections pins the direction-agnostic option
// vocabulary: the same goav.MIME / goav.Name / goav.Metadata values configure
// input and destination constructors, while Format stays destination-typed.
func TestMediaOptionsConfigureBothDirections(t *testing.T) {
	job := recipeOf(goav.FileInput("in", strings.NewReader(""),
		goav.Name("mic"),
		goav.MIME("audio/ogg"),
		goav.Metadata(av.Metadata{"session": "demo"}),
	).To(goav.Write("out", io.Discard,
		goav.Name("recording.ogg"),
		goav.MIME("audio/ogg"),
		goav.Metadata(av.Metadata{"session": "demo"}),
		goav.Format(av.FormatOgg),
	)))
	report, _ := job.Explain(context.Background())
	if len(report.Inputs) != 1 || report.Inputs[0].Name != "mic" || report.Inputs[0].MIMEType != "audio/ogg" {
		t.Fatalf("inputs: %+v", report.Inputs)
	}
	if len(report.Destinations) != 1 || report.Destinations[0].Name != "recording.ogg" || report.Destinations[0].MIMEType != "audio/ogg" || report.Destinations[0].Format != av.FormatOgg {
		t.Fatalf("destinations: %+v", report.Destinations)
	}
}

// TestWithLayersOptionsOntoConstructedValues pins the late-config seam: With
// applies the same option vocabulary to an already-constructed input or
// destination value and returns a configured copy.
func TestWithLayersOptionsOntoConstructedValues(t *testing.T) {
	input := goav.FileInput("in.ogg", strings.NewReader("")).With(goav.Name("mic"))
	output := goav.Write("out.ogg", io.Discard).With(goav.MIME("audio/ogg"))
	report, _ := recordJob(input, output).Explain(context.Background())
	if len(report.Inputs) != 1 || report.Inputs[0].Name != "mic" {
		t.Fatalf("inputs: %+v", report.Inputs)
	}
	if len(report.Destinations) != 1 || report.Destinations[0].MIMEType != "audio/ogg" {
		t.Fatalf("destinations: %+v", report.Destinations)
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
	).BuildLive(context.Background())
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
	).BuildLive(context.Background())
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
	).BuildLive(context.Background())
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
	).BuildLive(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_missing" {
		t.Fatalf("err = %v, want encode_missing with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "goav.Sink") ||
		!strings.Contains(err.Error(), "input.Video().To(output)") {
		t.Fatalf("err = %v, want decode output guidance", err)
	}
}

func TestRecordRecipeRejectsMissingOutput(t *testing.T) {
	_, err := recordJob(
		goav.FileInput("input.ogg", strings.NewReader("")),
	).BuildLive(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_invalid" {
		t.Fatalf("err = %v, want an Output-without-destination refusal", err)
	}
}

func TestRecordRecipeRejectsEmptyDestination(t *testing.T) {
	var target goav.Destination
	_, err := recordJob(
		goav.FileInput("input.ogg", strings.NewReader("")),
		target,
	).BuildLive(context.Background())
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
	).BuildLive(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_writer_missing" {
		t.Fatalf("err = %v, want output_writer_missing with matching BuildError code", err)
	}
}

func TestRecordRecipeRejectsUnnamedFileWithoutFormat(t *testing.T) {
	_, err := recordJob(
		goav.FileInput("input.ivf", strings.NewReader("")),
		goav.Write("", io.Discard),
	).BuildLive(context.Background())

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
	).BuildLive(context.Background())

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
	).UseRuntime(mustRuntime()).BuildLive(context.Background())

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
	).UseRuntime(mustRuntime()).BuildLive(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_muxer_missing" {
		t.Fatalf("err = %v, want destination_muxer_missing", err)
	}
	if buildErr.Operation != "open destination" {
		t.Fatalf("operation = %q, want open destination", buildErr.Operation)
	}
	if !strings.Contains(err.Error(), `format "mp4"`) ||
		!strings.Contains(err.Error(), "no muxer is registered") ||
		!strings.Contains(err.Error(), "this runtime has no muxers") {
		t.Fatalf("err = %v, want muxer adapter guidance", err)
	}
}

func TestRecordRecipeRejectsDuplicateOutputs(t *testing.T) {
	_, err := mustRuntime().Build(context.Background(), goav.FileInput("input.ivf", strings.NewReader("")).To(goav.Write("recording.ivf", io.Discard), goav.Write("recording.ivf", io.Discard)))

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_duplicate" {
		t.Fatalf("err = %v, want destination_duplicate with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), `destination "recording.ivf"`) ||
		!strings.Contains(err.Error(), "same destination value") {
		t.Fatalf("err = %v, want duplicate output guidance", err)
	}
}

func TestStreamRecipeRejectsDuplicateSinkDestinations(t *testing.T) {
	sink := func(context.Context, component.Message) error { return nil }
	_, err := mustRuntime().Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		To(goav.Sink(component.SinkFunc("frames", sink)), goav.Sink(component.SinkFunc("frames", sink))))

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_duplicate" {
		t.Fatalf("err = %v, want destination_duplicate with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), `destination "frames"`) ||
		!strings.Contains(err.Error(), "goav.Name") {
		t.Fatalf("err = %v, want duplicate sink guidance", err)
	}
}

func TestStreamRecipeRejectsDuplicateTypedDestinations(t *testing.T) {
	target := goav.Write("voice.ogg", io.Discard)
	_, err := mustRuntime().Describe(goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(target, target))

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_duplicate" {
		t.Fatalf("err = %v, want destination_duplicate with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), `destination "voice.ogg" more than once`) {
		t.Fatalf("err = %v, want duplicate destination guidance", err)
	}
}

func TestProviderRecipeRejectsNilProvider(t *testing.T) {
	_, err := recordJob(
		goav.Input(nil),
		goav.Write("recording.ogg", io.Discard),
	).BuildLive(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "input_invalid" {
		t.Fatalf("err = %v, want input_invalid wrapping errNilSource", err)
	}
}

func TestReadmeAudioEncodeRecipeIsSmall(t *testing.T) {
	meter := component.FrameFunc("meter", func(ctx context.Context, frame *av.Frame, emit component.Emit) error {
		return emit.Frame(frame)
	})
	job := goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Do(meter).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(goav.Write("archive.ogg", io.Discard))

	spec, err := mustRuntime().Describe(job)
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
	job := recipeOf(goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Resample(16_000, codec.Mono).
		Encode(codec.Opus(codec.Bitrate(48_000))).
		To(goav.Write("preview.ogg", io.Discard)))

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
}

func TestReadmeVideoResizeEncodeRecipeIsSmall(t *testing.T) {
	job := recipeOf(goav.FileInput("input.webm", strings.NewReader("")).
		Video().
		Decode().
		Resize(1280, 720).
		Encode(codec.VP9(codec.Bitrate(2_000_000))).
		To(goav.Write("preview.webm", io.Discard)))

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
}

func TestStreamRecipeRequiresExplicitDecodeBeforeFrameConsumers(t *testing.T) {
	meter := component.FrameFunc("meter", func(ctx context.Context, frame *av.Frame, emit component.Emit) error {
		return emit.Frame(frame)
	})
	tests := []struct {
		name string
		job  *testRecipe
		fix  string
	}{
		{
			name: "custom stage",
			job: recipeOf(goav.FileInput("input.ogg", strings.NewReader("")).
				Audio().
				Do(meter).
				To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error { return nil })))),
			fix: "call .Decode() before",
		},
		{
			name: "resize",
			job: recipeOf(goav.FileInput("input.webm", strings.NewReader("")).
				Video().
				Resize(1280, 720).
				To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error { return nil })))),
			fix: "call .Decode() before",
		},
		{
			name: "resample",
			job: recipeOf(goav.FileInput("input.ogg", strings.NewReader("")).
				Audio().
				Resample(16_000, codec.Mono).
				To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error { return nil })))),
			fix: "call .Decode() before",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.job.BuildLive(context.Background())
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

func TestStreamRecipeRejectsNegativeStreamIndex(t *testing.T) {
	_, err := mustRuntime().Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio(goav.StreamIndex(-1)).
		Decode().
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))))
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
	_, err := recipeOf(goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Do(nil).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		})))).
		BuildLive(context.Background())
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
	_, err := mustRuntime().Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Do(component.PacketFunc("packets", nil)).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))))
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stage_missing" {
		t.Fatalf("err = %v, want stage_missing with matching BuildError code", err)
	}
	if !errors.Is(err, component.ErrNilStageCallback) {
		t.Fatalf("err = %v, want ErrNilStageCallback cause", err)
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
	).BuildLive(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_invalid" {
		t.Fatalf("err = %v, want output_invalid with matching BuildError code", err)
	}
	if !errors.Is(err, component.ErrNilSinkCallback) {
		t.Fatalf("err = %v, want ErrNilSinkCallback cause", err)
	}
	if !strings.Contains(err.Error(), "SinkFunc") ||
		!strings.Contains(err.Error(), "non-nil sink") {
		t.Fatalf("err = %v, want SinkFunc nil-callback guidance", err)
	}
}

// TestStreamRecipeRejectsWrongMediaTransform pins the one media check every
// recipe form and runtime attach share: the shape solver's.
func TestStreamRecipeRejectsWrongMediaTransform(t *testing.T) {
	_, err := goavtest.Runtime().Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Resize(320, 180).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))))
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "operation_shape_mismatch" {
		t.Fatalf("err = %v, want operation_shape_mismatch", err)
	}
}

func TestStreamRecipeRejectsInvalidResize(t *testing.T) {
	_, err := mustRuntime().Build(context.Background(), goav.FileInput("input.webm", strings.NewReader("")).
		Video().
		Decode().
		Resize(0, 720).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))))
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
	_, err := mustRuntime().Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Resample(48_000, codec.Stereo).
		To(goav.Write("archive.ogg", io.Discard)))
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_missing" {
		t.Fatalf("err = %v, want encode_missing with matching BuildError code", err)
	}
}

func TestStreamRecipeRejectsMixedSinkAndFile(t *testing.T) {
	_, err := mustRuntime().Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		})), goav.Write("archive.ogg", io.Discard)))

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_kind_mixed" {
		t.Fatalf("err = %v, want output_kind_mixed with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "cannot mix sinks and muxed outputs") ||
		!strings.Contains(err.Error(), "fork the decoded Stream") {
		t.Fatalf("err = %v, want mixed output guidance", err)
	}
}

func TestStreamRecipeAllowsEncodedMuxAndSinkDestinations(t *testing.T) {
	rt := mustRuntime(
		runconfig.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{
				{Index: 0, ID: "audio", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}},
			}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
			registry.RegisterMuxer(av.FormatOgg, recipeAPIMuxerFactory{})
		}),
		runconfig.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, recipeAPIEncoderFactory{})
		}),
	)

	spec, err := rt.Describe(goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(goav.Write("archive.ogg", io.Discard), goav.Sink(component.SinkFunc("packets", func(context.Context, component.Message) error {
			return nil
		}))))
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
	_, err := mustRuntime().Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		Resample(16_000, codec.Mono).
		To(goav.Write("archive.ogg", io.Discard)))

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
	_, err := mustRuntime().Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		Encode(codec.VP9(codec.Bitrate(600_000))).
		To(goav.Write("archive.ogg", io.Discard)))

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
		codec codec.Spec
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
				runconfig.WithMuxer(av.FormatIVF, recipeAPIMuxerFactory{}),
				runconfig.WithEncoder(codec.Descriptor{ID: tt.codec.ID, Type: av.MediaVideo}, recipeAPIEncoderFactory{}),
			)
			_, err := rt.Describe(source.
				Video().
				Encode(tt.codec).
				To(goav.Write("archive.ivf", io.Discard, goav.Format(av.FormatIVF))))
			if err != nil {
				t.Fatalf("Describe() err = %v", err)
			}
		})
	}
}

func TestStreamRecipeReportsMissingCustomEncoder(t *testing.T) {
	rt := mustRuntime(runconfig.WithFormatAdapter(func(registry *format.SimpleRegistry) {
		registry.RegisterMuxer(av.FormatOgg, recipeAPIMuxerFactory{})
	}))
	_, err := rt.Build(context.Background(), goav.FileInput("input.wav", strings.NewReader("")).
		Audio().
		Decode().
		Encode(codec.Codec("pcm", av.MediaAudio)).
		To(goav.Write("archive.ogg", io.Discard)))
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
	_, err := mustRuntime().Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(-1))).
		To(goav.Write("archive.ogg", io.Discard)))
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
		job  *testRecipe
		want string
	}{
		{
			name: "fps",
			job: recipeOf(goav.FileInput("input.webm", strings.NewReader("")).
				Video().
				Decode().
				Encode(codec.VP9(codec.FPS(0))).
				To(goav.Write("preview.webm", io.Discard))),
			want: "encode FPS must be positive",
		},
		{
			name: "keyframe interval",
			job: recipeOf(goav.FileInput("input.webm", strings.NewReader("")).
				Video().
				Decode().
				Encode(codec.VP9(codec.KeyframeInterval(-1))).
				To(goav.Write("preview.webm", io.Discard))),
			want: "encode keyframe interval must be non-negative",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.job.BuildLive(context.Background())
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
	_, err := mustRuntime().Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Encode(codec.Opus(codec.SampleRate(0))).
		To(goav.Write("archive.ogg", io.Discard)))
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
	_, err := mustRuntime(bundle.Formats()).Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(goav.Write("archive.ivf", io.Discard)))
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
	rt := mustRuntime(runconfig.WithFormatAdapter(func(registry *format.SimpleRegistry) {
		registry.RegisterProber(recipeAPIStreamProber{streams: streams})
		registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{called: &demuxerOpened})
	}))

	_, err := rt.Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))))
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
	rt := mustRuntime(runconfig.WithFormatAdapter(func(registry *format.SimpleRegistry) {
		registry.RegisterProber(recipeAPIStreamProber{streams: streams})
		registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{called: &demuxerOpened})
	}))

	_, err := rt.Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))))
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "decode_adapter_missing" || !errors.Is(err, codec.ErrNotFound) {
		t.Fatalf("err = %v, want decode_adapter_missing wrapping codec.ErrNotFound", err)
	}
	if demuxerOpened {
		t.Fatal("demuxer opened before known decoder preflight failed")
	}
	if !strings.Contains(err.Error(), "codec=opus") ||
		!strings.Contains(err.Error(), "input.To(output)") ||
		strings.Contains(err.Error(), "cannot open input") ||
		strings.Contains(err.Error(), "stream_ambiguous") {
		t.Fatalf("err = %v, want probed decoder guidance before input diagnostics", err)
	}
}

func TestStreamRecipeReportsMissingTransformAdapterBeforeOpeningInput(t *testing.T) {
	_, err := mustRuntime(bundle.Codecs()).Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Resample(16_000, codec.Mono).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))))
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "transform_adapter_missing" || !errors.Is(err, filter.ErrNotFound) {
		t.Fatalf("err = %v, want transform_adapter_missing wrapping filter.ErrNotFound", err)
	}
	if !strings.Contains(err.Error(), "transform=resample") ||
		!strings.Contains(err.Error(), "bundle.Filters()") ||
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
		runconfig.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: streams})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{called: &demuxerOpened})
		}),
		runconfig.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, recipeAPIDecoderFactory{})
		}),
		runconfig.WithFilterAdapter(func(registry *filter.SimpleRegistry) {
			registry.RegisterFactory(filter.Descriptor{
				Name:   filter.FactoryResample,
				Input:  av.MediaVideo,
				Output: av.MediaVideo,
			}, recipeAPIFilterFactory{})
		}),
	)
	_, err := rt.Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Resample(16_000, codec.Mono).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))))
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
		spec codec.Spec
		code errcode.Code
	}{
		{name: "auto", spec: codec.Auto(), code: "encode_auto_unresolved"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := mustRuntime().Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
				Audio().
				Decode().
				Encode(tt.spec).
				To(goav.Write("archive.ogg", io.Discard)))
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
	task, err := job.BuildLive(context.Background())
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
	if err := bundle.MustNew().Run(context.Background(), goav.FileInput("input.ivf", bytes.NewReader(tinyIVF())).To(goav.Write("recording.ivf", &recording), goav.Write("preview.ivf", &preview))); err != nil {
		t.Fatal(err)
	}
	if recording.Len() == 0 || preview.Len() == 0 {
		t.Fatalf("recording=%d preview=%d, want both non-empty", recording.Len(), preview.Len())
	}
}

func TestFromFanoutRecipeDescribeMatchesBuiltGraph(t *testing.T) {
	var recording bytes.Buffer
	var preview bytes.Buffer
	rt := bundle.MustNew()
	job := goav.FileInput("input.ivf", bytes.NewReader(tinyIVF())).To(goav.Write("recording.ivf", &recording), goav.Write("preview.ivf", &preview))

	planned, err := rt.Describe(job)
	if err != nil {
		t.Fatal(err)
	}
	task, err := rt.Build(context.Background(), job)
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

	task, err := job.BuildLive(context.Background())
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
}

func TestBranchRecipeComposesAudioAndVideoIntoSharedOutput(t *testing.T) {
	web := goav.Write("out.webm", io.Discard)
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
}

func TestBranchRecipeRejectsDuplicateDestinations(t *testing.T) {
	web := goav.Write("web.webm", io.Discard)
	web2 := goav.Write("web.webm", io.Discard)
	_, err := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("720p").Encode(codec.VP9(codec.Bitrate(2_000_000))).To(web).
		Video("360p").Encode(codec.VP9(codec.Bitrate(600_000))).To(web2).
		BuildLive(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_duplicate" {
		t.Fatalf("err = %v, want destination_duplicate with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), `destination "web.webm"`) ||
		!strings.Contains(err.Error(), "same destination value") {
		t.Fatalf("err = %v, want duplicate destination guidance", err)
	}
}

func TestBranchRecipeRejectsDuplicateBranchDestinations(t *testing.T) {
	web := goav.Write("web.webm", io.Discard)
	_, err := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("720p").Encode(codec.VP9(codec.Bitrate(2_000_000))).To(web, web).
		BuildLive(context.Background())

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
		BuildLive(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_invalid" {
		t.Fatalf("err = %v, want stream_invalid with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), `two forks are named "720p"`) ||
		!strings.Contains(err.Error(), `.As("hd") and .As("sd")`) {
		t.Fatalf("err = %v, want duplicate branch guidance", err)
	}
}

func TestBranchRecipeRejectsInvalidDestination(t *testing.T) {
	_, err := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("360p").Encode(codec.VP9(codec.Bitrate(600_000))).
		To(goav.Write("preview.webm", nil)).
		BuildLive(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_writer_missing" {
		t.Fatalf("err = %v, want output_writer_missing with matching BuildError code", err)
	}
}

func TestBranchCompositionAcceptsSinkAfterResize(t *testing.T) {
	job := goav.FileInput("input.webm", strings.NewReader("")).
		Video().
		Decode().
		Resize(320, 180).
		As("thumb").
		To(goav.Sink(component.SinkFunc("thumbnail", func(context.Context, component.Message) error {
			return nil
		})))

	spec, err := mustRuntime().Describe(job)
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
	in := goav.FileInput("input.webm", strings.NewReader(""))
	trunk := in.
		Video().
		Decode().
		Resize(1280, 720).
		Tap("video.720p.frames")
	job := []goav.Output{
		trunk.
			Encode(codec.VP9(codec.Bitrate(2_000_000))).
			As("web").
			To(web),
		trunk.
			Resize(320, 180).
			As("thumb").
			To(thumbnail),
	}

	spec, err := mustRuntime().Describe(job...)
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
		runconfig.WithFormatAdapter(func(registry *format.SimpleRegistry) {
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
		runconfig.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP9, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
		}),
		runconfig.WithFilterAdapter(func(registry *filter.SimpleRegistry) {
			registry.RegisterFactory(filter.Descriptor{Name: filter.FactoryResize, Input: av.MediaVideo, Output: av.MediaVideo}, recipeAPIFilterFactory{})
		}),
	)

	web := goav.Write("web.ogg", io.Discard)
	thumbnail := goav.Sink(component.SinkFunc("thumbnail", func(context.Context, component.Message) error {
		return nil
	}))
	in := goav.FileInput("input.ogg", strings.NewReader(""))
	trunk := in.
		Video().
		Decode().
		Resize(1280, 720).
		Tap("video.720p.frames")
	report, err := rt.Explain(context.Background(),
		trunk.
			Encode(codec.VP9(codec.Bitrate(2_000_000))).
			As("web").
			To(web),
		trunk.
			Resize(320, 180).
			As("thumb").
			To(thumbnail),
	)
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
	in := goav.FileInput("input.webm", strings.NewReader(""))
	trunk := in.
		Video().
		Decode().
		Do(meter)
	job := []goav.Output{
		trunk.
			Encode(codec.VP9(codec.Bitrate(2_000_000))).
			As("web").
			To(packets),
		trunk.
			Resize(320, 180).
			As("preview").
			To(preview),
	}

	spec, err := mustRuntime().Describe(job...)
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

func TestBranchRecipeRejectsWrongMediaTransform(t *testing.T) {
	_, err := goavtest.Runtime().Build(context.Background(), goavtest.Packets(av.CodecOpus, av.Packet{Payload: av.Buffer{Bytes: []byte{1}}}).
		Audio().
		Decode().
		Resize(640, 360).
		As("bad").
		To(goavtest.NewCollector().Sink()))

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
		BuildLive(context.Background())

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
	_, err := mustRuntime().Build(context.Background(), goav.FileInput("input.webm", strings.NewReader("")).
		Video().
		Decode().
		Tap("video.decoded").
		Encode(codec.VP9(codec.Bitrate(600_000))).
		Resize(640, 360).
		As("360p").
		To(goav.Write("preview.webm", io.Discard)))

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
	_, err := mustRuntime().Build(context.Background(), goav.FileInput("input.webm", strings.NewReader("")).
		Video().
		Decode().
		Tap("video.decoded").
		Encode(codec.VP9(codec.Bitrate(600_000))).
		Encode(codec.VP8(codec.Bitrate(400_000))).
		As("360p").
		To(goav.Write("preview.webm", io.Discard)))

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_duplicate" {
		t.Fatalf("err = %v, want encode_duplicate with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "first encoder: vp9") ||
		!strings.Contains(err.Error(), "second encoder: vp8") ||
		!strings.Contains(err.Error(), "several encodings") {
		t.Fatalf("err = %v, want duplicate transcode encoder guidance", err)
	}
}

func TestBranchRecipeRejectsNegativeEncodeBitrate(t *testing.T) {
	_, err := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("bad").Encode(codec.VP9(codec.Bitrate(-1))).
		To(goav.Write("bad.webm", io.Discard)).
		BuildLive(context.Background())

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
