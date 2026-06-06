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
	"reflect"
	"strings"
	"testing"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/graphrender"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/rtpav"
	"github.com/thesyncim/goav/webrtcav"
)

type recipeAPIRTPReader struct{}

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

type recipeAPIRuntimeWithoutBuilder struct{}

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

func (recipeAPIRuntimeWithoutBuilder) Probe(context.Context, format.ProbeRequest) (format.ProbeResult, error) {
	return format.ProbeResult{}, nil
}

func (recipeAPIRuntimeWithoutBuilder) Graph() goav.GraphBuilder {
	return goav.Default().Graph()
}

func (recipeAPIRTPReader) Streams(context.Context) ([]goav.Stream, error) {
	return []goav.Stream{{ID: "audio", Type: "audio"}}, nil
}

func (recipeAPIRTPReader) PayloadMap() rtpav.PayloadMap {
	return nil
}

func (recipeAPIRTPReader) ReadRTP(context.Context) (*rtp.Packet, error) {
	return nil, io.EOF
}

func (recipeAPIRTPReader) Events() <-chan goav.Event {
	return nil
}

func (recipeAPIRTPReader) Close() error {
	return nil
}

func specText(spec pipeline.Spec) string {
	out, err := graphrender.RenderURI(spec, "goav:graph")
	if err != nil {
		return err.Error()
	}
	return out
}

func hasRequirement(requirements []goav.AdapterRequirement, kind string, codecID av.CodecID, formatID av.FormatID) bool {
	for i := range requirements {
		requirement := requirements[i]
		if requirement.Kind != kind {
			continue
		}
		if codecID != "" && requirement.Codec != codecID {
			continue
		}
		if formatID != "" && requirement.Format != formatID {
			continue
		}
		return true
	}
	return false
}

func operationKinds(operations []goav.OperationReport) []goav.OperationKind {
	kinds := make([]goav.OperationKind, 0, len(operations))
	for i := range operations {
		kinds = append(kinds, operations[i].Kind)
	}
	return kinds
}

func equalOperationKinds(a []goav.OperationKind, b []goav.OperationKind) bool {
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

func branchByName(branches []goav.BranchReport, name string) (goav.BranchReport, bool) {
	for i := range branches {
		if branches[i].Name == name {
			return branches[i], true
		}
	}
	return goav.BranchReport{}, false
}

func recordJob(input goav.InputSpec, outputs ...goav.OutputSpec) *goav.Job {
	return goav.From(input).Copy().To(outputs...)
}

func decodeJob(input goav.InputSpec, output goav.OutputSpec) *goav.Job {
	return goav.From(input).Stream().Decode().To(output)
}

type testTranscodeJob struct {
	input    goav.InputSpec
	runtime  goav.Runtime
	branches []testTranscodeBranch
	outputs  []testTranscodeOutput
}

type testTranscodeBranch struct {
	name       string
	media      av.MediaType
	flows      []goav.Flow
	transforms []goav.TransformSpec
	encode     goav.CodecSpec
	labels     []string
}

type testTranscodeOutput struct {
	name   string
	output goav.OutputSpec
}

type testTranscodeBranchBuilder struct {
	job   *testTranscodeJob
	index int
}

func transcodeJob(input goav.InputSpec) *testTranscodeJob {
	return &testTranscodeJob{input: input}
}

func (j *testTranscodeJob) UseRuntime(runtime goav.Runtime) *testTranscodeJob {
	j.runtime = runtime
	return j
}

func (j *testTranscodeJob) Audio(name string) *testTranscodeBranchBuilder {
	return j.stream(name, av.MediaAudio)
}

func (j *testTranscodeJob) Video(name string) *testTranscodeBranchBuilder {
	return j.stream(name, av.MediaVideo)
}

func (j *testTranscodeJob) stream(name string, media av.MediaType) *testTranscodeBranchBuilder {
	j.branches = append(j.branches, testTranscodeBranch{name: name, media: media})
	return &testTranscodeBranchBuilder{job: j, index: len(j.branches) - 1}
}

func (j *testTranscodeJob) Output(name string, output goav.OutputSpec) *testTranscodeJob {
	j.outputs = append(j.outputs, testTranscodeOutput{name: name, output: output})
	return j
}

func (j *testTranscodeJob) materialize() *goav.Job {
	job := goav.From(j.input)
	if j.runtime != nil {
		job.UseRuntime(j.runtime)
	}
	for i := range j.branches {
		branch := j.branches[i]
		var stream *goav.JobStreamBuilder
		switch branch.media {
		case av.MediaAudio:
			stream = job.Audio().Decode().Tap("audio.decoded")
		case av.MediaVideo:
			stream = job.Video().Decode().Tap("video.decoded")
		default:
			stream = job.Stream().Decode().Tap("stream.decoded")
		}
		builder := stream.Branch(branch.name)
		for _, flow := range branch.flows {
			builder = builder.Apply(flow)
		}
		for _, transform := range branch.transforms {
			if transform.Resize != nil {
				resize := transform.Resize
				builder = builder.Resize(resize.Width, resize.Height)
			}
			if transform.Resample != nil {
				resample := transform.Resample
				builder = builder.Resample(resample.SampleRate, resample.Channels)
			}
		}
		if branch.encode.ID != "" {
			builder = builder.Encode(branch.encode)
		}
		job = builder.To(branch.labels...)
	}
	for i := range j.outputs {
		job.Output(j.outputs[i].name, j.outputs[i].output)
	}
	return job
}

func (j *testTranscodeJob) Intent() goav.Intent {
	return j.materialize().Intent()
}

func (j *testTranscodeJob) Explain(ctx context.Context) (goav.PlanReport, error) {
	return j.materialize().Explain(ctx)
}

func (j *testTranscodeJob) Describe() (pipeline.Spec, error) {
	return j.materialize().Describe()
}

func (j *testTranscodeJob) Build(ctx context.Context) (goav.Task, error) {
	return j.materialize().Build(ctx)
}

func (j *testTranscodeJob) Run(ctx context.Context) error {
	return j.materialize().Run(ctx)
}

func (b *testTranscodeBranchBuilder) Apply(flow goav.Flow) *testTranscodeBranchBuilder {
	b.current().flows = append(b.current().flows, flow)
	return b
}

func (b *testTranscodeBranchBuilder) Resize(width int, height int) *testTranscodeBranchBuilder {
	b.current().transforms = append(b.current().transforms, goav.Resize(width, height))
	return b
}

func (b *testTranscodeBranchBuilder) Resample(sampleRate int, channels int) *testTranscodeBranchBuilder {
	b.current().transforms = append(b.current().transforms, goav.Resample(sampleRate, channels))
	return b
}

func (b *testTranscodeBranchBuilder) Opus(bitrate int) *testTranscodeBranchBuilder {
	return b.Encode(goav.Opus(goav.Bitrate(bitrate)))
}

func (b *testTranscodeBranchBuilder) VP8(bitrate int) *testTranscodeBranchBuilder {
	return b.Encode(goav.VP8(goav.Bitrate(bitrate)))
}

func (b *testTranscodeBranchBuilder) VP9(bitrate int) *testTranscodeBranchBuilder {
	return b.Encode(goav.VP9(goav.Bitrate(bitrate)))
}

func (b *testTranscodeBranchBuilder) Encode(codec goav.CodecSpec) *testTranscodeBranchBuilder {
	b.current().encode = codec
	return b
}

func (b *testTranscodeBranchBuilder) To(labels ...string) *testTranscodeJob {
	b.current().labels = append([]string(nil), labels...)
	return b.job
}

func (b *testTranscodeBranchBuilder) current() *testTranscodeBranch {
	return &b.job.branches[b.index]
}

func TestRuntimeInterfaceKeepsLegacyBuilderOutOfFrontDoor(t *testing.T) {
	runtimeType := reflect.TypeOf((*goav.Runtime)(nil)).Elem()
	if _, ok := runtimeType.MethodByName("New"); ok {
		t.Fatal("Runtime exposes legacy New builder; use Runtime.Graph for expert graph wiring")
	}
	if _, ok := runtimeType.MethodByName("Graph"); !ok {
		t.Fatal("Runtime should expose Graph as the expert graph entry point")
	}
}

func TestRecipesExposeStructuredExplain(t *testing.T) {
	jobType := reflect.TypeOf((*goav.Job)(nil))
	if _, ok := jobType.MethodByName("Explain"); !ok {
		t.Fatal("Job should expose Explain for structured workflow reports")
	}
	reportType := reflect.TypeOf(goav.PlanReport{})
	for _, method := range []string{"Text", "Mermaid", "DOT", "Render"} {
		if _, ok := reportType.MethodByName(method); ok {
			t.Fatalf("PlanReport exposes renderer method %s; keep rendering outside core", method)
		}
	}
}

func TestRecordRecipeExplainReturnsStructuredPlan(t *testing.T) {
	report, err := recordJob(
		goav.RTP(recipeAPIRTPReader{}).Name("video").Codec(goav.VP8()),
		goav.FileOutput("recording.ivf", io.Discard),
	).Explain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Summary == "" || report.Operation != "build job" || report.Intent.Name != "from" {
		t.Fatalf("report summary=%q operation=%q intent=%q", report.Summary, report.Operation, report.Intent.Name)
	}
	if len(report.Graph.Nodes) == 0 || len(report.Graph.Edges) == 0 {
		t.Fatalf("empty graph: %+v", report.Graph)
	}
	if len(report.Inputs) != 1 || report.Inputs[0].Name != "video" ||
		report.Inputs[0].Format != av.FormatRTP || report.Inputs[0].Codec != av.CodecVP8 ||
		!report.Inputs[0].Realtime {
		t.Fatalf("inputs=%+v", report.Inputs)
	}
	if len(report.Outputs) != 1 || report.Outputs[0].Name != "recording.ivf" ||
		report.Outputs[0].Format != av.FormatIVF || report.Outputs[0].Kind != "mux" {
		t.Fatalf("outputs=%+v", report.Outputs)
	}
	if !equalStrings(report.Outputs[0].Branches, []string{"video"}) {
		t.Fatalf("output branches=%+v", report.Outputs[0].Branches)
	}
	if len(report.Branches) != 1 || report.Branches[0].Name != "video" ||
		!equalOperationKinds(operationKinds(report.Branches[0].Operations), []goav.OperationKind{goav.OpDepacketize, goav.OpCopy}) {
		t.Fatalf("branches=%+v", report.Branches)
	}
	if !hasRequirement(report.RequiredAdapters, "rtp-depacketizer", av.CodecVP8, "") ||
		!hasRequirement(report.RequiredAdapters, "muxer", "", av.FormatIVF) {
		t.Fatalf("requirements=%+v", report.RequiredAdapters)
	}
	if len(report.Warnings) != 0 {
		t.Fatalf("warnings=%+v", report.Warnings)
	}
}

func TestTranscodeExplainReportsGenericMediaPlanBranches(t *testing.T) {
	rt := goav.New(
		goav.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{
				{Index: 0, ID: "audio", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}},
				{Index: 1, ID: "video", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo}},
			}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
			registry.RegisterMuxer(av.FormatOgg, recipeAPIMuxerFactory{})
		}),
		goav.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, recipeAPIDecoderFactory{})
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, recipeAPIEncoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP9, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
		}),
		goav.WithFilterAdapter(func(registry *filter.SimpleRegistry) {
			registry.RegisterFactory(filter.Descriptor{Name: filter.FactoryResize, Input: av.MediaVideo, Output: av.MediaVideo}, recipeAPIFilterFactory{})
			registry.RegisterFactory(filter.Descriptor{Name: filter.FactoryResample, Input: av.MediaAudio, Output: av.MediaAudio}, recipeAPIFilterFactory{})
		}),
	)

	report, err := transcodeJob(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Video("v").Resize(1280, 720).VP9(2_000_000).To("web").
		Audio("a").Resample(48_000, goav.Stereo).Opus(96_000).To("web").
		Output("web", goav.FileOutput("web.ogg", io.Discard)).
		Explain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Branches) != 2 {
		t.Fatalf("branches=%+v", report.Branches)
	}
	if len(report.Outputs) != 1 || report.Outputs[0].Name != "web" || !equalStrings(report.Outputs[0].Branches, []string{"v", "a"}) {
		t.Fatalf("outputs=%+v", report.Outputs)
	}
	want := []goav.OperationKind{goav.OpDemux, goav.OpSelect, goav.OpDecode, goav.OpTransform, goav.OpEncode}
	for _, name := range []string{"v", "a"} {
		branch, ok := branchByName(report.Branches, name)
		if !ok {
			t.Fatalf("missing branch %q in %+v", name, report.Branches)
		}
		if !equalOperationKinds(operationKinds(branch.Operations), want) {
			t.Fatalf("%s operations=%+v", name, branch.Operations)
		}
		if len(branch.Outputs) != 1 || branch.Outputs[0] != "web" {
			t.Fatalf("%s outputs=%+v", name, branch.Outputs)
		}
		for _, operation := range branch.Operations {
			if operation.Kind == goav.OperationKind("transcode") {
				t.Fatalf("branch %s has special transcode operation: %+v", name, branch.Operations)
			}
		}
	}
	if len(report.Decisions) < 4 {
		t.Fatalf("decisions=%+v, want decode and encode decisions per branch", report.Decisions)
	}
}

func TestPackageKeepsLegacyHelpersOutOfFrontDoor(t *testing.T) {
	packages, err := parser.ParseDir(token.NewFileSet(), ".", func(info os.FileInfo) bool {
		return !strings.HasSuffix(info.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	pkg, ok := packages["goav"]
	if !ok {
		t.Fatal("package goav not found")
	}
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
	}
	legacyTypes := map[string]bool{
		"Builder":         true,
		"Input":           true,
		"Output":          true,
		"AudioOption":     true,
		"RecordOption":    true,
		"ProbeRequest":    true,
		"ProbeResult":     true,
		"ResizeOption":    true,
		"Source":          true,
		"Stage":           true,
		"Sink":            true,
		"Metadata":        true,
		"CodecParameters": true,
		"CodecOption":     true,
		"JobOption":       true,
		"RTPOption":       true,
		"RTPInputOption":  true,
		"StreamBuilder":   true,
		"StreamOption":    true,
		"TrackOption":     true,
		"TranscodeJob":    true,
	}
	for filename, file := range pkg.Files {
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

func TestTeeIsTheOnlyPublicFlowBranchVerb(t *testing.T) {
	streamType := reflect.TypeOf((*goav.JobStreamBuilder)(nil))
	if _, ok := streamType.MethodByName("Tee"); !ok {
		t.Fatal("JobStreamBuilder should expose Tee for flow branches")
	}
	if _, ok := streamType.MethodByName("Fork"); ok {
		t.Fatal("JobStreamBuilder should not expose Fork; Tee is the public branch verb")
	}
}

func TestRuntimeBranchTapAnchorIsPublicAPI(t *testing.T) {
	graph := goav.Default().Graph()
	source := graph.Source("source", recipeAPISource{name: "source"})
	decode := graph.Stage("decode-audio", goav.PacketFunc("decode-audio", func(ctx context.Context, packet *goav.Packet, emit goav.Emit) error {
		return emit.Packet(packet)
	}))
	base := graph.Sink("base", goav.SinkFunc("base", func(context.Context, goav.Message) error {
		return nil
	}))
	graph.Connect(source.Out(), decode.In())
	graph.Connect(decode.Out(), base.In())
	task, err := graph.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	attachment, err := task.Attach(context.Background(),
		goav.Branch("levels").
			FromTap("audio.decoded").
			To(goav.SinkFunc("levels", func(context.Context, goav.Message) error {
				return nil
			})),
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

func TestReadmeKeepsAdvancedRuntimeKnobsOutOfFrontDoor(t *testing.T) {
	body, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, advanced := range []string{
		"WithFormatAdapter",
		"UseRuntime",
		"RTPBuffer",
		"MaxTimestampGap",
	} {
		if strings.Contains(text, advanced) {
			t.Fatalf("README exposes %s in the front-door guide", advanced)
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
	end := strings.Index(text, "## Adapter-Backed Workflows")
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
		"goav.PacketFunc or goav.FrameFunc meter",
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
	pkg, err := parser.ParseDir(token.NewFileSet(), ".", func(info os.FileInfo) bool {
		return !strings.HasSuffix(info.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	files := pkg["goav"].Files
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

func TestRecipeConstructorsDoNotExposeRuntimeOptions(t *testing.T) {
	inputType := reflect.TypeOf(goav.InputSpec{})
	jobType := reflect.TypeOf((*goav.Job)(nil))

	cases := []struct {
		name string
		fn   any
		in   []reflect.Type
		out  reflect.Type
	}{
		{name: "From", fn: goav.From, in: []reflect.Type{inputType}, out: jobType},
	}
	for _, tc := range cases {
		typ := reflect.TypeOf(tc.fn)
		if typ.IsVariadic() || typ.NumIn() != len(tc.in) || typ.NumOut() != 1 || typ.Out(0) != tc.out {
			t.Fatalf("%s type = %s", tc.name, typ)
		}
		for i := range tc.in {
			if typ.In(i) != tc.in[i] {
				t.Fatalf("%s type = %s", tc.name, typ)
			}
		}
	}
}

func TestRecipeReportsRuntimeWithoutCompilerBuilder(t *testing.T) {
	_, err := recordJob(
		goav.FileInput("input.ivf", strings.NewReader("")),
		goav.FileOutput("recording.ivf", io.Discard),
	).UseRuntime(recipeAPIRuntimeWithoutBuilder{}).Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "runtime_builder_missing" {
		t.Fatalf("err = %v, want runtime_builder_missing", err)
	}
	if !strings.Contains(err.Error(), "runtime cannot compile recipe jobs") ||
		!strings.Contains(err.Error(), "goav.Default") {
		t.Fatalf("err = %v, want runtime guidance", err)
	}
}

func TestReadmeRecordRecipeIsSmall(t *testing.T) {
	job := recordJob(
		goav.FileInput("input.ogg", strings.NewReader("")),
		goav.FileOutput("recording.ogg", io.Discard),
	)

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(specText(spec), "input.ogg -> recording.ogg") {
		t.Fatalf("spec:\n%s", specText(spec))
	}
	intent := job.Intent()
	if intent.Name != "from" || len(intent.Inputs) != 1 || len(intent.Outputs) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestReadmeRecordFanoutRecipeIsSmall(t *testing.T) {
	job := recordJob(
		goav.FileInput("input.ivf", strings.NewReader("")),
		goav.FileOutput("archive.ivf", io.Discard),
		goav.FileOutput("preview.ivf", io.Discard),
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
	intent := job.Intent()
	if intent.Name != "from" || len(intent.Inputs) != 1 || len(intent.Outputs) != 2 {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestReadmeAudioDecodeRecipeIsSmall(t *testing.T) {
	sink := goav.SinkFunc("frames", func(context.Context, goav.Message) error {
		return nil
	})
	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		To(goav.FrameSink(sink))

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
	intent := job.Intent()
	if len(intent.Streams) != 1 || intent.Streams[0].Select.Type != "audio" || !intent.Streams[0].Decode {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestReadmeCustomStageToCustomSinkRecipeIsSmall(t *testing.T) {
	meter := goav.FrameFunc("meter", func(ctx context.Context, frame *goav.Frame, emit goav.Emit) error {
		return emit.Frame(frame)
	})
	sink := goav.SinkFunc("levels", func(context.Context, goav.Message) error {
		return nil
	})

	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		Do(meter).
		To(goav.FrameSink(sink))

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "decode-audio -> meter") ||
		!strings.Contains(text, "meter -> levels") {
		t.Fatalf("spec:\n%s", text)
	}
	intent := job.Intent()
	if len(intent.Streams) != 1 || !intent.Streams[0].Decode || intent.Streams[0].Encode.ID != "" ||
		len(intent.Outputs) != 1 || intent.Outputs[0].Name != "levels" {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestStreamRecipeNamesCodecChangePolicy(t *testing.T) {
	sink := goav.SinkFunc("frames", func(context.Context, goav.Message) error {
		return nil
	})
	policy := goav.RealtimeCodecChangePolicy()
	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		OnCodecChange(policy).
		To(goav.FrameSink(sink))

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "codec-change=rebind-compatible,request-keyframe,drop-until-sync,fail-different-codec") {
		t.Fatalf("spec:\n%s", text)
	}
	intent := job.Intent()
	if len(intent.Streams) != 1 || intent.Streams[0].CodecChange != policy {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestAudioFlowAppliesToStreamRecipeIntent(t *testing.T) {
	voice := goav.AudioFlow("voice").
		Resample(16_000, goav.Mono).
		OpusVoice()

	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Apply(voice).
		To(goav.FileOutput("voice.ogg", io.Discard))

	intent := job.Intent()
	if len(intent.Streams) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
	stream := intent.Streams[0]
	if stream.Select.Type != av.MediaAudio || !stream.Decode ||
		len(stream.Transforms) != 1 || stream.Transforms[0].Resample == nil ||
		stream.Transforms[0].Resample.SampleRate != 16_000 ||
		stream.Transforms[0].Resample.Channels != goav.Mono ||
		stream.Encode.ID != av.CodecOpus || stream.Encode.Bitrate != 32_000 ||
		len(stream.RouteTo) != 1 || stream.RouteTo[0] != "voice.ogg" {
		t.Fatalf("stream intent: %+v", stream)
	}
}

func TestFlowTeeStaysOnJobAndBuildsBranchIntent(t *testing.T) {
	voice := goav.AudioFlow("voice").
		Resample(16_000, goav.Mono).
		OpusVoice()
	archive := goav.AudioFlow("archive").
		Resample(48_000, goav.Stereo).
		OpusMusic()

	job := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Audio(goav.StreamIndex(0)).
		Tee(
			voice.To(goav.FileOutput("voice.ogg", io.Discard)),
			archive.To(goav.FileOutput("archive.ogg", io.Discard)),
		)

	if reflect.TypeOf(job) != reflect.TypeOf((*goav.Job)(nil)) {
		t.Fatalf("Tee returned %T, want *goav.Job", job)
	}
	intent := job.Intent()
	if len(intent.Streams) != 2 || len(intent.Outputs) != 2 {
		t.Fatalf("intent: %+v", intent)
	}
	if intent.Streams[0].Name != "voice" || intent.Streams[1].Name != "archive" ||
		!intent.Streams[0].Select.UseIndex || intent.Streams[0].Select.Index != 0 ||
		intent.Streams[0].Encode.ID != av.CodecOpus || intent.Streams[1].Encode.ID != av.CodecOpus ||
		intent.Streams[0].Transforms[0].Resample.SampleRate != 16_000 ||
		intent.Streams[1].Transforms[0].Resample.SampleRate != 48_000 {
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
	preview := goav.VideoFlow("preview").
		Resize(640, 360).
		VP9(600_000)

	job := transcodeJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("preview").
		Apply(preview).
		To("web").
		Output("web", goav.FileOutput("preview.webm", io.Discard))

	intent := job.Intent()
	if len(intent.Streams) != 1 || intent.Streams[0].Name != "preview" ||
		len(intent.Streams[0].Transforms) != 1 ||
		intent.Streams[0].Transforms[0].Resize.Width != 640 ||
		intent.Streams[0].Transforms[0].Resize.Height != 360 ||
		intent.Streams[0].Encode.ID != av.CodecVP9 ||
		intent.Streams[0].Encode.Bitrate != 600_000 {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestVariantsGroupSelectedStreamBranches(t *testing.T) {
	job := goav.From(goav.FileInput("source.webm", strings.NewReader(""))).
		Video().
		Decode().
		Tap("video.decoded").
		Variants(
			goav.Variant("v1080").
				Resize(1920, 1080).
				VP9(4_000_000).
				To("watch"),
			goav.Variant("v360").
				Resize(640, 360).
				VP8(600_000).
				To("mobile"),
		).
		Audio().
		Decode().
		Tap("audio.decoded").
		Variants(
			goav.Variant("a96").
				Resample(48_000, goav.Stereo).
				Opus(96_000).
				To("watch", "mobile"),
		).
		Output("watch", goav.FileOutput("watch.webm", io.Discard)).
		Output("mobile", goav.FileOutput("mobile.webm", io.Discard))

	intent := job.Intent()
	if len(intent.Streams) != 3 || len(intent.Outputs) != 2 {
		t.Fatalf("intent: %+v", intent)
	}
	tests := []struct {
		name    string
		fromTap string
		codec   av.CodecID
		outputs []string
	}{
		{name: "v1080", fromTap: "video.decoded", codec: av.CodecVP9, outputs: []string{"watch"}},
		{name: "v360", fromTap: "video.decoded", codec: av.CodecVP8, outputs: []string{"mobile"}},
		{name: "a96", fromTap: "audio.decoded", codec: av.CodecOpus, outputs: []string{"watch", "mobile"}},
	}
	for i := range tests {
		stream := intent.Streams[i]
		if stream.Name != tests[i].name || stream.FromTap != tests[i].fromTap ||
			stream.Encode.ID != tests[i].codec || !equalStrings(stream.RouteTo, tests[i].outputs) {
			t.Fatalf("stream[%d]=%+v, want %+v", i, stream, tests[i])
		}
	}
	if intent.Streams[0].Transforms[0].Resize.Width != 1920 ||
		intent.Streams[1].Transforms[0].Resize.Width != 640 ||
		intent.Streams[2].Transforms[0].Resample.SampleRate != 48_000 {
		t.Fatalf("variant transforms: %+v", intent.Streams)
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

func TestFlowMediaMismatchIsActionable(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Apply(goav.AudioFlow("voice").OpusVoice()).
		To(goav.FileOutput("voice.webm", io.Discard)).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "flow_media_mismatch" {
		t.Fatalf("err = %v, want flow_media_mismatch", err)
	}
	if !strings.Contains(err.Error(), "AudioFlow") || !strings.Contains(err.Error(), "VideoFlow") {
		t.Fatalf("err = %v, want flow guidance", err)
	}
}

func TestFlowBranchSnapshotsBuilderState(t *testing.T) {
	flow := goav.AudioFlow("voice").
		Resample(16_000, goav.Mono).
		OpusVoice()
	branch := flow.To(goav.FileOutput("voice.ogg", io.Discard))

	flow.Resample(8_000, goav.Mono)
	job := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Audio().
		Tee(branch)

	intent := job.Intent()
	if len(intent.Streams) != 1 ||
		len(intent.Streams[0].Transforms) != 1 ||
		intent.Streams[0].Transforms[0].Resample.SampleRate != 16_000 {
		t.Fatalf("intent after mutating flow: %+v", intent)
	}
}

func TestNilFlowIsActionable(t *testing.T) {
	var flow *goav.AudioFlowBuilder
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Apply(flow).
		To(goav.FileOutput("voice.ogg", io.Discard)).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "flow_invalid" {
		t.Fatalf("err = %v, want flow_invalid", err)
	}
}

func TestNilFlowBranchIsActionable(t *testing.T) {
	var flow *goav.AudioFlowBuilder
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Tee(flow.To(goav.FileOutput("voice.ogg", io.Discard))).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "flow_invalid" {
		t.Fatalf("err = %v, want flow_invalid", err)
	}
}

func TestFlowTeeRejectsOuterOutputsAndDuplicateTee(t *testing.T) {
	voice := goav.AudioFlow("voice").OpusVoice()

	_, err := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Audio().
		Tee(voice.To(goav.FileOutput("voice.ogg", io.Discard))).
		To(goav.FileOutput("ignored.ogg", io.Discard)).
		Describe()
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_scope_mixed" {
		t.Fatalf("err = %v, want output_scope_mixed", err)
	}

	job := goav.From(goav.FileInput("input.webm", strings.NewReader("")))
	audio := job.Audio()
	audio.Tee(voice.To(goav.FileOutput("voice.ogg", io.Discard)))
	_, err = audio.Tee(voice.To(goav.FileOutput("other.ogg", io.Discard))).Describe()
	if !errors.As(err, &buildErr) || buildErr.Code != "flow_duplicate" {
		t.Fatalf("err = %v, want flow_duplicate", err)
	}
}

func TestFlowRejectsTransformsAfterEncode(t *testing.T) {
	flow := goav.AudioFlow("voice").
		OpusVoice().
		Resample(16_000, goav.Mono)

	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Apply(flow).
		To(goav.FileOutput("voice.ogg", io.Discard)).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_step_after_encode" {
		t.Fatalf("err = %v, want stream_step_after_encode", err)
	}
	if buildErr.Operation != "build flow" || !strings.Contains(err.Error(), "resample") {
		t.Fatalf("err = %v, want flow resample guidance", err)
	}
}

func TestFlowTeeDescribesLiveInputBranches(t *testing.T) {
	voice := goav.AudioFlow("voice").OpusVoice()
	archive := goav.AudioFlow("archive").OpusMusic()

	job := goav.From(goav.RTP(recipeAPIRTPReader{}).Name("audio").Codec(goav.Opus())).
		Audio().
		Tee(
			voice.To(goav.FileOutput("voice.ogg", io.Discard)),
			archive.To(goav.FileOutput("archive.ogg", io.Discard)),
		)

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	for _, want := range []string{
		"audio -> select-audio",
		"select-audio -> decode-audio",
		"decode-audio -> encode-voice",
		"decode-audio -> encode-archive",
		"encode-voice -> voice.ogg",
		"encode-archive -> archive.ogg",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("spec missing %q:\n%s", want, text)
		}
	}
	intent := job.Intent()
	if len(intent.Streams) != 2 || len(intent.Outputs) != 2 ||
		intent.Streams[0].Select.Type != av.MediaAudio ||
		intent.Streams[0].Encode.ID != av.CodecOpus ||
		intent.Streams[1].Encode.ID != av.CodecOpus {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestStreamRecipeRejectsUnsupportedCodecChangePolicy(t *testing.T) {
	sink := goav.SinkFunc("frames", func(context.Context, goav.Message) error {
		return nil
	})
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		OnCodecChange(goav.CodecChangePolicy{RebindCompatible: true}).
		To(goav.FrameSink(sink)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "codec_change_policy_unsupported" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want codec_change_policy_unsupported wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "RealtimeCodecChangePolicy") ||
		!strings.Contains(err.Error(), "different decoder codec") {
		t.Fatalf("err = %v, want codec-change policy guidance", err)
	}
}

func TestReadmeDecodeShortcutUsesFrameSink(t *testing.T) {
	sink := goav.SinkFunc("frames", func(context.Context, goav.Message) error {
		return nil
	})
	job := decodeJob(
		goav.RTP(recipeAPIRTPReader{}).Codec(goav.Opus()),
		goav.FrameSink(sink),
	)

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "rtp -> select") ||
		!strings.Contains(text, "select -> decode") ||
		!strings.Contains(text, "decode -> frames") {
		t.Fatalf("spec:\n%s", text)
	}
	intent := job.Intent()
	if len(intent.Streams) != 1 || !intent.Streams[0].Decode {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestWebRTCTrackRecipeReportsNilTrack(t *testing.T) {
	_, err := recordJob(
		goav.WebRTCTrack(nil),
		goav.FileOutput("recording.ivf", io.Discard),
	).Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "input_invalid" || !errors.Is(err, webrtcav.ErrNilTrack) {
		t.Fatalf("err = %v, want input_invalid wrapping ErrNilTrack", err)
	}
}

func TestRecipeAndRejectsMultipleFileInputs(t *testing.T) {
	_, err := goav.From(goav.FileInput("a.ivf", strings.NewReader(""))).
		And(goav.FileInput("b.ivf", strings.NewReader(""))).
		To(goav.FileOutput("out.ivf", io.Discard)).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "multi_input_unsupported" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want multi_input_unsupported wrapping ErrUnsupportedBuild", err)
	}
}

func TestRecipeAndRejectsDuplicateRealtimeInputNames(t *testing.T) {
	_, err := goav.From(goav.RTP(recipeAPIRTPReader{}).Name("media").Codec(goav.Opus())).
		And(goav.RTP(recipeAPIRTPReader{}).Name("media").Codec(goav.VP8())).
		To(goav.FileOutput("recording.webm", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "input_duplicate" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want input_duplicate wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), `realtime input name "media"`) ||
		!strings.Contains(err.Error(), "second input index: 1") ||
		!strings.Contains(err.Error(), "distinct .Name") {
		t.Fatalf("err = %v, want duplicate input guidance", err)
	}
}

func TestRecordRecipeRejectsEmptyInputSpec(t *testing.T) {
	_, err := recordJob(
		goav.InputSpec{},
		goav.FileOutput("recording.ogg", io.Discard),
	).Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "input_invalid" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want input_invalid wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "empty input spec") ||
		!strings.Contains(err.Error(), "goav.FileInput") {
		t.Fatalf("err = %v, want input constructor guidance", err)
	}
}

func TestDecodeRecipeRejectsNilFrameSink(t *testing.T) {
	_, err := decodeJob(
		goav.FileInput("input.ogg", strings.NewReader("")),
		goav.FrameSink(nil),
	).Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_invalid" || !errors.Is(err, goav.ErrNilSink) {
		t.Fatalf("err = %v, want output_invalid wrapping ErrNilSink", err)
	}
	if !strings.Contains(err.Error(), "non-nil sink") {
		t.Fatalf("err = %v, want frame sink guidance", err)
	}
}

func TestDecodeRecipeRejectsNilSinkFuncCallback(t *testing.T) {
	_, err := decodeJob(
		goav.FileInput("input.ogg", strings.NewReader("")),
		goav.FrameSink(goav.SinkFunc("frames", nil)),
	).Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_invalid" || !errors.Is(err, goav.ErrNilSink) {
		t.Fatalf("err = %v, want output_invalid wrapping ErrNilSink", err)
	}
	if !strings.Contains(err.Error(), "non-nil sink") {
		t.Fatalf("err = %v, want sink guidance", err)
	}
}

func TestDecodeRecipeRejectsMuxOutput(t *testing.T) {
	_, err := decodeJob(
		goav.FileInput("input.ogg", strings.NewReader("")),
		goav.FileOutput("frames.ogg", io.Discard),
	).Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_missing" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want encode_missing wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "goav.FrameSink") ||
		!strings.Contains(err.Error(), ".Copy().To(output)") {
		t.Fatalf("err = %v, want decode output guidance", err)
	}
}

func TestRecordRecipeRejectsFrameSinkOutput(t *testing.T) {
	_, err := recordJob(
		goav.FileInput("input.ogg", strings.NewReader("")),
		goav.FrameSink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
			return nil
		})),
	).Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_kind_invalid" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_kind_invalid wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "packet-preserving recipes") ||
		!strings.Contains(err.Error(), ".Stream().Decode().To(goav.FrameSink") ||
		!strings.Contains(err.Error(), "goav.FileOutput") {
		t.Fatalf("err = %v, want packet output guidance", err)
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

func TestRecordRecipeRejectsEmptyOutputSpec(t *testing.T) {
	_, err := recordJob(
		goav.FileInput("input.ogg", strings.NewReader("")),
		goav.OutputSpec{},
	).Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_invalid" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_invalid wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "empty output spec") ||
		!strings.Contains(err.Error(), "goav.FileOutput") {
		t.Fatalf("err = %v, want output constructor guidance", err)
	}
}

func TestRecordRecipeRejectsFileOutputWithoutWriter(t *testing.T) {
	_, err := recordJob(
		goav.FileInput("input.ogg", strings.NewReader("")),
		goav.FileOutput("recording.ogg", nil),
	).Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_writer_missing" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_writer_missing wrapping ErrUnsupportedBuild", err)
	}
}

func TestRecordRecipeRejectsUnnamedFileOutputWithoutFormat(t *testing.T) {
	_, err := recordJob(
		goav.FileInput("input.ivf", strings.NewReader("")),
		goav.FileOutput("", io.Discard),
	).Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_format_missing" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_format_missing wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "explicit format") ||
		!strings.Contains(err.Error(), "container extension") {
		t.Fatalf("err = %v, want format guidance", err)
	}
}

func TestRecordRecipeRejectsFormatOnlyOutputSpec(t *testing.T) {
	_, err := recordJob(
		goav.FileInput("input.ivf", strings.NewReader("")),
		goav.OutputSpec{}.Format(av.FormatIVF),
	).Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_target_missing" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_target_missing wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "no URI, writer, or sink") ||
		!strings.Contains(err.Error(), "goav.FileOutput") {
		t.Fatalf("err = %v, want output target guidance", err)
	}
}

func TestRecordRecipeReportsMissingInputDemuxer(t *testing.T) {
	_, err := recordJob(
		goav.FileInput("input.ogg", strings.NewReader("")),
		goav.FileOutput("recording.ivf", io.Discard),
	).Build(context.Background())

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

func TestRecordRecipeReportsMissingOutputMuxer(t *testing.T) {
	_, err := recordJob(
		goav.FileInput("input.ivf", bytes.NewReader(tinyIVF())),
		goav.FileOutput("recording.webm", io.Discard),
	).Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_muxer_missing" {
		t.Fatalf("err = %v, want output_muxer_missing", err)
	}
	if !strings.Contains(err.Error(), `format "matroska"`) ||
		!strings.Contains(err.Error(), "no muxer is registered") ||
		!strings.Contains(err.Error(), ".ivf") {
		t.Fatalf("err = %v, want muxer adapter guidance", err)
	}
}

func TestRecordRecipeRejectsDuplicateOutputs(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ivf", strings.NewReader(""))).
		To(
			goav.FileOutput("recording.ivf", io.Discard),
			goav.FileOutput("recording.ivf", io.Discard),
		).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_duplicate" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_duplicate wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), `output label "recording.ivf"`) ||
		!strings.Contains(err.Error(), "unique output name") {
		t.Fatalf("err = %v, want duplicate output guidance", err)
	}
}

func TestStreamRecipeRejectsDuplicateFrameSinkOutputs(t *testing.T) {
	sink := func(context.Context, goav.Message) error { return nil }
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		To(
			goav.FrameSink(goav.SinkFunc("frames", sink)),
			goav.FrameSink(goav.SinkFunc("frames", sink)),
		).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_duplicate" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_duplicate wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), `output label "frames"`) ||
		!strings.Contains(err.Error(), ".Name") {
		t.Fatalf("err = %v, want duplicate sink guidance", err)
	}
}

func TestRTPRecipeRejectsNilReader(t *testing.T) {
	_, err := recordJob(
		goav.RTP(nil).Name("audio"),
		goav.FileOutput("recording.ogg", io.Discard),
	).Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "rtp_reader_missing" || !errors.Is(err, goav.ErrNilSource) {
		t.Fatalf("err = %v, want rtp_reader_missing wrapping ErrNilSource", err)
	}
	if !strings.Contains(err.Error(), "non-nil rtpav.PacketReader") {
		t.Fatalf("err = %v, want RTP reader guidance", err)
	}
}

func TestRTPRecipeRequiresCodecIntent(t *testing.T) {
	_, err := recordJob(
		goav.RTP(recipeAPIRTPReader{}).Name("audio"),
		goav.FileOutput("recording.ogg", io.Discard),
	).Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "rtp_codec_missing" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want rtp_codec_missing wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), ".Codec(goav.Opus())") ||
		!strings.Contains(err.Error(), "goav.WebRTCTrack") {
		t.Fatalf("err = %v, want RTP codec guidance", err)
	}
}

func TestRTPRecipeRejectsNegativeBufferLimits(t *testing.T) {
	tests := []struct {
		name  string
		field string
		limit goav.RTPBufferLimits
	}{
		{name: "max ready", field: "MaxReady", limit: goav.RTPBufferLimits{MaxReady: -1}},
		{name: "max events", field: "MaxEvents", limit: goav.RTPBufferLimits{MaxEvents: -1}},
		{name: "max feedback", field: "MaxFeedback", limit: goav.RTPBufferLimits{MaxFeedback: -1}},
		{name: "max packets", field: "MaxPackets", limit: goav.RTPBufferLimits{MaxPackets: -1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := recordJob(
				goav.RTP(recipeAPIRTPReader{}).
					Name("audio").
					Codec(goav.Opus()).
					RTPBuffer(tt.limit),
				goav.FileOutput("recording.ogg", io.Discard),
			).Build(context.Background())
			var buildErr *goav.BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != "rtp_buffer_invalid" || !errors.Is(err, goav.ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want rtp_buffer_invalid wrapping ErrUnsupportedBuild", err)
			}
			if !strings.Contains(err.Error(), tt.field+"=-1") ||
				!strings.Contains(err.Error(), "zero for defaults") {
				t.Fatalf("err = %v, want RTP buffer guidance", err)
			}
		})
	}
}

func TestRTPRecipeRejectsInvalidTimestampGap(t *testing.T) {
	tests := []struct {
		name string
		gap  av.Duration
		want string
	}{
		{
			name: "negative",
			gap:  av.Duration{Value: -1, Base: av.RTPTimeBase(48000)},
			want: "negative timestamp gap",
		},
		{
			name: "missing timebase",
			gap:  av.Duration{Value: 960},
			want: "invalid timebase",
		},
		{
			name: "invalid denominator",
			gap:  av.Duration{Value: 960, Base: av.TimeBase{Num: 1}},
			want: "invalid timebase",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := recordJob(
				goav.RTP(recipeAPIRTPReader{}).
					Name("audio").
					Codec(goav.Opus()).
					MaxTimestampGap(tt.gap),
				goav.FileOutput("recording.ogg", io.Discard),
			).Build(context.Background())
			var buildErr *goav.BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != "rtp_timestamp_gap_invalid" || !errors.Is(err, goav.ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want rtp_timestamp_gap_invalid wrapping ErrUnsupportedBuild", err)
			}
			if !strings.Contains(err.Error(), tt.want) ||
				!strings.Contains(err.Error(), "MaxTimestampGap") {
				t.Fatalf("err = %v, want RTP timestamp-gap guidance", err)
			}
		})
	}
}

func TestRTPRecipeRejectsUnsupportedCodecIntent(t *testing.T) {
	_, err := recordJob(
		goav.RTP(recipeAPIRTPReader{}).Name("audio").Codec(goav.CodecSpec{ID: "pcm"}),
		goav.FileOutput("recording.ogg", io.Discard),
	).Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "rtp_codec_unsupported" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want rtp_codec_unsupported wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "pcm has no built-in RTP depacketizer") ||
		!strings.Contains(err.Error(), "goav.Opus()") ||
		!strings.Contains(err.Error(), "advanced receive adapter") ||
		strings.Contains(err.Error(), ".Depacketize") {
		t.Fatalf("err = %v, want RTP codec guidance", err)
	}
}

func TestRTPRecipeRejectsUnresolvedCodecIntents(t *testing.T) {
	tests := []struct {
		name string
		spec goav.CodecSpec
		code string
	}{
		{name: "auto", spec: goav.Auto(), code: "rtp_codec_auto_unresolved"},
		{name: "copy", spec: goav.Copy(), code: "rtp_codec_copy_invalid"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := recordJob(
				goav.RTP(recipeAPIRTPReader{}).Name("audio").Codec(tt.spec),
				goav.FileOutput("recording.ogg", io.Discard),
			).Build(context.Background())
			var buildErr *goav.BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, goav.ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want %s wrapping ErrUnsupportedBuild", err, tt.code)
			}
			if tt.code == "rtp_codec_auto_unresolved" &&
				(!strings.Contains(err.Error(), "set RTP receive intent") ||
					!strings.Contains(err.Error(), "advanced receive adapter") ||
					strings.Contains(err.Error(), ".Depacketize")) {
				t.Fatalf("err = %v, want recipe-first RTP codec guidance", err)
			}
		})
	}
}

func TestReadmeAudioEncodeRecipeIsSmall(t *testing.T) {
	meter := goav.FrameFunc("meter", func(ctx context.Context, frame *goav.Frame, emit goav.Emit) error {
		return emit.Frame(frame)
	})
	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Do(meter).
		Opus(96_000).
		To(goav.FileOutput("archive.ogg", io.Discard))

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
		Resample(16_000, goav.Mono).
		Opus(48_000).
		To(goav.FileOutput("preview.ogg", io.Discard))

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
	intent := job.Intent()
	if len(intent.Streams) != 1 || len(intent.Streams[0].Transforms) != 1 || intent.Streams[0].Transforms[0].Resample == nil {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestReadmeVideoResizeEncodeRecipeIsSmall(t *testing.T) {
	job := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Resize(1280, 720).
		VP9(2_000_000).
		To(goav.FileOutput("preview.webm", io.Discard))

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
	intent := job.Intent()
	if len(intent.Streams) != 1 || len(intent.Streams[0].Transforms) != 1 || intent.Streams[0].Transforms[0].Resize == nil {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestStreamRecipeIntentOperationsImplyDecode(t *testing.T) {
	sink := goav.SinkFunc("frames", func(context.Context, goav.Message) error {
		return nil
	})
	meter := goav.FrameFunc("meter", func(ctx context.Context, frame *goav.Frame, emit goav.Emit) error {
		return emit.Frame(frame)
	})
	tests := []struct {
		name string
		job  *goav.Job
	}{
		{
			name: "frame sink",
			job: goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
				Audio().
				To(goav.FrameSink(sink)),
		},
		{
			name: "custom stage",
			job: goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
				Audio().
				Do(meter).
				Opus(96_000).
				To(goav.FileOutput("archive.ogg", io.Discard)),
		},
		{
			name: "resample",
			job: goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
				Audio().
				Resample(16_000, goav.Mono).
				Opus(48_000).
				To(goav.FileOutput("preview.ogg", io.Discard)),
		},
		{
			name: "resize",
			job: goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
				Video().
				Resize(1280, 720).
				VP9(2_000_000).
				To(goav.FileOutput("preview.webm", io.Discard)),
		},
		{
			name: "encoder",
			job: goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
				Audio().
				Opus(96_000).
				To(goav.FileOutput("archive.ogg", io.Discard)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			intent := tt.job.Intent()
			if len(intent.Streams) != 1 || !intent.Streams[0].Decode {
				t.Fatalf("intent: %+v", intent)
			}
		})
	}
}

func TestStreamRecipeRequiresOperationForMuxOutput(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		To(goav.FileOutput("archive.ogg", io.Discard)).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_operation_missing" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want stream_operation_missing wrapping ErrUnsupportedBuild", err)
	}
}

func TestStreamRecipeRejectsGenericAndStreamOutputs(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		To(goav.FileOutput("archive.ogg", io.Discard)).
		Audio().
		Opus(96_000).
		To(goav.FileOutput("preview.ogg", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_scope_mixed" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_scope_mixed wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "stream recipes use stream-local outputs") ||
		!strings.Contains(err.Error(), ".Copy().To") ||
		!strings.Contains(err.Error(), ".Branch") {
		t.Fatalf("err = %v, want output scope guidance", err)
	}
}

func TestStreamRecipeRejectsJobLevelOutput(t *testing.T) {
	job := goav.From(goav.FileInput("input.ogg", strings.NewReader("")))
	job.Audio()
	job.To(goav.FrameSink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
		return nil
	})))
	_, err := job.Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_scope_mixed" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_scope_mixed wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), ".Audio()...To(...)") {
		t.Fatalf("err = %v, want stream-local To guidance", err)
	}
}

func TestStreamRecipeRejectsSecondStreamSelection(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Audio().
		To(goav.FrameSink(goav.SinkFunc("audio", func(context.Context, goav.Message) error {
			return nil
		}))).
		Video().
		To(goav.FrameSink(goav.SinkFunc("video", func(context.Context, goav.Message) error {
			return nil
		}))).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_duplicate" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want stream_duplicate wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "first stream: audio") ||
		!strings.Contains(err.Error(), "second stream: video") ||
		!strings.Contains(err.Error(), ".Branch") {
		t.Fatalf("err = %v, want duplicate stream guidance", err)
	}
}

func TestStreamRecipeRejectsNegativeStreamIndex(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio(goav.StreamIndex(-1)).
		To(goav.FrameSink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
			return nil
		}))).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_selector_invalid" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want stream_selector_invalid wrapping ErrUnsupportedBuild", err)
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
		To(goav.FrameSink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
			return nil
		}))).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stage_missing" || !errors.Is(err, goav.ErrNilStage) {
		t.Fatalf("err = %v, want stage_missing wrapping ErrNilStage", err)
	}
	if !strings.Contains(err.Error(), ".Do(stage)") ||
		!strings.Contains(err.Error(), "goav.FrameFunc") {
		t.Fatalf("err = %v, want custom stage guidance", err)
	}
}

func TestStreamRecipeRejectsWrongMediaTransform(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Resize(320, 180).
		To(goav.FrameSink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
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
		Resize(0, 720).
		To(goav.FrameSink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
			return nil
		}))).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "transform_invalid" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want transform_invalid wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "positive width and height") ||
		!strings.Contains(err.Error(), "width=0") {
		t.Fatalf("err = %v, want resize value guidance", err)
	}
}

func TestStreamRecipeRequiresEncoderForFileOutput(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Resample(48_000, goav.Stereo).
		To(goav.FileOutput("archive.ogg", io.Discard)).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_missing" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want encode_missing wrapping ErrUnsupportedBuild", err)
	}
}

func TestStreamRecipeRejectsMixedFrameSinkAndFileOutput(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		To(
			goav.FrameSink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
				return nil
			})),
			goav.FileOutput("archive.ogg", io.Discard),
		).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_kind_mixed" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_kind_mixed wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "cannot mix frame sinks and muxed outputs") ||
		!strings.Contains(err.Error(), ".Tap(...).Branch") {
		t.Fatalf("err = %v, want mixed output guidance", err)
	}
}

func TestStreamRecipeRejectsMixedEncodedOutputAndFrameSink(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Opus(96_000).
		To(
			goav.FileOutput("archive.ogg", io.Discard),
			goav.FrameSink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
				return nil
			})),
		).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_kind_mixed" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_kind_mixed wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), ".To(goav.FrameSink") ||
		!strings.Contains(err.Error(), ".To(goav.FileOutput") {
		t.Fatalf("err = %v, want decoded or encoded output guidance", err)
	}
}

func TestStreamRecipeRejectsProcessingAfterEncoder(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Opus(96_000).
		Resample(16_000, goav.Mono).
		To(goav.FileOutput("archive.ogg", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_step_after_encode" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want stream_step_after_encode wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "step: resample") ||
		!strings.Contains(err.Error(), "encoder: opus") ||
		!strings.Contains(err.Error(), "before .Opus") {
		t.Fatalf("err = %v, want terminal encoder guidance", err)
	}
}

func TestStreamRecipeRejectsDuplicateEncoder(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Opus(96_000).
		VP9(600_000).
		To(goav.FileOutput("archive.ogg", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_duplicate" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want encode_duplicate wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "first encoder: opus") ||
		!strings.Contains(err.Error(), "second encoder: vp9") ||
		!strings.Contains(err.Error(), "one terminal encoder") {
		t.Fatalf("err = %v, want duplicate encoder guidance", err)
	}
}

func TestStreamRecipeRejectsWorkInProgressRecipeEncoder(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		output string
		codec  goav.CodecSpec
	}{
		{name: "h264", input: "input.h264", output: "archive.h264", codec: goav.H264(goav.Bitrate(2_000_000))},
		{name: "av1", input: "input.ivf", output: "archive.ivf", codec: goav.AV1(goav.Bitrate(2_000_000))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := goav.From(goav.FileInput(tt.input, strings.NewReader(""))).
				Video().
				Encode(tt.codec).
				To(goav.FileOutput(tt.output, io.Discard)).
				Build(context.Background())
			var buildErr *goav.BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != "encode_work_in_progress" || !errors.Is(err, goav.ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want encode_work_in_progress wrapping ErrUnsupportedBuild", err)
			}
			if !strings.Contains(err.Error(), "recipe encoding is work in progress") ||
				!strings.Contains(err.Error(), "opus, vp8, and vp9") {
				t.Fatalf("err = %v, want work-in-progress encode guidance", err)
			}
		})
	}
}

func TestStreamRecipeReportsMissingCustomEncoder(t *testing.T) {
	rt := goav.New(goav.WithFormatAdapter(func(registry *format.SimpleRegistry) {
		registry.RegisterMuxer(av.FormatOgg, recipeAPIMuxerFactory{})
	}))
	_, err := goav.From(goav.FileInput("input.wav", strings.NewReader(""))).
		UseRuntime(rt).
		Audio().
		Encode(goav.Codec("pcm", av.MediaAudio)).
		To(goav.FileOutput("archive.ogg", io.Discard)).
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
		Opus(-1).
		To(goav.FileOutput("archive.ogg", io.Discard)).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_parameter_invalid" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want encode_parameter_invalid wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "bitrate must be non-negative") ||
		!strings.Contains(err.Error(), "bitrate=-1") {
		t.Fatalf("err = %v, want bitrate guidance", err)
	}
}

func TestStreamRecipeRejectsInvalidEncodeSampleRate(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Encode(goav.Opus(goav.SampleRate(0))).
		To(goav.FileOutput("archive.ogg", io.Discard)).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_parameter_invalid" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want encode_parameter_invalid wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "sample rate must be positive") ||
		!strings.Contains(err.Error(), "sample_rate=0") {
		t.Fatalf("err = %v, want sample-rate guidance", err)
	}
}

func TestStreamRecipeReportsMissingEncodeAdapterBeforeOpeningInput(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Opus(96_000).
		To(goav.FileOutput("archive.ivf", io.Discard)).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_adapter_missing" || !errors.Is(err, codec.ErrNotFound) {
		t.Fatalf("err = %v, want encode_adapter_missing wrapping codec.ErrNotFound", err)
	}
	if !strings.Contains(err.Error(), "codec=opus") ||
		!strings.Contains(err.Error(), "FrameSink") ||
		strings.Contains(err.Error(), "cannot open input") ||
		strings.Contains(err.Error(), "input_demuxer_missing") {
		t.Fatalf("err = %v, want encode adapter guidance before input diagnostics", err)
	}
}

func TestStreamRecipeReportsMissingDecodeAdapterBeforeOpeningLiveInput(t *testing.T) {
	rt := goav.New(goav.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
		registry.RegisterDescriptor(codec.Descriptor{
			ID:    av.CodecH264,
			Name:  "h264",
			Modes: []codec.Mode{codec.ModeDecode},
			Capabilities: codec.Capabilities{
				BuildTags: []string{"goav_goh264"},
			},
			Backend: codec.Backend{
				Name:   "goh264",
				Status: "planned-build-tagged",
			},
		})
	}))
	_, err := goav.From(goav.RTP(recipeAPIRTPReader{}).Name("video").Codec(goav.H264())).
		UseRuntime(rt).
		Video().
		To(goav.FrameSink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
			return nil
		}))).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "decode_adapter_unavailable" || !errors.Is(err, codec.ErrUnavailable) {
		t.Fatalf("err = %v, want decode_adapter_unavailable wrapping codec.ErrUnavailable", err)
	}
	if !strings.Contains(err.Error(), "codec=h264") ||
		!strings.Contains(err.Error(), "goav.From") ||
		strings.Contains(err.Error(), "stream") ||
		strings.Contains(err.Error(), "cannot open input") {
		t.Fatalf("err = %v, want decode adapter guidance before live input diagnostics", err)
	}
}

func TestStreamRecipeReportsAmbiguousLiveSelectionBeforeDecoderAdapter(t *testing.T) {
	_, err := goav.From(goav.RTP(recipeAPIRTPReader{}).Name("front").Codec(goav.VP8())).
		And(goav.RTP(recipeAPIRTPReader{}).Name("screen").Codec(goav.VP8())).
		Video().
		To(goav.FrameSink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
			return nil
		}))).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_ambiguous" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want stream_ambiguous wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "id=front") ||
		!strings.Contains(err.Error(), "id=screen") ||
		!strings.Contains(err.Error(), `.Video(goav.StreamID("front"))`) ||
		strings.Contains(err.Error(), "decoder adapter") {
		t.Fatalf("err = %v, want live stream-selection guidance before decoder diagnostics", err)
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
	rt := goav.New(goav.WithFormatAdapter(func(registry *format.SimpleRegistry) {
		registry.RegisterProber(recipeAPIStreamProber{streams: streams})
		registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{called: &demuxerOpened})
	}))

	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Audio().
		To(goav.FrameSink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
			return nil
		}))).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_ambiguous" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want stream_ambiguous wrapping ErrUnsupportedBuild", err)
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
	rt := goav.New(goav.WithFormatAdapter(func(registry *format.SimpleRegistry) {
		registry.RegisterProber(recipeAPIStreamProber{streams: streams})
		registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{called: &demuxerOpened})
	}))

	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Audio().
		To(goav.FrameSink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
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
		UseRuntime(goav.New(goav.WithStdCodecs())).
		Audio().
		Resample(16_000, goav.Mono).
		To(goav.FrameSink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
			return nil
		}))).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "transform_adapter_missing" || !errors.Is(err, filter.ErrNotFound) {
		t.Fatalf("err = %v, want transform_adapter_missing wrapping filter.ErrNotFound", err)
	}
	if !strings.Contains(err.Error(), "transform=resample") ||
		!strings.Contains(err.Error(), "goav.Default") ||
		strings.Contains(err.Error(), "input_demuxer_missing") ||
		strings.Contains(err.Error(), "cannot open input") {
		t.Fatalf("err = %v, want transform adapter guidance before input diagnostics", err)
	}
}

func TestStreamRecipeRejectsUnresolvedEncodeIntents(t *testing.T) {
	tests := []struct {
		name string
		spec goav.CodecSpec
		code string
	}{
		{name: "auto", spec: goav.Auto(), code: "encode_auto_unresolved"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
				Audio().
				Encode(tt.spec).
				To(goav.FileOutput("archive.ogg", io.Discard)).
				Build(context.Background())
			var buildErr *goav.BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, goav.ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want %s wrapping ErrUnsupportedBuild", err, tt.code)
			}
		})
	}
}

func TestDefaultRecordIVFRecipeRunShortcutRuns(t *testing.T) {
	var out bytes.Buffer
	if err := recordJob(
		goav.FileInput("input.ivf", bytes.NewReader(tinyIVF())),
		goav.FileOutput("preview.ivf", &out),
	).Run(context.Background()); err != nil {
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
		goav.FileOutput("preview.ivf", &out),
	)

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
			goav.FileOutput("recording.ivf", &recording),
			goav.FileOutput("preview.ivf", &preview),
		).
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
			goav.FileOutput("recording.ivf", &recording),
			goav.FileOutput("preview.ivf", &preview),
		)

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
		goav.FileOutput("", &out).Format(av.FormatIVF),
	)

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
	job := transcodeJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("720p").Resize(1280, 720).VP9(2_000_000).To("web").
		Video("360p").Resize(640, 360).VP9(600_000).To("preview").
		Output("web", goav.FileOutput("web.webm", io.Discard)).
		Output("preview", goav.FileOutput("preview.webm", io.Discard))

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
	intent := job.Intent()
	if len(intent.Streams) != 2 || !intent.Streams[0].Decode || !intent.Streams[1].Decode {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestTranscodeRecipeComposesAudioAndVideoIntoSharedOutput(t *testing.T) {
	job := transcodeJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("v360").Resize(640, 360).VP9(600_000).To("web").
		Audio("a96").Resample(48_000, goav.Stereo).Opus(96_000).To("web").
		Output("web", goav.FileOutput("out.webm", io.Discard))

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
	intent := job.Intent()
	if len(intent.Streams) != 2 ||
		len(intent.Streams[0].RouteTo) != 1 || intent.Streams[0].RouteTo[0] != "web" ||
		len(intent.Streams[1].RouteTo) != 1 || intent.Streams[1].RouteTo[0] != "web" ||
		len(intent.Outputs) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestTranscodeRecipeSingleBranchUsesOutputLabel(t *testing.T) {
	job := transcodeJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("360p").Resize(640, 360).VP9(600_000).
		To("preview").
		Output("preview", goav.FileOutput("preview.webm", io.Discard))

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "encode-360p -> preview.webm") {
		t.Fatalf("spec:\n%s", text)
	}
	intent := job.Intent()
	if len(intent.Streams) != 1 || len(intent.Streams[0].RouteTo) != 1 || len(intent.Outputs) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestTranscodeRecipeRejectsDuplicateOutputLabels(t *testing.T) {
	_, err := transcodeJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("720p").VP9(2_000_000).To("web").
		Output("web", goav.FileOutput("web.webm", io.Discard)).
		Output("web", goav.FileOutput("web2.webm", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_duplicate" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_duplicate wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), `output label "web"`) ||
		!strings.Contains(err.Error(), "unique .Output") ||
		!strings.Contains(err.Error(), ".To(label)") {
		t.Fatalf("err = %v, want duplicate output guidance", err)
	}
}

func TestTranscodeRecipeRejectsDuplicateBranchOutputLabels(t *testing.T) {
	_, err := transcodeJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("720p").VP9(2_000_000).To("web", "web").
		Output("web", goav.FileOutput("web.webm", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_duplicate" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_duplicate wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), `branch routes to output "web" more than once`) ||
		!strings.Contains(err.Error(), "second target index: 1") ||
		!strings.Contains(err.Error(), "list each output label once") {
		t.Fatalf("err = %v, want duplicate branch output guidance", err)
	}
}

func TestTranscodeRecipeRejectsUndefinedOutputLabel(t *testing.T) {
	_, err := transcodeJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("720p").VP9(2_000_000).To("missing").
		Output("web", goav.FileOutput("web.webm", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_missing" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_missing wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "output missing is referenced but not defined") ||
		!strings.Contains(err.Error(), `.Output(missing, goav.FileOutput`) ||
		!strings.Contains(err.Error(), "define shared outputs once") {
		t.Fatalf("err = %v, want undefined output label guidance", err)
	}
}

func TestTranscodeRecipeRejectsEmptyOutputLabel(t *testing.T) {
	_, err := transcodeJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("720p").VP9(2_000_000).
		To("").
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_label_invalid" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_label_invalid wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "target index: 0") ||
		!strings.Contains(err.Error(), `.Output(label, goav.FileOutput`) {
		t.Fatalf("err = %v, want output label guidance", err)
	}
}

func TestTranscodeRecipeRejectsEmptyOutputDefinitionLabel(t *testing.T) {
	_, err := transcodeJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("720p").VP9(2_000_000).
		To("web").
		Output("", goav.FileOutput("web.webm", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_label_invalid" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_label_invalid wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), `Output("label"`) ||
		!strings.Contains(err.Error(), `To("label"`) ||
		!strings.Contains(err.Error(), "output name: web.webm") {
		t.Fatalf("err = %v, want output definition label guidance", err)
	}
}

func TestTranscodeRecipeRejectsDuplicateBranchNames(t *testing.T) {
	_, err := transcodeJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("720p").VP9(2_000_000).To("archive").
		Video("720p").VP9(1_000_000).To("preview").
		Output("archive", goav.FileOutput("archive.webm", io.Discard)).
		Output("preview", goav.FileOutput("preview.webm", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_duplicate" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want stream_duplicate wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), `transcode branch name "720p"`) ||
		!strings.Contains(err.Error(), "first branch index: 0") ||
		!strings.Contains(err.Error(), `.Video("360p")`) {
		t.Fatalf("err = %v, want duplicate branch guidance", err)
	}
}

func TestTranscodeRecipeRejectsMissingBranchName(t *testing.T) {
	_, err := transcodeJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("").VP9(2_000_000).To("web").
		Output("web", goav.FileOutput("web.webm", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_name_missing" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want stream_name_missing wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "transcode branches need stable names") ||
		!strings.Contains(err.Error(), `.Video("720p")`) ||
		!strings.Contains(err.Error(), "media type: video") {
		t.Fatalf("err = %v, want branch name guidance", err)
	}
}

func TestTranscodeRecipeRejectsInvalidOutputSpec(t *testing.T) {
	_, err := transcodeJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("360p").VP9(600_000).
		To("preview").
		Output("preview", goav.FileOutput("preview.webm", nil)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_writer_missing" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_writer_missing wrapping ErrUnsupportedBuild", err)
	}
}

func TestBranchCompositionAcceptsRTPInputThenReportsMissingMuxer(t *testing.T) {
	_, err := transcodeJob(goav.RTP(recipeAPIRTPReader{}).Name("audio").Codec(goav.Opus())).
		Audio("main").Opus(96_000).To("archive").
		Output("archive", goav.FileOutput("archive.ogg", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_muxer_missing" {
		t.Fatalf("err = %v, want output_muxer_missing", err)
	}
	if !strings.Contains(err.Error(), "ogg muxer") {
		t.Fatalf("err = %v, want Ogg adapter guidance", err)
	}
}

func TestTranscodeRecipeRejectsFrameSinkOutput(t *testing.T) {
	_, err := transcodeJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("360p").VP9(600_000).
		To("preview").
		Output("preview", goav.FrameSink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
			return nil
		}))).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_kind_invalid" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_kind_invalid wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "transcode outputs are muxed output groups") ||
		!strings.Contains(err.Error(), "goav.FileOutput") ||
		!strings.Contains(err.Error(), "goav.From") {
		t.Fatalf("err = %v, want transcode output guidance", err)
	}
}

func TestTranscodeRecipeRequiresBranch(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_missing" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_missing wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "no output is configured") {
		t.Fatalf("err = %v, want output guidance", err)
	}
}

func TestTranscodeRecipeRequiresBranchOutput(t *testing.T) {
	job := transcodeJob(goav.FileInput("input.webm", strings.NewReader("")))
	job.Video("360p").VP9(600_000)
	_, err := job.Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_missing" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_missing wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "stream has no output target") ||
		!strings.Contains(err.Error(), "goav.FileOutput") {
		t.Fatalf("err = %v, want output guidance", err)
	}
}

func TestTranscodeRecipeRejectsNegativeStreamIndex(t *testing.T) {
	_, err := transcodeJob(goav.FileInput("input.webm", strings.NewReader(""))).
		materialize().
		Audio(goav.StreamIndex(-1)).
		Decode().
		Tap("audio.decoded").
		Branch("bad").
		Opus(64_000).
		To("bad").
		Output("bad", goav.FileOutput("bad.ogg", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_selector_invalid" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want stream_selector_invalid wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "stream index must be non-negative") ||
		!strings.Contains(err.Error(), "index=-1") {
		t.Fatalf("err = %v, want stream index guidance", err)
	}
}

func TestTranscodeRecipeRejectsWrongMediaTransform(t *testing.T) {
	_, err := transcodeJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("bad").Resample(16_000, goav.Mono).VP9(600_000).
		To("bad").
		Output("bad", goav.FileOutput("bad.webm", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "transform_media_mismatch" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want transform_media_mismatch wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "resample applies to audio branches") ||
		!strings.Contains(err.Error(), ".Video(...).Resize(...)") {
		t.Fatalf("err = %v, want media transform guidance", err)
	}
}

func TestTranscodeRecipeRejectsInvalidResample(t *testing.T) {
	_, err := transcodeJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Audio("bad").Resample(0, goav.Mono).Opus(64_000).
		To("bad").
		Output("bad", goav.FileOutput("bad.ogg", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "transform_invalid" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want transform_invalid wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "positive sample rate and channels") ||
		!strings.Contains(err.Error(), "sample_rate=0") {
		t.Fatalf("err = %v, want resample value guidance", err)
	}
}

func TestTranscodeRecipeRejectsProcessingAfterEncoder(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Decode().
		Tap("video.decoded").
		Branch("360p").
		VP9(600_000).
		Resize(640, 360).
		To("preview").
		Output("preview", goav.FileOutput("preview.webm", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_step_after_encode" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want stream_step_after_encode wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "step: resize") ||
		!strings.Contains(err.Error(), "encoder: vp9") ||
		!strings.Contains(err.Error(), ".To(...) after the encoder") {
		t.Fatalf("err = %v, want transcode terminal encoder guidance", err)
	}
}

func TestTranscodeRecipeRejectsDuplicateEncoder(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Decode().
		Tap("video.decoded").
		Branch("360p").
		VP9(600_000).
		VP8(400_000).
		To("preview").
		Output("preview", goav.FileOutput("preview.webm", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_duplicate" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want encode_duplicate wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "first encoder: vp9") ||
		!strings.Contains(err.Error(), "second encoder: vp8") ||
		!strings.Contains(err.Error(), "multiple encoded branches") {
		t.Fatalf("err = %v, want duplicate transcode encoder guidance", err)
	}
}

func TestTranscodeRecipeRejectsNegativeEncodeBitrate(t *testing.T) {
	_, err := transcodeJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("bad").VP9(-1).
		To("bad").
		Output("bad", goav.FileOutput("bad.webm", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_parameter_invalid" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want encode_parameter_invalid wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "bitrate must be non-negative") ||
		!strings.Contains(err.Error(), "bitrate=-1") {
		t.Fatalf("err = %v, want bitrate guidance", err)
	}
}

func TestTranscodeRecipeRejectsTransformChainUntilPlanSupportsIt(t *testing.T) {
	_, err := transcodeJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("360p").Resize(1280, 720).Resize(640, 360).VP9(600_000).
		To("preview").
		Output("preview", goav.FileOutput("preview.webm", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "transform_chain_unsupported" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want transform_chain_unsupported wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "one media transform") ||
		!strings.Contains(err.Error(), "create another Video") {
		t.Fatalf("err = %v, want transform chain guidance", err)
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
