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

type recipeAPIVideoRTPReader struct {
	recipeAPIRTPReader
}

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

type recipeAPICustomDestination struct{}

func (recipeAPICustomDestination) Name() string {
	return "custom"
}

func (recipeAPICustomDestination) Contract() goav.DestinationContract {
	return goav.DestinationContract{
		ByteStream: true,
		Formats:    []av.FormatID{av.FormatIVF},
		MIMETypes:  []string{"video/ivf"},
	}
}

func (recipeAPICustomDestination) Open(context.Context, goav.DestinationInfo) (goav.DestinationWriter, error) {
	return recipeAPICustomWriter{}, nil
}

type recipeAPICustomWriter struct{}

func (recipeAPICustomWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func (recipeAPICustomWriter) Close() error {
	return nil
}

func (recipeAPIRuntimeWithoutBuilder) Probe(context.Context, format.ProbeRequest) (format.ProbeResult, error) {
	return format.ProbeResult{}, nil
}

func (recipeAPIRTPReader) Streams(context.Context) ([]goav.Stream, error) {
	return []goav.Stream{{ID: "audio", Type: "audio"}}, nil
}

func (recipeAPIVideoRTPReader) Streams(context.Context) ([]goav.Stream, error) {
	return []goav.Stream{{
		ID:   "video",
		Type: av.MediaVideo,
		Codec: av.CodecParameters{
			ID:   av.CodecVP8,
			Type: av.MediaVideo,
		},
	}}, nil
}

func (recipeAPIVideoRTPReader) PayloadMap() rtpav.PayloadMap {
	return rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
		PayloadType: 96,
		Parameters: av.CodecParameters{
			ID:        av.CodecVP8,
			Type:      av.MediaVideo,
			ClockRate: 90000,
		},
	}})
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

func adapterRequirementByKind(requirements []goav.AdapterRequirement, kind string, name string) (goav.AdapterRequirement, bool) {
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
	return goav.AdapterRequirement{}, false
}

func adapterRequirementByKindAndOwner(requirements []goav.AdapterRequirement, kind string, name string, requiredBy string) (goav.AdapterRequirement, bool) {
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
	return goav.AdapterRequirement{}, false
}

func hasPlanWarning(warnings []goav.PlanDiagnostic, code string) bool {
	for i := range warnings {
		if warnings[i].Code == code {
			return true
		}
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

func operationSpecKinds(operations []goav.OperationSpec) []goav.OperationKind {
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

func TestMediaShapePublicContract(t *testing.T) {
	packet := goav.PacketShape(
		av.MediaAudio,
		av.CodecOpus,
		goav.ShapeStream("audio"),
		goav.ShapeAudio(48_000, goav.Stereo, av.SampleFormatS16),
		goav.ShapeRealtime(true),
	)
	if packet.Domain != goav.DomainPacket ||
		packet.MediaKind != av.MediaAudio ||
		packet.Codec != av.CodecOpus ||
		packet.SampleRate != 48_000 ||
		packet.Channels != goav.Stereo ||
		!packet.Realtime {
		t.Fatalf("packet shape=%+v, want opus audio packet shape", packet)
	}
	if !packet.CompatibleWith(goav.Shape(goav.ShapeDomain(goav.DomainPacket), goav.ShapeMedia(av.MediaAudio))) {
		t.Fatalf("packet shape should satisfy packet audio contract: %s", packet)
	}
	if (goav.ShapeSet{goav.FrameShape(av.MediaAudio)}).Accepts(packet) {
		t.Fatalf("frame shape set accepted packet shape: %s", packet)
	}

	var resizeContract goav.ShapeContract = goav.Resize(1280, 720)
	if !resizeContract.InputShapes().Accepts(goav.FrameShape(av.MediaVideo)) {
		t.Fatalf("resize input shapes=%+v, want video frame", resizeContract.InputShapes())
	}
	resized := resizeContract.OutputShapes(goav.FrameShape(
		av.MediaVideo,
		goav.ShapeVideo(1920, 1080, av.PixelFormatYUV420P),
	))[0]
	if resized.Domain != goav.DomainFrame ||
		resized.MediaKind != av.MediaVideo ||
		resized.Width != 1280 ||
		resized.Height != 720 ||
		resized.PixelFormat != av.PixelFormatYUV420P {
		t.Fatalf("resized shape=%+v, want 1280x720 video frame", resized)
	}

	var copyContract goav.ShapeContract = goav.Copy()
	if !copyContract.InputShapes().Accepts(packet) {
		t.Fatalf("copy input shapes=%+v, want packet domain", copyContract.InputShapes())
	}
	copied := copyContract.OutputShapes(packet)[0]
	if copied != packet {
		t.Fatalf("copied shape=%+v, want preserved packet %+v", copied, packet)
	}

	var operationContract goav.ShapeContract = goav.OperationSpec{Kind: goav.OpTransform, Transform: goav.Resample(16_000, goav.Mono)}
	resampled := operationContract.OutputShapes(goav.FrameShape(
		av.MediaAudio,
		goav.ShapeAudio(48_000, goav.Stereo, av.SampleFormatS16),
	))[0]
	if resampled.Domain != goav.DomainFrame ||
		resampled.MediaKind != av.MediaAudio ||
		resampled.SampleRate != 16_000 ||
		resampled.Channels != goav.Mono ||
		resampled.SampleFormat != av.SampleFormatS16 {
		t.Fatalf("resampled shape=%+v, want 16k mono audio frame", resampled)
	}
}

func TestSourceInputIntentUsesCustomProtocol(t *testing.T) {
	input := goav.Source("generated",
		goav.PacketShape(av.MediaAudio, av.CodecOpus,
			goav.ShapeAudio(48_000, goav.Stereo, av.SampleFormatS16),
		),
		func(context.Context, goav.SourcePush) error {
			return nil
		},
	)
	job := goav.From(input).
		Audio().
		Copy().
		To(goav.Sink(goav.SinkFunc("packets", func(context.Context, goav.Message) error {
			return nil
		})))

	intent := job.Intent()
	if len(intent.Inputs) != 1 ||
		intent.Inputs[0].Name != "generated" ||
		intent.Inputs[0].Protocol != av.ProtocolCustom ||
		intent.Inputs[0].Codec.ID != av.CodecOpus ||
		intent.Inputs[0].Codec.Parameters.SampleRate != 48_000 ||
		intent.Inputs[0].Codec.Parameters.Channels != goav.Stereo {
		t.Fatalf("intent inputs: %+v", intent.Inputs)
	}
}

func TestFlowReportsShapeContractAndTaps(t *testing.T) {
	flow := goav.Flow("voice").Audio().
		Decode().
		Resample(16_000, goav.Mono).
		Tap(goav.FrameTap("voice.frames"))

	inputs := flow.InputShapes()
	if len(inputs) != 1 || !inputs.Accepts(goav.PacketShape(av.MediaAudio, av.CodecOpus)) {
		t.Fatalf("flow input shapes=%+v, want audio packet", inputs)
	}
	outputs := flow.OutputShapes(goav.PacketShape(
		av.MediaAudio,
		av.CodecOpus,
		goav.ShapeAudio(48_000, goav.Stereo, av.SampleFormatS16),
	))
	if len(outputs) != 1 ||
		outputs[0].Domain != goav.DomainFrame ||
		outputs[0].MediaKind != av.MediaAudio ||
		outputs[0].SampleRate != 16_000 ||
		outputs[0].Channels != goav.Mono ||
		outputs[0].SampleFormat != av.SampleFormatS16 {
		t.Fatalf("flow output shapes=%+v, want 16k mono audio frame", outputs)
	}
	taps := flow.Taps()
	if len(taps) != 1 || taps[0].Name() != "voice.frames" || taps[0].Domain() != goav.DomainFrame {
		t.Fatalf("flow taps=%+v, want frame tap voice.frames", taps)
	}
}

func tapIntentNamesContain(taps []goav.TapIntent, name string) bool {
	for i := range taps {
		if taps[i].Name == name {
			return true
		}
	}
	return false
}

func branchByName(branches []goav.BranchReport, name string) (goav.BranchReport, bool) {
	for i := range branches {
		if branches[i].Name == name {
			return branches[i], true
		}
	}
	return goav.BranchReport{}, false
}

func tapReportByName(taps []goav.TapReport, name string) (goav.TapReport, bool) {
	for i := range taps {
		if taps[i].Name == name {
			return taps[i], true
		}
	}
	return goav.TapReport{}, false
}

func operationReportByKind(operations []goav.OperationReport, kind goav.OperationKind) (goav.OperationReport, bool) {
	for i := range operations {
		if operations[i].Kind == kind {
			return operations[i], true
		}
	}
	return goav.OperationReport{}, false
}

func countOperationReports(operations []goav.OperationReport, kind goav.OperationKind, shared bool) int {
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
	runtime  goav.Runtime
	branches []testTranscodeBranch
}

type testBranchStream interface {
	Branches(...goav.BranchSpec) *goav.Job
}

type testTranscodeBranch struct {
	name       string
	media      av.MediaType
	flows      []goav.Chain
	transforms []goav.TransformSpec
	encode     goav.CodecSpec
	targets    []goav.Destination
}

type testTranscodeBranchBuilder struct {
	job   *testBranchJob
	index int
}

func branchJob(input goav.InputSpec) *testBranchJob {
	return &testBranchJob{input: input}
}

func (j *testBranchJob) UseRuntime(runtime goav.Runtime) *testBranchJob {
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
		targets := make([]goav.Destination, 0, len(branch.targets))
		for i := range branch.targets {
			targets = append(targets, branch.targets[i])
		}
		job = stream.Branches(builder.To(targets...))
	}
	return job
}

func (j *testBranchJob) Intent() goav.Intent {
	return j.materialize().Intent()
}

func (j *testBranchJob) Explain(ctx context.Context) (goav.PlanReport, error) {
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

func (b *testTranscodeBranchBuilder) Apply(flow goav.Chain) *testTranscodeBranchBuilder {
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

func (b *testTranscodeBranchBuilder) Encode(codec goav.CodecSpec) *testTranscodeBranchBuilder {
	b.current().encode = codec
	return b
}

func (b *testTranscodeBranchBuilder) To(targets ...goav.Destination) *testBranchJob {
	b.current().targets = append([]goav.Destination(nil), targets...)
	return b.job
}

func (b *testTranscodeBranchBuilder) current() *testTranscodeBranch {
	return &b.job.branches[b.index]
}

func TestRuntimeInterfaceKeepsLegacyBuilderOutOfFrontDoor(t *testing.T) {
	runtimeType := reflect.TypeOf((*goav.Runtime)(nil)).Elem()
	if runtimeType.NumMethod() != 1 {
		t.Fatalf("Runtime methods = %d, want only Probe", runtimeType.NumMethod())
	}
	if _, ok := runtimeType.MethodByName("New"); ok {
		t.Fatal("Runtime exposes legacy New builder; use Expert(runtime).Graph for expert graph wiring")
	}
	if _, ok := runtimeType.MethodByName("Graph"); ok {
		t.Fatal("Runtime exposes Graph; use Expert(runtime).Graph for expert graph wiring")
	}
	if reflect.TypeOf(goav.Expert).Kind() != reflect.Func {
		t.Fatal("goav.Expert should expose the expert graph entry point")
	}
}

func TestExpertGraphRequiresStandardRuntime(t *testing.T) {
	graph := goav.Expert(recipeAPIRuntimeWithoutBuilder{}).Graph()
	if _, err := graph.Describe(); !errors.Is(err, goav.ErrExpertRuntimeRequired) {
		t.Fatalf("Describe err = %v, want ErrExpertRuntimeRequired", err)
	}
	if _, err := graph.Build(context.Background()); !errors.Is(err, goav.ErrExpertRuntimeRequired) {
		t.Fatalf("Build err = %v, want ErrExpertRuntimeRequired", err)
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
		goav.RTP(recipeAPIVideoRTPReader{}).Name("video").Codec(goav.VP8()),
		goav.File("recording.ivf", io.Discard),
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
	if len(report.Destinations) != 1 || report.Destinations[0].Name != "recording.ivf" ||
		report.Destinations[0].Format != av.FormatIVF || report.Destinations[0].Kind != "mux" {
		t.Fatalf("destinations=%+v", report.Destinations)
	}
	if !equalStrings(report.Destinations[0].Branches, []string{"video"}) {
		t.Fatalf("destination branches=%+v", report.Destinations[0].Branches)
	}
	if len(report.Branches) != 1 || report.Branches[0].Name != "video" ||
		!equalOperationKinds(operationKinds(report.Branches[0].Operations), []goav.OperationKind{goav.OpDepacketize, goav.OpCopy}) {
		t.Fatalf("branches=%+v", report.Branches)
	}
	if !hasRequirement(report.RequiredAdapters, "rtp-depacketizer", av.CodecVP8, "") ||
		!hasRequirement(report.RequiredAdapters, "muxer", "", av.FormatIVF) {
		t.Fatalf("requirements=%+v", report.RequiredAdapters)
	}
	muxer, ok := adapterRequirementByKind(report.RequiredAdapters, "muxer", string(av.FormatIVF))
	if !ok ||
		muxer.MaxStreams != 1 ||
		len(muxer.Media) != 1 ||
		muxer.Media[0] != av.MediaVideo ||
		len(muxer.Codecs) != 3 ||
		muxer.Codecs[0] != av.CodecVP8 ||
		muxer.Codecs[1] != av.CodecVP9 ||
		muxer.Codecs[2] != av.CodecAV1 ||
		muxer.Metadata["summary"] == "" {
		t.Fatalf("ivf requirement=%+v, want container capability details", muxer)
	}
	if len(report.Warnings) != 0 {
		t.Fatalf("warnings=%+v", report.Warnings)
	}
}

func TestExplainReturnsPartialReportForMissingMuxer(t *testing.T) {
	report, err := recordJob(
		goav.FileInput("input.ivf", bytes.NewReader(tinyIVF())),
		goav.File("recording.mp4", io.Discard),
	).Explain(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "target_muxer_missing" {
		t.Fatalf("err = %v, want target_muxer_missing", err)
	}
	if buildErr.Operation != "open target" {
		t.Fatalf("operation = %q, want open target", buildErr.Operation)
	}
	requirement, ok := adapterRequirementByKind(report.RequiredAdapters, "muxer", string(av.FormatMP4))
	if !ok || requirement.Status != "missing" || requirement.Format != av.FormatMP4 {
		t.Fatalf("requirements=%+v, want missing mp4 muxer", report.RequiredAdapters)
	}
	if len(report.Graph.Nodes) == 0 || report.Summary == "" {
		t.Fatalf("partial report not populated: %+v", report)
	}
	if len(report.Warnings) != 1 || report.Warnings[0].Code != "target_muxer_missing" {
		t.Fatalf("warnings=%+v", report.Warnings)
	}
	if len(report.Missing) != 1 || report.Missing[0].Kind != "muxer" || report.Missing[0].Status != "missing" {
		t.Fatalf("missing=%+v", report.Missing)
	}
}

func TestExplainReturnsPartialReportForMissingTransformAdapter(t *testing.T) {
	report, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(goav.New(goav.WithStdCodecs())).
		Audio().
		Resample(16_000, goav.Mono).
		To(goav.Sink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
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

	web := goav.File("web.ogg", io.Discard)
	report, err := branchJob(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Video("v").Resize(1280, 720).Encode(goav.VP9(goav.Bitrate(2_000_000))).To(web).
		Audio("a").Resample(48_000, goav.Stereo).Encode(goav.Opus(goav.Bitrate(96_000))).To(web).
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
	want := []goav.OperationKind{goav.OpDemux, goav.OpSelect, goav.OpDecode, goav.OpTap, goav.OpTransform, goav.OpEncode}
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
			if operation.Kind == goav.OperationKind("trans"+"code") {
				t.Fatalf("branch %s has special transcode operation: %+v", name, branch.Operations)
			}
		}
	}
	if len(report.Decisions) < 4 {
		t.Fatalf("decisions=%+v, want decode and encode decisions per branch", report.Decisions)
	}
}

func TestExplainReportsBranchShapeFromProbedInput(t *testing.T) {
	rt := goav.New(
		goav.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{{
				Index: 0,
				ID:    "audio",
				Type:  av.MediaAudio,
				Codec: av.CodecParameters{
					ID:           av.CodecOpus,
					Type:         av.MediaAudio,
					SampleRate:   48000,
					Channels:     goav.Stereo,
					SampleFormat: av.SampleFormatS16,
				},
			}}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
		}),
		goav.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, recipeAPIDecoderFactory{})
		}),
	)

	report, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Audio().
		Decode().
		Tap(goav.FrameTap("audio.decoded")).
		To(goav.Sink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
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
	if branch.Shape.Domain != goav.DomainPacket ||
		branch.Shape.MediaKind != av.MediaAudio ||
		branch.Shape.StreamID != "audio" ||
		branch.Shape.Codec != av.CodecOpus ||
		branch.Shape.SampleRate != 48000 ||
		branch.Shape.Channels != goav.Stereo ||
		branch.Shape.SampleFormat != av.SampleFormatS16 {
		t.Fatalf("branch shape=%+v, want probed audio packet shape", branch.Shape)
	}
	tap, ok := tapReportByName(report.Taps, "audio.decoded")
	if !ok {
		t.Fatalf("taps=%+v, want audio.decoded", report.Taps)
	}
	if tap.Shape.Domain != goav.DomainFrame ||
		tap.Shape.MediaKind != av.MediaAudio ||
		tap.Shape.StreamID != "audio" ||
		tap.Shape.Codec != av.CodecOpus ||
		tap.Shape.SampleRate != 48000 ||
		tap.Shape.Channels != goav.Stereo ||
		tap.Shape.SampleFormat != av.SampleFormatS16 {
		t.Fatalf("tap shape=%+v, want decoded audio frame shape", tap.Shape)
	}
}

func TestExplainReportsBranchShapeFromLiveCodecIntent(t *testing.T) {
	rt := goav.New(goav.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
		registry.RegisterDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, recipeAPIDecoderFactory{})
	}))

	report, err := goav.From(goav.RTP(recipeAPIRTPReader{}).Name("audio").Codec(goav.Opus())).
		UseRuntime(rt).
		Audio().
		Decode().
		Tap(goav.FrameTap("audio.decoded")).
		To(goav.Sink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
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
	if branch.Shape.Domain != goav.DomainPacket ||
		branch.Shape.MediaKind != av.MediaAudio ||
		branch.Shape.StreamID != "audio" ||
		branch.Shape.Codec != av.CodecOpus ||
		branch.Shape.SampleRate != 48000 ||
		branch.Shape.Channels != goav.Stereo ||
		!branch.Shape.Realtime {
		t.Fatalf("branch shape=%+v, want live Opus packet shape", branch.Shape)
	}
	tap, ok := tapReportByName(report.Taps, "audio.decoded")
	if !ok {
		t.Fatalf("taps=%+v, want audio.decoded", report.Taps)
	}
	if tap.Shape.Domain != goav.DomainFrame ||
		tap.Shape.MediaKind != av.MediaAudio ||
		tap.Shape.Codec != av.CodecOpus ||
		tap.Shape.SampleRate != 48000 ||
		tap.Shape.Channels != goav.Stereo ||
		!tap.Shape.Realtime {
		t.Fatalf("tap shape=%+v, want live decoded audio frame shape", tap.Shape)
	}
}

func TestExplainReportsOperationShapeThroughResizeAndEncode(t *testing.T) {
	rt := goav.New(
		goav.WithFormatAdapter(func(registry *format.SimpleRegistry) {
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
		goav.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP9, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
		}),
		goav.WithFilterAdapter(func(registry *filter.SimpleRegistry) {
			registry.RegisterFactory(filter.Descriptor{Name: filter.FactoryResize, Input: av.MediaVideo, Output: av.MediaVideo}, recipeAPIFilterFactory{})
		}),
	)

	web := goav.File("web.ogg", io.Discard)
	report, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Video().
		Decode().
		Branches(
			goav.Branch("preview").
				Resize(1280, 720).
				Shape(goav.Shape(goav.ShapeFramerate(30, 1))).
				Encode(goav.VP9(goav.Bitrate(2_000_000))).
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
	if branch.Shape.Domain != goav.DomainPacket ||
		branch.Shape.MediaKind != av.MediaVideo ||
		branch.Shape.Codec != av.CodecVP8 ||
		branch.Shape.Width != 1920 ||
		branch.Shape.Height != 1080 ||
		branch.Shape.PixelFormat != av.PixelFormatYUV420P {
		t.Fatalf("branch shape=%+v, want probed VP8 1920x1080 packet shape", branch.Shape)
	}
	resize, ok := operationReportByKind(branch.Operations, goav.OpTransform)
	if !ok {
		t.Fatalf("operations=%+v, want resize operation", branch.Operations)
	}
	if resize.Shape.Domain != goav.DomainFrame ||
		resize.Shape.MediaKind != av.MediaVideo ||
		resize.Shape.Codec != av.CodecVP8 ||
		resize.Shape.Width != 1280 ||
		resize.Shape.Height != 720 ||
		resize.Shape.PixelFormat != av.PixelFormatYUV420P {
		t.Fatalf("resize shape=%+v, want frame VP8 1280x720 shape", resize.Shape)
	}
	shape, ok := operationReportByKind(branch.Operations, goav.OpShape)
	if !ok {
		t.Fatalf("operations=%+v, want shape operation", branch.Operations)
	}
	if shape.Shape.Domain != goav.DomainFrame ||
		shape.Shape.MediaKind != av.MediaVideo ||
		shape.Shape.Codec != av.CodecVP8 ||
		shape.Shape.Width != 1280 ||
		shape.Shape.Height != 720 ||
		shape.Shape.PixelFormat != av.PixelFormatYUV420P ||
		shape.Shape.Framerate != (goav.Rational{Num: 30, Den: 1}) {
		t.Fatalf("shape operation=%+v, want frame VP8 1280x720@30 shape", shape.Shape)
	}
	encode, ok := operationReportByKind(branch.Operations, goav.OpEncode)
	if !ok {
		t.Fatalf("operations=%+v, want encode operation", branch.Operations)
	}
	if encode.Shape.Domain != goav.DomainPacket ||
		encode.Shape.MediaKind != av.MediaVideo ||
		encode.Shape.StreamID != "preview" ||
		encode.Shape.Codec != av.CodecVP9 ||
		encode.Shape.Width != 1280 ||
		encode.Shape.Height != 720 ||
		encode.Shape.PixelFormat != av.PixelFormatYUV420P ||
		encode.Shape.Framerate != (goav.Rational{Num: 30, Den: 1}) {
		t.Fatalf("encode shape=%+v, want packet VP9 1280x720@30 shape", encode.Shape)
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
		UseRuntime(goav.Default()).
		Video().
		Decode().
		Shape(goav.Shape(goav.ShapeDomain(goav.DomainPacket), goav.ShapeMedia(av.MediaVideo))).
		Encode(goav.VP9(goav.Bitrate(2_000_000))).
		To(goav.File("preview.ivf", io.Discard)).
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
	rt := goav.New(
		goav.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{
				{Index: 0, ID: "video", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo}},
			}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
			registry.RegisterMuxer(av.FormatOgg, recipeAPIMuxerFactory{})
		}),
		goav.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP9, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
		}),
		goav.WithFilterAdapter(func(registry *filter.SimpleRegistry) {
			registry.RegisterFactory(filter.Descriptor{Name: filter.FactoryResize, Input: av.MediaVideo, Output: av.MediaVideo}, recipeAPIFilterFactory{})
		}),
	)
	meter := goav.FrameFunc("meter", func(ctx context.Context, frame *goav.Frame, emit goav.Emit) error {
		return emit.Frame(frame)
	})

	web := goav.File("web.ogg", io.Discard)
	report, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Video().
		Decode().
		Branches(
			goav.Branch("v360").
				Resize(640, 360).
				Do(meter).
				Encode(goav.VP9(goav.Bitrate(600_000))).
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
	wantOps := []goav.OperationKind{goav.OpDemux, goav.OpSelect, goav.OpDecode, goav.OpTransform, goav.OpStage, goav.OpEncode}
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
	rt := goav.New(
		goav.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{
				{Index: 0, ID: "audio", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}},
			}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
		}),
		goav.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, recipeAPIDecoderFactory{})
		}),
		goav.WithFilterAdapter(func(registry *filter.SimpleRegistry) {
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
		Resample(16_000, goav.Mono).
		To(goav.Sink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
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
		Resample(16_000, goav.Mono).
		To(goav.Sink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
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
	rt := goav.New(
		goav.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{
				{Index: 0, ID: "audio", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}},
			}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
		}),
		goav.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, recipeAPIDecoderFactory{})
		}),
		goav.WithFilterAdapter(func(registry *filter.SimpleRegistry) {
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
		Resample(16_000, goav.Mono).
		To(goav.Sink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
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
	rt := goav.New(goav.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
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
		Encode(goav.Codec(custom, av.MediaAudio)).
		To(goav.Sink(goav.SinkFunc("packets", func(context.Context, goav.Message) error {
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
	rt := goav.New(
		goav.WithFormatAdapter(func(registry *format.SimpleRegistry) {
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
		goav.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
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
		To(goav.Sink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
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
	rt := goav.New(
		goav.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{
				{Index: 0, ID: "video", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo}},
			}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
			registry.RegisterMuxer(av.FormatIVF, recipeAPIMuxerFactory{})
		}),
		goav.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP9, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
		}),
	)

	web := goav.File("web.ivf", io.Discard, goav.Format(av.FormatIVF))
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Video().
		Decode().
		Branches(
			goav.Branch("v8").Encode(goav.VP8(goav.Bitrate(600_000))).To(web),
			goav.Branch("v9").Encode(goav.VP9(goav.Bitrate(900_000))).To(web),
		).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "target_mux_incompatible" {
		t.Fatalf("err = %v, want target_mux_incompatible", err)
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
	rt := goav.New(
		goav.WithFormatAdapter(func(registry *format.SimpleRegistry) {
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
		goav.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
		}),
	)

	target := goav.File("out.mkv", io.Discard, goav.Format(av.FormatMatroska))
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Video().
		Decode().
		Branches(goav.Branch("video").Encode(goav.VP8(goav.Bitrate(600_000))).To(target)).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "target_mux_incompatible" {
		t.Fatalf("err = %v, want target_mux_incompatible", err)
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
	rt := goav.New(
		goav.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{
				{Index: 0, ID: "video", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo}},
			}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
			registry.RegisterMuxer(av.FormatIVF, recipeAPIMuxerFactory{})
		}),
		goav.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP9, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
		}),
	)

	web := goav.File("web.ivf", io.Discard, goav.Format(av.FormatIVF))
	report, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Video().
		Decode().
		Branches(
			goav.Branch("v8").Encode(goav.VP8(goav.Bitrate(600_000))).To(web),
			goav.Branch("v9").Encode(goav.VP9(goav.Bitrate(900_000))).To(web),
		).
		Explain(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "target_mux_incompatible" {
		t.Fatalf("err = %v, want target_mux_incompatible", err)
	}
	if !hasPlanWarning(report.Warnings, "target_mux_incompatible") {
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
	rt := goav.New(
		goav.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{
				{Index: 0, ID: "video", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo}},
			}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
			registry.RegisterMuxer(av.FormatAnnexB, recipeAPIMuxerFactory{})
		}),
		goav.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
		}),
	)

	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Video().
		Decode().
		Encode(goav.VP8(goav.Bitrate(600_000))).
		To(goav.File("out.h264", io.Discard, goav.Format(av.FormatAnnexB))).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "target_mux_incompatible" {
		t.Fatalf("err = %v, want target_mux_incompatible", err)
	}
	if !strings.Contains(err.Error(), "Annex B destinations support one H264 video stream") ||
		!strings.Contains(err.Error(), "destination=out.h264") ||
		!strings.Contains(err.Error(), "branch=video codec=vp8 media=video") {
		t.Fatalf("err = %v, want Annex B codec guidance", err)
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

func TestRuntimeBranchTapAnchorIsPublicAPI(t *testing.T) {
	graph := goav.Expert(goav.Default()).Graph()
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
	_, err = task.Attach(context.Background(),
		goav.Branch("wrong-domain").
			From(goav.PacketTap("audio.decoded")).
			To(goav.Sink(goav.SinkFunc("wrong-domain", func(context.Context, goav.Message) error {
				return nil
			}))),
	)
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "tap_domain_mismatch" {
		t.Fatalf("err = %v, want tap_domain_mismatch", err)
	}
	attachment, err := task.Attach(context.Background(),
		goav.Branch("levels").
			From(goav.FrameTap("audio.decoded")).
			To(goav.Sink(goav.SinkFunc("levels", func(context.Context, goav.Message) error {
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

func TestTypedTapRefsDriveStreamIntent(t *testing.T) {
	decoded := goav.FrameTap("audio.decoded")
	encoded := goav.PacketTap("audio.encoded")

	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		Tap(decoded).
		Encode(goav.Opus(goav.Bitrate(96_000))).
		Tap(encoded).
		To(goav.File("encoded.ogg", io.Discard))

	intent := job.Intent()
	if len(intent.Streams) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
	taps := intent.Streams[0].Taps
	if len(taps) != 2 ||
		taps[0].Name != decoded.Name() ||
		taps[0].Domain != goav.DomainFrame ||
		taps[0].After != goav.OpDecode ||
		taps[1].Name != encoded.Name() ||
		taps[1].Domain != goav.DomainPacket ||
		taps[1].After != goav.OpEncode {
		t.Fatalf("taps: %+v", taps)
	}
}

func TestTypedTapDomainMismatchIsActionable(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Copy().
		Tap(goav.FrameTap("audio.packets")).
		To(goav.File("copy.ogg", io.Discard)).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "tap_domain_mismatch" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want tap_domain_mismatch wrapping ErrUnsupportedBuild", err)
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
				To(goav.Sink(goav.SinkFunc("levels", func(context.Context, goav.Message) error {
					return nil
				}))),
		).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "tap_domain_mismatch" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want tap_domain_mismatch wrapping ErrUnsupportedBuild", err)
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
	for _, advanced := range []string{
		"WithFormatAdapter",
		"UseRuntime",
		"RTPBuffer",
		"MaxTimestampGap",
		"Runtime.Graph",
		"Expert(",
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
		"goav.FrameTap(",
		"goav.PacketTap(",
		"goav.Sink(",
		"goav.File(",
		"goav.Flow(",
		"Reuse the same destination value",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("README should show %s in the public composition grammar", required)
		}
	}
}

func TestReadmeShowsCustomDestinations(t *testing.T) {
	body, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, required := range []string{
		"## Custom Destinations",
		"goav.Writer(",
		"goav.Custom(",
		"goav.DestinationInfo",
		"goav.Object(",
		"goav.Format(",
		"goav.MIME(",
		"goav.Metadata(",
		"Reuse one destination value",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("README should keep custom destination text %q", required)
		}
	}
}

func TestReadmeShowsCustomSources(t *testing.T) {
	body, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, required := range []string{
		"goav.Source(",
		"goav.PacketShape(",
		"goav.SourcePush",
		"push.Packet(",
		"push.Frame(",
		"push.EOS()",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("README should keep custom source text %q", required)
		}
	}
}

func TestDocsShowDebugDiagnosticsWorkflow(t *testing.T) {
	for _, file := range []string{
		"README.md",
		"docs/USE_CASES.md",
	} {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		text := string(body)
		for _, required := range []string{
			"Debug And Diagnostics",
			"job.Explain(ctx)",
			"task.Events()",
			"task.Attach(ctx",
			"Attachment.Snapshot()",
			"Task.Snapshot()",
			"task.Snapshot()",
			"goav.FrameFunc(\"rms\"",
		} {
			if !strings.Contains(text, required) {
				t.Fatalf("%s should show debug diagnostics workflow %q", file, required)
			}
		}
	}
}

func TestDocsShowCodecControlsAndDeclarativePerformanceGoal(t *testing.T) {
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	readmeText := string(readme)
	for _, required := range []string{
		"goav.Config(",
		"goav.Param(",
		"goav.Control(",
		"Opus, VP8, and VP9 are the full encode/decode recipe verticals",
		"public grammar stays Input, Stream, Operation, Tap, Branch, Destination, Flow,",
		"workflows should be expressible through declarative recipes",
	} {
		if !strings.Contains(readmeText, required) {
			t.Fatalf("README should keep codec/declarative goal text %q", required)
		}
	}

	performance, err := os.ReadFile("docs/PERFORMANCE.md")
	if err != nil {
		t.Fatal(err)
	}
	performanceText := string(performance)
	for _, required := range []string{
		"Hot paths must avoid hidden allocation",
		"Keep recipe, flow, branch, tap, destination, and codec abstractions cold-path",
		"do not dispatch through them for each packet or frame",
		"one cold-path executable `WorkPlan` and runtime `WorkPatch`",
		"must not route",
		"workflow-specific compiler dispatch",
	} {
		if !strings.Contains(performanceText, required) {
			t.Fatalf("performance docs should keep zero-cost goal text %q", required)
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
		"`Destination` is",
		"the routing handle: reusing the same `Destination` value",
		"DestinationProvider` is the extension point",
		"Direct `.To(...)` streams are only ergonomic syntax",
		"`branchComposePlan`, `runtimeBranch`, `destinationNames`",
		"normal composition does not import `goav/transcode`",
	} {
		if !strings.Contains(progressText, required) {
			t.Fatalf("progress should keep graph-plan goal text %q", required)
		}
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
		"node names from `Task.Describe()`",
		"`Target`, destination constructors",
		"`File`, `URIOut`, and `Sink` destination constructors",
		"TargetRef",
		"Recipes: From, chains, taps, branches, targets",
		"Intent graph: inputs, selected media, chain operations, targets, policies",
		"`Target`, `Destination`, and `Chain` composition",
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
		"direct `File`/`URIOut`/`Sink` destinations",
		"custom `Writer` destinations with `DestinationInfo`",
		"stable destination handles for shared mux/sink groups",
		"stable goav-owned destination handles",
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
	graph := goav.Expert(goav.Default()).Graph()
	node := graph.Source("source", recipeAPISource{name: "source"})
	_ = goav.Branch("node").From(node)
	_ = goav.Branch("stream").From(node.Stream("audio"))
}

func TestReadmeFlowExampleUsesDistinctBranches(t *testing.T) {
	body, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	if strings.Contains(text, "Apply(voice).\n    Apply(voice)") {
		t.Fatal("README should not show repeated direct flow application when branches are the intended split")
	}
	if got := strings.Count(text, `goav.Branch("voice").Apply(voice).To(voiceOut)`); got != 1 {
		t.Fatalf("README voice flow branch count = %d, want 1", got)
	}
	if got := strings.Count(text, `goav.Branch("archive").Apply(archive).To(archiveOut)`); got != 1 {
		t.Fatalf("README archive flow branch count = %d, want 1", got)
	}
}

func TestDocsExplainFlowVersusBranchRule(t *testing.T) {
	for _, file := range []string{"README.md", "docs/USE_CASES.md"} {
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
		"From(input) -> Chain -> operations -> Tap -> Branch -> Destination -> Task",
		"MediaShape",
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
	for _, file := range []string{"README.md", "docs/USE_CASES.md"} {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		text := string(body)
		for _, required := range []string{
			"Branch buffers",
			"goav.Blocking(",
			"goav.DropOldest(",
			"goav.Latest()",
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
	end := strings.Index(text, "## Composition Patterns")
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

func TestReadmeUsesDestinationOptions(t *testing.T) {
	body, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, required := range []string{
		"goav.File(\"\", out, goav.Format(av.FormatIVF))",
		"goav.Writer(",
		"goav.Object(",
		"goav.Format(",
		"goav.MIME(",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("README should keep destination option example %q", required)
		}
	}
	for _, forbidden := range []string{
		").Format(",
		").MIME(",
		"ConfigurableDestination",
		"Adapter-Backed Workflows",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("README should not teach stale destination/API spelling %q", forbidden)
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

func TestStreamIntentUsesTypedTapAnchor(t *testing.T) {
	stream := reflect.TypeOf(goav.StreamIntent{})
	if _, ok := stream.FieldByName("FromTap"); ok {
		t.Fatal("StreamIntent exposes FromTap; use typed TapRef field From")
	}
	field, ok := stream.FieldByName("From")
	if !ok {
		t.Fatal("StreamIntent should expose typed tap anchor field From")
	}
	if field.Type != reflect.TypeOf(goav.TapRef{}) {
		t.Fatalf("StreamIntent.From type = %s, want goav.TapRef", field.Type)
	}
}

func TestPublicIntentAndReportsUseDestinations(t *testing.T) {
	for _, tt := range []struct {
		name string
		typ  reflect.Type
	}{
		{name: "Intent", typ: reflect.TypeOf(goav.Intent{})},
		{name: "StreamIntent", typ: reflect.TypeOf(goav.StreamIntent{})},
		{name: "PlanReport", typ: reflect.TypeOf(goav.PlanReport{})},
		{name: "BranchReport", typ: reflect.TypeOf(goav.BranchReport{})},
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
			t.Fatalf("%s uses endpoint vocabulary; use target and target-ref naming in high-level composition code", file)
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

func TestRecipeReportsUnsupportedCustomRuntime(t *testing.T) {
	_, err := recordJob(
		goav.FileInput("input.ivf", strings.NewReader("")),
		goav.File("recording.ivf", io.Discard),
	).UseRuntime(recipeAPIRuntimeWithoutBuilder{}).Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "runtime_unsupported" {
		t.Fatalf("err = %v, want runtime_unsupported", err)
	}
	if !strings.Contains(err.Error(), "recipe compilation requires a goav runtime") ||
		!strings.Contains(err.Error(), "goav.Default") ||
		!strings.Contains(err.Error(), "goav.New") {
		t.Fatalf("err = %v, want runtime guidance", err)
	}
}

func TestReadmeRecordRecipeIsSmall(t *testing.T) {
	job := recordJob(
		goav.FileInput("input.ogg", strings.NewReader("")),
		goav.File("recording.ogg", io.Discard),
	)

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(specText(spec), "input.ogg -> recording.ogg") {
		t.Fatalf("spec:\n%s", specText(spec))
	}
	intent := job.Intent()
	if intent.Name != "from" || len(intent.Inputs) != 1 || len(intent.Destinations) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestRecordRecipeCanWriteToTypedTarget(t *testing.T) {
	target := goav.File("recording.ivf", io.Discard)
	job := goav.From(goav.FileInput("input.ivf", strings.NewReader(""))).
		Copy().
		To(target)

	intent := job.Intent()
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
		goav.File("archive.ivf", io.Discard),
		goav.File("preview.ivf", io.Discard),
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
	if intent.Name != "from" || len(intent.Inputs) != 1 || len(intent.Destinations) != 2 {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestReadmeAudioDecodeRecipeIsSmall(t *testing.T) {
	sink := goav.SinkFunc("frames", func(context.Context, goav.Message) error {
		return nil
	})
	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
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
	intent := job.Intent()
	if len(intent.Streams) != 1 || !intent.Streams[0].Decode || intent.Streams[0].Encode.ID != "" ||
		len(intent.Destinations) != 1 || intent.Destinations[0].Name != "levels" {
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
		To(goav.Sink(sink))

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

func TestAudioChainAppliesToStreamRecipeIntent(t *testing.T) {
	voice := goav.Flow("voice").
		Audio().
		Resample(16_000, goav.Mono).
		Encode(goav.Opus(goav.Bitrate(32_000), goav.Channels(goav.Mono)))

	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Apply(voice).
		To(goav.File("voice.ogg", io.Discard))

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
		len(stream.Destinations) != 1 || stream.Destinations[0] != "voice.ogg" {
		t.Fatalf("stream intent: %+v", stream)
	}
}

func TestStreamRecipeCanWriteToTypedTarget(t *testing.T) {
	voice := goav.Flow("voice").
		Audio().
		Resample(16_000, goav.Mono).
		Encode(goav.Opus(goav.Bitrate(32_000), goav.Channels(goav.Mono)))
	voiceOut := goav.File("voice.ogg", io.Discard)

	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Apply(voice).
		To(voiceOut)

	intent := job.Intent()
	if len(intent.Streams) != 1 || len(intent.Destinations) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
	stream := intent.Streams[0]
	if stream.Select.Type != av.MediaAudio ||
		!stream.Decode ||
		stream.Encode.ID != av.CodecOpus ||
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
	targets := []goav.Destination{
		goav.File("archive.ogg", io.Discard),
		goav.Sink(goav.SinkFunc("stats", func(context.Context, goav.Message) error {
			return nil
		})),
	}

	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Encode(goav.Opus(goav.Bitrate(96_000))).
		To(targets...)

	intent := job.Intent()
	if len(intent.Streams) != 1 ||
		!equalStrings(intent.Streams[0].Destinations, []string{"archive.ogg", "stats"}) ||
		len(intent.Destinations) != 2 ||
		intent.Destinations[0].Name != "archive.ogg" ||
		intent.Destinations[1].Name != "stats" {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestSharedDestinationHandleGroupsBranches(t *testing.T) {
	web := goav.File("web.ivf", io.Discard, goav.Format(av.FormatIVF))

	job := goav.From(goav.FileInput("input.ivf", strings.NewReader(""))).
		Video().
		Decode().
		Branches(
			goav.Branch("v720").Encode(goav.VP9(goav.Bitrate(2_000_000))).To(web),
			goav.Branch("v360").Resize(640, 360).Encode(goav.VP8(goav.Bitrate(600_000))).To(web),
		)

	intent := job.Intent()
	if len(intent.Streams) != 2 || len(intent.Destinations) != 1 || intent.Destinations[0].Name != "web.ivf" {
		t.Fatalf("intent: %+v", intent)
	}
	if !equalStrings(intent.Streams[0].Destinations, []string{"web.ivf"}) ||
		!equalStrings(intent.Streams[1].Destinations, []string{"web.ivf"}) {
		t.Fatalf("streams: %+v", intent.Streams)
	}
}

func TestDuplicateDestinationNameRequiresSameHandle(t *testing.T) {
	left := goav.File("web.ivf", io.Discard, goav.Format(av.FormatIVF))
	right := goav.File("web.ivf", io.Discard, goav.Format(av.FormatIVF))

	_, err := goav.From(goav.FileInput("input.ivf", strings.NewReader(""))).
		Video().
		Decode().
		Branches(
			goav.Branch("v720").Encode(goav.VP9(goav.Bitrate(2_000_000))).To(left),
			goav.Branch("v360").Encode(goav.VP8(goav.Bitrate(600_000))).To(right),
		).
		Describe()
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "target_duplicate" {
		t.Fatalf("err = %v, want target_duplicate", err)
	}
}

func TestDestinationConstructorsReturnDestination(t *testing.T) {
	destinationType := reflect.TypeOf((*goav.Destination)(nil)).Elem()
	for name, fn := range map[string]any{
		"Custom":      goav.Custom,
		"File":        goav.File,
		"Object":      goav.Object,
		"URIOut":      goav.URIOut,
		"Writer":      goav.Writer,
		"WriteCloser": goav.WriteCloser,
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

func TestExternalCustomDestinationCanBeTargeted(t *testing.T) {
	dest := recipeAPICustomDestination{}
	target := goav.Custom("custom", dest)

	job := goav.From(goav.RTP(recipeAPIVideoRTPReader{}).Name("video").Codec(goav.VP8())).
		Video().
		Copy().
		To(target)

	intent := job.Intent()
	if len(intent.Destinations) != 1 ||
		intent.Destinations[0].Name != "custom" ||
		intent.Destinations[0].Format != av.FormatIVF ||
		intent.Destinations[0].MIMEType != "video/ivf" {
		t.Fatalf("intent: %+v", intent)
	}
	if len(intent.Streams) != 1 || !equalStrings(intent.Streams[0].Destinations, []string{"custom"}) {
		t.Fatalf("intent streams: %+v", intent.Streams)
	}
}

func TestDestinationProviderIsWrappedByCustomHandle(t *testing.T) {
	providerType := reflect.TypeOf((*goav.DestinationProvider)(nil)).Elem()
	provider := reflect.TypeOf(recipeAPICustomDestination{})
	if !provider.Implements(providerType) {
		t.Fatalf("%v should implement DestinationProvider", provider)
	}
	destinationType := reflect.TypeOf(goav.Custom("custom", recipeAPICustomDestination{}))
	if destinationType.Name() != "Destination" || destinationType.Kind() != reflect.Struct {
		t.Fatalf("Custom return type = %v, want concrete Destination handle", destinationType)
	}
	if provider.AssignableTo(destinationType) {
		t.Fatalf("provider %v should not be assignable to Destination handle %v", provider, destinationType)
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
	meter := goav.FrameFunc("meter", func(context.Context, *av.Frame, goav.Emit) error {
		return nil
	})
	voice := goav.Flow("voice").
		Audio().
		Do(meter).
		Tap(goav.FrameTap("audio.after-meter")).
		Resample(16_000, goav.Mono).
		Encode(goav.Opus(goav.Bitrate(32_000), goav.Channels(goav.Mono)))

	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Apply(voice).
		To(goav.File("voice.ogg", io.Discard))

	intent := job.Intent()
	if len(intent.Streams) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
	operations := intent.Streams[0].Operations
	if len(operations) != 5 ||
		operations[0].Kind != goav.OpDecode ||
		operations[1].Kind != goav.OpStage || operations[1].Component != "meter" ||
		operations[2].Kind != goav.OpTap || operations[2].Tap.Name != "audio.after-meter" ||
		operations[3].Kind != goav.OpTransform || operations[3].Transform.Resample == nil ||
		operations[4].Kind != goav.OpEncode || operations[4].Encode.ID != av.CodecOpus {
		t.Fatalf("operations: %+v", operations)
	}
	if len(intent.Streams[0].Taps) != 1 || intent.Streams[0].Taps[0].Name != "audio.after-meter" {
		t.Fatalf("taps: %+v", intent.Streams[0].Taps)
	}
}

func TestFlowTapAfterEncodeIsPacketTap(t *testing.T) {
	voice := goav.Flow("voice").
		Audio().
		Encode(goav.Opus(goav.Bitrate(32_000), goav.Channels(goav.Mono))).
		Tap(goav.PacketTap("audio.voice.packets"))

	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Apply(voice).
		To(goav.File("voice.ogg", io.Discard))

	intent := job.Intent()
	if len(intent.Streams) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
	operations := intent.Streams[0].Operations
	if len(operations) != 3 ||
		operations[0].Kind != goav.OpDecode ||
		operations[1].Kind != goav.OpEncode ||
		operations[2].Kind != goav.OpTap ||
		operations[2].Tap.Name != "audio.voice.packets" ||
		operations[2].Tap.Domain != goav.DomainPacket ||
		operations[2].Tap.After != goav.OpEncode {
		t.Fatalf("operations: %+v", operations)
	}
	if len(intent.Streams[0].Taps) != 1 ||
		intent.Streams[0].Taps[0].Name != "audio.voice.packets" ||
		intent.Streams[0].Taps[0].Domain != goav.DomainPacket ||
		intent.Streams[0].Taps[0].After != goav.OpEncode {
		t.Fatalf("taps: %+v", intent.Streams[0].Taps)
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
		To(goav.File("copy.ogg", io.Discard))

	intent := job.Intent()
	if len(intent.Streams) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
	stream := intent.Streams[0]
	if stream.Decode ||
		!stream.Encode.Copy ||
		len(stream.Operations) != 2 ||
		stream.Operations[0].Kind != goav.OpCopy ||
		stream.Operations[1].Kind != goav.OpTap ||
		stream.Operations[1].Tap.Name != "audio.copied" ||
		stream.Operations[1].Tap.Domain != goav.DomainPacket ||
		stream.Operations[1].Tap.After != goav.OpCopy {
		t.Fatalf("stream intent: %+v", stream)
	}
	if len(stream.Taps) != 1 ||
		stream.Taps[0].Name != "audio.copied" ||
		stream.Taps[0].Domain != goav.DomainPacket ||
		stream.Taps[0].After != goav.OpCopy {
		t.Fatalf("taps: %+v", stream.Taps)
	}
}

func TestFlowBranchesStayOnJobAndBuildIntent(t *testing.T) {
	voice := goav.Flow("voice").
		Audio().
		Resample(16_000, goav.Mono).
		Encode(goav.Opus(goav.Bitrate(32_000), goav.Channels(goav.Mono)))
	archive := goav.Flow("archive").
		Audio().
		Resample(48_000, goav.Stereo).
		Encode(goav.Opus(goav.Bitrate(128_000), goav.Channels(goav.Stereo)))
	voiceOut := goav.File("voice.ogg", io.Discard)
	archiveOut := goav.File("archive.ogg", io.Discard)

	job := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Audio(goav.StreamIndex(0)).
		Branches(
			goav.Branch("voice").Apply(voice).To(voiceOut),
			goav.Branch("archive").Apply(archive).To(archiveOut),
		)

	if reflect.TypeOf(job) != reflect.TypeOf((*goav.Job)(nil)) {
		t.Fatalf("Branches returned %T, want *goav.Job", job)
	}
	intent := job.Intent()
	if len(intent.Streams) != 2 || len(intent.Destinations) != 2 {
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
	preview := goav.Flow("preview").
		Video().
		Resize(640, 360).
		Tap(goav.FrameTap("preview.frames")).
		Encode(goav.VP9(goav.Bitrate(600_000)))
	web := goav.File("preview.webm", io.Discard)

	job := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("preview").
		Apply(preview).
		To(web)

	intent := job.Intent()
	if len(intent.Streams) != 1 || intent.Streams[0].Name != "preview" ||
		len(intent.Streams[0].Transforms) != 1 ||
		intent.Streams[0].Transforms[0].Resize.Width != 640 ||
		intent.Streams[0].Transforms[0].Resize.Height != 360 ||
		intent.Streams[0].Encode.ID != av.CodecVP9 ||
		intent.Streams[0].Encode.Bitrate != 600_000 {
		t.Fatalf("intent: %+v", intent)
	}
	if !tapIntentNamesContain(intent.Streams[0].Taps, "preview.frames") {
		t.Fatalf("taps: %+v, want preview.frames", intent.Streams[0].Taps)
	}
	var previewTap goav.TapIntent
	for _, tap := range intent.Streams[0].Taps {
		if tap.Name == "preview.frames" {
			previewTap = tap
			break
		}
	}
	if previewTap.Name == "" || previewTap.After != goav.OpTransform {
		t.Fatalf("taps: %+v, want preview.frames after transform", intent.Streams[0].Taps)
	}
	operations := intent.Streams[0].Operations
	if len(operations) != 5 ||
		operations[0].Kind != goav.OpDecode ||
		operations[1].Kind != goav.OpTap || operations[1].Tap.Name != "video.decoded" ||
		operations[2].Kind != goav.OpTransform ||
		operations[3].Kind != goav.OpTap || operations[3].Tap.Name != "preview.frames" ||
		operations[4].Kind != goav.OpEncode {
		t.Fatalf("operations: %+v", operations)
	}
}

func TestBranchesGroupSelectedStreams(t *testing.T) {
	watch := goav.File("watch.webm", io.Discard)
	mobile := goav.File("mobile.webm", io.Discard)
	job := goav.From(goav.FileInput("source.webm", strings.NewReader(""))).
		Video().
		Decode().
		Tap(goav.FrameTap("video.decoded")).
		Branches(
			goav.Branch("v1080").
				Resize(1920, 1080).
				Encode(goav.VP9(goav.Bitrate(4_000_000))).
				To(watch),
			goav.Branch("v360").
				Resize(640, 360).
				Encode(goav.VP8(goav.Bitrate(600_000))).
				To(mobile),
		).
		Audio().
		Decode().
		Tap(goav.FrameTap("audio.decoded")).
		Branches(
			goav.Branch("a96").
				Resample(48_000, goav.Stereo).
				Encode(goav.Opus(goav.Bitrate(96_000))).
				To(watch, mobile),
		)

	intent := job.Intent()
	if len(intent.Streams) != 3 || len(intent.Destinations) != 2 {
		t.Fatalf("intent: %+v", intent)
	}
	tests := []struct {
		name       string
		from       string
		fromDomain goav.MediaDomain
		codec      av.CodecID
		outputs    []string
	}{
		{name: "v1080", from: "video.decoded", fromDomain: goav.DomainFrame, codec: av.CodecVP9, outputs: []string{"watch.webm"}},
		{name: "v360", from: "video.decoded", fromDomain: goav.DomainFrame, codec: av.CodecVP8, outputs: []string{"mobile.webm"}},
		{name: "a96", from: "audio.decoded", fromDomain: goav.DomainFrame, codec: av.CodecOpus, outputs: []string{"watch.webm", "mobile.webm"}},
	}
	for i := range tests {
		stream := intent.Streams[i]
		if stream.Name != tests[i].name || stream.From.Name() != tests[i].from || stream.From.Domain() != tests[i].fromDomain ||
			stream.Encode.ID != tests[i].codec || !equalStrings(stream.Destinations, tests[i].outputs) {
			t.Fatalf("stream[%d]=%+v, want %+v", i, stream, tests[i])
		}
	}
	if intent.Streams[0].Transforms[0].Resize.Width != 1920 ||
		intent.Streams[1].Transforms[0].Resize.Width != 640 ||
		intent.Streams[2].Transforms[0].Resample.SampleRate != 48_000 {
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
	meter := goav.FrameFunc("meter", func(ctx context.Context, frame *goav.Frame, emit goav.Emit) error {
		return emit.Frame(frame)
	})
	web := goav.File("web.webm", io.Discard)
	job := goav.From(goav.FileInput("source.webm", strings.NewReader(""))).
		Video().
		Decode().
		Tap(goav.FrameTap("video.decoded")).
		Branches(
			goav.Branch("v360").
				Do(meter).
				Resize(640, 360).
				Encode(goav.VP9(goav.Bitrate(600_000))).
				To(web),
		)

	intent := job.Intent()
	if len(intent.Streams) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
	want := []goav.OperationKind{goav.OpDecode, goav.OpTap, goav.OpStage, goav.OpTransform, goav.OpEncode}
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
	archive := goav.File("archive.ogg", io.Discard)
	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		Tap(goav.FrameTap("audio.decoded")).
		Branches(
			goav.Branch("archive").
				Encode(goav.Opus(goav.Bitrate(96_000))).
				Tap(goav.PacketTap("audio.encoded")).
				To(archive),
		)

	intent := job.Intent()
	if len(intent.Streams) != 1 {
		t.Fatalf("streams: %+v", intent.Streams)
	}
	var encodedTap goav.TapIntent
	for _, tap := range intent.Streams[0].Taps {
		if tap.Name == "audio.encoded" {
			encodedTap = tap
			break
		}
	}
	if encodedTap.Name == "" ||
		encodedTap.Domain != goav.DomainPacket ||
		encodedTap.MediaKind != av.MediaAudio ||
		encodedTap.After != goav.OpEncode {
		t.Fatalf("encoded tap = %+v, want packet tap after encode", encodedTap)
	}
	operations := intent.Streams[0].Operations
	if len(operations) != 4 ||
		operations[0].Kind != goav.OpDecode ||
		operations[1].Kind != goav.OpTap || operations[1].Tap.Name != "audio.decoded" ||
		operations[2].Kind != goav.OpEncode ||
		operations[3].Kind != goav.OpTap || operations[3].Tap.Name != "audio.encoded" ||
		operations[3].Tap.Domain != goav.DomainPacket ||
		operations[3].Tap.After != goav.OpEncode {
		t.Fatalf("operations: %+v", operations)
	}
}

func TestBranchCustomStageUsesOrderedOperations(t *testing.T) {
	meter := goav.FrameFunc("meter", func(ctx context.Context, frame *goav.Frame, emit goav.Emit) error {
		return emit.Frame(frame)
	})
	web := goav.File("web.webm", io.Discard)
	job := goav.From(goav.FileInput("source.webm", strings.NewReader(""))).
		Video().
		Decode().
		Tap(goav.FrameTap("video.decoded")).
		Branches(
			goav.Branch("v360").
				Resize(640, 360).
				Do(meter).
				Encode(goav.VP9(goav.Bitrate(600_000))).
				To(web),
		)

	intent := job.Intent()
	if len(intent.Streams) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
	want := []goav.OperationKind{goav.OpDecode, goav.OpTap, goav.OpTransform, goav.OpStage, goav.OpEncode}
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

func TestFlowMediaMismatchIsActionable(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Apply(goav.Flow("voice").Audio().Encode(goav.Opus(goav.Bitrate(32_000), goav.Channels(goav.Mono)))).
		To(goav.File("voice.webm", io.Discard)).
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
				Apply(goav.Flow("voice").Audio().Encode(goav.Opus(goav.Bitrate(32_000), goav.Channels(goav.Mono)))).
				To(goav.File("voice.webm", io.Discard)),
		).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "flow_media_mismatch" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want flow_media_mismatch wrapping ErrUnsupportedBuild", err)
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
				Apply(goav.Flow("voice").Audio().Resample(16_000, goav.Mono)).
				Apply(goav.Flow("preview").Video().Resize(640, 360)).
				To(goav.File("mixed.webm", io.Discard)),
		).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "flow_media_mismatch" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want flow_media_mismatch wrapping ErrUnsupportedBuild", err)
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
		Resample(16_000, goav.Mono).
		Encode(goav.Opus(goav.Bitrate(32_000), goav.Channels(goav.Mono)))
	branch := goav.Branch("voice").
		Apply(flow).
		To(goav.File("voice.ogg", io.Discard))

	flow.Resample(8_000, goav.Mono)
	job := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Audio().
		Branches(branch)

	intent := job.Intent()
	if len(intent.Streams) != 1 ||
		len(intent.Streams[0].Transforms) != 1 ||
		intent.Streams[0].Transforms[0].Resample.SampleRate != 16_000 {
		t.Fatalf("intent after mutating flow: %+v", intent)
	}
}

func TestFlowDecodeAppliesToPacketBranchIntent(t *testing.T) {
	flow := goav.Flow("voice").
		Audio().
		Decode().
		Resample(16_000, goav.Mono).
		Encode(goav.Opus(goav.Bitrate(64_000)))
	target := goav.Sink(goav.SinkFunc("voice", func(context.Context, goav.Message) error {
		return nil
	}))

	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Copy().
		Branches(goav.Branch("voice").Apply(flow).To(target))

	intent := job.Intent()
	if len(intent.Streams) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
	stream := intent.Streams[0]
	if stream.Name != "voice" ||
		!stream.Decode ||
		len(stream.Transforms) != 1 ||
		stream.Transforms[0].Resample.SampleRate != 16_000 ||
		stream.Encode.ID != av.CodecOpus ||
		stream.Encode.Bitrate != 64_000 ||
		len(stream.Destinations) != 1 ||
		stream.Destinations[0] != "voice" {
		t.Fatalf("stream intent: %+v", stream)
	}
	want := []goav.OperationKind{goav.OpDecode, goav.OpTransform, goav.OpEncode}
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
		To(goav.Sink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
			return nil
		})))

	intent := job.Intent()
	if len(intent.Streams) != 1 || len(intent.Destinations) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
	stream := intent.Streams[0]
	if stream.Name != "audio" ||
		!stream.Decode ||
		stream.Encode.ID != "" ||
		len(stream.Transforms) != 0 ||
		len(stream.Destinations) != 1 ||
		stream.Destinations[0] != "frames" {
		t.Fatalf("stream intent: %+v", stream)
	}
	want := []goav.OperationKind{goav.OpDecode, goav.OpTap}
	if !equalOperationKinds(operationSpecKinds(stream.Operations), want) {
		t.Fatalf("operations=%+v, want %+v", stream.Operations, want)
	}
	if len(stream.Taps) != 1 ||
		stream.Taps[0].Name != "audio.flow.decoded" ||
		stream.Taps[0].Domain != goav.DomainFrame ||
		stream.Taps[0].MediaKind != av.MediaAudio ||
		stream.Taps[0].After != goav.OpDecode {
		t.Fatalf("taps: %+v", stream.Taps)
	}
}

func TestFlowDecodeRejectsAfterStreamDecode(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		Apply(goav.Flow("voice").Audio().Decode()).
		To(goav.Sink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
			return nil
		}))).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "flow_decode_domain_mismatch" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want flow_decode_domain_mismatch wrapping ErrUnsupportedBuild", err)
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
			Resample(16_000, goav.Mono).
			Decode()).
		To(goav.Sink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
			return nil
		}))).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "flow_decode_order_invalid" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want flow_decode_order_invalid wrapping ErrUnsupportedBuild", err)
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
					Resample(16_000, goav.Mono).
					Copy()).
				To(goav.File("copy.ogg", io.Discard)),
		},
		{
			name: "after stream decode",
			job: goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
				Audio().
				Decode().
				Apply(goav.Flow("packets").Audio().Copy()).
				To(goav.File("copy.ogg", io.Discard)),
		},
		{
			name: "after branch frame operation",
			job: goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
				Audio().
				Branches(goav.Branch("copy").
					Decode().
					Apply(goav.Flow("packets").Audio().Copy()).
					To(goav.File("copy.ogg", io.Discard))),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.job.Describe()
			var buildErr *goav.BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != "flow_copy_domain_mismatch" || !errors.Is(err, goav.ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want flow_copy_domain_mismatch wrapping ErrUnsupportedBuild", err)
			}
			if !strings.Contains(err.Error(), "requires a packet-domain stream point") ||
				!strings.Contains(err.Error(), ".Copy().Tap") {
				t.Fatalf("err = %v, want flow copy domain guidance", err)
			}
		})
	}
}

func TestNilFlowIsActionable(t *testing.T) {
	var flow goav.Chain
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Apply(flow).
		To(goav.File("voice.ogg", io.Discard)).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "flow_invalid" {
		t.Fatalf("err = %v, want flow_invalid", err)
	}
}

func TestNilFlowBranchIsActionable(t *testing.T) {
	var flow goav.Chain
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Branches(goav.Branch("voice").Apply(flow).To(goav.File("voice.ogg", io.Discard))).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "flow_invalid" {
		t.Fatalf("err = %v, want flow_invalid", err)
	}
}

func TestBranchesRejectOuterOutputsAndDuplicateTargets(t *testing.T) {
	voice := goav.Flow("voice").Audio().Encode(goav.Opus(goav.Bitrate(32_000), goav.Channels(goav.Mono)))
	voiceOut := goav.File("voice.ogg", io.Discard)

	_, err := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Audio().
		Branches(goav.Branch("voice").Apply(voice).To(voiceOut)).
		To(goav.File("ignored.ogg", io.Discard)).
		Describe()
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_scope_mixed" {
		t.Fatalf("err = %v, want output_scope_mixed", err)
	}

	_, err = goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Audio().
		Branches(goav.Branch("voice").Apply(voice).To(voiceOut, voiceOut)).
		Describe()
	if !errors.As(err, &buildErr) || buildErr.Code != "target_duplicate" {
		t.Fatalf("err = %v, want target_duplicate", err)
	}
}

func TestFlowRejectsNonTapOperationsAfterEncode(t *testing.T) {
	tests := []struct {
		name string
		flow goav.Chain
		want string
	}{
		{
			name: "transform",
			flow: goav.Flow("voice").
				Audio().
				Encode(goav.Opus(goav.Bitrate(32_000), goav.Channels(goav.Mono))).
				Resample(16_000, goav.Mono),
			want: "resample",
		},
		{
			name: "stage",
			flow: goav.Flow("voice").
				Audio().
				Encode(goav.Opus(goav.Bitrate(32_000), goav.Channels(goav.Mono))).
				Do(goav.FrameFunc("meter", func(context.Context, *av.Frame, goav.Emit) error {
					return nil
				})),
			want: "custom stage",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
				Audio().
				Apply(tt.flow).
				To(goav.File("voice.ogg", io.Discard)).
				Describe()

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

func TestFlowBranchesDescribeLiveInputBranches(t *testing.T) {
	voice := goav.Flow("voice").Audio().Encode(goav.Opus(goav.Bitrate(32_000), goav.Channels(goav.Mono)))
	archive := goav.Flow("archive").Audio().Encode(goav.Opus(goav.Bitrate(128_000), goav.Channels(goav.Stereo)))
	voiceOut := goav.File("voice.ogg", io.Discard)
	archiveOut := goav.File("archive.ogg", io.Discard)

	job := goav.From(goav.RTP(recipeAPIRTPReader{}).Name("audio").Codec(goav.Opus())).
		Audio().
		Branches(
			goav.Branch("voice").Apply(voice).To(voiceOut),
			goav.Branch("archive").Apply(archive).To(archiveOut),
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
	if len(intent.Streams) != 2 || len(intent.Destinations) != 2 ||
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
		To(goav.Sink(sink)).
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

func TestReadmeDecodeShortcutUsesSinkTarget(t *testing.T) {
	sink := goav.SinkFunc("frames", func(context.Context, goav.Message) error {
		return nil
	})
	job := decodeJob(
		goav.RTP(recipeAPIRTPReader{}).Codec(goav.Opus()),
		goav.Sink(sink),
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
		goav.File("recording.ivf", io.Discard),
	).Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "input_invalid" || !errors.Is(err, webrtcav.ErrNilTrack) {
		t.Fatalf("err = %v, want input_invalid wrapping ErrNilTrack", err)
	}
}

func TestRecipeAndRejectsMultipleFileInputs(t *testing.T) {
	_, err := goav.From(goav.FileInput("a.ivf", strings.NewReader(""))).
		And(goav.FileInput("b.ivf", strings.NewReader(""))).
		To(goav.File("out.ivf", io.Discard)).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "multi_input_unsupported" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want multi_input_unsupported wrapping ErrUnsupportedBuild", err)
	}
}

func TestRecipeAndRejectsDuplicateRealtimeInputNames(t *testing.T) {
	_, err := goav.From(goav.RTP(recipeAPIRTPReader{}).Name("media").Codec(goav.Opus())).
		And(goav.RTP(recipeAPIRTPReader{}).Name("media").Codec(goav.VP8())).
		To(goav.File("recording.webm", io.Discard)).
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
		goav.File("recording.ogg", io.Discard),
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

func TestDecodeRecipeRejectsNilSinkTarget(t *testing.T) {
	_, err := decodeJob(
		goav.FileInput("input.ogg", strings.NewReader("")),
		goav.Sink(nil),
	).Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_invalid" || !errors.Is(err, goav.ErrNilSink) {
		t.Fatalf("err = %v, want output_invalid wrapping ErrNilSink", err)
	}
	if !strings.Contains(err.Error(), "non-nil sink") {
		t.Fatalf("err = %v, want sink target guidance", err)
	}
}

func TestDecodeRecipeRejectsNilSinkFuncCallback(t *testing.T) {
	_, err := decodeJob(
		goav.FileInput("input.ogg", strings.NewReader("")),
		goav.Sink(goav.SinkFunc("frames", nil)),
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
		goav.File("frames.ogg", io.Discard),
	).Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_missing" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want encode_missing wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "goav.Sink") ||
		!strings.Contains(err.Error(), ".Copy().To(output)") {
		t.Fatalf("err = %v, want decode output guidance", err)
	}
}

func TestPacketCopyRecipeAcceptsSinkTarget(t *testing.T) {
	spec, err := goav.From(goav.RTP(recipeAPIRTPReader{}).Name("audio").Codec(goav.Opus())).
		Copy().
		To(goav.Sink(goav.SinkFunc("packets", func(context.Context, goav.Message) error {
			return nil
		}))).
		Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "audio -> packets") {
		t.Fatalf("spec:\n%s", text)
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
	if !errors.As(err, &buildErr) || buildErr.Code != "target_invalid" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want target_invalid wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "destination is empty") ||
		!strings.Contains(err.Error(), "goav.File") ||
		!strings.Contains(err.Error(), "goav.Sink") {
		t.Fatalf("err = %v, want destination constructor guidance", err)
	}
}

func TestRecordRecipeRejectsFileWithoutWriter(t *testing.T) {
	_, err := recordJob(
		goav.FileInput("input.ogg", strings.NewReader("")),
		goav.File("recording.ogg", nil),
	).Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_writer_missing" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_writer_missing wrapping ErrUnsupportedBuild", err)
	}
}

func TestRecordRecipeRejectsUnnamedFileWithoutFormat(t *testing.T) {
	_, err := recordJob(
		goav.FileInput("input.ivf", strings.NewReader("")),
		goav.File("", io.Discard),
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

func TestRecordRecipeRejectsFormatOnlyDestination(t *testing.T) {
	_, err := recordJob(
		goav.FileInput("input.ivf", strings.NewReader("")),
		goav.URIOut("", goav.Format(av.FormatIVF)),
	).Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_target_missing" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_target_missing wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "no URI, writer, or sink") ||
		!strings.Contains(err.Error(), "goav.File") {
		t.Fatalf("err = %v, want output target guidance", err)
	}
}

func TestRecordRecipeReportsMissingInputDemuxer(t *testing.T) {
	_, err := recordJob(
		goav.FileInput("input.ogg", strings.NewReader("")),
		goav.File("recording.ivf", io.Discard),
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

func TestRecordRecipeReportsMissingTargetMuxer(t *testing.T) {
	_, err := recordJob(
		goav.FileInput("input.ivf", bytes.NewReader(tinyIVF())),
		goav.File("recording.mp4", io.Discard),
	).Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "target_muxer_missing" {
		t.Fatalf("err = %v, want target_muxer_missing", err)
	}
	if buildErr.Operation != "open target" {
		t.Fatalf("operation = %q, want open target", buildErr.Operation)
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
			goav.File("recording.ivf", io.Discard),
			goav.File("recording.ivf", io.Discard),
		).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_duplicate" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_duplicate wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), `output name "recording.ivf"`) ||
		!strings.Contains(err.Error(), "unique output name") {
		t.Fatalf("err = %v, want duplicate output guidance", err)
	}
}

func TestStreamRecipeRejectsDuplicateSinkTargets(t *testing.T) {
	sink := func(context.Context, goav.Message) error { return nil }
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		To(
			goav.Sink(goav.SinkFunc("frames", sink)),
			goav.Sink(goav.SinkFunc("frames", sink)),
		).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_duplicate" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_duplicate wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), `output name "frames"`) ||
		!strings.Contains(err.Error(), ".Name") {
		t.Fatalf("err = %v, want duplicate sink guidance", err)
	}
}

func TestStreamRecipeRejectsDuplicateTypedTargets(t *testing.T) {
	target := goav.File("voice.ogg", io.Discard)
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(goav.New()).
		Audio().
		Encode(goav.Opus(goav.Bitrate(96_000))).
		To(target, target).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_duplicate" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_duplicate wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), `output name "voice.ogg"`) {
		t.Fatalf("err = %v, want duplicate destination guidance", err)
	}
}

func TestRTPRecipeRejectsNilReader(t *testing.T) {
	_, err := recordJob(
		goav.RTP(nil).Name("audio"),
		goav.File("recording.ogg", io.Discard),
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
		goav.File("recording.ogg", io.Discard),
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
				goav.File("recording.ogg", io.Discard),
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
				goav.File("recording.ogg", io.Discard),
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
		goav.File("recording.ogg", io.Discard),
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
				goav.File("recording.ogg", io.Discard),
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
		Encode(goav.Opus(goav.Bitrate(96_000))).
		To(goav.File("archive.ogg", io.Discard))

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
		Encode(goav.Opus(goav.Bitrate(48_000))).
		To(goav.File("preview.ogg", io.Discard))

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
		Encode(goav.VP9(goav.Bitrate(2_000_000))).
		To(goav.File("preview.webm", io.Discard))

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
			name: "sink target",
			job: goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
				Audio().
				To(goav.Sink(sink)),
		},
		{
			name: "custom stage",
			job: goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
				Audio().
				Do(meter).
				Encode(goav.Opus(goav.Bitrate(96_000))).
				To(goav.File("archive.ogg", io.Discard)),
		},
		{
			name: "resample",
			job: goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
				Audio().
				Resample(16_000, goav.Mono).
				Encode(goav.Opus(goav.Bitrate(48_000))).
				To(goav.File("preview.ogg", io.Discard)),
		},
		{
			name: "resize",
			job: goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
				Video().
				Resize(1280, 720).
				Encode(goav.VP9(goav.Bitrate(2_000_000))).
				To(goav.File("preview.webm", io.Discard)),
		},
		{
			name: "encoder",
			job: goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
				Audio().
				Encode(goav.Opus(goav.Bitrate(96_000))).
				To(goav.File("archive.ogg", io.Discard)),
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
		To(goav.File("archive.ogg", io.Discard)).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_operation_missing" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want stream_operation_missing wrapping ErrUnsupportedBuild", err)
	}
}

func TestStreamRecipeRejectsGenericAndStreamOutputs(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		To(goav.File("archive.ogg", io.Discard)).
		Audio().
		Encode(goav.Opus(goav.Bitrate(96_000))).
		To(goav.File("preview.ogg", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_scope_mixed" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_scope_mixed wrapping ErrUnsupportedBuild", err)
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
	job.To(goav.Sink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
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
		To(goav.Sink(goav.SinkFunc("audio", func(context.Context, goav.Message) error {
			return nil
		}))).
		Video().
		To(goav.Sink(goav.SinkFunc("video", func(context.Context, goav.Message) error {
			return nil
		}))).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_duplicate" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want stream_duplicate wrapping ErrUnsupportedBuild", err)
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
		To(goav.Sink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
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
		To(goav.Sink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
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
		To(goav.Sink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
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
		To(goav.Sink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
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

func TestStreamRecipeRequiresEncoderForFile(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Resample(48_000, goav.Stereo).
		To(goav.File("archive.ogg", io.Discard)).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_missing" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want encode_missing wrapping ErrUnsupportedBuild", err)
	}
}

func TestStreamRecipeRejectsMixedSinkAndFile(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		To(
			goav.Sink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
				return nil
			})),
			goav.File("archive.ogg", io.Discard),
		).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_kind_mixed" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_kind_mixed wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "cannot mix sinks and muxed outputs") ||
		!strings.Contains(err.Error(), ".Branches") {
		t.Fatalf("err = %v, want mixed output guidance", err)
	}
}

func TestStreamRecipeAllowsEncodedMuxAndSinkTargets(t *testing.T) {
	rt := goav.New(
		goav.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{
				{Index: 0, ID: "audio", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}},
			}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
			registry.RegisterMuxer(av.FormatOgg, recipeAPIMuxerFactory{})
		}),
		goav.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, recipeAPIEncoderFactory{})
		}),
	)

	spec, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(rt).
		Audio().
		Encode(goav.Opus(goav.Bitrate(96_000))).
		To(
			goav.File("archive.ogg", io.Discard),
			goav.Sink(goav.SinkFunc("packets", func(context.Context, goav.Message) error {
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
		Encode(goav.Opus(goav.Bitrate(96_000))).
		Resample(16_000, goav.Mono).
		To(goav.File("archive.ogg", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_step_after_encode" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want stream_step_after_encode wrapping ErrUnsupportedBuild", err)
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
		Encode(goav.Opus(goav.Bitrate(96_000))).
		Encode(goav.VP9(goav.Bitrate(600_000))).
		To(goav.File("archive.ogg", io.Discard)).
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
				To(goav.File(tt.output, io.Discard)).
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
		To(goav.File("archive.ogg", io.Discard)).
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
		Encode(goav.Opus(goav.Bitrate(-1))).
		To(goav.File("archive.ogg", io.Discard)).
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
				Encode(goav.VP9(goav.FPS(0))).
				To(goav.File("preview.webm", io.Discard)),
			want: "encode FPS must be positive",
		},
		{
			name: "keyframe interval",
			job: goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
				Video().
				Encode(goav.VP9(goav.KeyframeInterval(-1))).
				To(goav.File("preview.webm", io.Discard)),
			want: "encode keyframe interval must be non-negative",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.job.Build(context.Background())
			var buildErr *goav.BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != "encode_parameter_invalid" || !errors.Is(err, goav.ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want encode_parameter_invalid wrapping ErrUnsupportedBuild", err)
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
		Encode(goav.Opus(goav.SampleRate(0))).
		To(goav.File("archive.ogg", io.Discard)).
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
		UseRuntime(goav.New(goav.WithStdFormats())).
		Audio().
		Encode(goav.Opus(goav.Bitrate(96_000))).
		To(goav.File("archive.ivf", io.Discard)).
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
		To(goav.Sink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
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
		To(goav.Sink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
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
		To(goav.Sink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
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
		To(goav.Sink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
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
		To(goav.Sink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
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

func TestStreamRecipeReportsIncompatibleTransformAdapterBeforeOpeningInput(t *testing.T) {
	streams := []av.Stream{{
		Index: 0,
		ID:    "audio",
		Type:  av.MediaAudio,
		Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio},
	}}
	demuxerOpened := false
	rt := goav.New(
		goav.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: streams})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{called: &demuxerOpened})
		}),
		goav.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, recipeAPIDecoderFactory{})
		}),
		goav.WithFilterAdapter(func(registry *filter.SimpleRegistry) {
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
		Resample(16_000, goav.Mono).
		To(goav.Sink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
			return nil
		}))).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "transform_adapter_incompatible" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want transform_adapter_incompatible wrapping ErrUnsupportedBuild", err)
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
				To(goav.File("archive.ogg", io.Discard)).
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
		goav.File("preview.ivf", &out),
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
		goav.File("preview.ivf", &out),
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
			goav.File("recording.ivf", &recording),
			goav.File("preview.ivf", &preview),
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
			goav.File("recording.ivf", &recording),
			goav.File("preview.ivf", &preview),
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
		goav.File("", &out, goav.Format(av.FormatIVF)),
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
	web := goav.File("web.webm", io.Discard)
	preview := goav.File("preview.webm", io.Discard)
	job := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("720p").Resize(1280, 720).Encode(goav.VP9(goav.Bitrate(2_000_000))).To(web).
		Video("360p").Resize(640, 360).Encode(goav.VP9(goav.Bitrate(600_000))).To(preview)

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

func TestBranchRecipeComposesAudioAndVideoIntoSharedOutput(t *testing.T) {
	web := goav.File("out.webm", io.Discard)
	job := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("v360").Resize(640, 360).Encode(goav.VP9(goav.Bitrate(600_000))).To(web).
		Audio("a96").Resample(48_000, goav.Stereo).Encode(goav.Opus(goav.Bitrate(96_000))).To(web)

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
		len(intent.Streams[0].Destinations) != 1 || intent.Streams[0].Destinations[0] != "out.webm" ||
		len(intent.Streams[1].Destinations) != 1 || intent.Streams[1].Destinations[0] != "out.webm" ||
		len(intent.Destinations) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestBranchRecipeSingleBranchUsesTarget(t *testing.T) {
	preview := goav.File("preview.webm", io.Discard)
	job := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("360p").Resize(640, 360).Encode(goav.VP9(goav.Bitrate(600_000))).
		To(preview)

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "encode-360p -> preview.webm") {
		t.Fatalf("spec:\n%s", text)
	}
	intent := job.Intent()
	if len(intent.Streams) != 1 || len(intent.Streams[0].Destinations) != 1 || len(intent.Destinations) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestBranchRecipeRejectsDuplicateDestinations(t *testing.T) {
	web := goav.File("web.webm", io.Discard)
	web2 := goav.File("web.webm", io.Discard)
	_, err := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("720p").Encode(goav.VP9(goav.Bitrate(2_000_000))).To(web).
		Video("360p").Encode(goav.VP9(goav.Bitrate(600_000))).To(web2).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "target_duplicate" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want target_duplicate wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), `destination "web.webm"`) ||
		!strings.Contains(err.Error(), "reuse the same destination value") {
		t.Fatalf("err = %v, want duplicate destination guidance", err)
	}
}

func TestBranchRecipeRejectsDuplicateBranchDestinations(t *testing.T) {
	web := goav.File("web.webm", io.Discard)
	_, err := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("720p").Encode(goav.VP9(goav.Bitrate(2_000_000))).To(web, web).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "target_duplicate" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want target_duplicate wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), `branch routes to destination "web.webm" more than once`) ||
		!strings.Contains(err.Error(), "second destination index: 1") ||
		!strings.Contains(err.Error(), "list each destination once") {
		t.Fatalf("err = %v, want duplicate branch destination guidance", err)
	}
}

func TestBranchRecipeRejectsDuplicateBranchNames(t *testing.T) {
	archive := goav.File("archive.webm", io.Discard)
	preview := goav.File("preview.webm", io.Discard)
	_, err := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("720p").Encode(goav.VP9(goav.Bitrate(2_000_000))).To(archive).
		Video("720p").Encode(goav.VP9(goav.Bitrate(1_000_000))).To(preview).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_duplicate" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want stream_duplicate wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), `branch name "720p"`) ||
		!strings.Contains(err.Error(), "first branch index: 0") ||
		!strings.Contains(err.Error(), `.Video("360p")`) {
		t.Fatalf("err = %v, want duplicate branch guidance", err)
	}
}

func TestBranchRecipeRejectsMissingBranchName(t *testing.T) {
	web := goav.File("web.webm", io.Discard)
	_, err := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("").Encode(goav.VP9(goav.Bitrate(2_000_000))).To(web).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_name_missing" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want stream_name_missing wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "branches need stable names") ||
		!strings.Contains(err.Error(), `.Video("720p")`) ||
		!strings.Contains(err.Error(), "media type: video") {
		t.Fatalf("err = %v, want branch name guidance", err)
	}
}

func TestBranchRecipeRejectsInvalidDestination(t *testing.T) {
	_, err := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("360p").Encode(goav.VP9(goav.Bitrate(600_000))).
		To(goav.File("preview.webm", nil)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_writer_missing" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_writer_missing wrapping ErrUnsupportedBuild", err)
	}
}

func TestBranchCompositionAcceptsRTPInputThenReportsMissingMuxer(t *testing.T) {
	_, err := branchJob(goav.RTP(recipeAPIRTPReader{}).Name("audio").Codec(goav.Opus())).
		Audio("main").Encode(goav.Opus(goav.Bitrate(96_000))).To(goav.File("archive.ogg", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "target_muxer_missing" {
		t.Fatalf("err = %v, want target_muxer_missing", err)
	}
	if buildErr.Node != "archive.ogg" {
		t.Fatalf("node = %q, want archive.ogg destination", buildErr.Node)
	}
	if !strings.Contains(err.Error(), "ogg muxer") {
		t.Fatalf("err = %v, want Ogg adapter guidance", err)
	}
}

func TestBranchCompositionAcceptsSinkTarget(t *testing.T) {
	job := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Decode().
		Branches(
			goav.Branch("preview").
				To(goav.Sink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
					return nil
				}))),
		)

	intent := job.Intent()
	if len(intent.Streams) != 1 ||
		intent.Streams[0].Name != "preview" ||
		intent.Streams[0].Encode.ID != "" ||
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
				To(goav.Sink(goav.SinkFunc("thumbnail", func(context.Context, goav.Message) error {
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
	web := goav.File("web.webm", io.Discard)
	thumbnail := goav.Sink(goav.SinkFunc("thumbnail", func(context.Context, goav.Message) error {
		return nil
	}))
	job := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Decode().
		Resize(1280, 720).
		Tap(goav.FrameTap("video.720p.frames")).
		Branches(
			goav.Branch("web").
				Encode(goav.VP9(goav.Bitrate(2_000_000))).
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
	rt := goav.New(
		goav.WithFormatAdapter(func(registry *format.SimpleRegistry) {
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
		goav.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP9, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
		}),
		goav.WithFilterAdapter(func(registry *filter.SimpleRegistry) {
			registry.RegisterFactory(filter.Descriptor{Name: filter.FactoryResize, Input: av.MediaVideo, Output: av.MediaVideo}, recipeAPIFilterFactory{})
		}),
	)

	web := goav.File("web.ogg", io.Discard)
	thumbnail := goav.Sink(goav.SinkFunc("thumbnail", func(context.Context, goav.Message) error {
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
				Encode(goav.VP9(goav.Bitrate(2_000_000))).
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
	if countOperationReports(webBranch.Operations, goav.OpDecode, true) != 1 ||
		countOperationReports(webBranch.Operations, goav.OpTransform, true) != 1 ||
		countOperationReports(webBranch.Operations, goav.OpTap, true) != 1 ||
		countOperationReports(webBranch.Operations, goav.OpEncode, false) != 1 {
		t.Fatalf("web operations=%+v, want shared decode/resize/tap and private encode", webBranch.Operations)
	}

	thumbBranch, ok := branchByName(report.Branches, "thumb")
	if !ok {
		t.Fatalf("branches=%+v, want thumb", report.Branches)
	}
	if countOperationReports(thumbBranch.Operations, goav.OpDecode, true) != 1 ||
		countOperationReports(thumbBranch.Operations, goav.OpTransform, true) != 1 ||
		countOperationReports(thumbBranch.Operations, goav.OpTap, true) != 1 ||
		countOperationReports(thumbBranch.Operations, goav.OpTransform, false) != 1 {
		t.Fatalf("thumb operations=%+v, want shared parent work and private thumbnail resize", thumbBranch.Operations)
	}
}

func TestBranchCompositionCanSplitFromEarlierTap(t *testing.T) {
	web := goav.File("web.webm", io.Discard)
	thumbnail := goav.Sink(goav.SinkFunc("thumbnail", func(context.Context, goav.Message) error {
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
				Encode(goav.VP9(goav.Bitrate(2_000_000))).
				To(web),
		)

	intent := job.Intent()
	if len(intent.Streams) != 2 {
		t.Fatalf("intent streams = %+v, want 2", intent.Streams)
	}
	if intent.Streams[0].From.Name() != "video.decoded" ||
		intent.Streams[0].From.Domain() != goav.DomainFrame ||
		len(intent.Streams[0].Transforms) != 1 ||
		intent.Streams[0].Transforms[0].Resize.Width != 320 {
		t.Fatalf("raw branch intent = %+v, want branch from decoded tap with only thumbnail resize", intent.Streams[0])
	}
	if intent.Streams[1].From.Name() != "video.720p.frames" ||
		intent.Streams[1].From.Domain() != goav.DomainFrame ||
		len(intent.Streams[1].Transforms) != 1 ||
		intent.Streams[1].Transforms[0].Resize.Width != 1280 {
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
				To(goav.Sink(goav.SinkFunc("preview", func(context.Context, goav.Message) error {
					return nil
				}))),
		).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "branch_tap_missing" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want branch_tap_missing wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), `Tap(goav.FrameTap("video.missing"))`) ||
		!strings.Contains(err.Error(), "current stream point") {
		t.Fatalf("err = %v, want planned tap guidance", err)
	}
}

func TestBranchCompositionRejectsGraphNodeSource(t *testing.T) {
	graphNode := goav.Expert(goav.Default()).Graph().Stage("decode-video", goav.PacketFunc("decode-video", func(ctx context.Context, packet *goav.Packet, emit goav.Emit) error {
		return emit.Packet(packet)
	}))
	_, err := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Decode().
		Branches(
			goav.Branch("preview").
				From(graphNode).
				Resize(320, 180).
				To(goav.Sink(goav.SinkFunc("preview", func(context.Context, goav.Message) error {
					return nil
				}))),
		).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "branch_source_invalid" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want branch_source_invalid wrapping ErrUnsupportedBuild", err)
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
		Encode(goav.VP9(goav.Bitrate(2_000_000))).
		Tap(goav.PacketTap("video.encoded")).
		Branches(
			goav.Branch("packets").
				To(goav.Sink(goav.SinkFunc("packets", func(context.Context, goav.Message) error {
					return nil
				}))),
		).
		Describe()

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_branch_source_invalid" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want encode_branch_source_invalid wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "stream encoders are terminal") ||
		!strings.Contains(err.Error(), "encoder: vp9") ||
		!strings.Contains(err.Error(), "Task.Attach") {
		t.Fatalf("err = %v, want planned parent encode guidance", err)
	}
}

func TestBranchCompositionSharesCurrentPointWithoutExplicitTap(t *testing.T) {
	web := goav.File("web.webm", io.Discard)
	thumbnail := goav.Sink(goav.SinkFunc("thumbnail", func(context.Context, goav.Message) error {
		return nil
	}))
	job := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Decode().
		Resize(1280, 720).
		Branches(
			goav.Branch("web").
				Encode(goav.VP9(goav.Bitrate(2_000_000))).
				To(web),
			goav.Branch("thumb").
				Resize(320, 180).
				To(thumbnail),
		)

	intent := job.Intent()
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
	meter := goav.FrameFunc("meter", func(ctx context.Context, frame *goav.Frame, emit goav.Emit) error {
		return emit.Frame(frame)
	})
	packets := goav.Sink(goav.SinkFunc("packets", func(context.Context, goav.Message) error {
		return nil
	}))
	preview := goav.Sink(goav.SinkFunc("preview", func(context.Context, goav.Message) error {
		return nil
	}))
	job := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Decode().
		Do(meter).
		Branches(
			goav.Branch("web").
				Encode(goav.VP9(goav.Bitrate(2_000_000))).
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
	archive := goav.File("archive.ivf", io.Discard)
	packets := goav.Sink(goav.SinkFunc("packets", func(context.Context, goav.Message) error {
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

	intent := job.Intent()
	if len(intent.Streams) != 2 ||
		intent.Streams[0].From.Name() != "video.packets" ||
		intent.Streams[0].From.Domain() != goav.DomainPacket ||
		intent.Streams[1].From.Name() != "video.packets" ||
		intent.Streams[1].From.Domain() != goav.DomainPacket {
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
				To(goav.Sink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
					return nil
				}))),
		).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "branch_decode_order_invalid" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want branch_decode_order_invalid wrapping ErrUnsupportedBuild", err)
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
				To(goav.Sink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
					return nil
				}))),
		).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "branch_decode_copy_invalid" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want branch_decode_copy_invalid wrapping ErrUnsupportedBuild", err)
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
				To(goav.Sink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
					return nil
				}))),
		).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "branch_decode_domain_mismatch" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want branch_decode_domain_mismatch wrapping ErrUnsupportedBuild", err)
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
	if !errors.As(err, &buildErr) || buildErr.Code != "output_missing" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_missing wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "no output is configured") {
		t.Fatalf("err = %v, want output guidance", err)
	}
}

func TestBranchRecipeRequiresBranchTarget(t *testing.T) {
	job := branchJob(goav.FileInput("input.webm", strings.NewReader("")))
	job.Video("360p").Encode(goav.VP9(goav.Bitrate(600_000)))
	_, err := job.Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "target_missing" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want target_missing wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "branch has no destination") ||
		!strings.Contains(err.Error(), "goav.File") {
		t.Fatalf("err = %v, want destination guidance", err)
	}
}

func TestBranchRecipeRejectsNegativeStreamIndex(t *testing.T) {
	_, err := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		materialize().
		Audio(goav.StreamIndex(-1)).
		Decode().
		Tap(goav.FrameTap("audio.decoded")).
		Branches(goav.Branch("bad").Encode(goav.Opus(goav.Bitrate(64_000))).To(goav.File("bad.ogg", io.Discard))).
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

func TestBranchRecipeRejectsWrongMediaTransform(t *testing.T) {
	_, err := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("bad").Resample(16_000, goav.Mono).Encode(goav.VP9(goav.Bitrate(600_000))).
		To(goav.File("bad.webm", io.Discard)).
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

func TestBranchRecipeRejectsInvalidResample(t *testing.T) {
	_, err := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Audio("bad").Resample(0, goav.Mono).Encode(goav.Opus(goav.Bitrate(64_000))).
		To(goav.File("bad.ogg", io.Discard)).
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

func TestBranchRecipeRejectsProcessingAfterEncoder(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Decode().
		Tap(goav.FrameTap("video.decoded")).
		Branches(
			goav.Branch("360p").
				Encode(goav.VP9(goav.Bitrate(600_000))).
				Resize(640, 360).
				To(goav.File("preview.webm", io.Discard)),
		).
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

func TestBranchRecipeRejectsDuplicateEncoder(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Decode().
		Tap(goav.FrameTap("video.decoded")).
		Branches(
			goav.Branch("360p").
				Encode(goav.VP9(goav.Bitrate(600_000))).
				Encode(goav.VP8(goav.Bitrate(400_000))).
				To(goav.File("preview.webm", io.Discard)),
		).
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

func TestBranchRecipeRejectsNegativeEncodeBitrate(t *testing.T) {
	_, err := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("bad").Encode(goav.VP9(goav.Bitrate(-1))).
		To(goav.File("bad.webm", io.Discard)).
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

func TestBranchRecipeDescribesTransformChain(t *testing.T) {
	spec, err := branchJob(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("360p").Resize(1280, 720).Resize(640, 360).Encode(goav.VP9(goav.Bitrate(600_000))).
		To(goav.File("preview.webm", io.Discard)).
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
