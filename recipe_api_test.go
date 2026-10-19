package goav_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"reflect"
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
	"github.com/thesyncim/goav/graphrender"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/provider"
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
	if _, ok := runtimeType.MethodByName("ExpertGraph"); ok {
		t.Fatal("Runtime exposes the ExpertGraph seam; expert reaches the runtime through internal/expertgraph")
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
