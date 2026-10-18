package goav

import (
	"context"
	"io"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
)

func testRecipeIRDestination(name string, kind recipeir.DestinationKind) recipeir.Destination {
	return recipeir.Destination{Name: name, Kind: kind}
}

type graphPlanTestLowerer struct {
	runtime *Runtime
	called  bool
}

func (l *graphPlanTestLowerer) spec() (pipeline.Spec, error) {
	return pipeline.Spec{Name: "goav"}, nil
}

func (l *graphPlanTestLowerer) runtimeRef() *Runtime {
	return l.runtime
}

func (l *graphPlanTestLowerer) lower(context.Context, graphPlan, pipeline.Graph, *builder) error {
	l.called = true
	return nil
}

func workPlanOperationsForBranch(operations []workOperation, branch string) []workOperation {
	out := make([]workOperation, 0)
	for i := range operations {
		if operations[i].Branch == branch {
			out = append(out, operations[i])
		}
	}
	return out
}

func workPlanOperationKindsForBranch(operations []workOperation, branch string) []plan.OperationKind {
	operations = workPlanOperationsForBranch(operations, branch)
	out := make([]plan.OperationKind, 0, len(operations))
	for i := range operations {
		out = append(out, operations[i].Kind)
	}
	return out
}

func TestResolvedJobOutputFormatsEnterMediaPlanBuild(t *testing.T) {
	runtime := mustNew(append(
		testBundleOptions(),
		withTestFormats(
			testFormatProber(remuxTestProber{}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
	)...)
	job := recipeFrom(
		Input(&liveTestProvider{
			name:    "audio",
			media:   av.MediaAudio,
			codecID: av.CodecOpus,
			streams: []av.Stream{{
				ID:   "audio",
				Type: av.MediaAudio,
				Codec: av.CodecParameters{
					ID:   av.CodecOpus,
					Type: av.MediaAudio,
				},
			}},
		}),
	).Copy().To(destinationHandle(fileDestination("recording.ogg", io.Discard))).UseRuntime(runtime)

	resolved, err := compileJobRecipeForBuild(job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuild() error = %v", err)
	}
	if len(resolved.outputAttachments) != 1 {
		t.Fatalf("resolved output attachments = %d, want 1", len(resolved.outputAttachments))
	}
	if got := destinationOpenFormat(resolved.outputAttachments[0]); got != av.FormatOgg {
		t.Fatalf("open output format = %q, want resolved Ogg format", got)
	}
	if got := destinationGraphFormat(resolved.outputAttachments[0]); got != "" {
		t.Fatalf("graph detail output format = %q, want inferred format hidden from graph detail", got)
	}
	task, err := resolved.Build(context.Background())
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	spec := task.Describe()
	if len(spec.Nodes) != 2 || spec.Nodes[1].Name != "recording.ogg" || spec.Nodes[1].Detail != "mux, protocol=file" {
		t.Fatalf("built spec = %+v, want inferred format hidden from mux detail", spec)
	}
}

func TestGraphPlanCarriesReportMetadata(t *testing.T) {
	web := destinationHandle(fileDestination("web.ivf", io.Discard).withFormat(av.FormatIVF))
	job := recipeFrom(FileInput("input.ivf", strings.NewReader(""))).
		Video().
		Decode().
		Tap(frameTapRef("video.decoded")).
		Branches(
			newBranch("360p").
				Resize(640, 360).
				Encode(codec.VP9(codec.Bitrate(600_000))).
				To(web),
		)

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	work := resolved.graphPlan.workPlan()
	if len(resolved.graphPlan.nodes) == 0 || len(resolved.graphPlan.edges) == 0 {
		t.Fatalf("graphPlan nodes=%+v edges=%+v, want planned operation graph", resolved.graphPlan.nodes, resolved.graphPlan.edges)
	}
	if len(work.Branches) != 1 || work.Branches[0].Name != "360p" {
		t.Fatalf("graphPlan work plan branches = %+v, want 360p branch", work.Branches)
	}
	if len(work.Taps) != 1 || work.Taps[0].Name != "video.decoded" {
		t.Fatalf("graphPlan work plan taps = %+v, want video.decoded tap", work.Taps)
	}
	if len(work.Destinations) != 1 || work.Destinations[0].Name != "web.ivf" || !reflect.DeepEqual(work.Destinations[0].Branches, []string{"360p"}) {
		t.Fatalf("graphPlan work plan destinations = %+v, want web.ivf owned by 360p", work.Destinations)
	}
	operations := work.Operations
	for _, want := range []plan.OperationKind{plan.OpDemux, plan.OpSelect, plan.OpDecode, plan.OpTap, plan.OpTransform, plan.OpEncode, plan.OpMux} {
		if !graphPlanOperationKindPresent(operations, want) {
			t.Fatalf("graphPlan operations = %+v, want %s", operations, want)
		}
	}
	if !graphPlanOperationNodePresent(operations, pipeline.NodeRef("resize-360p")) ||
		!graphPlanOperationNodePresent(operations, pipeline.NodeRef("encode-360p")) ||
		!graphPlanOperationTargetPresent(operations, "web.ivf") {
		t.Fatalf("graphPlan operations = %+v, want resize, encode, and web.ivf destination operations", operations)
	}
	report, err := newPlanReport("build job", resolved)
	if err != nil {
		t.Fatalf("newPlanReport() error = %v", err)
	}
	if len(report.Branches) != 1 || report.Branches[0].Name != "360p" {
		t.Fatalf("report branches = %+v, want graph-plan branch metadata", report.Branches)
	}
	if len(report.Taps) != 1 || report.Taps[0].Name != "video.decoded" {
		t.Fatalf("report taps = %+v, want graph-plan tap metadata", report.Taps)
	}
}

func TestGraphPlanViewsAreImmutable(t *testing.T) {
	job := recipeFrom(
		FileInput("input.ivf", strings.NewReader("")),
	).Copy().To(destinationHandle(fileDestination("recording.ivf", io.Discard).withFormat(av.FormatIVF)))

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	spec := resolved.graphPlan.spec()
	spec.Nodes[0].Name = "mutated"
	work := resolved.graphPlan.workPlan()
	work.Branches[0].Operations[0] = "mutated"
	work.Destinations[0].Branches[0] = "mutated"
	work.Operations[len(work.Operations)-1].Destinations[0] = "mutated"

	nextSpec := resolved.graphPlan.spec()
	if nextSpec.Nodes[0].Name == "mutated" {
		t.Fatal("graphPlan.spec() returned aliased nodes")
	}
	nextWork := resolved.graphPlan.workPlan()
	if nextWork.Branches[0].Operations[0] == "mutated" {
		t.Fatal("graphPlan.workPlan() returned aliased branch operations")
	}
	if nextWork.Destinations[0].Branches[0] == "mutated" {
		t.Fatal("graphPlan.workPlan() returned aliased destination branch refs")
	}
	nextOperations := resolved.graphPlan.workPlan().Operations
	if nextOperations[len(nextOperations)-1].Destinations[0] == "mutated" {
		t.Fatal("graphPlan.workPlan() returned aliased operation destination refs")
	}
}

func graphPlanOperationKindPresent(operations []workOperation, kind plan.OperationKind) bool {
	for i := range operations {
		if operations[i].Kind == kind {
			return true
		}
	}
	return false
}

func graphPlanOperationNodePresent(operations []workOperation, node pipeline.NodeRef) bool {
	for i := range operations {
		if operations[i].Node == node {
			return true
		}
	}
	return false
}

func graphPlanOperationTargetPresent(operations []workOperation, target string) bool {
	id := workDestinationID(target)
	for i := range operations {
		if slices.Contains(operations[i].Destinations, id) {
			return true
		}
	}
	return false
}

func TestCompileLiveFlowBranchesRecipeUsesMediaPlanBranchComposer(t *testing.T) {
	voice := destinationHandle(fileDestination("voice.ogg", io.Discard).withFormat(av.FormatOgg))
	archive := destinationHandle(fileDestination("archive.ogg", io.Discard).withFormat(av.FormatOgg))
	job := recipeFrom(Input(liveAudioOpusProvider("audio"))).
		Audio().
		Branches(
			newBranch("voice").Apply(newFlow("voice").Audio().Decode().Encode(codec.Opus(codec.Bitrate(32_000), codec.Channels(codec.Mono)))).To(voice),
			newBranch("archive").Apply(newFlow("archive").Audio().Decode().Encode(codec.Opus(codec.Bitrate(128_000), codec.Channels(codec.Stereo)))).To(archive),
		)

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	if len(resolved.inputAttachments) != 1 || resolved.inputAttachments[0].provider == nil {
		t.Fatal("resolved inputs = nil provider, want the live input carried on the resolved plan")
	}
	if len(resolved.recipe.Streams) != 2 {
		t.Fatalf("resolved streams = %+v, want two live flow branches", resolved.recipe.Streams)
	}
	spec, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	if !specHasNode(spec, "encode-voice") || !specHasNode(spec, "encode-archive") {
		t.Fatalf("spec = %+v, want flow branch encoders", spec)
	}
}

func TestRecipeResolvedBuildUsesMediaPlanBranchComposer(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
		withTestCodecs(
			testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
			testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		),
	)
	job := recipeFrom(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		Tap(frameTapRef("audio.decoded")).
		Branches(newBranch("main").Encode(codec.Opus(codec.Bitrate(96_000))).To(destinationHandle(fileDestination("archive.ogg", io.Discard))))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
}

func TestBranchComposeGraphPlanOperationsUseSharedNodeRefs(t *testing.T) {
	web := destinationHandle(fileDestination("web.ivf", io.Discard))
	thumbnail := Sink(SinkFunc("thumbnail", func(context.Context, Message) error {
		return nil
	}))
	job := recipeFrom(FileInput("input.ivf", strings.NewReader(""))).
		Video().
		Decode().
		Resize(1280, 720).
		Tap(frameTapRef("video.720p.frames")).
		Branches(
			newBranch("web").Encode(codec.VP9(codec.Bitrate(2_000_000))).To(web),
			newBranch("thumb").Resize(320, 180).To(thumbnail),
		)

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	operations := resolved.graphPlan.workPlan().Operations
	for _, want := range []pipeline.NodeRef{"select-video", "decode-video", "resize-video", "encode-web", "resize-thumb"} {
		if !graphPlanOperationNodePresent(operations, want) {
			t.Fatalf("graphPlan operations = %+v, want node %s", operations, want)
		}
	}
	for _, duplicate := range []pipeline.NodeRef{"decode-web", "decode-thumb", "resize-web"} {
		if graphPlanOperationNodePresent(operations, duplicate) {
			t.Fatalf("graphPlan operations = %+v, want shared node instead of %s", operations, duplicate)
		}
	}
}

func TestRecipeResolvedBuildUsesMediaPlanPacketCopy(t *testing.T) {
	job := recipeFrom(
		Input(liveVideoVP8Provider("video")),
	).Copy().To(destinationHandle(fileDestination("recording.ivf", io.Discard))).UseRuntime(testBundleRuntime())

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(context.Background())
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	built := task.Describe()
	if len(planned.Nodes) != len(built.Nodes) || len(planned.Edges) != len(built.Edges) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
}

func TestPacketCopyLowererPreservesAllStreamsForSingleSourceRemux(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream(), videoVP8TranscodeTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	muxers := &remuxTestMuxerFactory{}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
			testFormatMuxer(av.FormatOgg, muxers),
		),
	)
	task, err := recipeFrom(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Copy().
		To(destinationHandle(fileDestination("recording.ogg", io.Discard))).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if len(muxers.muxers) != 1 || muxers.muxers[0].streamCount != 2 ||
		!streamIDsEqual(muxers.muxers[0].openedStreams, []av.StreamID{"audio", "video"}) {
		t.Fatalf("muxers=%d first=%+v", len(muxers.muxers), muxers.muxers)
	}
}

func TestRecipeResolvedBuildUsesMediaPlanFileSinkDestination(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		),
		withTestCodecs(testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}})),
	)
	job := recipeFrom(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		To(Sink(&runtimeTestSink{name: "frames"}))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
}

func TestRecipeResolvedMediaPlanSinkDestinationPreservesCustomStage(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		),
		withTestCodecs(testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}})),
	)
	job := recipeFrom(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		Do(&runtimeTestStage{name: "meter"}).
		To(Sink(&runtimeTestSink{name: "frames"}))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	spec := task.Describe()
	if !specHasNode(spec, "meter") {
		t.Fatalf("built spec = %+v, want custom stage node", spec)
	}
}

func TestRecipeResolvedBuildUsesMediaPlanRTPSinkDestination(t *testing.T) {
	ctx := context.Background()
	runtime := mustNew(withTestCodecs(testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}})))
	job := recipeFrom(Input(liveAudioOpusProvider("audio"))).UseRuntime(runtime).
		Audio().
		Decode().
		To(Sink(&runtimeTestSink{name: "frames"}))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
}

func TestRecipeResolvedBuildUsesMediaPlanSelectedPacketSinkDestination(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		),
	)
	job := recipeFrom(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Copy().
		To(Sink(&runtimeTestSink{name: "packets"}))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
}

func TestRecipeResolvedBuildUsesMediaPlanFileEncodeOutput(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
		withTestCodecs(
			testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
			testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		),
	)
	job := recipeFrom(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		Do(&runtimeTestStage{name: "meter"}).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(destinationHandle(fileDestination("archive.ogg", io.Discard)))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	built := task.Describe()
	if !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
	if !specHasNode(built, "meter") || !specHasNode(built, "encode-audio") || !specHasNode(built, "archive.ogg") {
		t.Fatalf("built spec = %+v, want meter, encode, and mux nodes", built)
	}
}

func TestRecipeResolvedBuildUsesMediaPlanFileEncodeSinkDestination(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		),
		withTestCodecs(
			testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
			testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		),
	)
	job := recipeFrom(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(Sink(&runtimeTestSink{name: "packets"}))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	built := task.Describe()
	if !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
	if !specHasNode(built, "encode-audio") || !specHasNode(built, "packets") {
		t.Fatalf("built spec = %+v, want encode and sink nodes", built)
	}
}

func TestRecipeResolvedBuildUsesMediaPlanEncodeMuxAndSinkDestinations(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
		withTestCodecs(
			testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
			testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		),
	)
	job := recipeFrom(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(
			destinationHandle(fileDestination("archive.ogg", io.Discard)),
			Sink(&runtimeTestSink{name: "packets"}),
		)

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	built := task.Describe()
	if !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
	if !specHasNode(built, "encode-audio") || !specHasNode(built, "archive.ogg") || !specHasNode(built, "packets") {
		t.Fatalf("built spec = %+v, want encode, mux, and packet sink nodes", built)
	}
}

func TestRecipeResolvedBuildUsesMediaPlanRTPEncodeOutput(t *testing.T) {
	ctx := context.Background()
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
		withTestCodecs(
			testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
			testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		),
	)
	job := recipeFrom(Input(liveAudioOpusProvider("audio"))).UseRuntime(runtime).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(destinationHandle(fileDestination("archive.ogg", io.Discard)))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
}
