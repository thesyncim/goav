package goav_test

import (
	"bytes"
	"context"
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
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/plan"
	runconfig "github.com/thesyncim/goav/runconfig"
	"github.com/thesyncim/goav/shape"
)

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
