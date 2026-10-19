package goav_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/plan"
	runconfig "github.com/thesyncim/goav/runconfig"
)

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
