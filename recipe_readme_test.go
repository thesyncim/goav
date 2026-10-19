package goav_test

import (
	"bytes"
	"context"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/component"
)

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
