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
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/provider"
)

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
