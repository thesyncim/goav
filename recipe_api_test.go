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
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/graphrender"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/rtpav"
	"github.com/thesyncim/goav/webrtcav"
)

type recipeAPIRTPReader struct{}

type recipeAPIRuntimeWithoutBuilder struct{}

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

func TestRuntimeInterfaceKeepsLegacyBuilderOutOfFrontDoor(t *testing.T) {
	runtimeType := reflect.TypeOf((*goav.Runtime)(nil)).Elem()
	if _, ok := runtimeType.MethodByName("New"); ok {
		t.Fatal("Runtime exposes legacy New builder; use Runtime.Graph for expert graph wiring")
	}
	if _, ok := runtimeType.MethodByName("Graph"); !ok {
		t.Fatal("Runtime should expose Graph as the expert graph entry point")
	}
}

func TestTranscodeJobKeepsPlanIROutOfFrontDoor(t *testing.T) {
	transcodeType := reflect.TypeOf((*goav.TranscodeJob)(nil))
	if _, ok := transcodeType.MethodByName("Plan"); ok {
		t.Fatal("TranscodeJob exposes transcode.Plan; use Intent, Describe, Build, or Run")
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
		"WebRTCRemote":           true,
	}
	legacyTypes := map[string]bool{
		"Builder":         true,
		"Input":           true,
		"Output":          true,
		"ProbeRequest":    true,
		"ProbeResult":     true,
		"Source":          true,
		"Stage":           true,
		"Sink":            true,
		"Metadata":        true,
		"CodecParameters": true,
		"RTPOption":       true,
		"TrackOption":     true,
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

func TestRecipeReportsRuntimeWithoutCompilerBuilder(t *testing.T) {
	_, err := goav.Record(
		goav.FileInput("input.ivf", strings.NewReader("")),
		goav.FileOutput("recording.ivf", io.Discard),
		goav.UseRuntime(recipeAPIRuntimeWithoutBuilder{}),
	).Build(context.Background())

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
	job := goav.Record(
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
	if intent.Name != "record" || len(intent.Inputs) != 1 || len(intent.Outputs) != 1 {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestReadmeRecordFanoutRecipeIsSmall(t *testing.T) {
	job := goav.Record(
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
	if intent.Name != "record" || len(intent.Inputs) != 1 || len(intent.Outputs) != 2 {
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

func TestReadmeDecodeShortcutUsesFrameSink(t *testing.T) {
	sink := goav.SinkFunc("frames", func(context.Context, goav.Message) error {
		return nil
	})
	job := goav.Decode(
		goav.RTP(recipeAPIRTPReader{}).Codec(goav.Opus()),
		goav.FrameSink(sink),
	)

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "rtp -> select-opus") ||
		!strings.Contains(text, "select-opus -> decode-opus") ||
		!strings.Contains(text, "decode-opus -> frames") {
		t.Fatalf("spec:\n%s", text)
	}
	intent := job.Intent()
	if len(intent.Streams) != 1 || string(intent.Streams[0].Select.Codec) != "opus" || !intent.Streams[0].Decode {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestReadmeWebRTCTrackRecordRecipeIsSmall(t *testing.T) {
	job := goav.Record(
		goav.WebRTCTrack(&webrtc.TrackRemote{},
			goav.WithTrackCodec(webrtc.RTPCodecParameters{
				RTPCodecCapability: webrtc.RTPCodecCapability{
					MimeType:  webrtc.MimeTypeVP8,
					ClockRate: 90000,
				},
				PayloadType: 96,
			}),
			goav.WithTrackStream(goav.Stream{
				ID:   "video",
				Type: "video",
			}),
		),
		goav.FileOutput("recording.ivf", io.Discard),
	)

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "video -> recording.ivf") ||
		!strings.Contains(text, "rtp receive, depacketizers=1") {
		t.Fatalf("spec:\n%s", text)
	}
	intent := job.Intent()
	if len(intent.Inputs) != 1 ||
		string(intent.Inputs[0].Protocol) != "webrtc" ||
		string(intent.Inputs[0].Codec.ID) != "vp8" {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestReadmeWebRTCTrackRecordMultiInputRecipeIsSmall(t *testing.T) {
	job := goav.From(goav.WebRTCTrack(&webrtc.TrackRemote{},
		goav.WithTrackCodec(webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:  webrtc.MimeTypeOpus,
				ClockRate: 48000,
				Channels:  2,
			},
			PayloadType: 111,
		}),
		goav.WithTrackStream(goav.Stream{
			ID:   "audio",
			Type: "audio",
		}),
	)).
		And(goav.WebRTCTrack(&webrtc.TrackRemote{},
			goav.WithTrackCodec(webrtc.RTPCodecParameters{
				RTPCodecCapability: webrtc.RTPCodecCapability{
					MimeType:  webrtc.MimeTypeVP8,
					ClockRate: 90000,
				},
				PayloadType: 96,
			}),
			goav.WithTrackStream(goav.Stream{
				ID:   "video",
				Type: "video",
			}),
		)).
		To(goav.FileOutput("recording.webm", io.Discard))

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "audio -> recording.webm") ||
		!strings.Contains(text, "video -> recording.webm") ||
		strings.Count(text, "depacketizers=1") != 2 {
		t.Fatalf("spec:\n%s", text)
	}
	intent := job.Intent()
	if len(intent.Inputs) != 2 ||
		string(intent.Inputs[0].Codec.ID) != "opus" ||
		string(intent.Inputs[1].Codec.ID) != "vp8" {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestWebRTCTrackRecipeReportsNilTrack(t *testing.T) {
	_, err := goav.Record(
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
	if !errors.As(err, &buildErr) || buildErr.Code != "multi_input_unsupported" {
		t.Fatalf("err = %v, want multi_input_unsupported", err)
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
	_, err := goav.Record(
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
	_, err := goav.Decode(
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
	_, err := goav.Decode(
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
	_, err := goav.Decode(
		goav.FileInput("input.ogg", strings.NewReader("")),
		goav.FileOutput("frames.ogg", io.Discard),
	).Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "decode_output_invalid" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want decode_output_invalid wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "goav.Decode(input, goav.FrameSink") ||
		!strings.Contains(err.Error(), "goav.Record(input, output)") {
		t.Fatalf("err = %v, want decode output guidance", err)
	}
}

func TestRecordRecipeRejectsEmptyOutputSpec(t *testing.T) {
	_, err := goav.Record(
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
	_, err := goav.Record(
		goav.FileInput("input.ogg", strings.NewReader("")),
		goav.FileOutput("recording.ogg", nil),
	).Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_writer_missing" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_writer_missing wrapping ErrUnsupportedBuild", err)
	}
}

func TestRecordRecipeRejectsUnnamedFileOutputWithoutFormat(t *testing.T) {
	_, err := goav.Record(
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
	_, err := goav.Record(
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
	_, err := goav.Record(
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
	_, err := goav.Record(
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
	_, err := goav.Record(
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
			_, err := goav.Record(
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
			_, err := goav.Record(
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

func TestRTPRecipeRejectsUnsupportedAutoCodecIntent(t *testing.T) {
	_, err := goav.Record(
		goav.RTP(recipeAPIRTPReader{}).Name("audio").Codec(goav.CodecSpec{ID: "pcm"}),
		goav.FileOutput("recording.ogg", io.Discard),
	).Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "rtp_codec_unsupported" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want rtp_codec_unsupported wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "pcm has no built-in RTP depacketizer") ||
		!strings.Contains(err.Error(), ".Depacketize") {
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
			_, err := goav.Record(
				goav.RTP(recipeAPIRTPReader{}).Name("audio").Codec(tt.spec),
				goav.FileOutput("recording.ogg", io.Discard),
			).Build(context.Background())
			var buildErr *goav.BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, goav.ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want %s wrapping ErrUnsupportedBuild", err, tt.code)
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
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_operation_missing" {
		t.Fatalf("err = %v, want stream_operation_missing", err)
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
		!strings.Contains(err.Error(), "goav.Record") ||
		!strings.Contains(err.Error(), "goav.Transcode") {
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
		!strings.Contains(err.Error(), "goav.Transcode") {
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
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_missing" {
		t.Fatalf("err = %v, want encode_missing", err)
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
		!strings.Contains(err.Error(), "goav.Transcode") {
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
	_, err := goav.From(goav.FileInput("input.h264", strings.NewReader(""))).
		Video().
		Encode(goav.H264(goav.Bitrate(2_000_000))).
		To(goav.FileOutput("archive.h264", io.Discard)).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_work_in_progress" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want encode_work_in_progress wrapping ErrUnsupportedBuild", err)
	}
}

func TestStreamRecipeRejectsUnsupportedRecipeEncoder(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.wav", strings.NewReader(""))).
		Audio().
		Encode(goav.CodecSpec{ID: "pcm"}).
		To(goav.FileOutput("archive.wav", io.Discard)).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_unsupported" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want encode_unsupported wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "pcm is not a recipe encode target") ||
		!strings.Contains(err.Error(), "codec.EncodeConfig") {
		t.Fatalf("err = %v, want unsupported encode guidance", err)
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

func TestStreamRecipeRejectsUnresolvedEncodeIntents(t *testing.T) {
	tests := []struct {
		name string
		spec goav.CodecSpec
		code string
	}{
		{name: "auto", spec: goav.Auto(), code: "encode_auto_unresolved"},
		{name: "copy", spec: goav.Copy(), code: "copy_unresolved"},
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
	if err := goav.Record(
		goav.FileInput("input.ivf", bytes.NewReader(tinyIVF())),
		goav.FileOutput("preview.ivf", &out),
	).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if out.Len() == 0 {
		t.Fatal("empty output")
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

func TestDefaultRecordRecipeRunsWithExplicitUnnamedOutputFormat(t *testing.T) {
	var out bytes.Buffer
	job := goav.Record(
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
	job := goav.Transcode(goav.FileInput("input.webm", strings.NewReader(""))).
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

func TestTranscodeRecipeSingleBranchUsesOutputLabel(t *testing.T) {
	job := goav.Transcode(goav.FileInput("input.webm", strings.NewReader(""))).
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
	_, err := goav.Transcode(goav.FileInput("input.webm", strings.NewReader(""))).
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
	_, err := goav.Transcode(goav.FileInput("input.webm", strings.NewReader(""))).
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

func TestTranscodeRecipeRejectsEmptyOutputLabel(t *testing.T) {
	_, err := goav.Transcode(goav.FileInput("input.webm", strings.NewReader(""))).
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
	_, err := goav.Transcode(goav.FileInput("input.webm", strings.NewReader(""))).
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
	_, err := goav.Transcode(goav.FileInput("input.webm", strings.NewReader(""))).
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
	_, err := goav.Transcode(goav.FileInput("input.webm", strings.NewReader(""))).
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
	_, err := goav.Transcode(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("360p").VP9(600_000).
		To("preview").
		Output("preview", goav.FileOutput("preview.webm", nil)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_writer_missing" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_writer_missing wrapping ErrUnsupportedBuild", err)
	}
}

func TestTranscodeRecipeRequiresBranch(t *testing.T) {
	_, err := goav.Transcode(goav.FileInput("input.webm", strings.NewReader(""))).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_missing" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want stream_missing wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), ".Video(\"720p\")") || !strings.Contains(err.Error(), ".Audio(\"main\")") {
		t.Fatalf("err = %v, want branch guidance", err)
	}
}

func TestTranscodeRecipeRequiresBranchOutput(t *testing.T) {
	job := goav.Transcode(goav.FileInput("input.webm", strings.NewReader("")))
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
	_, err := goav.Transcode(goav.FileInput("input.webm", strings.NewReader(""))).
		Audio("bad", goav.StreamIndex(-1)).Opus(64_000).
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
	_, err := goav.Transcode(goav.FileInput("input.webm", strings.NewReader(""))).
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
	_, err := goav.Transcode(goav.FileInput("input.webm", strings.NewReader(""))).
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
	_, err := goav.Transcode(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("360p").VP9(600_000).Resize(640, 360).
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
	_, err := goav.Transcode(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("360p").VP9(600_000).VP8(400_000).
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
	_, err := goav.Transcode(goav.FileInput("input.webm", strings.NewReader(""))).
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
	_, err := goav.Transcode(goav.FileInput("input.webm", strings.NewReader(""))).
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
