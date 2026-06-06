package goav_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/graphrender"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/rtpav"
	"github.com/thesyncim/goav/webrtcav"
)

type recipeAPIRTPReader struct{}

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
	return graphrender.Render(spec, graphrender.Text)
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

func TestReadmeAudioDecodeRecipeIsSmall(t *testing.T) {
	sink := goav.SinkFunc("frames", func(context.Context, goav.Message) error {
		return nil
	})
	job := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
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
		nil,
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
		goav.SinkFunc("frames", nil),
	).Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_invalid" || !errors.Is(err, goav.ErrNilSink) {
		t.Fatalf("err = %v, want output_invalid wrapping ErrNilSink", err)
	}
	if !strings.Contains(err.Error(), "non-nil sink") {
		t.Fatalf("err = %v, want sink guidance", err)
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
		Decode().
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
		Decode().
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
		Decode().
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
		Decode().
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

func TestStreamRecipeRequiresOperation(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		To(goav.FrameSink(goav.SinkFunc("frames", func(context.Context, goav.Message) error {
			return nil
		}))).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_operation_missing" {
		t.Fatalf("err = %v, want stream_operation_missing", err)
	}
}

func TestStreamRecipeRejectsSecondStreamSelection(t *testing.T) {
	_, err := goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Audio().
		Decode().
		To(goav.FrameSink(goav.SinkFunc("audio", func(context.Context, goav.Message) error {
			return nil
		}))).
		Video().
		Decode().
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
		Decode().
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
		Decode().
		To(goav.FileOutput("archive.ogg", io.Discard)).
		Build(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_missing" {
		t.Fatalf("err = %v, want encode_missing", err)
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

func TestDefaultRecordIVFRecipeRuns(t *testing.T) {
	var out bytes.Buffer
	task, err := goav.Record(
		goav.FileInput("input.ivf", bytes.NewReader(tinyIVF())),
		goav.FileOutput("preview.ivf", &out),
	).Build(context.Background())
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
}

func TestTranscodeRecipeAcceptsDirectOutputSpec(t *testing.T) {
	job := goav.Transcode(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("360p").Resize(640, 360).VP9(600_000).
		To(goav.FileOutput("preview.webm", io.Discard))

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

func TestTranscodeRecipeRejectsDuplicateDirectOutputSpecs(t *testing.T) {
	_, err := goav.Transcode(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("720p").VP9(2_000_000).To(goav.FileOutput("same.webm", io.Discard)).
		Video("360p").VP9(600_000).To(goav.FileOutput("same.webm", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_duplicate" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_duplicate wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), `output label "same.webm"`) ||
		!strings.Contains(err.Error(), "distinct goav.FileOutput") {
		t.Fatalf("err = %v, want direct output duplicate guidance", err)
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

func TestTranscodeRecipeRejectsDuplicateDirectOutputSpecInBranch(t *testing.T) {
	_, err := goav.Transcode(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("720p").VP9(2_000_000).
		To(
			goav.FileOutput("same.webm", io.Discard),
			goav.FileOutput("same.webm", io.Discard),
		).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_duplicate" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_duplicate wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), `branch routes to output "same.webm" more than once`) ||
		!strings.Contains(err.Error(), "distinct labels") {
		t.Fatalf("err = %v, want duplicate direct branch output guidance", err)
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

func TestTranscodeRecipeRejectsInvalidOutputTarget(t *testing.T) {
	_, err := goav.Transcode(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("360p").VP9(600_000).
		To(42).
		Output("preview", goav.FileOutput("preview.webm", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_target_invalid" || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_target_invalid wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "target 0: unsupported target type int") ||
		!strings.Contains(err.Error(), "goav.FileOutput") {
		t.Fatalf("err = %v, want target type and output guidance", err)
	}
}

func TestTranscodeRecipeRejectsInvalidOutputSpec(t *testing.T) {
	_, err := goav.Transcode(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("360p").VP9(600_000).
		To(goav.FileOutput("preview.webm", nil)).
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
		To(goav.FileOutput("bad.ogg", io.Discard)).
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
		To(goav.FileOutput("bad.webm", io.Discard)).
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
		To(goav.FileOutput("bad.ogg", io.Discard)).
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

func TestTranscodeRecipeRejectsNegativeEncodeBitrate(t *testing.T) {
	_, err := goav.Transcode(goav.FileInput("input.webm", strings.NewReader(""))).
		Video("bad").VP9(-1).
		To(goav.FileOutput("bad.webm", io.Discard)).
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
		To(goav.FileOutput("preview.webm", io.Discard)).
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
