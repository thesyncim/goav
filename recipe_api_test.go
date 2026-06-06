package goav_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/webrtcav"
)

func TestReadmeRecordRecipeIsSmall(t *testing.T) {
	job := goav.Record(
		goav.FileInput("input.ogg", strings.NewReader("")),
		goav.FileOutput("recording.ogg", io.Discard),
	)

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(spec.String(), "input.ogg -> recording.ogg") {
		t.Fatalf("spec:\n%s", spec.String())
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
	text := spec.String()
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
	text := spec.String()
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
	text := spec.String()
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
	text := spec.String()
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
	text := spec.String()
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
	text := spec.String()
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
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_work_in_progress" {
		t.Fatalf("err = %v, want encode_work_in_progress", err)
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
	text := spec.String()
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
	text := spec.String()
	if !strings.Contains(text, "encode-360p -> preview.webm") {
		t.Fatalf("spec:\n%s", text)
	}
	intent := job.Intent()
	if len(intent.Streams) != 1 || len(intent.Streams[0].RouteTo) != 1 || len(intent.Outputs) != 1 {
		t.Fatalf("intent: %+v", intent)
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
