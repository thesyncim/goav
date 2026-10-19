package goav_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/goavtest"
	runconfig "github.com/thesyncim/goav/runconfig"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/source"
)

func TestStreamRecipeRequiresExplicitDecodeBeforeFrameConsumers(t *testing.T) {
	meter := component.FrameFunc("meter", func(ctx context.Context, frame *av.Frame, emit component.Emit) error {
		return emit.Frame(frame)
	})
	tests := []struct {
		name string
		job  *testRecipe
		fix  string
	}{
		{
			name: "custom stage",
			job: recipeOf(goav.FileInput("input.ogg", strings.NewReader("")).
				Audio().
				Do(meter).
				To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error { return nil })))),
			fix: "call .Decode() before",
		},
		{
			name: "resize",
			job: recipeOf(goav.FileInput("input.webm", strings.NewReader("")).
				Video().
				Resize(1280, 720).
				To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error { return nil })))),
			fix: "call .Decode() before",
		},
		{
			name: "resample",
			job: recipeOf(goav.FileInput("input.ogg", strings.NewReader("")).
				Audio().
				Resample(16_000, codec.Mono).
				To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error { return nil })))),
			fix: "call .Decode() before",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.job.BuildLive(context.Background())
			var buildErr *goav.BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != "operation_shape_mismatch" {
				t.Fatalf("err = %v, want operation_shape_mismatch with matching BuildError code", err)
			}
			if !strings.Contains(err.Error(), "needs decoded frames") || !strings.Contains(err.Error(), tt.fix) {
				t.Fatalf("err = %v, want explicit decode guidance %q", err, tt.fix)
			}
		})
	}
}

func TestStreamRecipeRejectsNegativeStreamIndex(t *testing.T) {
	_, err := mustRuntime().Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio(goav.StreamIndex(-1)).
		Decode().
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))))
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_selector_invalid" {
		t.Fatalf("err = %v, want stream_selector_invalid with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "stream index must be non-negative") ||
		!strings.Contains(err.Error(), "index=-1") ||
		!strings.Contains(err.Error(), "goav.StreamIndex(0)") {
		t.Fatalf("err = %v, want stream index guidance", err)
	}
}

func TestStreamRecipeRejectsNilCustomStage(t *testing.T) {
	_, err := recipeOf(goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Do(nil).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		})))).
		BuildLive(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stage_missing" {
		t.Fatalf("err = %v, want stage_missing wrapping errNilStage", err)
	}
	if !strings.Contains(err.Error(), ".Do(stage)") ||
		!strings.Contains(err.Error(), "component.FrameFunc") {
		t.Fatalf("err = %v, want custom stage guidance", err)
	}
}

func TestNilPacketFuncDoesNotBecomeSilentNilStage(t *testing.T) {
	_, err := mustRuntime().Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Do(component.PacketFunc("packets", nil)).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))))
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stage_missing" {
		t.Fatalf("err = %v, want stage_missing with matching BuildError code", err)
	}
	if !errors.Is(err, component.ErrNilStageCallback) {
		t.Fatalf("err = %v, want ErrNilStageCallback cause", err)
	}
	if !strings.Contains(err.Error(), "component.PacketFunc") ||
		!strings.Contains(err.Error(), "non-nil stage") {
		t.Fatalf("err = %v, want PacketFunc nil-callback guidance", err)
	}
}

func TestNilSinkFuncDoesNotBecomeSilentNilSink(t *testing.T) {
	_, err := decodeJob(
		goav.FileInput("input.ogg", strings.NewReader("")),
		goav.Sink(component.SinkFunc("frames", nil)),
	).BuildLive(context.Background())
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_invalid" {
		t.Fatalf("err = %v, want output_invalid with matching BuildError code", err)
	}
	if !errors.Is(err, component.ErrNilSinkCallback) {
		t.Fatalf("err = %v, want ErrNilSinkCallback cause", err)
	}
	if !strings.Contains(err.Error(), "SinkFunc") ||
		!strings.Contains(err.Error(), "non-nil sink") {
		t.Fatalf("err = %v, want SinkFunc nil-callback guidance", err)
	}
}

// TestStreamRecipeRejectsWrongMediaTransform pins the one media check every
// recipe form and runtime attach share: the shape solver's.
func TestStreamRecipeRejectsWrongMediaTransform(t *testing.T) {
	_, err := goavtest.Runtime().Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Resize(320, 180).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))))
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "operation_shape_mismatch" {
		t.Fatalf("err = %v, want operation_shape_mismatch", err)
	}
}

func TestStreamRecipeRejectsInvalidResize(t *testing.T) {
	_, err := mustRuntime().Build(context.Background(), goav.FileInput("input.webm", strings.NewReader("")).
		Video().
		Decode().
		Resize(0, 720).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))))
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "transform_invalid" {
		t.Fatalf("err = %v, want transform_invalid with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "positive width and height") ||
		!strings.Contains(err.Error(), "width=0") {
		t.Fatalf("err = %v, want resize value guidance", err)
	}
}

func TestStreamRecipeRequiresEncoderForFile(t *testing.T) {
	_, err := mustRuntime().Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Resample(48_000, codec.Stereo).
		To(goav.Write("archive.ogg", io.Discard)))
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_missing" {
		t.Fatalf("err = %v, want encode_missing with matching BuildError code", err)
	}
}

func TestStreamRecipeRejectsMixedSinkAndFile(t *testing.T) {
	_, err := mustRuntime().Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		})), goav.Write("archive.ogg", io.Discard)))

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_kind_mixed" {
		t.Fatalf("err = %v, want output_kind_mixed with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "cannot mix sinks and muxed outputs") ||
		!strings.Contains(err.Error(), "fork the decoded Stream") {
		t.Fatalf("err = %v, want mixed output guidance", err)
	}
}

func TestStreamRecipeAllowsEncodedMuxAndSinkDestinations(t *testing.T) {
	rt := mustRuntime(
		runconfig.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{
				{Index: 0, ID: "audio", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}},
			}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
			registry.RegisterMuxer(av.FormatOgg, recipeAPIMuxerFactory{})
		}),
		runconfig.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, recipeAPIEncoderFactory{})
		}),
	)

	spec, err := rt.Describe(goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(goav.Write("archive.ogg", io.Discard), goav.Sink(component.SinkFunc("packets", func(context.Context, component.Message) error {
			return nil
		}))))
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
	_, err := mustRuntime().Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		Resample(16_000, codec.Mono).
		To(goav.Write("archive.ogg", io.Discard)))

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_step_after_encode" {
		t.Fatalf("err = %v, want stream_step_after_encode with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "step: resample") ||
		!strings.Contains(err.Error(), "encoder: opus") ||
		!strings.Contains(err.Error(), "before .Encode") {
		t.Fatalf("err = %v, want terminal encoder guidance", err)
	}
}

func TestStreamRecipeRejectsDuplicateEncoder(t *testing.T) {
	_, err := mustRuntime().Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		Encode(codec.VP9(codec.Bitrate(600_000))).
		To(goav.Write("archive.ogg", io.Discard)))

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_duplicate" {
		t.Fatalf("err = %v, want encode_duplicate with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "first encoder: opus") ||
		!strings.Contains(err.Error(), "second encoder: vp9") ||
		!strings.Contains(err.Error(), "one terminal encoder") {
		t.Fatalf("err = %v, want duplicate encoder guidance", err)
	}
}

func TestStreamRecipeAllowsRuntimeRegisteredRecipeEncoders(t *testing.T) {
	tests := []struct {
		name  string
		codec codec.Spec
	}{
		{name: "h264", codec: codec.H264(codec.Bitrate(2_000_000))},
		{name: "av1", codec: codec.AV1(codec.Bitrate(2_000_000))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := goav.Source("frames",
				shape.Frame(av.MediaVideo, shape.Video(16, 16, av.PixelFormatI420)),
				func(_ context.Context, push source.Push) error { return push.EOS() },
			)
			rt := mustRuntime(
				runconfig.WithMuxer(av.FormatIVF, recipeAPIMuxerFactory{}),
				runconfig.WithEncoder(codec.Descriptor{ID: tt.codec.ID, Type: av.MediaVideo}, recipeAPIEncoderFactory{}),
			)
			_, err := rt.Describe(source.
				Video().
				Encode(tt.codec).
				To(goav.Write("archive.ivf", io.Discard, goav.Format(av.FormatIVF))))
			if err != nil {
				t.Fatalf("Describe() err = %v", err)
			}
		})
	}
}

func TestStreamRecipeReportsMissingCustomEncoder(t *testing.T) {
	rt := mustRuntime(runconfig.WithFormatAdapter(func(registry *format.SimpleRegistry) {
		registry.RegisterMuxer(av.FormatOgg, recipeAPIMuxerFactory{})
	}))
	_, err := rt.Build(context.Background(), goav.FileInput("input.wav", strings.NewReader("")).
		Audio().
		Decode().
		Encode(codec.Codec("pcm", av.MediaAudio)).
		To(goav.Write("archive.ogg", io.Discard)))
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
	_, err := mustRuntime().Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(-1))).
		To(goav.Write("archive.ogg", io.Discard)))
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_parameter_invalid" {
		t.Fatalf("err = %v, want encode_parameter_invalid with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "bitrate must be non-negative") ||
		!strings.Contains(err.Error(), "bitrate=-1") {
		t.Fatalf("err = %v, want bitrate guidance", err)
	}
}

func TestStreamRecipeRejectsInvalidCodecTimingOptions(t *testing.T) {
	tests := []struct {
		name string
		job  *testRecipe
		want string
	}{
		{
			name: "fps",
			job: recipeOf(goav.FileInput("input.webm", strings.NewReader("")).
				Video().
				Decode().
				Encode(codec.VP9(codec.FPS(0))).
				To(goav.Write("preview.webm", io.Discard))),
			want: "encode FPS must be positive",
		},
		{
			name: "keyframe interval",
			job: recipeOf(goav.FileInput("input.webm", strings.NewReader("")).
				Video().
				Decode().
				Encode(codec.VP9(codec.KeyframeInterval(-1))).
				To(goav.Write("preview.webm", io.Discard))),
			want: "encode keyframe interval must be non-negative",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.job.BuildLive(context.Background())
			var buildErr *goav.BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != "encode_parameter_invalid" {
				t.Fatalf("err = %v, want encode_parameter_invalid with matching BuildError code", err)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestStreamRecipeRejectsInvalidEncodeSampleRate(t *testing.T) {
	_, err := mustRuntime().Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Encode(codec.Opus(codec.SampleRate(0))).
		To(goav.Write("archive.ogg", io.Discard)))
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_parameter_invalid" {
		t.Fatalf("err = %v, want encode_parameter_invalid with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "sample rate must be positive") ||
		!strings.Contains(err.Error(), "sample_rate=0") {
		t.Fatalf("err = %v, want sample-rate guidance", err)
	}
}

func TestStreamRecipeReportsMissingEncodeAdapterBeforeOpeningInput(t *testing.T) {
	_, err := mustRuntime(bundle.Formats()).Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(goav.Write("archive.ivf", io.Discard)))
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
	rt := mustRuntime(runconfig.WithFormatAdapter(func(registry *format.SimpleRegistry) {
		registry.RegisterProber(recipeAPIStreamProber{streams: streams})
		registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{called: &demuxerOpened})
	}))

	_, err := rt.Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))))
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_ambiguous" {
		t.Fatalf("err = %v, want stream_ambiguous with matching BuildError code", err)
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
	rt := mustRuntime(runconfig.WithFormatAdapter(func(registry *format.SimpleRegistry) {
		registry.RegisterProber(recipeAPIStreamProber{streams: streams})
		registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{called: &demuxerOpened})
	}))

	_, err := rt.Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))))
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "decode_adapter_missing" || !errors.Is(err, codec.ErrNotFound) {
		t.Fatalf("err = %v, want decode_adapter_missing wrapping codec.ErrNotFound", err)
	}
	if demuxerOpened {
		t.Fatal("demuxer opened before known decoder preflight failed")
	}
	if !strings.Contains(err.Error(), "codec=opus") ||
		!strings.Contains(err.Error(), "input.To(output)") ||
		strings.Contains(err.Error(), "cannot open input") ||
		strings.Contains(err.Error(), "stream_ambiguous") {
		t.Fatalf("err = %v, want probed decoder guidance before input diagnostics", err)
	}
}

func TestStreamRecipeReportsMissingTransformAdapterBeforeOpeningInput(t *testing.T) {
	_, err := mustRuntime(bundle.Codecs()).Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Resample(16_000, codec.Mono).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))))
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "transform_adapter_missing" || !errors.Is(err, filter.ErrNotFound) {
		t.Fatalf("err = %v, want transform_adapter_missing wrapping filter.ErrNotFound", err)
	}
	if !strings.Contains(err.Error(), "transform=resample") ||
		!strings.Contains(err.Error(), "bundle.Filters()") ||
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
	rt := mustRuntime(
		runconfig.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: streams})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{called: &demuxerOpened})
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
	_, err := rt.Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Resample(16_000, codec.Mono).
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))))
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "transform_adapter_incompatible" {
		t.Fatalf("err = %v, want transform_adapter_incompatible with matching BuildError code", err)
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
		spec codec.Spec
		code errcode.Code
	}{
		{name: "auto", spec: codec.Auto(), code: "encode_auto_unresolved"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := mustRuntime().Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
				Audio().
				Decode().
				Encode(tt.spec).
				To(goav.Write("archive.ogg", io.Discard)))
			var buildErr *goav.BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code {
				t.Fatalf("err = %v, want %s with matching BuildError code", err, tt.code)
			}
		})
	}
}
