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
	"github.com/thesyncim/goav/format"
	runconfig "github.com/thesyncim/goav/runconfig"
)

func TestBuildRejectsIncompatibleIVFMuxGroupBeforeOpeningMuxer(t *testing.T) {
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
	_, err := rt.Build(context.Background(),
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
	for _, want := range []string{"destination=web", "format=ivf", "branch=v8 codec=vp8 media=video", "branch=v9 codec=vp9 media=video"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want detail %q", err, want)
		}
	}
	if !strings.Contains(err.Error(), "route exactly one VP8, VP9, or AV1 video branch") {
		t.Fatalf("err = %v, want IVF guidance", err)
	}
}

func TestBuildRejectsDescriptorBackedMuxIncompatibility(t *testing.T) {
	rt := mustRuntime(
		runconfig.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{
				{Index: 0, ID: "video", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo}},
			}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
			registry.RegisterMuxerDescriptor(format.Descriptor{
				Format:     av.FormatMatroska,
				Media:      []av.MediaType{av.MediaAudio},
				Codecs:     []av.CodecID{av.CodecOpus},
				MinStreams: 1,
				MaxStreams: 1,
				Metadata: av.Metadata{
					"summary": "test matroska target accepts one Opus audio stream",
				},
			}, recipeAPIMuxerFactory{})
		}),
		runconfig.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
		}),
	)

	target := goav.Write("out.mkv", io.Discard, goav.Format(av.FormatMatroska))
	_, err := rt.Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Video().
		Decode().
		Encode(codec.VP8(codec.Bitrate(600_000))).
		As("video").
		To(target))
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_mux_incompatible" {
		t.Fatalf("err = %v, want destination_mux_incompatible", err)
	}
	for _, want := range []string{
		"test matroska target accepts one Opus audio stream",
		"destination=out.mkv",
		"format=matroska",
		"branch=video codec=vp8 media=video",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want detail %q", err, want)
		}
	}
}

func TestBuildRejectsIncompatibleAnnexBMuxGroup(t *testing.T) {
	rt := mustRuntime(
		runconfig.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterProber(recipeAPIStreamProber{streams: []av.Stream{
				{Index: 0, ID: "video", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo}},
			}})
			registry.RegisterDemuxer(av.FormatOgg, recipeAPIDemuxerFactory{})
			registry.RegisterMuxer(av.FormatAnnexB, recipeAPIMuxerFactory{})
		}),
		runconfig.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
			registry.RegisterDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIDecoderFactory{})
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, recipeAPIEncoderFactory{})
		}),
	)

	_, err := rt.Build(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Video().
		Decode().
		Encode(codec.VP8(codec.Bitrate(600_000))).
		To(goav.Write("out.h264", io.Discard, goav.Format(av.FormatAnnexB))))
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_mux_incompatible" {
		t.Fatalf("err = %v, want destination_mux_incompatible", err)
	}
	if !strings.Contains(err.Error(), "Annex B destinations support one H264 video stream") ||
		!strings.Contains(err.Error(), "destination=out.h264") ||
		!strings.Contains(err.Error(), "branch=video codec=vp8 media=video") {
		t.Fatalf("err = %v, want Annex B codec guidance", err)
	}
}
