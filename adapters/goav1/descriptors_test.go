package goav1

import (
	"slices"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

func TestDescriptor(t *testing.T) {
	desc := Descriptor()
	if desc.ID != av.CodecAV1 || desc.Backend.Module != "github.com/thesyncim/goav1" {
		t.Fatalf("descriptor = %+v", desc)
	}
	if !desc.Supports(codec.ModeDecode) || desc.Supports(codec.ModeEncode) {
		t.Fatalf("decode descriptor modes = %v", desc.Modes)
	}
	for _, pixelFormat := range []string{
		av.PixelFormatI420,
		av.PixelFormatYUV420P,
		av.PixelFormatI422,
		av.PixelFormatYUV422P,
		av.PixelFormatI444,
		av.PixelFormatYUV444P,
		av.PixelFormatGray8,
	} {
		if !slices.Contains(desc.Capabilities.PixelFormats, pixelFormat) {
			t.Fatalf("pixel formats = %v, missing %s", desc.Capabilities.PixelFormats, pixelFormat)
		}
	}
	if len(desc.Capabilities.BuildTags) != 0 {
		t.Fatalf("build tags = %v", desc.Capabilities.BuildTags)
	}
	if desc.Backend.Status != "active" {
		t.Fatalf("backend status = %q", desc.Backend.Status)
	}
}

func TestEncoderDescriptor(t *testing.T) {
	desc := EncoderDescriptor()
	if desc.ID != av.CodecAV1 || desc.Backend.Module != "github.com/thesyncim/goav1" {
		t.Fatalf("descriptor = %+v", desc)
	}
	if !desc.Supports(codec.ModeEncode) || desc.Supports(codec.ModeDecode) {
		t.Fatalf("encode descriptor modes = %v", desc.Modes)
	}
	if !slices.Contains(desc.Capabilities.PixelFormats, av.PixelFormatI420) ||
		!slices.Contains(desc.Capabilities.PixelFormats, av.PixelFormatYUV420P) ||
		slices.Contains(desc.Capabilities.PixelFormats, av.PixelFormatI422) {
		t.Fatalf("encode pixel formats = %v", desc.Capabilities.PixelFormats)
	}
}
