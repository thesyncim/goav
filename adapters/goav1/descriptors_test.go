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
	if desc.Supports(codec.ModeEncode) {
		t.Fatal("goav1 descriptor should not claim encode support yet")
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
