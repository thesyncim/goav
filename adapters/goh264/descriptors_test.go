package goh264

import (
	"testing"

	"github.com/thesyncim/goav/av"
)

func TestDescriptor(t *testing.T) {
	desc := Descriptor()
	if desc.ID != av.CodecH264 || desc.Backend.Module != "github.com/thesyncim/goh264" {
		t.Fatalf("descriptor = %+v", desc)
	}
	if len(desc.Capabilities.BuildTags) != 1 || desc.Capabilities.BuildTags[0] != "goav_goh264" {
		t.Fatalf("build tags = %+v", desc.Capabilities.BuildTags)
	}
}
