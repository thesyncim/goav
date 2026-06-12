package goav

import (
	"reflect"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

func TestCodecDescriptorMediaCompatibilityContracts(t *testing.T) {
	audio := codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}
	video := codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}
	wildcard := codec.Descriptor{ID: av.CodecID("x-any")}

	if !codecDescriptorsMediaCompatible([]codec.Descriptor{audio}, "") {
		t.Fatal("empty media request should be compatible")
	}
	if !codecDescriptorsMediaCompatible([]codec.Descriptor{wildcard}, av.MediaVideo) {
		t.Fatal("wildcard descriptor should match requested media")
	}
	if !codecDescriptorsMediaCompatible([]codec.Descriptor{audio}, av.MediaAudio) {
		t.Fatal("audio descriptor did not match audio request")
	}
	if codecDescriptorsMediaCompatible([]codec.Descriptor{audio}, av.MediaVideo) {
		t.Fatal("audio descriptor matched video request")
	}

	descriptors := []codec.Descriptor{audio, video, wildcard}
	if got := descriptorsMatchingMedia(descriptors, ""); !reflect.DeepEqual(got, descriptors) {
		t.Fatalf("empty media descriptors = %#v, want original list", got)
	}
	if got, want := descriptorsMatchingMedia(descriptors, av.MediaAudio), []codec.Descriptor{audio, wildcard}; !reflect.DeepEqual(got, want) {
		t.Fatalf("audio descriptors = %#v, want %#v", got, want)
	}
	if got, want := descriptorSupportedMedia([]codec.Descriptor{wildcard, audio, audio, video}), []av.MediaType{av.MediaAudio, av.MediaVideo}; !reflect.DeepEqual(got, want) {
		t.Fatalf("supported media = %#v, want %#v", got, want)
	}
}

func TestCodecDescriptorFormatCompatibilityContracts(t *testing.T) {
	audioWildcard := codec.Descriptor{
		ID:   av.CodecID("x-audio-any"),
		Type: av.MediaAudio,
	}
	audioS16 := codec.Descriptor{
		ID:           av.CodecID("x-audio-s16"),
		Type:         av.MediaAudio,
		Capabilities: codec.Capabilities{SampleFormats: []string{av.SampleFormatS16}},
	}
	audioF32 := codec.Descriptor{
		ID:           av.CodecID("x-audio-f32"),
		Type:         av.MediaAudio,
		Capabilities: codec.Capabilities{SampleFormats: []string{"", av.SampleFormatF32, av.SampleFormatS16}},
	}
	videoI420 := codec.Descriptor{
		ID:           av.CodecID("x-video-i420"),
		Type:         av.MediaVideo,
		Capabilities: codec.Capabilities{PixelFormats: []string{av.PixelFormatI420}},
	}
	videoYUV := codec.Descriptor{
		ID:           av.CodecID("x-video-yuv"),
		Type:         av.MediaVideo,
		Capabilities: codec.Capabilities{PixelFormats: []string{"", av.PixelFormatYUV420P, av.PixelFormatI420}},
	}

	if !codecDescriptorsSampleFormatCompatible([]codec.Descriptor{audioS16}, "") {
		t.Fatal("empty sample format should be compatible")
	}
	if !codecDescriptorsSampleFormatCompatible([]codec.Descriptor{audioWildcard}, av.SampleFormatF32) {
		t.Fatal("descriptor with no sample format list should accept any sample format")
	}
	if !codecDescriptorsSampleFormatCompatible([]codec.Descriptor{audioS16}, av.SampleFormatS16) {
		t.Fatal("descriptor did not accept supported sample format")
	}
	if !codecDescriptorsSampleFormatCompatible([]codec.Descriptor{audioS16, audioF32}, av.SampleFormatF32) {
		t.Fatal("later descriptor did not accept supported sample format")
	}
	if codecDescriptorsSampleFormatCompatible([]codec.Descriptor{audioS16}, av.SampleFormatF32) {
		t.Fatal("descriptor accepted unsupported sample format")
	}

	if !codecDescriptorsPixelFormatCompatible([]codec.Descriptor{videoI420}, "") {
		t.Fatal("empty pixel format should be compatible")
	}
	if !codecDescriptorsPixelFormatCompatible([]codec.Descriptor{{ID: av.CodecID("x-video-any"), Type: av.MediaVideo}}, av.PixelFormatYUV420P) {
		t.Fatal("descriptor with no pixel format list should accept any pixel format")
	}
	if !codecDescriptorsPixelFormatCompatible([]codec.Descriptor{videoI420}, av.PixelFormatI420) {
		t.Fatal("descriptor did not accept supported pixel format")
	}
	if !codecDescriptorsPixelFormatCompatible([]codec.Descriptor{videoI420, videoYUV}, av.PixelFormatYUV420P) {
		t.Fatal("later descriptor did not accept supported pixel format")
	}
	if codecDescriptorsPixelFormatCompatible([]codec.Descriptor{videoI420}, av.PixelFormatYUV420P) {
		t.Fatal("descriptor accepted unsupported pixel format")
	}

	if got, want := descriptorSupportedSampleFormats([]codec.Descriptor{audioS16, audioF32}), []string{av.SampleFormatS16, av.SampleFormatF32}; !reflect.DeepEqual(got, want) {
		t.Fatalf("supported sample formats = %#v, want %#v", got, want)
	}
	if got, want := descriptorSupportedPixelFormats([]codec.Descriptor{videoI420, videoYUV}), []string{av.PixelFormatI420, av.PixelFormatYUV420P}; !reflect.DeepEqual(got, want) {
		t.Fatalf("supported pixel formats = %#v, want %#v", got, want)
	}
}
