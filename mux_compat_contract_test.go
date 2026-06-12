package goav

import (
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/format"
)

func TestDescriptorMuxReasonContracts(t *testing.T) {
	formatID := av.FormatID("xpack")
	if got := descriptorMuxReason(formatID, format.Descriptor{Metadata: av.Metadata{"summary": "custom mux accepts one opus audio stream"}}); got != "custom mux accepts one opus audio stream" {
		t.Fatalf("metadata reason = %q", got)
	}
	if got := descriptorMuxReason(formatID, format.Descriptor{MinStreams: 1, MaxStreams: 1}); got != "xpack destinations support 1 stream(s)" {
		t.Fatalf("single stream reason = %q", got)
	}
	if got := descriptorMuxReason(formatID, format.Descriptor{MaxStreams: 2, Media: []av.MediaType{av.MediaAudio}, Codecs: []av.CodecID{av.CodecOpus, "", av.CodecFLAC}}); got != "xpack destinations support up to 2 stream(s), media=audio, codecs=opus,flac" {
		t.Fatalf("capability reason = %q", got)
	}
	if got := descriptorMuxReason(formatID, format.Descriptor{}); got != "xpack destination rejected the planned mux group" {
		t.Fatalf("fallback reason = %q", got)
	}
}

func TestDescriptorMuxCompatibilityContracts(t *testing.T) {
	output := workDestination{Name: "archive.xpack", Format: av.FormatID("xpack")}
	streams := []plannedMuxStream{
		{Branch: "voice", Media: av.MediaAudio, Codec: av.CodecOpus},
		{Branch: "screen", Media: av.MediaVideo, Codec: av.CodecVP8},
	}

	if issue, ok := checkDescriptorMuxCompatibility(output, streams, format.Descriptor{}); ok {
		t.Fatalf("unrestricted descriptor issue = %+v, want none", issue)
	}
	issue, ok := checkDescriptorMuxCompatibility(output, streams, format.Descriptor{MaxStreams: 1})
	if !ok {
		t.Fatal("MaxStreams descriptor did not reject the mux group")
	}
	if issue.Code != errcode.DestinationMuxIncompatible || issue.Destination != "archive.xpack" || issue.Format != av.FormatID("xpack") {
		t.Fatalf("issue identity = %+v", issue)
	}
	if !strings.Contains(issue.Reason, "up to 1 stream") {
		t.Fatalf("reason = %q, want stream limit", issue.Reason)
	}
	wantDetails := []string{
		"destination=archive.xpack",
		"format=xpack",
		"branch=voice codec=opus media=audio",
		"branch=screen codec=vp8 media=video",
		"branches=voice,screen",
	}
	if !reflect.DeepEqual(issue.Details, wantDetails) {
		t.Fatalf("details = %#v, want %#v", issue.Details, wantDetails)
	}

	issue, ok = checkDescriptorMuxCompatibility(output, streams[:1], format.Descriptor{Media: []av.MediaType{av.MediaVideo}})
	if !ok || !strings.Contains(issue.Reason, "media=video") {
		t.Fatalf("media issue = %+v, want video-only rejection", issue)
	}
	issue, ok = checkDescriptorMuxCompatibility(output, streams[:1], format.Descriptor{Codecs: []av.CodecID{av.CodecFLAC}})
	if !ok || !strings.Contains(issue.Reason, "codecs=flac") {
		t.Fatalf("codec issue = %+v, want flac-only rejection", issue)
	}
}

func TestSingleVideoMuxCompatibilityContracts(t *testing.T) {
	output := workDestination{Name: "archive.ivf", Format: av.FormatIVF}
	codecs := map[av.CodecID]bool{av.CodecVP8: true, av.CodecVP9: true, av.CodecAV1: true}

	if issue, ok := checkSingleVideoMuxCompatibility(output, nil, codecs, "single video only"); !ok || issue.Code != errcode.DestinationMuxIncompatible {
		t.Fatalf("empty streams issue = %+v ok=%v", issue, ok)
	}
	if issue, ok := checkSingleVideoMuxCompatibility(output, []plannedMuxStream{{Branch: "unknown"}}, codecs, "single video only"); ok {
		t.Fatalf("unknown stream issue = %+v, want deferred", issue)
	}
	if issue, ok := checkSingleVideoMuxCompatibility(output, []plannedMuxStream{{Branch: "audio", Media: av.MediaAudio, Codec: av.CodecOpus}}, codecs, "single video only"); !ok || !strings.Contains(issue.Details[2], "media=audio") {
		t.Fatalf("audio issue = %+v ok=%v", issue, ok)
	}
	if issue, ok := checkSingleVideoMuxCompatibility(output, []plannedMuxStream{{Branch: "video", Media: av.MediaVideo, Codec: av.CodecH264}}, codecs, "single video only"); !ok || !strings.Contains(issue.Details[2], "codec=h264") {
		t.Fatalf("codec issue = %+v ok=%v", issue, ok)
	}
	if issue, ok := checkSingleVideoMuxCompatibility(output, []plannedMuxStream{{Branch: "video", Media: av.MediaVideo, Codec: av.CodecVP8}}, codecs, "single video only"); ok {
		t.Fatalf("compatible issue = %+v, want none", issue)
	}
}
