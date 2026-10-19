package compile

import (
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

func TestRuntimeNodeNamingContracts(t *testing.T) {
	if got := EncodeNodeName(EncodeRequest{Name: "preview"}); got != "encode-preview" {
		t.Fatalf("named encode node = %q", got)
	}
	if got := EncodeNodeName(EncodeRequest{Selector: av.StreamSelector{Name: "camera"}}); got != "encode-camera" {
		t.Fatalf("selector-name encode node = %q", got)
	}
	if got := EncodeNodeName(EncodeRequest{Selector: av.StreamSelector{ID: "v0"}}); got != "encode-v0" {
		t.Fatalf("selector-id encode node = %q", got)
	}
	if got := EncodeNodeName(EncodeRequest{Selector: av.StreamSelector{Codec: av.CodecVP8}}); got != "encode-vp8" {
		t.Fatalf("selector-codec encode node = %q", got)
	}
	if got := EncodeNodeName(EncodeRequest{Selector: av.StreamSelector{Type: av.MediaVideo}}); got != "encode-video" {
		t.Fatalf("selector-type encode node = %q", got)
	}
	if got := EncodeNodeName(EncodeRequest{Selector: av.StreamSelector{UseIndex: true, Index: 2}}); got != "encode-2" {
		t.Fatalf("selector-index encode node = %q", got)
	}
	if got := EncodeNodeName(EncodeRequest{Config: codec.EncodeConfig{Parameters: av.CodecParameters{ID: av.CodecVP9}}}); got != "encode-vp9" {
		t.Fatalf("parameter-codec encode node = %q", got)
	}
	if got := EncodeNodeName(EncodeRequest{Config: codec.EncodeConfig{Stream: av.Stream{Codec: av.CodecParameters{ID: av.CodecOpus}}}}); got != "encode-opus" {
		t.Fatalf("stream-codec encode node = %q", got)
	}
	if got := EncodeNodeName(EncodeRequest{}); got != "encode" {
		t.Fatalf("fallback encode node = %q", got)
	}

	if got := DecodeNodeName(av.StreamSelector{Name: "camera"}); got != "decode-camera" {
		t.Fatalf("selector-name decode node = %q", got)
	}
	if got := DecodeNodeName(av.StreamSelector{ID: "v0"}); got != "decode-v0" {
		t.Fatalf("selector-id decode node = %q", got)
	}
	if got := DecodeNodeName(av.StreamSelector{Codec: av.CodecH264}); got != "decode-h264" {
		t.Fatalf("selector-codec decode node = %q", got)
	}
	if got := DecodeNodeName(av.StreamSelector{Type: av.MediaAudio}); got != "decode-audio" {
		t.Fatalf("selector-type decode node = %q", got)
	}
	if got := DecodeNodeName(av.StreamSelector{UseIndex: true, Index: 1}); got != "decode-1" {
		t.Fatalf("selector-index decode node = %q", got)
	}
	if got := DecodeNodeName(av.StreamSelector{}); got != "decode" {
		t.Fatalf("fallback decode node = %q", got)
	}

	if got := SelectNodeName(av.StreamSelector{Name: "camera"}); got != "select-camera" {
		t.Fatalf("selector-name select node = %q", got)
	}
	if got := SelectNodeName(av.StreamSelector{ID: "v0"}); got != "select-v0" {
		t.Fatalf("selector-id select node = %q", got)
	}
	if got := SelectNodeName(av.StreamSelector{Codec: av.CodecVP8}); got != "select-vp8" {
		t.Fatalf("selector-codec select node = %q", got)
	}
	if got := SelectNodeName(av.StreamSelector{Type: av.MediaVideo}); got != "select-video" {
		t.Fatalf("selector-type select node = %q", got)
	}
	if got := SelectNodeName(av.StreamSelector{UseIndex: true, Index: 3}); got != "select-3" {
		t.Fatalf("selector-index select node = %q", got)
	}
	if got := SelectNodeName(av.StreamSelector{}); got != "select" {
		t.Fatalf("fallback select node = %q", got)
	}
}
