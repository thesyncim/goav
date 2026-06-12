package goav

import (
	"errors"
	"reflect"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/snapshot"
)

func TestRuntimeBranchStructuredErrorContracts(t *testing.T) {
	transformCause := errors.New("adapter missing")
	graphCause := errors.New("graph refused branch")
	packetShape := shape.Packet(av.MediaAudio, av.CodecOpus, shape.Stream("audio-main"))
	frameShape := shape.Frame(av.MediaVideo, shape.Stream("preview"))

	tests := []struct {
		name  string
		err   error
		code  errcode.Code
		cause error
	}{
		{name: "invalid branch", err: runtimeBranchInvalidError("missing source", "declare From(...)"), code: errcode.RuntimeBranchInvalid, cause: ErrUnsupportedBuild},
		{name: "anchor missing", err: runtimeBranchAnchorMissingError("old-node"), code: errcode.RuntimeBranchAnchorMissing, cause: pipeline.ErrUnknownNode},
		{
			name: "tap missing",
			err: runtimeBranchTapMissingError("preview.frames", []snapshot.Tap{
				{Name: "encoded", Domain: shape.DomainPacket, MediaKind: av.MediaVideo},
			}),
			code:  errcode.RuntimeBranchTapMissing,
			cause: pipeline.ErrUnknownNode,
		},
		{name: "node duplicate", err: runtimeBranchNodeDuplicateError("preview.sink"), code: errcode.RuntimeBranchNodeDuplicate, cause: pipeline.ErrNodeExists},
		{name: "tap duplicate", err: runtimeBranchTapDuplicateError("preview.frames"), code: errcode.RuntimeBranchTapDuplicate, cause: ErrUnsupportedBuild},
		{name: "transform media mismatch", err: runtimeBranchTransformMediaError("preview", "Resize", "video"), code: errcode.RuntimeBranchTransformMediaMismatch, cause: ErrUnsupportedBuild},
		{name: "transform open error", err: runtimeBranchTransformError("resize-preview", transformCause), code: errcode.RuntimeBranchTransformError, cause: transformCause},
		{name: "encode missing", err: runtimeBranchEncodeMissingError("archive"), code: errcode.RuntimeBranchEncodeMissing, cause: ErrUnsupportedBuild},
		{name: "encode domain mismatch", err: runtimeBranchEncodeDomainError("archive", packetShape), code: errcode.RuntimeBranchEncodeDomainMismatch, cause: ErrUnsupportedBuild},
		{name: "decode domain mismatch", err: runtimeBranchDecodeDomainError("preview", frameShape), code: errcode.RuntimeBranchDecodeDomainMismatch, cause: ErrUnsupportedBuild},
		{name: "decode codec missing", err: runtimeBranchDecodeCodecMissingError("", shape.Packet(av.MediaAudio, "")), code: errcode.RuntimeBranchDecodeCodecMissing, cause: ErrUnsupportedBuild},
		{name: "copy domain mismatch", err: runtimeBranchCopyDomainError("archive", frameShape), code: errcode.RuntimeBranchCopyDomainMismatch, cause: ErrUnsupportedBuild},
		{name: "mux codec missing", err: runtimeBranchMuxCodecMissingError("archive", shape.Packet(av.MediaVideo, "")), code: errcode.RuntimeBranchMuxCodecMissing, cause: ErrUnsupportedBuild},
		{name: "graph error", err: runtimeBranchGraphError("attach runtime branch", "archive", graphCause), code: errcode.RuntimeBranchGraphError, cause: graphCause},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertBuildErrorCode(t, tt.err, tt.code)
			if !errors.Is(tt.err, tt.cause) {
				t.Fatalf("err = %v, want cause %v", tt.err, tt.cause)
			}
			var buildErr *BuildError
			if !errors.As(tt.err, &buildErr) {
				t.Fatalf("err = %T, want *BuildError", tt.err)
			}
			if buildErr.Operation == "" {
				t.Fatal("Operation is empty")
			}
			if len(buildErr.Suggestions) == 0 {
				t.Fatal("Suggestions is empty")
			}
		})
	}

	if err := runtimeBranchTransformError("resize-preview", nil); err != nil {
		t.Fatalf("nil transform cause error = %v, want nil", err)
	}
}

func TestRuntimeBranchShapeDetails(t *testing.T) {
	got := runtimeBranchShapeDetails(shape.Packet(av.MediaAudio, av.CodecOpus, shape.Stream("audio-main")))
	want := []string{
		"domain=packet",
		"media=audio",
		"codec=opus",
		"stream=audio-main",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("shape details = %#v, want %#v", got, want)
	}

	got = runtimeBranchShapeDetails(shape.Frame(av.MediaVideo))
	want = []string{
		"domain=frame",
		"media=video",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("frame shape details = %#v, want %#v", got, want)
	}
}
