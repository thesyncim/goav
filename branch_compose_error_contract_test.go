package goav

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/plan"
)

func TestBranchComposeStructuredErrorContracts(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code errcode.Code
	}{
		{name: "empty plan", err: branchComposePlanEmptyError("branches"), code: errcode.BranchComposePlanEmpty},
		{name: "codec change conflict", err: branchComposeCodecChangeConflictError("main", "backup"), code: errcode.DecodePolicyConflict},
		{name: "duplicate branch", err: branchComposeDuplicateBranchError("preview", 2), code: errcode.BranchDuplicate},
		{name: "branch chain step", err: branchChainStepError("preview", "stage and resize were grouped"), code: errcode.BranchOperationChainUnsupported},
		{
			name: "media mismatch",
			err: mediaTransformMismatchError(
				mediaTransform{name: "resize-preview"},
				av.Stream{ID: "audio", Type: av.MediaAudio, Codec: av.CodecParameters{Type: av.MediaAudio}},
				"resize",
				"video",
			),
			code: errcode.BranchTransformMediaMismatch,
		},
		{
			name: "unmatched target",
			err: branchComposeTargetUnmatchedError(
				branchComposeTarget{Name: "mobile", Branches: []string{"preview"}},
				format.Output{Name: "mobile.webm"},
			),
			code: errcode.BranchDestinationUnmatched,
		},
		{
			name: "invalid target destination",
			err:  branchComposeTargetDestinationInvalidError(branchComposeTarget{Name: "mixed"}, "target cannot configure both a sink and a mux destination"),
			code: errcode.BranchDestinationInvalid,
		},
		{
			name: "encode missing",
			err: branchComposeTargetEncodeMissingError(
				branchComposeTarget{Name: "archive"},
				format.Output{Name: "archive.webm"},
				branchComposeRoute{name: "preview"},
			),
			code: errcode.EncodeMissing,
		},
		{
			name: "missing branch destination",
			err: branchIntentDestinationMissingError(streamIntent{
				Name:   "preview",
				Select: plan.StreamSelect{Type: av.MediaVideo},
			}),
			code: errcode.DestinationMissing,
		},
		{
			name: "empty destination name",
			err: branchDestinationNameEmptyError(streamBuild{
				name:     "preview",
				selector: av.StreamSelector{Type: av.MediaVideo},
			}, 3),
			code: errcode.DestinationInvalid,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertBuildErrorCode(t, tt.err, tt.code)
			if !errors.Is(tt.err, ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want ErrUnsupportedBuild cause", tt.err)
			}
		})
	}
}

func TestBranchComposeTargetNamesAndResizeContracts(t *testing.T) {
	sink := SinkFunc("named-sink", func(context.Context, Message) error { return nil })
	if got := branchComposeTargetNodeName(branchComposeTarget{Name: "explicit"}, "fallback"); got != "explicit" {
		t.Fatalf("target node name = %q, want explicit", got)
	}
	if got := branchComposeTargetNodeName(branchComposeTarget{Sink: sink}, "fallback"); got != "named-sink" {
		t.Fatalf("sink target node name = %q, want named-sink", got)
	}
	if got := branchComposeTargetNodeName(branchComposeTarget{}, "fallback"); got != "fallback" {
		t.Fatalf("fallback target node name = %q, want fallback", got)
	}
	if got := branchComposeTargetSinkNodeName(branchComposeTargetRoute{sink: sink}, 0); got != "named-sink" {
		t.Fatalf("sink route node name = %q, want named-sink", got)
	}
	if got := branchComposeTargetSinkNodeName(branchComposeTargetRoute{output: branchComposeTarget{Name: "declared"}}, 1); got != "declared" {
		t.Fatalf("declared route node name = %q, want declared", got)
	}
	if got := branchComposeTargetSinkNodeName(branchComposeTargetRoute{}, 0); got != "sink" {
		t.Fatalf("first anonymous sink node name = %q, want sink", got)
	}
	if got := branchComposeTargetSinkNodeName(branchComposeTargetRoute{}, 2); got != "sink-2" {
		t.Fatalf("later anonymous sink node name = %q, want sink-2", got)
	}
	if got := runtimeBranchComposeBranchName(branchComposeBranch{Name: "named"}, 0, 3); got != "named" {
		t.Fatalf("runtime branch name = %q, want named", got)
	}
	if got := runtimeBranchComposeBranchName(branchComposeBranch{}, 0, 1); got != "branch" {
		t.Fatalf("single runtime branch name = %q, want branch", got)
	}
	if got := runtimeBranchComposeBranchName(branchComposeBranch{}, 1, 3); got != "branch-2" {
		t.Fatalf("indexed runtime branch name = %q, want branch-2", got)
	}

	stream := av.Stream{ID: "preview", Type: av.MediaVideo, Codec: av.CodecParameters{Type: av.MediaVideo, Width: 640, Height: 360}}
	if err := applyResizeConfigToStream(&stream, filter.ResizeConfig{Width: 320, Height: 180}); err != nil {
		t.Fatal(err)
	}
	if stream.Codec.Width != 320 || stream.Codec.Height != 180 {
		t.Fatalf("exact resize stream = %+v, want 320x180", stream.Codec)
	}
	if err := applyResizeConfigToStream(&stream, filter.ResizeConfig{Mode: filter.ResizePassthrough, Width: 10, Height: 10}); err != nil {
		t.Fatal(err)
	}
	if stream.Codec.Width != 320 || stream.Codec.Height != 180 {
		t.Fatalf("passthrough resize stream = %+v, want unchanged 320x180", stream.Codec)
	}
	if err := applyResizeConfigToStream(&stream, filter.ResizeConfig{Mode: filter.ResizeFit, Width: 100, Height: 100}); err != nil {
		t.Fatal(err)
	}
	if stream.Codec.Width != 100 || stream.Codec.Height != 56 {
		t.Fatalf("fit resize stream = %+v, want 100x56", stream.Codec)
	}
	if err := applyResizeConfigToStream(&stream, filter.ResizeConfig{Mode: filter.ResizeFill, Width: 50, Height: 25}); err != nil {
		t.Fatal(err)
	}
	if stream.Codec.Width != 50 || stream.Codec.Height != 25 {
		t.Fatalf("fill resize stream = %+v, want 50x25", stream.Codec)
	}

	for _, config := range []filter.ResizeConfig{
		{Mode: filter.ResizeFit, Width: 0, Height: 100},
		{Mode: filter.ResizeFit, Width: 1, Height: 1},
		{Mode: filter.ResizeFill, Width: 0, Height: 100},
		{Mode: filter.ResizeMode("stretch"), Width: 100, Height: 100},
	} {
		bad := av.Stream{ID: "bad", Type: av.MediaVideo, Codec: av.CodecParameters{Type: av.MediaVideo, Width: 0, Height: 0}}
		err := applyResizeConfigToStream(&bad, config)
		assertBuildErrorCode(t, err, errcode.TranscodeResizeInvalid)
	}
	tinyFit := av.Stream{ID: "tiny", Type: av.MediaVideo, Codec: av.CodecParameters{Type: av.MediaVideo, Width: 640, Height: 360}}
	err := applyResizeConfigToStream(&tinyFit, filter.ResizeConfig{Mode: filter.ResizeFit, Width: 1, Height: 1})
	assertBuildErrorCode(t, err, errcode.TranscodeResizeInvalid)

	if w, h := resizeFitStreamDimensions(1920, 1080, 640, 480); w != 640 || h != 360 {
		t.Fatalf("fit 16:9 into 4:3 = %dx%d, want 640x360", w, h)
	}
	if w, h := resizeFitStreamDimensions(800, 1200, 640, 480); w != 320 || h != 480 {
		t.Fatalf("fit portrait into 4:3 = %dx%d, want 320x480", w, h)
	}
	if got := evenStreamDimension(1); got != 0 {
		t.Fatalf("evenStreamDimension(1) = %d, want 0", got)
	}
	if got := evenStreamDimension(7); got != 6 {
		t.Fatalf("evenStreamDimension(7) = %d, want 6", got)
	}
}
