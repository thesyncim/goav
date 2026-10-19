package compile

import (
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/internal/builderr"
	"github.com/thesyncim/goav/shape"
)

func TestRuntimeMissingAndCodecIDContracts(t *testing.T) {
	var refusal *builderr.Refusal
	err := runtimeMissingError("compile job")
	if !errors.As(err, &refusal) || refusal.Code != errcode.RuntimeMissing || !errors.Is(err, builderr.ErrUnsupportedBuild) {
		t.Fatalf("runtime missing = %v, want a runtime_missing refusal caused by ErrUnsupportedBuild", err)
	}
	if got := joinCodecIDs(nil); got != "" {
		t.Fatalf("empty codec ids = %q, want empty", got)
	}
	if got := joinCodecIDs([]av.CodecID{av.CodecVP8, "", av.CodecOpus, av.CodecID("x_custom")}); got != "vp8,opus,x_custom" {
		t.Fatalf("codec ids = %q", got)
	}
}

func TestShapeAdapterSelectionErrorAndPreferenceDropReasons(t *testing.T) {
	ambiguous := &shapeAdapterSelectionError{
		Media:      av.MediaVideo,
		Needed:     shape.AllowResize(),
		Candidates: []string{"resize-a", "resize-b"},
		Cause:      errShapeAdapterAmbiguous,
	}
	if !errors.Is(ambiguous, errShapeAdapterAmbiguous) {
		t.Fatalf("ambiguous error = %v, want errShapeAdapterAmbiguous", ambiguous)
	}
	if got := ambiguous.Error(); got != errShapeAdapterAmbiguous.Error() {
		t.Fatalf("ambiguous Error() = %q", got)
	}
	if got := preferenceDropReason(ambiguous); !strings.Contains(got, "resize-a, resize-b") {
		t.Fatalf("ambiguous preference reason = %q", got)
	}
	missing := &shapeAdapterSelectionError{
		Media:  av.MediaAudio,
		Needed: shape.AllowResample(),
		Cause:  errShapeAdapterMissing,
	}
	if got := preferenceDropReason(missing); got != "no registered adapter can perform the preferred conversion" {
		t.Fatalf("missing preference reason = %q", got)
	}
	if got := preferenceDropReason(errors.New("plain")); got != "the preferred conversion cannot be planned" {
		t.Fatalf("plain preference reason = %q", got)
	}
}

func TestTransformHelperAndCustomJoinProfileContracts(t *testing.T) {
	if got := resizeModeWithDefault(""); got != filter.ResizeExact {
		t.Fatalf("default resize mode = %s, want exact", got)
	}
	if got := resizeModeWithDefault(filter.ResizeFill); got != filter.ResizeFill {
		t.Fatalf("explicit resize mode = %s, want fill", got)
	}
	if in, out := transformAdapterExpectedMedia("custom-filter"); in != "" || out != "" {
		t.Fatalf("custom transform expected media = %s/%s, want empty", in, out)
	}
	if got := transformMethodName("custom-filter"); got != "Do" {
		t.Fatalf("custom transform method = %q, want Do", got)
	}
	var refusal *builderr.Refusal
	if _, err := customJoinProfile(nil); !errors.As(err, &refusal) || refusal.Code != errcode.JoinStageInvalid {
		t.Fatalf("CustomJoinProfile(nil) = %v, want a join_stage_invalid refusal", err)
	}
}
