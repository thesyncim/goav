package goav

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/pipeline"
)

func TestSelectorActiveIDContracts(t *testing.T) {
	empty := newSelectorStage("empty", nil, "out")
	if got := empty.activeID(); got != "" {
		t.Fatalf("empty activeID = %q, want empty", got)
	}

	selector := newSelectorStage("select", []av.StreamID{"left", "right"}, "out")
	if got := selector.activeID(); got != "left" {
		t.Fatalf("default activeID = %q, want left", got)
	}
	if err := selector.SetActive("right"); err != nil {
		t.Fatal(err)
	}
	if got := selector.activeID(); got != "right" {
		t.Fatalf("switched activeID = %q, want right", got)
	}
	selector.activeIndex.Store(-1)
	if got := selector.activeID(); got != "" {
		t.Fatalf("negative activeID = %q, want empty", got)
	}
	selector.activeIndex.Store(99)
	if got := selector.activeID(); got != "" {
		t.Fatalf("out-of-range activeID = %q, want empty", got)
	}
}

func TestSwitchBoundaryReachedContracts(t *testing.T) {
	frame := &pipeline.Message{Kind: pipeline.MessageFrame, Frame: &av.Frame{}}
	packet := &pipeline.Message{Kind: pipeline.MessagePacket, Packet: &av.Packet{}}
	keyPacket := &pipeline.Message{Kind: pipeline.MessagePacket, Packet: &av.Packet{Keyframe: true}}
	event := &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventStats}}
	nilPacket := &pipeline.Message{Kind: pipeline.MessagePacket}

	if !switchBoundaryReached(switchNextFrame, frame) || !switchBoundaryReached(switchNextFrame, packet) {
		t.Fatal("next-frame boundary should open on frames and packets")
	}
	if switchBoundaryReached(switchNextFrame, event) {
		t.Fatal("next-frame boundary should not open on events")
	}
	if !switchBoundaryReached(switchNextKeyframe, frame) {
		t.Fatal("next-keyframe boundary should open on frames")
	}
	if switchBoundaryReached(switchNextKeyframe, packet) {
		t.Fatal("next-keyframe boundary should not open on non-keyframe packets")
	}
	if switchBoundaryReached(switchNextKeyframe, nilPacket) {
		t.Fatal("next-keyframe boundary should not open on nil packets")
	}
	if !switchBoundaryReached(switchNextKeyframe, keyPacket) {
		t.Fatal("next-keyframe boundary should open on keyframe packets")
	}
	if !switchBoundaryReached(switchBoundaryKind("unknown"), event) {
		t.Fatal("unknown boundary should default open")
	}
	if switchBoundaryReachedAt(switchMediaTime, 50*time.Millisecond, syncPacketMessage("v", 49*time.Millisecond)) {
		t.Fatal("media-time boundary should not open before target PTS")
	}
	if !switchBoundaryReachedAt(switchMediaTime, 50*time.Millisecond, syncPacketMessage("v", 50*time.Millisecond)) {
		t.Fatal("media-time boundary should open at target PTS")
	}
}

func TestAtMediaTimeRejectsNegativeBoundary(t *testing.T) {
	policy := rebranchPolicyFromOptions([]RebranchArg{SwitchAt(AtMediaTime(-time.Millisecond))})
	if policy.invalid == "" {
		t.Fatal("negative media-time boundary should mark rebranch policy invalid")
	}
}

func TestBuildErrorUnwrapContracts(t *testing.T) {
	var nilBuildErr *BuildError
	if nilBuildErr.Unwrap() != nil {
		t.Fatal("nil BuildError should unwrap to nil")
	}
	cause := errors.New("root cause")
	buildErr := &BuildError{cause: cause}
	if !errors.Is(buildErr, cause) {
		t.Fatalf("BuildError did not unwrap cause: %v", buildErr)
	}
}

func TestBuildErrorTypedDetailsAndFixes(t *testing.T) {
	buildErr := &BuildError{
		Family:    errcode.FamilyForCode(decodeAdapterMissingCode),
		Code:      decodeAdapterMissingCode,
		Operation: "build stream",
		Reason:    "decoder is missing",
		fields: []buildErrorDetail{
			{Key: "codec", Value: av.CodecOpus},
			{Key: "attempts", Value: 2},
		},
		fixes: []buildErrorFix{
			{Message: "register an Opus decoder"},
		},
		cause: errUnsupportedBuild,
	}
	if got, ok := buildErr.Detail[av.CodecID]("codec"); !ok || got != av.CodecOpus {
		t.Fatalf("codec detail = %#v, %v; want %q, true", got, ok, av.CodecOpus)
	}
	if got, ok := buildErr.Detail[int]("attempts"); !ok || got != 2 {
		t.Fatalf("attempts detail = %#v, %v; want 2, true", got, ok)
	}
	rendered := buildErr.Error()
	if !strings.Contains(rendered, "codec=opus") ||
		!strings.Contains(rendered, "attempts=2") ||
		!strings.Contains(rendered, "register an Opus decoder") {
		t.Fatalf("rendered typed BuildError = %q", rendered)
	}

	if got, ok := (&BuildError{}).Detail[av.FormatID]("format"); ok || got != "" {
		t.Fatalf("empty detail = %#v, %v; want nil, false", got, ok)
	}
}
