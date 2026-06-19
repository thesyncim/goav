package goav

import (
	"errors"
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
	policy := rebranchPolicyFromOptions([]RebranchOption{SwitchAt(AtMediaTime(-time.Millisecond))})
	if policy.invalid == "" {
		t.Fatal("negative media-time boundary should mark rebranch policy invalid")
	}
}

func TestBuildErrorAndCompilerPassErrorContracts(t *testing.T) {
	var nilBuildErr *BuildError
	if nilBuildErr.Unwrap() != nil {
		t.Fatal("nil BuildError should unwrap to nil")
	}
	cause := errors.New("root cause")
	buildErr := &BuildError{Cause: cause}
	if !errors.Is(buildErr, cause) {
		t.Fatalf("BuildError did not unwrap cause: %v", buildErr)
	}

	plain := errors.New("plain")
	if got := compilerPassError("compile", "lower", plain); got != plain {
		t.Fatalf("plain compiler error = %v, want original", got)
	}

	diagnostic := &BuildError{Code: errcode.StreamMissing, Reason: "has diagnostic"}
	if got := compilerPassError("compile", "lower", diagnostic); got != diagnostic {
		t.Fatalf("diagnostic compiler error = %v, want original", got)
	}

	empty := &BuildError{}
	wrapped := compilerPassError("compile recipe", "lower", empty)
	var got *BuildError
	if !errors.As(wrapped, &got) {
		t.Fatalf("wrapped = %T, want *BuildError", wrapped)
	}
	if got.Code != errcode.CompilerPassFailed ||
		got.Operation != "compile recipe" ||
		got.Reason != "recipe compiler pass failed without a diagnostic" ||
		len(got.Details) != 1 ||
		got.Details[0] != "pass=lower" ||
		!errors.Is(got, empty) {
		t.Fatalf("wrapped compiler pass error = %+v", got)
	}

	withOperation := &BuildError{Operation: "build branch"}
	wrapped = compilerPassError("compile recipe", "lower", withOperation)
	if !errors.As(wrapped, &got) || got.Operation != "build branch" {
		t.Fatalf("wrapped operation = %+v", got)
	}

	_, err := recipeIntentCompiler{passes: []recipeCompilePass{nil}}.Compile(recipeCompileState{
		operation: "compile recipe",
	})
	if !errors.As(err, &got) || got.Code != errcode.CompilerPassInvalid ||
		got.Operation != "compile recipe" ||
		!errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("nil compiler pass err = %v, want compiler_pass_invalid wrapping ErrUnsupportedBuild", err)
	}
}
