// Custom-join contract tests, consumer side: the goav.Join identity rules
// (name, stage), the in-tree uniqueness refusal, and the solver service a
// stage contract buys — all through the public API. The end-to-end symmetry
// proof (explicit decoded arms, branches, nesting, Select re-expressed) lives in
// adapterproof/join_proof_test.go.

package goav_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

const shapeConversionInsertedDiagnostic = "shape_conversion_inserted"

// funnelStage is a minimal external convergence stage: every arm frame
// forwards restamped to the join's output id, one joined EOS once the
// declared number of arms ended. Its contract pins frame-domain S16 mono
// 8kHz arms, so packet-domain callers must declare .Decode() before the
// planner applies solver conversions.
type funnelStage struct {
	name string
	arms int
	eos  map[av.StreamID]struct{}
}

func newFunnelStage(name string, arms int) *funnelStage {
	return &funnelStage{name: name, arms: arms, eos: make(map[av.StreamID]struct{}, arms)}
}

func (s *funnelStage) Name() string { return s.name }

func (s *funnelStage) InputShapes() shape.Set {
	return shape.Set{shape.Frame(av.MediaAudio, shape.Audio(8000, 1, av.SampleFormatS16))}
}

func (s *funnelStage) OutputShapes(shape.Spec) shape.Set {
	return shape.Set{shape.Frame(av.MediaAudio, shape.Audio(8000, 1, av.SampleFormatS16))}
}

func (s *funnelStage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	switch msg.Kind {
	case pipeline.MessageFrame:
		if msg.Frame == nil {
			return nil
		}
		clone := *msg.Frame
		clone.StreamID = av.StreamID(s.name)
		return emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: &clone})
	case pipeline.MessageEvent:
		if msg.Event == nil || msg.Event.Type != av.EventEndOfStream {
			return nil
		}
		s.eos[msg.Event.StreamID] = struct{}{}
		if len(s.eos) >= s.arms {
			out := av.Event{Type: av.EventEndOfStream, StreamID: av.StreamID(s.name)}
			return emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageEvent, Event: &out})
		}
		return nil
	default:
		return nil
	}
}

func (s *funnelStage) Close() error { return nil }

func buildErrorCode(t *testing.T, err error) errcode.Code {
	t.Helper()
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) {
		t.Fatalf("err = %v, want *goav.BuildError", err)
	}
	return buildErr.Code
}

// TestJoinRejectsInvalidNames pins the join-name contract: the name is the
// node name, the joined stream id, and the error-code family, so empty,
// non-snake-safe, and built-in-reserved names refuse with join_name_invalid.
func TestJoinRejectsInvalidNames(t *testing.T) {
	for _, name := range []string{"", "Cross Fade", "cross-fade", "9lives", "mix", "composite", "select"} {
		job := goav.Join(name, newFunnelStage(name, 2),
			goav.From(goavtest.Audio(8000, 1, []int16{1})).Audio(),
			goav.From(goavtest.Audio(8000, 1, []int16{2})).Audio(),
		).To(goavtest.NewCollector().Sink()).UseRuntime(goavtest.Runtime())
		_, err := job.Describe()
		if err == nil {
			t.Fatalf("Join(%q) built; want join_name_invalid", name)
		}
		if code := buildErrorCode(t, err); code != errcode.JoinNameInvalid {
			t.Fatalf("Join(%q) code = %s, want %s", name, code, errcode.JoinNameInvalid)
		}
	}
}

// TestJoinRejectsStageMismatch pins the stage identity rules: a nil stage and
// a stage whose Name() differs from the join name refuse with
// join_stage_invalid — the stage IS the node, so the names must agree.
func TestJoinRejectsStageMismatch(t *testing.T) {
	describe := func(stage pipeline.Stage) error {
		_, err := goav.Join("funnel", stage,
			goav.From(goavtest.Audio(8000, 1, []int16{1})).Audio(),
			goav.From(goavtest.Audio(8000, 1, []int16{2})).Audio(),
		).
			To(goavtest.NewCollector().Sink()).UseRuntime(goavtest.Runtime()).Describe()
		return err
	}
	err := describe(nil)
	if code := buildErrorCode(t, err); code != errcode.JoinStageInvalid {
		t.Fatalf("nil stage code = %s, want %s", code, errcode.JoinStageInvalid)
	}
	err = describe(newFunnelStage("other", 2))
	if code := buildErrorCode(t, err); code != errcode.JoinStageInvalid {
		t.Fatalf("name-mismatch code = %s, want %s", code, errcode.JoinStageInvalid)
	}
}

// TestJoinRejectsDuplicateNameInTree pins in-tree uniqueness: a custom stage
// cannot be auto-renamed like a built-in (the caller built it with its output
// id), so two custom joins of one name in the same tree refuse instead of
// silently suffixing.
func TestJoinRejectsDuplicateNameInTree(t *testing.T) {
	_, err := goav.Mix(
		goav.Join("dup", newFunnelStage("dup", 2),
			goav.From(goavtest.Audio(8000, 1, []int16{1})).Audio(),
			goav.From(goavtest.Audio(8000, 1, []int16{2})).Audio(),
		),
		goav.Join("dup", newFunnelStage("dup", 2),
			goav.From(goavtest.Audio(8000, 1, []int16{3})).Audio(),
			goav.From(goavtest.Audio(8000, 1, []int16{4})).Audio(),
		),
	).
		To(goavtest.NewCollector().Sink()).UseRuntime(goavtest.Runtime()).Describe()
	if code := buildErrorCode(t, err); code != errcode.JoinNameInvalid {
		t.Fatalf("duplicate custom name code = %s, want %s", code, errcode.JoinNameInvalid)
	}
}

// TestJoinArmErrorsCarryTheJoinsCodeFamily pins the derived error-code
// contract: a custom join named "funnel" raises funnel_inputs exactly like
// Mix raises mix_inputs — the name is the error-code family.
func TestJoinArmErrorsCarryTheJoinsCodeFamily(t *testing.T) {
	_, err := goav.Join("funnel", newFunnelStage("funnel", 1),
		goav.From(goavtest.Audio(8000, 1, []int16{1})).Audio(),
	).To(goavtest.NewCollector().Sink()).UseRuntime(goavtest.Runtime()).Describe()
	if code := buildErrorCode(t, err); code != errcode.Code("funnel_inputs") {
		t.Fatalf("one-arm join code = %s, want funnel_inputs", code)
	}
}

// TestJoinSolverConvertsMismatchedArms pins the solver service an external
// stage contract buys: a 16kHz arm against the contract's 8kHz S16 mono gets
// a real per-arm resample planned and lowered — the same arm conversions Mix
// enjoys, derived from the contract instead of the built-in table.
func TestJoinSolverConvertsMismatchedArms(t *testing.T) {
	ctx := context.Background()
	out := goavtest.NewCollector()
	job := goav.Join("funnel", newFunnelStage("funnel", 2),
		goav.From(goavtest.Audio(8000, 1, []int16{1})).Audio(),
		goav.From(goavtest.Audio(16000, 1, []int16{2, 2})).Audio(),
	).To(out.Sink()).UseRuntime(goavtest.Runtime())

	planned, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	resamples := 0
	for _, node := range planned.Nodes {
		if strings.HasPrefix(node.Name, "funnel-resample-") {
			resamples++
		}
	}
	if resamples != 1 {
		t.Fatalf("planned arm resamples = %d, want 1; nodes=%+v", resamples, planned.Nodes)
	}
	// Explain reports the inserted conversion exactly like a built-in join.
	report, err := job.Explain(ctx)
	if err != nil {
		t.Fatal(err)
	}
	inserted := false
	for _, warning := range report.Warnings {
		if warning.Code == shapeConversionInsertedDiagnostic {
			inserted = true
		}
	}
	if !inserted {
		t.Fatalf("Explain warnings = %+v, want the inserted arm conversion reported", report.Warnings)
	}
	if err := job.Run(ctx); err != nil {
		t.Fatal(err)
	}
	for _, frame := range out.Frames() {
		if frame.Audio == nil || frame.Audio.SampleRate != 8000 {
			t.Fatalf("joined frame = %+v, want every arm converted to 8kHz before the stage", frame.Audio)
		}
		if frame.StreamID != "funnel" {
			t.Fatalf("joined frame id = %s, want the join's output id", frame.StreamID)
		}
	}
	if len(out.Frames()) != 2 {
		t.Fatalf("joined frames = %d, want 2", len(out.Frames()))
	}
}
