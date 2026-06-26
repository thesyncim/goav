package goav

import (
	"context"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

type customJoinBuilderStage struct {
	name string
	arms int
	seen map[av.StreamID]struct{}
}

func newCustomJoinBuilderStage(name string, arms int) *customJoinBuilderStage {
	return &customJoinBuilderStage{name: name, arms: arms, seen: make(map[av.StreamID]struct{}, arms)}
}

func (s *customJoinBuilderStage) Name() string { return s.name }

func (s *customJoinBuilderStage) InputShapes() shape.Set {
	return shape.Set{shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Mono, av.SampleFormatS16))}
}

func (s *customJoinBuilderStage) OutputShapes(shape.Spec) shape.Set {
	return shape.Set{shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Mono, av.SampleFormatS16))}
}

func (s *customJoinBuilderStage) Handle(ctx context.Context, msg *pipeline.Message, emit pipeline.Emitter) error {
	switch msg.Kind {
	case pipeline.MessageFrame:
		if msg.Frame == nil {
			return nil
		}
		frame := cloneMixTestFrame(msg.Frame)
		frame.StreamID = av.StreamID(s.name)
		return emit.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: frame})
	case pipeline.MessageEvent:
		if msg.Event == nil || msg.Event.Type != av.EventEndOfStream {
			return nil
		}
		s.seen[msg.Event.StreamID] = struct{}{}
		if len(s.seen) < s.arms {
			return nil
		}
		event := av.Event{Type: av.EventEndOfStream, StreamID: av.StreamID(s.name)}
		return emit.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageEvent, Event: &event})
	default:
		return nil
	}
}

func (s *customJoinBuilderStage) Close() error { return nil }

func TestCustomJoinBuilderTapBranchesAndJoinArmContracts(t *testing.T) {
	ctx := context.Background()
	var nilJoin *joinStream
	if arm := nilJoin.joinArm(); arm.join != nil {
		t.Fatalf("nil custom join arm = %+v, want zero", arm)
	}

	stage := newCustomJoinBuilderStage("funnel", 2)
	joined := Join("funnel", stage,
		From(mixTestAudioSource("a", 100)).Audio(),
		From(mixTestAudioSource("b", 50)).Audio(),
	).Tap(FrameTap("funnel.frames"))

	spec := joined.spec()
	if spec.kind != "funnel" ||
		spec.custom == nil ||
		spec.custom.name != "funnel" ||
		spec.custom.stage != stage ||
		len(spec.arms) != 2 ||
		len(spec.taps) != 1 ||
		spec.taps[0].name != "funnel.frames" {
		t.Fatalf("custom join spec = %+v", spec)
	}
	arm := joined.joinArm()
	if arm.join == nil ||
		arm.join.kind != "funnel" ||
		arm.join.custom == nil ||
		len(arm.join.taps) != 1 {
		t.Fatalf("custom join arm = %+v", arm.join)
	}

	var got [][]int16
	collect := Sink(SinkFunc("monitor", func(_ context.Context, msg Message) error {
		if msg.Kind == pipeline.MessageFrame && msg.Frame != nil && msg.Frame.Audio != nil {
			got = append(got, mixTestReadS16(msg.Frame))
		}
		return nil
	}))
	task, err := joined.Branches(
		Branch("monitor").From(FrameTap("funnel.frames")).To(collect),
	).BuildLive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	tap, ok := findTap(task.Taps(), "funnel.frames")
	if !ok {
		t.Fatalf("taps = %+v, want funnel.frames", task.Taps())
	}
	if tap.Domain != shape.DomainFrame || tap.MediaKind != av.MediaAudio || tap.Node != "funnel" {
		t.Fatalf("tap = %+v, want frame audio tap on funnel", tap)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("branch frames = %v, want two forwarded custom-join frames", got)
	}
	seen := make(map[int16]int, len(got))
	for i := range got {
		if len(got[i]) != 1 {
			t.Fatalf("branch frame %d = %v, want one sample", i, got[i])
		}
		seen[got[i][0]]++
	}
	if seen[100] != 1 || seen[50] != 1 {
		t.Fatalf("branch frames = %v, want forwarded custom-join frames [100] and [50]", got)
	}
}

func TestCustomJoinBranchesRejectInvalidBuilder(t *testing.T) {
	_, err := Join("bad-name", newCustomJoinBuilderStage("bad-name", 2),
		From(mixTestAudioSource("a", 100)).Audio(),
		From(mixTestAudioSource("b", 50)).Audio(),
	).Branches(
		Branch("monitor").To(branchBuilderTestSink("monitor")),
	).BuildLive(context.Background())
	assertBuildErrorCode(t, err, errcode.JoinNameInvalid)

	if _, err := customJoinProfile(nil); err == nil {
		t.Fatal("customJoinProfile(nil) = nil, want join stage error")
	} else {
		assertBuildErrorCode(t, err, errcode.JoinStageInvalid)
	}
}
