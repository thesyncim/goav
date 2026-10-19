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

type customJoinTestStage struct {
	name string
	arms int
	seen map[av.StreamID]struct{}
}

func newCustomJoinTestStage(name string, arms int) *customJoinTestStage {
	return &customJoinTestStage{name: name, arms: arms, seen: make(map[av.StreamID]struct{}, arms)}
}

func (s *customJoinTestStage) Name() string { return s.name }

func (s *customJoinTestStage) InputShapes() shape.Set {
	return shape.Set{shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Mono, av.SampleFormatS16))}
}

func (s *customJoinTestStage) OutputShapes(shape.Spec) shape.Set {
	return shape.Set{shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Mono, av.SampleFormatS16))}
}

func (s *customJoinTestStage) Handle(ctx context.Context, msg *pipeline.Message, emit pipeline.Emitter) error {
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

func (s *customJoinTestStage) Close() error { return nil }

func TestCustomJoinTapsAndForks(t *testing.T) {
	ctx := context.Background()

	stage := newCustomJoinTestStage("funnel", 2)
	joined := Join("funnel", stage,
		mixTestAudioSource("a", 100).Audio(),
		mixTestAudioSource("b", 50).Audio(),
	).Tap("funnel.frames")

	var got [][]int16
	collect := Sink(SinkFunc("monitor", func(_ context.Context, msg Message) error {
		if msg.Kind == pipeline.MessageFrame && msg.Frame != nil && msg.Frame.Audio != nil {
			got = append(got, mixTestReadS16(msg.Frame))
		}
		return nil
	}))
	task, err := mustNew().Build(ctx, joined.As("monitor").To(collect))
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

func TestCustomJoinForksRejectAnInvalidName(t *testing.T) {
	_, err := mustNew().Build(context.Background(), Join("bad-name", newCustomJoinTestStage("bad-name", 2),
		mixTestAudioSource("a", 100).Audio(),
		mixTestAudioSource("b", 50).Audio(),
	).As("monitor").To(branchBuilderTestSink("monitor")))
	assertBuildErrorCode(t, err, errcode.JoinNameInvalid)

	if _, err := customJoinProfile(nil); err == nil {
		t.Fatal("customJoinProfile(nil) = nil, want join stage error")
	} else {
		assertBuildErrorCode(t, err, errcode.JoinStageInvalid)
	}
}
