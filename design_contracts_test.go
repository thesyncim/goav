package goav_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
	sourcepkg "github.com/thesyncim/goav/source"
)

func TestSingleUseReadAndWriteRefuseSecondBuild(t *testing.T) {
	ctx := context.Background()
	rt := bundle.MustNew()

	// Create a tiny valid IVF clip in memory so the first Build opens it cleanly.
	var clip bytes.Buffer
	src := goavtest.Video(16, 16, 1)
	if err := rt.Run(ctx, src.Video().Encode(codec.VP8()).To(goav.Write("clip.ivf", &clip))); err != nil {
		t.Fatalf("seed clip: %v", err)
	}

	// Reusing goav.Read across two Builds must return a structured *BuildError,
	// while Describe and Explain must not consume the single-use handle.
	in := goav.Read("clip.ivf", bytes.NewReader(clip.Bytes()))
	out := in.Video().To(goav.Write("out1.ivf", io.Discard))
	if _, err := rt.Describe(out); err != nil {
		t.Fatalf("Describe with Read: %v", err)
	}
	if _, err := rt.Explain(ctx, out); err != nil {
		t.Fatalf("Explain with Read: %v", err)
	}
	t1, err := rt.Build(ctx, out)
	if err != nil {
		t.Fatalf("first Build with Read: %v", err)
	}
	_ = t1.Close()

	_, err = rt.Build(ctx, in.Video().To(goav.Write("out2.ivf", io.Discard)))
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) {
		t.Fatalf("second Build with same Read err = %v, want *goav.BuildError", err)
	}
	if buildErr.Family != errcode.FamilyInput {
		t.Fatalf("second Build with same Read family = %q, want %q", buildErr.Family, errcode.FamilyInput)
	}

	// Reusing goav.Write across two Builds must return a structured *BuildError.
	dst := goav.Write("shared.ivf", &bytes.Buffer{})
	t2, err := rt.Build(ctx, goav.Read("clip.ivf", bytes.NewReader(clip.Bytes())).Video().To(dst))
	if err != nil {
		t.Fatalf("first Build with Write: %v", err)
	}
	_ = t2.Close()

	_, err = rt.Build(ctx, goav.Read("clip.ivf", bytes.NewReader(clip.Bytes())).Video().To(dst))
	if !errors.As(err, &buildErr) {
		t.Fatalf("second Build with same Write err = %v, want *goav.BuildError", err)
	}
	if buildErr.Family != errcode.FamilyDestination {
		t.Fatalf("second Build with same Write family = %q, want %q", buildErr.Family, errcode.FamilyDestination)
	}
}

func TestDirectRunnerSupportsLiveControl(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rt := bundle.MustNew()
	release := make(chan struct{})
	gotControl := make(chan av.EventType, 1)

	in := goav.Source("live-src", shape.Spec{Domain: shape.DomainFrame, MediaKind: av.MediaVideo}, func(ctx context.Context, _ sourcepkg.Push) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	tapStage := &eventTapStage{name: "probe", got: gotControl}
	sink := &discardSink{name: "sink"}

	task, err := rt.Build(ctx, in.Video().Do(tapStage).Tap("obs").To(goav.Sink(sink)))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer func() { _ = task.Close() }()

	if mode := task.Spec().Execution.Mode; mode != "direct" {
		t.Fatalf("Execution.Mode = %q, want direct", mode)
	}

	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	// Retry until Run marks the direct runner active.
	for {
		err := task.Control(ctx, control.Keyframe("live-src").AtTap("obs"))
		if err == nil {
			break
		}
		if !errors.Is(err, control.ErrNotRunning) {
			t.Fatalf("direct task.Control err = %v", err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for direct task to accept control: %v", ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}

	select {
	case kind := <-gotControl:
		if kind != av.EventKeyframeRequired {
			t.Fatalf("injected event type = %q, want %q", kind, av.EventKeyframeRequired)
		}
	case <-ctx.Done():
		t.Fatalf("injected control never reached direct stage: %v", ctx.Err())
	}

	close(release)
	if err := <-runErr; err != nil {
		t.Fatalf("Run err = %v", err)
	}
}

func TestPipelineCapabilityInterfaces(t *testing.T) {
	for _, cfg := range []pipeline.GraphConfig{
		{Name: "direct"},
		{Name: "buffered", Buffer: pipeline.BufferPolicy{Capacity: 4, Drop: pipeline.DropBlock}},
	} {
		g, err := pipeline.NewGraph(cfg)
		if err != nil {
			t.Fatalf("NewGraph(%s): %v", cfg.Name, err)
		}
		defer func() { _ = g.Close() }()

		if _, ok := g.(pipeline.ContextCloser); !ok {
			t.Errorf("%s graph does not implement pipeline.ContextCloser", cfg.Name)
		}
		if _, ok := g.(pipeline.NodeRemoverContext); !ok {
			t.Errorf("%s graph does not implement pipeline.NodeRemoverContext", cfg.Name)
		}
		if _, ok := g.(pipeline.NodePauser); !ok {
			t.Errorf("%s graph does not implement pipeline.NodePauser", cfg.Name)
		}
		if _, ok := g.(pipeline.NodeInjector); !ok {
			t.Errorf("%s graph does not implement pipeline.NodeInjector", cfg.Name)
		}
		if _, ok := g.(pipeline.SourceControlProber); !ok {
			t.Errorf("%s graph does not implement pipeline.SourceControlProber", cfg.Name)
		}
	}
}

type eventTapStage struct {
	name string
	got  chan av.EventType
}

func (s *eventTapStage) Name() string { return s.name }
func (s *eventTapStage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	if msg != nil && msg.Kind == pipeline.MessageEvent && msg.Event != nil {
		select {
		case s.got <- msg.Event.Type:
		default:
		}
	}
	return emitter.Emit(ctx, msg)
}
func (s *eventTapStage) Close() error { return nil }

type discardSink struct{ name string }

func (s *discardSink) Name() string                                    { return s.name }
func (s *discardSink) Handle(context.Context, *pipeline.Message) error { return nil }
func (s *discardSink) Close() error                                    { return nil }

func TestControlEventsUseTypedPayloadsWithoutMetadataMap(t *testing.T) {
	bitrateMsg, err := control.Must(control.SetBitrate("v", 750_000)).Message()
	if err != nil {
		t.Fatal(err)
	}
	if bitrateMsg.Event.Bitrate != 750_000 || bitrateMsg.Event.Metadata != nil {
		t.Fatalf("SetBitrate event = %+v, want typed Bitrate with nil Metadata", bitrateMsg.Event)
	}
	if got, ok := codec.EventBitrate(bitrateMsg.Event); !ok || got != 750_000 {
		t.Fatalf("EventBitrate = (%d, %v), want (750000, true)", got, ok)
	}

	rateMsg, err := control.Must(control.Rate(1.5)).Message()
	if err != nil {
		t.Fatal(err)
	}
	if rateMsg.Event.Rate != 1.5 || rateMsg.Event.Metadata != nil {
		t.Fatalf("Rate event = %+v, want typed Rate with nil Metadata", rateMsg.Event)
	}
	if got, ok := av.EventRateValue(rateMsg.Event); !ok || got != 1.5 {
		t.Fatalf("EventRateValue = (%v, %v), want (1.5, true)", got, ok)
	}

	segMsg, err := control.Must(control.Segment(time.Second, 3*time.Second)).Message()
	if err != nil {
		t.Fatal(err)
	}
	if segMsg.Event.End != 3*time.Second || segMsg.Event.Metadata != nil {
		t.Fatalf("Segment event = %+v, want typed End with nil Metadata", segMsg.Event)
	}
	if got, ok := av.EventSegmentEnd(segMsg.Event); !ok || got != 3*time.Second {
		t.Fatalf("EventSegmentEnd = (%v, %v), want (3s, true)", got, ok)
	}

	selMsg, err := control.SelectActive("arm_b").Message()
	if err != nil {
		t.Fatal(err)
	}
	if selMsg.Event.Type != av.EventSelectActive || selMsg.Event.StreamID != "arm_b" {
		t.Fatalf("SelectActive event = %+v, want EventSelectActive for arm_b", selMsg.Event)
	}
}
