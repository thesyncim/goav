// Root acceptance tests rewritten against goavtest — the consumer-side proof
// that pipeline tests need no internal fixtures: deterministic inputs, the
// Collector, and the goavtest runtime cover mix, live control, and
// multi-input selection end to end.

package goav_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/goavtest/expect"
)

// TestMixRunsTwoAudioSourcesIntoSink is the Mix acceptance test, consumer
// side: two deterministic sources, summed sample-by-sample into a collector.
func TestMixRunsTwoAudioSourcesIntoSink(t *testing.T) {
	ctx := context.Background()
	out := goavtest.NewCollector()
	task, err := goav.Mix(
		goav.From(goavtest.Audio(48000, 1, []int16{100, 200})).Audio(),
		goav.From(goavtest.Audio(48000, 1, []int16{50, -50})).Audio(),
	).To(out.Sink()).UseRuntime(goavtest.Runtime()).Build(ctx)
	expect.NoError(t, err)
	defer task.Close()
	expect.NoError(t, task.Run(ctx))
	expect.S16(t, out, [][]int16{{150, 150}})
}

// TestSelectSwitchesActiveArmMidRun is the live-switch acceptance test,
// consumer side: two never-ending LiveAudio arms, the control plane flips the
// active arm both ways, and the collector's Wait observes each switch land.
func TestSelectSwitchesActiveArmMidRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out := goavtest.NewCollector()
	task, err := goav.Select(
		goav.From(goavtest.LiveAudio("a", 48000, 1, []int16{100})).Audio(),
		goav.From(goavtest.LiveAudio("b", 48000, 1, []int16{200})).Audio(),
	).To(out.Sink()).UseRuntime(goavtest.Runtime()).Build(ctx)
	expect.NoError(t, err)
	t.Cleanup(func() { _ = task.Close() })

	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	// waitForSample waits until a frame with the given first sample arrives at
	// or after the start index, so each switch asserts on NEW traffic only.
	waitForSample := func(start int, sample int16) error {
		return out.Wait(ctx, func(c *goavtest.Collector) bool {
			frames := c.S16()
			for i := start; i < len(frames); i++ {
				if len(frames[i]) != 0 && frames[i][0] == sample {
					return true
				}
			}
			return false
		})
	}

	// Default active is the first arm "a" (sample 100): it must arrive first.
	if err := waitForSample(0, 100); err != nil {
		t.Fatalf("default arm a frame never forwarded: %v", err)
	}

	// Switch live to arm "b" (sample 200) through the control plane.
	expect.NoError(t, controlWhenRunning(ctx, task, goav.SelectActive("b")))
	if err := waitForSample(len(out.S16()), 200); err != nil {
		t.Fatalf("after switch, arm b frame never forwarded: %v", err)
	}

	// Switch back to "a" to prove the control plane drives the switch both ways.
	expect.NoError(t, task.Control(ctx, goav.SelectActive("a")))
	if err := waitForSample(len(out.S16()), 100); err != nil {
		t.Fatalf("after switch back, arm a frame never forwarded: %v", err)
	}

	cancel()
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v", err)
	}
}

// controlWhenRunning retries a control until the graph is running and
// accepts it — the public-API form of the internal retry helper.
func controlWhenRunning(ctx context.Context, task goav.Task, control goav.Control) error {
	for {
		err := task.Control(ctx, control)
		if err == nil || !errors.Is(err, goav.ErrControlNotRunning) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

// TestFakeDecodeFeedsRealResample pins the goavtest/std-filter seam: the
// passthrough codec's fabricated decode frame (a foreign opus packet becomes
// the deterministic 48kHz silent frame shaped by the declared stream) is a
// well-formed av.Frame the REAL std resample filter converts — fake codec,
// real conversion, one chain.
func TestFakeDecodeFeedsRealResample(t *testing.T) {
	ctx := context.Background()
	out := goavtest.NewCollector()
	input := goavtest.Packets(av.CodecOpus, av.Packet{Payload: av.Buffer{Bytes: []byte{1, 2, 3}, Ownership: av.BufferImmutable}}).
		With(goav.Codec(codec.Opus()))
	err := goav.From(input).
		Audio().Decode().
		Resample(44_100, 1).
		To(out.Sink()).
		UseRuntime(goavtest.Runtime()).
		Run(ctx)
	expect.NoError(t, err)
	frames := out.Frames()
	if len(frames) != 1 || frames[0].Audio == nil {
		t.Fatalf("frames = %d, want exactly one audio frame", len(frames))
	}
	audio := frames[0].Audio
	expect.Equal(t, "resampled sample rate", audio.SampleRate, 44_100)
	expect.Equal(t, "resampled channels", audio.Channels, 1)
	// The fake decode frame carries 480 samples of 48kHz silence; the real
	// resampler converts the duration, not the count: ~441 samples at 44.1kHz.
	if audio.Samples < 400 || audio.Samples > 480 {
		t.Fatalf("resampled samples = %d, want ≈441 (10ms at 44.1kHz)", audio.Samples)
	}
}

// TestFlowInFlowRunsEndToEnd is the flow-composition acceptance test,
// consumer side: a flow applies another flow, the composed flow applies to an
// ordinary chain, and the spliced stage really runs.
func TestFlowInFlowRunsEndToEnd(t *testing.T) {
	ctx := context.Background()
	var metered int
	meter := goav.FrameFunc("meter", func(ctx context.Context, frame *goav.Frame, emit goav.Emit) error {
		metered++
		return emit.Frame(frame)
	})
	inner := goav.Flow("inner").Audio().Do(meter)
	outer := goav.Flow("outer").Audio().Apply(inner)

	out := goavtest.NewCollector()
	err := goav.From(goavtest.Audio(48000, 1, []int16{100, 200})).
		Audio().
		Apply(outer).
		To(out.Sink()).
		UseRuntime(goavtest.Runtime()).
		Run(ctx)
	expect.NoError(t, err)
	expect.Equal(t, "metered frames", metered, 1)
	expect.S16(t, out, [][]int16{{100, 200}})
}

// The multi-input ambiguity acceptance test moved to error_acceptance_test.go
// (TestErrorAcceptanceAmbiguousStreamSelectionListsCandidates), the
// error-quality checklist this consumer-side suite feeds.
