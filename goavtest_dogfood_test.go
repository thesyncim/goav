// Root acceptance tests rewritten against goavtest — the consumer-side proof
// that pipeline tests need no internal fixtures: deterministic inputs, the
// Collector, and the goavtest runtime cover mix, live control, and
// multi-input selection end to end.

package goav_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/codes"
	"github.com/thesyncim/goav/goavtest"
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
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if got, want := out.S16(), [][]int16{{150, 150}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("mixed = %v, want %v", got, want)
	}
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
	if err != nil {
		t.Fatal(err)
	}
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
	if err := controlWhenRunning(ctx, task, goav.SelectActive("b")); err != nil {
		t.Fatalf("SelectActive to b: %v", err)
	}
	if err := waitForSample(len(out.S16()), 200); err != nil {
		t.Fatalf("after switch, arm b frame never forwarded: %v", err)
	}

	// Switch back to "a" to prove the control plane drives the switch both ways.
	if err := task.Control(ctx, goav.SelectActive("a")); err != nil {
		t.Fatalf("SelectActive back to a: %v", err)
	}
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

// TestFromMultiInputAmbiguousSelectionListsCandidates is the multi-input
// ambiguity acceptance test, consumer side: two named goavtest inputs, one
// unnarrowed .Audio() chain, and the refusal lists every candidate with its
// input plus concrete narrowing suggestions.
func TestFromMultiInputAmbiguousSelectionListsCandidates(t *testing.T) {
	_, err := goav.From(
		goavtest.Audio(48000, 1, []int16{1}).Name("mic-a"),
		goavtest.Audio(48000, 1, []int16{1}).Name("mic-b"),
	).
		Audio().
		To(goavtest.NewCollector().Sink()).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != codes.StreamAmbiguous || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want stream_ambiguous wrapping ErrUnsupportedBuild", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, "input=mic-a") || !strings.Contains(msg, "input=mic-b") {
		t.Fatalf("err = %v, want candidates listed with their inputs", err)
	}
	if !strings.Contains(msg, `.Audio(goav.InputName("mic-a"))`) {
		t.Fatalf("err = %v, want InputName narrowing suggestion", err)
	}
	if !strings.Contains(msg, "goav.StreamID(") {
		t.Fatalf("err = %v, want StreamID narrowing suggestion", err)
	}
}
