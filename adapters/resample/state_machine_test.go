package resample

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/filter"
)

func TestFlushIntoReturnsErrResultFullAndTransitionsCleanly(t *testing.T) {
	ctx := context.Background()
	resampler := newTestFilter(t, filter.ResampleConfig{SampleRate: 4, Channels: 1}, audioStream(2, 1))
	frame := audioFrame("audio", 2, 1, []int16{0, 1000})
	result := filter.Result{Frames: []av.Frame{preallocAudioFrame(8)}[:0]}

	if err := resampler.FilterInto(ctx, &frame, &result); err != nil {
		t.Fatal(err)
	}
	if resampler.state != resampleStreaming {
		t.Fatalf("state = %d, want resampleStreaming", resampler.state)
	}

	// Full result buffer while tail samples remain must return ErrResultFull
	// and keep the filter in resampleStreaming so a retry succeeds.
	full := filter.Result{}
	if err := resampler.FlushInto(ctx, &full); !errors.Is(err, filter.ErrResultFull) {
		t.Fatalf("full FlushInto err = %v, want ErrResultFull", err)
	}
	if resampler.state != resampleStreaming {
		t.Fatalf("state after ErrResultFull = %d, want resampleStreaming", resampler.state)
	}

	// Retry with capacity emits the tail and transitions to resampleFlushed.
	result.Frames = result.Frames[:0]
	if err := resampler.FlushInto(ctx, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Frames) != 1 {
		t.Fatalf("flushed frames = %d, want 1", len(result.Frames))
	}
	if resampler.state != resampleFlushed {
		t.Fatalf("state after flush = %d, want resampleFlushed", resampler.state)
	}

	// Subsequent FlushInto is a clean no-op (even with a full result).
	if err := resampler.FlushInto(ctx, &full); err != nil {
		t.Fatalf("second FlushInto err = %v, want nil", err)
	}

	// New FilterInto after flush resets the stream and returns to resampleStreaming.
	result.Frames = result.Frames[:0]
	if err := resampler.FilterInto(ctx, &frame, &result); err != nil {
		t.Fatal(err)
	}
	if resampler.state != resampleStreaming {
		t.Fatalf("state after new stream = %d, want resampleStreaming", resampler.state)
	}
	if got, want := samplesFromFrame(&result.Frames[0]), []int16{0, 500}; !s16Equal(got, want) {
		t.Fatalf("restarted samples = %v, want %v", got, want)
	}
}
