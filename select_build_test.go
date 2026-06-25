package goav

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// selectTestOneShotSource emits a single S16 frame on its stream then EOS, so the
// static test can assert exactly which arm's frame reaches the sink.
func selectTestOneShotSource(id av.StreamID, samples ...int16) InputSpec {
	return Source(string(id),
		shape.Frame(av.MediaAudio, shape.Audio(48000, codec.Mono, av.SampleFormatS16), shape.Stream(id)),
		func(_ context.Context, push SourcePush) error {
			b := make([]byte, len(samples)*2)
			for i := range samples {
				binary.LittleEndian.PutUint16(b[i*2:], uint16(samples[i]))
			}
			frame := &av.Frame{
				StreamID: id, Type: av.MediaAudio,
				Audio:  &av.AudioFrame{SampleRate: 48000, Channels: 1, SampleFormat: av.SampleFormatS16, Samples: len(samples)},
				Planes: []av.Plane{{Buffer: av.Buffer{Bytes: b, Ownership: av.BufferImmutable}}},
			}
			if _, err := push.Frame(frame); err != nil {
				return err
			}
			return push.EOS()
		})
}

// selectTestLiveSource floods one S16 sample on its stream until the context is
// cancelled, so both arms always have live traffic while a test flips the switch.
func selectTestLiveSource(id av.StreamID, sample int16) InputSpec {
	return Source(string(id),
		shape.Frame(av.MediaAudio, shape.Audio(48000, codec.Mono, av.SampleFormatS16), shape.Stream(id)),
		func(ctx context.Context, push SourcePush) error {
			for {
				if err := ctx.Err(); err != nil {
					return nil
				}
				b := []byte{byte(sample), byte(sample >> 8)}
				frame := &av.Frame{
					StreamID: id, Type: av.MediaAudio,
					Audio:  &av.AudioFrame{SampleRate: 48000, Channels: 1, SampleFormat: av.SampleFormatS16, Samples: 1},
					Planes: []av.Plane{{Buffer: av.Buffer{Bytes: b, Ownership: av.BufferImmutable}}},
				}
				if _, err := push.Frame(frame); err != nil {
					if errors.Is(err, pipeline.ErrBackpressure) {
						continue
					}
					return nil
				}
			}
		})
}

func TestSelectForwardsDefaultArmToSink(t *testing.T) {
	ctx := context.Background()
	var got [][]int16
	var ids []av.StreamID
	sink := Sink(SinkFunc("out", func(_ context.Context, m Message) error {
		if m.Kind == pipeline.MessageFrame && m.Frame != nil && m.Frame.Audio != nil {
			got = append(got, mixTestReadS16(m.Frame))
			ids = append(ids, m.Frame.StreamID)
		}
		return nil
	}))

	task, err := Select(
		From(selectTestOneShotSource("a", 100)).Audio(),
		From(selectTestOneShotSource("b", 200)).Audio(),
	).To(sink).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}

	// Only the default (first) arm "a" is forwarded; "b" is dropped. The output id is
	// rewritten to the selector's output stream.
	if len(got) != 1 || !reflect.DeepEqual(got[0], []int16{100}) {
		t.Fatalf("forwarded=%v, want [[100]] (only default arm a)", got)
	}
	if ids[0] != "select" {
		t.Fatalf("output stream=%s, want select (rewritten)", ids[0])
	}
}

// TestSelectSwitchesActiveArmMidRun moved to goavtest_dogfood_test.go: it is
// now the consumer-side live-switch acceptance test written against goavtest.

func TestSelectFansOutToMultipleSinks(t *testing.T) {
	ctx := context.Background()
	var first, second [][]int16
	firstSink := Sink(SinkFunc("first", func(_ context.Context, m Message) error {
		if m.Kind == pipeline.MessageFrame && m.Frame != nil && m.Frame.Audio != nil {
			first = append(first, mixTestReadS16(m.Frame))
		}
		return nil
	}))
	secondSink := Sink(SinkFunc("second", func(_ context.Context, m Message) error {
		if m.Kind == pipeline.MessageFrame && m.Frame != nil && m.Frame.Audio != nil {
			second = append(second, mixTestReadS16(m.Frame))
		}
		return nil
	}))

	task, err := Select(
		From(selectTestOneShotSource("a", 100)).Audio(),
		From(selectTestOneShotSource("b", 200)).Audio(),
	).To(firstSink, secondSink).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}

	// Both sinks receive the switched stream: only the default arm "a" forwards,
	// and each destination sees the same frames.
	if !reflect.DeepEqual(first, [][]int16{{100}}) || !reflect.DeepEqual(second, [][]int16{{100}}) {
		t.Fatalf("first=%v second=%v, want [[100]] at both sinks", first, second)
	}
}

func TestSelectRequiresTwoArms(t *testing.T) {
	_, err := Select(From(selectTestOneShotSource("a", 1)).Audio()).
		To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		Build(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "select_inputs" {
		t.Fatalf("err = %v, want select_inputs", err)
	}
}

func TestSelectRequiresDistinctArmIDs(t *testing.T) {
	_, err := Select(
		From(selectTestOneShotSource("dup", 1)).Audio(),
		From(selectTestOneShotSource("dup", 2)).Audio(),
	).To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		Build(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "select_arm" {
		t.Fatalf("err = %v, want select_arm (distinct ids enforced)", err)
	}
}

func TestSelectRequiresSinkDestination(t *testing.T) {
	_, err := Select(
		From(selectTestOneShotSource("a", 1)).Audio(),
		From(selectTestOneShotSource("b", 2)).Audio(),
	).To(Write("selected.ogg", io.Discard)).
		Build(context.Background())
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "select_destination" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want select_destination wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "goav.Sink") ||
		!strings.Contains(err.Error(), ".Branches") {
		t.Fatalf("err = %v, want sink-or-branches guidance", err)
	}
}

// selectSwitchSink records the StreamID-tagged sample of every forwarded frame and
// lets a test wait until a frame carrying a wanted sample arrives.
type selectSwitchSink struct {
	name string
	mu   sync.Mutex
	seen map[int16]struct{}
}

func newSelectSwitchSink(name string) *selectSwitchSink {
	return &selectSwitchSink{name: name, seen: make(map[int16]struct{})}
}

func (s *selectSwitchSink) Name() string { return s.name }

func (s *selectSwitchSink) Handle(_ context.Context, msg *pipeline.Message) error {
	if msg == nil || msg.Kind != pipeline.MessageFrame || msg.Frame == nil {
		return nil
	}
	got := mixTestReadS16(msg.Frame)
	if len(got) == 0 {
		return nil
	}
	s.mu.Lock()
	s.seen[got[0]] = struct{}{}
	s.mu.Unlock()
	return nil
}

func (s *selectSwitchSink) Close() error { return nil }

func (s *selectSwitchSink) waitFor(ctx context.Context, sample int16) error {
	for {
		s.mu.Lock()
		_, ok := s.seen[sample]
		s.mu.Unlock()
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

func (s *selectSwitchSink) resetSeen() {
	s.mu.Lock()
	s.seen = make(map[int16]struct{})
	s.mu.Unlock()
}
