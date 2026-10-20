package goav_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/runconfig"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/source"
)

type deferredTaskSink struct {
	entered, release chan struct{}
	failure          error
	closes           atomic.Int32
}

func (*deferredTaskSink) Name() string { return "finalizing-sink" }
func (s *deferredTaskSink) Handle(_ context.Context, msg *pipeline.Message) error {
	if msg.Frame != nil {
		close(s.entered)
		<-s.release
	}
	return nil
}
func (s *deferredTaskSink) Close() error { s.closes.Add(1); return s.failure }

func TestTaskRetainsDeferredCleanupFailure(t *testing.T) {
	rt, err := goav.New(runconfig.WithCloseWaitTimeout(5 * time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("recording finalization failed")
	sink := &deferredTaskSink{entered: make(chan struct{}), release: make(chan struct{}), failure: failure}
	input := goav.Source("audio", shape.Frame(av.MediaAudio, shape.Audio(48000, 1, av.SampleFormatS16)), func(_ context.Context, push source.Push) error {
		_, err := push.Frame(&av.Frame{Type: av.MediaAudio, Audio: &av.AudioFrame{SampleRate: 48000, Channels: 1, SampleFormat: av.SampleFormatS16, Samples: 1}, Planes: []av.Plane{{Buffer: av.Buffer{Bytes: []byte{0, 0}}}}})
		return err
	})
	task, err := rt.Build(t.Context(), input.Audio().To(goav.Sink(sink)))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- task.Run(t.Context()) }()
	<-sink.entered
	if err := task.Close(); !errors.Is(err, pipeline.ErrCloseWait) {
		t.Fatalf("first Close=%v", err)
	}
	close(sink.release)
	<-done
	deadline := time.Now().Add(time.Second)
	for task.Stats().PendingCleanup != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if task.Stats().PendingCleanup != 0 {
		t.Fatal("pending cleanup did not finish")
	}
	for range 2 {
		if err := task.Close(); !errors.Is(err, failure) {
			t.Fatalf("later task Close=%v", err)
		}
	}
	if sink.closes.Load() != 1 {
		t.Fatalf("closes=%d", sink.closes.Load())
	}
}
