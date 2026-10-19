package goav

// These tests pin the public payload contract end to end (north-star §8 /
// acceptance #22): buffered branches share the producer's payload by
// reference, never a copy per branch, and a branch that writes its delivery
// takes its own reference and makes it writable, so it can corrupt neither a
// sibling branch nor the producer.

import (
	"context"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

func copyContractFrame(payload av.Buffer) av.Frame {
	return av.Frame{
		StreamID: "video",
		Type:     av.MediaVideo,
		Video:    &av.VideoFrame{Width: 1280, Height: 720, PixelFormat: av.PixelFormatI420},
		Planes: []av.Plane{{
			Buffer: payload,
			Stride: len(payload.Bytes),
		}},
	}
}

// newCopyContractTask builds the running-task harness the contract tests
// attach branches to: a waiting frame source feeding a base sink on a buffered
// graph, with a frame tap on the
// source node for the branches to hang from.
func newCopyContractTask(t *testing.T, ctx context.Context, frame *av.Frame) (LiveTask, *runtimeBranchWaitingSource) {
	t.Helper()
	source := &runtimeBranchWaitingSource{
		name:   "source",
		ready:  make(chan struct{}),
		resume: make(chan struct{}),
		msg:    pipeline.Message{Kind: pipeline.MessageFrame, Frame: frame},
	}
	base := &runtimeTestSink{name: "base"}
	graph := expertGraph(mustNew(WithBufferPolicy(pipeline.BufferPolicy{Capacity: 2})))
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []TapSnapshot{{
		Name:      "video.frames",
		MediaKind: av.MediaVideo,
		Domain:    shape.DomainFrame,
		Shape: shape.Spec{
			Domain:      shape.DomainFrame,
			MediaKind:   av.MediaVideo,
			StreamID:    "video",
			Width:       1280,
			Height:      720,
			PixelFormat: av.PixelFormatI420,
		},
		Node: "source",
	}}
	return builtTask, source
}

// copyContractMutatingSink overwrites every plane byte of the frame it is
// delivered, the legal way (its own reference, made writable), then signals
// so a sibling can assert its own view afterwards.
type copyContractMutatingSink struct {
	name    string
	mutated chan struct{}
}

func (s *copyContractMutatingSink) Name() string {
	return s.name
}

func (s *copyContractMutatingSink) Handle(_ context.Context, msg *pipeline.Message) error {
	if msg == nil || msg.Kind != pipeline.MessageFrame || msg.Frame == nil {
		return nil
	}
	own := msg.Frame.Ref()
	own.MakeWritable()
	for i := range own.Planes {
		data := own.Planes[i].Buffer.Bytes
		for j := range data {
			data[j] = 0xEE
		}
	}
	own.Release()
	close(s.mutated)
	return nil
}

func (s *copyContractMutatingSink) Close() error {
	return nil
}

// copyContractObservingSink waits until the mutating sibling has written, then
// records the frame bytes it was delivered.
type copyContractObservingSink struct {
	name  string
	wait  <-chan struct{}
	bytes []byte
}

func (s *copyContractObservingSink) Name() string {
	return s.name
}

func (s *copyContractObservingSink) Handle(ctx context.Context, msg *pipeline.Message) error {
	select {
	case <-s.wait:
	case <-ctx.Done():
		return ctx.Err()
	}
	if msg != nil && msg.Frame != nil && len(msg.Frame.Planes) != 0 {
		s.bytes = append([]byte(nil), msg.Frame.Planes[0].Buffer.Bytes...)
	}
	return nil
}

func (s *copyContractObservingSink) Close() error {
	return nil
}

// copyContractAliasSink records whether the delivered frame's first plane
// shares the producer's backing array (reference sharing) or is a copy.
type copyContractAliasSink struct {
	name     string
	original []byte
	seen     bool
	aliased  bool
	bytes    []byte
}

func (s *copyContractAliasSink) Name() string {
	return s.name
}

func (s *copyContractAliasSink) Handle(_ context.Context, msg *pipeline.Message) error {
	if msg == nil || msg.Kind != pipeline.MessageFrame || msg.Frame == nil || len(msg.Frame.Planes) == 0 {
		return nil
	}
	data := msg.Frame.Planes[0].Buffer.Bytes
	if s.seen || len(data) == 0 || len(s.original) == 0 {
		return nil
	}
	s.seen = true
	s.aliased = &data[0] == &s.original[0]
	s.bytes = append([]byte(nil), data...)
	return nil
}

func (s *copyContractAliasSink) Close() error {
	return nil
}

func equalByteValues(a []byte, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestCopyContractMutableFanoutBranchCannotCorruptSibling is acceptance #22
// through the public grammar: one pooled frame fans out to two buffered
// branches by reference; the "mutate" branch writes its delivery the legal
// way (its own reference, MakeWritable, which copies because the frame is
// shared), and the "observe" branch, which reads only after the mutation
// happened, still sees the original bytes, as does the producer.
func TestCopyContractMutableFanoutBranchCannotCorruptSibling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	original := []byte{1, 2, 3, 4}
	payload := av.NewBuffer(len(original))
	copy(payload.Bytes, original)
	frame := copyContractFrame(payload)
	contractTask, source := newCopyContractTask(t, ctx, &frame)
	defer contractTask.Close()

	mutator := &copyContractMutatingSink{name: "mutator", mutated: make(chan struct{})}
	observer := &copyContractObservingSink{name: "observer", wait: mutator.mutated}

	runErr := make(chan error, 1)
	go func() {
		runErr <- contractTask.Run(ctx)
	}()
	select {
	case <-source.ready:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	for name, sink := range map[string]pipeline.Sink{"mutate": mutator, "observe": observer} {
		if _, err := contractTask.Attach(ctx, contractTask.Tap("video.frames").
			Buffer(flow.Blocking(2)).
			As(name).
			To(Sink(sink))); err != nil {
			t.Fatal(err)
		}
	}
	close(source.resume)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if !equalByteValues(observer.bytes, original) {
		t.Fatalf("sibling branch bytes = %v, want original %v after mutate branch wrote 0xEE", observer.bytes, original)
	}
	if !equalByteValues(frame.Planes[0].Buffer.Bytes, original) {
		t.Fatalf("producer bytes = %v, want untouched original %v", frame.Planes[0].Buffer.Bytes, original)
	}
}

// TestCopyContractBranchesShareOnePayload is the "no copy per edge" half
// through the public grammar: two buffered branches read the producer's very
// bytes, ordinary or pooled.
func TestCopyContractBranchesShareOnePayload(t *testing.T) {
	for _, pooled := range []bool{false, true} {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		original := []byte{9, 8, 7, 6}
		payload := av.Buffer{Bytes: original}
		if pooled {
			payload = av.NewBuffer(len(original))
			copy(payload.Bytes, original)
		}
		frame := copyContractFrame(payload)
		contractTask, source := newCopyContractTask(t, ctx, &frame)

		left := &copyContractAliasSink{name: "left", original: payload.Bytes}
		right := &copyContractAliasSink{name: "right", original: payload.Bytes}
		runErr := make(chan error, 1)
		go func() {
			runErr <- contractTask.Run(ctx)
		}()
		select {
		case <-source.ready:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		for _, attach := range []struct {
			name   string
			buffer flow.BranchBuffer
			sink   pipeline.Sink
		}{
			{"left", flow.Blocking(2), left},
			{"right", flow.DropOldest(2), right},
		} {
			if _, err := contractTask.Attach(ctx, contractTask.Tap("video.frames").
				Buffer(attach.buffer).
				As(attach.name).
				To(Sink(attach.sink))); err != nil {
				t.Fatal(err)
			}
		}
		close(source.resume)
		if err := <-runErr; err != nil {
			t.Fatal(err)
		}
		for _, sink := range []*copyContractAliasSink{left, right} {
			if !sink.seen || !sink.aliased || !equalByteValues(sink.bytes, original) {
				t.Fatalf("pooled=%v branch %s seen=%v aliased=%v bytes=%v, want the producer's bytes by reference", pooled, sink.name, sink.seen, sink.aliased, sink.bytes)
			}
		}
		_ = contractTask.Close()
		cancel()
	}
}
