package goav

// These tests pin the public buffer copy-mode ownership contract end to end
// (north-star §8 / acceptance #22): a buffered branch binds every queued
// message into its own preallocated slots, so under flow.CopyIfMutable (the
// default) a branch that mutates its delivered frame bytes can corrupt neither
// a sibling branch nor the producer; flow.CopyAlways defensively copies even
// av.BufferImmutable payloads; and flow.CopyNever is safe-only — it shares by
// reference and refuses mutable payloads instead of silently sharing them.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/snapshot"
)

func copyContractFrame(ownership av.BufferOwnership, data []byte) av.Frame {
	return av.Frame{
		StreamID: "video",
		Type:     av.MediaVideo,
		Video:    &av.VideoFrame{Width: 1280, Height: 720, PixelFormat: av.PixelFormatI420},
		Planes: []av.Plane{{
			Buffer: av.Buffer{Bytes: data, Ownership: ownership},
			Stride: len(data),
		}},
	}
}

// newCopyContractTask builds the running-task harness the contract tests
// attach branches to: a waiting frame source feeding a base sink on a buffered
// graph whose default policy can copy mutable frames, with a frame tap on the
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
	graph := expertGraph(MustNew(WithBufferPolicy(pipeline.BufferPolicy{Capacity: 2, CopyFrameBytes: 64})))
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []snapshot.Tap{{
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
// delivered, then signals so a sibling can assert its own view afterwards.
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
	for i := range msg.Frame.Planes {
		data := msg.Frame.Planes[i].Buffer.Bytes
		for j := range data {
			data[j] = 0xEE
		}
	}
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
// through the public grammar: one mutable (av.BufferOwned) frame fans out to
// two buffered branches under the default flow.CopyIfMutable; the "mutate"
// branch overwrites its delivered plane bytes, and the "observe" branch —
// which reads only after the mutation happened — still sees the original
// bytes, as does the producer's backing array.
func TestCopyContractMutableFanoutBranchCannotCorruptSibling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	original := []byte{1, 2, 3, 4}
	frame := copyContractFrame(av.BufferOwned, append([]byte(nil), original...))
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
	if _, err := contractTask.Attach(ctx, Branch("mutate").
		From(FrameTap("video.frames")).
		Buffer(flow.Blocking(2, flow.BufferCopyBounds(0, 64))).
		To(Sink(mutator))); err != nil {
		t.Fatal(err)
	}
	if _, err := contractTask.Attach(ctx, Branch("observe").
		From(FrameTap("video.frames")).
		Buffer(flow.Blocking(2, flow.BufferCopyBounds(0, 64))).
		To(Sink(observer))); err != nil {
		t.Fatal(err)
	}
	close(source.resume)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if !equalByteValues(observer.bytes, original) {
		t.Fatalf("sibling branch bytes = %v, want original %v after mutate branch wrote 0xEE", observer.bytes, original)
	}
	if !equalByteValues(frame.Planes[0].Buffer.Bytes, original) {
		t.Fatalf("producer backing = %v, want untouched original %v", frame.Planes[0].Buffer.Bytes, original)
	}
}

// TestCopyContractCopyAlwaysCopiesImmutableFrames proves the CopyAlways half
// of the contract end to end: a frame declared av.BufferImmutable is shared by
// reference under the default CopyIfMutable but defensively copied into
// branch-owned backing under flow.CopyAlways.
func TestCopyContractCopyAlwaysCopiesImmutableFrames(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	original := []byte{9, 8, 7, 6}
	frame := copyContractFrame(av.BufferImmutable, original)
	contractTask, source := newCopyContractTask(t, ctx, &frame)
	defer contractTask.Close()

	defensive := &copyContractAliasSink{name: "defensive", original: original}
	shared := &copyContractAliasSink{name: "shared", original: original}

	runErr := make(chan error, 1)
	go func() {
		runErr <- contractTask.Run(ctx)
	}()
	select {
	case <-source.ready:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := contractTask.Attach(ctx, Branch("defensive").
		From(FrameTap("video.frames")).
		Buffer(flow.Blocking(2,
			flow.BufferCopyMode(flow.CopyAlways),
			flow.BufferCopyBounds(0, 64),
		)).
		To(Sink(defensive))); err != nil {
		t.Fatal(err)
	}
	if _, err := contractTask.Attach(ctx, Branch("shared").
		From(FrameTap("video.frames")).
		Buffer(flow.Blocking(2)).
		To(Sink(shared))); err != nil {
		t.Fatal(err)
	}
	close(source.resume)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if !defensive.seen || defensive.aliased {
		t.Fatalf("CopyAlways branch seen=%v aliased=%v, want a defensive copy of the immutable frame", defensive.seen, defensive.aliased)
	}
	if !equalByteValues(defensive.bytes, original) {
		t.Fatalf("CopyAlways branch bytes = %v, want %v", defensive.bytes, original)
	}
	if !shared.seen || !shared.aliased {
		t.Fatalf("CopyIfMutable branch seen=%v aliased=%v, want the immutable frame shared by reference", shared.seen, shared.aliased)
	}
}

// TestCopyContractCopyNeverIsSafeOnly pins both halves of the CopyNever
// contract: a mutable payload meeting a CopyNever branch is refused with a
// goav-structured error that still wraps pipeline.ErrBufferedMessageUnsafe
// (never silently shared), while an immutable payload flows by reference
// without any copy.
func TestCopyContractCopyNeverIsSafeOnly(t *testing.T) {
	t.Run("mutable payload is refused", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		frame := copyContractFrame(av.BufferOwned, []byte{1, 2, 3, 4})
		contractTask, source := newCopyContractTask(t, ctx, &frame)
		defer contractTask.Close()

		readonly := &copyContractAliasSink{name: "readonly", original: frame.Planes[0].Buffer.Bytes}
		runErr := make(chan error, 1)
		go func() {
			runErr <- contractTask.Run(ctx)
		}()
		select {
		case <-source.ready:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if _, err := contractTask.Attach(ctx, Branch("readonly").
			From(FrameTap("video.frames")).
			Buffer(flow.Latest(flow.BufferCopyMode(flow.CopyNever))).
			To(Sink(readonly))); err != nil {
			t.Fatal(err)
		}
		close(source.resume)
		err := <-runErr
		var buildErr *BuildError
		if !errors.As(err, &buildErr) || buildErr.Code != errcode.BufferPayloadUnsafe || !errors.Is(err, pipeline.ErrBufferedMessageUnsafe) {
			t.Fatalf("run err = %v, want buffer_payload_unsafe wrapping ErrBufferedMessageUnsafe", err)
		}
		if got, ok := buildErr.Detail("cause"); !ok || got != "pipeline.ErrBufferedMessageUnsafe" {
			t.Fatalf("cause detail = %#v, %v; want pipeline.ErrBufferedMessageUnsafe, true", got, ok)
		}
		if got, ok := buildErr.Detail("copy_never_branches"); !ok || got != "readonly" {
			t.Fatalf("copy_never_branches detail = %#v, %v; want readonly, true", got, ok)
		}
		for _, want := range []string{
			"copy_never_branches=readonly",
			"flow.BufferCopyBounds(packetBytes, frameBytes)",
			"flow.CopyNever",
			"av.BufferImmutable",
		} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("run err = %v, want %q", err, want)
			}
		}
		if readonly.seen {
			t.Fatal("CopyNever branch received a mutable payload it should have refused")
		}
	})

	t.Run("immutable payload shares by reference", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		original := []byte{4, 3, 2, 1}
		frame := copyContractFrame(av.BufferImmutable, original)
		contractTask, source := newCopyContractTask(t, ctx, &frame)
		defer contractTask.Close()

		readonly := &copyContractAliasSink{name: "readonly", original: original}
		runErr := make(chan error, 1)
		go func() {
			runErr <- contractTask.Run(ctx)
		}()
		select {
		case <-source.ready:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if _, err := contractTask.Attach(ctx, Branch("readonly").
			From(FrameTap("video.frames")).
			Buffer(flow.Latest(flow.BufferCopyMode(flow.CopyNever))).
			To(Sink(readonly))); err != nil {
			t.Fatal(err)
		}
		close(source.resume)
		if err := <-runErr; err != nil {
			t.Fatal(err)
		}
		if !readonly.seen || !readonly.aliased {
			t.Fatalf("CopyNever branch seen=%v aliased=%v, want the immutable frame shared by reference", readonly.seen, readonly.aliased)
		}
		if !equalByteValues(readonly.bytes, original) {
			t.Fatalf("CopyNever branch bytes = %v, want %v", readonly.bytes, original)
		}
	})
}

func TestCopyContractTooSmallBoundsAreStructured(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	frame := copyContractFrame(av.BufferOwned, []byte{1, 2, 3, 4})
	contractTask, source := newCopyContractTask(t, ctx, &frame)
	defer contractTask.Close()

	readonly := &copyContractAliasSink{name: "tiny", original: frame.Planes[0].Buffer.Bytes}
	runErr := make(chan error, 1)
	go func() {
		runErr <- contractTask.Run(ctx)
	}()
	select {
	case <-source.ready:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := contractTask.Attach(ctx, Branch("tiny").
		From(FrameTap("video.frames")).
		Buffer(flow.Latest(flow.BufferCopyBounds(0, 1))).
		To(Sink(readonly))); err != nil {
		t.Fatal(err)
	}
	close(source.resume)
	err := <-runErr
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != errcode.BufferPayloadTooLarge || !errors.Is(err, pipeline.ErrMessageTooLarge) {
		t.Fatalf("run err = %v, want buffer_payload_too_large wrapping ErrMessageTooLarge", err)
	}
	if got, ok := buildErr.Detail("cause"); !ok || got != "pipeline.ErrMessageTooLarge" {
		t.Fatalf("cause detail = %#v, %v; want pipeline.ErrMessageTooLarge, true", got, ok)
	}
	for _, want := range []string{
		"pipeline.ErrMessageTooLarge",
		"flow.BufferCopyBounds(packetBytes, frameBytes)",
		"CopyFrameBytes",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("run err = %v, want %q", err, want)
		}
	}
	if readonly.seen {
		t.Fatal("branch received a payload larger than its copy bounds")
	}
}
