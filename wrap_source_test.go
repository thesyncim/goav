// WrapSource consumer tests: the input-decoration seam, exercised through the
// public API only. The decorator that was previously unimplementable — count
// the messages a BUILT-IN input produces without reimplementing it — wraps a
// custom source and a file input, and the node-identity pin keeps Describe()
// ≡ BuildLive() even when the decorator misreports its name.

package goav_test

import (
	"bytes"
	"context"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/pipeline"
)

// countingSource decorates any pipeline.Source: it interposes on the emitter
// to count media messages. It deliberately misreports Name() to prove the
// seam pins node identity back to the planned input node.
type countingSource struct {
	inner pipeline.Source
	count *atomic.Int64
}

func (s countingSource) Name() string { return "renamed-by-decorator" }

func (s countingSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	return s.inner.Start(ctx, countingEmitter{inner: emitter, count: s.count})
}

func (s countingSource) Close() error { return s.inner.Close() }

type countingEmitter struct {
	inner pipeline.Emitter
	count *atomic.Int64
}

func (e countingEmitter) Emit(ctx context.Context, msg *pipeline.Message) error {
	if msg != nil && (msg.Kind == pipeline.MessageFrame || msg.Kind == pipeline.MessagePacket) {
		e.count.Add(1)
	}
	return e.inner.Emit(ctx, msg)
}

// TestWrapSourceDecoratesCustomInput wraps a goavtest source with a counting
// decorator: the media flows unchanged, the decorator observes every frame,
// and the planned graph keeps the input's own node (Describe() ≡ BuildLive()
// despite the decorator's misreported name).
func TestWrapSourceDecoratesCustomInput(t *testing.T) {
	ctx := context.Background()
	var count atomic.Int64
	input := goav.WrapSource(goavtest.Audio(8000, 1, []int16{1, 2}, []int16{3, 4}),
		func(source pipeline.Source) pipeline.Source {
			return countingSource{inner: source, count: &count}
		})

	out := goavtest.NewCollector()
	job := goav.From(input).Audio().To(out.Sink()).UseRuntime(goavtest.Runtime())
	planned, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	task, err := job.BuildLive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("Describe() != BuildLive() under WrapSource:\nplanned: %+v\nbuilt:   %+v", planned, built)
	}
	for _, node := range task.Describe().Nodes {
		if node.Name == "renamed-by-decorator" {
			t.Fatal("decorator renamed the input node; WrapSource must pin node identity")
		}
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if got, want := out.S16(), [][]int16{{1, 2}, {3, 4}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("decorated output = %v, want unchanged %v", got, want)
	}
	if count.Load() != 2 {
		t.Fatalf("decorator counted %d media messages, want 2", count.Load())
	}
}

// TestWrapSourceDecoratesFileInput proves decoration over a built-in input
// that opens INTERNALLY (the previously-closed case): a fake-container file
// demuxes through the runtime's format seam, and the decorator still sees
// every packet the demux source emits.
func TestWrapSourceDecoratesFileInput(t *testing.T) {
	ctx := context.Background()
	rt := goavtest.Runtime()

	// Mux two packets into an in-memory fake container.
	var file bytes.Buffer
	err := goav.From(goavtest.Packets(av.CodecOpus,
		av.Packet{Payload: av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable}, Keyframe: true},
		av.Packet{Payload: av.Buffer{Bytes: []byte{2}, Ownership: av.BufferImmutable}},
	)).Copy().To(goav.Write("in.webm", &file)).UseRuntime(rt).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var count atomic.Int64
	input := goav.WrapSource(goav.FileInput("in.webm", bytes.NewReader(file.Bytes())),
		func(source pipeline.Source) pipeline.Source {
			return countingSource{inner: source, count: &count}
		})
	out := goavtest.NewCollector()
	if err := goav.From(input).Copy().To(out.Sink()).UseRuntime(rt).Run(ctx); err != nil {
		t.Fatal(err)
	}
	if got := len(out.Packets()); got != 2 {
		t.Fatalf("delivered packets = %d, want 2 (decoration must not alter the stream)", got)
	}
	if count.Load() != 2 {
		t.Fatalf("decorator counted %d packets from the file input, want 2", count.Load())
	}
}
