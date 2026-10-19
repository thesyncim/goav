package goav_test

import (
	"context"
	"errors"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/goavtest"
	runconfig "github.com/thesyncim/goav/runconfig"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/source"
)

// churnSink counts the frames it receives and signals once it has seen want.
type churnSink struct {
	seen atomic.Int64
	want int64
	done chan struct{}
}

func (s *churnSink) handle(_ context.Context, msg component.Message) error {
	if msg.Frame != nil && s.seen.Add(1) == s.want {
		close(s.done)
	}
	return nil
}

// TestAttachDetachChurnPlateaus pins retired-node reclamation: a live
// buffered task attaches a resizing branch, carries frames through it, and
// detaches it, 200 times. The live heap stays flat across the cycles, and
// Stats reports exactly the nodes Describe shows: a detached branch leaves no
// queue, slots, or payload memory behind.
//
// The measurement is the live heap after full collections with nothing in
// flight. The source emits only the frames a cycle asks for, and the test
// waits for every one to be delivered, so no frame is queued or being
// allocated when the heap is read; a free-running source made the reading
// depend on queue occupancy at that instant (megabytes on a slow runner).
// Growth is a least-squares slope over several windows, so one noisy sample
// can neither pass nor fail it.
func TestAttachDetachChurnPlateaus(t *testing.T) {
	if testing.Short() {
		t.Skip("churn probe")
	}
	const width, height = 320, 240
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	requests := make(chan int)
	pushed := make(chan struct{})
	var emitted atomic.Int64
	input := goav.Source("cam",
		shape.Frame(av.MediaVideo, shape.Video(width, height, av.PixelFormatI420), shape.Realtime(true)),
		func(ctx context.Context, push source.Push) error {
			base := av.TimeBase{Num: 1, Den: 30}
			for {
				select {
				case <-ctx.Done():
					return nil
				case n := <-requests:
					for range n {
						frame := churnFrame(width, height, emitted.Load(), base)
						if _, err := push.Frame(&frame); err != nil {
							return nil
						}
						emitted.Add(1)
					}
					pushed <- struct{}{}
				}
			}
		})
	main := &churnSink{want: -1}
	rt := goavtest.Runtime(runconfig.WithBufferPolicy(flow.Blocking(16).PipelinePolicy()))
	task, err := rt.Build(ctx, input.Video().Tap("cam.frames").
		To(goav.Sink(component.SinkFunc("main", main.handle))))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	cycle := func(i int) {
		const frames = 8
		sink := &churnSink{want: frames, done: make(chan struct{})}
		attachment, err := task.Attach(ctx, task.Tap("cam.frames").
			As("monitor-"+strconv.Itoa(i)).
			Resize(160, 120).
			To(goav.Sink(component.SinkFunc("monitor-sink-"+strconv.Itoa(i), sink.handle))))
		if err != nil {
			t.Fatal(err)
		}
		requests <- frames
		<-pushed
		select {
		case <-sink.done:
		case <-time.After(10 * time.Second):
			t.Fatalf("cycle %d: branch saw %d frames, want %d", i, sink.seen.Load(), sink.want)
		}
		if err := task.Detach(ctx, attachment); err != nil {
			t.Fatal(err)
		}
		// Nothing is in flight once the main branch has every frame too.
		deadline := time.Now().Add(10 * time.Second)
		for main.seen.Load() < emitted.Load() {
			if time.Now().After(deadline) {
				t.Fatalf("cycle %d: main saw %d of %d frames", i, main.seen.Load(), emitted.Load())
			}
			time.Sleep(time.Millisecond)
		}
	}

	const warmup, window, windows = 50, 25, 6
	for i := range warmup {
		cycle(i)
	}
	var xs, ys []float64
	for w := 0; w <= windows; w++ {
		if w > 0 {
			for i := range window {
				cycle(warmup + (w-1)*window + i)
			}
		}
		xs = append(xs, float64(warmup+w*window))
		ys = append(ys, float64(liveHeap()))
	}
	slope := leastSquaresSlope(xs, ys)
	t.Logf("live heap at cycles %v: %v B; slope %.0f B/cycle", xs, ys, slope)

	stats := task.Stats()
	spec := task.Spec()
	if len(stats.Nodes) != len(spec.Nodes) {
		t.Errorf("Stats reports %d nodes, Describe shows %d: retired nodes still counted", len(stats.Nodes), len(spec.Nodes))
	}
	// A retired branch used to keep its queue and slot backing, about 2.8 MB
	// a cycle; what is left is runtime bookkeeping, a few hundred bytes.
	const budget = 4 << 10
	if slope > budget {
		t.Errorf("live heap grows %.0f B per attach/detach cycle, want at most %d: retired branches keep memory", slope, budget)
	}

	cancel()
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

// churnFrame is a fresh I420 frame, its luma filled with byte(n).
func churnFrame(width, height int, n int64, base av.TimeBase) av.Frame {
	chroma := (width / 2) * (height / 2)
	luma := make([]byte, width*height)
	for i := range luma {
		luma[i] = byte(n)
	}
	return av.Frame{
		Type:     av.MediaVideo,
		PTS:      av.Timestamp{Value: n, Base: base},
		Duration: av.Duration{Value: 1, Base: base},
		Video:    &av.VideoFrame{Width: width, Height: height, PixelFormat: av.PixelFormatI420},
		Planes: []av.Plane{
			{Buffer: av.Buffer{Bytes: luma}, Stride: width},
			{Buffer: av.Buffer{Bytes: make([]byte, chroma)}, Stride: width / 2},
			{Buffer: av.Buffer{Bytes: make([]byte, chroma)}, Stride: width / 2},
		},
	}
}

// liveHeap is the heap the last full collection found reachable. Two
// collections empty sync.Pool (primary, then victim), and FreeOSMemory forces
// a third and returns freed spans, so no recyclable block is counted.
func liveHeap() uint64 {
	runtime.GC()
	runtime.GC()
	debug.FreeOSMemory()
	sample := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(sample)
	return sample[0].Value.Uint64()
}

// leastSquaresSlope is the slope of the least-squares line through (xs, ys).
func leastSquaresSlope(xs, ys []float64) float64 {
	var mx, my float64
	for i := range xs {
		mx += xs[i]
		my += ys[i]
	}
	n := float64(len(xs))
	mx, my = mx/n, my/n
	var num, den float64
	for i := range xs {
		num += (xs[i] - mx) * (ys[i] - my)
		den += (xs[i] - mx) * (xs[i] - mx)
	}
	return num / den
}
