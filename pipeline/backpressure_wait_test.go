//go:build unix

package pipeline

import (
	"context"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"
)

// slowSink takes a fixed time per message, so a DropBlock producer feeding a
// one-slot queue parks on nearly every emit.
type slowSink struct {
	name  string
	delay time.Duration
}

func (s *slowSink) Name() string { return s.name }
func (s *slowSink) Handle(context.Context, *Message) error {
	time.Sleep(s.delay)
	return nil
}
func (s *slowSink) Close() error { return nil }

// blockedGraph runs holdSource -> sink over a one-slot DropBlock queue and
// returns the captured emitter plus a stop func that ends the run.
func blockedGraph(t *testing.T, sink Sink) (Emitter, func()) {
	t.Helper()
	graph, err := NewGraph(GraphConfig{Name: "blocked", Buffer: BufferPolicy{Capacity: 1, Drop: DropBlock}})
	if err != nil {
		t.Fatal(err)
	}
	source := &holdSource{name: "src", ready: make(chan struct{}), release: make(chan struct{})}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(route("src", "sink")); err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- graph.Run(context.Background()) }()
	<-source.ready
	return source.emitter, func() {
		close(source.release)
		if err := <-runDone; err != nil {
			t.Fatal(err)
		}
		_ = graph.Close()
	}
}

// TestGraphBufferedBlockedEmitAllocs pins the DropBlock wait at zero
// allocations: a producer outpacing a slow sink parks on almost every emit and
// is woken by the worker's slot signal, with no timer allocated per wait (the
// old 100µs time.After poll allocated on every wakeup).
func TestGraphBufferedBlockedEmitAllocs(t *testing.T) {
	emitter, stop := blockedGraph(t, &slowSink{name: "sink", delay: 20 * time.Microsecond})
	defer stop()
	ctx := context.Background()
	msg := benchPacketMessage(64, true)
	if allocs := testing.AllocsPerRun(200, func() {
		if err := emitter.Emit(ctx, msg); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("blocked DropBlock emit allocs = %v, want 0", allocs)
	}
}

// TestGraphBufferedBlockedProducerIdles pins that a producer parked on a full
// DropBlock queue costs nothing while it waits: no allocations and next to no
// CPU across the window (the old poll woke 10,000 times a second), and it
// still resumes the moment the sink frees a slot.
func TestGraphBufferedBlockedProducerIdles(t *testing.T) {
	gate := make(chan struct{})
	emitter, stop := blockedGraph(t, &gateSink{name: "sink", gate: gate})
	defer stop()
	open := sync.OnceFunc(func() { close(gate) })
	defer open() // before stop, so a failed assertion cannot strand the drain
	ctx := context.Background()
	msg := benchPacketMessage(64, true)
	// One message is held in the sink's Handle and one fills the queue; the
	// third parks the producer.
	for range 2 {
		if err := emitter.Emit(ctx, msg); err != nil {
			t.Fatal(err)
		}
	}
	emitted := make(chan error, 1)
	go func() { emitted <- emitter.Emit(ctx, msg) }()
	time.Sleep(20 * time.Millisecond) // let the producer park

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	cpuBefore := processCPU(t)
	const window = 200 * time.Millisecond
	time.Sleep(window)
	cpu := processCPU(t) - cpuBefore
	runtime.ReadMemStats(&after)
	select {
	case err := <-emitted:
		t.Fatalf("producer did not block on the full queue (err = %v)", err)
	default:
	}
	if mallocs := after.Mallocs - before.Mallocs; mallocs > 64 {
		t.Fatalf("parked producer allocated %d times in %v, want none", mallocs, window)
	}
	if cpu > window/10 {
		t.Fatalf("process used %v of CPU in %v with the producer parked, want it idle", cpu, window)
	}

	open()
	select {
	case err := <-emitted:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("parked producer did not resume after the sink freed a slot")
	}
}

func processCPU(t *testing.T) time.Duration {
	t.Helper()
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		t.Fatal(err)
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}
