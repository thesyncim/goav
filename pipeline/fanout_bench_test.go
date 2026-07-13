package pipeline

import (
	"context"
	"strconv"
	"testing"

	"github.com/thesyncim/goav/av"
)

// fanoutSizes are the 1->N fanout widths exercised by the scaling benchmarks.
// They are the P1 measurement baseline and the regression gate for P2 (remove
// global serialization) and P3 (zero-copy / independent backpressure).
//
// Run scaling sweeps with, e.g.:
//
//	go test -run XXX -bench 'Fanout' -benchmem -cpu 1,2,4,8 ./pipeline/
var fanoutSizes = []int{1, 8, 64, 512}

const fanoutPayload = 1200 // ~one RTP MTU payload

// benchCaptureSource records the emitter handed to it at Start so a benchmark
// loop can drive emit->deliver directly on the data plane.
type benchCaptureSource struct {
	name    string
	emitter Emitter
}

func (s *benchCaptureSource) Name() string { return s.name }

func (s *benchCaptureSource) Start(_ context.Context, e Emitter) error {
	s.emitter = e
	return nil
}

func (s *benchCaptureSource) Close() error { return nil }

// benchDrainSource emits n copies of msg when Run starts it; used to drive the
// buffered runner end-to-end (workers drain concurrently).
type benchDrainSource struct {
	name string
	n    int
	msg  *Message
}

func (s *benchDrainSource) Name() string { return s.name }

func (s *benchDrainSource) Start(ctx context.Context, e Emitter) error {
	for i := 0; i < s.n; i++ {
		if err := e.Emit(ctx, s.msg); err != nil {
			return err
		}
	}
	return nil
}

func (s *benchDrainSource) Close() error { return nil }

// benchSink is a stateless no-op sink so the benchmark measures the data-plane
// (routing, stats, locking, copies) rather than sink work.
type benchSink struct{ name string }

func (s *benchSink) Name() string                           { return s.name }
func (s *benchSink) Handle(context.Context, *Message) error { return nil }
func (s *benchSink) Close() error                           { return nil }

func benchPacketMessage(size int, immutable bool) *Message {
	owner := av.BufferImmutable
	if !immutable {
		owner = av.BufferBorrowed
	}
	return &Message{
		Kind: MessagePacket,
		Packet: &av.Packet{
			StreamID: "v",
			Payload: av.Buffer{
				Bytes:     make([]byte, size),
				Ownership: owner,
			},
		},
	}
}

func buildDirectFanout(b *testing.B, n int) (*benchCaptureSource, Graph) {
	b.Helper()
	g, err := NewGraph(GraphConfig{Name: "bench"})
	if err != nil {
		b.Fatal(err)
	}
	src := &benchCaptureSource{name: "src"}
	if _, err := g.AddSource(src, BufferPolicy{}); err != nil {
		b.Fatal(err)
	}
	names := make([]string, n)
	for i := 0; i < n; i++ {
		names[i] = "sink-" + strconv.Itoa(i)
		if _, err := g.AddSink(&benchSink{name: names[i]}, BufferPolicy{}); err != nil {
			b.Fatal(err)
		}
	}
	if err := g.Connect(Route{From: "src", To: names}); err != nil {
		b.Fatal(err)
	}
	if err := g.Run(context.Background()); err != nil { // captures src.emitter
		b.Fatal(err)
	}
	return src, g
}

// BenchmarkDirectFanout measures serial emit->deliver across a 1->N direct
// fanout (no copy: immutable payload). The per-message cost includes the global
// statsMu and g.mu.RLock that P2 targets.
func BenchmarkDirectFanout(b *testing.B) {
	msg := benchPacketMessage(fanoutPayload, true)
	ctx := context.Background()
	for _, n := range fanoutSizes {
		b.Run("N="+strconv.Itoa(n), func(b *testing.B) {
			src, g := buildDirectFanout(b, n)
			defer g.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := src.emitter.Emit(ctx, msg); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkDirectFanoutParallel measures concurrent producers hitting the same
// graph. With -cpu 1,2,4,8 the ns/op ratio is the parallel-scaling number: the
// global statsMu serializes every message today, so throughput is expected to
// flatten as cores increase. P2 should make this scale near-linearly.
func BenchmarkDirectFanoutParallel(b *testing.B) {
	ctx := context.Background()
	for _, n := range fanoutSizes {
		b.Run("N="+strconv.Itoa(n), func(b *testing.B) {
			src, g := buildDirectFanout(b, n)
			defer g.Close()
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				msg := benchPacketMessage(fanoutPayload, true)
				for pb.Next() {
					if err := src.emitter.Emit(ctx, msg); err != nil {
						panic(err)
					}
				}
			})
		})
	}
}

// BenchmarkBufferedFanout measures steady buffered 1->N throughput with workers
// draining concurrently. "shared" uses an immutable payload and shares the slice
// header; "copy" uses a borrowed payload with defensive per-target copies;
// "refcount" uses a borrowed payload and copies it once into graph-owned backing
// for refcounted fanout. DropOldest avoids spurious backpressure teardown so the
// baseline is stable.
func BenchmarkBufferedFanout(b *testing.B) {
	for _, mode := range []struct {
		name       string
		immutable  bool
		copyAlways bool
	}{
		{"shared", true, false},
		{"copy", false, true},
		{"refcount", false, false},
	} {
		for _, n := range fanoutSizes {
			b.Run(mode.name+"/N="+strconv.Itoa(n), func(b *testing.B) {
				runBufferedFanout(b, n, mode.immutable, mode.copyAlways)
			})
		}
	}
}

func runBufferedFanout(b *testing.B, n int, immutable bool, copyAlways bool) {
	policy := BufferPolicy{Capacity: 1024, Drop: DropOldest, CopyAlways: copyAlways}
	if !immutable {
		policy.CopyPacketBytes = fanoutPayload
	}
	msg := benchPacketMessage(fanoutPayload, immutable)
	src := &holdSource{name: "src", ready: make(chan struct{}), release: make(chan struct{})}
	g, err := NewGraph(GraphConfig{Name: "bench", Buffer: policy})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := g.AddSource(src, BufferPolicy{}); err != nil {
		b.Fatal(err)
	}
	names := make([]string, n)
	for i := 0; i < n; i++ {
		names[i] = "sink-" + strconv.Itoa(i)
		if _, err := g.AddSink(&benchSink{name: names[i]}, BufferPolicy{}); err != nil {
			b.Fatal(err)
		}
	}
	if err := g.Connect(Route{From: "src", To: names}); err != nil {
		b.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- g.Run(context.Background()) }()
	<-src.ready
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := src.emitter.Emit(context.Background(), msg); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	close(src.release)
	if err := <-runDone; err != nil {
		b.Fatal(err)
	}
	if err := g.Close(); err != nil {
		b.Fatal(err)
	}
}
