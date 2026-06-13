package goav_test

import (
	"bytes"
	"context"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/shape"
)

// BenchmarkLatencyRecordPackets is the percentile harness for the canonical
// packet-record path. It records the time for each SourcePush.Packet call to be
// accepted by the graph and reports quantiles as custom benchmark metrics.
func BenchmarkLatencyRecordPackets(b *testing.B) {
	latencies := make([]int64, b.N)
	payload := bytes.Repeat([]byte{0x7a}, benchPacketBytes)
	input := goav.Source("latency-cam",
		shape.Packet(av.MediaVideo, av.CodecVP8, shape.Video(640, 360, av.PixelFormatI420)),
		func(_ context.Context, push goav.SourcePush) error {
			base := av.TimeBase{Num: 1, Den: 90_000}
			packet := av.Packet{
				Type:     av.MediaVideo,
				Payload:  av.Buffer{Bytes: payload, Ownership: av.BufferImmutable},
				Keyframe: true,
			}
			for i := 0; i < b.N; i++ {
				packet.PTS = av.Timestamp{Value: int64(i) * 3000, Base: base}
				packet.DTS = packet.PTS
				packet.Duration = av.Duration{Value: 3000, Base: base}
				start := time.Now()
				if _, err := push.Packet(&packet); err != nil {
					return err
				}
				latencies[i] = time.Since(start).Nanoseconds()
			}
			return push.EOS()
		})

	runBenchTask(b, goav.From(input).
		Video().
		Copy().
		To(benchSink("latency-packets")).
		UseRuntime(benchRuntime()))
	reportLatencyQuantiles(b, latencies)
}

// BenchmarkSustainedRecordMemory is a bounded memory-smoke harness. It reports
// live heap and runtime-reserved memory after a complete packet-record run;
// scripts/bench/perf-lab.sh wraps this with OS time output to capture RSS on
// platforms that expose it.
func BenchmarkSustainedRecordMemory(b *testing.B) {
	runtime.GC()
	runBenchTask(b, goav.From(benchVideoPackets("rss-cam", b.N)).
		Video().
		Copy().
		To(benchSink("rss-packets")).
		UseRuntime(benchRuntime()))
	runtime.GC()

	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	b.ReportMetric(float64(stats.HeapAlloc), "heap_live_B")
	b.ReportMetric(float64(stats.Sys), "runtime_sys_B")
}

// BenchmarkRealOpusEncode is the real-codec throughput path: deterministic S16
// frames into the standard Opus encoder, then packet sink.
func BenchmarkRealOpusEncode(b *testing.B) {
	runBenchTask(b, goav.From(benchAudioFrames("real-opus-in", b.N, 48_000, 1, benchAudioSamples)).
		Audio().
		Encode(codec.Opus()).
		To(benchSink("real-opus-packets")).
		UseRuntime(goav.Default(goav.WithRealtime(false))))
}

// BenchmarkRealOpusDecode decodes one payload produced by the standard Opus
// encoder, so the benchmark exercises the real decoder rather than a fixture.
func BenchmarkRealOpusDecode(b *testing.B) {
	payload := realOpusPayload(b)
	runBenchTask(b, goav.From(benchAudioPackets("real-opus-packets", b.N, payload)).
		Audio().
		Decode().
		To(benchSink("real-opus-frames")).
		UseRuntime(goav.Default(goav.WithRealtime(false))))
}

func realOpusPayload(b *testing.B) []byte {
	b.Helper()
	out := goavtest.NewCollector()
	err := goav.From(benchAudioFrames("real-opus-seed", 1, 48_000, 1, benchAudioSamples)).
		Audio().
		Encode(codec.Opus()).
		To(out.Sink()).
		UseRuntime(goav.Default(goav.WithRealtime(false))).
		Run(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	packets := out.Packets()
	if len(packets) != 1 {
		b.Fatalf("seed encode produced %d packets, want 1", len(packets))
	}
	return append([]byte(nil), packets[0].Payload.Bytes...)
}

func reportLatencyQuantiles(b *testing.B, values []int64) {
	b.Helper()
	if len(values) == 0 {
		return
	}
	sorted := append([]int64(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	b.ReportMetric(float64(percentile(sorted, 0.50)), "p50_ns")
	b.ReportMetric(float64(percentile(sorted, 0.95)), "p95_ns")
	b.ReportMetric(float64(percentile(sorted, 0.99)), "p99_ns")
}

func percentile(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(float64(len(sorted)-1) * p)
	return sorted[index]
}
