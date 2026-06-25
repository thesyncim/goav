package goav_test

import (
	"bytes"
	"context"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/pipeline"
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
		UseRuntime(bundle.MustNew(goav.WithRealtime(false))))
}

// BenchmarkRealOpusDecode decodes one payload produced by the standard Opus
// encoder, so the benchmark exercises the real decoder rather than a fixture.
func BenchmarkRealOpusDecode(b *testing.B) {
	payload := realOpusPayload(b)
	runBenchTask(b, goav.From(benchAudioPackets("real-opus-packets", b.N, payload)).
		Audio().
		Decode().
		To(benchSink("real-opus-frames")).
		UseRuntime(bundle.MustNew(goav.WithRealtime(false))))
}

func realOpusPayload(b *testing.B) []byte {
	b.Helper()
	out := goavtest.NewCollector()
	err := goav.From(benchAudioFrames("real-opus-seed", 1, 48_000, 1, benchAudioSamples)).
		Audio().
		Encode(codec.Opus()).
		To(out.Sink()).
		UseRuntime(bundle.MustNew(goav.WithRealtime(false))).
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

// BenchmarkLiveRoomSync is the live-room sync smoke: audio and video packet
// sources share one SyncPolicy, report source-push latency quantiles, expose
// sync drops through graph stats, and record delivered A/V drift at the sink.
func BenchmarkLiveRoomSync(b *testing.B) {
	perStream := b.N / 2
	if perStream < 1 {
		perStream = 1
	}
	latencies := make([]int64, perStream*2)
	var latencyIndex atomic.Int64
	var sourceDrops atomic.Uint64
	sink := &liveRoomSyncSink{}
	policy := goav.Sync("live-room", goav.SyncTolerance(20*time.Millisecond), goav.SyncDropLate())

	ctx := context.Background()
	task, err := goav.From(
		benchLiveRoomVideoPackets("live-video", perStream, latencies, &latencyIndex, &sourceDrops),
		benchLiveRoomAudioPackets("live-audio", perStream, latencies, &latencyIndex, &sourceDrops),
	).
		Video(goav.InputName("live-video")).Sync(policy).Copy().To(goav.Sink(goav.SinkFunc("live-room-video", sink.collect))).
		Audio(goav.InputName("live-audio")).Sync(policy).Copy().To(goav.Sink(goav.SinkFunc("live-room-audio", sink.collect))).
		UseRuntime(benchRuntime()).
		Build(ctx)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	if err := task.Run(ctx); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	stats := task.Stats()
	if err := task.Close(); err != nil {
		b.Fatal(err)
	}

	used := int(latencyIndex.Load())
	if used > len(latencies) {
		used = len(latencies)
	}
	reportLatencyQuantiles(b, latencies[:used])
	delivered, maxDrift := sink.snapshot()
	b.ReportMetric(float64(sourceDrops.Load()), "source_drops")
	b.ReportMetric(float64(stats.DropReasons[pipeline.DropSync]), "sync_drops")
	b.ReportMetric(float64(delivered), "delivered")
	b.ReportMetric(float64(maxDrift.Nanoseconds()), "max_drift_ns")
}

type liveRoomSyncSink struct {
	mu        sync.Mutex
	delivered uint64
	audioPTS  time.Duration
	videoPTS  time.Duration
	haveAudio bool
	haveVideo bool
	maxDrift  time.Duration
}

func (s *liveRoomSyncSink) collect(_ context.Context, msg goav.Message) error {
	if msg.Packet == nil || !msg.Packet.PTS.Base.Valid() {
		return nil
	}
	pts, ok := msg.Packet.PTS.ToDuration()
	if !ok {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch msg.Packet.Type {
	case av.MediaAudio:
		s.audioPTS = pts
		s.haveAudio = true
	case av.MediaVideo:
		s.videoPTS = pts
		s.haveVideo = true
	default:
		return nil
	}
	s.delivered++
	if s.haveAudio && s.haveVideo {
		drift := s.audioPTS - s.videoPTS
		if drift < 0 {
			drift = -drift
		}
		if drift > s.maxDrift {
			s.maxDrift = drift
		}
	}
	return nil
}

func (s *liveRoomSyncSink) snapshot() (uint64, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.delivered, s.maxDrift
}

func benchLiveRoomVideoPackets(name string, n int, latencies []int64, index *atomic.Int64, drops *atomic.Uint64) goav.InputSpec {
	payload := bytes.Repeat([]byte{0x33}, benchPacketBytes)
	return goav.Source(name,
		shape.Packet(av.MediaVideo, av.CodecVP8, shape.Video(640, 360, av.PixelFormatI420)),
		func(_ context.Context, push goav.SourcePush) error {
			base := av.TimeBase{Num: 1, Den: 90_000}
			packet := av.Packet{
				StreamID: av.StreamID(name),
				Type:     av.MediaVideo,
				Payload:  av.Buffer{Bytes: payload, Ownership: av.BufferImmutable},
			}
			for i := 0; i < n; i++ {
				packet.PTS = av.Timestamp{Value: int64(i) * 1800, Base: base}
				packet.DTS = packet.PTS
				packet.Duration = av.Duration{Value: 1800, Base: base}
				packet.Keyframe = i%50 == 0
				if err := benchLiveRoomPushPacket(push, &packet, latencies, index, drops); err != nil {
					return err
				}
			}
			return push.EOS()
		})
}

func benchLiveRoomAudioPackets(name string, n int, latencies []int64, index *atomic.Int64, drops *atomic.Uint64) goav.InputSpec {
	payload := bytes.Repeat([]byte{0x55}, benchPacketBytes/8)
	return goav.Source(name,
		shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48_000, 1, av.SampleFormatS16)),
		func(_ context.Context, push goav.SourcePush) error {
			base := av.TimeBase{Num: 1, Den: 48_000}
			packet := av.Packet{
				StreamID: av.StreamID(name),
				Type:     av.MediaAudio,
				Payload:  av.Buffer{Bytes: payload, Ownership: av.BufferImmutable},
				Keyframe: true,
			}
			for i := 0; i < n; i++ {
				packet.PTS = av.Timestamp{Value: int64(i) * benchAudioSamples, Base: base}
				packet.DTS = packet.PTS
				packet.Duration = av.Duration{Value: benchAudioSamples, Base: base}
				if err := benchLiveRoomPushPacket(push, &packet, latencies, index, drops); err != nil {
					return err
				}
			}
			return push.EOS()
		})
}

func benchLiveRoomPushPacket(push goav.SourcePush, packet *av.Packet, latencies []int64, index *atomic.Int64, drops *atomic.Uint64) error {
	start := time.Now()
	result, err := push.Packet(packet)
	if err != nil {
		return err
	}
	if result.Dropped {
		drops.Add(1)
	}
	slot := int(index.Add(1) - 1)
	if slot < len(latencies) {
		latencies[slot] = time.Since(start).Nanoseconds()
	}
	return nil
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
