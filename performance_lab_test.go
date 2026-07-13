package goav_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/pipeline"
	runconfig "github.com/thesyncim/goav/runconfig"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/source"
)

// BenchmarkLatencyRecordPackets is the percentile harness for the canonical
// packet-record path. It records the time for each SourcePush.Packet call to be
// accepted by the graph and reports quantiles as custom benchmark metrics.
func BenchmarkLatencyRecordPackets(b *testing.B) {
	latencies := make([]int64, b.N)
	payload := bytes.Repeat([]byte{0x7a}, benchPacketBytes)
	input := goav.Source("latency-cam",
		shape.Packet(av.MediaVideo, av.CodecVP8, shape.Video(640, 360, av.PixelFormatI420)),
		func(_ context.Context, push source.Push) error {
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
		UseRuntime(bundle.MustNew(runconfig.WithRealtime(false))))
}

// BenchmarkRealOpusDecode decodes one payload produced by the standard Opus
// encoder, so the benchmark exercises the real decoder rather than a fixture.
func BenchmarkRealOpusDecode(b *testing.B) {
	payload := realOpusPayload(b)
	runBenchTask(b, goav.From(benchAudioPackets("real-opus-packets", b.N, payload)).
		Audio().
		Decode().
		To(benchSink("real-opus-frames")).
		UseRuntime(bundle.MustNew(runconfig.WithRealtime(false))))
}

func realOpusPayload(b *testing.B) []byte {
	b.Helper()
	out := goavtest.NewCollector()
	err := goav.From(benchAudioFrames("real-opus-seed", 1, 48_000, 1, benchAudioSamples)).
		Audio().
		Encode(codec.Opus()).
		To(out.Sink()).
		UseRuntime(bundle.MustNew(runconfig.WithRealtime(false))).
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
	policy := flow.Sync("live-room", flow.SyncTolerance(20*time.Millisecond), flow.SyncDropLate())

	ctx := context.Background()
	task, err := goav.From(
		benchLiveRoomVideoPackets("live-video", perStream, latencies, &latencyIndex, &sourceDrops),
		benchLiveRoomAudioPackets("live-audio", perStream, latencies, &latencyIndex, &sourceDrops),
	).
		Video(goav.InputName("live-video")).Sync(policy).Copy().To(goav.Sink(component.SinkFunc("live-room-video", sink.collect))).
		Audio(goav.InputName("live-audio")).Sync(policy).Copy().To(goav.Sink(component.SinkFunc("live-room-audio", sink.collect))).
		UseRuntime(benchRuntime()).
		BuildLive(ctx)
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

// BenchmarkLiveRoomAttachDetachSoak combines the two release-soak concerns D5
// cares about: A/V sync keeps packets moving while the control plane repeatedly
// attaches and detaches monitor branches from stable packet taps.
func BenchmarkLiveRoomAttachDetachSoak(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	videoTicks := make(chan int, 4096)
	audioTicks := make(chan int, 4096)
	latencies := make([]int64, maxBenchSamples(b.N*16, 256))
	var latencyIndex atomic.Int64
	var sourceDrops atomic.Uint64
	sink := &liveRoomSyncSink{}
	policy := flow.Sync("live-room-churn", flow.SyncTolerance(20*time.Millisecond), flow.SyncDropLate())

	task, err := goav.From(
		benchLiveRoomChurnVideoPackets("churn-video", videoTicks, latencies, &latencyIndex, &sourceDrops),
		benchLiveRoomChurnAudioPackets("churn-audio", audioTicks, latencies, &latencyIndex, &sourceDrops),
	).
		Video(goav.InputName("churn-video")).
		Sync(policy).
		Copy().
		Tap(goav.PacketTap("live.video.packets")).
		To(goav.Sink(component.SinkFunc("live-room-churn-video", sink.collect))).
		Audio(goav.InputName("churn-audio")).
		Sync(policy).
		Copy().
		Tap(goav.PacketTap("live.audio.packets")).
		To(goav.Sink(component.SinkFunc("live-room-churn-audio", sink.collect))).
		UseRuntime(benchRuntime(runconfig.WithBufferPolicy(pipeline.BufferPolicy{
			Capacity:        4,
			Drop:            pipeline.DropOldest,
			CopyPacketBytes: benchPacketBytes,
			CopyFrameBytes:  1,
		}))).
		BuildLive(ctx)
	if err != nil {
		b.Fatal(err)
	}

	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	b.ReportAllocs()
	b.ResetTimer()
	mediaSeq := 0
	for i := 0; i < b.N; i++ {
		mediaSeq = benchLiveRoomEmitChurnTicks(ctx, b, videoTicks, audioTicks, mediaSeq, 2)
		tap := goav.PacketTap("live.video.packets")
		if i%2 != 0 {
			tap = goav.PacketTap("live.audio.packets")
		}
		attachment, err := task.Attach(ctx, goav.Branch("live-room-monitor-"+strconv.Itoa(i)).
			From(tap).
			Copy().
			To(benchSink("live-room-monitor-sink-"+strconv.Itoa(i))))
		if err != nil {
			b.Fatal(err)
		}
		mediaSeq = benchLiveRoomEmitChurnTicks(ctx, b, videoTicks, audioTicks, mediaSeq, 2)
		if err := task.Detach(ctx, attachment); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	close(videoTicks)
	close(audioTicks)

	stats := task.Stats()
	cancel()
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, goav.ErrClosed) {
		b.Fatal(err)
	}
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
	b.ReportMetric(float64(stats.Dropped), "graph_drops")
	b.ReportMetric(float64(delivered), "delivered")
	b.ReportMetric(float64(maxDrift.Nanoseconds()), "max_drift_ns")
}

func TestPerformanceLabRecordDriftSoak(t *testing.T) {
	duration := performanceLabSoakDuration(t, "GOAV_PERF_RECORD_DRIFT_SOAK_DURATION")
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	payload := bytes.Repeat([]byte{0x5a}, benchPacketBytes)
	var packets atomic.Uint64
	start := time.Now()
	deadline := start.Add(duration)
	input := goav.Source("record-drift-wall-soak",
		shape.Packet(av.MediaVideo, av.CodecVP8, shape.Video(640, 360, av.PixelFormatI420)),
		func(ctx context.Context, push source.Push) error {
			base := av.TimeBase{Num: 1, Den: 90_000}
			packet := av.Packet{
				Type:    av.MediaVideo,
				Payload: av.Buffer{Bytes: payload, Ownership: av.BufferImmutable},
			}
			for i := 0; ; i++ {
				if i&1023 == 0 {
					if err := ctx.Err(); err != nil {
						return nil
					}
					if !time.Now().Before(deadline) {
						return push.EOS()
					}
				}
				packet.PTS = av.Timestamp{Value: int64(i) * 3000, Base: base}
				packet.DTS = packet.PTS
				packet.Duration = av.Duration{Value: 3000, Base: base}
				packet.Keyframe = i%30 == 0
				if _, err := push.Packet(&packet); err != nil {
					return err
				}
				packets.Add(1)
			}
		})

	err := goav.From(input).
		Video().
		Copy().
		To(benchSink("record-drift-wall-soak-packets")).
		UseRuntime(benchRuntime()).
		Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	packetCount := packets.Load()
	nsPerPacket := int64(0)
	if packetCount > 0 {
		nsPerPacket = elapsed.Nanoseconds() / int64(packetCount)
	}
	fmt.Printf("goav_perf_record_drift_soak duration_ns=%d packets=%d ns_per_packet=%d heap_drift_B=%d gc_cycles=%d gc_pause_total_ns=%d\n",
		elapsed.Nanoseconds(),
		packetCount,
		nsPerPacket,
		int64(after.HeapAlloc)-int64(before.HeapAlloc),
		after.NumGC-before.NumGC,
		after.PauseTotalNs-before.PauseTotalNs)
}

func TestPerformanceLabLiveRoomAttachDetachSoak(t *testing.T) {
	duration := performanceLabSoakDuration(t, "GOAV_PERF_LIVE_ROOM_CHURN_SOAK_DURATION")
	interval := performanceLabOptionalDuration(t, "GOAV_PERF_LIVE_ROOM_CHURN_INTERVAL")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	videoTicks := make(chan int, 4096)
	audioTicks := make(chan int, 4096)
	latencies := make([]int64, 1<<20)
	var latencyIndex atomic.Int64
	var sourceDrops atomic.Uint64
	sink := &liveRoomSyncSink{}
	policy := flow.Sync("live-room-churn-wall", flow.SyncTolerance(20*time.Millisecond), flow.SyncDropLate())

	task, err := goav.From(
		benchLiveRoomChurnVideoPackets("churn-wall-video", videoTicks, latencies, &latencyIndex, &sourceDrops),
		benchLiveRoomChurnAudioPackets("churn-wall-audio", audioTicks, latencies, &latencyIndex, &sourceDrops),
	).
		Video(goav.InputName("churn-wall-video")).
		Sync(policy).
		Copy().
		Tap(goav.PacketTap("live.wall.video.packets")).
		To(goav.Sink(component.SinkFunc("live-room-churn-wall-video", sink.collect))).
		Audio(goav.InputName("churn-wall-audio")).
		Sync(policy).
		Copy().
		Tap(goav.PacketTap("live.wall.audio.packets")).
		To(goav.Sink(component.SinkFunc("live-room-churn-wall-audio", sink.collect))).
		UseRuntime(benchRuntime(runconfig.WithBufferPolicy(pipeline.BufferPolicy{
			Capacity:        4,
			Drop:            pipeline.DropOldest,
			CopyPacketBytes: benchPacketBytes,
			CopyFrameBytes:  1,
		}))).
		BuildLive(ctx)
	if err != nil {
		t.Fatal(err)
	}

	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	const nameSlots = 1024
	videoBranchNames, videoSinks := liveRoomWallMonitorNames("video", nameSlots)
	audioBranchNames, audioSinks := liveRoomWallMonitorNames("audio", nameSlots)
	start := time.Now()
	deadline := start.Add(duration)
	nextOperation := start
	mediaSeq := 0
	operations := 0
	for time.Now().Before(deadline) {
		mediaSeq = testLiveRoomEmitChurnTicks(ctx, t, videoTicks, audioTicks, mediaSeq, 2)
		tap := goav.PacketTap("live.wall.video.packets")
		branchName := videoBranchNames[operations%nameSlots]
		sink := videoSinks[operations%nameSlots]
		if operations%2 != 0 {
			tap = goav.PacketTap("live.wall.audio.packets")
			branchName = audioBranchNames[operations%nameSlots]
			sink = audioSinks[operations%nameSlots]
		}
		attachment, err := task.Attach(ctx, goav.Branch(branchName).
			From(tap).
			Copy().
			To(sink))
		if err != nil {
			t.Fatal(err)
		}
		mediaSeq = testLiveRoomEmitChurnTicks(ctx, t, videoTicks, audioTicks, mediaSeq, 2)
		if err := task.Detach(ctx, attachment); err != nil {
			t.Fatal(err)
		}
		operations++
		if interval > 0 {
			nextOperation = nextOperation.Add(interval)
			if sleep := time.Until(nextOperation); sleep > 0 {
				time.Sleep(sleep)
			} else if -sleep > interval {
				nextOperation = time.Now()
			}
		}
	}
	elapsed := time.Since(start)
	close(videoTicks)
	close(audioTicks)

	stats := task.Stats()
	cancel()
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, goav.ErrClosed) {
		t.Fatal(err)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}

	used := int(latencyIndex.Load())
	if used > len(latencies) {
		used = len(latencies)
	}
	p50, p95, p99 := latencyQuantiles(latencies[:used])
	delivered, maxDrift := sink.snapshot()
	nsPerOp := int64(0)
	if operations > 0 {
		nsPerOp = elapsed.Nanoseconds() / int64(operations)
	}
	fmt.Printf("goav_perf_live_room_churn_soak duration_ns=%d operations=%d churn_interval_ns=%d ns_per_op=%d p50_ns=%d p95_ns=%d p99_ns=%d source_drops=%d sync_drops=%d graph_drops=%d delivered=%d max_drift_ns=%d\n",
		elapsed.Nanoseconds(),
		operations,
		interval.Nanoseconds(),
		nsPerOp,
		p50,
		p95,
		p99,
		sourceDrops.Load(),
		stats.DropReasons[pipeline.DropSync],
		stats.Dropped,
		delivered,
		maxDrift.Nanoseconds())
}

func liveRoomWallMonitorNames(kind string, n int) ([]string, []goav.Destination) {
	branches := make([]string, n)
	sinks := make([]goav.Destination, n)
	for i := range branches {
		suffix := kind + "-" + strconv.Itoa(i)
		branches[i] = "live-room-wall-monitor-" + suffix
		sinks[i] = benchSink("live-room-wall-monitor-sink-" + suffix)
	}
	return branches, sinks
}

func benchLiveRoomEmitChurnTicks(ctx context.Context, b *testing.B, videoTicks chan<- int, audioTicks chan<- int, start int, count int) int {
	b.Helper()
	for i := 0; i < count; i++ {
		seq := start + i
		select {
		case videoTicks <- seq:
		case <-ctx.Done():
			b.Fatal(ctx.Err())
		}
		select {
		case audioTicks <- seq:
		case <-ctx.Done():
			b.Fatal(ctx.Err())
		}
	}
	return start + count
}

func testLiveRoomEmitChurnTicks(ctx context.Context, t *testing.T, videoTicks chan<- int, audioTicks chan<- int, start int, count int) int {
	t.Helper()
	for i := 0; i < count; i++ {
		seq := start + i
		select {
		case videoTicks <- seq:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		select {
		case audioTicks <- seq:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	return start + count
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

func (s *liveRoomSyncSink) collect(_ context.Context, msg component.Message) error {
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
		func(_ context.Context, push source.Push) error {
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
		func(_ context.Context, push source.Push) error {
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

func benchLiveRoomPushPacket(push source.Push, packet *av.Packet, latencies []int64, index *atomic.Int64, drops *atomic.Uint64) error {
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

func benchLiveRoomChurnVideoPackets(name string, ticks <-chan int, latencies []int64, index *atomic.Int64, drops *atomic.Uint64) goav.InputSpec {
	payload := bytes.Repeat([]byte{0x33}, benchPacketBytes)
	return goav.Source(name,
		shape.Packet(av.MediaVideo, av.CodecVP8, shape.Video(640, 360, av.PixelFormatI420), shape.Realtime(true)),
		func(ctx context.Context, push source.Push) error {
			base := av.TimeBase{Num: 1, Den: 90_000}
			packet := av.Packet{
				StreamID: av.StreamID(name),
				Type:     av.MediaVideo,
				Payload:  av.Buffer{Bytes: payload, Ownership: av.BufferImmutable},
			}
			for i := range ticks {
				if err := ctx.Err(); err != nil {
					return nil
				}
				packet.PTS = av.Timestamp{Value: int64(i) * 1800, Base: base}
				packet.DTS = packet.PTS
				packet.Duration = av.Duration{Value: 1800, Base: base}
				packet.Keyframe = i%50 == 0
				if err := benchLiveRoomPushPacketUntilAccepted(ctx, push, &packet, latencies, index, drops); err != nil {
					return err
				}
			}
			return push.EOS()
		})
}

func benchLiveRoomChurnAudioPackets(name string, ticks <-chan int, latencies []int64, index *atomic.Int64, drops *atomic.Uint64) goav.InputSpec {
	payload := bytes.Repeat([]byte{0x55}, benchPacketBytes/8)
	return goav.Source(name,
		shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48_000, 1, av.SampleFormatS16), shape.Realtime(true)),
		func(ctx context.Context, push source.Push) error {
			base := av.TimeBase{Num: 1, Den: 48_000}
			packet := av.Packet{
				StreamID: av.StreamID(name),
				Type:     av.MediaAudio,
				Payload:  av.Buffer{Bytes: payload, Ownership: av.BufferImmutable},
				Keyframe: true,
			}
			for i := range ticks {
				if err := ctx.Err(); err != nil {
					return nil
				}
				packet.PTS = av.Timestamp{Value: int64(i) * benchAudioSamples, Base: base}
				packet.DTS = packet.PTS
				packet.Duration = av.Duration{Value: benchAudioSamples, Base: base}
				if err := benchLiveRoomPushPacketUntilAccepted(ctx, push, &packet, latencies, index, drops); err != nil {
					return err
				}
			}
			return push.EOS()
		})
}

func benchLiveRoomPushPacketUntilAccepted(ctx context.Context, push source.Push, packet *av.Packet, latencies []int64, index *atomic.Int64, drops *atomic.Uint64) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		err := benchLiveRoomPushPacket(push, packet, latencies, index, drops)
		if err == nil {
			return nil
		}
		if errors.Is(err, goav.ErrBackpressure) {
			runtime.Gosched()
			continue
		}
		if errors.Is(err, goav.ErrClosed) || errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
}

func reportLatencyQuantiles(b *testing.B, values []int64) {
	b.Helper()
	if len(values) == 0 {
		return
	}
	p50, p95, p99 := latencyQuantiles(values)
	b.ReportMetric(float64(p50), "p50_ns")
	b.ReportMetric(float64(p95), "p95_ns")
	b.ReportMetric(float64(p99), "p99_ns")
}

func latencyQuantiles(values []int64) (int64, int64, int64) {
	if len(values) == 0 {
		return 0, 0, 0
	}
	sorted := append([]int64(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return percentile(sorted, 0.50), percentile(sorted, 0.95), percentile(sorted, 0.99)
}

func performanceLabSoakDuration(t *testing.T, name string) time.Duration {
	t.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		t.Skipf("%s is not set", name)
	}
	duration, err := time.ParseDuration(raw)
	if err != nil {
		t.Fatalf("parse %s=%q: %v", name, raw, err)
	}
	if duration <= 0 {
		t.Fatalf("%s must be positive, got %s", name, raw)
	}
	return duration
}

func performanceLabOptionalDuration(t *testing.T, name string) time.Duration {
	t.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return 0
	}
	duration, err := time.ParseDuration(raw)
	if err != nil {
		t.Fatalf("parse %s=%q: %v", name, raw, err)
	}
	if duration < 0 {
		t.Fatalf("%s must be non-negative, got %s", name, raw)
	}
	return duration
}

func percentile(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(float64(len(sorted)-1) * p)
	return sorted[index]
}

func maxBenchSamples(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
