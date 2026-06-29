package goav_test

import (
	"context"
	"errors"
	"runtime"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/goavtest"
	goavruntime "github.com/thesyncim/goav/runtime"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/source"
)

// These are the reproducible artifacts behind real per-codec throughput claims:
// deterministic I420 frames through each standard encoder (and a captured real
// payload back through the decoder), offline-paced for stable measurement. Run
// with -bench to release artifacts; PERFORMANCE.md states what they do and do
// not prove.

const benchVideoW, benchVideoH = 128, 96

func benchRealCodecRuntime() *goav.Runtime {
	return bundle.MustNew(goavruntime.WithRealtime(false))
}

func benchRealVideoEncode(b *testing.B, spec codec.CodecSpec) {
	job := goav.From(benchVideoFrames("real-video-in", b.N, benchVideoW, benchVideoH)).
		Video().
		Encode(spec).
		To(benchSink("real-video-packets")).
		UseRuntime(benchRealCodecRuntime())
	task, err := job.BuildLive(context.Background())
	if err != nil {
		// The bundle ships some codecs decode-only (for example H.264). Skip with
		// the honest reason instead of failing — there is no encoder to measure.
		// A missing encoder surfaces as FamilyEncode (encoder stage) or
		// FamilyCodec (encoder adapter not registered).
		var buildErr *goav.BuildError
		if errors.As(err, &buildErr) &&
			(buildErr.Family == errcode.FamilyEncode || buildErr.Family == errcode.FamilyCodec) {
			b.Skipf("no bundled encoder for this codec: %v", err)
		}
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	if err := task.Run(context.Background()); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	if err := task.Close(); err != nil {
		b.Fatal(err)
	}
}

// Real video codec encode throughput.
func BenchmarkRealVP8Encode(b *testing.B)  { benchRealVideoEncode(b, codec.VP8()) }
func BenchmarkRealVP9Encode(b *testing.B)  { benchRealVideoEncode(b, codec.VP9()) }
func BenchmarkRealH264Encode(b *testing.B) { benchRealVideoEncode(b, codec.H264()) }
func BenchmarkRealAV1Encode(b *testing.B)  { benchRealVideoEncode(b, codec.AV1()) }

// BenchmarkRealVP8Decode decodes a real VP8 keyframe captured from the standard
// encoder, exercising the actual decoder rather than a fixture payload.
func BenchmarkRealVP8Decode(b *testing.B) {
	payload := realVP8Payload(b)
	runBenchTask(b, goav.From(benchVideoPacketsPayload("real-vp8-packets", b.N, av.CodecVP8, payload)).
		Video().
		Decode().
		To(benchSink("real-vp8-frames")).
		UseRuntime(benchRealCodecRuntime()))
}

func realVP8Payload(b *testing.B) []byte {
	b.Helper()
	out := goavtest.NewCollector()
	err := goav.From(benchVideoFrames("real-vp8-seed", 1, benchVideoW, benchVideoH)).
		Video().
		Encode(codec.VP8()).
		To(out.Sink()).
		UseRuntime(benchRealCodecRuntime()).
		Run(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	packets := out.Packets()
	if len(packets) == 0 {
		b.Fatal("seed encode produced no packets")
	}
	return append([]byte(nil), packets[0].Payload.Bytes...)
}

// benchVideoPacketsPayload pushes n copies of one real encoded video keyframe,
// PTS-stamped at 30fps, for real decoder throughput measurement.
func benchVideoPacketsPayload(name string, n int, codecID av.CodecID, payload []byte) goav.InputSpec {
	return goav.Source(name,
		shape.Packet(av.MediaVideo, codecID, shape.Video(benchVideoW, benchVideoH, av.PixelFormatI420)),
		func(_ context.Context, push source.Push) error {
			base := av.TimeBase{Num: 1, Den: 90_000}
			packet := av.Packet{
				Type:     av.MediaVideo,
				Payload:  av.Buffer{Bytes: payload, Ownership: av.BufferImmutable},
				Keyframe: true,
			}
			for i := 0; i < n; i++ {
				packet.PTS = av.Timestamp{Value: int64(i) * 3000, Base: base}
				packet.DTS = packet.PTS
				packet.Duration = av.Duration{Value: 3000, Base: base}
				if _, err := push.Packet(&packet); err != nil {
					return err
				}
			}
			return push.EOS()
		})
}

// BenchmarkSoakRecordDrift is the soak harness: a long offline-paced packet
// record that reports heap drift and GC cost, so an extended -benchtime run
// (for example -benchtime=10m or a large -benchtime=Nx) yields a reproducible
// stability artifact rather than a single ns/op number.
func BenchmarkSoakRecordDrift(b *testing.B) {
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	runBenchTask(b, goav.From(benchVideoPackets("soak-cam", b.N)).
		Video().
		Copy().
		To(benchSink("soak-packets")).
		UseRuntime(benchRuntime()))

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	b.ReportMetric(float64(after.HeapAlloc)-float64(before.HeapAlloc), "heap_drift_B")
	b.ReportMetric(float64(after.NumGC-before.NumGC), "gc_cycles")
	b.ReportMetric(float64(after.PauseTotalNs-before.PauseTotalNs), "gc_pause_total_ns")
}
