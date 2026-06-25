// Benchmarks for the canonical goav workloads, run through the public recipe
// grammar against the deterministic goavtest runtime (fake passthrough codecs,
// fake byte-faithful containers, real bundled resize/resample filters), so the
// numbers are dependency-free and reproducible: they measure goav's pipeline
// machinery, not a codec backend.
//
// Pattern: every benchmark builds its task once (cold path, untimed), then the
// source pushes exactly b.N messages through the running graph, so ns/op and
// allocs/op are per-message steady-state figures including drain and EOS,
// amortized over b.N. ns/op is the honest latency proxy here; p50/p95
// percentiles would need a histogram harness and are deliberately not faked.
//
// Run:  scripts/bench/run.sh        (saves timestamped -benchmem output)
// Diff: scripts/bench/compare.sh    (benchstat old new)
package goav_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"strconv"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/goavtest"
	runconfig "github.com/thesyncim/goav/runconfig"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/source"
)

const (
	benchPacketBytes  = 1200 // ~one RTP MTU payload
	benchAudioSamples = 960  // 20ms @ 48kHz mono
)

// benchRuntime is the deterministic offline runtime every benchmark uses:
// goavtest fakes plus WithRealtime(false), so nothing paces on a clock and the
// fake clock records no sleep trace while b.N grows.
func benchRuntime(opts ...runconfig.Option) *goav.Runtime {
	return goavtest.Runtime(append([]runconfig.Option{runconfig.WithRealtime(false)}, opts...)...)
}

// benchSink is a no-op message destination, so sink work never pollutes the
// pipeline numbers.
func benchSink(name string) goav.Destination {
	return goav.Sink(component.SinkFunc(name, func(context.Context, component.Message) error {
		return nil
	}))
}

// benchVideoPackets pushes n VP8-tagged packets (1200B shared immutable
// payload, 30fps timestamps, a keyframe every 30th) and ends the stream.
func benchVideoPackets(name string, n int) goav.InputSpec {
	payload := bytes.Repeat([]byte{0x5a}, benchPacketBytes)
	return goav.Source(name,
		shape.Packet(av.MediaVideo, av.CodecVP8, shape.Video(640, 360, av.PixelFormatI420)),
		func(_ context.Context, push source.Push) error {
			base := av.TimeBase{Num: 1, Den: 90_000}
			packet := av.Packet{
				Type:    av.MediaVideo,
				Payload: av.Buffer{Bytes: payload, Ownership: av.BufferImmutable},
			}
			for i := 0; i < n; i++ {
				packet.PTS = av.Timestamp{Value: int64(i) * 3000, Base: base}
				packet.DTS = packet.PTS
				packet.Duration = av.Duration{Value: 3000, Base: base}
				packet.Keyframe = i%30 == 0
				if _, err := push.Packet(&packet); err != nil {
					return err
				}
			}
			return push.EOS()
		})
}

// benchAudioPackets pushes n copies of the given encoded payloads in order
// (cycling), stamped as 20ms opus-tagged packets.
func benchAudioPackets(name string, n int, payload []byte) goav.InputSpec {
	return goav.Source(name,
		shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48_000, 1, av.SampleFormatS16)),
		func(_ context.Context, push source.Push) error {
			base := av.TimeBase{Num: 1, Den: 48_000}
			packet := av.Packet{
				Type:    av.MediaAudio,
				Payload: av.Buffer{Bytes: payload, Ownership: av.BufferImmutable},
			}
			for i := 0; i < n; i++ {
				packet.PTS = av.Timestamp{Value: int64(i) * benchAudioSamples, Base: base}
				packet.DTS = packet.PTS
				packet.Duration = av.Duration{Value: benchAudioSamples, Base: base}
				packet.Keyframe = true
				if _, err := push.Packet(&packet); err != nil {
					return err
				}
			}
			return push.EOS()
		})
}

// benchAudioFrames pushes n S16 interleaved audio frames over one shared
// immutable payload, PTS-stamped in sample time.
func benchAudioFrames(name string, n, sampleRate, channels, samplesPerChannel int) goav.InputSpec {
	payload := make([]byte, samplesPerChannel*channels*2)
	for i := 0; i+1 < len(payload); i += 2 {
		binary.LittleEndian.PutUint16(payload[i:], uint16(int16(i%2048-1024)))
	}
	return goav.Source(name,
		shape.Frame(av.MediaAudio, shape.Audio(sampleRate, channels, av.SampleFormatS16)),
		func(_ context.Context, push source.Push) error {
			base := av.TimeBase{Num: 1, Den: int64(sampleRate)}
			frame := av.Frame{
				Type: av.MediaAudio,
				Audio: &av.AudioFrame{
					SampleRate:   sampleRate,
					Channels:     channels,
					SampleFormat: av.SampleFormatS16,
					Samples:      samplesPerChannel,
				},
				Planes: []av.Plane{{Buffer: av.Buffer{Bytes: payload, Ownership: av.BufferImmutable}}},
			}
			for i := 0; i < n; i++ {
				frame.PTS = av.Timestamp{Value: int64(i) * int64(samplesPerChannel), Base: base}
				frame.Duration = av.Duration{Value: int64(samplesPerChannel), Base: base}
				if _, err := push.Frame(&frame); err != nil {
					return err
				}
			}
			return push.EOS()
		})
}

// benchVideoFrames pushes n I420 frames over shared immutable planes at 30fps.
func benchVideoFrames(name string, n, width, height int) goav.InputSpec {
	chromaW, chromaH := (width+1)/2, (height+1)/2
	luma := bytes.Repeat([]byte{0x80}, width*height)
	cb := bytes.Repeat([]byte{0x80}, chromaW*chromaH)
	cr := bytes.Repeat([]byte{0x80}, chromaW*chromaH)
	return goav.Source(name,
		shape.Frame(av.MediaVideo, shape.Video(width, height, av.PixelFormatI420)),
		func(_ context.Context, push source.Push) error {
			base := av.TimeBase{Num: 1, Den: 30}
			frame := av.Frame{
				Type:  av.MediaVideo,
				Video: &av.VideoFrame{Width: width, Height: height, PixelFormat: av.PixelFormatI420},
				Planes: []av.Plane{
					{Buffer: av.Buffer{Bytes: luma, Ownership: av.BufferImmutable}, Stride: width},
					{Buffer: av.Buffer{Bytes: cb, Ownership: av.BufferImmutable}, Stride: chromaW},
					{Buffer: av.Buffer{Bytes: cr, Ownership: av.BufferImmutable}, Stride: chromaW},
				},
			}
			for i := 0; i < n; i++ {
				frame.PTS = av.Timestamp{Value: int64(i), Base: base}
				frame.Duration = av.Duration{Value: 1, Base: base}
				if _, err := push.Frame(&frame); err != nil {
					return err
				}
			}
			return push.EOS()
		})
}

// runBenchTask builds the job (untimed cold path) and times one Run that
// drains the b.N messages the job's sources push.
func runBenchTask(b *testing.B, job *goav.Job) {
	b.Helper()
	ctx := context.Background()
	task, err := job.Build(ctx)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	if err := task.Run(ctx); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	if err := task.Close(); err != nil {
		b.Fatal(err)
	}
}

// passthroughOpusPayload returns one payload produced by the goavtest
// passthrough opus encoder, so decode benchmarks exercise the byte-faithful
// unwrap path (plane reuse) instead of the allocating fallback-frame path.
func passthroughOpusPayload(b *testing.B) []byte {
	b.Helper()
	ctx := context.Background()
	out := goavtest.NewCollector()
	err := goav.From(benchAudioFrames("seed", 1, 48_000, 1, benchAudioSamples)).
		Audio().
		Encode(codec.Opus()).
		To(out.Sink()).
		UseRuntime(benchRuntime()).
		Run(ctx)
	if err != nil {
		b.Fatal(err)
	}
	packets := out.Packets()
	if len(packets) != 1 {
		b.Fatalf("seed encode produced %d packets, want 1", len(packets))
	}
	return packets[0].Payload.Bytes
}

// BenchmarkRecordPackets is the RTP-style record path: a packet source feeding
// a packet-preserving Copy into a (fake-container) file. ns/op is the per-packet
// record cost including mux framing.
func BenchmarkRecordPackets(b *testing.B) {
	runBenchTask(b, goav.From(benchVideoPackets("cam", b.N)).
		Video().
		Copy().
		To(goav.File("out.mp4", io.Discard)).
		UseRuntime(benchRuntime()))
}

// BenchmarkRemuxPackets is the file→file remux: demux a (fake-container) file
// of b.N packets and mux every packet into a new file, packet-preserving.
func BenchmarkRemuxPackets(b *testing.B) {
	ctx := context.Background()
	var input bytes.Buffer
	err := goav.From(benchVideoPackets("cam", b.N)).
		Video().
		Copy().
		To(goav.File("in.mp4", &input)).
		UseRuntime(benchRuntime()).
		Run(ctx)
	if err != nil {
		b.Fatal(err)
	}
	runBenchTask(b, goav.From(goav.FileInput("in.mp4", bytes.NewReader(input.Bytes()))).
		Video().
		Copy().
		To(goav.File("out.mp4", io.Discard)).
		UseRuntime(benchRuntime()))
}

// BenchmarkDecodeToFrameSink is the decode path: packets through the (fake)
// decoder into a frame sink.
func BenchmarkDecodeToFrameSink(b *testing.B) {
	payload := passthroughOpusPayload(b)
	runBenchTask(b, goav.From(benchAudioPackets("mic", b.N, payload)).
		Audio().
		Decode().
		To(benchSink("frames")).
		UseRuntime(benchRuntime()))
}

// BenchmarkDecodeEncode is the transcode shape: decode then re-encode (both
// fake passthrough codecs), delivering encoded packets to a sink.
func BenchmarkDecodeEncode(b *testing.B) {
	payload := passthroughOpusPayload(b)
	runBenchTask(b, goav.From(benchAudioPackets("mic", b.N, payload)).
		Audio().
		Decode().
		Encode(codec.Opus()).
		To(benchSink("packets")).
		UseRuntime(benchRuntime()))
}

// BenchmarkResample is the real bundled resample filter: 44.1kHz stereo S16 frames
// converted to 48kHz mono.
func BenchmarkResample(b *testing.B) {
	runBenchTask(b, goav.From(benchAudioFrames("mic", b.N, 44_100, 2, 882)).
		Audio().
		Resample(48_000, 1).
		To(benchSink("resampled")).
		UseRuntime(benchRuntime()))
}

// BenchmarkResize is the real bundled resize filter: 320x180 I420 frames scaled to
// 160x90.
func BenchmarkResize(b *testing.B) {
	runBenchTask(b, goav.From(benchVideoFrames("cam", b.N, 320, 180)).
		Video().
		Resize(160, 90).
		To(benchSink("resized")).
		UseRuntime(benchRuntime()))
}

// BenchmarkBranchFanout decodes once and fans the frames out to N planned
// branches, each ending in its own sink. ns/op is per source packet (each one
// is delivered N times).
func BenchmarkBranchFanout(b *testing.B) {
	payload := passthroughOpusPayload(b)
	for _, n := range []int{2, 8} {
		b.Run("branches="+strconv.Itoa(n), func(b *testing.B) {
			branches := make([]goav.BranchSpec, n)
			for i := range branches {
				name := "branch-" + strconv.Itoa(i)
				branches[i] = goav.Branch(name).To(benchSink(name))
			}
			runBenchTask(b, goav.From(benchAudioPackets("mic", b.N, payload)).
				Audio().
				Decode().
				Branches(branches...).
				UseRuntime(benchRuntime()))
		})
	}
}

// BenchmarkSharedMuxGroup is the shared mux group: one video and one audio
// chain encode (fake passthrough) and explicit mux grouping, muxing both
// streams into a single (fake-container) file. Sources split b.N between them.
func BenchmarkSharedMuxGroup(b *testing.B) {
	half := b.N / 2
	if half < 1 {
		half = 1
	}
	out := goav.File("call.webm", io.Discard)
	runBenchTask(b, goav.From(
		benchVideoFrames("cam", half, 320, 180),
		benchAudioFrames("mic", half, 48_000, 1, benchAudioSamples),
	).
		Video(goav.InputName("cam")).Encode(codec.VP8()).To(out).
		Audio(goav.InputName("mic")).Encode(codec.Opus()).To(out).
		UseRuntime(benchRuntime()))
}

// BenchmarkMix sums N synchronized audio arms into one stream. The graph runs
// buffered (blocking, so nothing sheds) because the arms are concurrent
// sources; ns/op is per input frame across all arms — one mixed output frame
// costs roughly N times that.
func BenchmarkMix(b *testing.B) {
	for _, n := range []int{2, 8} {
		b.Run("arms="+strconv.Itoa(n), func(b *testing.B) {
			per := b.N / n
			if per < 1 {
				per = 1
			}
			arms := make([]goav.JoinArm, n)
			for i := range arms {
				arms[i] = goav.From(benchAudioFrames("arm-"+strconv.Itoa(i), per, 48_000, 1, benchAudioSamples)).Audio()
			}
			runBenchTask(b, goav.Mix(arms...).
				To(benchSink("mixed")).
				UseRuntime(benchRuntime(runconfig.WithBufferPolicy(flow.Blocking(64).PipelinePolicy()))))
		})
	}
}

// BenchmarkComposite paints two video arms onto one canvas per step. ns/op is
// per input frame; one composed canvas costs roughly twice that.
func BenchmarkComposite(b *testing.B) {
	per := b.N / 2
	if per < 1 {
		per = 1
	}
	runBenchTask(b, goav.Composite(
		goav.From(benchVideoFrames("cam", per, 160, 90)).Video().Region(0, 0),
		goav.From(benchVideoFrames("screen", per, 160, 90)).Video().Region(160, 0),
	).
		To(benchSink("canvas")).
		UseRuntime(benchRuntime(runconfig.WithBufferPolicy(flow.Blocking(64).PipelinePolicy()))))
}

// BenchmarkSelectPassthrough is the one-of-N live switch in its steady state:
// two arms push concurrently, the selector forwards the active arm and sheds
// the other at the gate. ns/op is per pushed frame across both arms.
func BenchmarkSelectPassthrough(b *testing.B) {
	per := b.N / 2
	if per < 1 {
		per = 1
	}
	runBenchTask(b, goav.Select(
		goav.From(benchAudioFrames("live", per, 48_000, 1, benchAudioSamples)).Audio(),
		goav.From(benchAudioFrames("standby", per, 48_000, 1, benchAudioSamples)).Audio(),
	).
		To(benchSink("selected")).
		UseRuntime(benchRuntime()))
}

// BenchmarkAttachDetachUnderLoad measures the control plane against live
// traffic: a never-ending source streams through a tapped chain on a buffered
// graph while each benchmark op attaches a runtime branch at the tap and
// detaches it again.
func BenchmarkAttachDetachUnderLoad(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	task, err := goav.From(goavtest.LiveAudio("live", 48_000, 1, make([]int16, benchAudioSamples))).
		Audio().
		Tap(goav.FrameTap("live.frames")).
		To(benchSink("main")).
		UseRuntime(benchRuntime(runconfig.WithBufferPolicy(flow.DropOldest(64).PipelinePolicy()))).
		Build(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer task.Close()

	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		attachment, err := task.Attach(ctx, goav.Branch("monitor-"+strconv.Itoa(i)).
			From(goav.FrameTap("live.frames")).
			To(benchSink("monitor-sink-"+strconv.Itoa(i))))
		if err != nil {
			b.Fatal(err)
		}
		if err := task.Detach(ctx, attachment); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	cancel()
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		b.Fatal(err)
	}
}

// BenchmarkSourcePush is the flow-control hot path seen by a custom source:
// SourcePush.Frame into a bounded queue ahead of a no-op sink. "dropping"
// sheds on full (push never blocks); "blocking" paces the producer through the
// queue (true backpressure).
func BenchmarkSourcePush(b *testing.B) {
	for _, mode := range []struct {
		name   string
		buffer flow.BranchBuffer
	}{
		{"dropping", flow.DropOldest(64)},
		{"blocking", flow.Blocking(64)},
	} {
		b.Run(mode.name, func(b *testing.B) {
			runBenchTask(b, goav.From(benchAudioFrames("mic", b.N, 48_000, 1, benchAudioSamples)).
				Audio().
				To(benchSink("out")).
				UseRuntime(benchRuntime(runconfig.WithBufferPolicy(mode.buffer.PipelinePolicy()))))
		})
	}
}
