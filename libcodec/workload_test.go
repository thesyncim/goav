package libcodec_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
)

// TestFileLadderWorkload consumes independently encoded clips, including
// audio and delayed H.264 pictures. The opt-in driver records the input
// hashes, encoder settings, output quality, CPU and RSS alongside this log.
func TestFileLadderWorkload(t *testing.T) {
	clips := os.Getenv("GOAV_WORKLOAD_CLIPS")
	if clips == "" {
		t.Skip("set GOAV_WORKLOAD_CLIPS or run scripts/bench/workloads.sh")
	}
	backend, option := libH264(t)
	for _, size := range []string{"720p", "1080p"} {
		t.Run(size, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(clips, size+".mp4"))
			if err != nil {
				t.Fatal(err)
			}
			inputVideo, inputAudio := demuxed(t, data)
			if len(inputVideo) < 30 || len(inputAudio) == 0 {
				t.Fatal("workload needs video GOPs and simultaneous audio")
			}
			in := goav.FileInput(size+".mp4", bytes.NewReader(data))
			meter := &ladderMeter{starts: make(map[time.Duration]time.Time)}
			video := in.Video().Decode(codec.Backend(backend)).Do(meter)
			rt := bundle.MustNew(option)
			var hd, sd bytes.Buffer
			hdOut := goav.Write("hd.mp4", &hd, goav.Format(av.FormatMP4))
			sdOut := goav.Write("sd.mp4", &sd, goav.Format(av.FormatMP4))
			rungs := []struct {
				name                   string
				width, height, bitrate int
				destination            goav.Destination
				data                   *bytes.Buffer
			}{
				{"hd", 1280, 720, 2_000_000, hdOut, &hd},
				{"sd", 640, 360, 800_000, sdOut, &sd},
			}
			var outputs []goav.Output
			for _, rung := range rungs {
				encoded := video.Resize(rung.width, rung.height).Encode(codec.H264(codec.Backend(backend), codec.Bitrate(rung.bitrate), codec.KeyframeInterval(30)))
				outputs = append(outputs, encoded.To(rung.destination), in.Audio().To(rung.destination), encoded.To(goav.Sink(&ladderLatencySink{name: rung.name, meter: meter})))
			}
			before := runtime.NumGoroutine()
			started := time.Now()
			task, err := rt.Build(t.Context(), outputs...)
			if err != nil {
				t.Fatal(err)
			}
			defer task.Close()
			decoders := 0
			for _, node := range task.Spec().Nodes {
				if strings.Contains(node.Name, "decode") {
					decoders++
				}
			}
			if decoders != 1 {
				t.Fatalf("ladder has %d decoders, want shared decode", decoders)
			}
			if err := task.Run(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := task.Close(); err != nil {
				t.Fatal(err)
			}
			elapsed := time.Since(started)
			if meter.frames != len(inputVideo) {
				t.Fatalf("decoded %d frames for %d input packets", meter.frames, len(inputVideo))
			}
			if stats := task.Stats(); stats.PendingCleanup != 0 || stats.Dropped != 0 {
				t.Fatalf("unclean workload: %+v", stats)
			}
			var drift time.Duration
			for _, rung := range rungs {
				v, a := demuxed(t, rung.data.Bytes())
				if len(v) != len(inputVideo) || len(a) != len(inputAudio) {
					t.Fatalf("%s: video=%d audio=%d, want %d/%d", rung.name, len(v), len(a), len(inputVideo), len(inputAudio))
				}
				endVideo, _ := v[len(v)-1].PTS.ToDuration()
				endAudio, _ := a[len(a)-1].PTS.ToDuration()
				delta := endVideo - endAudio
				if delta < 0 {
					delta = -delta
				}
				drift = max(drift, delta)
				if delta > 100*time.Millisecond {
					t.Fatalf("%s A/V tail drift %s", rung.name, delta)
				}
				if outputDir := os.Getenv("GOAV_WORKLOAD_OUTPUT"); outputDir != "" {
					if err := os.WriteFile(filepath.Join(outputDir, size+"-"+rung.name+".mp4"), rung.data.Bytes(), 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			slices.Sort(meter.latencies)
			quantile := func(q int) int64 { return meter.latencies[(len(meter.latencies)-1)*q/100].Nanoseconds() }
			var memory runtime.MemStats
			runtime.ReadMemStats(&memory)
			report, _ := json.Marshal(map[string]any{
				"workload": "file_ladder", "size": size, "backend": backend,
				"frames": meter.frames, "elapsed_ns": elapsed.Nanoseconds(), "input_fps": float64(meter.frames) / elapsed.Seconds(),
				"decode_to_encode_p50_ns": quantile(50), "decode_to_encode_p95_ns": quantile(95), "decode_to_encode_p99_ns": quantile(99),
				"av_tail_drift_ns": drift.Nanoseconds(), "goroutines_before": before, "goroutines_after": runtime.NumGoroutine(),
				"heap_alloc_bytes": memory.HeapAlloc, "hd_bitrate": 2_000_000, "sd_bitrate": 800_000,
			})
			t.Log(string(report))
		})
	}
}

type ladderMeter struct {
	mu        sync.Mutex
	starts    map[time.Duration]time.Time
	frames    int
	latencies []time.Duration
}

func (*ladderMeter) Name() string { return "latency-start" }
func (m *ladderMeter) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	if msg.Frame != nil {
		pts, _ := msg.Frame.PTS.ToDuration()
		m.mu.Lock()
		m.starts[pts] = time.Now()
		m.frames++
		m.mu.Unlock()
	}
	return emitter.Emit(ctx, msg)
}
func (*ladderMeter) Close() error { return nil }

type ladderLatencySink struct {
	name  string
	meter *ladderMeter
}

func (s *ladderLatencySink) Name() string { return "latency-" + s.name }
func (s *ladderLatencySink) Handle(_ context.Context, msg *pipeline.Message) error {
	if msg.Packet == nil {
		return nil
	}
	pts, _ := msg.Packet.PTS.ToDuration()
	s.meter.mu.Lock()
	defer s.meter.mu.Unlock()
	start, ok := s.meter.starts[pts]
	if !ok {
		return fmt.Errorf("encoded PTS %s has no decoded frame", pts)
	}
	s.meter.latencies = append(s.meter.latencies, time.Since(start))
	return nil
}
func (*ladderLatencySink) Close() error { return nil }
