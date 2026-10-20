package webrtcav

import (
	"context"
	"encoding/json"
	"os"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/pipeline"
)

// TestWebRTCLifecycleSoak keeps one negotiated sender alive while its logical
// output repeatedly detaches, idles and reattaches. Every cycle must deliver
// media and route a remote PLI to the current attachment, then unsubscribe.
func TestWebRTCLifecycleSoak(t *testing.T) {
	raw := os.Getenv("GOAV_WEBRTC_SOAK_DURATION")
	if raw == "" {
		t.Skip("set GOAV_WEBRTC_SOAK_DURATION or run scripts/bench/workloads.sh")
	}
	duration, err := time.ParseDuration(raw)
	if err != nil || duration <= 0 {
		t.Fatalf("invalid soak duration %q", raw)
	}
	before := runtime.NumGoroutine()
	offerer, err := loopbackAPI().NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer offerer.Close()
	answerer, err := loopbackAPI().NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer answerer.Close()
	track, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000}, "v", "soak")
	if err != nil {
		t.Fatal(err)
	}
	sender, err := offerer.AddTrack(track)
	if err != nil {
		t.Fatal(err)
	}
	remote := make(chan *webrtc.TrackRemote, 1)
	answerer.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) { remote <- track })
	connect(t, offerer, answerer)
	sink := LocalTrack(track, sender).(*egress)
	defer sink.Close()
	var got *webrtc.TrackRemote
	latencies := make([]time.Duration, 0, 4096)
	start := time.Now()
	cycles := 0
	var maximum time.Duration
	var over50ms int
	defer func() {
		slices.Sort(latencies)
		q := func(percent int) int64 {
			if len(latencies) == 0 {
				return 0
			}
			return latencies[(len(latencies)-1)*percent/100].Nanoseconds()
		}
		report, _ := json.Marshal(map[string]any{"workload": "webrtc_reattach", "cycles": cycles, "elapsed_ns": time.Since(start).Nanoseconds(),
			"latency_scope": "trailing_window", "latency_window_samples": len(latencies),
			"media_and_feedback_p50_ns": q(50), "media_and_feedback_p95_ns": q(95), "media_and_feedback_p99_ns": q(99),
			"whole_run_max_ns": maximum.Nanoseconds(), "whole_run_over_50ms": over50ms, "failed": t.Failed(),
			"goroutines_before": before, "goroutines_after": runtime.NumGoroutine()})
		t.Log(string(report))
	}()
	ctx, cancel := context.WithTimeout(t.Context(), duration+15*time.Second)
	defer cancel()
	for time.Since(start) < duration || cycles < 2 {
		feedback := make(chan struct{}, 1)
		sink.UseControl(func(_ context.Context, ctrl control.Control) error {
			if ctrl.Type() == control.KeyframeType {
				select {
				case feedback <- struct{}{}:
				default:
				}
			}
			return nil
		})
		if err := sink.OpenStreams(ctx, []av.Stream{{ID: "v", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8, ClockRate: 90000}}}); err != nil {
			t.Fatal(err)
		}
		packet := &av.Packet{StreamID: "v", Keyframe: true, Payload: av.Buffer{Bytes: []byte{0x10, 0, 0x9d, 0x01}}, PTS: av.Timestamp{Value: int64(cycles) * 3000, Base: av.RTPTimeBase(90000)}}
		sent := time.Now()
		if err := sink.Handle(ctx, &pipeline.Message{Kind: pipeline.MessagePacket, Packet: packet}); err != nil {
			t.Fatal(err)
		}
		if got == nil {
			// The first RTP can precede ICE connectivity. Keep sending until
			// the remote track becomes visible, then read its first packet.
			tick := time.NewTicker(10 * time.Millisecond)
		waitTrack:
			for {
				select {
				case got = <-remote:
					break waitTrack
				case <-tick.C:
					if err := sink.Handle(ctx, &pipeline.Message{Kind: pipeline.MessagePacket, Packet: packet}); err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("no remote media")
				}
			}
			tick.Stop()
		}
		if err := got.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, _, err := got.ReadRTP(); err != nil {
			t.Fatal(err)
		}
		if err := answerer.WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: uint32(got.SSRC())}}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-feedback:
		case <-time.After(5 * time.Second):
			t.Fatal("feedback did not reach active attachment")
		}
		latency := time.Since(sent)
		maximum = max(maximum, latency)
		if latency > 50*time.Millisecond {
			over50ms++
		}
		// Bound instrumentation memory for hour-scale runs.
		if len(latencies) < cap(latencies) {
			latencies = append(latencies, latency)
		} else {
			latencies[cycles%len(latencies)] = latency
		}
		if err := sink.Close(); err != nil {
			t.Fatal(err)
		}
		senderFeedbacks.Lock()
		source := senderFeedbacks.readers[sender]
		senderFeedbacks.Unlock()
		if source == nil {
			t.Fatal("sender feedback stopped during reattachment")
		}
		source.mu.Lock()
		listeners := len(source.listeners)
		source.mu.Unlock()
		if listeners != 0 {
			t.Fatalf("retained %d attachment listeners", listeners)
		}
		cycles++
		time.Sleep(10 * time.Millisecond)
	}
	if err := offerer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := answerer.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if runtime.NumGoroutine() > before {
		t.Fatalf("goroutines before=%d after=%d", before, runtime.NumGoroutine())
	}
}
