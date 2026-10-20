package webrtcav

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/source"
)

type soakCountSink struct {
	name   string
	frames atomic.Int64
}

func (s *soakCountSink) Name() string { return s.name }
func (s *soakCountSink) Close() error { return nil }
func (s *soakCountSink) Handle(_ context.Context, m *pipeline.Message) error {
	if m.Frame != nil {
		s.frames.Add(1)
	}
	return nil
}

type soakEncoderProbe struct{ keys atomic.Int64 }

func (*soakEncoderProbe) Name() string { return "active-encoder-output" }
func (*soakEncoderProbe) Close() error { return nil }
func (s *soakEncoderProbe) Handle(_ context.Context, m *pipeline.Message) error {
	if m.Packet != nil && m.Packet.Keyframe {
		s.keys.Add(1)
	}
	return nil
}
func soakAudio() goav.InputSpec {
	return goav.Source("tone", shape.Frame(av.MediaAudio, shape.Audio(48000, 1, av.SampleFormatS16), shape.Realtime(true)), func(ctx context.Context, push source.Push) error {
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for n := int64(0); ; n++ {
			select {
			case <-ctx.Done():
				return nil
			case <-tick.C:
			}
			pcm := make([]byte, 960*2)
			for i := range 960 {
				binary.LittleEndian.PutUint16(pcm[i*2:], uint16(int16(3000*math.Sin(2*math.Pi*440*float64(n*960+int64(i))/48000))))
			}
			frame := av.Frame{Type: av.MediaAudio, PTS: av.Timestamp{Value: n * 960, Base: av.RTPTimeBase(48000)}, Duration: av.Duration{Value: 960, Base: av.RTPTimeBase(48000)},
				Audio: &av.AudioFrame{SampleRate: 48000, Channels: 1, ChannelLayout: "mono", SampleFormat: av.SampleFormatS16, Samples: 960}, Planes: []av.Plane{{Buffer: av.Buffer{Bytes: pcm}}}}
			if _, err := push.Frame(&frame); err != nil {
				return err
			}
		}
	})
}
func soakWait(t *testing.T, ctx context.Context, what string, ready func() bool) {
	t.Helper()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for !ready() {
		select {
		case <-ctx.Done():
			t.Fatalf("%s: %v", what, ctx.Err())
		case <-tick.C:
		}
	}
}

// TestTaskAVLifecycleSoak complements the transport probe with actual task
// mutation, continuous encoded A/V, a finalized recording and remote decoding.
func TestTaskAVLifecycleSoak(t *testing.T) {
	raw := os.Getenv("GOAV_WEBRTC_SOAK_DURATION")
	if raw == "" {
		t.Skip("set GOAV_WEBRTC_SOAK_DURATION or run scripts/bench/workloads.sh")
	}
	duration, err := time.ParseDuration(raw)
	if err != nil || duration <= 0 {
		t.Fatalf("invalid duration %q", raw)
	}
	ctx, cancel := context.WithTimeout(t.Context(), duration+30*time.Second)
	defer cancel()
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
	videoTrack, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000}, "video", "task-soak")
	if err != nil {
		t.Fatal(err)
	}
	audioTrack, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2}, "audio", "task-soak")
	if err != nil {
		t.Fatal(err)
	}
	videoSender, err := offerer.AddTrack(videoTrack)
	if err != nil {
		t.Fatal(err)
	}
	audioSender, err := offerer.AddTrack(audioTrack)
	if err != nil {
		t.Fatal(err)
	}
	tracks := make(chan *webrtc.TrackRemote, 2)
	answerer.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) { tracks <- track })
	connect(t, offerer, answerer)
	path := filepath.Join(t.TempDir(), "recording.webm")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	rt := bundle.MustNew()
	recording := goav.Write("recording.webm", file)
	video := pacedVideo("camera").Video().Tap("video.frames")
	audio := soakAudio().Audio().Tap("audio.frames")
	task, err := rt.Build(ctx, video.Encode(codec.VP8()).To(recording), audio.Encode(codec.Opus()).To(recording))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- task.Run(runCtx) }()
	var receiver goav.Task
	var receivedVideo *webrtc.TrackRemote
	videoCount, audioCount := &soakCountSink{name: "remote-video"}, &soakCountSink{name: "remote-audio"}
	var receiveDone chan error
	var stopReceive context.CancelFunc
	defer func() {
		if stopReceive != nil {
			stopReceive()
			<-receiveDone
			_ = receiver.Close()
		}
	}()
	start := time.Now()
	cycles := 0
	videoSink, audioSink := LocalTrack(videoTrack, videoSender), LocalTrack(audioTrack, audioSender)
	defer func() {
		if t.Failed() {
			t.Logf("cycles=%d remote video=%d audio=%d", cycles, videoCount.frames.Load(), audioCount.frames.Load())
			select {
			case err := <-done:
				t.Logf("source run: %v", err)
			default:
			}
			select {
			case err := <-receiveDone:
				t.Logf("receive run: %v", err)
				receiveDone <- err
			default:
			}
		}
	}()
	for time.Since(start) < duration || cycles < 2 {
		probe := &soakEncoderProbe{}
		encodedVideo := task.Tap("video.frames").Encode(codec.VP8(codec.KeyframeInterval(100000)))
		attachment, err := task.Attach(ctx,
			encodedVideo.To(goav.Sink(videoSink, goav.Name("live-video"))),
			encodedVideo.To(goav.Sink(probe)),
			task.Tap("audio.frames").Encode(codec.Opus()).To(goav.Sink(audioSink, goav.Name("live-audio"))))
		if err != nil {
			t.Fatal(err)
		}
		if receiver == nil {
			var receivedAudio *webrtc.TrackRemote
			for receivedVideo == nil || receivedAudio == nil {
				select {
				case track := <-tracks:
					if track.Kind() == webrtc.RTPCodecTypeVideo {
						receivedVideo = track
					} else {
						receivedAudio = track
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			receiver, err = rt.Build(ctx, goav.Input(Track(receivedVideo)).Video().Decode().To(goav.Sink(videoCount)), goav.Input(Track(receivedAudio)).Audio().Decode().To(goav.Sink(audioCount)))
			if err != nil {
				t.Fatal(err)
			}
			var receiveCtx context.Context
			receiveCtx, stopReceive = context.WithCancel(ctx)
			receiveDone = make(chan error, 1)
			go func() { receiveDone <- receiver.Run(receiveCtx) }()
		}
		waitCtx, waitCancel := context.WithTimeout(ctx, 3*time.Second)
		v, a := videoCount.frames.Load(), audioCount.frames.Load()
		soakWait(t, waitCtx, "decodable A/V after attachment", func() bool {
			return videoCount.frames.Load() > v && audioCount.frames.Load() > a && probe.keys.Load() > 0
		})
		keys := probe.keys.Load()
		if err := answerer.WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: uint32(receivedVideo.SSRC())}}); err != nil {
			t.Fatal(err)
		}
		soakWait(t, waitCtx, "PLI at replacement encoder", func() bool { return probe.keys.Load() > keys })
		waitCancel()
		if err := task.Detach(ctx, attachment); err != nil {
			t.Fatal(err)
		}
		if task.Stats().PendingCleanup != 0 {
			t.Fatal("attachment cleanup still pending")
		}
		for _, sender := range []*webrtc.RTPSender{videoSender, audioSender} {
			senderFeedbacks.Lock()
			feedback := senderFeedbacks.readers[sender]
			senderFeedbacks.Unlock()
			if feedback == nil {
				t.Fatal("feedback pump lost")
			}
			feedback.mu.Lock()
			listeners := len(feedback.listeners)
			feedback.mu.Unlock()
			if listeners != 0 {
				t.Fatalf("%d stale feedback subscribers", listeners)
			}
		}
		cycles++
	}
	stop()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
		t.Fatal(err)
	}
	stopReceive()
	if err := <-receiveDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := receiver.Close(); err != nil {
		t.Fatal(err)
	}
	stopReceive = nil
	file, err = os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	input := goav.FileInput("recording.webm", file)
	recordedVideo, recordedAudio := &soakCountSink{name: "recorded-video"}, &soakCountSink{name: "recorded-audio"}
	if err := rt.Run(ctx, input.Video().Decode().To(goav.Sink(recordedVideo)), input.Audio().Decode().To(goav.Sink(recordedAudio))); err != nil {
		t.Fatal(err)
	}
	if recordedVideo.frames.Load() < int64(cycles) || recordedAudio.frames.Load() < int64(cycles) {
		t.Fatal("recording lacks continuous decodable A/V")
	}
	report, _ := json.Marshal(map[string]any{"workload": "task_av_reattach", "cycles": cycles, "elapsed_ns": time.Since(start).Nanoseconds(), "remote_video_frames": videoCount.frames.Load(), "remote_audio_frames": audioCount.frames.Load(), "recorded_video_frames": recordedVideo.frames.Load(), "recorded_audio_frames": recordedAudio.frames.Load(), "pending_cleanup": task.Stats().PendingCleanup, "drops": task.Stats().Dropped})
	t.Log(string(report))
}
