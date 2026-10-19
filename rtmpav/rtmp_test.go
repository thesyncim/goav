package rtmpav

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/container/flv"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/internal/amf0"
	runconfig "github.com/thesyncim/goav/runconfig"
)

// decodeIngest runs a task over a Listen input: VP9 frames and Opus frames
// decoded from what a publisher sends. It returns the collectors and the
// run's error.
func decodeIngest(t *testing.T, rt *goav.Runtime, in goav.InputSpec) (video, audio *goavtest.Collector, done <-chan error) {
	t.Helper()
	video, audio = goavtest.NewCollector(), goavtest.NewCollector()
	errs := make(chan error, 1)
	go func() {
		errs <- rt.Run(testContext(t), in.Video().Decode().To(video.Sink()), in.Audio().Decode().To(audio.Sink()))
	}()
	return video, audio, errs
}

// A file published over E-RTMP (VP9 and Opus) to Listen arrives whole:
// every frame decodes, and the ingest ends when the publisher stops.
func TestPublishToListenDecodes(t *testing.T) {
	srv, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	rt := testRuntime(t)
	video, audio, done := decodeIngest(t, rt, goav.Input(srv.Input("live/test-key")))
	publish := goav.URIInput(fixtures + "vp9_opus.flv").To(goav.URI("rtmp://" + srv.Addr().String() + "/live/test-key"))
	if err := rt.Run(testContext(t), publish); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := <-done; !errors.Is(err, ErrStreamEnded) {
		t.Fatalf("ingest ended with %v, want ErrStreamEnded", err)
	}
	if n := len(video.Frames()); n != 10 || video.Frames()[0].Video.Width != 64 {
		t.Fatalf("%d VP9 frames, want 10 of 64 pixels wide", n)
	}
	if n := len(audio.Frames()); n < 40 {
		t.Fatalf("%d Opus frames", n)
	}
}

// A classic H.264 and AAC publish records to FLV through Listen, the
// packets copied.
func TestPublishClassicCopiesThroughListen(t *testing.T) {
	srv, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	rt := testRuntime(t)
	out := filepath.Join(t.TempDir(), "recorded.flv")
	done := make(chan error, 1)
	go func() { done <- rt.Run(testContext(t), goav.Input(srv.Input("app/key")).To(goav.URI(out))) }()
	if err := rt.Run(testContext(t), goav.URIInput(fixtures+"h264_aac.flv").To(goav.URI("rtmp://"+srv.Addr().String()+"/app/key"))); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := <-done; !errors.Is(err, ErrStreamEnded) {
		t.Fatalf("record ended with %v", err)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	d := &flv.Demuxer{}
	if err := d.Open(context.Background(), format.Input{Reader: bytes.NewReader(data)}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	var codecs []string
	for _, s := range d.Streams() {
		codecs = append(codecs, string(s.Codec.ID))
	}
	if strings.Join(codecs, ",") != "h264,aac" {
		t.Fatalf("recorded %v", codecs)
	}
	video := 0
	res := format.ReadResult{Packet: &av.Packet{}, Events: make([]av.Event, 0, 1)}
	for {
		res.Reset()
		if d.ReadInto(context.Background(), &res) != nil {
			break
		}
		if res.PacketReady && res.Packet.Type == av.MediaVideo {
			video++
		}
	}
	if video != 10 {
		t.Fatalf("%d video packets recorded, want 10", video)
	}
}

// rtmps publishes over TLS to a Listen serving a test certificate.
func TestRTMPS(t *testing.T) {
	server, client := testCertificates(t)
	srv, err := Listen("127.0.0.1:0", WithTLSConfig(server))
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	rt := testRuntime(t, WithTLSConfig(client))
	video, _, done := decodeIngest(t, rt, goav.Input(srv.Input("live/tls-key")))
	if err := rt.Run(testContext(t), goav.URIInput(fixtures+"vp9_opus.flv").To(goav.URI("rtmps://"+srv.Addr().String()+"/live/tls-key"))); err != nil {
		t.Fatalf("publish over rtmps: %v", err)
	}
	<-done
	if n := len(video.Frames()); n != 10 {
		t.Fatalf("%d frames over rtmps", n)
	}
	// A client that does not trust the certificate is refused.
	err = testRuntime(t).Run(testContext(t), goav.URIInput(fixtures+"vp9_opus.flv").To(goav.URI("rtmps://"+srv.Addr().String()+"/live/tls-key")))
	if err == nil || !strings.Contains(err.Error(), "tls") {
		t.Fatalf("untrusted certificate: %v", err)
	}
}

// A play receives the server's stream and decodes it.
func TestPlay(t *testing.T) {
	h := newHarness(t, fixtureTags(t, "vp9_opus"))
	frames := goavtest.NewCollector()
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- testRuntime(t).Run(ctx, goav.URIInput(h.url("play-key")).Video().Decode().To(frames.Sink()))
	}()
	if _, err := frames.WaitFrames(ctx, 10); err != nil {
		t.Fatalf("frames: %v (%d)", err, len(frames.Frames()))
	}
	cancel()
	<-done
}

// A play that drops reconnects under goav.Reconnect: the task sees the
// reconnect, and the second play's stream decodes.
func TestPlayReconnects(t *testing.T) {
	h := newHarness(t, fixtureTags(t, "vp9_opus"))
	h.dropAfter = 20
	frames := goavtest.NewCollector()
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	task, err := testRuntime(t).Build(ctx, goav.URIInput(h.url("key"), goav.Reconnect(3, 10*time.Millisecond)).Video().Decode().To(frames.Sink()))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	watch := task.Watch(goav.WatchTypes(av.EventReconnect))
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	select {
	case <-watch.Events():
	case <-ctx.Done():
		t.Fatal("no reconnect event")
	}
	if _, err := frames.WaitFrames(ctx, 12); err != nil {
		t.Fatalf("frames after the reconnect: %v (%d)", err, len(frames.Frames()))
	}
	h.mu.Lock()
	plays := h.plays
	h.mu.Unlock()
	if plays != 2 {
		t.Fatalf("%d plays, want 2", plays)
	}
	cancel()
	<-done
}

// The stream key never shows: not in Describe or Explain, not in the
// errors of a failed connect or a refused publish, not in a running
// publish's snapshot.
func TestStreamKeyRedacted(t *testing.T) {
	const key = "sk-canary-31337"
	rt := testRuntime(t)
	closed := goav.URIInput(fixtures + "vp9_opus.flv").To(goav.URI("rtmp://" + freePort(t) + "/live/" + key + "?sig=s3cr3t"))
	spec, err := rt.Describe(closed)
	if err != nil {
		t.Fatal(err)
	}
	report, err := rt.Explain(context.Background(), closed)
	if err != nil {
		t.Fatal(err)
	}
	for name, v := range map[string]any{"describe": spec, "explain": report} {
		data, _ := json.Marshal(v)
		if bytes.Contains(data, []byte(key)) || bytes.Contains(data, []byte("s3cr3t")) {
			t.Fatalf("%s shows the key: %s", name, data)
		}
	}
	err = rt.Run(testContext(t), closed)
	if err == nil || strings.Contains(err.Error(), key) || strings.Contains(err.Error(), "s3cr3t") {
		t.Fatalf("connect error %v", err)
	}

	srv, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	err = rt.Run(testContext(t), goav.URIInput(fixtures+"vp9_opus.flv").To(goav.URI("rtmp://"+srv.Addr().String()+"/live/"+key)))
	var refused *StatusError
	if !errors.As(err, &refused) || refused.Code != "NetStream.Publish.BadName" || strings.Contains(err.Error(), key) {
		t.Fatalf("publish to a stream no input takes: %v", err)
	}

	in := srv.Input("live/" + key)
	_, _, done := decodeIngest(t, rt, goav.Input(in))
	task, err := rt.Build(testContext(t), goav.URIInput(fixtures+"vp9_opus.flv").To(goav.URI("rtmp://"+srv.Addr().String()+"/live/"+key)))
	if err != nil {
		t.Fatal(err)
	}
	ran := make(chan error, 1)
	go func() { ran <- task.Run(testContext(t)) }()
	time.Sleep(200 * time.Millisecond)
	snapshot, _ := json.Marshal(task.Snapshot())
	if bytes.Contains(snapshot, []byte(key)) {
		t.Fatalf("snapshot shows the key: %s", snapshot)
	}
	if err := <-ran; err != nil {
		t.Fatal(err)
	}
	task.Close()
	<-done
}

// Frames published to rtmp encode to H.264, which every server reads: with
// no H.264 encoder the plan refuses with that encode instead of picking
// VP9, and with one registered it inserts it.
func TestFramesEncodeToH264(t *testing.T) {
	out := goavtest.Video(64, 64, 1).Video().To(goav.URI("rtmp://127.0.0.1:1935/live/key"))
	_, err := testRuntime(t).Describe(out)
	var refusal *goav.BuildError
	if !errors.As(err, &refusal) || !strings.Contains(strings.Join(refusal.FixLines(), "\n"), ".Encode(codec.H264())") {
		t.Fatalf("frames without an H.264 encoder: %v", err)
	}
	scheme := RTMP()
	rt, err := goav.New(append([]runconfig.Option{goavtest.Codec(av.CodecH264)}, runconfig.Scheme("rtmp", scheme))...)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := rt.Describe(out)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, node := range spec.Nodes {
		found = found || strings.HasPrefix(node.Name, "encode-") && strings.Contains(node.Detail, "h264")
	}
	if !found {
		t.Fatalf("no h264 encode in %+v", spec.Nodes)
	}
}

// A server that negotiates Enhanced RTMP and lists only the classic
// codecs refuses VP9 with the encode that fixes it; one that lists none
// is sent VP9.
func TestCodecNegotiation(t *testing.T) {
	h := newHarness(t, nil)
	h.caps = amf0.Object{{Name: "fourCcList", Value: []any{"avc1", "mp4a"}}}
	err := testRuntime(t).Run(testContext(t), goav.URIInput(fixtures+"vp9_opus.flv").To(goav.URI(h.url("key"))))
	if !errors.Is(err, ErrCodecRefused) || !strings.Contains(err.Error(), ".Decode().Encode(codec.H264())") {
		t.Fatalf("vp9 to a classic server: %v", err)
	}
	h.caps = nil
	if err := testRuntime(t).Run(testContext(t), goav.URIInput(fixtures+"vp9_opus.flv").To(goav.URI(h.url("key")))); err != nil {
		t.Fatalf("vp9 to a server that states nothing: %v", err)
	}
}
