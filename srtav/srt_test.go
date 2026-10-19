package srtav_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/launch"
	"github.com/thesyncim/goav/provider"
	"github.com/thesyncim/goav/runconfig"
	"github.com/thesyncim/goav/srtav"
)

const secret = "correct horse battery"

func runtime(t *testing.T) *goav.Runtime {
	t.Helper()
	return bundle.MustNew(runconfig.Scheme("srt", srtav.SRT()))
}

// freePort returns a loopback UDP port nothing holds.
func freePort(t *testing.T) int {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).Port
}

// awaitListener waits until something holds the port: the listener is up.
func awaitListener(t *testing.T, port int) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port})
		if err != nil {
			return
		}
		conn.Close()
	}
	t.Fatal("the listener never came up")
}

// sendTone publishes a live test tone, encoded to Opus, to an srt URL: the
// launch program `testsrc audio live ! encode opus ! uri …`, paced in real
// time as a contribution encoder sends.
func sendTone(ctx context.Context, rt *goav.Runtime, url string, d time.Duration) error {
	outs, err := launch.Compile(fmt.Sprintf("testsrc audio rate=48000 channels=2 duration=%s live ! encode opus ! uri %q", d, url), launch.URIs("srt"))
	if err != nil {
		return err
	}
	return rt.Run(ctx, outs...)
}

// TestEncodeOverSRTAndDecode runs testsrc → Opus → SRT → decode across
// loopback, caller to listener, encrypted: the listener decodes every frame
// the caller encoded.
func TestEncodeOverSRTAndDecode(t *testing.T) {
	port := freePort(t)
	rt := runtime(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	listen := fmt.Sprintf("srt://:%d?passphrase=%s&latency=80", port, url.QueryEscape(secret))
	call := fmt.Sprintf("srt://127.0.0.1:%d?passphrase=%s&latency=80", port, url.QueryEscape(secret))

	plan, err := rt.Describe(goav.URIInput(listen).Audio().Decode().To(goavtest.NewCollector().Sink()))
	if err != nil || !plan.Realtime {
		t.Fatalf("receive plan realtime=%v err=%v", plan.Realtime, err)
	}
	if text := fmt.Sprintf("%+v", plan); strings.Contains(text, "horse") {
		t.Fatalf("the passphrase reached the plan:\n%s", text)
	}
	frames := goavtest.NewCollector()
	receiveCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- rt.Run(receiveCtx, goav.URIInput(listen).Audio().Decode().To(frames.Sink())) }()
	awaitListener(t, port)
	if err := sendTone(ctx, rt, call, time.Second); err != nil {
		t.Fatalf("send: %v", err)
	}
	got, err := frames.WaitFrames(ctx, 50) // 1s of 20ms frames
	stop()
	if runErr := <-done; runErr != nil && !errors.Is(runErr, context.Canceled) && !errors.Is(runErr, srtav.ErrDisconnected) {
		t.Fatalf("receive: %v", runErr)
	}
	if err != nil {
		t.Fatalf("decoded %d of the 50 frames sent: %v", len(frames.Frames()), err)
	}
	if got[0].Audio.SampleRate != 48000 || got[0].Audio.Channels != 2 {
		t.Fatalf("decoded %+v", got[0].Audio)
	}
}

// TestCopyOverSRT copies an H.264 and AAC file over SRT: the listener reads
// every packet the file holds, unchanged, and the caller took the file's
// media time to send it.
func TestCopyOverSRT(t *testing.T) {
	data, err := os.ReadFile("../container/mp4/testdata/h264_aac.mp4")
	if err != nil {
		t.Fatal(err)
	}
	rt := runtime(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	sent := goavtest.NewCollector()
	if err := rt.Run(ctx, goav.FileInput("in.mp4", bytes.NewReader(data)).To(sent.Sink())); err != nil {
		t.Fatal(err)
	}
	port := freePort(t)
	got := goavtest.NewCollector()
	receiveCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- rt.Run(receiveCtx, goav.URIInput(fmt.Sprintf("srt://:%d", port)).To(got.Sink())) }()
	awaitListener(t, port)
	start := time.Now()
	if err := rt.Run(ctx, goav.FileInput("in.mp4", bytes.NewReader(data)).To(goav.URI(fmt.Sprintf("srt://127.0.0.1:%d", port)))); err != nil {
		t.Fatalf("send: %v", err)
	}
	if took := time.Since(start); took < 300*time.Millisecond {
		t.Fatalf("a 500ms file went out in %v: the destination is not paced", took)
	}
	want := sent.Packets()
	received, err := got.WaitPackets(ctx, len(want))
	stop()
	<-done
	if err != nil {
		t.Fatalf("received %d of %d packets: %v", len(got.Packets()), len(want), err)
	}
	// The transport stream gives keyframes their parameter sets in band;
	// every other byte and each time arrives as sent.
	byKind := func(packets []*av.Packet, kind av.MediaType) []*av.Packet {
		var out []*av.Packet
		for _, p := range packets {
			if p.Type == kind {
				out = append(out, p)
			}
		}
		return out
	}
	for _, kind := range []av.MediaType{av.MediaVideo, av.MediaAudio} {
		w, g := byKind(want, kind), byKind(received, kind)
		if len(w) != len(g) {
			t.Fatalf("%s: received %d packets, sent %d", kind, len(g), len(w))
		}
		for i := range w {
			if !bytes.HasSuffix(g[i].Payload.Bytes, w[i].Payload.Bytes) || g[i].Keyframe != w[i].Keyframe {
				t.Fatalf("%s packet %d differs", kind, i)
			}
			wd, _ := w[i].PTS.ToDuration()
			gd, _ := g[i].PTS.ToDuration()
			if d := gd - wd - 700*time.Millisecond; d < -12*time.Microsecond || d > 12*time.Microsecond {
				t.Fatalf("%s packet %d at %v, sent at %v", kind, i, gd, wd)
			}
		}
	}
}

// TestPassphraseMismatch calls a listener with the wrong passphrase, and
// with none: both fail with ErrPassphrase, naming no secret, and the
// listener keeps listening for a caller that has it.
func TestPassphraseMismatch(t *testing.T) {
	port := freePort(t)
	rt := runtime(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	got := goavtest.NewCollector()
	receiveCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	listen := fmt.Sprintf("srt://:%d?passphrase=%s&streamid=live/a", port, url.QueryEscape(secret))
	go func() { done <- rt.Run(receiveCtx, goav.URIInput(listen).Audio().To(got.Sink())) }()
	awaitListener(t, port)
	send := func(query string) error {
		return sendTone(ctx, rt, fmt.Sprintf("srt://127.0.0.1:%d?%s", port, query), 100*time.Millisecond)
	}
	for _, query := range []string{"passphrase=wrong+horse+battery&streamid=live/a", "streamid=live/a"} {
		err := send(query)
		var rejected *srtav.RejectedError
		if !errors.Is(err, srtav.ErrPassphrase) || !errors.As(err, &rejected) {
			t.Fatalf("%s: %v, want ErrPassphrase", query, err)
		}
		if strings.Contains(err.Error(), "horse") {
			t.Fatalf("the error names the passphrase: %v", err)
		}
	}
	err := send("passphrase=" + url.QueryEscape(secret) + "&streamid=live/b")
	var rejected *srtav.RejectedError
	if !errors.As(err, &rejected) || rejected.Reason != 1404 || errors.Is(err, srtav.ErrPassphrase) {
		t.Fatalf("another stream id: %v, want a 1404 rejection", err)
	}
	if err := sendTone(ctx, rt, fmt.Sprintf("srt://127.0.0.1:%d?passphrase=%s&streamid=live/a", port, url.QueryEscape(secret)), 300*time.Millisecond); err != nil {
		t.Fatalf("the right passphrase: %v", err)
	}
	if _, err := got.WaitPackets(ctx, 5); err != nil {
		t.Fatalf("the listener got %d packets after the refusals: %v", len(got.Packets()), err)
	}
}

// TestReconnect drops a listener's caller mid-stream: goav.Reconnect
// listens again, the next caller's media arrives, and the task reports the
// reconnect.
func TestReconnect(t *testing.T) {
	port := freePort(t)
	rt := runtime(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	got := goavtest.NewCollector()
	built := make(chan goav.LiveTask, 1)
	go func() {
		task, err := rt.Build(ctx, goav.URIInput(fmt.Sprintf("srt://:%d", port), goav.Reconnect(0, 50*time.Millisecond)).Audio().To(got.Sink()))
		if err != nil {
			t.Error(err)
		}
		built <- task
	}()
	send := func() {
		t.Helper()
		awaitListener(t, port)
		call := fmt.Sprintf("srt://127.0.0.1:%d", port)
		if err := sendTone(ctx, rt, call, 600*time.Millisecond); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	send()
	task := <-built
	if task == nil {
		t.FailNow()
	}
	defer task.Close()
	watch := task.Watch(goav.WatchTypes(av.EventReconnect))
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()
	first, err := got.WaitPackets(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	var drop av.Event
	select {
	case drop = <-watch.Events():
	case err := <-runErr:
		t.Fatalf("the task ended with %v, not a reconnect", err)
	case <-ctx.Done():
		t.Fatal("no reconnect after the caller left")
	}
	if !errors.Is(drop.Cause, srtav.ErrDisconnected) {
		t.Fatalf("dropped with %v, want ErrDisconnected", drop.Cause)
	}
	time.Sleep(100 * time.Millisecond) // past the backoff: the listener is up again
	before := len(first)
	send()
	for {
		select {
		case event := <-watch.Events():
			if event.Metadata["state"] != "reconnected" {
				continue
			}
			if _, err := got.WaitPackets(ctx, before+5); err != nil {
				t.Fatalf("after the reconnect: %d packets: %v", len(got.Packets()), err)
			}
			return
		case <-ctx.Done():
			t.Fatal("never reconnected")
		}
	}
}

// TestBindRefusals names the fix for an srt URL that cannot work, before
// anything connects.
func TestBindRefusals(t *testing.T) {
	scheme := srtav.SRT().(provider.InputScheme)
	for raw, want := range map[string]string{
		"srt://host":                       "no port",
		"srt://:9000?mode=caller":          "calls no host",
		"srt://host:9000?mode=both":        "want caller or listener",
		"srt://host:9000?pbkeylen=20":      "want 16, 24, or 32",
		"srt://host:9000?latency=fast":     "want milliseconds",
		"srt://host:9000?passphrase=short": "SRT takes 10 to 80",
		"srt://host:9000?pkt_size=1316":    "unknown parameter \"pkt_size\"",
	} {
		u, _ := url.Parse(raw)
		if _, err := scheme.Input(u); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v, want %q", raw, err, want)
		}
	}
}
