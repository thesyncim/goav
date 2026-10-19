package rtmpav

import (
	"bytes"
	"context"
	"errors"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/goavtest"
)

// The ffmpeg tests interoperate with FFmpeg's RTMP client and server, the
// reference most servers and players are checked against; they skip when
// ffmpeg is not installed.

func ffmpegPath(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg not installed")
	}
	return path
}

// ffprobeCodecs is the comma-separated codecs ffprobe reads in a file.
func ffprobeCodecs(t *testing.T, path string) string {
	t.Helper()
	probe, err := exec.LookPath("ffprobe")
	if err != nil {
		t.Skip("ffprobe not installed")
	}
	out, err := exec.Command(probe, "-v", "error", "-show_entries", "stream=codec_name", "-of", "csv=p=0", path).CombinedOutput()
	if err != nil {
		t.Fatalf("ffprobe: %v\n%s", err, out)
	}
	return strings.Join(strings.Fields(string(out)), ",")
}

// retryRefused runs run until the server it dials is listening.
func retryRefused(t *testing.T, run func() error) error {
	t.Helper()
	for range 100 {
		err := run()
		if err == nil || !errors.Is(err, syscall.ECONNREFUSED) {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
	return errors.New("the server never listened")
}

// FFmpeg publishes classic and enhanced FLV to Listen, which records it
// with the codecs it carried.
func TestFFmpegPublishesToListen(t *testing.T) {
	ffmpeg := ffmpegPath(t)
	for fixture, want := range map[string]string{"h264_aac": "h264,aac", "hevc_aac": "hevc,aac", "av1_opus": "av1,opus"} {
		t.Run(fixture, func(t *testing.T) {
			srv, err := Listen("127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer srv.Close()
			out := filepath.Join(t.TempDir(), "recorded.flv")
			done := make(chan error, 1)
			go func() {
				done <- testRuntime(t).Run(testContext(t), goav.Input(srv.Input("live/ffmpeg-key")).To(goav.URI(out)))
			}()
			cmd := exec.Command(ffmpeg, "-v", "error", "-re", "-i", fixtures+fixture+".flv", "-c", "copy", "-f", "flv", "rtmp://"+srv.Addr().String()+"/live/ffmpeg-key")
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("ffmpeg publish: %v\n%s", err, output)
			}
			if err := <-done; !errors.Is(err, ErrStreamEnded) {
				t.Fatalf("record ended with %v", err)
			}
			if got := ffprobeCodecs(t, out); got != want {
				t.Fatalf("recorded %s, want %s", got, want)
			}
		})
	}
}

// goav publishes classic and enhanced codecs to FFmpeg's RTMP server,
// which records what it receives.
func TestPublishToFFmpeg(t *testing.T) {
	ffmpeg := ffmpegPath(t)
	for fixture, want := range map[string]string{"h264_aac": "h264,aac", "hevc_aac": "hevc,aac", "vp9_opus": "vp9,opus"} {
		t.Run(fixture, func(t *testing.T) {
			addr := freePort(t)
			out := filepath.Join(t.TempDir(), "received.flv")
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var stderr bytes.Buffer
			cmd := exec.CommandContext(ctx, ffmpeg, "-v", "error", "-y", "-listen", "1", "-i", "rtmp://"+addr+"/live/key", "-c", "copy", out)
			cmd.Stderr = &stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			err := retryRefused(t, func() error {
				return testRuntime(t).Run(ctx, goav.URIInput(fixtures+fixture+".flv").To(goav.URI("rtmp://"+addr+"/live/key")))
			})
			if err != nil {
				t.Fatalf("publish: %v\nffmpeg: %s", err, stderr.String())
			}
			if err := cmd.Wait(); err != nil {
				t.Fatalf("ffmpeg: %v\n%s", err, stderr.String())
			}
			if got := ffprobeCodecs(t, out); got != want {
				t.Fatalf("ffmpeg received %s, want %s", got, want)
			}
		})
	}
}

// goav plays from FFmpeg's RTMP server and decodes the stream.
func TestPlayFromFFmpeg(t *testing.T) {
	ffmpeg := ffmpegPath(t)
	addr := freePort(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, ffmpeg, "-v", "error", "-re", "-i", fixtures+"vp9_opus.flv", "-c", "copy", "-f", "flv", "-listen", "1", "rtmp://"+addr+"/live/key")
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Wait() }()
	frames := goavtest.NewCollector()
	err := retryRefused(t, func() error {
		return testRuntime(t).Run(ctx, goav.URIInput("rtmp://"+addr+"/live/key").Video().Decode().To(frames.Sink()))
	})
	// ffmpeg closes the connection when its file ends.
	if err != nil && !errors.Is(err, ErrStreamEnded) {
		t.Fatalf("play: %v\nffmpeg: %s", err, stderr.String())
	}
	if n := len(frames.Frames()); n < 9 {
		t.Fatalf("%d frames played from ffmpeg\nffmpeg: %s", n, stderr.String())
	}
}
