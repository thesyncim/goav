package webm

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestExternalFFProbeRecognizesWebM(t *testing.T) {
	tool := requireTool(t, "ffprobe")
	file := writeCompatibilityWebM(t)
	output := runExternal(t, tool, "-v", "error", "-show_entries", "stream=codec_name,width,height,sample_rate,channels", "-of", "default=nw=1", file)
	if !strings.Contains(output, "vp8") || !strings.Contains(output, "opus") {
		t.Fatalf("ffprobe output missing codecs:\n%s", output)
	}
}

func TestExternalFFProbeReportsSeekableDuration(t *testing.T) {
	tool := requireTool(t, "ffprobe")
	file := writeSeekableCompatibilityWebM(t)
	output := strings.TrimSpace(runExternal(t, tool, "-v", "error", "-show_entries", "format=duration", "-of", "default=nw=1:nk=1", file))
	duration, err := strconv.ParseFloat(output, 64)
	if err != nil {
		t.Fatalf("duration output = %q: %v", output, err)
	}
	if duration < 0.019 || duration > 0.021 {
		t.Fatalf("duration = %f, want about 0.020", duration)
	}
}

func TestExternalMKVToolNixCompat(t *testing.T) {
	file := writeCompatibilityWebM(t)
	if tool, ok := lookupTool("mkvalidator"); ok {
		runExternal(t, tool, file)
	}
	if tool, ok := lookupTool("mkvinfo"); ok {
		runExternal(t, tool, file)
	}
	if tool, ok := lookupTool("mkvextract"); ok {
		out := filepath.Join(t.TempDir(), "track-0.bin")
		runExternal(t, tool, "tracks", file, "0:"+out)
	}
	if tool, ok := lookupTool("mkvmerge"); ok {
		out := filepath.Join(t.TempDir(), "remux.webm")
		runExternal(t, tool, "-o", out, file)
	}
}

func writeSeekableCompatibilityWebM(t *testing.T) string {
	t.Helper()
	file := filepath.Join(t.TempDir(), "seekable.webm")
	w, err := os.Create(file)
	if err != nil {
		t.Fatal(err)
	}
	muxer, err := NewMuxer(w, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	videoID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{TrackID: videoID, TimeNS: 0, DurationNS: 20_000_000, Keyframe: true, Data: []byte{0x10, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00}}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return file
}

func writeCompatibilityWebM(t *testing.T) string {
	t.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	videoID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	audioID, err := muxer.AddTrack(Track{
		Type:  TrackAudio,
		Codec: CodecOpus,
		Audio: AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{TrackID: videoID, TimeNS: 0, Keyframe: true, Data: []byte{0x10, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00}}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{TrackID: audioID, TimeNS: 20_000_000, Keyframe: true, Data: []byte{0xf8, 0xff, 0xfe}}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "sample.webm")
	if err := os.WriteFile(file, buffer.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return file
}

func requireTool(t *testing.T, name string) string {
	t.Helper()
	tool, ok := lookupTool(name)
	if !ok {
		t.Skipf("%s not installed", name)
	}
	return tool
}

func lookupTool(name string) (string, bool) {
	tool, err := exec.LookPath(name)
	return tool, err == nil
}

func runExternal(t *testing.T, tool string, args ...string) string {
	t.Helper()
	cmd := exec.Command(tool, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s failed: %v\n%s", filepath.Base(tool), strings.Join(args, " "), err, output)
	}
	return string(output)
}
