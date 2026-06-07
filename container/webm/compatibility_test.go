package webm

import (
	"bytes"
	"errors"
	"io"
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
	for _, codec := range []string{"vp8", "vp9", "av1", "opus"} {
		if !strings.Contains(output, codec) {
			t.Fatalf("ffprobe output missing %s:\n%s", codec, output)
		}
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

func TestExternalDemuxerReadsFFmpegWebMCodecs(t *testing.T) {
	tests := []struct {
		name  string
		codec Codec
		typ   TrackType
		write func(testing.TB) string
	}{
		{name: "vp8", codec: CodecVP8, typ: TrackVideo, write: writeFFmpegVP8WebM},
		{name: "vp9", codec: CodecVP9, typ: TrackVideo, write: writeFFmpegVP9WebM},
		{name: "av1", codec: CodecAV1, typ: TrackVideo, write: writeFFmpegAV1WebM},
		{name: "opus", codec: CodecOpus, typ: TrackAudio, write: writeFFmpegOpusWebM},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file := tt.write(t)
			input, err := os.Open(file)
			if err != nil {
				t.Fatal(err)
			}
			defer input.Close()
			demuxer, err := NewDemuxer(input, DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			tracks := demuxer.Tracks()
			if len(tracks) != 1 {
				t.Fatalf("tracks = %d, want 1", len(tracks))
			}
			if tracks[0].Codec != tt.codec || tracks[0].Type != tt.typ {
				t.Fatalf("track = %+v, want %s %v", tracks[0], tt.name, tt.typ)
			}
			if tt.typ == TrackVideo && (tracks[0].Video.Width != 16 || tracks[0].Video.Height != 16) {
				t.Fatalf("video = %+v, want 16x16", tracks[0].Video)
			}
			if tt.typ == TrackAudio && (tracks[0].Audio.SampleRate != 48000 || tracks[0].Audio.Channels == 0) {
				t.Fatalf("audio = %+v, want 48000 Hz opus", tracks[0].Audio)
			}
			packet := Packet{Data: make([]byte, 0, 1<<20)}
			for {
				err := demuxer.ReadPacket(&packet)
				if errors.Is(err, io.EOF) {
					t.Fatalf("no packet read from ffmpeg %s webm", tt.name)
				}
				if err != nil {
					t.Fatal(err)
				}
				if packet.TrackID != tracks[0].ID {
					continue
				}
				if len(packet.Data) == 0 {
					t.Fatalf("empty packet for ffmpeg %s webm", tt.name)
				}
				if tt.typ == TrackVideo && !packet.Keyframe {
					t.Fatalf("packet = %+v, want keyframe payload", packet)
				}
				return
			}
		})
	}
}

func TestExternalRemuxesFFmpegWebMCodecs(t *testing.T) {
	tool := requireTool(t, "ffprobe")
	tests := []struct {
		name        string
		ffprobe     string
		write       func(testing.TB) string
		requireType TrackType
	}{
		{name: "vp8", ffprobe: "vp8", write: writeFFmpegVP8WebM, requireType: TrackVideo},
		{name: "vp9", ffprobe: "vp9", write: writeFFmpegVP9WebM, requireType: TrackVideo},
		{name: "av1", ffprobe: "av1", write: writeFFmpegAV1WebM, requireType: TrackVideo},
		{name: "opus", ffprobe: "opus", write: writeFFmpegOpusWebM, requireType: TrackAudio},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file := remuxFirstWebMPacket(t, tt.write(t), tt.requireType)
			output := runExternal(t, tool, "-v", "error", "-show_entries", "stream=codec_name", "-of", "default=nw=1", file)
			if !strings.Contains(output, tt.ffprobe) {
				t.Fatalf("ffprobe output missing %s:\n%s", tt.ffprobe, output)
			}
		})
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

func writeFFmpegVP8WebM(t testing.TB) string {
	t.Helper()
	tool := requireTool(t, "ffmpeg")
	file := filepath.Join(t.TempDir(), "ffmpeg-vp8.webm")
	runExternalOrSkip(t, tool,
		"-y",
		"-hide_banner",
		"-loglevel", "error",
		"-f", "lavfi",
		"-i", "testsrc=size=16x16:rate=1:duration=1",
		"-frames:v", "1",
		"-c:v", "libvpx",
		"-deadline", "realtime",
		"-cpu-used", "8",
		"-b:v", "100k",
		file,
	)
	return file
}

func writeFFmpegVP9WebM(t testing.TB) string {
	t.Helper()
	tool := requireTool(t, "ffmpeg")
	file := filepath.Join(t.TempDir(), "ffmpeg-vp9.webm")
	runExternalOrSkip(t, tool,
		"-y",
		"-hide_banner",
		"-loglevel", "error",
		"-f", "lavfi",
		"-i", "testsrc=size=16x16:rate=1:duration=1",
		"-frames:v", "1",
		"-c:v", "libvpx-vp9",
		"-deadline", "realtime",
		"-cpu-used", "8",
		"-b:v", "100k",
		file,
	)
	return file
}

func writeFFmpegAV1WebM(t testing.TB) string {
	t.Helper()
	tool := requireTool(t, "ffmpeg")
	file := filepath.Join(t.TempDir(), "ffmpeg-av1.webm")
	runExternalOrSkip(t, tool,
		"-y",
		"-hide_banner",
		"-loglevel", "error",
		"-f", "lavfi",
		"-i", "testsrc=size=16x16:rate=1:duration=1",
		"-frames:v", "1",
		"-c:v", "libsvtav1",
		"-preset", "13",
		"-crf", "50",
		file,
	)
	return file
}

func writeFFmpegOpusWebM(t testing.TB) string {
	t.Helper()
	tool := requireTool(t, "ffmpeg")
	file := filepath.Join(t.TempDir(), "ffmpeg-opus.webm")
	runExternalOrSkip(t, tool,
		"-y",
		"-hide_banner",
		"-loglevel", "error",
		"-f", "lavfi",
		"-i", "sine=frequency=1000:sample_rate=48000:duration=0.02",
		"-c:a", "libopus",
		"-application", "voip",
		"-frame_duration", "20",
		file,
	)
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
	vp9ID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP9,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	av1ID, err := muxer.AddTrack(Track{
		Type:         TrackVideo,
		Codec:        CodecAV1,
		Video:        VideoConfig{Width: 16, Height: 16},
		CodecPrivate: webmAV1CodecConfig(),
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
	if err := muxer.WritePacket(Packet{TrackID: vp9ID, TimeNS: 0, Keyframe: true, Data: []byte{0x83, 0x49, 0x83}}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{TrackID: av1ID, TimeNS: 0, Keyframe: true, Data: webmAV1SequenceHeaderOBU()}); err != nil {
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

func remuxFirstWebMPacket(t testing.TB, file string, requireType TrackType) string {
	t.Helper()
	input, err := os.Open(file)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	demuxer, err := NewDemuxer(input, DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 1 {
		t.Fatalf("tracks = %d, want 1", len(tracks))
	}
	if tracks[0].Type != requireType {
		t.Fatalf("track = %+v, want type %v", tracks[0], requireType)
	}
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(tracks[0])
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 1<<20)}
	for {
		err := demuxer.ReadPacket(&packet)
		if errors.Is(err, io.EOF) {
			t.Fatalf("no packet read from %s", file)
		}
		if err != nil {
			t.Fatal(err)
		}
		if packet.TrackID != tracks[0].ID {
			continue
		}
		if len(packet.Data) == 0 {
			t.Fatalf("empty packet from %s", file)
		}
		packet.TrackID = trackID
		if err := muxer.WritePacket(packet); err != nil {
			t.Fatal(err)
		}
		break
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "remuxed.webm")
	if err := os.WriteFile(out, buffer.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return out
}

func requireTool(t testing.TB, name string) string {
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

func runExternal(t testing.TB, tool string, args ...string) string {
	t.Helper()
	cmd := exec.Command(tool, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s failed: %v\n%s", filepath.Base(tool), strings.Join(args, " "), err, output)
	}
	return string(output)
}

func runExternalOrSkip(t testing.TB, tool string, args ...string) string {
	t.Helper()
	cmd := exec.Command(tool, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Skipf("%s %s unavailable: %v\n%s", filepath.Base(tool), strings.Join(args, " "), err, output)
	}
	return string(output)
}
