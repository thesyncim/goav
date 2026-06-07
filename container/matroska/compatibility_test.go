package matroska

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestExternalFFProbeRecognizesMatroskaWebRTCCodecs(t *testing.T) {
	tool := requireExternalTool(t, "ffprobe")
	file := writeCompatibilityMatroska(t)
	output := runExternalTool(t, tool, "-v", "error", "-show_entries", "stream=codec_name,width,height,sample_rate,channels", "-of", "default=nw=1", file)
	for _, codec := range []string{"opus", "av1", "h264", "vp9", "vp8"} {
		if !strings.Contains(output, codec) {
			t.Fatalf("ffprobe output missing %s:\n%s", codec, output)
		}
	}
}

func TestExternalFFProbeRecognizesGeneratedMatroskaCodecPrivate(t *testing.T) {
	tool := requireExternalTool(t, "ffprobe")
	tests := []struct {
		name  string
		codec string
		file  string
	}{
		{name: "av1", codec: "av1", file: writeGeneratedPrivateMatroska(t, CodecAV1, av1SequenceHeaderOBU())},
		{name: "h264", codec: "h264", file: writeGeneratedPrivateMatroska(t, CodecH264, h264AnnexBParameterAccessUnit())},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := runExternalTool(t, tool, "-v", "error", "-show_entries", "stream=codec_name,width,height", "-of", "default=nw=1", tt.file)
			if !strings.Contains(output, tt.codec) {
				t.Fatalf("ffprobe output missing %s:\n%s", tt.codec, output)
			}
		})
	}
}

func TestExternalFFProbeReportsOpusCodecTiming(t *testing.T) {
	tool := requireExternalTool(t, "ffprobe")
	file := writeOpusTimingMatroska(t)
	output := runExternalTool(t, tool, "-v", "error", "-show_entries", "stream=codec_name,initial_padding", "-of", "default=nw=1", file)
	for _, want := range []string{"codec_name=opus", "initial_padding=312"} {
		if !strings.Contains(output, want) {
			t.Fatalf("ffprobe output missing %s:\n%s", want, output)
		}
	}
}

func TestExternalDemuxerReadsFFmpegMatroskaCodecs(t *testing.T) {
	tests := []struct {
		name       string
		codec      Codec
		typ        TrackType
		write      func(testing.TB) string
		wantPrefix []byte
	}{
		{name: "h264", codec: CodecH264, typ: TrackVideo, write: writeFFmpegH264Matroska, wantPrefix: []byte{0, 0, 0, 1}},
		{name: "av1", codec: CodecAV1, typ: TrackVideo, write: writeFFmpegAV1Matroska},
		{name: "vp8", codec: CodecVP8, typ: TrackVideo, write: writeFFmpegVP8Matroska},
		{name: "vp9", codec: CodecVP9, typ: TrackVideo, write: writeFFmpegVP9Matroska},
		{name: "opus", codec: CodecOpus, typ: TrackAudio, write: writeFFmpegOpusMatroska},
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
					t.Fatalf("no packet read from ffmpeg %s matroska", tt.name)
				}
				if err != nil {
					t.Fatal(err)
				}
				if packet.TrackID != tracks[0].ID {
					continue
				}
				if len(packet.Data) == 0 {
					t.Fatalf("empty packet for ffmpeg %s matroska", tt.name)
				}
				if len(tt.wantPrefix) != 0 && !bytes.HasPrefix(packet.Data, tt.wantPrefix) {
					t.Fatalf("packet data prefix = %x, want %x", packet.Data[:min(len(packet.Data), 8)], tt.wantPrefix)
				}
				if tt.typ == TrackVideo && !packet.Keyframe {
					t.Fatalf("packet = %+v, want keyframe payload", packet)
				}
				return
			}
		})
	}
}

func TestExternalRemuxesFFmpegMatroskaCodecs(t *testing.T) {
	tool := requireExternalTool(t, "ffprobe")
	tests := []struct {
		name        string
		ffprobe     string
		write       func(testing.TB) string
		wantPrefix  []byte
		requireType TrackType
	}{
		{name: "h264", ffprobe: "h264", write: writeFFmpegH264Matroska, wantPrefix: []byte{0, 0, 0, 1}, requireType: TrackVideo},
		{name: "av1", ffprobe: "av1", write: writeFFmpegAV1Matroska, requireType: TrackVideo},
		{name: "vp8", ffprobe: "vp8", write: writeFFmpegVP8Matroska, requireType: TrackVideo},
		{name: "vp9", ffprobe: "vp9", write: writeFFmpegVP9Matroska, requireType: TrackVideo},
		{name: "opus", ffprobe: "opus", write: writeFFmpegOpusMatroska, requireType: TrackAudio},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file := remuxFirstMatroskaPacket(t, tt.write(t), tt.requireType, tt.wantPrefix)
			output := runExternalTool(t, tool, "-v", "error", "-show_entries", "stream=codec_name", "-of", "default=nw=1", file)
			if !strings.Contains(output, tt.ffprobe) {
				t.Fatalf("ffprobe output missing %s:\n%s", tt.ffprobe, output)
			}
		})
	}
}

func TestExternalMatroskaToolCompat(t *testing.T) {
	file := writeCompatibilityMatroska(t)
	if tool, ok := lookupExternalTool("mkvalidator"); ok {
		runExternalTool(t, tool, file)
	}
	if tool, ok := lookupExternalTool("mkvinfo"); ok {
		runExternalTool(t, tool, file)
	}
	if tool, ok := lookupExternalTool("mkvextract"); ok {
		out := filepath.Join(t.TempDir(), "track-0.bin")
		runExternalTool(t, tool, "tracks", file, "0:"+out)
	}
	if tool, ok := lookupExternalTool("mkvmerge"); ok {
		out := filepath.Join(t.TempDir(), "remux.mkv")
		runExternalTool(t, tool, "-o", out, file)
	}
}

func writeCompatibilityMatroska(t *testing.T) string {
	t.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	opusID, err := muxer.AddTrack(Track{
		Type:  TrackAudio,
		Codec: CodecOpus,
		Audio: AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	av1ID, err := muxer.AddTrack(Track{
		Type:         TrackVideo,
		Codec:        CodecAV1,
		Video:        VideoConfig{Width: 16, Height: 16},
		CodecPrivate: av1CodecConfig(),
	})
	if err != nil {
		t.Fatal(err)
	}
	h264ID, err := muxer.AddTrack(Track{
		Type:         TrackVideo,
		Codec:        CodecH264,
		Video:        VideoConfig{Width: 16, Height: 16},
		CodecPrivate: h264AVCDecoderConfig(),
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
	vp8ID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	packets := []Packet{
		{TrackID: opusID, TimeNS: 0, Keyframe: true, Data: []byte{0xf8, 0xff, 0xfe}},
		{TrackID: av1ID, TimeNS: 0, Keyframe: true, Data: av1SequenceHeaderOBU()},
		{TrackID: h264ID, TimeNS: 0, Keyframe: true, Data: h264AnnexBAccessUnit()},
		{TrackID: vp9ID, TimeNS: 0, Keyframe: true, Data: []byte{0x83, 0x49, 0x83}},
		{TrackID: vp8ID, TimeNS: 0, Keyframe: true, Data: []byte{0x10, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00}},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			t.Fatalf("write packet %d: %v", i, err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "sample.mkv")
	if err := os.WriteFile(file, buffer.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return file
}

func writeFFmpegH264Matroska(t testing.TB) string {
	t.Helper()
	tool := requireExternalTool(t, "ffmpeg")
	file := filepath.Join(t.TempDir(), "ffmpeg-h264.mkv")
	runExternalToolOrSkip(t, tool,
		"-y",
		"-hide_banner",
		"-loglevel", "error",
		"-f", "lavfi",
		"-i", "testsrc=size=16x16:rate=1:duration=1",
		"-frames:v", "1",
		"-c:v", "libx264",
		"-preset", "ultrafast",
		"-tune", "zerolatency",
		"-pix_fmt", "yuv420p",
		file,
	)
	return file
}

func writeFFmpegAV1Matroska(t testing.TB) string {
	t.Helper()
	tool := requireExternalTool(t, "ffmpeg")
	file := filepath.Join(t.TempDir(), "ffmpeg-av1.mkv")
	runExternalToolOrSkip(t, tool,
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

func writeFFmpegVP8Matroska(t testing.TB) string {
	t.Helper()
	tool := requireExternalTool(t, "ffmpeg")
	file := filepath.Join(t.TempDir(), "ffmpeg-vp8.mkv")
	runExternalToolOrSkip(t, tool,
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

func writeFFmpegVP9Matroska(t testing.TB) string {
	t.Helper()
	tool := requireExternalTool(t, "ffmpeg")
	file := filepath.Join(t.TempDir(), "ffmpeg-vp9.mkv")
	runExternalToolOrSkip(t, tool,
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

func writeFFmpegOpusMatroska(t testing.TB) string {
	t.Helper()
	tool := requireExternalTool(t, "ffmpeg")
	file := filepath.Join(t.TempDir(), "ffmpeg-opus.mkv")
	runExternalToolOrSkip(t, tool,
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

func writeOpusTimingMatroska(t *testing.T) string {
	t.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:         TrackAudio,
		Codec:        CodecOpus,
		Audio:        AudioConfig{SampleRate: 48000, Channels: 2},
		CodecPrivate: expectedOpusHeadWithPreSkip(2, 48000, 312),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: []byte{0xf8, 0xff, 0xfe}}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "opus-timing.mkv")
	if err := os.WriteFile(file, buffer.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return file
}

func remuxFirstMatroskaPacket(t testing.TB, file string, requireType TrackType, wantPrefix []byte) string {
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
		if len(wantPrefix) != 0 && !bytes.HasPrefix(packet.Data, wantPrefix) {
			t.Fatalf("packet data prefix = %x, want %x", packet.Data[:min(len(packet.Data), 8)], wantPrefix)
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
	out := filepath.Join(t.TempDir(), "remuxed.mkv")
	if err := os.WriteFile(out, buffer.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return out
}

func writeGeneratedPrivateMatroska(t *testing.T, codec Codec, data []byte) string {
	t.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: codec,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: data}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "generated.mkv")
	if err := os.WriteFile(file, buffer.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return file
}

func requireExternalTool(t testing.TB, name string) string {
	t.Helper()
	tool, ok := lookupExternalTool(name)
	if !ok {
		t.Skipf("%s not installed", name)
	}
	return tool
}

func lookupExternalTool(name string) (string, bool) {
	tool, err := exec.LookPath(name)
	return tool, err == nil
}

func runExternalTool(t testing.TB, tool string, args ...string) string {
	t.Helper()
	cmd := exec.Command(tool, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s failed: %v\n%s", filepath.Base(tool), strings.Join(args, " "), err, output)
	}
	return string(output)
}

func runExternalToolOrSkip(t testing.TB, tool string, args ...string) string {
	t.Helper()
	cmd := exec.Command(tool, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Skipf("%s %s unavailable: %v\n%s", filepath.Base(tool), strings.Join(args, " "), err, output)
	}
	return string(output)
}
