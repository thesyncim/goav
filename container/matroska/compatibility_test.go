package matroska

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestExternalFFProbeRecognizesMatroskaWebRTCCodecs(t *testing.T) {
	tool := requireExternalTool(t, "ffprobe")
	file := writeCompatibilityMatroska(t)
	output := runExternalTool(t, tool, "-v", "error", "-show_entries", "stream=codec_name,width,height,sample_rate,channels", "-of", "default=nw=1", file)
	for _, codec := range []string{"opus", "pcm_mulaw", "pcm_alaw", "av1", "h264", "vp9", "vp8"} {
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

func TestExternalFFProbeRecognizesGeneratedMSACMG711CodecPrivate(t *testing.T) {
	tool := requireExternalTool(t, "ffprobe")
	tests := []struct {
		name      string
		codec     Codec
		probe     string
		sample    []byte
		wantAudio []string
	}{
		{name: "pcmu", codec: CodecPCMU, probe: "pcm_mulaw", sample: []byte{0xff}, wantAudio: []string{"sample_rate=8000", "channels=1", "bits_per_sample=8"}},
		{name: "pcma", codec: CodecPCMA, probe: "pcm_alaw", sample: []byte{0xd5}, wantAudio: []string{"sample_rate=8000", "channels=1", "bits_per_sample=8"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file := writeGeneratedG711Matroska(t, tt.codec, tt.sample)
			output := runExternalTool(t, tool, "-v", "error", "-show_entries", "stream=codec_name,sample_rate,channels,bits_per_sample", "-of", "default=nw=1", file)
			if !strings.Contains(output, tt.probe) {
				t.Fatalf("ffprobe output missing %s:\n%s", tt.probe, output)
			}
			for _, want := range tt.wantAudio {
				if !strings.Contains(output, want) {
					t.Fatalf("ffprobe output missing %s:\n%s", want, output)
				}
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
		{name: "pcmu", codec: CodecPCMU, typ: TrackAudio, write: writeFFmpegPCMUMatroska},
		{name: "pcma", codec: CodecPCMA, typ: TrackAudio, write: writeFFmpegPCMAMatroska},
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
			if tt.codec == CodecOpus && (tracks[0].Audio.SampleRate != 48000 || tracks[0].Audio.Channels == 0) {
				t.Fatalf("audio = %+v, want 48000 Hz opus", tracks[0].Audio)
			}
			if (tt.codec == CodecPCMU || tt.codec == CodecPCMA) && (tracks[0].Audio.SampleRate != 8000 || tracks[0].Audio.Channels != 1 || tracks[0].Audio.BitDepth != 8) {
				t.Fatalf("audio = %+v, want 8000 Hz mono 8-bit G.711", tracks[0].Audio)
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
		{name: "pcmu", ffprobe: "pcm_mulaw", write: writeFFmpegPCMUMatroska, requireType: TrackAudio},
		{name: "pcma", ffprobe: "pcm_alaw", write: writeFFmpegPCMAMatroska, requireType: TrackAudio},
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

func TestExternalReadsAndRemuxesFFmpegMatroskaRecordings(t *testing.T) {
	tool := requireExternalTool(t, "ffprobe")
	tests := []struct {
		name  string
		codec Codec
		probe string
		write func(testing.TB) string
	}{
		{name: "h264", codec: CodecH264, probe: "h264", write: writeFFmpegH264OpusMatroskaRecording},
		{name: "av1", codec: CodecAV1, probe: "av1", write: writeFFmpegAV1OpusMatroskaRecording},
		{name: "vp9", codec: CodecVP9, probe: "vp9", write: writeFFmpegVP9OpusMatroskaRecording},
		{name: "vp8", codec: CodecVP8, probe: "vp8", write: writeFFmpegVP8OpusMatroskaRecording},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file := tt.write(t)
			tracks, stats := readMatroskaRecording(t, file)
			video := requireMatroskaRecordingTrack(t, tracks, tt.codec, TrackVideo)
			audio := requireMatroskaRecordingTrack(t, tracks, CodecOpus, TrackAudio)
			assertMatroskaRecordingStats(t, stats, video.ID, audio.ID)
			assertMatroskaRecordingMatchesFFProbe(t, "ffmpeg", tracks, stats, probeExternalMatroskaRecordingStats(t, tool, file))

			remuxed := remuxMatroskaRecording(t, file)
			output := runExternalTool(t, tool, "-v", "error", "-show_entries", "stream=codec_name", "-of", "default=nw=1", remuxed)
			for _, codec := range []string{tt.probe, "opus"} {
				if !strings.Contains(output, codec) {
					t.Fatalf("ffprobe output missing %s:\n%s", codec, output)
				}
			}
			remuxedTracks, remuxedStats := readMatroskaRecording(t, remuxed)
			remuxedVideo := requireMatroskaRecordingTrack(t, remuxedTracks, tt.codec, TrackVideo)
			remuxedAudio := requireMatroskaRecordingTrack(t, remuxedTracks, CodecOpus, TrackAudio)
			assertMatroskaRecordingStats(t, remuxedStats, remuxedVideo.ID, remuxedAudio.ID)
			assertMatroskaRecordingMatchesFFProbe(t, "remuxed ffmpeg", remuxedTracks, remuxedStats, probeExternalMatroskaRecordingStats(t, tool, remuxed))
		})
	}
}

func TestExternalFFProbeMatroskaPacketOracle(t *testing.T) {
	tool := requireExternalTool(t, "ffprobe")
	file, want := writePacketOracleMatroska(t)
	probe := probeExternalMatroskaPackets(t, tool, file)
	local := readLocalMatroskaPacketOracle(t, file)
	assertExternalMatroskaPackets(t, "ffprobe", probe, want)
	assertExternalMatroskaPackets(t, "local", local, want)
}

func TestExternalMKVMergeMatroskaPacketRoundTrip(t *testing.T) {
	mkvmerge := requireExternalTool(t, "mkvmerge")
	ffprobe := requireExternalTool(t, "ffprobe")
	file, _ := writePacketOracleMatroska(t)
	wantPayloads := readLocalMatroskaPacketPayloads(t, file)
	remuxed := filepath.Join(t.TempDir(), "mkvmerge-packet-oracle.mkv")
	runExternalTool(t, mkvmerge, "--quiet", "-o", remuxed, file)

	assertExternalMatroskaCodecPackets(t, "mkvmerge ffprobe", probeExternalMatroskaCodecPackets(t, ffprobe, remuxed), externalMatroskaCodecPacketsForPayloads(t, wantPayloads))
	assertLocalMatroskaPacketPayloads(t, "mkvmerge local", readLocalMatroskaPacketPayloads(t, remuxed), wantPayloads)
}

func TestExternalFFmpegMatroskaPacketRoundTrip(t *testing.T) {
	ffmpeg := requireExternalTool(t, "ffmpeg")
	ffprobe := requireExternalTool(t, "ffprobe")
	file, _ := writePacketOracleMatroska(t)
	wantPayloads := readLocalMatroskaPacketPayloads(t, file)
	remuxed := filepath.Join(t.TempDir(), "ffmpeg-packet-oracle.mkv")
	runExternalToolOrSkip(t, ffmpeg, "-y", "-hide_banner", "-loglevel", "error", "-i", file, "-map", "0", "-c", "copy", remuxed)

	assertExternalMatroskaCodecPackets(t, "ffmpeg ffprobe", probeExternalMatroskaCodecPackets(t, ffprobe, remuxed), externalMatroskaCodecPacketsForPayloads(t, wantPayloads))
	assertLocalMatroskaPacketPayloads(t, "ffmpeg local", readLocalMatroskaPacketPayloads(t, remuxed), wantPayloads)
}

func TestExternalMKVExtractMatroskaTimestamps(t *testing.T) {
	mkvextract := requireExternalTool(t, "mkvextract")
	file, want := writePacketOracleMatroska(t)
	assertMKVExtractMatroskaTimestamps(t, mkvextract, file, expectedMatroskaTimestampsByStream(want))
}

func TestExternalMKVExtractMatroskaTrackPayloads(t *testing.T) {
	mkvextract := requireExternalTool(t, "mkvextract")
	file, _ := writePacketOracleMatroska(t)
	want := expectedMatroskaExtractedTrackPayloads(t, readLocalMatroskaPacketPayloads(t, file))
	assertMKVExtractMatroskaTrackPayloads(t, mkvextract, file, want)
}

func writeFFmpegAV1OpusMatroskaRecording(t testing.TB) string {
	t.Helper()
	tool := requireExternalTool(t, "ffmpeg")
	file := filepath.Join(t.TempDir(), "ffmpeg-av1-opus-recording.mkv")
	runExternalToolOrSkip(t, tool,
		"-y",
		"-hide_banner",
		"-loglevel", "error",
		"-f", "lavfi",
		"-i", "testsrc=size=16x16:rate=5:duration=1",
		"-f", "lavfi",
		"-i", "sine=frequency=1000:sample_rate=48000:duration=1",
		"-map", "0:v:0",
		"-map", "1:a:0",
		"-shortest",
		"-c:v", "libsvtav1",
		"-preset", "13",
		"-crf", "50",
		"-g", "5",
		"-c:a", "libopus",
		"-application", "voip",
		"-frame_duration", "20",
		file,
	)
	return file
}

func writeFFmpegVP9OpusMatroskaRecording(t testing.TB) string {
	t.Helper()
	tool := requireExternalTool(t, "ffmpeg")
	file := filepath.Join(t.TempDir(), "ffmpeg-vp9-opus-recording.mkv")
	runExternalToolOrSkip(t, tool,
		"-y",
		"-hide_banner",
		"-loglevel", "error",
		"-f", "lavfi",
		"-i", "testsrc=size=16x16:rate=5:duration=1",
		"-f", "lavfi",
		"-i", "sine=frequency=1000:sample_rate=48000:duration=1",
		"-map", "0:v:0",
		"-map", "1:a:0",
		"-shortest",
		"-c:v", "libvpx-vp9",
		"-deadline", "realtime",
		"-cpu-used", "8",
		"-b:v", "100k",
		"-g", "5",
		"-c:a", "libopus",
		"-application", "voip",
		"-frame_duration", "20",
		file,
	)
	return file
}

func writeFFmpegVP8OpusMatroskaRecording(t testing.TB) string {
	t.Helper()
	tool := requireExternalTool(t, "ffmpeg")
	file := filepath.Join(t.TempDir(), "ffmpeg-vp8-opus-recording.mkv")
	runExternalToolOrSkip(t, tool,
		"-y",
		"-hide_banner",
		"-loglevel", "error",
		"-f", "lavfi",
		"-i", "testsrc=size=16x16:rate=5:duration=1",
		"-f", "lavfi",
		"-i", "sine=frequency=1000:sample_rate=48000:duration=1",
		"-map", "0:v:0",
		"-map", "1:a:0",
		"-shortest",
		"-c:v", "libvpx",
		"-deadline", "realtime",
		"-cpu-used", "8",
		"-b:v", "100k",
		"-g", "5",
		"-c:a", "libopus",
		"-application", "voip",
		"-frame_duration", "20",
		file,
	)
	return file
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
	pcmuID, err := muxer.AddTrack(Track{
		Type:  TrackAudio,
		Codec: CodecPCMU,
		Audio: AudioConfig{SampleRate: 8000, Channels: 1, BitDepth: 8},
	})
	if err != nil {
		t.Fatal(err)
	}
	pcmaID, err := muxer.AddTrack(Track{
		Type:  TrackAudio,
		Codec: CodecPCMA,
		Audio: AudioConfig{SampleRate: 8000, Channels: 1, BitDepth: 8},
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
		{TrackID: pcmuID, TimeNS: 0, Keyframe: true, Data: []byte{0xff}},
		{TrackID: pcmaID, TimeNS: 0, Keyframe: true, Data: []byte{0xd5}},
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

type externalMatroskaPacket struct {
	StreamIndex int
	TimeNS      int64
	DurationNS  int64
	Keyframe    bool
	Size        int
}

type localMatroskaPacketPayload struct {
	StreamIndex int
	Codec       Codec
	Data        []byte
}

type externalMatroskaCodecPacket struct {
	Codec string
	Size  int
}

func writePacketOracleMatroska(t testing.TB) (string, []externalMatroskaPacket) {
	t.Helper()
	file := filepath.Join(t.TempDir(), "packet-oracle.mkv")
	output, err := os.Create(file)
	if err != nil {
		t.Fatal(err)
	}
	muxer, err := NewMuxer(output, MuxerOptions{})
	if err != nil {
		_ = output.Close()
		t.Fatal(err)
	}
	type oracleTrack struct {
		id      uint32
		stream  int
		track   Track
		payload []byte
		size    func(testing.TB, []byte) int
	}
	rawSize := func(tb testing.TB, data []byte) int {
		tb.Helper()
		return len(data)
	}
	h264Size := func(tb testing.TB, data []byte) int {
		tb.Helper()
		size, err := h264AnnexBToAVCSize(data, 4)
		if err != nil {
			tb.Fatal(err)
		}
		return size
	}
	tracks := []oracleTrack{
		{
			stream:  0,
			track:   Track{Type: TrackAudio, Codec: CodecOpus, Audio: AudioConfig{SampleRate: 48000, Channels: 2}},
			payload: []byte{0xf8, 0xff, 0xfe},
			size:    rawSize,
		},
		{
			stream:  1,
			track:   Track{Type: TrackVideo, Codec: CodecAV1, Video: VideoConfig{Width: 16, Height: 16}, CodecPrivate: av1CodecConfig()},
			payload: av1SequenceHeaderOBU(),
			size:    rawSize,
		},
		{
			stream:  2,
			track:   Track{Type: TrackVideo, Codec: CodecH264, Video: VideoConfig{Width: 16, Height: 16}, CodecPrivate: h264AVCDecoderConfig()},
			payload: h264AnnexBAccessUnit(),
			size:    h264Size,
		},
		{
			stream:  3,
			track:   Track{Type: TrackVideo, Codec: CodecVP9, Video: VideoConfig{Width: 16, Height: 16}},
			payload: []byte{0x83, 0x49, 0x83},
			size:    rawSize,
		},
		{
			stream:  4,
			track:   Track{Type: TrackVideo, Codec: CodecVP8, Video: VideoConfig{Width: 16, Height: 16}},
			payload: []byte{0x10, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00},
			size:    rawSize,
		},
	}
	for i := range tracks {
		tracks[i].id, err = muxer.AddTrack(tracks[i].track)
		if err != nil {
			_ = output.Close()
			t.Fatal(err)
		}
	}
	want := make([]externalMatroskaPacket, 0, len(tracks)*2)
	for cycle := 0; cycle < 2; cycle++ {
		timeNS := int64(cycle) * 20_000_000
		for i := range tracks {
			keyframe := true
			if err := muxer.WritePacket(Packet{
				TrackID:    tracks[i].id,
				TimeNS:     timeNS,
				DurationNS: 20_000_000,
				Keyframe:   keyframe,
				Data:       tracks[i].payload,
			}); err != nil {
				_ = output.Close()
				t.Fatalf("write packet stream %d cycle %d: %v", tracks[i].stream, cycle, err)
			}
			want = append(want, externalMatroskaPacket{
				StreamIndex: tracks[i].stream,
				TimeNS:      timeNS,
				DurationNS:  20_000_000,
				Keyframe:    keyframe,
				Size:        tracks[i].size(t, tracks[i].payload),
			})
		}
	}
	if err := muxer.Close(); err != nil {
		_ = output.Close()
		t.Fatal(err)
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}
	return file, want
}

func probeExternalMatroskaPackets(t testing.TB, tool string, file string) []externalMatroskaPacket {
	t.Helper()
	output := runExternalTool(t, tool, "-v", "quiet", "-show_entries", "packet=stream_index,pts_time,duration_time,flags,size", "-of", "json", file)
	var decoded struct {
		Packets []struct {
			StreamIndex  int    `json:"stream_index"`
			PTSTime      string `json:"pts_time"`
			DurationTime string `json:"duration_time"`
			Flags        string `json:"flags"`
			Size         string `json:"size"`
		} `json:"packets"`
	}
	if err := json.Unmarshal([]byte(output), &decoded); err != nil {
		t.Fatalf("decode ffprobe packets: %v\n%s", err, output)
	}
	packets := make([]externalMatroskaPacket, 0, len(decoded.Packets))
	for i := range decoded.Packets {
		timeNS, err := parseExternalMatroskaSecondsNS(decoded.Packets[i].PTSTime)
		if err != nil {
			t.Fatalf("packet %d pts_time %q: %v", i, decoded.Packets[i].PTSTime, err)
		}
		durationNS, err := parseExternalMatroskaSecondsNS(decoded.Packets[i].DurationTime)
		if err != nil {
			t.Fatalf("packet %d duration_time %q: %v", i, decoded.Packets[i].DurationTime, err)
		}
		size, err := strconv.Atoi(decoded.Packets[i].Size)
		if err != nil {
			t.Fatalf("packet %d size %q: %v", i, decoded.Packets[i].Size, err)
		}
		packets = append(packets, externalMatroskaPacket{
			StreamIndex: decoded.Packets[i].StreamIndex,
			TimeNS:      timeNS,
			DurationNS:  durationNS,
			Keyframe:    strings.Contains(decoded.Packets[i].Flags, "K"),
			Size:        size,
		})
	}
	return packets
}

func probeExternalMatroskaCodecPackets(t testing.TB, tool string, file string) []externalMatroskaCodecPacket {
	t.Helper()
	output := runExternalTool(t, tool, "-v", "quiet", "-show_entries", "packet=stream_index,size", "-show_entries", "stream=index,codec_name", "-of", "json", file)
	var decoded struct {
		Packets []struct {
			StreamIndex int    `json:"stream_index"`
			Size        string `json:"size"`
		} `json:"packets"`
		Streams []struct {
			Index     int    `json:"index"`
			CodecName string `json:"codec_name"`
		} `json:"streams"`
	}
	if err := json.Unmarshal([]byte(output), &decoded); err != nil {
		t.Fatalf("decode ffprobe packets: %v\n%s", err, output)
	}
	streams := make(map[int]string, len(decoded.Streams))
	for i := range decoded.Streams {
		streams[decoded.Streams[i].Index] = decoded.Streams[i].CodecName
	}
	packets := make([]externalMatroskaCodecPacket, 0, len(decoded.Packets))
	for i := range decoded.Packets {
		codec, ok := streams[decoded.Packets[i].StreamIndex]
		if !ok {
			t.Fatalf("packet %d references unknown stream %d", i, decoded.Packets[i].StreamIndex)
		}
		size, err := strconv.Atoi(decoded.Packets[i].Size)
		if err != nil {
			t.Fatalf("packet %d size %q: %v", i, decoded.Packets[i].Size, err)
		}
		packets = append(packets, externalMatroskaCodecPacket{Codec: codec, Size: size})
	}
	return packets
}

func readLocalMatroskaPacketOracle(t testing.TB, file string) []externalMatroskaPacket {
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
	streams := make(map[uint32]int, len(tracks))
	for i := range tracks {
		streams[tracks[i].ID] = i
	}
	var packets []externalMatroskaPacket
	packet := Packet{Data: make([]byte, 0, 1<<20)}
	for {
		err := demuxer.ReadPacket(&packet)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		stream, ok := streams[packet.TrackID]
		if !ok {
			t.Fatalf("packet references unknown track %d", packet.TrackID)
		}
		size := len(packet.Data)
		if tracks[stream].Codec == CodecH264 {
			size, err = h264AnnexBToAVCSize(packet.Data, 4)
			if err != nil {
				t.Fatal(err)
			}
		}
		packets = append(packets, externalMatroskaPacket{
			StreamIndex: stream,
			TimeNS:      packet.TimeNS,
			DurationNS:  packet.DurationNS,
			Keyframe:    packet.Keyframe,
			Size:        size,
		})
	}
	return packets
}

func readLocalMatroskaPacketPayloads(t testing.TB, file string) []localMatroskaPacketPayload {
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
	streams := make(map[uint32]int, len(tracks))
	for i := range tracks {
		streams[tracks[i].ID] = i
	}
	var packets []localMatroskaPacketPayload
	packet := Packet{Data: make([]byte, 0, 1<<20)}
	for {
		err := demuxer.ReadPacket(&packet)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		stream, ok := streams[packet.TrackID]
		if !ok {
			t.Fatalf("packet references unknown track %d", packet.TrackID)
		}
		packets = append(packets, localMatroskaPacketPayload{
			StreamIndex: stream,
			Codec:       tracks[stream].Codec,
			Data:        append([]byte(nil), packet.Data...),
		})
	}
	return packets
}

func assertExternalMatroskaPackets(t testing.TB, name string, got []externalMatroskaPacket, want []externalMatroskaPacket) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s packets = %d, want %d: %+v", name, len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s packet %d = %+v, want %+v", name, i, got[i], want[i])
		}
	}
}

func assertMKVExtractMatroskaTimestamps(t testing.TB, tool string, file string, want map[int][]int64) {
	t.Helper()
	outDir := t.TempDir()
	args := []string{file, "timestamps_v2"}
	paths := make(map[int]string, len(want))
	for stream := range want {
		path := filepath.Join(outDir, fmt.Sprintf("track-%d.timestamps.txt", stream))
		paths[stream] = path
		args = append(args, fmt.Sprintf("%d:%s", stream, path))
	}
	runExternalTool(t, tool, args...)
	for stream, path := range paths {
		got := readMatroskaTimestampsV2(t, path)
		if !equalInt64Slices(got, want[stream]) {
			t.Fatalf("mkvextract stream %d timestamps = %v, want %v", stream, got, want[stream])
		}
	}
}

func expectedMatroskaTimestampsByStream(packets []externalMatroskaPacket) map[int][]int64 {
	timestamps := make(map[int][]int64)
	lastEnd := make(map[int]int64)
	for i := range packets {
		timestamps[packets[i].StreamIndex] = append(timestamps[packets[i].StreamIndex], packets[i].TimeNS)
		if packets[i].DurationNS > 0 {
			lastEnd[packets[i].StreamIndex] = packets[i].TimeNS + packets[i].DurationNS
		}
	}
	for stream, end := range lastEnd {
		timestamps[stream] = append(timestamps[stream], end)
	}
	return timestamps
}

func readMatroskaTimestampsV2(t testing.TB, file string) []int64 {
	t.Helper()
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	var timestamps []int64
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		timeNS, err := parseMatroskaTimestampV2NS(line)
		if err != nil {
			t.Fatalf("parse %s line %q: %v", file, line, err)
		}
		timestamps = append(timestamps, timeNS)
	}
	return timestamps
}

func parseMatroskaTimestampV2NS(value string) (int64, error) {
	parts := strings.SplitN(value, ".", 2)
	milliseconds, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, err
	}
	nanoseconds := milliseconds * 1_000_000
	if len(parts) == 2 {
		frac := parts[1]
		if len(frac) > 6 {
			frac = frac[:6]
		}
		for len(frac) < 6 {
			frac += "0"
		}
		fracNS, err := strconv.ParseInt(frac, 10, 64)
		if err != nil {
			return 0, err
		}
		nanoseconds += fracNS
	}
	return nanoseconds, nil
}

func equalInt64Slices(left []int64, right []int64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func assertMKVExtractMatroskaTrackPayloads(t testing.TB, tool string, file string, want map[int][][][]byte) {
	t.Helper()
	outDir := t.TempDir()
	args := []string{file, "tracks", "--raw"}
	paths := make(map[int]string, len(want))
	for stream := range want {
		path := filepath.Join(outDir, fmt.Sprintf("track-%d.raw", stream))
		paths[stream] = path
		args = append(args, fmt.Sprintf("%d:%s", stream, path))
	}
	runExternalTool(t, tool, args...)
	for stream, path := range paths {
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		assertExtractedMatroskaPayloads(t, stream, got, want[stream])
	}
}

func expectedMatroskaExtractedTrackPayloads(t testing.TB, payloads []localMatroskaPacketPayload) map[int][][][]byte {
	t.Helper()
	expected := make(map[int][][][]byte)
	for i := range payloads {
		expected[payloads[i].StreamIndex] = append(expected[payloads[i].StreamIndex], matroskaExtractedPayloadAlternatives(t, payloads[i]))
	}
	return expected
}

func matroskaExtractedPayloadAlternatives(t testing.TB, payload localMatroskaPacketPayload) [][]byte {
	t.Helper()
	alternatives := [][]byte{payload.Data}
	if payload.Codec != CodecH264 {
		return alternatives
	}
	var buffer bytes.Buffer
	var scratch [16]byte
	if err := h264WriteAnnexBAsAVC(&buffer, payload.Data, 4, &scratch); err != nil {
		t.Fatal(err)
	}
	return append(alternatives, buffer.Bytes())
}

func assertExtractedMatroskaPayloads(t testing.TB, stream int, extracted []byte, want [][][]byte) {
	t.Helper()
	offset := 0
	for i := range want {
		found := false
		for j := range want[i] {
			index := bytes.Index(extracted[offset:], want[i][j])
			if index < 0 {
				continue
			}
			offset += index + len(want[i][j])
			found = true
			break
		}
		if !found {
			t.Fatalf("mkvextract stream %d missing packet %d alternatives %x in extracted payload %x", stream, i, want[i], extracted)
		}
	}
}

func assertExternalMatroskaCodecPackets(t testing.TB, name string, got []externalMatroskaCodecPacket, want []externalMatroskaCodecPacket) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s packets = %d, want %d: %+v", name, len(got), len(want), got)
	}
	matched := make([]bool, len(got))
	for i := range want {
		found := false
		for j := range got {
			if matched[j] || got[j] != want[i] {
				continue
			}
			matched[j] = true
			found = true
			break
		}
		if !found {
			t.Fatalf("%s missing packet %+v in %+v", name, want[i], got)
		}
	}
}

func assertLocalMatroskaPacketPayloads(t testing.TB, name string, got []localMatroskaPacketPayload, want []localMatroskaPacketPayload) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s packets = %d, want %d", name, len(got), len(want))
	}
	matched := make([]bool, len(got))
	for i := range want {
		found := false
		for j := range got {
			if matched[j] || got[j].Codec != want[i].Codec || !bytes.Equal(got[j].Data, want[i].Data) {
				continue
			}
			matched[j] = true
			found = true
			break
		}
		if !found {
			t.Fatalf("%s missing packet codec=%d data=%x in %+v", name, want[i].Codec, want[i].Data, got)
		}
	}
}

func externalMatroskaCodecPacketsForPayloads(t testing.TB, payloads []localMatroskaPacketPayload) []externalMatroskaCodecPacket {
	t.Helper()
	packets := make([]externalMatroskaCodecPacket, 0, len(payloads))
	for i := range payloads {
		size := len(payloads[i].Data)
		if payloads[i].Codec == CodecH264 {
			var err error
			size, err = h264AnnexBToAVCSize(payloads[i].Data, 4)
			if err != nil {
				t.Fatal(err)
			}
		}
		packets = append(packets, externalMatroskaCodecPacket{
			Codec: externalMatroskaCodecName(t, payloads[i].Codec),
			Size:  size,
		})
	}
	return packets
}

func externalMatroskaCodecName(t testing.TB, codec Codec) string {
	t.Helper()
	switch codec {
	case CodecOpus:
		return "opus"
	case CodecAV1:
		return "av1"
	case CodecH264:
		return "h264"
	case CodecVP9:
		return "vp9"
	case CodecVP8:
		return "vp8"
	default:
		t.Fatalf("unsupported external codec %d", codec)
		return ""
	}
}

func parseExternalMatroskaSecondsNS(value string) (int64, error) {
	if value == "" || value == "N/A" {
		return 0, nil
	}
	parts := strings.SplitN(value, ".", 2)
	seconds, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, err
	}
	var nanos int64
	if len(parts) == 2 {
		frac := parts[1]
		if len(frac) > 9 {
			frac = frac[:9]
		}
		for len(frac) < 9 {
			frac += "0"
		}
		nanos, err = strconv.ParseInt(frac, 10, 64)
		if err != nil {
			return 0, err
		}
	}
	return seconds*1_000_000_000 + nanos, nil
}

func writeGeneratedG711Matroska(t *testing.T, codec Codec, data []byte) string {
	t.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackAudio,
		Codec: codec,
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
	file := filepath.Join(t.TempDir(), "generated-g711.mkv")
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

func writeFFmpegPCMUMatroska(t testing.TB) string {
	t.Helper()
	tool := requireExternalTool(t, "ffmpeg")
	file := filepath.Join(t.TempDir(), "ffmpeg-pcmu.mkv")
	runExternalToolOrSkip(t, tool,
		"-y",
		"-hide_banner",
		"-loglevel", "error",
		"-f", "lavfi",
		"-i", "sine=frequency=1000:sample_rate=8000:duration=0.02",
		"-c:a", "pcm_mulaw",
		file,
	)
	return file
}

func writeFFmpegPCMAMatroska(t testing.TB) string {
	t.Helper()
	tool := requireExternalTool(t, "ffmpeg")
	file := filepath.Join(t.TempDir(), "ffmpeg-pcma.mkv")
	runExternalToolOrSkip(t, tool,
		"-y",
		"-hide_banner",
		"-loglevel", "error",
		"-f", "lavfi",
		"-i", "sine=frequency=1000:sample_rate=8000:duration=0.02",
		"-c:a", "pcm_alaw",
		file,
	)
	return file
}

func writeFFmpegH264OpusMatroskaRecording(t testing.TB) string {
	t.Helper()
	tool := requireExternalTool(t, "ffmpeg")
	file := filepath.Join(t.TempDir(), "ffmpeg-h264-opus-recording.mkv")
	runExternalToolOrSkip(t, tool,
		"-y",
		"-hide_banner",
		"-loglevel", "error",
		"-f", "lavfi",
		"-i", "testsrc=size=16x16:rate=5:duration=1",
		"-f", "lavfi",
		"-i", "sine=frequency=1000:sample_rate=48000:duration=1",
		"-map", "0:v:0",
		"-map", "1:a:0",
		"-shortest",
		"-c:v", "libx264",
		"-preset", "ultrafast",
		"-tune", "zerolatency",
		"-pix_fmt", "yuv420p",
		"-g", "5",
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

type externalMatroskaRecordingStats struct {
	packets   map[uint32]int
	keyframes map[uint32]int
	lastTime  map[uint32]int64
}

type probedMatroskaRecordingStats struct {
	packets   int
	keyframes int
}

func readMatroskaRecording(t testing.TB, file string) ([]Track, externalMatroskaRecordingStats) {
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
	stats := externalMatroskaRecordingStats{
		packets:   make(map[uint32]int, len(tracks)),
		keyframes: make(map[uint32]int, len(tracks)),
		lastTime:  make(map[uint32]int64, len(tracks)),
	}
	seen := make(map[uint32]bool, len(tracks))
	packet := Packet{Data: make([]byte, 0, 1<<20)}
	for {
		err := demuxer.ReadPacket(&packet)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if len(packet.Data) == 0 {
			t.Fatalf("empty packet from %s for track %d", file, packet.TrackID)
		}
		if seen[packet.TrackID] && packet.TimeNS < stats.lastTime[packet.TrackID] {
			t.Fatalf("track %d time moved backward: got %d after %d", packet.TrackID, packet.TimeNS, stats.lastTime[packet.TrackID])
		}
		seen[packet.TrackID] = true
		stats.lastTime[packet.TrackID] = packet.TimeNS
		stats.packets[packet.TrackID]++
		if packet.Keyframe {
			stats.keyframes[packet.TrackID]++
		}
	}
	return tracks, stats
}

func probeExternalMatroskaRecordingStats(t testing.TB, tool string, file string) map[string]probedMatroskaRecordingStats {
	t.Helper()
	output := runExternalTool(t, tool, "-v", "quiet", "-show_entries", "packet=stream_index,flags", "-show_entries", "stream=index,codec_name", "-of", "json", file)
	var decoded struct {
		Packets []struct {
			StreamIndex int    `json:"stream_index"`
			Flags       string `json:"flags"`
		} `json:"packets"`
		Streams []struct {
			Index     int    `json:"index"`
			CodecName string `json:"codec_name"`
		} `json:"streams"`
	}
	if err := json.Unmarshal([]byte(output), &decoded); err != nil {
		t.Fatalf("decode ffprobe recording packets: %v\n%s", err, output)
	}
	streams := make(map[int]string, len(decoded.Streams))
	for i := range decoded.Streams {
		streams[decoded.Streams[i].Index] = decoded.Streams[i].CodecName
	}
	stats := make(map[string]probedMatroskaRecordingStats, len(decoded.Streams))
	for i := range decoded.Packets {
		codec, ok := streams[decoded.Packets[i].StreamIndex]
		if !ok {
			t.Fatalf("packet %d references unknown stream %d", i, decoded.Packets[i].StreamIndex)
		}
		entry := stats[codec]
		entry.packets++
		if strings.Contains(decoded.Packets[i].Flags, "K") {
			entry.keyframes++
		}
		stats[codec] = entry
	}
	return stats
}

func requireMatroskaRecordingTrack(t testing.TB, tracks []Track, codec Codec, typ TrackType) Track {
	t.Helper()
	for i := range tracks {
		if tracks[i].Codec == codec && tracks[i].Type == typ {
			return tracks[i]
		}
	}
	t.Fatalf("missing %v %v track in %+v", codec, typ, tracks)
	return Track{}
}

func assertMatroskaRecordingStats(t testing.TB, stats externalMatroskaRecordingStats, videoID uint32, audioID uint32) {
	t.Helper()
	if stats.packets[videoID] < 3 {
		t.Fatalf("video packets = %d, want at least 3", stats.packets[videoID])
	}
	if stats.packets[audioID] < 10 {
		t.Fatalf("audio packets = %d, want at least 10", stats.packets[audioID])
	}
	if stats.keyframes[videoID] == 0 {
		t.Fatalf("video keyframes = 0")
	}
	if stats.lastTime[videoID] <= 0 || stats.lastTime[audioID] <= 0 {
		t.Fatalf("last times = %+v, want positive audio/video timelines", stats.lastTime)
	}
}

func assertMatroskaRecordingMatchesFFProbe(t testing.TB, name string, tracks []Track, local externalMatroskaRecordingStats, probe map[string]probedMatroskaRecordingStats) {
	t.Helper()
	for i := range tracks {
		codec := externalMatroskaCodecName(t, tracks[i].Codec)
		probed, ok := probe[codec]
		if !ok {
			t.Fatalf("%s missing ffprobe stats for %s in %+v", name, codec, probe)
		}
		if local.packets[tracks[i].ID] != probed.packets {
			t.Fatalf("%s %s packets = %d, want ffprobe %d", name, codec, local.packets[tracks[i].ID], probed.packets)
		}
		if local.keyframes[tracks[i].ID] != probed.keyframes {
			t.Fatalf("%s %s keyframes = %d, want ffprobe %d", name, codec, local.keyframes[tracks[i].ID], probed.keyframes)
		}
	}
}

func remuxMatroskaRecording(t testing.TB, file string) string {
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
	out := filepath.Join(t.TempDir(), "recording-remux.mkv")
	output, err := os.Create(out)
	if err != nil {
		t.Fatal(err)
	}
	muxer, err := NewMuxer(output, MuxerOptions{})
	if err != nil {
		_ = output.Close()
		t.Fatal(err)
	}
	trackIDs := make(map[uint32]uint32, len(demuxer.Tracks()))
	for _, track := range demuxer.Tracks() {
		id, err := muxer.AddTrack(track)
		if err != nil {
			_ = output.Close()
			t.Fatal(err)
		}
		trackIDs[track.ID] = id
	}
	packet := Packet{Data: make([]byte, 0, 1<<20)}
	for {
		err := demuxer.ReadPacket(&packet)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			_ = output.Close()
			t.Fatal(err)
		}
		id, ok := trackIDs[packet.TrackID]
		if !ok {
			_ = output.Close()
			t.Fatalf("packet references unknown track %d", packet.TrackID)
		}
		packet.TrackID = id
		if err := muxer.WritePacket(packet); err != nil {
			_ = output.Close()
			t.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		_ = output.Close()
		t.Fatal(err)
	}
	if err := output.Close(); err != nil {
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
