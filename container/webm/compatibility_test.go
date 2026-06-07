package webm

import (
	"bytes"
	"encoding/json"
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

func TestExternalReadsAndRemuxesFFmpegWebMRecordings(t *testing.T) {
	tool := requireTool(t, "ffprobe")
	tests := []struct {
		name  string
		codec Codec
		probe string
		write func(testing.TB) string
	}{
		{name: "vp8", codec: CodecVP8, probe: "vp8", write: writeFFmpegVP8OpusWebMRecording},
		{name: "vp9", codec: CodecVP9, probe: "vp9", write: writeFFmpegVP9OpusWebMRecording},
		{name: "av1", codec: CodecAV1, probe: "av1", write: writeFFmpegAV1OpusWebMRecording},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file := tt.write(t)
			tracks, stats := readWebMRecording(t, file)
			video := requireWebMRecordingTrack(t, tracks, tt.codec, TrackVideo)
			audio := requireWebMRecordingTrack(t, tracks, CodecOpus, TrackAudio)
			assertWebMRecordingStats(t, stats, video.ID, audio.ID)
			assertWebMRecordingMatchesFFProbe(t, "ffmpeg", tracks, stats, probeExternalWebMRecordingStats(t, tool, file))

			remuxed := remuxWebMRecording(t, file)
			output := runExternal(t, tool, "-v", "error", "-show_entries", "stream=codec_name", "-of", "default=nw=1", remuxed)
			for _, codec := range []string{tt.probe, "opus"} {
				if !strings.Contains(output, codec) {
					t.Fatalf("ffprobe output missing %s:\n%s", codec, output)
				}
			}
			remuxedTracks, remuxedStats := readWebMRecording(t, remuxed)
			remuxedVideo := requireWebMRecordingTrack(t, remuxedTracks, tt.codec, TrackVideo)
			remuxedAudio := requireWebMRecordingTrack(t, remuxedTracks, CodecOpus, TrackAudio)
			assertWebMRecordingStats(t, remuxedStats, remuxedVideo.ID, remuxedAudio.ID)
			assertWebMRecordingMatchesFFProbe(t, "remuxed ffmpeg", remuxedTracks, remuxedStats, probeExternalWebMRecordingStats(t, tool, remuxed))
		})
	}
}

func TestExternalFFProbeWebMPacketOracle(t *testing.T) {
	tool := requireTool(t, "ffprobe")
	file, want := writePacketOracleWebM(t)
	probe := probeExternalWebMPackets(t, tool, file)
	local := readLocalWebMPacketOracle(t, file)
	assertExternalWebMPackets(t, "ffprobe", probe, want)
	assertExternalWebMPackets(t, "local", local, want)
}

func TestExternalMKVMergeWebMPacketRoundTrip(t *testing.T) {
	mkvmerge := requireTool(t, "mkvmerge")
	ffprobe := requireTool(t, "ffprobe")
	file, _ := writePacketOracleWebM(t)
	wantPayloads := readLocalWebMPacketPayloads(t, file)
	remuxed := filepath.Join(t.TempDir(), "mkvmerge-packet-oracle.webm")
	runExternal(t, mkvmerge, "--quiet", "--webm", "-o", remuxed, file)

	assertExternalWebMCodecPackets(t, "mkvmerge ffprobe", probeExternalWebMCodecPackets(t, ffprobe, remuxed), externalWebMCodecPacketsForPayloads(t, wantPayloads))
	assertLocalWebMPacketPayloads(t, "mkvmerge local", readLocalWebMPacketPayloads(t, remuxed), wantPayloads)
}

func TestExternalFFmpegWebMPacketRoundTrip(t *testing.T) {
	ffmpeg := requireTool(t, "ffmpeg")
	ffprobe := requireTool(t, "ffprobe")
	file, _ := writePacketOracleWebM(t)
	wantPayloads := readLocalWebMPacketPayloads(t, file)
	remuxed := filepath.Join(t.TempDir(), "ffmpeg-packet-oracle.webm")
	runExternalOrSkip(t, ffmpeg, "-y", "-hide_banner", "-loglevel", "error", "-i", file, "-map", "0", "-c", "copy", remuxed)

	assertExternalWebMCodecPackets(t, "ffmpeg ffprobe", probeExternalWebMCodecPackets(t, ffprobe, remuxed), externalWebMCodecPacketsForPayloads(t, wantPayloads))
	assertLocalWebMPacketPayloads(t, "ffmpeg local", readLocalWebMPacketPayloads(t, remuxed), wantPayloads)
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

type externalWebMPacket struct {
	StreamIndex int
	TimeNS      int64
	DurationNS  int64
	Keyframe    bool
	Size        int
}

type localWebMPacketPayload struct {
	Codec Codec
	Data  []byte
}

type externalWebMCodecPacket struct {
	Codec string
	Size  int
}

func writePacketOracleWebM(t testing.TB) (string, []externalWebMPacket) {
	t.Helper()
	file := filepath.Join(t.TempDir(), "packet-oracle.webm")
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
	}
	tracks := []oracleTrack{
		{
			stream:  0,
			track:   Track{Type: TrackVideo, Codec: CodecVP8, Video: VideoConfig{Width: 16, Height: 16}},
			payload: []byte{0x10, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00},
		},
		{
			stream:  1,
			track:   Track{Type: TrackVideo, Codec: CodecVP9, Video: VideoConfig{Width: 16, Height: 16}},
			payload: []byte{0x83, 0x49, 0x83},
		},
		{
			stream:  2,
			track:   Track{Type: TrackVideo, Codec: CodecAV1, Video: VideoConfig{Width: 16, Height: 16}, CodecPrivate: webmAV1CodecConfig()},
			payload: webmAV1SequenceHeaderOBU(),
		},
		{
			stream:  3,
			track:   Track{Type: TrackAudio, Codec: CodecOpus, Audio: AudioConfig{SampleRate: 48000, Channels: 2}},
			payload: []byte{0xf8, 0xff, 0xfe},
		},
	}
	for i := range tracks {
		tracks[i].id, err = muxer.AddTrack(tracks[i].track)
		if err != nil {
			_ = output.Close()
			t.Fatal(err)
		}
	}
	want := make([]externalWebMPacket, 0, len(tracks)*2)
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
			want = append(want, externalWebMPacket{
				StreamIndex: tracks[i].stream,
				TimeNS:      timeNS,
				DurationNS:  20_000_000,
				Keyframe:    keyframe,
				Size:        len(tracks[i].payload),
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

func probeExternalWebMPackets(t testing.TB, tool string, file string) []externalWebMPacket {
	t.Helper()
	output := runExternal(t, tool, "-v", "quiet", "-show_entries", "packet=stream_index,pts_time,duration_time,flags,size", "-of", "json", file)
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
	packets := make([]externalWebMPacket, 0, len(decoded.Packets))
	for i := range decoded.Packets {
		timeNS, err := parseExternalWebMSecondsNS(decoded.Packets[i].PTSTime)
		if err != nil {
			t.Fatalf("packet %d pts_time %q: %v", i, decoded.Packets[i].PTSTime, err)
		}
		durationNS, err := parseExternalWebMSecondsNS(decoded.Packets[i].DurationTime)
		if err != nil {
			t.Fatalf("packet %d duration_time %q: %v", i, decoded.Packets[i].DurationTime, err)
		}
		size, err := strconv.Atoi(decoded.Packets[i].Size)
		if err != nil {
			t.Fatalf("packet %d size %q: %v", i, decoded.Packets[i].Size, err)
		}
		packets = append(packets, externalWebMPacket{
			StreamIndex: decoded.Packets[i].StreamIndex,
			TimeNS:      timeNS,
			DurationNS:  durationNS,
			Keyframe:    strings.Contains(decoded.Packets[i].Flags, "K"),
			Size:        size,
		})
	}
	return packets
}

func probeExternalWebMCodecPackets(t testing.TB, tool string, file string) []externalWebMCodecPacket {
	t.Helper()
	output := runExternal(t, tool, "-v", "quiet", "-show_entries", "packet=stream_index,size", "-show_entries", "stream=index,codec_name", "-of", "json", file)
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
	packets := make([]externalWebMCodecPacket, 0, len(decoded.Packets))
	for i := range decoded.Packets {
		codec, ok := streams[decoded.Packets[i].StreamIndex]
		if !ok {
			t.Fatalf("packet %d references unknown stream %d", i, decoded.Packets[i].StreamIndex)
		}
		size, err := strconv.Atoi(decoded.Packets[i].Size)
		if err != nil {
			t.Fatalf("packet %d size %q: %v", i, decoded.Packets[i].Size, err)
		}
		packets = append(packets, externalWebMCodecPacket{Codec: codec, Size: size})
	}
	return packets
}

func readLocalWebMPacketOracle(t testing.TB, file string) []externalWebMPacket {
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
	var packets []externalWebMPacket
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
		packets = append(packets, externalWebMPacket{
			StreamIndex: stream,
			TimeNS:      packet.TimeNS,
			DurationNS:  packet.DurationNS,
			Keyframe:    packet.Keyframe,
			Size:        len(packet.Data),
		})
	}
	return packets
}

func readLocalWebMPacketPayloads(t testing.TB, file string) []localWebMPacketPayload {
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
	var packets []localWebMPacketPayload
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
		packets = append(packets, localWebMPacketPayload{
			Codec: tracks[stream].Codec,
			Data:  append([]byte(nil), packet.Data...),
		})
	}
	return packets
}

func assertExternalWebMPackets(t testing.TB, name string, got []externalWebMPacket, want []externalWebMPacket) {
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

func assertExternalWebMCodecPackets(t testing.TB, name string, got []externalWebMCodecPacket, want []externalWebMCodecPacket) {
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

func assertLocalWebMPacketPayloads(t testing.TB, name string, got []localWebMPacketPayload, want []localWebMPacketPayload) {
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

func externalWebMCodecPacketsForPayloads(t testing.TB, payloads []localWebMPacketPayload) []externalWebMCodecPacket {
	t.Helper()
	packets := make([]externalWebMCodecPacket, 0, len(payloads))
	for i := range payloads {
		packets = append(packets, externalWebMCodecPacket{
			Codec: externalWebMCodecName(t, payloads[i].Codec),
			Size:  len(payloads[i].Data),
		})
	}
	return packets
}

func externalWebMCodecName(t testing.TB, codec Codec) string {
	t.Helper()
	switch codec {
	case CodecOpus:
		return "opus"
	case CodecAV1:
		return "av1"
	case CodecVP9:
		return "vp9"
	case CodecVP8:
		return "vp8"
	default:
		t.Fatalf("unsupported external codec %d", codec)
		return ""
	}
}

func parseExternalWebMSecondsNS(value string) (int64, error) {
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

func writeFFmpegVP8OpusWebMRecording(t testing.TB) string {
	t.Helper()
	tool := requireTool(t, "ffmpeg")
	file := filepath.Join(t.TempDir(), "ffmpeg-vp8-opus-recording.webm")
	runExternalOrSkip(t, tool,
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

func writeFFmpegVP9OpusWebMRecording(t testing.TB) string {
	t.Helper()
	tool := requireTool(t, "ffmpeg")
	file := filepath.Join(t.TempDir(), "ffmpeg-vp9-opus-recording.webm")
	runExternalOrSkip(t, tool,
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

func writeFFmpegAV1OpusWebMRecording(t testing.TB) string {
	t.Helper()
	tool := requireTool(t, "ffmpeg")
	file := filepath.Join(t.TempDir(), "ffmpeg-av1-opus-recording.webm")
	runExternalOrSkip(t, tool,
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

type externalWebMRecordingStats struct {
	packets   map[uint32]int
	keyframes map[uint32]int
	lastTime  map[uint32]int64
}

type probedWebMRecordingStats struct {
	packets   int
	keyframes int
}

func readWebMRecording(t testing.TB, file string) ([]Track, externalWebMRecordingStats) {
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
	stats := externalWebMRecordingStats{
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

func probeExternalWebMRecordingStats(t testing.TB, tool string, file string) map[string]probedWebMRecordingStats {
	t.Helper()
	output := runExternal(t, tool, "-v", "quiet", "-show_entries", "packet=stream_index,flags", "-show_entries", "stream=index,codec_name", "-of", "json", file)
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
	stats := make(map[string]probedWebMRecordingStats, len(decoded.Streams))
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

func requireWebMRecordingTrack(t testing.TB, tracks []Track, codec Codec, typ TrackType) Track {
	t.Helper()
	for i := range tracks {
		if tracks[i].Codec == codec && tracks[i].Type == typ {
			return tracks[i]
		}
	}
	t.Fatalf("missing %v %v track in %+v", codec, typ, tracks)
	return Track{}
}

func assertWebMRecordingStats(t testing.TB, stats externalWebMRecordingStats, videoID uint32, audioID uint32) {
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

func assertWebMRecordingMatchesFFProbe(t testing.TB, name string, tracks []Track, local externalWebMRecordingStats, probe map[string]probedWebMRecordingStats) {
	t.Helper()
	for i := range tracks {
		codec := externalWebMCodecName(t, tracks[i].Codec)
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

func remuxWebMRecording(t testing.TB, file string) string {
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
	out := filepath.Join(t.TempDir(), "recording-remux.webm")
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
