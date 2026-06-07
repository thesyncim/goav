package matroska

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	luispater "github.com/luispater/matroska-go"
)

var externalBenchmarkDemuxerOptions = DemuxerOptions{MaxLacePayload: 64 << 10}

func BenchmarkExternalMatroskaRecordingScan(b *testing.B) {
	file := writeFFmpegH264OpusMatroskaRecording(b)
	data := readBenchmarkFile(b, file)
	ffprobe := requireExternalTool(b, "ffprobe")
	mkvinfo := requireExternalTool(b, "mkvinfo")
	b.Run("go-demux", func(b *testing.B) {
		packet := Packet{Data: make([]byte, 0, 1<<20)}
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if packets := scanMatroskaRecordingBytes(b, data, &packet); packets == 0 {
				b.Fatal("no packets scanned")
			}
		}
	})
	b.Run("ffprobe-show-packets", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			runExternalBenchmarkCommand(b, ffprobe, "-v", "error", "-show_packets", "-of", "compact", file)
		}
	})
	b.Run("mkvinfo-summary", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			runExternalBenchmarkCommand(b, mkvinfo, "--summary", file)
		}
	})
}

func BenchmarkExternalMatroskaRecordingRemux(b *testing.B) {
	file := writeFFmpegH264OpusMatroskaRecording(b)
	data := readBenchmarkFile(b, file)
	ffmpeg := requireExternalTool(b, "ffmpeg")
	mkvmerge := requireExternalTool(b, "mkvmerge")
	outDir := b.TempDir()
	b.Run("go-remux-discard", func(b *testing.B) {
		packet := Packet{Data: make([]byte, 0, 1<<20)}
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if packets := remuxMatroskaRecordingBytes(b, data, io.Discard, &packet); packets == 0 {
				b.Fatal("no packets remuxed")
			}
		}
	})
	b.Run("ffmpeg-copy-file", func(b *testing.B) {
		out := filepath.Join(outDir, "copy.mkv")
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			runExternalBenchmarkCommand(b, ffmpeg, "-y", "-v", "error", "-i", file, "-map", "0", "-c", "copy", out)
		}
	})
	b.Run("mkvmerge-copy-file", func(b *testing.B) {
		out := filepath.Join(outDir, "mkvmerge-copy.mkv")
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = os.Remove(out)
			runExternalBenchmarkCommand(b, mkvmerge, "--quiet", "-o", out, file)
		}
	})
}

func BenchmarkExternalMatroskaGoLibraryWebRTCCorpusScan(b *testing.B) {
	payloads := benchmarkWebRTCPayloads()
	data := makeBenchmarkWebRTCCorpusMatroskaData(b, benchmarkWebRTCCorpusCycles, payloads)
	b.Run("goav-demux", func(b *testing.B) {
		packet := Packet{Data: make([]byte, 0, 1<<20)}
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if packets := scanMatroskaRecordingBytes(b, data, &packet); packets == 0 {
				b.Fatal("no packets scanned")
			}
		}
	})
	b.Run("luispater-matroska-go-demux", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if packets := scanLuispaterMatroskaRecordingBytes(b, data); packets == 0 {
				b.Fatal("no packets scanned")
			}
		}
	})
}

func scanMatroskaRecordingBytes(tb testing.TB, data []byte, packet *Packet) int {
	tb.Helper()
	demuxer, err := NewDemuxer(bytes.NewReader(data), externalBenchmarkDemuxerOptions)
	if err != nil {
		tb.Fatal(err)
	}
	packets := 0
	for {
		err := demuxer.ReadPacket(packet)
		if errors.Is(err, io.EOF) {
			return packets
		}
		if err != nil {
			tb.Fatal(err)
		}
		packets++
	}
}

func scanLuispaterMatroskaRecordingBytes(tb testing.TB, data []byte) int {
	tb.Helper()
	demuxer, err := luispater.NewDemuxer(bytes.NewReader(data))
	if err != nil {
		tb.Fatal(err)
	}
	defer demuxer.Close()
	packets := 0
	for {
		packet, err := demuxer.ReadPacket()
		if errors.Is(err, io.EOF) {
			return packets
		}
		if err != nil {
			tb.Fatal(err)
		}
		if packet == nil || len(packet.Data) == 0 {
			tb.Fatal("empty packet")
		}
		packets++
	}
}

func remuxMatroskaRecordingBytes(tb testing.TB, data []byte, output io.Writer, packet *Packet) int {
	tb.Helper()
	demuxer, err := NewDemuxer(bytes.NewReader(data), externalBenchmarkDemuxerOptions)
	if err != nil {
		tb.Fatal(err)
	}
	muxer, err := NewMuxer(output, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	trackIDs := make(map[uint32]uint32, len(demuxer.Tracks()))
	for _, track := range demuxer.Tracks() {
		id, err := muxer.AddTrack(track)
		if err != nil {
			tb.Fatal(err)
		}
		trackIDs[track.ID] = id
	}
	packets := 0
	for {
		err := demuxer.ReadPacket(packet)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			tb.Fatal(err)
		}
		id, ok := trackIDs[packet.TrackID]
		if !ok {
			tb.Fatalf("packet references unknown track %d", packet.TrackID)
		}
		packet.TrackID = id
		if err := muxer.WritePacket(*packet); err != nil {
			tb.Fatal(err)
		}
		packets++
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
	return packets
}

func readBenchmarkFile(tb testing.TB, file string) []byte {
	tb.Helper()
	data, err := os.ReadFile(file)
	if err != nil {
		tb.Fatal(err)
	}
	return data
}

func runExternalBenchmarkCommand(tb testing.TB, tool string, args ...string) {
	tb.Helper()
	cmd := exec.Command(tool, args...)
	var stderr bytes.Buffer
	cmd.Stdout = io.Discard
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		tb.Fatalf("%s failed: %v\n%s", filepath.Base(tool), err, stderr.String())
	}
}
