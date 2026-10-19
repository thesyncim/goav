package ts

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/container/mp4"
	"github.com/thesyncim/goav/format"
)

// TestRoundTrip remuxes transport streams ffmpeg wrote, H.264 with AAC and
// HEVC with Opus, and reads back identical packets, timestamps, and codec
// parameters; the muxed bytes pass the structural validator and ffprobe.
func TestRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		fixture string
		codecs  []av.CodecID
	}{
		{"testdata/h264_aac.ts", []av.CodecID{av.CodecH264, av.CodecAAC}},
		{"testdata/hevc_opus.ts", []av.CodecID{av.CodecHEVC, av.CodecOpus}},
	} {
		t.Run(tc.fixture, func(t *testing.T) {
			data, err := os.ReadFile(tc.fixture)
			if err != nil {
				t.Fatal(err)
			}
			in := demuxBytes(t, data)
			if len(in.streams) != len(tc.codecs) {
				t.Fatalf("streams = %+v", in.streams)
			}
			for i, codec := range tc.codecs {
				if in.streams[i].Codec.ID != codec || len(in.streams[i].Codec.ExtraData.Bytes) == 0 {
					t.Fatalf("stream %d = %+v, want configured %s", i, in.streams[i], codec)
				}
			}
			muxed := muxBytes(t, MuxerOptions{}, in.streams, in.packets)
			report := validateTS(t, muxed)
			out := demuxBytes(t, muxed)
			assertSameStreams(t, out.streams, in.streams)
			assertSamePackets(t, out.packets, in.packets, 0)
			if report.pcrPID != 256 {
				t.Fatalf("PCR on PID %d, want the video PID 256", report.pcrPID)
			}
			ffprobeAgrees(t, muxed, in)
		})
	}
}

// TestMuxFromMP4 muxes the packets of an MP4, which start at zero and carry
// raw AAC, H.264 without parameter sets in band, and timestamps in the
// tracks' own timescales. The transport stream lifts every timestamp by the
// PCR delay and gives keyframes their parameter sets; everything else reads
// back unchanged.
func TestMuxFromMP4(t *testing.T) {
	data, err := os.ReadFile("../mp4/testdata/h264_aac.mp4")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	d := &mp4.FormatDemuxer{}
	if err := d.Open(ctx, format.Input{Reader: bytes.NewReader(data)}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	streams := d.Streams()
	var packets []av.Packet
	result := format.ReadResult{Packet: &av.Packet{}}
	for d.ReadInto(ctx, &result) == nil {
		p := *result.Packet
		p.Payload.Bytes = bytes.Clone(p.Payload.Bytes)
		packets = append(packets, p)
		result.Reset()
	}
	muxed := muxBytes(t, MuxerOptions{}, streams, packets)
	validateTS(t, muxed)
	out := demuxBytes(t, muxed)
	if len(out.packets) != len(packets) {
		t.Fatalf("read %d packets, wrote %d", len(out.packets), len(packets))
	}
	if out.streams[0].Codec.Width != 64 || out.streams[0].Codec.Height != 48 || out.streams[1].Codec.SampleRate != 44100 {
		t.Fatalf("streams = %+v", out.streams)
	}
	sets, _ := parameterSets(av.CodecH264, streams[0].Codec.ExtraData.Bytes)
	byStream := map[av.StreamID]av.StreamID{"256": streams[0].ID, "257": streams[1].ID}
	j := map[av.StreamID]int{}
	for _, got := range out.packets {
		var want av.Packet
		for _, p := range packets[j[got.StreamID]:] {
			j[got.StreamID]++
			if p.StreamID == byStream[got.StreamID] {
				want = p
				break
			}
		}
		payload := want.Payload.Bytes
		if want.Keyframe && got.Type == av.MediaVideo {
			payload = append(append([]byte(nil), sets...), payload...)
		}
		if !bytes.Equal(got.Payload.Bytes, payload) || got.Keyframe != want.Keyframe {
			t.Fatalf("stream %s packet differs: %d bytes key=%v, want %d key=%v", got.StreamID, len(got.Payload.Bytes), got.Keyframe, len(payload), want.Keyframe)
		}
		if g, w := mustDuration(t, got.PTS)-defaultDelay, mustDuration(t, want.PTS); absDuration(g-w) > 12*time.Microsecond {
			t.Fatalf("stream %s PTS %v, want %v + the %v delay", got.StreamID, g+defaultDelay, w, defaultDelay)
		}
	}
}

func mustDuration(t *testing.T, ts av.Timestamp) time.Duration {
	t.Helper()
	d, ok := ts.ToDuration()
	if !ok {
		t.Fatalf("timestamp %+v does not convert", ts)
	}
	return d
}

func absDuration(d time.Duration) time.Duration { return max(d, -d) }

func assertSameStreams(t *testing.T, got, want []av.Stream) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d streams, want %d", len(got), len(want))
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.ID != w.ID || g.Codec.ID != w.Codec.ID || !bytes.Equal(g.Codec.ExtraData.Bytes, w.Codec.ExtraData.Bytes) ||
			g.Codec.SampleRate != w.Codec.SampleRate || g.Codec.Channels != w.Codec.Channels ||
			g.Codec.Width != w.Codec.Width || g.Codec.Height != w.Codec.Height || g.Language != w.Language {
			t.Fatalf("stream %d = %+v, want %+v", i, g, w)
		}
	}
}

// assertSamePackets compares payloads, keyframes, and timestamps, the
// latter after removing shift (in 90 kHz ticks).
func assertSamePackets(t *testing.T, got, want []av.Packet, shift int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d packets, want %d", len(got), len(want))
	}
	for i := range want {
		g, w := got[i], want[i]
		gp, _ := ticks90k(g.PTS)
		gd, _ := ticks90k(g.DTS)
		wp, _ := ticks90k(w.PTS)
		wd, _ := ticks90k(w.DTS)
		if g.StreamID != w.StreamID || !bytes.Equal(g.Payload.Bytes, w.Payload.Bytes) || g.Keyframe != w.Keyframe ||
			gp-shift != wp || gd-shift != wd || g.Corrupt || g.LossBefore {
			t.Fatalf("packet %d = %s %d bytes pts=%d dts=%d key=%v corrupt=%v loss=%v; want %s %d bytes pts=%d dts=%d key=%v",
				i, g.StreamID, len(g.Payload.Bytes), gp, gd, g.Keyframe, g.Corrupt, g.LossBefore, w.StreamID, len(w.Payload.Bytes), wp+shift, wd+shift, w.Keyframe)
		}
	}
}

// ffprobeAgrees has ffprobe, when installed, read the muxed stream without
// a complaint and count the packets the demuxer read.
func ffprobeAgrees(t *testing.T, muxed []byte, want demuxed) {
	t.Helper()
	ffprobe, err := exec.LookPath("ffprobe")
	if err != nil {
		t.Log("ffprobe is not installed; the structural validator stands alone")
		return
	}
	path := t.TempDir() + "/out.ts"
	if err := os.WriteFile(path, muxed, 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(ffprobe, "-v", "error", "-count_packets", "-show_entries", "stream=codec_name,nb_read_packets", "-of", "csv=p=0", path)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil || stderr.Len() != 0 {
		t.Fatalf("ffprobe: %v\n%s", err, stderr.String())
	}
	counts := map[av.StreamID]int{}
	for _, p := range want.packets {
		counts[p.StreamID]++
	}
	var lines []string
	for _, s := range want.streams {
		name := string(s.Codec.ID)
		lines = append(lines, name+","+strconv.Itoa(counts[s.ID]))
	}
	// ffprobe lists the program's streams, then every stream: read the last.
	all := strings.Split(strings.TrimSpace(string(out)), "\n")
	if got := strings.Join(all[max(0, len(all)-len(lines)):], "\n"); got != strings.Join(lines, "\n") {
		t.Fatalf("ffprobe reads\n%s\nwant\n%s", got, strings.Join(lines, "\n"))
	}
}
