package goav_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/container/mp4"
	"github.com/thesyncim/goav/format"
)

// TestTSFileRoundTrip copies an MP4 into a .ts file and back to MP4 with
// the bundled runtime: the extension picks the transport stream both ways,
// and every packet survives.
func TestTSFileRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	dir := t.TempDir()
	ts, back := filepath.Join(dir, "copy.ts"), filepath.Join(dir, "back.mp4")
	if err := bundle.Run(ctx, goav.URIInput(mp4Fixture).To(goav.URI(ts))); err != nil {
		t.Fatalf("mp4 to ts: %v", err)
	}
	data, err := os.ReadFile(ts)
	if err != nil || len(data) == 0 || len(data)%188 != 0 || data[0] != 0x47 {
		t.Fatalf("copy.ts: %d bytes, %v", len(data), err)
	}
	if err := bundle.Run(ctx, goav.URIInput(ts).To(goav.URI(back))); err != nil {
		t.Fatalf("ts to mp4: %v", err)
	}
	original, copied := mp4PacketCounts(t, mp4Fixture), mp4PacketCounts(t, back)
	if len(copied) != 2 || copied[av.CodecH264] != original[av.CodecH264] || copied[av.CodecAAC] != original[av.CodecAAC] {
		t.Fatalf("back.mp4 has %v packets, the original %v", copied, original)
	}
}

func mp4PacketCounts(t *testing.T, path string) map[av.CodecID]int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	d := &mp4.FormatDemuxer{}
	if err := d.Open(ctx, format.Input{Reader: bytes.NewReader(data)}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	codecs := map[av.StreamID]av.CodecID{}
	for _, s := range d.Streams() {
		codecs[s.ID] = s.Codec.ID
	}
	counts := map[av.CodecID]int{}
	result := format.ReadResult{Packet: &av.Packet{}}
	for d.ReadInto(ctx, &result) == nil {
		counts[codecs[result.Packet.StreamID]]++
		result.Reset()
	}
	return counts
}

// TestTSHEVCToMP4 remuxes ffmpeg's HEVC and Opus transport stream into MP4:
// the demuxer's hvcC and picture size are what an hvc1 sample entry needs.
func TestTSHEVCToMP4(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out := filepath.Join(t.TempDir(), "hevc.mp4")
	if err := bundle.Run(ctx, goav.URIInput("container/ts/testdata/hevc_opus.ts").To(goav.URI(out))); err != nil {
		t.Fatalf("ts to mp4: %v", err)
	}
	if counts := mp4PacketCounts(t, out); counts[av.CodecHEVC] != 13 || counts[av.CodecOpus] != 26 {
		t.Fatalf("hevc.mp4 has %v packets, the stream 13 HEVC and 26 Opus", counts)
	}
}
