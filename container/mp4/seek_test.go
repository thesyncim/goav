package mp4

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
)

// syntheticTrak builds one progressive track: count samples of sampleSize
// bytes stored contiguously at chunkOffset, decode delta deltaTicks at
// timescale 1000, the 1-based syncs as keyframes (nil = all sync).
func syntheticTrak(id uint32, handler string, entry []byte, count, sampleSize int, deltaTicks uint32, chunkOffset uint32, syncs []uint32) []byte {
	sizes := []byte{}
	for range count {
		sizes = append(sizes, u32b(uint32(sampleSize))...)
	}
	children := [][]byte{
		fullBox("stsd", 0, 0, cat(u32b(1), entry)),
		fullBox("stts", 0, 0, cat(u32b(1), u32b(uint32(count)), u32b(deltaTicks))),
		fullBox("stsc", 0, 0, cat(u32b(1), u32b(1), u32b(uint32(count)), u32b(1))),
		fullBox("stsz", 0, 0, cat(u32b(0), u32b(uint32(count)), sizes)),
		fullBox("stco", 0, 0, cat(u32b(1), u32b(chunkOffset))),
	}
	if syncs != nil {
		table := u32b(uint32(len(syncs)))
		for _, s := range syncs {
			table = append(table, u32b(s)...)
		}
		children = append(children, fullBox("stss", 0, 0, table))
	}
	stbl := container("stbl", children...)
	mdhd := fullBox("mdhd", 0, 0, cat(u32b(0), u32b(0), u32b(1000), u32b(uint32(count)*deltaTicks), u16b(0), u16b(0)))
	hdlr := fullBox("hdlr", 0, 0, cat(u32b(0), []byte(handler), make([]byte, 12), []byte("x\x00")))
	tkhd := fullBox("tkhd", 0, 1, cat(u32b(0), u32b(0), u32b(id)))
	return container("trak", tkhd, container("mdia", mdhd, hdlr, container("minf", stbl)))
}

func opusEntry() []byte {
	p := cat(make([]byte, 6), u16b(1), make([]byte, 8), u16b(2), u16b(16), make([]byte, 4), u32b(48000<<16))
	return box("Opus", p...)
}

// buildSeekMP4 is a video-only file: six 10-byte samples 500 ms apart with
// keyframes at samples 1 and 4 (0 s and 1.5 s).
func buildSeekMP4() []byte {
	ftyp := box("ftyp", cat([]byte("isom"), u32b(0x200), []byte("isom"))...)
	const mdatPayload = 20 + 8 // ftyp is 20 bytes, then the mdat header
	mdat := box("mdat", make([]byte, 60)...)
	trak := syntheticTrak(1, "vide", avc1Entry(64, 48, nil), 6, 10, 500, mdatPayload, []uint32{1, 4})
	return cat(ftyp, mdat, container("moov", fullBox("mvhd", 0, 0, make([]byte, 96)), trak))
}

func openFormatDemuxer(t *testing.T, data []byte) *FormatDemuxer {
	t.Helper()
	d := &FormatDemuxer{}
	if err := d.Open(context.Background(), format.Input{Reader: bytes.NewReader(data)}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	return d
}

func readPacket(t *testing.T, d *FormatDemuxer) av.Packet {
	t.Helper()
	packet := av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 1024)}}
	if err := d.ReadInto(context.Background(), &format.ReadResult{Packet: &packet}); err != nil {
		t.Fatal(err)
	}
	return packet
}

func TestFormatDemuxerSeeksToKeyframeAtOrBefore(t *testing.T) {
	d := openFormatDemuxer(t, buildSeekMP4())
	ctx := context.Background()
	for _, tt := range []struct {
		position time.Duration
		wantMS   int64
	}{
		{1700 * time.Millisecond, 1500}, // between keyframes: back to 1.5 s
		{1500 * time.Millisecond, 1500}, // on a keyframe
		{1400 * time.Millisecond, 0},    // before the second keyframe
		{100 * time.Second, 1500},       // past the end: last keyframe
		{0, 0},
	} {
		if err := d.Seek(ctx, tt.position); err != nil {
			t.Fatalf("Seek(%v): %v", tt.position, err)
		}
		packet := readPacket(t, d)
		if packet.PTS.Value != tt.wantMS || !packet.Keyframe {
			t.Fatalf("Seek(%v) resumed at pts %d (keyframe %v), want keyframe at %d ms", tt.position, packet.PTS.Value, packet.Keyframe, tt.wantMS)
		}
	}
	if err := d.Seek(ctx, -time.Second); err == nil {
		t.Fatal("Seek(-1s) accepted a negative position")
	}
	var _ format.Seeker = d
}

// TestDemuxerOrdersTracksByTime pins time-ordered delivery: a file whose
// mdat holds all video then all audio (no chunk interleave) still delivers
// samples in decode-time order across tracks.
func TestDemuxerOrdersTracksByTime(t *testing.T) {
	ftyp := box("ftyp", cat([]byte("isom"), u32b(0x200), []byte("isom"))...)
	const payload = 20 + 8
	mdat := box("mdat", make([]byte, 40)...) // 4 video x 5 B, then 4 audio x 5 B
	video := syntheticTrak(1, "vide", avc1Entry(64, 48, nil), 4, 5, 100, payload, nil)
	audio := syntheticTrak(2, "soun", opusEntry(), 4, 5, 100, payload+20, nil)
	data := cat(ftyp, mdat, container("moov", fullBox("mvhd", 0, 0, make([]byte, 96)), video, audio))

	d, err := NewDemuxer(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	var sample Sample
	var got []int
	for d.ReadInto(nil, &sample) == nil {
		got = append(got, sample.TrackIndex)
	}
	want := []int{0, 1, 0, 1, 0, 1, 0, 1}
	if len(got) != len(want) {
		t.Fatalf("read %d samples, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("track order = %v, want %v (interleaved by time, not file offset)", got, want)
		}
	}
}
