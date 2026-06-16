package mp4

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/thesyncim/goav/av"
)

func u16b(v uint16) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, v)
	return b
}

func u32b(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

func cat(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func fullBox(typ string, version uint8, flags uint32, payload []byte) []byte {
	head := []byte{version, byte(flags >> 16), byte(flags >> 8), byte(flags)}
	return box(typ, append(head, payload...)...)
}

// avc1Entry builds a minimal avc1 visual sample entry carrying an avcC config.
func avc1Entry(width, height uint16, avcC []byte) []byte {
	p := cat(
		make([]byte, 6), // reserved
		u16b(1),         // data_reference_index
		make([]byte, 16),
		u16b(width),
		u16b(height),
		make([]byte, 4+4+4+2+32+2+2),
		box("avcC", avcC...),
	)
	return box("avc1", p...)
}

// buildVideoMP4 assembles ftyp + mdat + moov with one avc1 track of three
// samples sized 10/20/30 at decode deltas of 512 ticks (timescale 1000), the
// first a sync sample. The chunk offset points at the mdat payload.
func buildVideoMP4() []byte {
	avcC := []byte{0xaa, 0xbb, 0xcc}
	stsd := fullBox("stsd", 0, 0, cat(u32b(1), avc1Entry(64, 48, avcC)))
	stts := fullBox("stts", 0, 0, cat(u32b(1), u32b(3), u32b(512)))
	stsc := fullBox("stsc", 0, 0, cat(u32b(1), u32b(1), u32b(3), u32b(1)))
	stsz := fullBox("stsz", 0, 0, cat(u32b(0), u32b(3), u32b(10), u32b(20), u32b(30)))
	stco := fullBox("stco", 0, 0, cat(u32b(1), u32b(28)))
	stss := fullBox("stss", 0, 0, cat(u32b(1), u32b(1)))
	stbl := container("stbl", stsd, stts, stsc, stsz, stco, stss)
	minf := container("minf", stbl)
	mdhd := fullBox("mdhd", 0, 0, cat(u32b(0), u32b(0), u32b(1000), u32b(1536), u16b(0), u16b(0)))
	hdlr := fullBox("hdlr", 0, 0, cat(u32b(0), []byte("vide"), make([]byte, 12), []byte("v\x00")))
	mdia := container("mdia", mdhd, hdlr, minf)
	tkhd := fullBox("tkhd", 0, 1, cat(u32b(0), u32b(0), u32b(1)))
	trak := container("trak", tkhd, mdia)
	mvhd := fullBox("mvhd", 0, 0, make([]byte, 96))
	moov := container("moov", mvhd, trak)

	ftyp := box("ftyp", cat([]byte("isom"), u32b(0x200), []byte("isom"))...)
	mdat := box("mdat", make([]byte, 60)...) // 10+20+30 sample bytes
	return cat(ftyp, mdat, moov)
}

func TestParseMovieVideoTrack(t *testing.T) {
	data := buildVideoMP4()
	tracks, err := parseMovie(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("parseMovie: %v", err)
	}
	if len(tracks) != 1 {
		t.Fatalf("tracks = %d, want 1", len(tracks))
	}
	tr := tracks[0]
	if tr.codec != av.CodecH264 || tr.media != av.MediaVideo {
		t.Fatalf("codec=%s media=%s, want h264/video", tr.codec, tr.media)
	}
	if tr.id != 1 || tr.timescale != 1000 {
		t.Fatalf("id=%d timescale=%d, want 1/1000", tr.id, tr.timescale)
	}
	if tr.params.Width != 64 || tr.params.Height != 48 {
		t.Fatalf("geometry = %dx%d, want 64x48", tr.params.Width, tr.params.Height)
	}
	if !bytes.Equal(tr.params.ExtraData.Bytes, []byte{0xaa, 0xbb, 0xcc}) {
		t.Fatalf("extradata = %v, want avcC bytes", tr.params.ExtraData.Bytes)
	}
	if tr.params.ClockRate != 1000 {
		t.Fatalf("clock rate = %d, want 1000", tr.params.ClockRate)
	}

	wantOffsets := []int64{28, 38, 58}
	wantSizes := []int{10, 20, 30}
	wantDTS := []int64{0, 512, 1024}
	wantKey := []bool{true, false, false}
	if len(tr.samples) != 3 {
		t.Fatalf("samples = %d, want 3", len(tr.samples))
	}
	for i, s := range tr.samples {
		if s.offset != wantOffsets[i] || s.size != wantSizes[i] || s.dts != wantDTS[i] || s.cts != wantDTS[i] || s.keyframe != wantKey[i] {
			t.Fatalf("sample %d = %+v, want offset=%d size=%d dts=%d key=%v", i, s, wantOffsets[i], wantSizes[i], wantDTS[i], wantKey[i])
		}
	}
}

func TestParseMovieRejectsNoMoov(t *testing.T) {
	data := box("ftyp", cat([]byte("isom"), u32b(0))...)
	if _, err := parseMovie(bytes.NewReader(data), int64(len(data))); err == nil {
		t.Fatal("parseMovie accepted a file with no moov")
	}
}

func TestParseStszUniformAndExplicit(t *testing.T) {
	uniform := fullBox("stsz", 0, 0, cat(u32b(7), u32b(4)))
	sizes := parseStsz(uniform[8:])
	if len(sizes) != 4 || sizes[0] != 7 || sizes[3] != 7 {
		t.Fatalf("uniform stsz = %v, want four 7s", sizes)
	}
	explicit := fullBox("stsz", 0, 0, cat(u32b(0), u32b(2), u32b(11), u32b(22)))
	sizes = parseStsz(explicit[8:])
	if len(sizes) != 2 || sizes[0] != 11 || sizes[1] != 22 {
		t.Fatalf("explicit stsz = %v, want [11 22]", sizes)
	}
}

func TestParseStszRefusesHugeCount(t *testing.T) {
	// sample_size 0, count claims far more entries than the payload holds.
	bad := fullBox("stsz", 0, 0, cat(u32b(0), u32b(1<<30)))
	if sizes := parseStsz(bad[8:]); sizes != nil {
		t.Fatalf("parseStsz accepted an oversized count: %d entries", len(sizes))
	}
}
