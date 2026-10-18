package mp4

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/container/matroska"
	"github.com/thesyncim/goav/format"
)

type sliceWriteSeeker struct {
	data []byte
	pos  int64
}

func (w *sliceWriteSeeker) Write(p []byte) (int, error) {
	if end := w.pos + int64(len(p)); end > int64(len(w.data)) {
		w.data = append(w.data, make([]byte, end-int64(len(w.data)))...)
	}
	n := copy(w.data[w.pos:], p)
	w.pos += int64(n)
	return n, nil
}

func (w *sliceWriteSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		w.pos = offset
	case io.SeekCurrent:
		w.pos += offset
	case io.SeekEnd:
		w.pos = int64(len(w.data)) + offset
	}
	return w.pos, nil
}

// TestOpusRemuxesIntoMatroska pins that MP4 Opus copies into Matroska with
// its timing intact: the OpusHead rebuilt from dOps is the CodecPrivate the
// Matroska muxer needs (raw dOps was refused), and the pre-skip the edit list
// leaves before zero comes back out of the Matroska file unchanged. Encoders
// write the edit to equal the pre-skip; here both are 6 ms (288 samples), so
// the millisecond Matroska timestamps hold the times exactly.
func TestOpusRemuxesIntoMatroska(t *testing.T) {
	ctx := context.Background()
	ftyp := box("ftyp", cat([]byte("isom"), u32b(0x200), []byte("isom"))...)
	mdat := box("mdat", bytes.Repeat([]byte{0xfc}, 20)...)
	audio := editedTrak(1, "soun", opusEntryWithPreSkip(288), 4, 5, 20, 20+8, 0, elstBox([2]int32{80, 6}))
	src := openFormatDemuxer(t, cat(ftyp, mdat, container("moov", movieHeader(1000), audio)))
	defer src.Close()

	out := &sliceWriteSeeker{}
	muxer := &matroska.FormatMuxer{}
	if err := muxer.Open(ctx, format.Output{Writer: out}, src.Streams(), format.OpenOptions{}); err != nil {
		t.Fatalf("Matroska refused the MP4 Opus stream: %v", err)
	}
	var want []av.Timestamp
	for {
		packet := av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 64)}}
		err := src.ReadInto(ctx, &format.ReadResult{Packet: &packet})
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, packet.PTS)
		if err := muxer.Write(ctx, &packet, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}
	if first, _ := want[0].ToDuration(); first >= 0 {
		t.Fatalf("first MP4 packet at %v, want the pre-skip before zero", first)
	}

	dst := &matroska.FormatDemuxer{}
	if err := dst.Open(ctx, format.Input{Reader: bytes.NewReader(out.data)}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	for i := range want {
		packet := av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 64)}}
		if err := dst.ReadInto(ctx, &format.ReadResult{Packet: &packet}); err != nil {
			t.Fatal(err)
		}
		got, _ := packet.PTS.ToDuration()
		if exp, _ := want[i].ToDuration(); got != exp {
			t.Fatalf("packet %d at %v after the remux, want %v", i, got, exp)
		}
	}
}
