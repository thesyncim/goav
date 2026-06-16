package mp4

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

// box builds a 32-bit-size box from a type and payload.
func box(typ string, payload ...byte) []byte {
	size := boxHeaderSize + len(payload)
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(size))
	b = append(b, typ...)
	return append(b, payload...)
}

// container builds a box whose payload is the concatenation of child boxes.
func container(typ string, children ...[]byte) []byte {
	var payload []byte
	for _, c := range children {
		payload = append(payload, c...)
	}
	return box(typ, payload...)
}

// largeBox builds a 64-bit-largesize box (size field == 1).
func largeBox(typ string, payload []byte) []byte {
	total := boxLargeHeaderSize + len(payload)
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, 1)
	b = append(b, typ...)
	var ls [8]byte
	binary.BigEndian.PutUint64(ls[:], uint64(total))
	b = append(b, ls[:]...)
	return append(b, payload...)
}

func TestReadBoxHeaderForms(t *testing.T) {
	t.Run("32-bit size", func(t *testing.T) {
		data := box("ftyp", 1, 2, 3, 4)
		h, err := readBoxHeader(bytes.NewReader(data), 0, int64(len(data)))
		if err != nil {
			t.Fatalf("readBoxHeader: %v", err)
		}
		if h.Type != "ftyp" || h.Size != int64(len(data)) || h.PayloadSize != 4 || h.PayloadOffset != 8 {
			t.Fatalf("header = %+v", h)
		}
	})

	t.Run("64-bit largesize", func(t *testing.T) {
		data := largeBox("mdat", []byte{9, 9, 9})
		h, err := readBoxHeader(bytes.NewReader(data), 0, int64(len(data)))
		if err != nil {
			t.Fatalf("readBoxHeader: %v", err)
		}
		if h.Type != "mdat" || h.Size != int64(len(data)) || h.PayloadOffset != 16 || h.PayloadSize != 3 {
			t.Fatalf("header = %+v", h)
		}
	})

	t.Run("size 0 runs to limit", func(t *testing.T) {
		data := make([]byte, 8+5)
		binary.BigEndian.PutUint32(data[0:4], 0) // size 0 -> to end
		copy(data[4:8], "mdat")
		h, err := readBoxHeader(bytes.NewReader(data), 0, int64(len(data)))
		if err != nil {
			t.Fatalf("readBoxHeader: %v", err)
		}
		if h.Size != int64(len(data)) || h.PayloadSize != 5 {
			t.Fatalf("header = %+v", h)
		}
	})

	t.Run("size below header is truncated", func(t *testing.T) {
		data := make([]byte, 8)
		binary.BigEndian.PutUint32(data[0:4], 4) // < header size
		copy(data[4:8], "junk")
		if _, err := readBoxHeader(bytes.NewReader(data), 0, int64(len(data))); !errors.Is(err, ErrTruncated) {
			t.Fatalf("err = %v, want ErrTruncated", err)
		}
	})

	t.Run("size past limit is truncated", func(t *testing.T) {
		data := box("moov", 1, 2, 3, 4)
		// claim a larger size than the buffer holds
		binary.BigEndian.PutUint32(data[0:4], uint32(len(data)+100))
		if _, err := readBoxHeader(bytes.NewReader(data), 0, int64(len(data))); !errors.Is(err, ErrTruncated) {
			t.Fatalf("err = %v, want ErrTruncated", err)
		}
	})
}

func TestWalkBoxesSiblingsAndNesting(t *testing.T) {
	leafA := box("leaf", 'a')
	leafB := box("leaf", 'b', 'b')
	parent := container("moov", leafA, leafB)
	top := append(box("ftyp", 'i', 's', 'o', 'm'), parent...)

	var topTypes []string
	if err := walkBoxes(bytes.NewReader(top), 0, int64(len(top)), func(h boxHeader) error {
		topTypes = append(topTypes, h.Type)
		if h.Type == "moov" {
			var leaves []int
			if err := walkBoxes(bytes.NewReader(top), h.PayloadOffset, h.end(), func(c boxHeader) error {
				if c.Type == "leaf" {
					leaves = append(leaves, int(c.PayloadSize))
				}
				return nil
			}); err != nil {
				return err
			}
			if len(leaves) != 2 || leaves[0] != 1 || leaves[1] != 2 {
				t.Fatalf("nested leaves = %v, want [1 2]", leaves)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("walkBoxes: %v", err)
	}
	if len(topTypes) != 2 || topTypes[0] != "ftyp" || topTypes[1] != "moov" {
		t.Fatalf("top boxes = %v, want [ftyp moov]", topTypes)
	}
}

func TestWalkBoxesVisitErrorStops(t *testing.T) {
	data := append(box("aaaa"), box("bbbb")...)
	sentinel := errors.New("stop")
	var seen int
	err := walkBoxes(bytes.NewReader(data), 0, int64(len(data)), func(boxHeader) error {
		seen++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
	if seen != 1 {
		t.Fatalf("visited %d boxes, want 1 before the error stopped the walk", seen)
	}
}

func TestParserBoundsAreSafe(t *testing.T) {
	p := newParser([]byte{0x01, 0x02, 0x03, 0x04, 0x05})
	if got := p.u8(); got != 1 {
		t.Fatalf("u8 = %d", got)
	}
	if got := p.u16(); got != 0x0203 {
		t.Fatalf("u16 = %#x", got)
	}
	// Two bytes left; asking for u32 overruns and must record an error, not panic.
	_ = p.u32()
	if p.err() == nil {
		t.Fatal("reading past the end did not record an error")
	}
	// Subsequent reads stay zero and keep the error sticky.
	if got := p.u8(); got != 0 || p.err() == nil {
		t.Fatalf("post-error read = %d, err = %v", got, p.err())
	}
}

func TestParserFullBoxAndTake(t *testing.T) {
	p := newParser([]byte{0x01, 0x00, 0x00, 0x05, 0xaa, 0xbb})
	version, flags := p.fullBox()
	if version != 1 || flags != 5 {
		t.Fatalf("fullBox version=%d flags=%d, want 1, 5", version, flags)
	}
	b := p.take(2)
	if !bytes.Equal(b, []byte{0xaa, 0xbb}) {
		t.Fatalf("take = %v", b)
	}
	if p.remaining() != 0 || p.err() != nil {
		t.Fatalf("remaining=%d err=%v", p.remaining(), p.err())
	}
}

// readPayload must refuse an absurd declared payload without allocating it.
func TestReadPayloadRefusesHugeBox(t *testing.T) {
	if _, err := readPayload(bytes.NewReader([]byte{}), boxHeader{PayloadSize: maxBoxPayload + 1}); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

var _ io.ReaderAt = (*bytes.Reader)(nil)
