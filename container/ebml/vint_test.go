package ebml

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"testing"
)

func TestSizeVINTBoundaries(t *testing.T) {
	tests := []struct {
		value uint64
		hex   string
		width int
	}{
		{value: 0, hex: "80", width: 1},
		{value: 1, hex: "81", width: 1},
		{value: 126, hex: "fe", width: 1},
		{value: 127, hex: "407f", width: 2},
		{value: 16382, hex: "7ffe", width: 2},
		{value: 16383, hex: "203fff", width: 3},
		{value: 2097150, hex: "3ffffe", width: 3},
		{value: 2097151, hex: "101fffff", width: 4},
	}
	var buf [MaxSizeWidth]byte
	for _, tt := range tests {
		n, err := EncodeSizeVINT(buf[:], tt.value)
		if err != nil {
			t.Fatalf("EncodeSizeVINT(%d): %v", tt.value, err)
		}
		if n != tt.width {
			t.Fatalf("EncodeSizeVINT(%d) width = %d, want %d", tt.value, n, tt.width)
		}
		if got := hex.EncodeToString(buf[:n]); got != tt.hex {
			t.Fatalf("EncodeSizeVINT(%d) = %s, want %s", tt.value, got, tt.hex)
		}
		size, err := DecodeSizeVINT(buf[:n])
		if err != nil {
			t.Fatalf("DecodeSizeVINT(%s): %v", tt.hex, err)
		}
		if size.Value != tt.value || size.Unknown || size.Width != tt.width {
			t.Fatalf("DecodeSizeVINT(%s) = %+v", tt.hex, size)
		}
	}
}

func TestUnknownSizeVINT(t *testing.T) {
	tests := []struct {
		width int
		hex   string
	}{
		{width: 1, hex: "ff"},
		{width: 2, hex: "7fff"},
		{width: 4, hex: "1fffffff"},
		{width: 8, hex: "01ffffffffffffff"},
	}
	var buf [MaxSizeWidth]byte
	for _, tt := range tests {
		n, err := EncodeUnknownSize(buf[:], tt.width)
		if err != nil {
			t.Fatalf("EncodeUnknownSize(%d): %v", tt.width, err)
		}
		if n != tt.width {
			t.Fatalf("width = %d, want %d", n, tt.width)
		}
		if got := hex.EncodeToString(buf[:n]); got != tt.hex {
			t.Fatalf("unknown size %d = %s, want %s", tt.width, got, tt.hex)
		}
		size, err := DecodeSizeVINT(buf[:n])
		if err != nil {
			t.Fatal(err)
		}
		if !size.Unknown || size.Width != tt.width {
			t.Fatalf("size = %+v, want unknown width %d", size, tt.width)
		}
	}
}

func TestIDVINT(t *testing.T) {
	tests := []ID{0x1a45dfa3, 0x18538067, 0x1654ae6b, 0xae, 0xa3, 0x9f}
	var buf [MaxSizeWidth]byte
	for _, id := range tests {
		n, err := EncodeID(buf[:], id)
		if err != nil {
			t.Fatalf("EncodeID(0x%x): %v", uint64(id), err)
		}
		got, width, err := DecodeIDVINT(buf[:n])
		if err != nil {
			t.Fatalf("DecodeIDVINT(0x%x): %v", uint64(id), err)
		}
		if got != id || width != n {
			t.Fatalf("id = 0x%x width %d, want 0x%x width %d", uint64(got), width, uint64(id), n)
		}
	}
	if _, _, err := DecodeIDVINT([]byte{0x80}); !errors.Is(err, ErrInvalidElementID) {
		t.Fatalf("zero-data element id err = %v, want ErrInvalidElementID", err)
	}
}

func TestReaderWriterHeaderAndSkip(t *testing.T) {
	var out bytes.Buffer
	writer := NewWriter(&out)
	if err := writer.WriteElement(0x4282, []byte("matroska")); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteVoid(8); err != nil {
		t.Fatal(err)
	}

	reader := NewReader(bytes.NewReader(out.Bytes()), ReaderOptions{MaxElementSize: 16})
	header, err := reader.ReadHeader()
	if err != nil {
		t.Fatal(err)
	}
	if header.ID != 0x4282 || header.Size.Value != 8 || header.DataOffset != int64(header.HeaderSize) {
		t.Fatalf("header = %+v", header)
	}
	payload := make([]byte, header.Size.Value)
	if err := reader.ReadFull(payload); err != nil {
		t.Fatal(err)
	}
	if string(payload) != "matroska" {
		t.Fatalf("payload = %q", payload)
	}

	header, err = reader.ReadHeader()
	if err != nil {
		t.Fatal(err)
	}
	if header.ID != 0xec {
		t.Fatalf("id = 0x%x, want Void", uint64(header.ID))
	}
	if err := reader.Skip(header.Size.Value); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ReadHeader(); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}
}

func TestReaderMaxElementSize(t *testing.T) {
	var out bytes.Buffer
	writer := NewWriter(&out)
	if err := writer.WriteHeader(0x4282, 17); err != nil {
		t.Fatal(err)
	}
	reader := NewReader(bytes.NewReader(out.Bytes()), ReaderOptions{MaxElementSize: 16})
	if _, err := reader.ReadHeader(); !errors.Is(err, ErrElementTooLarge) {
		t.Fatalf("err = %v, want ErrElementTooLarge", err)
	}
}

func TestLimitedReaderConsumesParentOffset(t *testing.T) {
	var out bytes.Buffer
	writer := NewWriter(&out)
	if err := writer.WriteElement(0x4282, []byte("webm")); err != nil {
		t.Fatal(err)
	}
	reader := NewReader(bytes.NewReader(out.Bytes()), ReaderOptions{})
	header, err := reader.ReadHeader()
	if err != nil {
		t.Fatal(err)
	}
	limited := reader.Limited(header.Size.Value)
	n, err := io.Copy(io.Discard, limited)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("limited read = %d, want 4", n)
	}
	if reader.Offset() != int64(len(out.Bytes())) {
		t.Fatalf("offset = %d, want %d", reader.Offset(), len(out.Bytes()))
	}
}

func TestSeekableSizePatch(t *testing.T) {
	ws := &memoryWriteSeeker{}
	writer := NewWriter(ws)
	patch, err := writer.StartSizedElement(0x1549a966, 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteString(0x4d80, "goav"); err != nil {
		t.Fatal(err)
	}
	if err := writer.FinishSizedElement(patch); err != nil {
		t.Fatal(err)
	}
	reader := NewReader(bytes.NewReader(ws.bytes), ReaderOptions{})
	header, err := reader.ReadHeader()
	if err != nil {
		t.Fatal(err)
	}
	if header.ID != 0x1549a966 || header.Size.Unknown || header.Size.Value == 0 {
		t.Fatalf("header = %+v", header)
	}
}

func TestWriteAndValidateCRC32(t *testing.T) {
	payload := []byte("matroska")
	var out bytes.Buffer
	writer := NewWriter(&out)
	if err := writer.WriteCRC32(payload); err != nil {
		t.Fatal(err)
	}
	reader := NewReader(bytes.NewReader(out.Bytes()), ReaderOptions{})
	header, err := reader.ReadHeader()
	if err != nil {
		t.Fatal(err)
	}
	if header.ID != CRC32ID || header.Size.Value != 4 {
		t.Fatalf("header = %+v", header)
	}
	var stored [4]byte
	if err := reader.ReadFull(stored[:]); err != nil {
		t.Fatal(err)
	}
	if !ValidateCRC32(stored[:], payload) {
		t.Fatalf("crc did not validate")
	}
	stored[0] ^= 0xff
	if ValidateCRC32(stored[:], payload) {
		t.Fatalf("corrupt crc validated")
	}
}

func FuzzDecodeSizeVINT(f *testing.F) {
	for _, seed := range [][]byte{
		{0x80},
		{0xfe},
		{0x40, 0x7f},
		{0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		{0x00},
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		size, err := DecodeSizeVINT(data)
		if err != nil {
			return
		}
		if size.Width < 1 || size.Width > MaxSizeWidth || len(data) < size.Width {
			t.Fatalf("invalid successful decode: len=%d size=%+v", len(data), size)
		}
	})
}

func FuzzReaderHeaders(f *testing.F) {
	var out bytes.Buffer
	writer := NewWriter(&out)
	_ = writer.WriteElement(0x4282, []byte("matroska"))
	f.Add(out.Bytes())
	f.Add([]byte{0x1a, 0x45, 0xdf, 0xa3, 0x80})
	f.Fuzz(func(t *testing.T, data []byte) {
		reader := NewReader(bytes.NewReader(data), ReaderOptions{MaxElementSize: 1 << 20})
		for {
			header, err := reader.ReadHeader()
			if err != nil {
				return
			}
			if header.Size.Unknown {
				return
			}
			if err := reader.Skip(header.Size.Value); err != nil {
				return
			}
		}
	})
}

func BenchmarkEncodeSizeVINT(b *testing.B) {
	var scratch [MaxSizeWidth]byte
	for i := 0; i < b.N; i++ {
		if _, err := EncodeSizeVINT(scratch[:], uint64(i&0xffff)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeSizeVINT(b *testing.B) {
	data := []byte{0x40, 0x7f}
	for i := 0; i < b.N; i++ {
		if _, err := DecodeSizeVINT(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkElementScan(b *testing.B) {
	var out bytes.Buffer
	writer := NewWriter(&out)
	for i := 0; i < 1024; i++ {
		if err := writer.WriteElement(0x4282, []byte("matroska")); err != nil {
			b.Fatal(err)
		}
	}
	data := out.Bytes()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := NewReader(bytes.NewReader(data), ReaderOptions{})
		for {
			header, err := reader.ReadHeader()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			if err := reader.Skip(header.Size.Value); err != nil {
				b.Fatal(err)
			}
		}
	}
}

type memoryWriteSeeker struct {
	bytes []byte
	pos   int64
}

func (m *memoryWriteSeeker) Write(p []byte) (int, error) {
	end := m.pos + int64(len(p))
	if end > int64(len(m.bytes)) {
		next := make([]byte, end)
		copy(next, m.bytes)
		m.bytes = next
	}
	copy(m.bytes[m.pos:end], p)
	m.pos = end
	return len(p), nil
}

func (m *memoryWriteSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += m.pos
	case io.SeekEnd:
		offset += int64(len(m.bytes))
	default:
		return 0, errors.New("invalid whence")
	}
	if offset < 0 {
		return 0, errors.New("negative offset")
	}
	m.pos = offset
	return offset, nil
}
