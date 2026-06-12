package ebml

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"math"
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

func TestSizeVINTRoundTripRepresentativeValues(t *testing.T) {
	values := []uint64{
		0, 1, 2, 10, 126, 127, 128,
		255, 256, 16_382, 16_383, 16_384,
		2_097_150, 2_097_151, 2_097_152,
		1<<28 - 2, 1<<28 - 1, 1 << 28,
		1<<35 - 2, 1<<35 - 1, 1 << 35,
		1<<42 - 2, 1<<42 - 1, 1 << 42,
		1<<49 - 2, 1<<49 - 1, 1 << 49,
		1<<56 - 2,
	}
	var scratch [MaxSizeWidth]byte
	for _, value := range values {
		n, err := EncodeSizeVINT(scratch[:], value)
		if err != nil {
			t.Fatalf("EncodeSizeVINT(%d): %v", value, err)
		}
		size, err := DecodeSizeVINT(scratch[:n])
		if err != nil {
			t.Fatalf("DecodeSizeVINT(%d): %v", value, err)
		}
		if size.Value != value || size.Unknown || size.Width != n {
			t.Fatalf("roundtrip %d = %+v width %d", value, size, n)
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
	tests := []ID{0x1a45dfa3, 0x18538067, 0x1654ae6b, 0xae, 0xa3, 0x9f, 0x80}
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

func TestReaderResetAtPreservesLogicalOffset(t *testing.T) {
	data := []byte{0, 0, 0, 0, 0, 0, 0, 0x42, 0x82, 0x81, 'x'}
	input := bytes.NewReader(data)
	if _, err := input.Seek(7, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	reader := NewReader(input, ReaderOptions{})
	reader.ResetAt(input, ReaderOptions{}, 7)
	header, err := reader.ReadHeader()
	if err != nil {
		t.Fatal(err)
	}
	if header.Offset != 7 || header.DataOffset != 10 {
		t.Fatalf("header offsets = offset %d data %d, want 7 and 10", header.Offset, header.DataOffset)
	}
	payload := make([]byte, header.Size.Value)
	if err := reader.ReadFull(payload); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload, []byte{'x'}) {
		t.Fatalf("payload = %v, want [120]", payload)
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

func TestElementErrorFormattingAndUnwrap(t *testing.T) {
	var nilErr *ElementError
	if got := nilErr.Error(); got != "<nil>" {
		t.Fatalf("nil Error() = %q", got)
	}
	if err := nilErr.Unwrap(); err != nil {
		t.Fatalf("nil Unwrap() = %v", err)
	}

	offsetOnly := &ElementError{Offset: 12, Err: ErrInvalidVINT}
	if got := offsetOnly.Error(); got != "ebml: offset 12: ebml: invalid variable-size integer" {
		t.Fatalf("offset Error() = %q", got)
	}
	if !errors.Is(offsetOnly, ErrInvalidVINT) {
		t.Fatalf("errors.Is(offsetOnly, ErrInvalidVINT) = false")
	}

	withID := &ElementError{ID: 0x1a45dfa3, Offset: 7, Err: ErrInvalidSize}
	if got := withID.Error(); got != "ebml: element 0x1a45dfa3 at offset 7: ebml: invalid element size" {
		t.Fatalf("element Error() = %q", got)
	}
}

func TestReaderResetRemainingAndKnownSize(t *testing.T) {
	var nilReader *Reader
	if got := nilReader.Offset(); got != 0 {
		t.Fatalf("nil Offset() = %d", got)
	}
	if _, ok := nilReader.Remaining(); ok {
		t.Fatal("nil Remaining() reported true")
	}
	if n, err := nilReader.Read(make([]byte, 1)); n != 0 || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("nil Read() = %d, %v", n, err)
	}

	limited := &io.LimitedReader{R: bytes.NewReader([]byte{1, 2, 3}), N: 3}
	reader := NewReader(limited, ReaderOptions{})
	if got, ok := reader.Remaining(); !ok || got != 3 {
		t.Fatalf("Remaining() = %d, %v; want 3, true", got, ok)
	}
	buf := make([]byte, 2)
	if n, err := reader.Read(buf); n != 2 || err != nil {
		t.Fatalf("Read() = %d, %v", n, err)
	}
	if got, ok := reader.Remaining(); !ok || got != 1 {
		t.Fatalf("Remaining() after read = %d, %v; want 1, true", got, ok)
	}
	reader.Reset(bytes.NewReader([]byte{9}), ReaderOptions{MaxElementSize: 1})
	if reader.Offset() != 0 {
		t.Fatalf("Reset offset = %d, want 0", reader.Offset())
	}
	if _, ok := reader.Remaining(); ok {
		t.Fatal("Remaining() on plain reader reported true")
	}

	header := Header{ID: 0x4282, Size: Size{Value: 5}, Offset: 10}
	if got, err := header.KnownSize(); err != nil || got != 5 {
		t.Fatalf("KnownSize() = %d, %v; want 5, nil", got, err)
	}
	header.Size.Unknown = true
	if _, err := header.KnownSize(); !errors.Is(err, ErrUnknownSize) {
		t.Fatalf("KnownSize() unknown err = %v, want ErrUnknownSize", err)
	}
}

func TestUnsignedVINTEncodingAndReading(t *testing.T) {
	values := []uint64{0, 1, 126, 127, 128, 16_383, maxVINTData(4), maxVINTData(8)}
	var scratch [MaxSizeWidth]byte
	for _, value := range values {
		n, err := EncodeUnsignedVINT(scratch[:], value)
		if err != nil {
			t.Fatalf("EncodeUnsignedVINT(%d): %v", value, err)
		}
		decoded, width, err := DecodeUnsignedVINT(scratch[:n])
		if err != nil {
			t.Fatalf("DecodeUnsignedVINT(%d): %v", value, err)
		}
		if decoded != value || width != n {
			t.Fatalf("roundtrip %d = %d width %d, want width %d", value, decoded, width, n)
		}
		read, readWidth, err := ReadUnsignedVINT(bytes.NewReader(scratch[:n]), &scratch)
		if err != nil {
			t.Fatalf("ReadUnsignedVINT(%d): %v", value, err)
		}
		if read != value || readWidth != n {
			t.Fatalf("read %d = %d width %d, want width %d", value, read, readWidth, n)
		}
	}

	if _, err := UnsignedVINTWidth(math.MaxUint64); !errors.Is(err, ErrVINTOverflow) {
		t.Fatalf("UnsignedVINTWidth(MaxUint64) = %v, want ErrVINTOverflow", err)
	}
	if _, err := EncodeUnsignedVINTWidth(scratch[:], 1, 0); !errors.Is(err, ErrInvalidVINT) {
		t.Fatalf("EncodeUnsignedVINTWidth bad width = %v, want ErrInvalidVINT", err)
	}
	if _, err := EncodeUnsignedVINTWidth(scratch[:], maxVINTData(1)+1, 1); !errors.Is(err, ErrInvalidVINT) {
		t.Fatalf("EncodeUnsignedVINTWidth too large = %v, want ErrInvalidVINT", err)
	}
}

func TestWriterScalarHelpersAndOffsets(t *testing.T) {
	var nilWriter *Writer
	if got := nilWriter.Offset(); got != 0 {
		t.Fatalf("nil writer offset = %d", got)
	}
	if n, err := nilWriter.Write([]byte{1}); n != 0 || !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("nil writer Write() = %d, %v", n, err)
	}

	var out bytes.Buffer
	writer := NewWriter(&out)
	if err := writer.WriteUnknownHeader(0x1a45dfa3, 4); err != nil {
		t.Fatal(err)
	}
	if writer.Offset() != int64(out.Len()) {
		t.Fatalf("offset = %d, len = %d", writer.Offset(), out.Len())
	}

	tests := []struct {
		name    string
		write   func(*Writer) error
		id      ID
		payload []byte
	}{
		{name: "uint", id: 0x4285, payload: []byte{0x01, 0x00}, write: func(w *Writer) error { return w.WriteUInt(0x4285, 0x100) }},
		{name: "int negative", id: 0x4489, payload: []byte{0xff}, write: func(w *Writer) error { return w.WriteInt(0x4489, -1) }},
		{name: "int positive", id: 0x4489, payload: []byte{0x00, 0x80}, write: func(w *Writer) error { return w.WriteInt(0x4489, 128) }},
		{name: "float64", id: 0x4489, payload: float64Payload(1.5), write: func(w *Writer) error { return w.WriteFloat64(0x4489, 1.5) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			writer := NewWriter(&out)
			if err := tt.write(writer); err != nil {
				t.Fatal(err)
			}
			header, payload := readOneElement(t, out.Bytes())
			if header.ID != tt.id {
				t.Fatalf("id = 0x%x, want 0x%x", uint64(header.ID), uint64(tt.id))
			}
			if !bytes.Equal(payload, tt.payload) {
				t.Fatalf("payload = %x, want %x", payload, tt.payload)
			}
		})
	}

	for _, value := range []uint64{0xff, 0x100, 0x1_0000, 0x1_000000, 0x1_00000000, 0x1_0000000000, 0x1_000000000000, 0x1_00000000000000} {
		if width := uintPayloadWidth(value); width < 1 || width > 8 {
			t.Fatalf("uintPayloadWidth(%x) = %d", value, width)
		}
	}
	for _, value := range []int64{-129, -128, -1, 0, 127, 128, 1 << 40} {
		if width := intPayloadWidth(value); width < 1 || width > 8 {
			t.Fatalf("intPayloadWidth(%d) = %d", value, width)
		}
	}
}

func TestWriterErrorsAndSizePatchValidation(t *testing.T) {
	var out bytes.Buffer
	writer := NewWriter(&out)
	if err := writer.WriteHeader(0, 1); !errors.Is(err, ErrInvalidElementID) {
		t.Fatalf("WriteHeader invalid id = %v, want ErrInvalidElementID", err)
	}
	if err := writer.WriteUnknownHeader(0x4282, 0); !errors.Is(err, ErrInvalidVINT) {
		t.Fatalf("WriteUnknownHeader invalid width = %v, want ErrInvalidVINT", err)
	}
	if _, err := writer.StartSizedElement(0x4282, 0); !errors.Is(err, ErrInvalidSize) {
		t.Fatalf("StartSizedElement invalid width = %v, want ErrInvalidSize", err)
	}
	if _, err := writer.StartSizedElement(0, 1); !errors.Is(err, ErrInvalidElementID) {
		t.Fatalf("StartSizedElement invalid id = %v, want ErrInvalidElementID", err)
	}
	if err := writer.FinishSizedElement(SizePatch{}); !errors.Is(err, ErrNonSeekableWriter) {
		t.Fatalf("FinishSizedElement non-seekable = %v, want ErrNonSeekableWriter", err)
	}

	ws := &memoryWriteSeeker{}
	seekWriter := NewWriter(ws)
	patch, err := seekWriter.StartSizedElement(0x1549a966, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seekWriter.Write(make([]byte, int(maxKnownSize(1)+1))); err != nil {
		t.Fatal(err)
	}
	if err := seekWriter.FinishSizedElement(patch); !errors.Is(err, ErrInvalidSize) {
		t.Fatalf("FinishSizedElement oversized = %v, want ErrInvalidSize", err)
	}

	if err := seekWriter.FinishSizedElement(SizePatch{Width: 0}); !errors.Is(err, ErrInvalidSize) {
		t.Fatalf("FinishSizedElement bad patch = %v, want ErrInvalidSize", err)
	}
}

func float64Payload(value float64) []byte {
	var payload [8]byte
	binary.BigEndian.PutUint64(payload[:], math.Float64bits(value))
	return payload[:]
}

func readOneElement(t *testing.T, data []byte) (Header, []byte) {
	t.Helper()
	reader := NewReader(bytes.NewReader(data), ReaderOptions{})
	header, err := reader.ReadHeader()
	if err != nil {
		t.Fatal(err)
	}
	size, err := header.KnownSize()
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, size)
	if err := reader.ReadFull(payload); err != nil {
		t.Fatal(err)
	}
	return header, payload
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
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0x1a, 0x45, 0xdf, 0xa3, 0x80})
	f.Add([]byte{0x42, 0x86, 0x81, 0x01})
	f.Add([]byte{0xff, 0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) != 0 {
			_, _ = VINTWidth(data[0])
		}
		_, _, _ = DecodeIDVINT(data)
		_, _, _ = DecodeUnsignedVINT(data)
		_, _ = DecodeSizeVINT(data)

		reader := NewReader(bytes.NewReader(data), ReaderOptions{MaxElementSize: 1 << 16})
		for i := 0; i < 16; i++ {
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
