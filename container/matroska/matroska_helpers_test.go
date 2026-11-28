package matroska

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"testing"

	"github.com/thesyncim/goav/container/ebml"
)

func ebmlReader(tb testing.TB, data []byte) *ebml.Reader {
	tb.Helper()
	return ebml.NewReader(bytes.NewReader(data), ebml.ReaderOptions{})
}

func countClusters(tb testing.TB, data []byte) int {
	tb.Helper()
	reader := ebmlReader(tb, data)
	header, err := reader.ReadHeader()
	if err != nil {
		tb.Fatal(err)
	}
	if header.ID != idEBML {
		tb.Fatalf("first id = 0x%x, want EBML", uint64(header.ID))
	}
	if err := reader.Skip(header.Size.Value); err != nil {
		tb.Fatal(err)
	}
	segment, err := reader.ReadHeader()
	if err != nil {
		tb.Fatal(err)
	}
	if segment.ID != idSegment {
		tb.Fatalf("second id = 0x%x, want Segment", uint64(segment.ID))
	}
	clusters := 0
	for {
		child, err := reader.ReadHeader()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			tb.Fatal(err)
		}
		if child.ID == idCluster {
			clusters++
			if child.Size.Unknown {
				continue
			}
		}
		if child.Size.Unknown {
			break
		}
		if err := reader.Skip(child.Size.Value); err != nil {
			tb.Fatal(err)
		}
	}
	return clusters
}

func collectTopLevelPositions(tb testing.TB, data []byte) map[ebml.ID]uint64 {
	tb.Helper()
	reader := ebmlReader(tb, data)
	header, err := reader.ReadHeader()
	if err != nil {
		tb.Fatal(err)
	}
	if header.ID != idEBML {
		tb.Fatalf("first id = 0x%x, want EBML", uint64(header.ID))
	}
	if err := reader.Skip(header.Size.Value); err != nil {
		tb.Fatal(err)
	}
	segment, err := reader.ReadHeader()
	if err != nil {
		tb.Fatal(err)
	}
	if segment.ID != idSegment || segment.Size.Unknown {
		tb.Fatalf("segment = %+v, want known-size Segment", segment)
	}
	positions := make(map[ebml.ID]uint64)
	segmentEnd := segment.DataOffset + int64(segment.Size.Value)
	for reader.Offset() < segmentEnd {
		child, err := reader.ReadHeader()
		if err != nil {
			tb.Fatal(err)
		}
		positions[child.ID] = uint64(child.Offset - segment.DataOffset)
		if child.Size.Unknown {
			tb.Fatalf("unexpected unknown top-level element %+v", child)
		}
		if err := reader.Skip(child.Size.Value); err != nil {
			tb.Fatal(err)
		}
	}
	return positions
}

func assertTopLevelMasterStartsWithCRC32(tb testing.TB, data []byte, id ebml.ID) {
	tb.Helper()
	payload := topLevelElementPayload(tb, data, id)
	reader := ebml.NewReader(bytes.NewReader(payload), ebml.ReaderOptions{})
	child, err := reader.ReadHeader()
	if err != nil {
		tb.Fatal(err)
	}
	if child.ID != idCRC32 || child.Size.Unknown || child.Size.Value != 4 {
		tb.Fatalf("top-level 0x%x first child = %+v, want CRC32", uint64(id), child)
	}
}

func topLevelElementPayload(tb testing.TB, data []byte, id ebml.ID) []byte {
	tb.Helper()
	reader := ebmlReader(tb, data)
	header, err := reader.ReadHeader()
	if err != nil {
		tb.Fatal(err)
	}
	if header.ID != idEBML {
		tb.Fatalf("first id = 0x%x, want EBML", uint64(header.ID))
	}
	if err := reader.Skip(header.Size.Value); err != nil {
		tb.Fatal(err)
	}
	segment, err := reader.ReadHeader()
	if err != nil {
		tb.Fatal(err)
	}
	if segment.ID != idSegment {
		tb.Fatalf("segment = %+v, want Segment", segment)
	}
	var segmentEnd int64
	if !segment.Size.Unknown {
		segmentEnd = segment.DataOffset + int64(segment.Size.Value)
	}
	for segment.Size.Unknown || reader.Offset() < segmentEnd {
		child, err := reader.ReadHeader()
		if err != nil {
			if segment.Size.Unknown && errors.Is(err, io.EOF) {
				break
			}
			tb.Fatal(err)
		}
		if child.ID == id {
			if child.Size.Unknown || child.Size.Value > uint64(math.MaxInt) {
				tb.Fatalf("top-level 0x%x has invalid size %+v", uint64(id), child.Size)
			}
			payload := make([]byte, int(child.Size.Value))
			if err := reader.ReadFull(payload); err != nil {
				tb.Fatal(err)
			}
			return payload
		}
		if child.Size.Unknown {
			tb.Fatalf("unexpected unknown top-level child %+v while looking for 0x%x", child, uint64(id))
		}
		if err := reader.Skip(child.Size.Value); err != nil {
			tb.Fatal(err)
		}
	}
	tb.Fatalf("missing top-level element 0x%x", uint64(id))
	return nil
}

func assertSeekEntry(tb testing.TB, entries []SeekEntry, id ebml.ID, position uint64) {
	tb.Helper()
	for i := range entries {
		if entries[i].ID == uint64(id) {
			if entries[i].Position != position {
				tb.Fatalf("seek entry 0x%x position = %d, want %d", uint64(id), entries[i].Position, position)
			}
			return
		}
	}
	tb.Fatalf("missing seek entry 0x%x in %+v", uint64(id), entries)
}

func readInfoDuration(tb testing.TB, payload []byte) (float64, bool) {
	tb.Helper()
	reader := ebml.NewReader(bytes.NewReader(payload), ebml.ReaderOptions{})
	for reader.Offset() < int64(len(payload)) {
		header, err := reader.ReadHeader()
		if err != nil {
			tb.Fatal(err)
		}
		if header.ID == idDuration {
			if header.Size.Value != 8 {
				tb.Fatalf("duration size = %d, want 8", header.Size.Value)
			}
			var scratch [8]byte
			if err := reader.ReadFull(scratch[:]); err != nil {
				tb.Fatal(err)
			}
			return math.Float64frombits(binary.BigEndian.Uint64(scratch[:])), true
		}
		if header.Size.Unknown {
			tb.Fatalf("unexpected unknown Info child: %+v", header)
		}
		if err := reader.Skip(header.Size.Value); err != nil {
			tb.Fatal(err)
		}
	}
	return 0, false
}

func firstBlockFlags(tb testing.TB, data []byte, id ebml.ID) byte {
	tb.Helper()
	flags, ok := firstBlockFlagsInPayload(tb, data, id)
	if !ok {
		tb.Fatalf("missing block 0x%x", uint64(id))
	}
	return flags
}

func firstBlockFlagsInPayload(tb testing.TB, payload []byte, id ebml.ID) (byte, bool) {
	tb.Helper()
	reader := ebml.NewReader(bytes.NewReader(payload), ebml.ReaderOptions{})
	for reader.Offset() < int64(len(payload)) {
		header, err := reader.ReadHeader()
		if err != nil {
			tb.Fatal(err)
		}
		if header.ID == id {
			if header.Size.Unknown || header.Size.Value > uint64(len(payload)) {
				tb.Fatalf("invalid block size %+v", header.Size)
			}
			blockPayload := make([]byte, int(header.Size.Value))
			if err := reader.ReadFull(blockPayload); err != nil {
				tb.Fatal(err)
			}
			return rawBlockFlags(tb, blockPayload), true
		}
		if header.Size.Unknown {
			if isBlockSearchMaster(header.ID) {
				flags, ok := firstBlockFlagsInPayload(tb, payload[reader.Offset():], id)
				if ok {
					return flags, true
				}
			}
			return 0, false
		}
		if header.Size.Value > uint64(len(payload)) {
			tb.Fatalf("invalid element size %+v", header.Size)
		}
		if isBlockSearchMaster(header.ID) {
			childPayload := make([]byte, int(header.Size.Value))
			if err := reader.ReadFull(childPayload); err != nil {
				tb.Fatal(err)
			}
			flags, ok := firstBlockFlagsInPayload(tb, childPayload, id)
			if ok {
				return flags, true
			}
			continue
		}
		if err := reader.Skip(header.Size.Value); err != nil {
			tb.Fatal(err)
		}
	}
	return 0, false
}

func isBlockSearchMaster(id ebml.ID) bool {
	switch id {
	case idSegment, idCluster, idBlockGroup:
		return true
	default:
		return false
	}
}

func rawBlockFlags(tb testing.TB, payload []byte) byte {
	tb.Helper()
	reader := bytes.NewReader(payload)
	var scratch [ebml.MaxSizeWidth]byte
	if _, _, err := ebml.ReadUnsignedVINT(reader, &scratch); err != nil {
		tb.Fatal(err)
	}
	var header [3]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		tb.Fatal(err)
	}
	return header[2]
}

func readFirstDiscardPadding(tb testing.TB, data []byte) (int64, bool) {
	tb.Helper()
	reader := ebml.NewReader(bytes.NewReader(data), ebml.ReaderOptions{})
	for {
		header, err := reader.ReadHeader()
		if errors.Is(err, io.EOF) {
			return 0, false
		}
		if err != nil {
			tb.Fatal(err)
		}
		if header.ID == idBlockGroup {
			return readDiscardPaddingFromBlockGroup(tb, data[header.DataOffset:header.DataOffset+int64(header.Size.Value)])
		}
		if header.Size.Unknown {
			continue
		}
		if err := reader.Skip(header.Size.Value); err != nil {
			tb.Fatal(err)
		}
	}
}

func readDiscardPaddingFromBlockGroup(tb testing.TB, payload []byte) (int64, bool) {
	tb.Helper()
	reader := ebml.NewReader(bytes.NewReader(payload), ebml.ReaderOptions{})
	for reader.Offset() < int64(len(payload)) {
		header, err := reader.ReadHeader()
		if err != nil {
			tb.Fatal(err)
		}
		if header.ID == idDiscardPad {
			value, err := readIntPayload(reader, header.Size.Value)
			if err != nil {
				tb.Fatal(err)
			}
			return value, true
		}
		if header.Size.Unknown {
			tb.Fatalf("unexpected unknown BlockGroup child: %+v", header)
		}
		if err := reader.Skip(header.Size.Value); err != nil {
			tb.Fatal(err)
		}
	}
	return 0, false
}

func unknownElementBytes(tb testing.TB, id ebml.ID, payload []byte) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	writer := ebml.NewWriter(&buffer)
	if err := writer.WriteElement(id, payload); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func assertUnknownElement(tb testing.TB, name string, elements []UnknownElement, id ebml.ID, raw []byte) {
	tb.Helper()
	if len(elements) != 1 {
		tb.Fatalf("%s unknown elements = %d, want 1", name, len(elements))
	}
	if elements[0].ID != uint64(id) || !bytes.Equal(elements[0].Raw, raw) {
		tb.Fatalf("%s unknown element = %+v raw=%x, want id=0x%x raw=%x", name, elements[0], elements[0].Raw, uint64(id), raw)
	}
}

type discardWriter struct{}

func (discardWriter) Write(payload []byte) (int, error) {
	return len(payload), nil
}

type memoryWriteSeeker struct {
	bytes []byte
	pos   int64
}

func (m *memoryWriteSeeker) Write(p []byte) (int, error) {
	end := m.pos + int64(len(p))
	if end > int64(len(m.bytes)) {
		oldLen := len(m.bytes)
		endLen := int(end)
		if endLen <= cap(m.bytes) {
			m.bytes = m.bytes[:endLen]
			clear(m.bytes[oldLen:])
		} else {
			nextCap := max(cap(m.bytes)*2, endLen)
			next := make([]byte, endLen, nextCap)
			copy(next, m.bytes)
			m.bytes = next
		}
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

var errFailingWriter = errors.New("failing writer")

type failToggleWriteSeeker struct {
	memoryWriteSeeker
	fail bool
}

func (m *failToggleWriteSeeker) Write(payload []byte) (int, error) {
	if m.fail {
		return 0, errFailingWriter
	}
	return m.memoryWriteSeeker.Write(payload)
}
