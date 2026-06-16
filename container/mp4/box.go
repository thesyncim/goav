package mp4

import (
	"encoding/binary"
	"io"
)

const (
	boxHeaderSize      = 8  // 32-bit size + 4cc type
	boxLargeHeaderSize = 16 // size==1: 64-bit largesize follows the type
	// maxBoxPayload caps a single box payload buffered into memory (sample
	// tables, codec config). Sample-table boxes scale with sample count, so this
	// bound keeps a crafted huge box from exhausting memory during parsing.
	maxBoxPayload = 256 << 20
)

// boxHeader is one parsed ISO BMFF box header. Size is the total box size
// including the header; PayloadOffset/PayloadSize bound the contents.
type boxHeader struct {
	Type          string
	Offset        int64
	Size          int64
	PayloadOffset int64
	PayloadSize   int64
}

func (h boxHeader) end() int64 { return h.Offset + h.Size }

// readBoxHeader reads the box header at off. limit is the exclusive upper bound
// (the parent box end or file size); a declared size of 0 means the box runs to
// limit, and a size of 1 selects the 64-bit largesize form. It returns io.EOF
// when there is no room for another header.
func readBoxHeader(r io.ReaderAt, off, limit int64) (boxHeader, error) {
	if off < 0 || off+boxHeaderSize > limit {
		return boxHeader{}, io.EOF
	}
	var buf [boxLargeHeaderSize]byte
	if _, err := r.ReadAt(buf[:boxHeaderSize], off); err != nil {
		return boxHeader{}, err
	}
	size := int64(binary.BigEndian.Uint32(buf[0:4]))
	typ := string(buf[4:8])
	headerSize := int64(boxHeaderSize)
	switch size {
	case 1:
		if off+boxLargeHeaderSize > limit {
			return boxHeader{}, ErrTruncated
		}
		if _, err := r.ReadAt(buf[8:16], off+8); err != nil {
			return boxHeader{}, err
		}
		size = int64(binary.BigEndian.Uint64(buf[8:16]))
		headerSize = boxLargeHeaderSize
	case 0:
		size = limit - off
	}
	if size < headerSize || off+size > limit {
		return boxHeader{}, ErrTruncated
	}
	return boxHeader{
		Type:          typ,
		Offset:        off,
		Size:          size,
		PayloadOffset: off + headerSize,
		PayloadSize:   size - headerSize,
	}, nil
}

// walkBoxes iterates the boxes in [start, end), invoking visit for each. A
// visit error stops the walk and propagates. Each step advances by the box
// size, which readBoxHeader guarantees is at least the header size, so a
// malformed size cannot loop forever.
func walkBoxes(r io.ReaderAt, start, end int64, visit func(boxHeader) error) error {
	for off := start; off+boxHeaderSize <= end; {
		h, err := readBoxHeader(r, off, end)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err := visit(h); err != nil {
			return err
		}
		off = h.end()
	}
	return nil
}

// readPayload reads a box's full payload into a fresh buffer, refusing payloads
// larger than maxBoxPayload so a crafted box cannot exhaust memory.
func readPayload(r io.ReaderAt, h boxHeader) ([]byte, error) {
	if h.PayloadSize < 0 || h.PayloadSize > maxBoxPayload {
		return nil, ErrInvalidData
	}
	buf := make([]byte, h.PayloadSize)
	if h.PayloadSize == 0 {
		return buf, nil
	}
	if _, err := r.ReadAt(buf, h.PayloadOffset); err != nil {
		return nil, err
	}
	return buf, nil
}

// parser is a bounds-checked cursor over a box payload. Every read that would
// pass the end records ErrTruncated and returns a zero value; callers check
// err() once after parsing instead of on every field.
type parser struct {
	data []byte
	pos  int
	bad  bool
}

func newParser(b []byte) *parser { return &parser{data: b} }

func (p *parser) err() error {
	if p.bad {
		return ErrTruncated
	}
	return nil
}

func (p *parser) remaining() int { return len(p.data) - p.pos }

func (p *parser) need(n int) bool {
	if p.bad || n < 0 || p.pos+n > len(p.data) {
		p.bad = true
		return false
	}
	return true
}

func (p *parser) u8() uint8 {
	if !p.need(1) {
		return 0
	}
	v := p.data[p.pos]
	p.pos++
	return v
}

func (p *parser) u16() uint16 {
	if !p.need(2) {
		return 0
	}
	v := binary.BigEndian.Uint16(p.data[p.pos:])
	p.pos += 2
	return v
}

func (p *parser) u24() uint32 {
	if !p.need(3) {
		return 0
	}
	v := uint32(p.data[p.pos])<<16 | uint32(p.data[p.pos+1])<<8 | uint32(p.data[p.pos+2])
	p.pos += 3
	return v
}

func (p *parser) u32() uint32 {
	if !p.need(4) {
		return 0
	}
	v := binary.BigEndian.Uint32(p.data[p.pos:])
	p.pos += 4
	return v
}

func (p *parser) i32() int32 { return int32(p.u32()) }

func (p *parser) u64() uint64 {
	if !p.need(8) {
		return 0
	}
	v := binary.BigEndian.Uint64(p.data[p.pos:])
	p.pos += 8
	return v
}

func (p *parser) skip(n int) {
	if p.need(n) {
		p.pos += n
	}
}

// take returns the next n bytes as a sub-slice into the payload (no copy).
func (p *parser) take(n int) []byte {
	if !p.need(n) {
		return nil
	}
	b := p.data[p.pos : p.pos+n]
	p.pos += n
	return b
}

// fullBox reads the version byte and 24-bit flags that prefix a FullBox.
func (p *parser) fullBox() (version uint8, flags uint32) {
	version = p.u8()
	flags = p.u24()
	return version, flags
}
