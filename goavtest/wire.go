// The tiny binary encoding shared by the passthrough codec (Codec) and the
// fake container (Format): explicit little-endian fields, length-prefixed
// strings and byte blobs. It exists so encoded packets and muxed files are
// byte-faithful — what a test pushes in is exactly what it reads back out.

package goavtest

import "encoding/binary"

func appendU8(b []byte, v byte) []byte {
	return append(b, v)
}

func appendU32(b []byte, v uint32) []byte {
	return binary.LittleEndian.AppendUint32(b, v)
}

func appendI64(b []byte, v int64) []byte {
	return binary.LittleEndian.AppendUint64(b, uint64(v))
}

func appendWireString(b []byte, s string) []byte {
	b = appendU32(b, uint32(len(s)))
	return append(b, s...)
}

func appendWireBytes(b []byte, p []byte) []byte {
	b = appendU32(b, uint32(len(p)))
	return append(b, p...)
}

// wireReader walks one encoded buffer; any out-of-bounds read latches ok=false
// and every later read returns zero values, so parsers check ok once at the end.
type wireReader struct {
	b   []byte
	off int
	ok  bool
}

func newWireReader(b []byte) *wireReader {
	return &wireReader{b: b, ok: true}
}

func (r *wireReader) take(n int) []byte {
	if !r.ok || n < 0 || r.off+n > len(r.b) {
		r.ok = false
		return nil
	}
	out := r.b[r.off : r.off+n]
	r.off += n
	return out
}

func (r *wireReader) u8() byte {
	b := r.take(1)
	if !r.ok {
		return 0
	}
	return b[0]
}

func (r *wireReader) u32() uint32 {
	b := r.take(4)
	if !r.ok {
		return 0
	}
	return binary.LittleEndian.Uint32(b)
}

func (r *wireReader) i64() int64 {
	b := r.take(8)
	if !r.ok {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(b))
}

func (r *wireReader) str() string {
	return string(r.take(int(r.u32())))
}

func (r *wireReader) blob() []byte {
	return r.take(int(r.u32()))
}
