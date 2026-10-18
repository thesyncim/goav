package mp4

import "encoding/binary"

// boxWriter builds ISO BMFF boxes into one byte slice. start writes a box
// header with a placeholder size and end patches it, so boxes nest the way
// the format does.
type boxWriter struct {
	b    []byte
	open []int
}

func (w *boxWriter) start(typ string) {
	w.open = append(w.open, len(w.b))
	w.b = append(w.b, 0, 0, 0, 0)
	w.b = append(w.b, typ...)
}

// startFull starts a FullBox: a box with a version and 24 bits of flags.
func (w *boxWriter) startFull(typ string, version uint8, flags uint32) {
	w.start(typ)
	w.u32(uint32(version)<<24 | flags&0xffffff)
}

func (w *boxWriter) end() {
	at := w.open[len(w.open)-1]
	w.open = w.open[:len(w.open)-1]
	binary.BigEndian.PutUint32(w.b[at:], uint32(len(w.b)-at))
}

func (w *boxWriter) u8(v uint8)     { w.b = append(w.b, v) }
func (w *boxWriter) u16(v uint16)   { w.b = binary.BigEndian.AppendUint16(w.b, v) }
func (w *boxWriter) u32(v uint32)   { w.b = binary.BigEndian.AppendUint32(w.b, v) }
func (w *boxWriter) u64(v uint64)   { w.b = binary.BigEndian.AppendUint64(w.b, v) }
func (w *boxWriter) i16(v int16)    { w.u16(uint16(v)) }
func (w *boxWriter) bytes(v []byte) { w.b = append(w.b, v...) }
func (w *boxWriter) str(v string)   { w.b = append(w.b, v...) }
func (w *boxWriter) zeros(n int)    { w.b = append(w.b, make([]byte, n)...) }
func (w *boxWriter) box(typ string, payload []byte) {
	w.start(typ)
	w.bytes(payload)
	w.end()
}

// matrix writes the identity transformation matrix tkhd and mvhd carry.
func (w *boxWriter) matrix() {
	for _, v := range [9]uint32{0x00010000, 0, 0, 0, 0x00010000, 0, 0, 0, 0x40000000} {
		w.u32(v)
	}
}
