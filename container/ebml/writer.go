package ebml

import (
	"encoding/binary"
	"io"
	"math"
)

type Writer struct {
	w       io.Writer
	offset  int64
	scratch [16]byte
}

type SizePatch struct {
	Offset int64
	Width  int
	Start  int64
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

func (w *Writer) Offset() int64 {
	if w == nil {
		return 0
	}
	return w.offset
}

func (w *Writer) Write(p []byte) (int, error) {
	if w == nil || w.w == nil {
		return 0, io.ErrClosedPipe
	}
	n, err := writeFull(w.w, p)
	w.offset += int64(n)
	return n, err
}

func (w *Writer) WriteHeader(id ID, size uint64) error {
	n, err := EncodeID(w.scratch[:], id)
	if err != nil {
		return err
	}
	m, err := EncodeSizeVINT(w.scratch[n:], size)
	if err != nil {
		return err
	}
	_, err = w.Write(w.scratch[:n+m])
	return err
}

func (w *Writer) WriteUnknownHeader(id ID, sizeWidth int) error {
	n, err := EncodeID(w.scratch[:], id)
	if err != nil {
		return err
	}
	m, err := EncodeUnknownSize(w.scratch[n:], sizeWidth)
	if err != nil {
		return err
	}
	_, err = w.Write(w.scratch[:n+m])
	return err
}

func (w *Writer) WriteElement(id ID, payload []byte) error {
	if err := w.WriteHeader(id, uint64(len(payload))); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func (w *Writer) WriteString(id ID, value string) error {
	if err := w.WriteHeader(id, uint64(len(value))); err != nil {
		return err
	}
	_, err := w.Write([]byte(value))
	return err
}

func (w *Writer) WriteUInt(id ID, value uint64) error {
	n := uintPayloadWidth(value)
	if err := w.WriteHeader(id, uint64(n)); err != nil {
		return err
	}
	for i := n - 1; i >= 0; i-- {
		w.scratch[i] = byte(value)
		value >>= 8
	}
	_, err := w.Write(w.scratch[:n])
	return err
}

func (w *Writer) WriteInt(id ID, value int64) error {
	n := intPayloadWidth(value)
	if err := w.WriteHeader(id, uint64(n)); err != nil {
		return err
	}
	for i := n - 1; i >= 0; i-- {
		w.scratch[i] = byte(value)
		value >>= 8
	}
	_, err := w.Write(w.scratch[:n])
	return err
}

func (w *Writer) WriteFloat64(id ID, value float64) error {
	if err := w.WriteHeader(id, 8); err != nil {
		return err
	}
	binary.BigEndian.PutUint64(w.scratch[:8], math.Float64bits(value))
	_, err := w.Write(w.scratch[:8])
	return err
}

func (w *Writer) WriteVoid(payloadSize uint64) error {
	if err := w.WriteHeader(0xec, payloadSize); err != nil {
		return err
	}
	clear(w.scratch[:])
	for payloadSize != 0 {
		n := len(w.scratch)
		if payloadSize < uint64(n) {
			n = int(payloadSize)
		}
		written, err := w.Write(w.scratch[:n])
		payloadSize -= uint64(written)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) StartSizedElement(id ID, width int) (SizePatch, error) {
	if width < 1 || width > MaxSizeWidth {
		return SizePatch{}, ErrInvalidSize
	}
	n, err := EncodeID(w.scratch[:], id)
	if err != nil {
		return SizePatch{}, err
	}
	m, err := EncodeUnknownSize(w.scratch[n:], width)
	if err != nil {
		return SizePatch{}, err
	}
	if _, err := w.Write(w.scratch[:n+m]); err != nil {
		return SizePatch{}, err
	}
	return SizePatch{Offset: w.offset - int64(width), Width: width, Start: w.offset}, nil
}

func (w *Writer) FinishSizedElement(patch SizePatch) error {
	seeker, ok := w.w.(io.Seeker)
	if !ok {
		return ErrNonSeekableWriter
	}
	current := w.offset
	if patch.Width < 1 || patch.Width > MaxSizeWidth || patch.Start < patch.Offset+int64(patch.Width) || current < patch.Start {
		return ErrInvalidSize
	}
	size := uint64(current - patch.Start)
	if _, err := seeker.Seek(patch.Offset, io.SeekStart); err != nil {
		return err
	}
	w.offset = patch.Offset
	n, err := EncodeSizeVINTWidth(w.scratch[:], size, patch.Width)
	if err != nil {
		return err
	}
	if _, err := w.Write(w.scratch[:n]); err != nil {
		return err
	}
	if _, err := seeker.Seek(current, io.SeekStart); err != nil {
		return err
	}
	w.offset = current
	return nil
}

func uintPayloadWidth(value uint64) int {
	switch {
	case value <= 0xff:
		return 1
	case value <= 0xffff:
		return 2
	case value <= 0xffffff:
		return 3
	case value <= 0xffffffff:
		return 4
	case value <= 0xffffffffff:
		return 5
	case value <= 0xffffffffffff:
		return 6
	case value <= 0xffffffffffffff:
		return 7
	default:
		return 8
	}
}

func intPayloadWidth(value int64) int {
	for width := 1; width < 8; width++ {
		shift := uint(width * 8)
		min := int64(-1) << (shift - 1)
		max := int64(1)<<(shift-1) - 1
		if value >= min && value <= max {
			return width
		}
	}
	return 8
}

func writeFull(w io.Writer, payload []byte) (int, error) {
	total := 0
	for len(payload) != 0 {
		n, err := w.Write(payload)
		total += n
		payload = payload[n:]
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}
