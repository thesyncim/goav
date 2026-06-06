package ebml

import (
	"encoding/binary"
	"hash/crc32"
	"io"
)

const (
	CRC32ID ID = 0xbf
	VoidID  ID = 0xec
)

func CRC32(payload []byte) uint32 {
	return crc32.ChecksumIEEE(payload)
}

func PutCRC32(dst []byte, payload []byte) error {
	if len(dst) < 4 {
		return io.ErrShortBuffer
	}
	binary.LittleEndian.PutUint32(dst[:4], CRC32(payload))
	return nil
}

func ValidateCRC32(stored []byte, payload []byte) bool {
	if len(stored) != 4 {
		return false
	}
	return binary.LittleEndian.Uint32(stored) == CRC32(payload)
}

func (w *Writer) WriteCRC32(payload []byte) error {
	if err := w.WriteHeader(CRC32ID, 4); err != nil {
		return err
	}
	if err := PutCRC32(w.scratch[:4], payload); err != nil {
		return err
	}
	_, err := w.Write(w.scratch[:4])
	return err
}
