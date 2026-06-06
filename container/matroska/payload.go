package matroska

import (
	"encoding/binary"
	"io"
	"math"

	"github.com/thesyncim/goav/container/ebml"
)

func writeBinary(w *ebml.Writer, id ebml.ID, payload []byte) error {
	if err := w.WriteHeader(id, uint64(len(payload))); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func readUIntPayload(r io.Reader, size uint64) (uint64, error) {
	var scratch [8]byte
	return readUIntPayloadScratch(r, size, &scratch)
}

func readBoolFlagPayload(r io.Reader, size uint64) (bool, error) {
	value, err := readUIntPayload(r, size)
	if err != nil {
		return false, err
	}
	switch value {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, ErrInvalidData
	}
}

func readUIntPayloadScratch(r io.Reader, size uint64, scratch *[8]byte) (uint64, error) {
	if size > 8 {
		return 0, ErrInvalidData
	}
	clear(scratch[:])
	if _, err := io.ReadFull(r, scratch[8-size:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(scratch[:]), nil
}

func readIntPayload(r io.Reader, size uint64) (int64, error) {
	if size == 0 || size > 8 {
		return 0, ErrInvalidData
	}
	var scratch [8]byte
	if _, err := io.ReadFull(r, scratch[8-size:]); err != nil {
		return 0, err
	}
	if scratch[8-size]&0x80 != 0 {
		for i := 0; i < int(8-size); i++ {
			scratch[i] = 0xff
		}
	}
	return int64(binary.BigEndian.Uint64(scratch[:])), nil
}

func readFloatPayload(r io.Reader, size uint64) (float64, error) {
	var scratch [8]byte
	switch size {
	case 4:
		if _, err := io.ReadFull(r, scratch[:4]); err != nil {
			return 0, err
		}
		return float64(math.Float32frombits(binary.BigEndian.Uint32(scratch[:4]))), nil
	case 8:
		if _, err := io.ReadFull(r, scratch[:8]); err != nil {
			return 0, err
		}
		return math.Float64frombits(binary.BigEndian.Uint64(scratch[:8])), nil
	default:
		return 0, ErrInvalidData
	}
}

func readStringPayload(r io.Reader, size uint64) (string, error) {
	if size > uint64(^uint(0)>>1) {
		return "", ErrInvalidData
	}
	payload := make([]byte, int(size))
	if _, err := io.ReadFull(r, payload); err != nil {
		return "", err
	}
	return string(payload), nil
}

func readBinaryPayload(r io.Reader, size uint64) ([]byte, error) {
	if size > uint64(^uint(0)>>1) {
		return nil, ErrInvalidData
	}
	payload := make([]byte, int(size))
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func readElementIDPayload(r io.Reader, size uint64) (ebml.ID, error) {
	if size == 0 || size > ebml.MaxIDWidth {
		return 0, ErrInvalidData
	}
	var scratch [ebml.MaxIDWidth]byte
	if _, err := io.ReadFull(r, scratch[:size]); err != nil {
		return 0, err
	}
	id, width, err := ebml.DecodeIDVINT(scratch[:size])
	if err != nil {
		return 0, err
	}
	if uint64(width) != size {
		return 0, ErrInvalidData
	}
	return id, nil
}

func skipElement(r *ebml.Reader, header ebml.Header) error {
	if header.Size.Unknown {
		return ErrUnsupportedElement
	}
	return r.Skip(header.Size.Value)
}
