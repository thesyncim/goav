package matroska

import (
	"encoding/binary"
	"io"
	"math"
	"time"

	"github.com/thesyncim/goav/container/ebml"
)

var ebmlDateEpoch = time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)

func writeBinary(w *ebml.Writer, id ebml.ID, payload []byte) error {
	if err := w.WriteHeader(id, uint64(len(payload))); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func writeDate(w *ebml.Writer, id ebml.ID, value time.Time) error {
	nanos, err := ebmlDateNanos(value)
	if err != nil {
		return err
	}
	if err := w.WriteHeader(id, 8); err != nil {
		return err
	}
	var scratch [8]byte
	binary.BigEndian.PutUint64(scratch[:], uint64(nanos))
	_, err = w.Write(scratch[:])
	return err
}

func ebmlDateNanos(value time.Time) (int64, error) {
	utc := value.UTC()
	nanos := utc.Sub(ebmlDateEpoch)
	if !ebmlDateEpoch.Add(nanos).Equal(utc) {
		return 0, ErrInvalidData
	}
	return int64(nanos), nil
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

func readDatePayload(r io.Reader, size uint64) (time.Time, error) {
	switch size {
	case 0:
		return ebmlDateEpoch, nil
	case 8:
		value, err := readIntPayload(r, size)
		if err != nil {
			return time.Time{}, err
		}
		return ebmlDateEpoch.Add(time.Duration(value)), nil
	default:
		return time.Time{}, ErrInvalidData
	}
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
	payload, err := readSizedPayload(r, size)
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

func readBinaryPayload(r io.Reader, size uint64) ([]byte, error) {
	return readSizedPayload(r, size)
}

func readSizedPayload(r io.Reader, size uint64) ([]byte, error) {
	if err := validatePayloadReadSize(r, size); err != nil {
		return nil, err
	}
	payload := make([]byte, int(size))
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func validatePayloadReadSize(r io.Reader, size uint64) error {
	if size > maxIntValue {
		return ErrInvalidData
	}
	reader, ok := r.(*ebml.Reader)
	if !ok {
		return nil
	}
	remaining, ok := reader.Remaining()
	if ok && size > remaining {
		return ErrInvalidData
	}
	return nil
}

func readUnknownElementPayload(r *ebml.Reader, header ebml.Header) (UnknownElement, error) {
	if header.Size.Unknown {
		return UnknownElement{}, ErrUnsupportedElement
	}
	if header.Size.Value > uint64(^uint(0)>>1) ||
		uint64(header.HeaderSize) > uint64(^uint(0)>>1)-header.Size.Value {
		return UnknownElement{}, ErrInvalidData
	}
	raw := make([]byte, int(uint64(header.HeaderSize)+header.Size.Value))
	idWidth, err := ebml.EncodeID(raw, header.ID)
	if err != nil {
		return UnknownElement{}, err
	}
	if idWidth+header.Size.Width != header.HeaderSize {
		return UnknownElement{}, ErrInvalidData
	}
	if _, err := ebml.EncodeSizeVINTWidth(raw[idWidth:], header.Size.Value, header.Size.Width); err != nil {
		return UnknownElement{}, err
	}
	if err := r.ReadFull(raw[header.HeaderSize:]); err != nil {
		return UnknownElement{}, err
	}
	return UnknownElement{ID: uint64(header.ID), Raw: raw}, nil
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
