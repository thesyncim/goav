package matroska

import (
	"encoding/binary"
	"io"
)

var h264StartCode = [...]byte{0x00, 0x00, 0x00, 0x01}

func h264TrackNALULengthSize(track Track) (int, bool, error) {
	if track.Codec != CodecH264 || len(track.CodecPrivate) == 0 {
		return 0, false, nil
	}
	config, err := parseAVCDecoderConfigurationRecord(track.CodecPrivate)
	if err != nil {
		return 0, false, err
	}
	return config.NALULengthSize, true, nil
}

func h264MuxedPayloadSize(track Track, data []byte) (int, bool, error) {
	if track.Codec != CodecH264 || len(track.CodecPrivate) == 0 {
		return len(data), false, nil
	}
	config, err := parseAVCDecoderConfigurationRecord(track.CodecPrivate)
	if err != nil {
		return 0, false, err
	}
	lengthSize := config.NALULengthSize
	if h264ValidateAVCSample(data, lengthSize) == nil {
		return len(data), false, nil
	}
	size, err := h264AnnexBToAVCSize(data, lengthSize)
	if err != nil {
		return 0, false, err
	}
	return size, true, nil
}

func h264WriteMuxedPayload(w io.Writer, track Track, data []byte, scratch *[16]byte) error {
	if track.Codec != CodecH264 || len(track.CodecPrivate) == 0 {
		_, err := w.Write(data)
		return err
	}
	config, err := parseAVCDecoderConfigurationRecord(track.CodecPrivate)
	if err != nil {
		return err
	}
	lengthSize := config.NALULengthSize
	if h264ValidateAVCSample(data, lengthSize) == nil {
		_, err = w.Write(data)
		return err
	}
	return h264WriteAnnexBAsAVC(w, data, lengthSize, scratch)
}

func h264AnnexBToAVCSize(data []byte, lengthSize int) (int, error) {
	total := 0
	err := h264IterAnnexBNALUs(data, func(nalu []byte) error {
		if err := h264ValidateNALUSize(len(nalu), lengthSize); err != nil {
			return err
		}
		total += lengthSize + len(nalu)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return total, nil
}

func h264WriteAnnexBAsAVC(w io.Writer, data []byte, lengthSize int, scratch *[16]byte) error {
	return h264IterAnnexBNALUs(data, func(nalu []byte) error {
		if err := h264ValidateNALUSize(len(nalu), lengthSize); err != nil {
			return err
		}
		putH264NALUSize(scratch[:], len(nalu), lengthSize)
		if _, err := w.Write(scratch[:lengthSize]); err != nil {
			return err
		}
		_, err := w.Write(nalu)
		return err
	})
}

func h264IterAnnexBNALUs(data []byte, fn func([]byte) error) error {
	start, width := h264FindStartCode(data, 0)
	if start < 0 {
		return ErrInvalidData
	}
	for i := 0; i < start; i++ {
		if data[i] != 0 {
			return ErrInvalidData
		}
	}
	for start >= 0 {
		naluStart := start + width
		next, nextWidth := h264FindStartCode(data, naluStart)
		naluEnd := len(data)
		if next >= 0 {
			naluEnd = next
		}
		if naluStart >= naluEnd {
			return ErrInvalidData
		}
		if err := fn(data[naluStart:naluEnd]); err != nil {
			return err
		}
		start = next
		width = nextWidth
	}
	return nil
}

func h264FindStartCode(data []byte, offset int) (int, int) {
	for i := offset; i+3 <= len(data); i++ {
		if data[i] != 0 || data[i+1] != 0 {
			continue
		}
		if data[i+2] == 1 {
			return i, 3
		}
		if i+4 <= len(data) && data[i+2] == 0 && data[i+3] == 1 {
			return i, 4
		}
	}
	return -1, 0
}

func h264ValidateNALUSize(size int, lengthSize int) error {
	if size <= 0 {
		return ErrInvalidData
	}
	switch lengthSize {
	case 1:
		if size > 0xff {
			return ErrInvalidData
		}
	case 2:
		if size > 0xffff {
			return ErrInvalidData
		}
	case 4:
		if uint64(size) > uint64(^uint32(0)) {
			return ErrInvalidData
		}
	default:
		return ErrInvalidData
	}
	return nil
}

func putH264NALUSize(dst []byte, size int, lengthSize int) {
	switch lengthSize {
	case 1:
		dst[0] = byte(size)
	case 2:
		binary.BigEndian.PutUint16(dst[:2], uint16(size))
	case 4:
		binary.BigEndian.PutUint32(dst[:4], uint32(size))
	}
}

func h264ValidateAVCSample(data []byte, lengthSize int) error {
	offset := 0
	for offset < len(data) {
		size, next, err := h264ReadAVCNALUSize(data, offset, lengthSize)
		if err != nil {
			return err
		}
		if size == 0 || next+size > len(data) {
			return ErrInvalidData
		}
		naluType := data[next] & avcNALUTypeMask
		if naluType == 0 || naluType > 23 {
			return ErrInvalidData
		}
		offset = next + size
	}
	if offset == 0 {
		return ErrInvalidData
	}
	return nil
}

func h264ReadAVCNALUSize(data []byte, offset int, lengthSize int) (int, int, error) {
	if offset < 0 || offset+lengthSize > len(data) {
		return 0, 0, ErrInvalidData
	}
	switch lengthSize {
	case 1:
		return int(data[offset]), offset + 1, nil
	case 2:
		return int(binary.BigEndian.Uint16(data[offset : offset+2])), offset + 2, nil
	case 4:
		value := binary.BigEndian.Uint32(data[offset : offset+4])
		if uint64(value) > uint64(^uint(0)>>1) {
			return 0, 0, ErrInvalidData
		}
		return int(value), offset + 4, nil
	default:
		return 0, 0, ErrInvalidData
	}
}

func h264AVCToAnnexBSize(data []byte, lengthSize int) (int, error) {
	total := 0
	offset := 0
	for offset < len(data) {
		size, next, err := h264ReadAVCNALUSize(data, offset, lengthSize)
		if err != nil {
			return 0, err
		}
		if size == 0 || next+size > len(data) {
			return 0, ErrInvalidData
		}
		total += len(h264StartCode) + size
		offset = next + size
	}
	if offset == 0 {
		return 0, ErrInvalidData
	}
	return total, nil
}

func h264AVCToAnnexBInPlace(data []byte, outSize int, lengthSize int) ([]byte, error) {
	if cap(data) < outSize {
		return nil, ErrPayloadTooSmall
	}
	if lengthSize == len(h264StartCode) && len(data) == outSize {
		offset := 0
		for offset < len(data) {
			size, next, err := h264ReadAVCNALUSize(data, offset, lengthSize)
			if err != nil {
				return nil, err
			}
			if size == 0 || next+size > len(data) {
				return nil, ErrInvalidData
			}
			copy(data[offset:next], h264StartCode[:])
			offset = next + size
		}
		return data, nil
	}
	input := make([]byte, len(data))
	copy(input, data)
	out := data[:outSize]
	inOffset := 0
	outOffset := 0
	for inOffset < len(input) {
		size, next, err := h264ReadAVCNALUSize(input, inOffset, lengthSize)
		if err != nil {
			return nil, err
		}
		if size == 0 || next+size > len(input) {
			return nil, ErrInvalidData
		}
		copy(out[outOffset:], h264StartCode[:])
		outOffset += len(h264StartCode)
		copy(out[outOffset:], input[next:next+size])
		outOffset += size
		inOffset = next + size
	}
	return out, nil
}
