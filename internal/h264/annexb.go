package h264

import "encoding/binary"

// ForEachNAL calls fn with each NAL unit of the Annex B access unit data,
// without its start code. Data must begin with a start code, after optional
// leading zero bytes.
func ForEachNAL(data []byte, fn func(nal []byte) error) error {
	start, width := findStartCode(data, 0)
	if start < 0 {
		return ErrInvalid
	}
	for i := range start {
		if data[i] != 0 {
			return ErrInvalid
		}
	}
	for start >= 0 {
		nalStart := start + width
		next, nextWidth := findStartCode(data, nalStart)
		nalEnd := len(data)
		if next >= 0 {
			nalEnd = next
		}
		if nalStart >= nalEnd {
			return ErrInvalid
		}
		if err := fn(data[nalStart:nalEnd]); err != nil {
			return err
		}
		start, width = next, nextWidth
	}
	return nil
}

func findStartCode(data []byte, offset int) (int, int) {
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

// AppendAVC appends the Annex B access unit src to dst as an AVCC sample
// with 4-byte NAL lengths.
func AppendAVC(dst []byte, src []byte) ([]byte, error) {
	err := ForEachNAL(src, func(nal []byte) error {
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(nal)))
		dst = append(dst, nal...)
		return nil
	})
	return dst, err
}

// ConfigFromAnnexB builds an avcC record, with 4-byte NAL lengths, from the
// parameter sets the Annex B access units carry.
func ConfigFromAnnexB(units ...[]byte) ([]byte, error) {
	var sps, pps [][]byte
	for _, unit := range units {
		if err := ForEachNAL(unit, func(nal []byte) error {
			switch nal[0] & 0x1f {
			case NALSPS:
				if len(nal) < 4 || len(sps) == 31 {
					return ErrInvalid
				}
				sps = append(sps, nal)
			case NALPPS:
				if len(pps) == 255 {
					return ErrInvalid
				}
				pps = append(pps, nal)
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	if len(sps) == 0 || len(pps) == 0 {
		return nil, ErrInvalid
	}
	record := []byte{1, sps[0][1], sps[0][2], sps[0][3], 0xff, 0xe0 | byte(len(sps))}
	for _, set := range sps {
		if len(set) > 0xffff {
			return nil, ErrInvalid
		}
		record = binary.BigEndian.AppendUint16(record, uint16(len(set)))
		record = append(record, set...)
	}
	record = append(record, byte(len(pps)))
	for _, set := range pps {
		if len(set) > 0xffff {
			return nil, ErrInvalid
		}
		record = binary.BigEndian.AppendUint16(record, uint16(len(set)))
		record = append(record, set...)
	}
	return record, nil
}
