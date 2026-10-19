package kit

import (
	"encoding/binary"
	"errors"

	"github.com/thesyncim/goav/av"
)

// Bitstream is how a video backend frames NAL units: goav carries H.264 and
// HEVC as Annex B, and the kit converts for a backend that wants
// length-prefixed AVCC, moving the parameter sets between the stream and the
// codec configuration record.
type Bitstream int

// The bitstream framings.
const (
	AnnexB Bitstream = iota
	AVCC
)

// CodecHEVC is H.265/HEVC, which the goav vocabulary does not name yet.
const CodecHEVC av.CodecID = "hevc"

var errBitstream = errors.New("kit: malformed bitstream")

var startCode = []byte{0, 0, 0, 1}

// nalUnits splits Annex B data into NAL units without their start codes.
func nalUnits(data []byte) [][]byte {
	var units [][]byte
	start := -1
	for i := 0; i+2 < len(data); {
		if data[i] == 0 && data[i+1] == 0 && data[i+2] == 1 {
			if start >= 0 {
				units = append(units, trimTrailingZeros(data[start:i]))
			}
			i += 3
			start = i
			continue
		}
		i++
	}
	if start >= 0 && start < len(data) {
		units = append(units, data[start:])
	}
	return units
}

func trimTrailingZeros(nal []byte) []byte {
	for len(nal) > 0 && nal[len(nal)-1] == 0 {
		nal = nal[:len(nal)-1]
	}
	return nal
}

func hasStartCode(data []byte) bool {
	return len(data) >= 3 && data[0] == 0 && data[1] == 0 && (data[2] == 1 || (len(data) >= 4 && data[2] == 0 && data[3] == 1))
}

// lengthPrefixed splits AVCC data with 4-byte lengths into NAL units.
func lengthPrefixed(data []byte) ([][]byte, error) {
	var units [][]byte
	for len(data) > 0 {
		if len(data) < 4 {
			return nil, errBitstream
		}
		n := binary.BigEndian.Uint32(data)
		data = data[4:]
		if uint64(n) > uint64(len(data)) {
			return nil, errBitstream
		}
		units = append(units, data[:n])
		data = data[n:]
	}
	return units, nil
}

// accessUnit reads a goav packet payload: Annex B, or length-prefixed when a
// source hands it over unconverted.
func accessUnit(data []byte) ([][]byte, error) {
	if hasStartCode(data) {
		return nalUnits(data), nil
	}
	return lengthPrefixed(data)
}

func appendAnnexB(dst []byte, units ...[]byte) []byte {
	for _, unit := range units {
		dst = append(dst, startCode...)
		dst = append(dst, unit...)
	}
	return dst
}

func appendAVCC(dst []byte, units ...[]byte) []byte {
	for _, unit := range units {
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(unit)))
		dst = append(dst, unit...)
	}
	return dst
}

// nalKind is a NAL unit's type in its codec's numbering.
func nalKind(id av.CodecID, unit []byte) int {
	if len(unit) == 0 {
		return -1
	}
	if id == CodecHEVC {
		return int(unit[0]>>1) & 0x3f
	}
	return int(unit[0] & 0x1f)
}

// parameterSet reports a VPS, SPS, or PPS.
func parameterSet(id av.CodecID, unit []byte) bool {
	kind := nalKind(id, unit)
	if id == CodecHEVC {
		return kind >= 32 && kind <= 34
	}
	return kind == 7 || kind == 8
}

// randomAccess reports an IDR (H.264) or IRAP (HEVC) slice.
func randomAccess(id av.CodecID, unit []byte) bool {
	kind := nalKind(id, unit)
	if id == CodecHEVC {
		return kind >= 16 && kind <= 23
	}
	return kind == 5
}

// parameterSetsOf collects the parameter sets in units, in order.
func parameterSetsOf(id av.CodecID, units [][]byte) [][]byte {
	var sets [][]byte
	for _, unit := range units {
		if parameterSet(id, unit) {
			sets = append(sets, unit)
		}
	}
	return sets
}

// configParameterSets reads the parameter sets of a codec configuration:
// an avcC or hvcC record, or Annex B parameter sets.
func configParameterSets(id av.CodecID, config []byte) ([][]byte, error) {
	switch {
	case len(config) == 0:
		return nil, nil
	case hasStartCode(config):
		return parameterSetsOf(id, nalUnits(config)), nil
	case id == CodecHEVC:
		return hvcCParameterSets(config)
	default:
		return avcCParameterSets(config)
	}
}

func avcCParameterSets(record []byte) ([][]byte, error) {
	if len(record) < 7 || record[0] != 1 {
		return nil, errBitstream
	}
	var sets [][]byte
	data := record[5:]
	read := func(count int) error {
		for range count {
			if len(data) < 2 {
				return errBitstream
			}
			n := int(binary.BigEndian.Uint16(data))
			if len(data) < 2+n {
				return errBitstream
			}
			sets = append(sets, data[2:2+n])
			data = data[2+n:]
		}
		return nil
	}
	if err := read(int(data[0] & 0x1f)); err != nil {
		return nil, err
	}
	data = data[1:]
	if len(data) == 0 {
		return sets, nil
	}
	count := int(data[0])
	data = data[1:]
	return sets, read(count)
}

func hvcCParameterSets(record []byte) ([][]byte, error) {
	if len(record) < 23 || record[0] != 1 {
		return nil, errBitstream
	}
	arrays := int(record[22])
	data := record[23:]
	var sets [][]byte
	for range arrays {
		if len(data) < 3 {
			return nil, errBitstream
		}
		count := int(binary.BigEndian.Uint16(data[1:]))
		data = data[3:]
		for range count {
			if len(data) < 2 {
				return nil, errBitstream
			}
			n := int(binary.BigEndian.Uint16(data))
			if len(data) < 2+n {
				return nil, errBitstream
			}
			sets = append(sets, data[2:2+n])
			data = data[2+n:]
		}
	}
	return sets, nil
}

// avcC builds an AVCDecoderConfigurationRecord with 4-byte lengths from
// H.264 parameter sets, or nil when they hold no SPS.
func avcC(sets [][]byte) []byte {
	var sps, pps [][]byte
	for _, set := range sets {
		switch nalKind(av.CodecH264, set) {
		case 7:
			sps = append(sps, set)
		case 8:
			pps = append(pps, set)
		}
	}
	if len(sps) == 0 || len(sps[0]) < 4 {
		return nil
	}
	record := []byte{1, sps[0][1], sps[0][2], sps[0][3], 0xfc | 3, 0xe0 | byte(len(sps))}
	for _, set := range sps {
		record = binary.BigEndian.AppendUint16(record, uint16(len(set)))
		record = append(record, set...)
	}
	record = append(record, byte(len(pps)))
	for _, set := range pps {
		record = binary.BigEndian.AppendUint16(record, uint16(len(set)))
		record = append(record, set...)
	}
	return record
}
