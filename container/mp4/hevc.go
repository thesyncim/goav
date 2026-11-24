package mp4

import (
	"encoding/binary"

	"github.com/thesyncim/goav/internal/h264"
	"github.com/thesyncim/goav/internal/hevc"
)

// hvc1Config is the hvcC record an hvc1 sample entry carries for a stream
// whose extradata is a record or Annex B parameter sets, or nil.
func hvc1Config(extra []byte) []byte {
	if h264.HasStartCode(extra) {
		record, _ := hevc.ConfigFromAnnexB(extra)
		return record
	}
	record, _ := hevc.CompleteConfig(extra)
	return record
}

// appendHVC1 writes an Annex B access unit as an hvc1 sample: 4-byte
// length-prefixed NAL units, less the parameter sets the sample entry holds
// (ISO/IEC 14496-15, 8.4.1).
func appendHVC1(dst, src []byte) ([]byte, error) {
	start, width := findAnnexBStartCode(src, 0)
	if start < 0 {
		return dst, h264.ErrInvalid
	}
	for i := range start {
		if src[i] != 0 {
			return dst, h264.ErrInvalid
		}
	}
	for start >= 0 {
		nalStart := start + width
		next, nextWidth := findAnnexBStartCode(src, nalStart)
		nalEnd := len(src)
		if next >= 0 {
			nalEnd = next
		}
		if nalStart >= nalEnd {
			return dst, h264.ErrInvalid
		}
		nal := src[nalStart:nalEnd]
		if !hevc.ParameterSet(hevc.NALType(nal)) {
			dst = binary.BigEndian.AppendUint32(dst, uint32(len(nal)))
			dst = append(dst, nal...)
		}
		start, width = next, nextWidth
	}
	return dst, nil
}

func findAnnexBStartCode(data []byte, offset int) (int, int) {
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
