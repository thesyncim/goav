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
	err := h264.ForEachNAL(src, func(nal []byte) error {
		if hevc.ParameterSet(hevc.NALType(nal)) {
			return nil
		}
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(nal)))
		dst = append(dst, nal...)
		return nil
	})
	return dst, err
}
