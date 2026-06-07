package matroska

import (
	"encoding/binary"
	"io"
)

const (
	h265NALUVPS = 32
	h265NALUSPS = 33
	h265NALUPPS = 34
)

type h265DecoderConfigurationRecord struct {
	GeneralProfileSpace              int
	GeneralTierFlag                  bool
	GeneralProfileIDC                int
	GeneralProfileCompatibilityFlags uint32
	GeneralConstraintIndicatorFlags  uint64
	GeneralLevelIDC                  int
	MinSpatialSegmentationIDC        uint16
	ParallelismType                  int
	ChromaFormat                     int
	BitDepthLumaMinus8               int
	BitDepthChromaMinus8             int
	AvgFrameRate                     uint16
	ConstantFrameRate                int
	NumTemporalLayers                int
	TemporalIDNested                 bool
	NALULengthSize                   int
	VPSCount                         int
	SPSCount                         int
	PPSCount                         int
}

type h265SPSHeader struct {
	GeneralProfileSpace              int
	GeneralTierFlag                  bool
	GeneralProfileIDC                int
	GeneralProfileCompatibilityFlags uint32
	GeneralConstraintIndicatorFlags  uint64
	GeneralLevelIDC                  int
	NumTemporalLayers                int
	TemporalIDNested                 bool
}

func h265TrackNALULengthSize(track Track) (int, bool, error) {
	if track.Codec != CodecH265 || len(track.CodecPrivate) == 0 {
		return 0, false, nil
	}
	config, err := parseHEVCDecoderConfigurationRecord(track.CodecPrivate)
	if err != nil {
		return 0, false, err
	}
	return config.NALULengthSize, true, nil
}

func trackNALULengthSize(track Track) (int, bool, error) {
	switch track.Codec {
	case CodecH264:
		return h264TrackNALULengthSize(track)
	case CodecH265:
		return h265TrackNALULengthSize(track)
	default:
		return 0, false, nil
	}
}

func h265MuxedPayloadSize(track Track, data []byte) (int, bool, error) {
	if track.Codec != CodecH265 || len(track.CodecPrivate) == 0 {
		return len(data), false, nil
	}
	config, err := parseHEVCDecoderConfigurationRecord(track.CodecPrivate)
	if err != nil {
		return 0, false, err
	}
	lengthSize := config.NALULengthSize
	if h265ValidateLengthPrefixedSample(data, lengthSize) == nil {
		return len(data), false, nil
	}
	size, err := h264AnnexBToAVCSize(data, lengthSize)
	if err != nil {
		return 0, false, err
	}
	return size, true, nil
}

func h265WriteMuxedPayload(w io.Writer, track Track, data []byte, scratch *[16]byte) error {
	if track.Codec != CodecH265 || len(track.CodecPrivate) == 0 {
		_, err := w.Write(data)
		return err
	}
	config, err := parseHEVCDecoderConfigurationRecord(track.CodecPrivate)
	if err != nil {
		return err
	}
	lengthSize := config.NALULengthSize
	if h265ValidateLengthPrefixedSample(data, lengthSize) == nil {
		_, err = w.Write(data)
		return err
	}
	return h264WriteAnnexBAsAVC(w, data, lengthSize, scratch)
}

func parseHEVCDecoderConfigurationRecord(private []byte) (h265DecoderConfigurationRecord, error) {
	if len(private) < 23 || private[0] != 1 {
		return h265DecoderConfigurationRecord{}, ErrInvalidData
	}
	if private[13]&0xf0 != 0xf0 || private[15]&0xfc != 0xfc ||
		private[16]&0xfc != 0xfc || private[17]&0xf8 != 0xf8 ||
		private[18]&0xf8 != 0xf8 {
		return h265DecoderConfigurationRecord{}, ErrInvalidData
	}
	lengthSizeMinusOne := private[21] & 0x03
	if lengthSizeMinusOne == 2 {
		return h265DecoderConfigurationRecord{}, ErrInvalidData
	}
	config := h265DecoderConfigurationRecord{
		GeneralProfileSpace:              int(private[1] >> 6),
		GeneralTierFlag:                  private[1]&0x20 != 0,
		GeneralProfileIDC:                int(private[1] & 0x1f),
		GeneralProfileCompatibilityFlags: binary.BigEndian.Uint32(private[2:6]),
		GeneralConstraintIndicatorFlags:  h265ReadUint48(private[6:12]),
		GeneralLevelIDC:                  int(private[12]),
		MinSpatialSegmentationIDC:        binary.BigEndian.Uint16(private[13:15]) & 0x0fff,
		ParallelismType:                  int(private[15] & 0x03),
		ChromaFormat:                     int(private[16] & 0x03),
		BitDepthLumaMinus8:               int(private[17] & 0x07),
		BitDepthChromaMinus8:             int(private[18] & 0x07),
		AvgFrameRate:                     binary.BigEndian.Uint16(private[19:21]),
		ConstantFrameRate:                int(private[21] >> 6),
		NumTemporalLayers:                int((private[21] >> 3) & 0x07),
		TemporalIDNested:                 private[21]&0x04 != 0,
		NALULengthSize:                   int(lengthSizeMinusOne) + 1,
	}
	offset := 23
	arrayCount := int(private[22])
	if arrayCount == 0 {
		return h265DecoderConfigurationRecord{}, ErrInvalidData
	}
	for i := 0; i < arrayCount; i++ {
		if offset+3 > len(private) {
			return h265DecoderConfigurationRecord{}, ErrInvalidData
		}
		arrayHeader := private[offset]
		if arrayHeader&0x40 != 0 {
			return h265DecoderConfigurationRecord{}, ErrInvalidData
		}
		naluType := int(arrayHeader & 0x3f)
		count := int(binary.BigEndian.Uint16(private[offset+1 : offset+3]))
		if count == 0 {
			return h265DecoderConfigurationRecord{}, ErrInvalidData
		}
		offset += 3
		for j := 0; j < count; j++ {
			nal, next, err := readHEVCConfigNALU(private, offset)
			if err != nil {
				return h265DecoderConfigurationRecord{}, err
			}
			if h265NALUType(nal) != naluType {
				return h265DecoderConfigurationRecord{}, ErrInvalidData
			}
			switch naluType {
			case h265NALUVPS:
				config.VPSCount++
			case h265NALUSPS:
				config.SPSCount++
			case h265NALUPPS:
				config.PPSCount++
			}
			offset = next
		}
	}
	if offset != len(private) || config.VPSCount == 0 || config.SPSCount == 0 || config.PPSCount == 0 {
		return h265DecoderConfigurationRecord{}, ErrInvalidData
	}
	return config, nil
}

func readHEVCConfigNALU(private []byte, offset int) ([]byte, int, error) {
	if offset < 0 || offset+2 > len(private) {
		return nil, 0, ErrInvalidData
	}
	size := int(binary.BigEndian.Uint16(private[offset : offset+2]))
	offset += 2
	if size == 0 || offset+size > len(private) {
		return nil, 0, ErrInvalidData
	}
	nal := private[offset : offset+size]
	if err := h265ValidateNALU(nal); err != nil {
		return nil, 0, err
	}
	return nal, offset + size, nil
}

func h265HEVCDecoderConfigurationRecordFromAnnexBFrames(frames [][]byte) ([]byte, error) {
	var vps [][]byte
	var sps [][]byte
	var pps [][]byte
	for i := range frames {
		if err := h265CollectParameterSets(frames[i], &vps, &sps, &pps); err != nil {
			return nil, err
		}
	}
	if len(vps) == 0 || len(sps) == 0 || len(pps) == 0 ||
		len(vps) > 0xffff || len(sps) > 0xffff || len(pps) > 0xffff {
		return nil, ErrInvalidData
	}
	header, err := h265ParseSPSHeader(sps[0])
	if err != nil {
		return nil, err
	}
	total := 23
	for _, sets := range [][][]byte{vps, sps, pps} {
		total += 3
		for i := range sets {
			if len(sets[i]) == 0 || len(sets[i]) > 0xffff {
				return nil, ErrInvalidData
			}
			total += 2 + len(sets[i])
		}
	}
	private := make([]byte, 0, total)
	private = append(private, 1)
	private = append(private, byte(header.GeneralProfileSpace<<6)|h265BoolBit(header.GeneralTierFlag, 5)|byte(header.GeneralProfileIDC))
	private = binary.BigEndian.AppendUint32(private, header.GeneralProfileCompatibilityFlags)
	private = h265AppendUint48(private, header.GeneralConstraintIndicatorFlags)
	private = append(private, byte(header.GeneralLevelIDC), 0xf0, 0x00, 0xfc, 0xfd, 0xf8, 0xf8, 0x00, 0x00)
	numTemporalLayers := header.NumTemporalLayers
	if numTemporalLayers == 0 {
		numTemporalLayers = 1
	}
	private = append(private, byte(numTemporalLayers<<3)|h265BoolBit(header.TemporalIDNested, 2)|0x03, 3)
	private = h265AppendNALUArray(private, h265NALUVPS, vps)
	private = h265AppendNALUArray(private, h265NALUSPS, sps)
	private = h265AppendNALUArray(private, h265NALUPPS, pps)
	return private, nil
}

func h265AppendNALUArray(dst []byte, naluType int, nalus [][]byte) []byte {
	dst = append(dst, 0x80|byte(naluType&0x3f))
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(nalus)))
	for i := range nalus {
		dst = binary.BigEndian.AppendUint16(dst, uint16(len(nalus[i])))
		dst = append(dst, nalus[i]...)
	}
	return dst
}

func h265CollectParameterSets(data []byte, vps *[][]byte, sps *[][]byte, pps *[][]byte) error {
	return h264IterAnnexBNALUs(data, func(nalu []byte) error {
		if err := h265ValidateNALU(nalu); err != nil {
			return err
		}
		switch h265NALUType(nalu) {
		case h265NALUVPS:
			if len(*vps) == 0xffff {
				return ErrInvalidData
			}
			*vps = append(*vps, nalu)
		case h265NALUSPS:
			if len(*sps) == 0xffff {
				return ErrInvalidData
			}
			*sps = append(*sps, nalu)
		case h265NALUPPS:
			if len(*pps) == 0xffff {
				return ErrInvalidData
			}
			*pps = append(*pps, nalu)
		}
		return nil
	})
}

func h265ParseSPSHeader(nalu []byte) (h265SPSHeader, error) {
	if h265NALUType(nalu) != h265NALUSPS {
		return h265SPSHeader{}, ErrInvalidData
	}
	rbsp, err := h265NALURBSP(nalu)
	if err != nil {
		return h265SPSHeader{}, err
	}
	br := av1BitReader{data: rbsp}
	if _, err := br.readBits(4); err != nil {
		return h265SPSHeader{}, err
	}
	maxSubLayersMinusOne, err := br.readBits(3)
	if err != nil {
		return h265SPSHeader{}, err
	}
	if maxSubLayersMinusOne > 6 {
		return h265SPSHeader{}, ErrInvalidData
	}
	temporalIDNested, err := br.readBool()
	if err != nil {
		return h265SPSHeader{}, err
	}
	profileSpace, err := br.readBits(2)
	if err != nil {
		return h265SPSHeader{}, err
	}
	tier, err := br.readBool()
	if err != nil {
		return h265SPSHeader{}, err
	}
	profileIDC, err := br.readBits(5)
	if err != nil {
		return h265SPSHeader{}, err
	}
	compatibility, err := br.readBits(32)
	if err != nil {
		return h265SPSHeader{}, err
	}
	constraints, err := br.readBits(48)
	if err != nil {
		return h265SPSHeader{}, err
	}
	level, err := br.readBits(8)
	if err != nil {
		return h265SPSHeader{}, err
	}
	return h265SPSHeader{
		GeneralProfileSpace:              int(profileSpace),
		GeneralTierFlag:                  tier,
		GeneralProfileIDC:                int(profileIDC),
		GeneralProfileCompatibilityFlags: uint32(compatibility),
		GeneralConstraintIndicatorFlags:  constraints,
		GeneralLevelIDC:                  int(level),
		NumTemporalLayers:                int(maxSubLayersMinusOne) + 1,
		TemporalIDNested:                 temporalIDNested,
	}, nil
}

func h265NALURBSP(nalu []byte) ([]byte, error) {
	if err := h265ValidateNALU(nalu); err != nil {
		return nil, err
	}
	payload := nalu[2:]
	for i := 2; i < len(payload); i++ {
		if payload[i] == 0x03 && payload[i-1] == 0 && payload[i-2] == 0 {
			out := make([]byte, 0, len(payload))
			zeros := 0
			for _, b := range payload {
				if zeros >= 2 && b == 0x03 {
					continue
				}
				out = append(out, b)
				if b == 0 {
					zeros++
				} else {
					zeros = 0
				}
			}
			return out, nil
		}
	}
	return payload, nil
}

func h265ValidateLengthPrefixedSample(data []byte, lengthSize int) error {
	offset := 0
	for offset < len(data) {
		size, next, err := h264ReadAVCNALUSize(data, offset, lengthSize)
		if err != nil {
			return err
		}
		if size == 0 || next+size > len(data) {
			return ErrInvalidData
		}
		if err := h265ValidateNALU(data[next : next+size]); err != nil {
			return err
		}
		offset = next + size
	}
	if offset == 0 {
		return ErrInvalidData
	}
	return nil
}

func h265ValidateNALU(nalu []byte) error {
	if len(nalu) < 2 || nalu[0]&0x80 != 0 || nalu[1]&0x07 == 0 {
		return ErrInvalidData
	}
	return nil
}

func h265NALUType(nalu []byte) int {
	if len(nalu) == 0 {
		return -1
	}
	return int((nalu[0] >> 1) & 0x3f)
}

func h265ReadUint48(data []byte) uint64 {
	return uint64(data[0])<<40 | uint64(data[1])<<32 | uint64(data[2])<<24 |
		uint64(data[3])<<16 | uint64(data[4])<<8 | uint64(data[5])
}

func h265AppendUint48(dst []byte, value uint64) []byte {
	return append(dst, byte(value>>40), byte(value>>32), byte(value>>24), byte(value>>16), byte(value>>8), byte(value))
}

func h265BoolBit(value bool, bit uint) byte {
	if value {
		return 1 << bit
	}
	return 0
}
