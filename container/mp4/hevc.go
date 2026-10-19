package mp4

import (
	"cmp"
	"encoding/binary"
	"slices"

	"github.com/thesyncim/goav/internal/bitio"
	"github.com/thesyncim/goav/internal/h264"
)

// hvcCLengthSize is the NAL length width an hvcC record declares, or 0 when
// record is not one.
func hvcCLengthSize(record []byte) int {
	if len(record) < 23 || record[0] != 1 {
		return 0
	}
	return int(record[21]&3) + 1
}

// hvc1Config is the hvcC record an hvc1 sample entry carries: 4-byte NAL
// lengths, and its parameter-set arrays marked complete, since an hvc1
// track's samples carry none (ISO/IEC 14496-15, 8.4.1). It returns nil when
// record is not a well-formed hvcC.
func hvc1Config(record []byte) []byte {
	if hvcCLengthSize(record) == 0 {
		return nil
	}
	config := slices.Clone(record)
	config[21] = config[21]&^3 | 3
	p := 23
	for range int(config[22]) {
		if p+3 > len(config) {
			return nil
		}
		if parameterSetNAL(config[p] & 0x3f) {
			config[p] |= 0x80 // array_completeness
		}
		units := int(binary.BigEndian.Uint16(config[p+1:]))
		p += 3
		for range units {
			if p+2 > len(config) {
				return nil
			}
			p += 2 + int(binary.BigEndian.Uint16(config[p:]))
		}
	}
	if p > len(config) {
		return nil
	}
	return config
}

// appendHVC1 writes an Annex B access unit as an hvc1 sample: 4-byte
// length-prefixed NAL units, less the parameter sets the sample entry holds.
func appendHVC1(dst, src []byte) ([]byte, error) {
	err := h264.ForEachNAL(src, func(nal []byte) error {
		if parameterSetNAL(nal[0] >> 1 & 0x3f) {
			return nil
		}
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(nal)))
		dst = append(dst, nal...)
		return nil
	})
	return dst, err
}

// parameterSetNAL reports an HEVC VPS, SPS, or PPS NAL unit type.
func parameterSetNAL(nalType byte) bool {
	return nalType >= 32 && nalType <= 34
}

// hvcCFromAnnexB builds an hvc1 track's hvcC record from the VPS, SPS, and
// PPS that Annex B data (a keyframe, or extradata) carries, reading the
// profile, tier, level, chroma format, and bit depths from the SPS.
func hvcCFromAnnexB(data []byte) ([]byte, error) {
	var sets [3][][]byte // VPS, SPS, PPS
	if err := h264.ForEachNAL(data, func(nal []byte) error {
		if t := nal[0] >> 1 & 0x3f; parameterSetNAL(t) {
			if len(nal) > 0xffff || len(sets[t-32]) == 0xffff {
				return ErrInvalidData
			}
			sets[t-32] = append(sets[t-32], nal)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if len(sets[0]) == 0 || len(sets[1]) == 0 || len(sets[2]) == 0 {
		return nil, ErrInvalidData
	}
	sps, err := parseHEVCSPS(sets[1][0])
	if err != nil {
		return nil, err
	}
	record := make([]byte, 23, 64)
	record[0] = 1
	copy(record[1:13], sps.profileTierLevel[:])
	record[13], record[14] = 0xf0, 0 // min_spatial_segmentation_idc 0: unrestricted
	record[15] = 0xfc                // parallelismType 0: unknown
	record[16] = 0xfc | sps.chromaFormat
	record[17] = 0xf8 | (sps.lumaDepth - 8)
	record[18] = 0xf8 | (sps.chromaDepth - 8)
	record[21] = sps.temporalLayers<<3 | sps.temporalIDNested<<2 | 3 // avgFrameRate 0, 4-byte lengths
	record[22] = 3
	for i, nals := range sets {
		record = append(record, 0x80|byte(32+i)) // complete: the samples carry none
		record = binary.BigEndian.AppendUint16(record, uint16(len(nals)))
		for _, nal := range nals {
			record = binary.BigEndian.AppendUint16(record, uint16(len(nal)))
			record = append(record, nal...)
		}
	}
	return record, nil
}

// hevcSPS is what an hvcC record repeats from a sequence parameter set.
type hevcSPS struct {
	profileTierLevel       [12]byte // the general profile, tier, and level
	temporalLayers         byte
	temporalIDNested       byte
	chromaFormat           byte
	lumaDepth, chromaDepth byte
}

// parseHEVCSPS reads an SPS NAL unit (H.265 7.3.2.2) up to its bit depths.
func parseHEVCSPS(nal []byte) (hevcSPS, error) {
	var sps hevcSPS
	rbsp := unescapeRBSP(nal[min(2, len(nal)):])
	if len(rbsp) < 13 {
		return sps, ErrInvalidData
	}
	subLayers := int(rbsp[0] >> 1 & 7) // sps_max_sub_layers_minus1
	sps.temporalLayers = byte(subLayers) + 1
	sps.temporalIDNested = rbsp[0] & 1
	copy(sps.profileTierLevel[:], rbsp[1:13])
	r := bitio.Reader{Data: rbsp[13:], Err: ErrInvalidData}
	var failed error
	bits := func(n int) uint64 {
		v, err := r.Bits(n)
		failed = cmp.Or(failed, err)
		return v
	}
	ue := func() uint64 {
		v, err := r.UVLC()
		failed = cmp.Or(failed, err)
		return v
	}
	var profile, level [7]uint64
	if subLayers > 0 {
		for i := range subLayers {
			profile[i], level[i] = bits(1), bits(1)
		}
		bits(2 * (8 - subLayers)) // reserved_zero_2bits
	}
	for i := range subLayers {
		for n := int(88*profile[i] + 8*level[i]); n > 0; n -= 64 {
			bits(min(n, 64)) // the sub-layer's profile and level
		}
	}
	ue() // sps_seq_parameter_set_id
	chroma := ue()
	if chroma == 3 {
		bits(1) // separate_colour_plane_flag
	}
	ue()              // pic_width_in_luma_samples
	ue()              // pic_height_in_luma_samples
	if bits(1) == 1 { // conformance_window_flag
		ue()
		ue()
		ue()
		ue()
	}
	luma, chromaDepth := ue(), ue()
	if failed != nil || chroma > 3 || luma > 8 || chromaDepth > 8 {
		return sps, ErrInvalidData
	}
	sps.chromaFormat, sps.lumaDepth, sps.chromaDepth = byte(chroma), byte(luma)+8, byte(chromaDepth)+8
	return sps, nil
}

// unescapeRBSP drops the emulation-prevention bytes from a NAL unit's
// payload: 00 00 03 becomes 00 00.
func unescapeRBSP(payload []byte) []byte {
	out := make([]byte, 0, len(payload))
	zeros := 0
	for _, b := range payload {
		if zeros >= 2 && b == 3 {
			zeros = 0
			continue
		}
		if b == 0 {
			zeros++
		} else {
			zeros = 0
		}
		out = append(out, b)
	}
	return out
}
