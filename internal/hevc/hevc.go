// Package hevc holds the HEVC (H.265) bitstream helpers the containers
// share: parameter-set NAL units, the sequence parameter set fields a
// decoder configuration repeats, and the hvcC decoder configuration record
// (ISO/IEC 14496-15, 8.3.3). Inside goav, HEVC packets travel as Annex B,
// as H.264 ones do (see internal/h264).
package hevc

import (
	"cmp"
	"encoding/binary"
	"errors"
	"slices"

	"github.com/thesyncim/goav/internal/bitio"
	"github.com/thesyncim/goav/internal/h264"
)

// ErrInvalid reports a malformed parameter set or hvcC record.
var ErrInvalid = errors.New("hevc: invalid bitstream")

// The parameter-set NAL unit types.
const (
	NALVPS = 32
	NALSPS = 33
	NALPPS = 34
)

// NALType is the type of a NAL unit, read from its two-byte header.
func NALType(nal []byte) byte {
	if len(nal) == 0 {
		return 0
	}
	return nal[0] >> 1 & 0x3f
}

// ParameterSet reports a VPS, SPS, or PPS NAL unit type.
func ParameterSet(nalType byte) bool {
	return nalType >= NALVPS && nalType <= NALPPS
}

// Config is what the conversions need from an hvcC record.
type Config struct {
	// LengthSize is the byte width (1, 2, or 4) of each NAL length prefix.
	LengthSize int
	// ParameterSets are the record's VPS, SPS, and PPS units, aliasing it.
	ParameterSets [][]byte
}

// ParseConfig parses an hvcC record.
func ParseConfig(record []byte) (Config, error) {
	if len(record) < 23 || record[0] != 1 || record[21]&3 == 2 {
		return Config{}, ErrInvalid
	}
	config := Config{LengthSize: int(record[21]&3) + 1}
	err := forEachArray(record, func(int, []byte) {}, func(nal []byte) {
		config.ParameterSets = append(config.ParameterSets, nal)
	})
	return config, err
}

// forEachArray walks an hvcC record's NAL unit arrays: array is called with
// each array header's offset, nal with each unit.
func forEachArray(record []byte, array func(at int, header []byte), nal func([]byte)) error {
	p := 23
	for range int(record[22]) {
		if p+3 > len(record) {
			return ErrInvalid
		}
		array(p, record[p:p+3])
		units := int(binary.BigEndian.Uint16(record[p+1:]))
		p += 3
		for range units {
			if p+2 > len(record) {
				return ErrInvalid
			}
			n := int(binary.BigEndian.Uint16(record[p:]))
			if p+2+n > len(record) || n == 0 {
				return ErrInvalid
			}
			nal(record[p+2 : p+2+n])
			p += 2 + n
		}
	}
	return nil
}

// CompleteConfig returns a copy of record as an hvc1 sample entry carries
// it, whose samples hold no parameter sets: 4-byte NAL lengths, and the
// parameter-set arrays marked complete.
func CompleteConfig(record []byte) ([]byte, error) {
	if _, err := ParseConfig(record); err != nil {
		return nil, err
	}
	out := slices.Clone(record)
	out[21] = out[21]&^3 | 3
	err := forEachArray(out, func(at int, header []byte) {
		if ParameterSet(header[0] & 0x3f) {
			out[at] |= 0x80 // array_completeness
		}
	}, func([]byte) {})
	return out, err
}

// ConfigFromAnnexB builds a complete hvcC record, with 4-byte NAL lengths,
// from the VPS, SPS, and PPS that Annex B data (a keyframe, or extradata)
// carries; the profile, tier, level, chroma format, and bit depths come
// from the first SPS.
func ConfigFromAnnexB(data []byte) ([]byte, error) {
	var sets [3][][]byte // VPS, SPS, PPS
	if err := h264.ForEachNAL(data, func(nal []byte) error {
		if t := NALType(nal); ParameterSet(t) {
			if len(nal) > 0xffff || len(sets[t-NALVPS]) == 0xffff {
				return ErrInvalid
			}
			sets[t-NALVPS] = append(sets[t-NALVPS], nal)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if len(sets[0]) == 0 || len(sets[1]) == 0 || len(sets[2]) == 0 {
		return nil, ErrInvalid
	}
	sps, err := ParseSPS(sets[1][0])
	if err != nil {
		return nil, err
	}
	record := make([]byte, 23, 64)
	record[0] = 1
	copy(record[1:13], sps.ProfileTierLevel[:])
	record[13], record[14] = 0xf0, 0 // min_spatial_segmentation_idc 0: unrestricted
	record[15] = 0xfc                // parallelismType 0: unknown
	record[16] = 0xfc | sps.ChromaFormat
	record[17] = 0xf8 | (sps.LumaDepth - 8)
	record[18] = 0xf8 | (sps.ChromaDepth - 8)
	record[21] = sps.TemporalLayers<<3 | sps.TemporalIDNested<<2 | 3 // avgFrameRate 0, 4-byte lengths
	record[22] = 3
	for i, nals := range sets {
		record = append(record, 0x80|byte(NALVPS+i)) // complete
		record = binary.BigEndian.AppendUint16(record, uint16(len(nals)))
		for _, nal := range nals {
			record = binary.BigEndian.AppendUint16(record, uint16(len(nal)))
			record = append(record, nal...)
		}
	}
	return record, nil
}

// SPS is what a decoder configuration repeats from a sequence parameter
// set, and the picture size it declares.
type SPS struct {
	// ProfileTierLevel is the general profile, tier, and level: the 12
	// bytes an hvcC record copies.
	ProfileTierLevel [12]byte
	TemporalLayers   byte
	TemporalIDNested byte
	// ChromaFormat is chroma_format_idc: 0 monochrome, 1 4:2:0, 2 4:2:2,
	// 3 4:4:4.
	ChromaFormat           byte
	LumaDepth, ChromaDepth byte
	// Width and Height are the picture size after the conformance window.
	Width, Height int
}

// ParseSPS reads an SPS NAL unit (H.265 7.3.2.2) up to its bit depths.
func ParseSPS(nal []byte) (SPS, error) {
	var sps SPS
	if NALType(nal) != NALSPS {
		return sps, ErrInvalid
	}
	rbsp := unescape(nal[min(2, len(nal)):])
	if len(rbsp) < 13 {
		return sps, ErrInvalid
	}
	subLayers := int(rbsp[0] >> 1 & 7) // sps_max_sub_layers_minus1
	if subLayers == 7 {
		return sps, ErrInvalid
	}
	sps.TemporalLayers = byte(subLayers) + 1
	sps.TemporalIDNested = rbsp[0] & 1
	copy(sps.ProfileTierLevel[:], rbsp[1:13])
	r := bitio.Reader{Data: rbsp[13:], Err: ErrInvalid}
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
	width, height := ue(), ue()
	var crop [4]uint64 // left, right, top, bottom
	if bits(1) == 1 {  // conformance_window_flag
		for i := range crop {
			crop[i] = ue()
		}
	}
	luma, chromaDepth := ue(), ue()
	if failed != nil || chroma > 3 || luma > 8 || chromaDepth > 8 || width > 1<<16 || height > 1<<16 {
		return sps, ErrInvalid
	}
	subWidth, subHeight := uint64(1), uint64(1) // SubWidthC, SubHeightC (Table 6-1)
	if chroma == 1 || chroma == 2 {
		subWidth = 2
	}
	if chroma == 1 {
		subHeight = 2
	}
	cropWidth, cropHeight := subWidth*(crop[0]+crop[1]), subHeight*(crop[2]+crop[3])
	if cropWidth >= width || cropHeight >= height {
		return sps, ErrInvalid
	}
	sps.ChromaFormat, sps.LumaDepth, sps.ChromaDepth = byte(chroma), byte(luma)+8, byte(chromaDepth)+8
	sps.Width, sps.Height = int(width-cropWidth), int(height-cropHeight)
	return sps, nil
}

// unescape drops the emulation-prevention bytes from a NAL unit's payload:
// 00 00 03 becomes 00 00.
func unescape(payload []byte) []byte {
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
