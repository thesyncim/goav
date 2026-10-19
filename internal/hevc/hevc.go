// Package hevc holds the H.265/HEVC helpers the containers share: the hvcC
// decoder configuration record, read, and built from the parameter sets an
// Annex B stream carries. Like H.264, HEVC packets travel inside goav as
// Annex B; the NAL framing conversions are package h264's, which do not
// depend on the codec.
package hevc

import (
	"encoding/binary"
	"errors"

	"github.com/thesyncim/goav/internal/bitio"
	"github.com/thesyncim/goav/internal/h264"
)

// ErrInvalid reports a malformed hvcC record or parameter set.
var ErrInvalid = errors.New("hevc: invalid bitstream")

// The parameter-set NAL unit types.
const (
	NALVPS = 32
	NALSPS = 33
	NALPPS = 34
)

// NALType is the type of a NAL unit, from its two-byte header.
func NALType(nal []byte) int {
	if len(nal) < 2 {
		return -1
	}
	return int(nal[0]>>1) & 0x3f
}

// Config is what the conversions need from an hvcC record.
type Config struct {
	// LengthSize is the byte width (1, 2, or 4) of each NAL length prefix.
	LengthSize int
	// ParameterSets are the record's NAL units (VPS, SPS, PPS, SEI) in
	// record order; they alias the record.
	ParameterSets [][]byte
}

// recordHeader is the size of the fixed part of an hvcC record.
const recordHeader = 23

// ParseConfig parses an hvcC record.
func ParseConfig(record []byte) (Config, error) {
	if len(record) < recordHeader || record[0] != 1 {
		return Config{}, ErrInvalid
	}
	config := Config{LengthSize: int(record[21]&0x03) + 1}
	if config.LengthSize == 3 {
		return Config{}, ErrInvalid
	}
	arrays := int(record[22])
	pos := recordHeader
	for range arrays {
		if pos+3 > len(record) {
			return Config{}, ErrInvalid
		}
		count := int(binary.BigEndian.Uint16(record[pos+1:]))
		pos += 3
		for range count {
			if pos+2 > len(record) {
				return Config{}, ErrInvalid
			}
			size := int(binary.BigEndian.Uint16(record[pos:]))
			pos += 2
			if size == 0 || pos+size > len(record) {
				return Config{}, ErrInvalid
			}
			config.ParameterSets = append(config.ParameterSets, record[pos:pos+size])
			pos += size
		}
	}
	return config, nil
}

// ConfigFromAnnexB builds an hvcC record, with 4-byte NAL lengths, from the
// VPS, SPS, and PPS the Annex B data carries; the record's profile, tier,
// level, chroma format, and bit depths are the first SPS's.
func ConfigFromAnnexB(units ...[]byte) ([]byte, error) {
	var sets [3][][]byte
	for _, unit := range units {
		if err := h264.ForEachNAL(unit, func(nal []byte) error {
			if kind := NALType(nal); kind >= NALVPS && kind <= NALPPS {
				if len(sets[kind-NALVPS]) == 0xffff || len(nal) > 0xffff {
					return ErrInvalid
				}
				sets[kind-NALVPS] = append(sets[kind-NALVPS], nal)
			}
			return nil
		}); err != nil {
			return nil, ErrInvalid
		}
	}
	for i := range sets {
		if len(sets[i]) == 0 {
			return nil, ErrInvalid
		}
	}
	sps, err := parseSPS(sets[NALSPS-NALVPS][0])
	if err != nil {
		return nil, err
	}
	record := make([]byte, 0, 128)
	record = append(record, 1)
	record = append(record, sps.ptl[:]...)
	record = append(record,
		0xf0, 0x00, // min_spatial_segmentation_idc 0
		0xfc,                    // parallelismType 0
		0xfc|sps.chromaFormat,   // chromaFormat
		0xf8|sps.bitDepthLuma,   // bitDepthLumaMinus8
		0xf8|sps.bitDepthChroma, // bitDepthChromaMinus8
		0, 0,                    // avgFrameRate unspecified
		// constantFrameRate 0, numTemporalLayers, temporalIdNested,
		// lengthSizeMinusOne 3.
		sps.temporalLayers<<3|sps.temporalIDNested<<2|3,
		3,
	)
	for i, nals := range sets {
		record = append(record, 0x80|byte(NALVPS+i)) // array_completeness
		record = binary.BigEndian.AppendUint16(record, uint16(len(nals)))
		for _, nal := range nals {
			record = binary.BigEndian.AppendUint16(record, uint16(len(nal)))
			record = append(record, nal...)
		}
	}
	return record, nil
}

// spsFields is what an hvcC record copies from a sequence parameter set.
type spsFields struct {
	// ptl is general_profile_space through general_level_idc: twelve
	// bytes the record carries verbatim.
	ptl              [12]byte
	temporalLayers   byte
	temporalIDNested byte
	chromaFormat     byte
	bitDepthLuma     byte
	bitDepthChroma   byte
}

// parseSPS reads the fields an hvcC record needs from an SPS NAL unit
// (ITU-T H.265 7.3.2.2).
func parseSPS(nal []byte) (spsFields, error) {
	if NALType(nal) != NALSPS {
		return spsFields{}, ErrInvalid
	}
	rbsp := unescape(nal[2:])
	r := bitio.Reader{Data: rbsp, Err: ErrInvalid}
	var err error
	bits := func(n int) uint64 {
		v, e := r.Bits(n)
		err = errors.Join(err, e)
		return v
	}
	ue := func() uint64 {
		v, e := r.UE()
		err = errors.Join(err, e)
		return v
	}
	var sps spsFields
	bits(4) // sps_video_parameter_set_id
	maxSubLayersMinus1 := int(bits(3))
	sps.temporalLayers = byte(maxSubLayersMinus1 + 1)
	sps.temporalIDNested = byte(bits(1))
	for i := range sps.ptl {
		sps.ptl[i] = byte(bits(8))
	}
	var profilePresent, levelPresent [8]bool
	for i := range maxSubLayersMinus1 {
		profilePresent[i] = bits(1) == 1
		levelPresent[i] = bits(1) == 1
	}
	if maxSubLayersMinus1 > 0 {
		bits(2 * (8 - maxSubLayersMinus1)) // reserved_zero_2bits
	}
	for i := range maxSubLayersMinus1 {
		if profilePresent[i] {
			bits(32)
			bits(56)
		}
		if levelPresent[i] {
			bits(8)
		}
	}
	ue() // sps_seq_parameter_set_id
	chroma := ue()
	if chroma == 3 {
		bits(1) // separate_colour_plane_flag
	}
	width, height := ue(), ue()
	if bits(1) == 1 { // conformance_window_flag: the window is not the size
		for range 4 {
			ue()
		}
	}
	luma, chromaDepth := ue(), ue()
	if err != nil || chroma > 3 || luma > 8 || chromaDepth > 8 || width == 0 || height == 0 || width > 1<<16 || height > 1<<16 {
		return spsFields{}, ErrInvalid
	}
	sps.chromaFormat = byte(chroma)
	sps.bitDepthLuma, sps.bitDepthChroma = byte(luma), byte(chromaDepth)
	return sps, nil
}

// unescape removes the emulation-prevention bytes from a NAL payload.
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
