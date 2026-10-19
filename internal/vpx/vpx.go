// Package vpx reads what a VP8 or VP9 keyframe header says about its stream,
// and builds the VP codec configuration record (vpcC) the containers carry.
package vpx

import (
	"encoding/binary"
	"errors"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/internal/bitio"
)

// Keyframe is what a VP8 or VP9 keyframe header says about its stream.
type Keyframe struct {
	// Chroma is vpcC's chroma subsampling code: 0 4:2:0, 2 4:2:2, 3 4:4:4.
	Profile, BitDepth, Chroma uint8
	Width, Height             int
}

var errShort = errors.New("vpx: short keyframe header")

// ParseKeyframe reads a keyframe's profile, bit depth, chroma subsampling,
// and size; ok is false for data that is not a keyframe of codec.
func ParseKeyframe(codec av.CodecID, data []byte) (Keyframe, bool) {
	if codec == av.CodecVP8 {
		// Frame tag, then the 0x9d012a start code and 14-bit sizes.
		if len(data) < 10 || data[0]&1 != 0 || data[3] != 0x9d || data[4] != 0x01 || data[5] != 0x2a {
			return Keyframe{}, false
		}
		return Keyframe{
			Profile:  data[0] >> 1 & 7,
			BitDepth: 8,
			Width:    int(binary.LittleEndian.Uint16(data[6:]) & 0x3fff),
			Height:   int(binary.LittleEndian.Uint16(data[8:]) & 0x3fff),
		}, true
	}
	// A short header reads as zeros and fails a check; err reports it.
	r := bitio.Reader{Data: data, Err: errShort}
	var err error
	bits := func(n int) uint64 {
		v, e := r.Bits(n)
		if err == nil {
			err = e
		}
		return v
	}
	var info Keyframe
	if bits(2) != 2 { // frame_marker
		return Keyframe{}, false
	}
	low, high := bits(1), bits(1)
	info.Profile = uint8(high<<1 | low)
	if info.Profile == 3 {
		bits(1)
	}
	if bits(1) == 1 || bits(1) != 0 { // show_existing_frame, frame_type
		return Keyframe{}, false
	}
	bits(2) // show_frame, error_resilient_mode
	if bits(24) != 0x498342 {
		return Keyframe{}, false
	}
	info.BitDepth = 8
	if info.Profile >= 2 {
		info.BitDepth = 10
		if bits(1) == 1 {
			info.BitDepth = 12
		}
	}
	subX, subY := uint64(1), uint64(1)
	if bits(3) != 7 { // not sRGB
		bits(1) // color_range
		if info.Profile == 1 || info.Profile == 3 {
			subX, subY = bits(1), bits(1)
			bits(1)
		}
	} else {
		subX, subY = 0, 0
		if info.Profile == 1 || info.Profile == 3 {
			bits(1)
		}
	}
	switch {
	case subX == 1 && subY == 1:
		info.Chroma = 0
	case subX == 1:
		info.Chroma = 2
	default:
		info.Chroma = 3
	}
	info.Width = int(bits(16)) + 1
	info.Height = int(bits(16)) + 1
	return info, err == nil
}

// Record is the payload of a vpcC box (version 1, its FullBox version and
// flags included), which an E-FLV VP9 sequence start carries too: k's
// profile, bit depth, and chroma subsampling, the level a width x height
// picture needs, and unspecified colour. A zero Keyframe records 8-bit
// profile 0.
func Record(k Keyframe, width, height int) []byte {
	return []byte{
		1, 0, 0, 0, // version 1, flags 0
		k.Profile,
		level(width, height),
		max(k.BitDepth, 8)<<4 | k.Chroma<<1,
		2, 2, 2, // colour primaries, transfer, matrix: unspecified
		0, 0, // codecInitializationDataSize
	}
}

// level is the lowest VP9 level whose picture size holds width x height.
func level(width, height int) uint8 {
	size := width * height
	for _, level := range []struct {
		max   int
		level uint8
	}{{36864, 10}, {73728, 11}, {122880, 20}, {245760, 21}, {552960, 30}, {983040, 31}, {2228224, 40}, {8912896, 50}, {35651584, 60}} {
		if size <= level.max {
			return level.level
		}
	}
	return 62
}
