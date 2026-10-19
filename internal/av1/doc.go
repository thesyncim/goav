// Package av1 reads the parts of an AV1 bitstream containers need: OBU
// framing, the sequence header, and the AV1CodecConfigurationRecord (av1C)
// that Matroska CodecPrivate and the ISO BMFF av1C box both carry.
package av1

import "errors"

// ErrInvalid reports a malformed OBU or sequence header.
var ErrInvalid = errors.New("av1: invalid bitstream")

// obuTemporalDelimiter is the OBU type a container's samples leave out.
const obuTemporalDelimiter = 2

// StripTemporalDelimiters returns a temporal unit without its temporal
// delimiter OBUs, as ISO BMFF and Matroska samples store it. It returns data
// itself when there is none to strip.
func StripTemporalDelimiters(data []byte) ([]byte, error) {
	found := false
	if err := av1ForEachOBU(data, func(obu av1OBU) error {
		found = found || obu.Type == obuTemporalDelimiter
		return nil
	}); err != nil {
		return nil, err
	}
	if !found {
		return data, nil
	}
	out := make([]byte, 0, len(data))
	_ = av1ForEachOBU(data, func(obu av1OBU) error {
		if obu.Type != obuTemporalDelimiter {
			out = append(out, obu.Data...)
		}
		return nil
	})
	return out, nil
}

// AppendWithoutTemporalDelimiters appends the temporal unit data to dst
// without its temporal delimiter OBUs.
func AppendWithoutTemporalDelimiters(dst, data []byte) ([]byte, error) {
	err := av1ForEachOBU(data, func(obu av1OBU) error {
		if obu.Type != obuTemporalDelimiter {
			dst = append(dst, obu.Data...)
		}
		return nil
	})
	return dst, err
}
