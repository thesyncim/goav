// Package av1 reads the parts of an AV1 bitstream containers need: OBU
// framing, the sequence header, and the AV1CodecConfigurationRecord (av1C)
// that Matroska CodecPrivate and the ISO BMFF av1C box both carry.
package av1

import "errors"

// ErrInvalid reports a malformed OBU or sequence header.
var ErrInvalid = errors.New("av1: invalid bitstream")

// obuTemporalDelimiter is the OBU type a container's samples leave out.
const obuTemporalDelimiter = 2

// AppendWithoutTemporalDelimiters appends the OBUs of data to dst, skipping
// temporal delimiter OBUs. It returns data itself without copying into dst
// when data contains no temporal delimiter OBUs.
func AppendWithoutTemporalDelimiters(dst, data []byte) ([]byte, error) {
	found := false
	for offset := 0; offset < len(data); {
		obu, next, err := readAV1OBU(data, offset)
		if err != nil {
			return nil, err
		}
		if obu.Type == obuTemporalDelimiter {
			found = true
		}
		offset = next
	}
	if !found {
		return data, nil
	}
	for offset := 0; offset < len(data); {
		obu, next, err := readAV1OBU(data, offset)
		if err != nil {
			return nil, err
		}
		if obu.Type != obuTemporalDelimiter {
			dst = append(dst, obu.Data...)
		}
		offset = next
	}
	return dst, nil
}

// StripTemporalDelimiters returns a temporal unit without its temporal
// delimiter OBUs, as ISO BMFF and Matroska samples store it. It returns data
// itself when there is none to strip.
func StripTemporalDelimiters(data []byte) ([]byte, error) {
	found := false
	for offset := 0; offset < len(data); {
		obu, next, err := readAV1OBU(data, offset)
		if err != nil {
			return nil, err
		}
		if obu.Type == obuTemporalDelimiter {
			found = true
		}
		offset = next
	}
	if !found {
		return data, nil
	}
	return AppendWithoutTemporalDelimiters(make([]byte, 0, len(data)), data)
}
