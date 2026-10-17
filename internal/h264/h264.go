// Package h264 holds the H.264 bitstream helpers the containers share: avcC
// decoder-configuration parsing and AVCC (length-prefixed) to Annex B
// (start-code) conversion. Inside goav, H.264 packets travel as Annex B; the
// demuxers of length-prefixed containers convert on read and the muxers of
// those containers convert back on write.
package h264

import (
	"errors"
)

// ErrInvalid reports a malformed avcC record or AVCC sample.
var ErrInvalid = errors.New("h264: invalid bitstream")

// StartCode is the 4-byte Annex B start code.
var StartCode = [4]byte{0, 0, 0, 1}

// NAL unit types the helpers inspect.
const (
	NALIDR = 5
	NALSPS = 7
	NALPPS = 8
)

// Config is the subset of an AVCDecoderConfigurationRecord (avcC) the
// conversions need.
type Config struct {
	// LengthSize is the byte width (1, 2, or 4) of each NAL length prefix in
	// AVCC samples.
	LengthSize int
	SPS        [][]byte
	PPS        [][]byte
}

// ParseConfig parses an avcC record. The SPS/PPS slices alias record.
func ParseConfig(record []byte) (Config, error) {
	if len(record) < 7 || record[0] != 1 {
		return Config{}, ErrInvalid
	}
	config := Config{LengthSize: int(record[4]&0x03) + 1}
	if config.LengthSize == 3 {
		return Config{}, ErrInvalid
	}
	pos := 5
	spsCount := int(record[pos] & 0x1f)
	pos++
	for range spsCount {
		nal, next, err := readSized(record, pos)
		if err != nil {
			return Config{}, err
		}
		config.SPS = append(config.SPS, nal)
		pos = next
	}
	if pos >= len(record) {
		return Config{}, ErrInvalid
	}
	ppsCount := int(record[pos])
	pos++
	for range ppsCount {
		nal, next, err := readSized(record, pos)
		if err != nil {
			return Config{}, err
		}
		config.PPS = append(config.PPS, nal)
		pos = next
	}
	return config, nil
}

func readSized(record []byte, pos int) ([]byte, int, error) {
	if pos+2 > len(record) {
		return nil, 0, ErrInvalid
	}
	size := int(record[pos])<<8 | int(record[pos+1])
	pos += 2
	if size == 0 || pos+size > len(record) {
		return nil, 0, ErrInvalid
	}
	return record[pos : pos+size], pos + size, nil
}

// AppendParameterSets appends the config's SPS and PPS NAL units to dst as
// Annex B, each behind a start code.
func (c Config) AppendParameterSets(dst []byte) []byte {
	for _, nal := range c.SPS {
		dst = append(dst, StartCode[:]...)
		dst = append(dst, nal...)
	}
	for _, nal := range c.PPS {
		dst = append(dst, StartCode[:]...)
		dst = append(dst, nal...)
	}
	return dst
}

// AppendAnnexB appends the AVCC sample src, converted to Annex B, to dst.
func AppendAnnexB(dst []byte, src []byte, lengthSize int) ([]byte, error) {
	for pos := 0; pos < len(src); {
		if pos+lengthSize > len(src) {
			return dst, ErrInvalid
		}
		size := 0
		for i := range lengthSize {
			size = size<<8 | int(src[pos+i])
		}
		pos += lengthSize
		if size == 0 || pos+size > len(src) {
			return dst, ErrInvalid
		}
		dst = append(dst, StartCode[:]...)
		dst = append(dst, src[pos:pos+size]...)
		pos += size
	}
	return dst, nil
}

// HasStartCode reports whether data begins with an Annex B start code.
func HasStartCode(data []byte) bool {
	return len(data) >= 3 && data[0] == 0 && data[1] == 0 &&
		(data[2] == 1 || (len(data) >= 4 && data[2] == 0 && data[3] == 1))
}

// ContainsNAL reports whether the Annex B access unit data carries a NAL unit
// of nalType.
func ContainsNAL(data []byte, nalType byte) bool {
	for i := 0; i+3 < len(data); i++ {
		if data[i] != 0 || data[i+1] != 0 {
			continue
		}
		switch {
		case data[i+2] == 1:
			if data[i+3]&0x1f == nalType {
				return true
			}
		case data[i+2] == 0 && i+4 < len(data) && data[i+3] == 1:
			if data[i+4]&0x1f == nalType {
				return true
			}
		}
	}
	return false
}
