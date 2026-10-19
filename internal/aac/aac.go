// Package aac reads the AAC framing the containers meet: an ADTS header,
// whose fields rebuild the AudioSpecificConfig a raw-AAC container
// declares.
package aac

// ADTS reads an ADTS frame header at the start of data: its length, and
// the two-byte AudioSpecificConfig its profile, sampling frequency, and
// channel configuration make; ok is false for data that starts no ADTS
// frame.
func ADTS(data []byte) (header int, config []byte, ok bool) {
	if len(data) < 7 || data[0] != 0xff || data[1]&0xf6 != 0xf0 {
		return 0, nil, false
	}
	header = 7
	if data[1]&1 == 0 { // CRC present
		header = 9
	}
	if len(data) < header {
		return 0, nil, false
	}
	objectType := data[2]>>6 + 1
	frequency := data[2] >> 2 & 0xf
	channels := data[2]&1<<2 | data[3]>>6
	return header, []byte{objectType<<3 | frequency>>1, frequency<<7 | channels<<3}, true
}

// sampleRates are the sampling frequencies an AudioSpecificConfig indexes.
var sampleRates = [...]int{96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000, 7350}

// ParseConfig reads the sampling frequency and channel count an
// AudioSpecificConfig (ISO/IEC 14496-3 1.6.2.1) declares; ok is false for
// one too short or naming no frequency. Channels is 0 for a channel
// configuration the program config element carries.
func ParseConfig(config []byte) (sampleRate, channels int, ok bool) {
	var bits uint64
	for i := range min(len(config), 8) {
		bits |= uint64(config[i]) << (56 - 8*i)
	}
	n := len(config) * 8
	pos := 0
	read := func(width int) int {
		v := int(bits << pos >> (64 - width))
		pos += width
		return v
	}
	if n < 16 {
		return 0, 0, false
	}
	if read(5) == 31 { // audioObjectTypeExt
		read(6)
	}
	index := read(4)
	switch {
	case index == 15:
		if n < pos+24+4 {
			return 0, 0, false
		}
		sampleRate = read(24)
	case index < len(sampleRates):
		sampleRate = sampleRates[index]
	default:
		return 0, 0, false
	}
	channels = read(4)
	if channels == 7 {
		channels = 8
	} else if channels > 7 {
		channels = 0
	}
	return sampleRate, channels, pos <= n
}
