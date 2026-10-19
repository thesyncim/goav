package ts

import (
	"encoding/binary"
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/internal/bitio"
	"github.com/thesyncim/goav/internal/h264"
)

// NAL unit types the muxer and demuxer act on.
const (
	h264AUD  = 9
	hevcAUD  = 35
	hevcVPS  = 32
	hevcPPS  = 34
	hevcIRAP = 16 // BLA, IDR, and CRA are 16 through 23
)

// h264AUDUnit and hevcAUDUnit are access unit delimiters that admit any
// slice type (primary_pic_type 7, pic_type 2).
var (
	h264AUDUnit = []byte{0, 0, 0, 1, 0x09, 0xf0}
	hevcAUDUnit = []byte{0, 0, 0, 1, 0x46, 0x01, 0x50}
)

func nalType(codec av.CodecID, nal []byte) int {
	if len(nal) == 0 {
		return -1
	}
	if codec == av.CodecHEVC {
		return int(nal[0]>>1) & 0x3f
	}
	return int(nal[0] & 0x1f)
}

func isAUD(codec av.CodecID, kind int) bool {
	return codec == av.CodecHEVC && kind == hevcAUD || codec == av.CodecH264 && kind == h264AUD
}

func isParameterSet(codec av.CodecID, kind int) bool {
	if codec == av.CodecHEVC {
		return kind >= hevcVPS && kind <= hevcPPS
	}
	return kind == h264.NALSPS || kind == h264.NALPPS
}

func isVCL(codec av.CodecID, kind int) bool {
	if codec == av.CodecHEVC {
		return kind >= 0 && kind < 32
	}
	return kind >= 1 && kind <= 5
}

func isRandomAccess(codec av.CodecID, kind int) bool {
	if codec == av.CodecHEVC {
		return kind >= hevcIRAP && kind <= 23
	}
	return kind == h264.NALIDR
}

// startCode finds the next Annex B start code at or after from: its offset
// and its length (3 or 4), or -1.
func startCode(data []byte, from int) (int, int) {
	for i := from; i+3 <= len(data); i++ {
		if data[i+2] > 1 {
			i += 2
			continue
		}
		if data[i] != 0 || data[i+1] != 0 {
			continue
		}
		if data[i+2] == 1 {
			if i > from && data[i-1] == 0 {
				return i - 1, 4
			}
			return i, 3
		}
	}
	return -1, 0
}

// accessUnit is what the muxer and demuxer read from the head of an Annex B
// access unit, up to its first slice: whether it opens with a delimiter
// (and where the delimiter ends), whether parameter sets precede the
// slices, and whether the first slice is a random access point.
type accessUnit struct {
	audEnd   int // 0 without a leading delimiter
	params   bool
	keyframe bool
	annexB   bool
}

func readAccessUnit(codec av.CodecID, data []byte) accessUnit {
	var au accessUnit
	at, width := startCode(data, 0)
	if at < 0 {
		return au
	}
	au.annexB = true
	for first := true; at >= 0; first = false {
		begin := at + width
		next, nextWidth := startCode(data, begin)
		end := len(data)
		if next >= 0 {
			end = next
		}
		kind := nalType(codec, data[begin:end])
		switch {
		case first && isAUD(codec, kind):
			au.audEnd = end
		case isParameterSet(codec, kind):
			au.params = true
		case isVCL(codec, kind):
			au.keyframe = isRandomAccess(codec, kind)
			return au
		}
		at, width = next, nextWidth
	}
	return au
}

// parameterSets returns a stream's parameter sets as Annex B from its
// ExtraData, which is an avcC or hvcC record or Annex B parameter sets.
func parameterSets(codec av.CodecID, extra []byte) ([]byte, error) {
	switch {
	case len(extra) == 0:
		return nil, nil
	case h264.HasStartCode(extra):
		var out []byte
		err := h264.ForEachNAL(extra, func(nal []byte) error {
			if isParameterSet(codec, nalType(codec, nal)) {
				out = append(append(out, h264.StartCode[:]...), nal...)
			}
			return nil
		})
		return out, err
	case codec == av.CodecH264:
		config, err := h264.ParseConfig(extra)
		if err != nil {
			return nil, err
		}
		return config.AppendParameterSets(nil), nil
	default:
		return hvcCParameterSets(extra)
	}
}

// hvcCParameterSets reads the NAL unit arrays of an HEVC decoder
// configuration record (ISO/IEC 14496-15 §8.3.3) as Annex B.
func hvcCParameterSets(record []byte) ([]byte, error) {
	if len(record) < 23 || record[0] != 1 {
		return nil, fmt.Errorf("%w: hvcC record", ErrInvalidData)
	}
	var out []byte
	pos := 23
	for range int(record[22]) {
		if pos+3 > len(record) {
			return nil, fmt.Errorf("%w: hvcC record", ErrInvalidData)
		}
		count := int(binary.BigEndian.Uint16(record[pos+1:]))
		pos += 3
		for range count {
			if pos+2 > len(record) {
				return nil, fmt.Errorf("%w: hvcC record", ErrInvalidData)
			}
			size := int(binary.BigEndian.Uint16(record[pos:]))
			pos += 2
			if size == 0 || pos+size > len(record) {
				return nil, fmt.Errorf("%w: hvcC record", ErrInvalidData)
			}
			out = append(append(out, h264.StartCode[:]...), record[pos:pos+size]...)
			pos += size
		}
	}
	return out, nil
}

// aacSampleRates is the MPEG-4 sampling frequency index table.
var aacSampleRates = [...]int{96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000, 7350}

// aacConfig is what an ADTS header and an AudioSpecificConfig share: the
// object type, the sampling frequency index, and the channel configuration.
type aacConfig struct {
	objectType int
	rateIndex  int
	channels   int
}

func (c aacConfig) sampleRate() int { return aacSampleRates[c.rateIndex] }

// aacConfigOf reads a stream's AAC configuration from its AudioSpecificConfig,
// or from its sample rate and channels (AAC-LC) without one. ADTS has two
// bits of object type, so only Main, LC, SSR, and LTP fit; HE-AAC, signalled
// explicitly, is written as its LC core, which decoders extend implicitly.
func aacConfigOf(params av.CodecParameters) (aacConfig, error) {
	if extra := params.ExtraData.Bytes; len(extra) >= 2 {
		r := bitio.Reader{Data: extra, Err: ErrInvalidData}
		object := audioObjectType(&r)
		index, _ := r.Bits(4)
		if index == 15 {
			r.Bits(24) // an explicit rate, which ADTS cannot carry
		}
		channels, _ := r.Bits(4)
		if object == 5 || object == 29 {
			// Explicit SBR: the extension rate, then the core object type.
			if extension, _ := r.Bits(4); extension == 15 {
				r.Bits(24)
			}
			object = audioObjectType(&r)
		}
		if object < 1 || object > 4 || index >= uint64(len(aacSampleRates)) || channels == 0 || channels > 7 {
			return aacConfig{}, fmt.Errorf("%w: AAC object type %d at rate index %d with %d channels has no ADTS form", ErrUnsupported, object, index, channels)
		}
		return aacConfig{objectType: int(object), rateIndex: int(index), channels: int(channels)}, nil
	}
	for i, rate := range aacSampleRates {
		if rate == params.SampleRate && params.Channels >= 1 && params.Channels <= 7 {
			return aacConfig{objectType: 2, rateIndex: i, channels: params.Channels}, nil
		}
	}
	return aacConfig{}, fmt.Errorf("%w: AAC stream without an AudioSpecificConfig or a standard sample rate and channel count", ErrUnsupported)
}

// audioObjectType reads an AudioSpecificConfig object type, escape included.
func audioObjectType(r *bitio.Reader) uint64 {
	object, _ := r.Bits(5)
	if object == 31 {
		extended, _ := r.Bits(6)
		object = 32 + extended
	}
	return object
}

// audioSpecificConfig is the two-byte AudioSpecificConfig of c.
func (c aacConfig) audioSpecificConfig() []byte {
	v := uint16(c.objectType)<<11 | uint16(c.rateIndex)<<7 | uint16(c.channels)<<3
	return []byte{byte(v >> 8), byte(v)}
}

// adtsHeader writes the 7-byte ADTS header (no CRC) of a frame carrying
// size payload bytes.
func (c aacConfig) adtsHeader(dst *[7]byte, size int) {
	n := size + 7
	dst[0] = 0xff
	dst[1] = 0xf1
	dst[2] = byte(c.objectType-1)<<6 | byte(c.rateIndex)<<2 | byte(c.channels>>2)
	dst[3] = byte(c.channels&3)<<6 | byte(n>>11)
	dst[4] = byte(n >> 3)
	dst[5] = byte(n&7)<<5 | 0x1f
	dst[6] = 0xfc
}

// adtsFrame reads the ADTS frame at the head of b: its configuration, header
// length, and total length. ok is false when b holds no whole frame.
func adtsFrame(b []byte) (c aacConfig, header, length int, ok bool) {
	if len(b) < 7 || b[0] != 0xff || b[1]&0xf6 != 0xf0 {
		return aacConfig{}, 0, 0, false
	}
	header = 7
	if b[1]&1 == 0 {
		header = 9
	}
	length = int(b[3]&3)<<11 | int(b[4])<<3 | int(b[5]>>5)
	c = aacConfig{objectType: int(b[2]>>6) + 1, rateIndex: int(b[2]>>2) & 0xf, channels: int(b[2]&1)<<2 | int(b[3]>>6)}
	if length < header || length > len(b) || c.rateIndex >= len(aacSampleRates) {
		return aacConfig{}, 0, 0, false
	}
	return c, header, length, true
}

// Opus channel mappings for channel_config_code 1..8 (RFC 7845 §5.1.1.2,
// Vorbis order): the stream and coupled-stream counts and the mapping.
var (
	opusStreams = [9]byte{0, 1, 1, 2, 2, 3, 4, 4, 5}
	opusCoupled = [9]byte{0, 0, 1, 1, 2, 2, 2, 3, 3}
	opusMapping = [9][]byte{nil, {0}, {0, 1}, {0, 2, 1}, {0, 1, 2, 3}, {0, 4, 1, 2, 3}, {0, 4, 1, 2, 3, 5}, {0, 4, 1, 2, 3, 5, 6}, {0, 6, 1, 2, 3, 4, 5, 7}}
)

// opusChannelCode is the channel_config_code the PMT carries for a stream:
// the channel count, for mapping family 0 (one or two channels) or family 1
// in Vorbis order (up to eight).
func opusChannelCode(params av.CodecParameters) (byte, int, error) {
	head := params.ExtraData.Bytes
	if len(head) < 19 || string(head[:8]) != "OpusHead" {
		channels := params.Channels
		if channels == 0 {
			channels = 2
		}
		if channels > 2 {
			return 0, 0, fmt.Errorf("%w: a %d-channel Opus stream needs its OpusHead", ErrUnsupported, channels)
		}
		return byte(channels), 0, nil
	}
	channels, family := int(head[9]), head[18]
	preSkip := int(binary.LittleEndian.Uint16(head[10:]))
	switch {
	case family == 0 && channels >= 1 && channels <= 2:
		return byte(channels), preSkip, nil
	case family == 1 && channels >= 1 && channels <= 8 && len(head) >= 21+channels &&
		head[19] == opusStreams[channels] && head[20] == opusCoupled[channels] &&
		string(head[21:21+channels]) == string(opusMapping[channels]):
		return byte(channels), preSkip, nil
	}
	return 0, 0, fmt.Errorf("%w: Opus channel mapping family %d with %d channels has no transport stream channel code", ErrUnsupported, family, channels)
}

// opusHead builds the identification header for a channel_config_code, with
// pre-skip in 48 kHz samples.
func opusHead(code byte, preSkip int) ([]byte, int, bool) {
	channels := int(code)
	if code == 0 || code > 8 {
		return nil, 0, false
	}
	head := make([]byte, 19, 21+channels)
	copy(head, "OpusHead")
	head[8] = 1
	head[9] = code
	binary.LittleEndian.PutUint16(head[10:], uint16(preSkip))
	binary.LittleEndian.PutUint32(head[12:], 48000)
	if channels > 2 {
		head[18] = 1
		head = append(head, opusStreams[channels], opusCoupled[channels])
		head = append(head, opusMapping[channels]...)
	}
	return head, channels, true
}

// opusControlHeader writes the control header that precedes each Opus
// packet in a PES: the 11-bit prefix, the trim flags, the packet size in
// 255-byte steps, and the start trim when there is one.
func opusControlHeader(dst []byte, size, startTrim int) []byte {
	flags := byte(0xe0)
	if startTrim > 0 {
		flags |= 0x10
	}
	dst = append(dst, 0x7f, flags)
	for ; size >= 255; size -= 255 {
		dst = append(dst, 0xff)
	}
	dst = append(dst, byte(size))
	if startTrim > 0 {
		dst = binary.BigEndian.AppendUint16(dst, uint16(min(startTrim, 0x1fff)))
	}
	return dst
}

// opusPacket reads the control header at the head of b: the packet, its
// start trim, and the bytes consumed. ok is false when b holds no whole one.
func opusPacket(b []byte) (packet []byte, startTrim, consumed int, ok bool) {
	if len(b) < 3 || b[0] != 0x7f || b[1]&0xe0 != 0xe0 {
		return nil, 0, 0, false
	}
	flags := b[1]
	pos, size := 2, 0
	for {
		if pos >= len(b) {
			return nil, 0, 0, false
		}
		v := int(b[pos])
		pos++
		size += v
		if v != 255 {
			break
		}
	}
	if flags&0x10 != 0 {
		if pos+2 > len(b) {
			return nil, 0, 0, false
		}
		startTrim = int(binary.BigEndian.Uint16(b[pos:]) & 0x1fff)
		pos += 2
	}
	if flags&0x08 != 0 {
		pos += 2
	}
	if flags&0x04 != 0 {
		if pos >= len(b) {
			return nil, 0, 0, false
		}
		pos += 1 + int(b[pos])
	}
	if pos+size > len(b) || size == 0 {
		return nil, 0, 0, false
	}
	return b[pos : pos+size], startTrim, pos + size, true
}

// opusSamples is an Opus packet's duration in 48 kHz samples (RFC 6716
// §3.1), or 0 when its TOC cannot say.
func opusSamples(packet []byte) int {
	if len(packet) == 0 {
		return 0
	}
	config := int(packet[0] >> 3)
	var frame int
	switch {
	case config < 12:
		frame = [4]int{480, 960, 1920, 2880}[config&3]
	case config < 16:
		frame = [2]int{480, 960}[config&1]
	default:
		frame = [4]int{120, 240, 480, 960}[config&3]
	}
	switch packet[0] & 3 {
	case 0:
		return frame
	case 1, 2:
		return 2 * frame
	default:
		if len(packet) < 2 {
			return 0
		}
		return int(packet[1]&0x3f) * frame
	}
}
