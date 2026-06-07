package matroska

import (
	"encoding/binary"
	"strings"

	"github.com/thesyncim/goav/av"
)

const codecPrivateScratchSize = 32

const (
	opusSampleRate           = 48000
	opusDefaultSeekPreRollNS = 80_000_000
)

const (
	avcNALUTypeMask = 0x1f
	avcNALUSPS      = 7
	avcNALUPPS      = 8
)

const (
	av1OBUSequenceHeader = 1
	av1MaxLEB128Bytes    = 8
)

const (
	waveFormatALaw       uint16 = 0x0006
	waveFormatMuLaw      uint16 = 0x0007
	waveFormatExBaseSize        = 18
)

const (
	codecIDOpus     = "A_OPUS"
	codecIDVorbis   = "A_VORBIS"
	codecIDFLAC     = "A_FLAC"
	codecIDAAC      = "A_AAC"
	codecIDMS       = "A_MS/ACM"
	codecIDVP8      = "V_VP8"
	codecIDVP9      = "V_VP9"
	codecIDAV1      = "V_AV1"
	codecIDH264     = "V_MPEG4/ISO/AVC"
	codecIDH265     = "V_MPEGH/ISO/HEVC"
	codecIDTextUTF8 = "S_TEXT/UTF8"
)

func matroskaCodecID(codec Codec) (string, error) {
	switch codec {
	case CodecOpus:
		return codecIDOpus, nil
	case CodecVorbis:
		return codecIDVorbis, nil
	case CodecFLAC:
		return codecIDFLAC, nil
	case CodecAAC:
		return codecIDAAC, nil
	case CodecPCMU, CodecPCMA:
		return codecIDMS, nil
	case CodecVP8:
		return codecIDVP8, nil
	case CodecVP9:
		return codecIDVP9, nil
	case CodecAV1:
		return codecIDAV1, nil
	case CodecH264:
		return codecIDH264, nil
	case CodecH265:
		return codecIDH265, nil
	case CodecTextUTF8:
		return codecIDTextUTF8, nil
	default:
		return "", ErrUnsupportedCodec
	}
}

func codecFromMatroskaID(id string, private []byte) Codec {
	switch id {
	case codecIDOpus:
		return CodecOpus
	case codecIDVorbis:
		return CodecVorbis
	case codecIDFLAC:
		return CodecFLAC
	case codecIDAAC:
		return CodecAAC
	case codecIDVP8:
		return CodecVP8
	case codecIDVP9:
		return CodecVP9
	case codecIDAV1:
		return CodecAV1
	case codecIDH264:
		return CodecH264
	case codecIDH265:
		return CodecH265
	case codecIDTextUTF8:
		return CodecTextUTF8
	case codecIDMS:
		if len(private) >= 2 {
			return codecFromMSACMTag(binary.LittleEndian.Uint16(private[:2]))
		}
	}
	if strings.HasPrefix(id, "A_AAC/") || strings.HasPrefix(id, "A_AAC-") {
		return CodecAAC
	}
	return CodecUnknown
}

func codecFromMSACMTag(tag uint16) Codec {
	switch tag {
	case waveFormatMuLaw:
		return CodecPCMU
	case waveFormatALaw:
		return CodecPCMA
	default:
		return CodecUnknown
	}
}

func codecFromAV(id av.CodecID) Codec {
	switch id {
	case av.CodecOpus:
		return CodecOpus
	case av.CodecVorbis:
		return CodecVorbis
	case av.CodecFLAC:
		return CodecFLAC
	case av.CodecAAC:
		return CodecAAC
	case av.CodecVP8:
		return CodecVP8
	case av.CodecVP9:
		return CodecVP9
	case av.CodecAV1:
		return CodecAV1
	case av.CodecH264:
		return CodecH264
	case av.CodecPCM:
		return CodecUnknown
	case av.CodecTextUTF8:
		return CodecTextUTF8
	default:
		return CodecUnknown
	}
}

func codecToAV(codec Codec) av.CodecID {
	switch codec {
	case CodecOpus:
		return av.CodecOpus
	case CodecVorbis:
		return av.CodecVorbis
	case CodecFLAC:
		return av.CodecFLAC
	case CodecAAC:
		return av.CodecAAC
	case CodecVP8:
		return av.CodecVP8
	case CodecVP9:
		return av.CodecVP9
	case CodecAV1:
		return av.CodecAV1
	case CodecH264:
		return av.CodecH264
	case CodecPCMU, CodecPCMA:
		return av.CodecPCM
	case CodecTextUTF8:
		return av.CodecTextUTF8
	default:
		return av.CodecUnknown
	}
}

type aacAudioSpecificConfig struct {
	ObjectType int
	SampleRate int
	Channels   int
}

func parseAACAudioSpecificConfig(private []byte) (aacAudioSpecificConfig, error) {
	reader := aacConfigBitReader{data: private}
	objectType, err := readAACAudioObjectType(&reader)
	if err != nil || objectType == 0 {
		return aacAudioSpecificConfig{}, ErrInvalidData
	}
	sampleRate, err := readAACSamplingFrequency(&reader)
	if err != nil {
		return aacAudioSpecificConfig{}, ErrInvalidData
	}
	channelConfig, err := reader.read(4)
	if err != nil {
		return aacAudioSpecificConfig{}, ErrInvalidData
	}
	channels := aacChannelCount(int(channelConfig))
	if channels == 0 {
		return aacAudioSpecificConfig{}, ErrInvalidData
	}
	if objectType == 5 || objectType == 29 {
		extensionRate, err := readAACSamplingFrequency(&reader)
		if err != nil {
			return aacAudioSpecificConfig{}, ErrInvalidData
		}
		extensionObjectType, err := readAACAudioObjectType(&reader)
		if err != nil || extensionObjectType == 0 {
			return aacAudioSpecificConfig{}, ErrInvalidData
		}
		objectType = extensionObjectType
		sampleRate = extensionRate
	}
	return aacAudioSpecificConfig{
		ObjectType: objectType,
		SampleRate: sampleRate,
		Channels:   channels,
	}, nil
}

type aacConfigBitReader struct {
	data []byte
	bit  int
}

func (r *aacConfigBitReader) read(bits int) (uint32, error) {
	if bits <= 0 || bits > 32 || len(r.data)*8-r.bit < bits {
		return 0, ErrInvalidData
	}
	var value uint32
	for i := 0; i < bits; i++ {
		byteIndex := r.bit / 8
		bitIndex := 7 - r.bit%8
		value = (value << 1) | uint32((r.data[byteIndex]>>bitIndex)&1)
		r.bit++
	}
	return value, nil
}

func readAACAudioObjectType(reader *aacConfigBitReader) (int, error) {
	value, err := reader.read(5)
	if err != nil {
		return 0, err
	}
	if value == 31 {
		extension, err := reader.read(6)
		if err != nil {
			return 0, err
		}
		value = 32 + extension
	}
	return int(value), nil
}

func readAACSamplingFrequency(reader *aacConfigBitReader) (int, error) {
	index, err := reader.read(4)
	if err != nil {
		return 0, err
	}
	if index == 15 {
		value, err := reader.read(24)
		if err != nil || value == 0 || uint64(value) > maxIntValue {
			return 0, ErrInvalidData
		}
		return int(value), nil
	}
	if index >= uint32(len(aacSamplingFrequencies)) || aacSamplingFrequencies[index] == 0 {
		return 0, ErrInvalidData
	}
	return aacSamplingFrequencies[index], nil
}

var aacSamplingFrequencies = [...]int{
	96000,
	88200,
	64000,
	48000,
	44100,
	32000,
	24000,
	22050,
	16000,
	12000,
	11025,
	8000,
	7350,
}

func aacChannelCount(config int) int {
	switch config {
	case 1:
		return 1
	case 2:
		return 2
	case 3:
		return 3
	case 4:
		return 4
	case 5:
		return 5
	case 6:
		return 6
	case 7:
		return 8
	case 11:
		return 7
	case 12, 14:
		return 8
	default:
		return 0
	}
}

type flacCodecPrivate struct {
	Channels      int
	SampleRate    int
	BitsPerSample int
}

func parseFLACCodecPrivate(private []byte) (flacCodecPrivate, error) {
	if len(private) < 4+4+34 || string(private[:4]) != "fLaC" {
		return flacCodecPrivate{}, ErrInvalidData
	}
	header := private[4:8]
	if header[0]&0x7f != 0 {
		return flacCodecPrivate{}, ErrInvalidData
	}
	if metadataBlockLength(header) != 34 {
		return flacCodecPrivate{}, ErrInvalidData
	}
	streamInfo := private[8:42]
	minBlockSize := binary.BigEndian.Uint16(streamInfo[0:2])
	maxBlockSize := binary.BigEndian.Uint16(streamInfo[2:4])
	if minBlockSize == 0 || maxBlockSize == 0 || minBlockSize > maxBlockSize {
		return flacCodecPrivate{}, ErrInvalidData
	}
	sampleRate := int(uint32(streamInfo[10])<<12 | uint32(streamInfo[11])<<4 | uint32(streamInfo[12]>>4))
	channels := int((streamInfo[12]>>1)&0x07) + 1
	bitsPerSample := int(((streamInfo[12]&0x01)<<4)|(streamInfo[13]>>4)) + 1
	if sampleRate == 0 || bitsPerSample == 0 {
		return flacCodecPrivate{}, ErrInvalidData
	}
	return flacCodecPrivate{
		Channels:      channels,
		SampleRate:    sampleRate,
		BitsPerSample: bitsPerSample,
	}, nil
}

func metadataBlockLength(header []byte) int {
	return int(header[1])<<16 | int(header[2])<<8 | int(header[3])
}

type vorbisCodecPrivate struct {
	Channels   int
	SampleRate int
}

func parseVorbisCodecPrivate(private []byte) (vorbisCodecPrivate, error) {
	if len(private) < 4 || private[0] != 2 {
		return vorbisCodecPrivate{}, ErrInvalidData
	}
	offset := 1
	var sizes [2]int
	for i := range sizes {
		size := 0
		for {
			if offset >= len(private) {
				return vorbisCodecPrivate{}, ErrInvalidData
			}
			value := int(private[offset])
			offset++
			if uint64(size) > maxIntValue-uint64(value) {
				return vorbisCodecPrivate{}, ErrInvalidData
			}
			size += value
			if value != 255 {
				break
			}
		}
		if size == 0 {
			return vorbisCodecPrivate{}, ErrInvalidData
		}
		sizes[i] = size
	}
	if sizes[0] > len(private)-offset || sizes[1] > len(private)-offset-sizes[0] {
		return vorbisCodecPrivate{}, ErrInvalidData
	}
	lastSize := len(private) - offset - sizes[0] - sizes[1]
	if lastSize == 0 {
		return vorbisCodecPrivate{}, ErrInvalidData
	}
	identification := private[offset : offset+sizes[0]]
	offset += sizes[0]
	comment := private[offset : offset+sizes[1]]
	offset += sizes[1]
	setup := private[offset:]
	if len(identification) < 30 ||
		!hasVorbisPacketHeader(identification, 1) ||
		!hasVorbisPacketHeader(comment, 3) ||
		!hasVorbisPacketHeader(setup, 5) {
		return vorbisCodecPrivate{}, ErrInvalidData
	}
	if binary.LittleEndian.Uint32(identification[7:11]) != 0 {
		return vorbisCodecPrivate{}, ErrInvalidData
	}
	channels := int(identification[11])
	sampleRate := binary.LittleEndian.Uint32(identification[12:16])
	if channels == 0 || sampleRate == 0 || uint64(sampleRate) > maxIntValue {
		return vorbisCodecPrivate{}, ErrInvalidData
	}
	blockSizes := identification[28]
	if blockSizes&0x0f > blockSizes>>4 || identification[29]&0x01 == 0 {
		return vorbisCodecPrivate{}, ErrInvalidData
	}
	return vorbisCodecPrivate{Channels: channels, SampleRate: int(sampleRate)}, nil
}

func hasVorbisPacketHeader(packet []byte, packetType byte) bool {
	return len(packet) >= 7 &&
		packet[0] == packetType &&
		packet[1] == 'v' &&
		packet[2] == 'o' &&
		packet[3] == 'r' &&
		packet[4] == 'b' &&
		packet[5] == 'i' &&
		packet[6] == 's'
}

func defaultCodecPrivate(track Track, scratch *[codecPrivateScratchSize]byte) []byte {
	switch track.Codec {
	case CodecOpus:
		channels := track.Audio.Channels
		if channels == 0 {
			channels = 2
		}
		if channels < 1 || channels > 2 {
			return nil
		}
		sampleRate := track.Audio.SampleRate
		if sampleRate == 0 {
			sampleRate = 48000
		}
		if sampleRate < 0 || uint64(sampleRate) > uint64(^uint32(0)) {
			return nil
		}
		copy(scratch[:], "OpusHead")
		scratch[8] = 1
		scratch[9] = byte(channels)
		binary.LittleEndian.PutUint16(scratch[10:12], 0)
		binary.LittleEndian.PutUint32(scratch[12:16], uint32(sampleRate))
		binary.LittleEndian.PutUint16(scratch[16:18], 0)
		scratch[18] = 0
		return scratch[:19]
	case CodecPCMU, CodecPCMA:
		tag := waveFormatMuLaw
		if track.Codec == CodecPCMA {
			tag = waveFormatALaw
		}
		channels := uint16(track.Audio.Channels)
		if channels == 0 {
			channels = 1
		}
		sampleRate := uint32(track.Audio.SampleRate)
		if sampleRate == 0 {
			sampleRate = 8000
		}
		binary.LittleEndian.PutUint16(scratch[0:2], tag)
		binary.LittleEndian.PutUint16(scratch[2:4], channels)
		binary.LittleEndian.PutUint32(scratch[4:8], sampleRate)
		binary.LittleEndian.PutUint32(scratch[8:12], sampleRate*uint32(channels))
		binary.LittleEndian.PutUint16(scratch[12:14], channels)
		binary.LittleEndian.PutUint16(scratch[14:16], 8)
		binary.LittleEndian.PutUint16(scratch[16:18], 0)
		return scratch[:waveFormatExBaseSize]
	default:
		return nil
	}
}

type msACMWaveFormat struct {
	FormatTag      uint16
	Channels       int
	SampleRate     int
	AvgBytesPerSec uint32
	BlockAlign     int
	BitsPerSample  int
	ExtraSize      int
}

func parseMSACMWaveFormat(private []byte) (msACMWaveFormat, error) {
	if len(private) < waveFormatExBaseSize {
		return msACMWaveFormat{}, ErrInvalidData
	}
	extraSize := int(binary.LittleEndian.Uint16(private[16:18]))
	if extraSize != len(private)-waveFormatExBaseSize {
		return msACMWaveFormat{}, ErrInvalidData
	}
	sampleRate := binary.LittleEndian.Uint32(private[4:8])
	if sampleRate == 0 || uint64(sampleRate) > maxIntValue {
		return msACMWaveFormat{}, ErrInvalidData
	}
	channels := binary.LittleEndian.Uint16(private[2:4])
	blockAlign := binary.LittleEndian.Uint16(private[12:14])
	if channels == 0 || blockAlign == 0 {
		return msACMWaveFormat{}, ErrInvalidData
	}
	avgBytesPerSec := binary.LittleEndian.Uint32(private[8:12])
	if avgBytesPerSec == 0 {
		return msACMWaveFormat{}, ErrInvalidData
	}
	return msACMWaveFormat{
		FormatTag:      binary.LittleEndian.Uint16(private[0:2]),
		Channels:       int(channels),
		SampleRate:     int(sampleRate),
		AvgBytesPerSec: avgBytesPerSec,
		BlockAlign:     int(blockAlign),
		BitsPerSample:  int(binary.LittleEndian.Uint16(private[14:16])),
		ExtraSize:      extraSize,
	}, nil
}

func validateG711MSACMWaveFormat(format msACMWaveFormat, codec Codec) error {
	wantTag := waveFormatMuLaw
	if codec == CodecPCMA {
		wantTag = waveFormatALaw
	}
	if codec != CodecPCMU && codec != CodecPCMA {
		return ErrInvalidData
	}
	if format.FormatTag != wantTag || format.BitsPerSample != 8 || format.ExtraSize != 0 {
		return ErrInvalidData
	}
	if format.BlockAlign != format.Channels {
		return ErrInvalidData
	}
	expectedAvg := uint64(format.SampleRate) * uint64(format.Channels)
	if expectedAvg > uint64(^uint32(0)) || format.AvgBytesPerSec != uint32(expectedAvg) {
		return ErrInvalidData
	}
	return nil
}

type avcDecoderConfigurationRecord struct {
	ProfileIDC           byte
	ProfileCompatibility byte
	LevelIDC             byte
	NALULengthSize       int
	SPSCount             int
	PPSCount             int
}

func parseAVCDecoderConfigurationRecord(private []byte) (avcDecoderConfigurationRecord, error) {
	if len(private) < 7 || private[0] != 1 {
		return avcDecoderConfigurationRecord{}, ErrInvalidData
	}
	if private[4]&0xfc != 0xfc || private[5]&0xe0 != 0xe0 {
		return avcDecoderConfigurationRecord{}, ErrInvalidData
	}
	lengthSizeMinusOne := private[4] & 0x03
	if lengthSizeMinusOne == 2 {
		return avcDecoderConfigurationRecord{}, ErrInvalidData
	}
	offset := 6
	spsCount := int(private[5] & 0x1f)
	if spsCount == 0 {
		return avcDecoderConfigurationRecord{}, ErrInvalidData
	}
	for i := 0; i < spsCount; i++ {
		nal, next, err := readAVCParameterSet(private, offset)
		if err != nil {
			return avcDecoderConfigurationRecord{}, err
		}
		if nal[0]&avcNALUTypeMask != avcNALUSPS {
			return avcDecoderConfigurationRecord{}, ErrInvalidData
		}
		offset = next
	}
	if offset >= len(private) {
		return avcDecoderConfigurationRecord{}, ErrInvalidData
	}
	ppsCount := int(private[offset])
	offset++
	if ppsCount == 0 {
		return avcDecoderConfigurationRecord{}, ErrInvalidData
	}
	for i := 0; i < ppsCount; i++ {
		nal, next, err := readAVCParameterSet(private, offset)
		if err != nil {
			return avcDecoderConfigurationRecord{}, err
		}
		if nal[0]&avcNALUTypeMask != avcNALUPPS {
			return avcDecoderConfigurationRecord{}, ErrInvalidData
		}
		offset = next
	}
	return avcDecoderConfigurationRecord{
		ProfileIDC:           private[1],
		ProfileCompatibility: private[2],
		LevelIDC:             private[3],
		NALULengthSize:       int(lengthSizeMinusOne) + 1,
		SPSCount:             spsCount,
		PPSCount:             ppsCount,
	}, nil
}

func readAVCParameterSet(private []byte, offset int) ([]byte, int, error) {
	if offset < 0 || offset+2 > len(private) {
		return nil, 0, ErrInvalidData
	}
	size := int(binary.BigEndian.Uint16(private[offset : offset+2]))
	offset += 2
	if size == 0 || offset+size > len(private) {
		return nil, 0, ErrInvalidData
	}
	return private[offset : offset+size], offset + size, nil
}

type av1CodecConfigurationRecord struct {
	SeqProfile                       int
	SeqLevelIdx0                     int
	SeqTier0                         bool
	HighBitDepth                     bool
	TwelveBit                        bool
	Monochrome                       bool
	ChromaSubsamplingX               bool
	ChromaSubsamplingY               bool
	ChromaSamplePosition             int
	InitialPresentationDelaySet      bool
	InitialPresentationDelayMinusOne int
	ConfigOBUCount                   int
	SequenceHeaderOBUPresent         bool
}

func parseAV1CodecConfigurationRecord(private []byte) (av1CodecConfigurationRecord, error) {
	if len(private) < 4 || private[0]&0x80 == 0 || private[0]&0x7f != 1 {
		return av1CodecConfigurationRecord{}, ErrInvalidData
	}
	profile := int(private[1] >> 5)
	if profile > 2 {
		return av1CodecConfigurationRecord{}, ErrInvalidData
	}
	highBitDepth := private[2]&0x40 != 0
	twelveBit := private[2]&0x20 != 0
	if twelveBit && (!highBitDepth || profile != 2) {
		return av1CodecConfigurationRecord{}, ErrInvalidData
	}
	chromaSamplePosition := int(private[2] & 0x03)
	if chromaSamplePosition == 3 {
		return av1CodecConfigurationRecord{}, ErrInvalidData
	}
	if private[3]&0xe0 != 0 {
		return av1CodecConfigurationRecord{}, ErrInvalidData
	}
	initialDelaySet := private[3]&0x10 != 0
	if !initialDelaySet && private[3]&0x0f != 0 {
		return av1CodecConfigurationRecord{}, ErrInvalidData
	}
	config := av1CodecConfigurationRecord{
		SeqProfile:                       profile,
		SeqLevelIdx0:                     int(private[1] & 0x1f),
		SeqTier0:                         private[2]&0x80 != 0,
		HighBitDepth:                     highBitDepth,
		TwelveBit:                        twelveBit,
		Monochrome:                       private[2]&0x10 != 0,
		ChromaSubsamplingX:               private[2]&0x08 != 0,
		ChromaSubsamplingY:               private[2]&0x04 != 0,
		ChromaSamplePosition:             chromaSamplePosition,
		InitialPresentationDelaySet:      initialDelaySet,
		InitialPresentationDelayMinusOne: int(private[3] & 0x0f),
	}
	offset := 4
	for offset < len(private) {
		obuType, next, err := readAV1ConfigOBU(private, offset)
		if err != nil {
			return av1CodecConfigurationRecord{}, err
		}
		if obuType == av1OBUSequenceHeader {
			if config.SequenceHeaderOBUPresent || config.ConfigOBUCount != 0 {
				return av1CodecConfigurationRecord{}, ErrInvalidData
			}
			config.SequenceHeaderOBUPresent = true
		}
		config.ConfigOBUCount++
		offset = next
	}
	return config, nil
}

func readAV1ConfigOBU(private []byte, offset int) (int, int, error) {
	if offset < 0 || offset >= len(private) {
		return 0, 0, ErrInvalidData
	}
	header := private[offset]
	if header&0x80 != 0 || header&0x01 != 0 {
		return 0, 0, ErrInvalidData
	}
	obuType := int((header >> 3) & 0x0f)
	if obuType == 0 {
		return 0, 0, ErrInvalidData
	}
	hasExtension := header&0x04 != 0
	hasSize := header&0x02 != 0
	if !hasSize {
		return 0, 0, ErrInvalidData
	}
	offset++
	if hasExtension {
		if offset >= len(private) || private[offset]&0x07 != 0 {
			return 0, 0, ErrInvalidData
		}
		offset++
	}
	size, sizeWidth, err := readAV1LEB128(private, offset)
	if err != nil {
		return 0, 0, err
	}
	offset += sizeWidth
	if size > uint64(len(private)-offset) {
		return 0, 0, ErrInvalidData
	}
	return obuType, offset + int(size), nil
}

func readAV1LEB128(data []byte, offset int) (uint64, int, error) {
	var value uint64
	for i := 0; i < av1MaxLEB128Bytes; i++ {
		if offset+i >= len(data) {
			return 0, 0, ErrInvalidData
		}
		b := data[offset+i]
		value |= uint64(b&0x7f) << uint(i*7)
		if b&0x80 == 0 {
			return value, i + 1, nil
		}
	}
	return 0, 0, ErrInvalidData
}

type opusHead struct {
	Channels   int
	SampleRate int
	PreSkip    int
}

func parseOpusHead(private []byte) (opusHead, error) {
	if len(private) < 19 || !hasOpusHeadMagic(private) {
		return opusHead{}, ErrInvalidData
	}
	if private[8]&0xf0 != 0 {
		return opusHead{}, ErrInvalidData
	}
	channels := int(private[9])
	if channels == 0 {
		return opusHead{}, ErrInvalidData
	}
	preSkip := int(binary.LittleEndian.Uint16(private[10:12]))
	sampleRate := binary.LittleEndian.Uint32(private[12:16])
	if uint64(sampleRate) > maxIntValue {
		return opusHead{}, ErrInvalidData
	}
	mappingFamily := private[18]
	if mappingFamily == 0 {
		if channels > 2 || len(private) != 19 {
			return opusHead{}, ErrInvalidData
		}
		return opusHead{Channels: channels, SampleRate: int(sampleRate), PreSkip: preSkip}, nil
	}
	if len(private) != 21+channels {
		return opusHead{}, ErrInvalidData
	}
	streams := int(private[19])
	coupled := int(private[20])
	if streams == 0 || coupled > streams {
		return opusHead{}, ErrInvalidData
	}
	decodedChannels := streams + coupled
	if decodedChannels > 255 {
		return opusHead{}, ErrInvalidData
	}
	for i := 0; i < channels; i++ {
		index := private[21+i]
		if index != 255 && int(index) >= decodedChannels {
			return opusHead{}, ErrInvalidData
		}
	}
	return opusHead{Channels: channels, SampleRate: int(sampleRate), PreSkip: preSkip}, nil
}

func opusCodecDelayNS(preSkip int) int64 {
	return int64(preSkip) * timeNS / opusSampleRate
}

func hasOpusHeadMagic(private []byte) bool {
	return len(private) >= 8 &&
		private[0] == 'O' &&
		private[1] == 'p' &&
		private[2] == 'u' &&
		private[3] == 's' &&
		private[4] == 'H' &&
		private[5] == 'e' &&
		private[6] == 'a' &&
		private[7] == 'd'
}
