package av1

// Config is a parsed AV1CodecConfigurationRecord.
type Config struct {
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

// ParseConfig reads an av1C record: its header fields and config OBUs.
func ParseConfig(private []byte) (Config, error) {
	if len(private) < 4 || private[0]&0x80 == 0 || private[0]&0x7f != 1 {
		return Config{}, ErrInvalid
	}
	profile := int(private[1] >> 5)
	if profile > 2 {
		return Config{}, ErrInvalid
	}
	highBitDepth := private[2]&0x40 != 0
	twelveBit := private[2]&0x20 != 0
	if twelveBit && (!highBitDepth || profile != 2) {
		return Config{}, ErrInvalid
	}
	chromaSamplePosition := int(private[2] & 0x03)
	if chromaSamplePosition == 3 {
		return Config{}, ErrInvalid
	}
	if private[3]&0xe0 != 0 {
		return Config{}, ErrInvalid
	}
	initialDelaySet := private[3]&0x10 != 0
	if !initialDelaySet && private[3]&0x0f != 0 {
		return Config{}, ErrInvalid
	}
	config := Config{
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
			return Config{}, err
		}
		if obuType == av1OBUSequenceHeader {
			if config.SequenceHeaderOBUPresent || config.ConfigOBUCount != 0 {
				return Config{}, ErrInvalid
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
		return 0, 0, ErrInvalid
	}
	header := private[offset]
	if header&0x80 != 0 || header&0x01 != 0 {
		return 0, 0, ErrInvalid
	}
	obuType := int((header >> 3) & 0x0f)
	if obuType == 0 {
		return 0, 0, ErrInvalid
	}
	hasExtension := header&0x04 != 0
	hasSize := header&0x02 != 0
	if !hasSize {
		return 0, 0, ErrInvalid
	}
	offset++
	if hasExtension {
		if offset >= len(private) || private[offset]&0x07 != 0 {
			return 0, 0, ErrInvalid
		}
		offset++
	}
	size, sizeWidth, err := readAV1LEB128(private, offset)
	if err != nil {
		return 0, 0, err
	}
	offset += sizeWidth
	if size > uint64(len(private)-offset) {
		return 0, 0, ErrInvalid
	}
	return obuType, offset + int(size), nil
}

func readAV1LEB128(data []byte, offset int) (uint64, int, error) {
	var value uint64
	for i := range av1MaxLEB128Bytes {
		if offset+i >= len(data) {
			return 0, 0, ErrInvalid
		}
		b := data[offset+i]
		value |= uint64(b&0x7f) << uint(i*7)
		if b&0x80 == 0 {
			return value, i + 1, nil
		}
	}
	return 0, 0, ErrInvalid
}

const (
	av1OBUSequenceHeader = 1
	av1MaxLEB128Bytes    = 8
)
