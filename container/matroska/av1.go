package matroska

func av1CodecConfigurationRecordFromFrames(frames [][]byte) ([]byte, error) {
	var sequenceOBU []byte
	var sequence av1SequenceHeader
	for i := range frames {
		if err := av1ForEachOBU(frames[i], func(obu av1OBU) error {
			if obu.Type != av1OBUSequenceHeader {
				return nil
			}
			if sequenceOBU != nil {
				return ErrInvalidData
			}
			parsed, err := parseAV1SequenceHeader(obu.Payload)
			if err != nil {
				return err
			}
			sequence = parsed
			sequenceOBU = obu.Data
			return nil
		}); err != nil {
			return nil, err
		}
	}
	if sequenceOBU == nil {
		return nil, ErrInvalidData
	}
	private := make([]byte, 0, 4+len(sequenceOBU))
	private = append(private,
		0x81,
		byte(sequence.SeqProfile<<5)|byte(sequence.SeqLevelIdx0),
		av1CodecConfigByte2(sequence),
		av1CodecConfigByte3(sequence),
	)
	private = append(private, sequenceOBU...)
	return private, nil
}

func av1CodecConfigByte2(sequence av1SequenceHeader) byte {
	var b byte
	if sequence.SeqTier0 {
		b |= 0x80
	}
	if sequence.HighBitDepth {
		b |= 0x40
	}
	if sequence.TwelveBit {
		b |= 0x20
	}
	if sequence.Monochrome {
		b |= 0x10
	}
	if sequence.ChromaSubsamplingX {
		b |= 0x08
	}
	if sequence.ChromaSubsamplingY {
		b |= 0x04
	}
	b |= byte(sequence.ChromaSamplePosition)
	return b
}

func av1CodecConfigByte3(sequence av1SequenceHeader) byte {
	if !sequence.InitialPresentationDelaySet {
		return 0
	}
	return 0x10 | byte(sequence.InitialPresentationDelayMinusOne)
}

type av1OBU struct {
	Type    int
	Data    []byte
	Payload []byte
}

func av1ForEachOBU(data []byte, fn func(av1OBU) error) error {
	offset := 0
	for offset < len(data) {
		obu, next, err := readAV1OBU(data, offset)
		if err != nil {
			return err
		}
		if err := fn(obu); err != nil {
			return err
		}
		offset = next
	}
	return nil
}

func readAV1OBU(data []byte, offset int) (av1OBU, int, error) {
	if offset < 0 || offset >= len(data) {
		return av1OBU{}, 0, ErrInvalidData
	}
	start := offset
	header := data[offset]
	if header&0x80 != 0 || header&0x01 != 0 {
		return av1OBU{}, 0, ErrInvalidData
	}
	obuType := int((header >> 3) & 0x0f)
	if obuType == 0 {
		return av1OBU{}, 0, ErrInvalidData
	}
	hasExtension := header&0x04 != 0
	hasSize := header&0x02 != 0
	if !hasSize {
		return av1OBU{}, 0, ErrInvalidData
	}
	offset++
	if hasExtension {
		if offset >= len(data) || data[offset]&0x07 != 0 {
			return av1OBU{}, 0, ErrInvalidData
		}
		offset++
	}
	size, sizeWidth, err := readAV1LEB128(data, offset)
	if err != nil {
		return av1OBU{}, 0, err
	}
	offset += sizeWidth
	if size > uint64(len(data)-offset) {
		return av1OBU{}, 0, ErrInvalidData
	}
	next := offset + int(size)
	return av1OBU{
		Type:    obuType,
		Data:    data[start:next],
		Payload: data[offset:next],
	}, next, nil
}

type av1SequenceHeader struct {
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
}

func parseAV1SequenceHeader(payload []byte) (av1SequenceHeader, error) {
	br := av1BitReader{data: payload}
	seqProfile, err := br.readBits(3)
	if err != nil {
		return av1SequenceHeader{}, err
	}
	if seqProfile > 2 {
		return av1SequenceHeader{}, ErrInvalidData
	}
	if _, err := br.readBool(); err != nil {
		return av1SequenceHeader{}, err
	}
	reducedStillPictureHeader, err := br.readBool()
	if err != nil {
		return av1SequenceHeader{}, err
	}
	sequence := av1SequenceHeader{SeqProfile: int(seqProfile)}
	if reducedStillPictureHeader {
		level, err := br.readBits(5)
		if err != nil {
			return av1SequenceHeader{}, err
		}
		sequence.SeqLevelIdx0 = int(level)
	} else {
		initialDelaySet, initialDelayMinusOne, err := parseAV1OperatingParameters(&br, &sequence)
		if err != nil {
			return av1SequenceHeader{}, err
		}
		sequence.InitialPresentationDelaySet = initialDelaySet
		sequence.InitialPresentationDelayMinusOne = initialDelayMinusOne
	}
	if err := skipAV1SequenceHeaderUntilColorConfig(&br, int(seqProfile), reducedStillPictureHeader); err != nil {
		return av1SequenceHeader{}, err
	}
	if err := parseAV1ColorConfig(&br, &sequence); err != nil {
		return av1SequenceHeader{}, err
	}
	return sequence, nil
}

func parseAV1OperatingParameters(br *av1BitReader, sequence *av1SequenceHeader) (bool, int, error) {
	timingInfoPresent, err := br.readBool()
	if err != nil {
		return false, 0, err
	}
	decoderModelInfoPresent := false
	bufferDelayLength := uint64(0)
	if timingInfoPresent {
		if err := skipAV1TimingInfo(br); err != nil {
			return false, 0, err
		}
		decoderModelInfoPresent, err = br.readBool()
		if err != nil {
			return false, 0, err
		}
		if decoderModelInfoPresent {
			length, err := skipAV1DecoderModelInfo(br)
			if err != nil {
				return false, 0, err
			}
			bufferDelayLength = length
		}
	}
	initialDisplayDelayPresent, err := br.readBool()
	if err != nil {
		return false, 0, err
	}
	opCountMinusOne, err := br.readBits(5)
	if err != nil {
		return false, 0, err
	}
	initialDelaySet := false
	initialDelayMinusOne := 0
	for i := uint64(0); i <= opCountMinusOne; i++ {
		if _, err := br.readBits(12); err != nil {
			return false, 0, err
		}
		level, err := br.readBits(5)
		if err != nil {
			return false, 0, err
		}
		tier := false
		if level > 7 {
			tier, err = br.readBool()
			if err != nil {
				return false, 0, err
			}
		}
		if i == 0 {
			sequence.SeqLevelIdx0 = int(level)
			sequence.SeqTier0 = tier
		}
		if decoderModelInfoPresent {
			present, err := br.readBool()
			if err != nil {
				return false, 0, err
			}
			if present {
				if _, err := br.readBits(int(bufferDelayLength)); err != nil {
					return false, 0, err
				}
				if _, err := br.readBits(int(bufferDelayLength)); err != nil {
					return false, 0, err
				}
				if _, err := br.readBool(); err != nil {
					return false, 0, err
				}
			}
		}
		if initialDisplayDelayPresent {
			present, err := br.readBool()
			if err != nil {
				return false, 0, err
			}
			if present {
				delay, err := br.readBits(4)
				if err != nil {
					return false, 0, err
				}
				if i == 0 {
					initialDelaySet = true
					initialDelayMinusOne = int(delay)
				}
			}
		}
	}
	return initialDelaySet, initialDelayMinusOne, nil
}

func skipAV1TimingInfo(br *av1BitReader) error {
	if _, err := br.readBits(32); err != nil {
		return err
	}
	if _, err := br.readBits(32); err != nil {
		return err
	}
	equalPictureInterval, err := br.readBool()
	if err != nil {
		return err
	}
	if equalPictureInterval {
		if err := br.readUVLC(); err != nil {
			return err
		}
	}
	return nil
}

func skipAV1DecoderModelInfo(br *av1BitReader) (uint64, error) {
	bufferDelayLengthMinusOne, err := br.readBits(5)
	if err != nil {
		return 0, err
	}
	if _, err := br.readBits(32); err != nil {
		return 0, err
	}
	if _, err := br.readBits(5); err != nil {
		return 0, err
	}
	if _, err := br.readBits(5); err != nil {
		return 0, err
	}
	return bufferDelayLengthMinusOne + 1, nil
}

func skipAV1SequenceHeaderUntilColorConfig(br *av1BitReader, seqProfile int, reducedStillPictureHeader bool) error {
	frameWidthBitsMinusOne, err := br.readBits(4)
	if err != nil {
		return err
	}
	frameHeightBitsMinusOne, err := br.readBits(4)
	if err != nil {
		return err
	}
	if _, err := br.readBits(int(frameWidthBitsMinusOne + 1)); err != nil {
		return err
	}
	if _, err := br.readBits(int(frameHeightBitsMinusOne + 1)); err != nil {
		return err
	}
	if !reducedStillPictureHeader {
		frameIDNumbersPresent, err := br.readBool()
		if err != nil {
			return err
		}
		if frameIDNumbersPresent {
			deltaFrameIDLengthMinusTwo, err := br.readBits(4)
			if err != nil {
				return err
			}
			additionalFrameIDLengthMinusOne, err := br.readBits(3)
			if err != nil {
				return err
			}
			if deltaFrameIDLengthMinusTwo+additionalFrameIDLengthMinusOne+3 > 16 {
				return ErrInvalidData
			}
		}
		if err := skipAV1FeatureFlags(br); err != nil {
			return err
		}
	} else if seqProfile > 2 {
		return ErrInvalidData
	}
	if _, err := br.readBool(); err != nil {
		return err
	}
	if _, err := br.readBool(); err != nil {
		return err
	}
	_, err = br.readBool()
	return err
}

func skipAV1FeatureFlags(br *av1BitReader) error {
	for i := 0; i < 7; i++ {
		if _, err := br.readBool(); err != nil {
			return err
		}
	}
	enableOrderHint, err := br.readBool()
	if err != nil {
		return err
	}
	if enableOrderHint {
		if _, err := br.readBool(); err != nil {
			return err
		}
		if _, err := br.readBool(); err != nil {
			return err
		}
	}
	chooseScreenContentTools, err := br.readBool()
	if err != nil {
		return err
	}
	forceScreenContentTools := uint64(2)
	if !chooseScreenContentTools {
		force, err := br.readBits(1)
		if err != nil {
			return err
		}
		forceScreenContentTools = force
	}
	if forceScreenContentTools > 0 {
		chooseIntegerMV, err := br.readBool()
		if err != nil {
			return err
		}
		if !chooseIntegerMV {
			if _, err := br.readBits(1); err != nil {
				return err
			}
		}
	}
	if enableOrderHint {
		if _, err := br.readBits(3); err != nil {
			return err
		}
	}
	return nil
}

func parseAV1ColorConfig(br *av1BitReader, sequence *av1SequenceHeader) error {
	highBitDepth, err := br.readBool()
	if err != nil {
		return err
	}
	sequence.HighBitDepth = highBitDepth
	if sequence.SeqProfile == 2 && highBitDepth {
		twelveBit, err := br.readBool()
		if err != nil {
			return err
		}
		sequence.TwelveBit = twelveBit
	}
	if sequence.SeqProfile != 1 {
		monochrome, err := br.readBool()
		if err != nil {
			return err
		}
		sequence.Monochrome = monochrome
	}
	colorDescriptionPresent, err := br.readBool()
	if err != nil {
		return err
	}
	if colorDescriptionPresent {
		if _, err := br.readBits(8); err != nil {
			return err
		}
		if _, err := br.readBits(8); err != nil {
			return err
		}
		if _, err := br.readBits(8); err != nil {
			return err
		}
	}
	if _, err := br.readBool(); err != nil {
		return err
	}
	if sequence.Monochrome {
		return nil
	}
	switch sequence.SeqProfile {
	case 0:
		sequence.ChromaSubsamplingX = true
		sequence.ChromaSubsamplingY = true
		position, err := br.readBits(2)
		if err != nil {
			return err
		}
		sequence.ChromaSamplePosition = int(position)
	case 1:
	case 2:
		if sequence.TwelveBit {
			x, err := br.readBool()
			if err != nil {
				return err
			}
			sequence.ChromaSubsamplingX = x
			if x {
				y, err := br.readBool()
				if err != nil {
					return err
				}
				sequence.ChromaSubsamplingY = y
			}
		} else {
			sequence.ChromaSubsamplingX = true
		}
		if sequence.ChromaSubsamplingX && sequence.ChromaSubsamplingY {
			position, err := br.readBits(2)
			if err != nil {
				return err
			}
			sequence.ChromaSamplePosition = int(position)
		}
	default:
		return ErrInvalidData
	}
	if sequence.ChromaSamplePosition == 3 {
		return ErrInvalidData
	}
	if _, err := br.readBool(); err != nil {
		return err
	}
	return nil
}

type av1BitReader struct {
	data []byte
	bit  int
}

func (r *av1BitReader) readBool() (bool, error) {
	value, err := r.readBits(1)
	return value != 0, err
}

func (r *av1BitReader) readBits(n int) (uint64, error) {
	if n < 0 || n > 64 || len(r.data)*8-r.bit < n {
		return 0, ErrInvalidData
	}
	var value uint64
	for i := 0; i < n; i++ {
		byteIndex := r.bit >> 3
		shift := 7 - (r.bit & 7)
		value = (value << 1) | uint64((r.data[byteIndex]>>shift)&1)
		r.bit++
	}
	return value, nil
}

func (r *av1BitReader) readUVLC() error {
	leadingZeros := 0
	for {
		bit, err := r.readBits(1)
		if err != nil {
			return err
		}
		if bit == 1 {
			break
		}
		leadingZeros++
		if leadingZeros >= 32 {
			return nil
		}
	}
	_, err := r.readBits(leadingZeros)
	return err
}
