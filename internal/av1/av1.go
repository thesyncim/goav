package av1

import "github.com/thesyncim/goav/internal/bitio"

func ConfigFromFrames(frames [][]byte) ([]byte, error) {
	var sequenceOBU []byte
	var sequence av1SequenceHeader
	for i := range frames {
		if err := av1ForEachOBU(frames[i], func(obu av1OBU) error {
			if obu.Type != av1OBUSequenceHeader {
				return nil
			}
			if sequenceOBU != nil {
				return ErrInvalid
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
		return nil, ErrInvalid
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
		return av1OBU{}, 0, ErrInvalid
	}
	start := offset
	header := data[offset]
	if header&0x80 != 0 || header&0x01 != 0 {
		return av1OBU{}, 0, ErrInvalid
	}
	obuType := int((header >> 3) & 0x0f)
	if obuType == 0 {
		return av1OBU{}, 0, ErrInvalid
	}
	hasExtension := header&0x04 != 0
	hasSize := header&0x02 != 0
	if !hasSize {
		return av1OBU{}, 0, ErrInvalid
	}
	offset++
	if hasExtension {
		if offset >= len(data) || data[offset]&0x07 != 0 {
			return av1OBU{}, 0, ErrInvalid
		}
		offset++
	}
	size, sizeWidth, err := readAV1LEB128(data, offset)
	if err != nil {
		return av1OBU{}, 0, err
	}
	offset += sizeWidth
	if size > uint64(len(data)-offset) {
		return av1OBU{}, 0, ErrInvalid
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
	br := bitio.Reader{Data: payload, Err: ErrInvalid}
	seqProfile, err := br.Bits(3)
	if err != nil {
		return av1SequenceHeader{}, err
	}
	if seqProfile > 2 {
		return av1SequenceHeader{}, ErrInvalid
	}
	if _, err := br.Bool(); err != nil {
		return av1SequenceHeader{}, err
	}
	reducedStillPictureHeader, err := br.Bool()
	if err != nil {
		return av1SequenceHeader{}, err
	}
	sequence := av1SequenceHeader{SeqProfile: int(seqProfile)}
	if reducedStillPictureHeader {
		level, err := br.Bits(5)
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

func parseAV1OperatingParameters(br *bitio.Reader, sequence *av1SequenceHeader) (bool, int, error) {
	timingInfoPresent, err := br.Bool()
	if err != nil {
		return false, 0, err
	}
	decoderModelInfoPresent := false
	bufferDelayLength := uint64(0)
	if timingInfoPresent {
		if err := skipAV1TimingInfo(br); err != nil {
			return false, 0, err
		}
		decoderModelInfoPresent, err = br.Bool()
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
	initialDisplayDelayPresent, err := br.Bool()
	if err != nil {
		return false, 0, err
	}
	opCountMinusOne, err := br.Bits(5)
	if err != nil {
		return false, 0, err
	}
	initialDelaySet := false
	initialDelayMinusOne := 0
	for i := uint64(0); i <= opCountMinusOne; i++ {
		if _, err := br.Bits(12); err != nil {
			return false, 0, err
		}
		level, err := br.Bits(5)
		if err != nil {
			return false, 0, err
		}
		tier := false
		if level > 7 {
			tier, err = br.Bool()
			if err != nil {
				return false, 0, err
			}
		}
		if i == 0 {
			sequence.SeqLevelIdx0 = int(level)
			sequence.SeqTier0 = tier
		}
		if decoderModelInfoPresent {
			present, err := br.Bool()
			if err != nil {
				return false, 0, err
			}
			if present {
				if _, err := br.Bits(int(bufferDelayLength)); err != nil {
					return false, 0, err
				}
				if _, err := br.Bits(int(bufferDelayLength)); err != nil {
					return false, 0, err
				}
				if _, err := br.Bool(); err != nil {
					return false, 0, err
				}
			}
		}
		if initialDisplayDelayPresent {
			present, err := br.Bool()
			if err != nil {
				return false, 0, err
			}
			if present {
				delay, err := br.Bits(4)
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

func skipAV1TimingInfo(br *bitio.Reader) error {
	if _, err := br.Bits(32); err != nil {
		return err
	}
	if _, err := br.Bits(32); err != nil {
		return err
	}
	equalPictureInterval, err := br.Bool()
	if err != nil {
		return err
	}
	if equalPictureInterval {
		if err := br.SkipUVLC(); err != nil {
			return err
		}
	}
	return nil
}

func skipAV1DecoderModelInfo(br *bitio.Reader) (uint64, error) {
	bufferDelayLengthMinusOne, err := br.Bits(5)
	if err != nil {
		return 0, err
	}
	if _, err := br.Bits(32); err != nil {
		return 0, err
	}
	if _, err := br.Bits(5); err != nil {
		return 0, err
	}
	if _, err := br.Bits(5); err != nil {
		return 0, err
	}
	return bufferDelayLengthMinusOne + 1, nil
}

func skipAV1SequenceHeaderUntilColorConfig(br *bitio.Reader, seqProfile int, reducedStillPictureHeader bool) error {
	frameWidthBitsMinusOne, err := br.Bits(4)
	if err != nil {
		return err
	}
	frameHeightBitsMinusOne, err := br.Bits(4)
	if err != nil {
		return err
	}
	if _, err := br.Bits(int(frameWidthBitsMinusOne + 1)); err != nil {
		return err
	}
	if _, err := br.Bits(int(frameHeightBitsMinusOne + 1)); err != nil {
		return err
	}
	if !reducedStillPictureHeader {
		frameIDNumbersPresent, err := br.Bool()
		if err != nil {
			return err
		}
		if frameIDNumbersPresent {
			deltaFrameIDLengthMinusTwo, err := br.Bits(4)
			if err != nil {
				return err
			}
			additionalFrameIDLengthMinusOne, err := br.Bits(3)
			if err != nil {
				return err
			}
			if deltaFrameIDLengthMinusTwo+additionalFrameIDLengthMinusOne+3 > 16 {
				return ErrInvalid
			}
		}
		if err := skipAV1FeatureFlags(br); err != nil {
			return err
		}
	} else if seqProfile > 2 {
		return ErrInvalid
	}
	if _, err := br.Bool(); err != nil {
		return err
	}
	if _, err := br.Bool(); err != nil {
		return err
	}
	_, err = br.Bool()
	return err
}

func skipAV1FeatureFlags(br *bitio.Reader) error {
	for range 7 {
		if _, err := br.Bool(); err != nil {
			return err
		}
	}
	enableOrderHint, err := br.Bool()
	if err != nil {
		return err
	}
	if enableOrderHint {
		if _, err := br.Bool(); err != nil {
			return err
		}
		if _, err := br.Bool(); err != nil {
			return err
		}
	}
	chooseScreenContentTools, err := br.Bool()
	if err != nil {
		return err
	}
	forceScreenContentTools := uint64(2)
	if !chooseScreenContentTools {
		force, err := br.Bits(1)
		if err != nil {
			return err
		}
		forceScreenContentTools = force
	}
	if forceScreenContentTools > 0 {
		chooseIntegerMV, err := br.Bool()
		if err != nil {
			return err
		}
		if !chooseIntegerMV {
			if _, err := br.Bits(1); err != nil {
				return err
			}
		}
	}
	if enableOrderHint {
		if _, err := br.Bits(3); err != nil {
			return err
		}
	}
	return nil
}

func parseAV1ColorConfig(br *bitio.Reader, sequence *av1SequenceHeader) error {
	highBitDepth, err := br.Bool()
	if err != nil {
		return err
	}
	sequence.HighBitDepth = highBitDepth
	if sequence.SeqProfile == 2 && highBitDepth {
		twelveBit, err := br.Bool()
		if err != nil {
			return err
		}
		sequence.TwelveBit = twelveBit
	}
	if sequence.SeqProfile != 1 {
		monochrome, err := br.Bool()
		if err != nil {
			return err
		}
		sequence.Monochrome = monochrome
	}
	colorDescriptionPresent, err := br.Bool()
	if err != nil {
		return err
	}
	if colorDescriptionPresent {
		if _, err := br.Bits(8); err != nil {
			return err
		}
		if _, err := br.Bits(8); err != nil {
			return err
		}
		if _, err := br.Bits(8); err != nil {
			return err
		}
	}
	if _, err := br.Bool(); err != nil {
		return err
	}
	if sequence.Monochrome {
		return nil
	}
	switch sequence.SeqProfile {
	case 0:
		sequence.ChromaSubsamplingX = true
		sequence.ChromaSubsamplingY = true
		position, err := br.Bits(2)
		if err != nil {
			return err
		}
		sequence.ChromaSamplePosition = int(position)
	case 1:
	case 2:
		if sequence.TwelveBit {
			x, err := br.Bool()
			if err != nil {
				return err
			}
			sequence.ChromaSubsamplingX = x
			if x {
				y, err := br.Bool()
				if err != nil {
					return err
				}
				sequence.ChromaSubsamplingY = y
			}
		} else {
			sequence.ChromaSubsamplingX = true
		}
		if sequence.ChromaSubsamplingX && sequence.ChromaSubsamplingY {
			position, err := br.Bits(2)
			if err != nil {
				return err
			}
			sequence.ChromaSamplePosition = int(position)
		}
	default:
		return ErrInvalid
	}
	if sequence.ChromaSamplePosition == 3 {
		return ErrInvalid
	}
	if _, err := br.Bool(); err != nil {
		return err
	}
	return nil
}
