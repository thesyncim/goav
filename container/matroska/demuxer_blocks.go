package matroska

import (
	"bytes"
	"compress/bzip2"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"io"
	"math"

	"github.com/thesyncim/goav/container/ebml"
)

func (d *Demuxer) enterCluster(header ebml.Header) error {
	d.inCluster = true
	d.clusterUnknown = header.Size.Unknown
	if header.Size.Unknown {
		d.clusterEnd = 0
	} else {
		d.clusterEnd = header.DataOffset + int64(header.Size.Value)
	}
	d.clusterTimecode = 0
	return nil
}

func (d *Demuxer) readSimpleBlock(header ebml.Header, dst *Packet) error {
	if header.Size.Unknown {
		return ErrInvalidData
	}
	return d.readBlockPayload(d.reader, header.Size.Value, dst, true)
}

func (d *Demuxer) readBlockGroup(header ebml.Header, dst *Packet) (err error) {
	if header.Size.Unknown {
		return ErrInvalidData
	}
	defer func() {
		if err != nil && !errors.Is(err, ErrPayloadTooSmall) {
			d.clearLace()
		}
	}()
	dst.Reset()
	d.lastLaceFrameIndex = -1
	d.lastLaceFrameCount = 0
	d.lastLaceBaseTimeNS = 0
	d.groupLimit.R = d.reader
	d.groupLimit.N = int64(header.Size.Value)
	d.groupReader.Reset(&d.groupLimit, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize})
	haveBlock := false
	referenceSeen := false
	durationSeen := false
	additionsSeen := false
	prioritySeen := false
	discardSeen := false
	codecStateSeen := false
	durationTicks := uint64(0)
	var payloadErr error
	for d.groupLimit.N > 0 {
		child, err := d.groupReader.ReadHeader()
		if err != nil {
			return err
		}
		switch child.ID {
		case idBlock:
			if err := markElementSeen(&haveBlock); err != nil {
				return err
			}
			if payloadErr != nil {
				if err := skipElement(d.groupReader, child); err != nil {
					return err
				}
				continue
			}
			if err := d.readBlockPayload(d.groupReader, child.Size.Value, dst, false); err != nil {
				if !errors.Is(err, ErrPayloadTooSmall) {
					return err
				}
				payloadErr = err
			}
		case idBlockDuration:
			if err := markElementSeen(&durationSeen); err != nil {
				return err
			}
			value, err := readUIntPayloadScratch(d.groupReader, child.Size.Value, &d.uintScratch)
			if err != nil {
				return err
			}
			durationTicks = value
		case idBlockAdditions:
			if err := markElementSeen(&additionsSeen); err != nil {
				return err
			}
			additions, err := d.parseBlockAdditions(d.groupReader, child)
			if err != nil {
				return err
			}
			dst.BlockAdditions = append(dst.BlockAdditions, additions...)
		case idReferencePriority:
			if err := markElementSeen(&prioritySeen); err != nil {
				return err
			}
			value, err := readUIntPayloadScratch(d.groupReader, child.Size.Value, &d.uintScratch)
			if err != nil {
				return err
			}
			dst.ReferencePriority = value
		case idReferenceBlk:
			ticks, err := readIntPayload(d.groupReader, child.Size.Value)
			if err != nil {
				return err
			}
			dst.ReferenceBlockTimeNS = append(dst.ReferenceBlockTimeNS, ticks)
			referenceSeen = true
		case idDiscardPad:
			if err := markElementSeen(&discardSeen); err != nil {
				return err
			}
			paddingNS, err := readIntPayload(d.groupReader, child.Size.Value)
			if err != nil {
				return err
			}
			dst.DiscardPaddingNS = paddingNS
		case idCodecState:
			if err := markElementSeen(&codecStateSeen); err != nil {
				return err
			}
			state, err := readBinaryPayload(d.groupReader, child.Size.Value)
			if err != nil {
				return err
			}
			dst.CodecState = state
		default:
			if err := skipElement(d.groupReader, child); err != nil {
				return err
			}
		}
	}
	if !haveBlock {
		return ErrInvalidData
	}
	metadataTrackID := dst.TrackID
	if metadataTrackID == 0 && d.laceFrameCount > 0 {
		metadataTrackID = d.laceTrackID
	}
	if metadataTrackID == 0 {
		if payloadErr != nil {
			return payloadErr
		}
		return ErrUnknownTrack
	}
	track, ok := d.track(metadataTrackID)
	if !ok {
		return ErrUnknownTrack
	}
	for i := range dst.ReferenceBlockTimeNS {
		timeNS, err := scaleTrackTicksNS(dst.ReferenceBlockTimeNS[i], d.timecodeScaleNS, track)
		if err != nil {
			return err
		}
		dst.ReferenceBlockTimeNS[i] = timeNS
	}
	if durationTicks != 0 {
		if durationTicks > uint64(math.MaxInt64) {
			return ErrInvalidData
		}
		durationNS, err := scaleTrackTicksNS(int64(durationTicks), d.timecodeScaleNS, track)
		if err != nil {
			return err
		}
		if d.lastLaceFrameCount > 0 {
			durationNS /= int64(d.lastLaceFrameCount)
			d.laceDurationNS = durationNS
			dst.TimeNS = d.lastLaceBaseTimeNS + int64(d.lastLaceFrameIndex)*durationNS
		} else if d.laceFrameCount > 0 {
			durationNS /= int64(d.laceFrameCount)
			d.laceDurationNS = durationNS
		}
		dst.DurationNS = durationNS
	}
	if maxAdditionID, err := validateBlockAdditions(dst.BlockAdditions); err != nil {
		return err
	} else if maxAdditionID != 0 {
		if maxAdditionID > track.MaxBlockAdditionID {
			return ErrInvalidData
		}
	}
	dst.Keyframe = !referenceSeen
	if d.laceFrameCount > 0 {
		d.laceKeyframe = !referenceSeen
		d.captureLaceGroupMetadata(dst)
	}
	if payloadErr != nil {
		return payloadErr
	}
	return nil
}

func (d *Demuxer) captureLaceGroupMetadata(src *Packet) {
	d.laceReferences = append(d.laceReferences[:0], src.ReferenceBlockTimeNS...)
	d.lacePriority = src.ReferencePriority
	d.laceDiscardPadNS = src.DiscardPaddingNS
	d.laceCodecState = append(d.laceCodecState[:0], src.CodecState...)
	d.laceAdditions = clonePacketBlockAdditions(d.laceAdditions, src.BlockAdditions)
}

func clonePacketBlockAdditions(dst []BlockAddition, src []BlockAddition) []BlockAddition {
	if len(src) == 0 {
		return dst[:0]
	}
	dst = append(dst[:0], src...)
	for i := range src {
		if src[i].Data != nil {
			dst[i].Data = append([]byte(nil), src[i].Data...)
		}
	}
	return dst
}

func (d *Demuxer) parseBlockAdditions(parent *ebml.Reader, header ebml.Header) ([]BlockAddition, error) {
	if header.Size.Unknown {
		return nil, ErrInvalidData
	}
	limit := io.LimitedReader{R: parent, N: int64(header.Size.Value)}
	reader := ebml.NewReader(&limit, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize})
	var additions []BlockAddition
	for limit.N > 0 {
		child, err := reader.ReadHeader()
		if err != nil {
			return nil, err
		}
		switch child.ID {
		case idBlockMore:
			addition, err := d.parseBlockMore(reader, child)
			if err != nil {
				return nil, err
			}
			id := blockAdditionID(addition.ID)
			if hasBlockAdditionID(additions, id) {
				return nil, ErrInvalidData
			}
			additions = append(additions, addition)
		default:
			if err := skipElement(reader, child); err != nil {
				return nil, err
			}
		}
	}
	if len(additions) == 0 {
		return nil, ErrInvalidData
	}
	return additions, nil
}

func (d *Demuxer) parseBlockMore(parent *ebml.Reader, header ebml.Header) (BlockAddition, error) {
	if header.Size.Unknown {
		return BlockAddition{}, ErrInvalidData
	}
	limit := io.LimitedReader{R: parent, N: int64(header.Size.Value)}
	reader := ebml.NewReader(&limit, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize})
	addition := BlockAddition{ID: 1}
	idSeen := false
	haveAdditional := false
	for limit.N > 0 {
		child, err := reader.ReadHeader()
		if err != nil {
			return BlockAddition{}, err
		}
		switch child.ID {
		case idBlockAddID:
			if err := markElementSeen(&idSeen); err != nil {
				return BlockAddition{}, err
			}
			id, err := readUIntPayloadScratch(reader, child.Size.Value, &d.uintScratch)
			if err != nil {
				return BlockAddition{}, err
			}
			if id == 0 {
				return BlockAddition{}, ErrInvalidData
			}
			addition.ID = id
		case idBlockAdditional:
			if haveAdditional {
				return BlockAddition{}, ErrInvalidData
			}
			data, err := readBinaryPayload(reader, child.Size.Value)
			if err != nil {
				return BlockAddition{}, err
			}
			addition.Data = data
			haveAdditional = true
		default:
			if err := skipElement(reader, child); err != nil {
				return BlockAddition{}, err
			}
		}
	}
	if !haveAdditional {
		return BlockAddition{}, ErrInvalidData
	}
	return addition, nil
}

func (d *Demuxer) readBlockPayload(r io.Reader, size uint64, dst *Packet, simple bool) error {
	d.blockLimit.R = d.reader
	if r != nil {
		d.blockLimit.R = r
	}
	d.blockLimit.N = int64(size)
	trackID, _, err := ebml.ReadUnsignedVINT(&d.blockLimit, &d.scratch)
	if err != nil {
		return err
	}
	trackNumber, err := trackIDFromUint(trackID)
	if err != nil {
		return err
	}
	track, ok := d.track(trackNumber)
	if !ok {
		return ErrUnknownTrack
	}
	if _, err := io.ReadFull(&d.blockLimit, d.blockHeader[:]); err != nil {
		return err
	}
	flags := d.blockHeader[2]
	lacing := flags & simpleBlockLacingMask
	if lacing != 0 && track.FlagLacingSet && !track.FlagLacing {
		return ErrInvalidData
	}
	if d.blockLimit.N < 0 || d.blockLimit.N > int64(^uint(0)>>1) {
		return ErrInvalidData
	}
	blockTimecode := int16(binary.BigEndian.Uint16(d.blockHeader[:2]))
	timeNS, err := trackBlockTimestampNS(d.clusterTimecode, int64(blockTimecode), d.timecodeScaleNS, track)
	if err != nil || timeNS < 0 {
		return ErrInvalidData
	}
	if lacing != 0 {
		return d.readLacedBlockPayload(track, trackNumber, timeNS, flags, lacing, dst, simple)
	}
	frameSize := int(d.blockLimit.N)
	if len(track.ContentEncodings) != 0 {
		encoding, err := blockContentEncoding(track)
		if err != nil {
			return err
		}
		if encoding.compression == blockContentTransformNone && !encoding.encryptionSet {
			var headerStrip []byte
			if encoding.headerSet {
				headerStrip = encoding.headerSettings
			}
			if err := d.readPlainBlockFrame(track, frameSize, headerStrip, dst); err != nil {
				return err
			}
		} else {
			frame, err := d.readBlockFrameScratch(frameSize)
			if err != nil {
				return err
			}
			if err := d.decodeContentEncodedBlockFrame(track, frame, encoding, dst); err != nil {
				return err
			}
		}
	} else if err := d.readPlainBlockFrame(track, frameSize, nil, dst); err != nil {
		return err
	}
	dst.TrackID = trackNumber
	dst.TimeNS = timeNS
	if simple {
		dst.Keyframe = flags&simpleBlockKeyframe != 0
	}
	dst.Invisible = flags&simpleBlockInvisible != 0
	if simple {
		dst.Discardable = flags&simpleBlockDiscardable != 0
	}
	return nil
}

func (d *Demuxer) readPlainBlockFrame(track Track, frameSize int, headerStrip []byte, dst *Packet) error {
	outSize := frameSize + len(headerStrip)
	if cap(dst.Data) < outSize {
		if err := drainLimited(&d.blockLimit); err != nil {
			return err
		}
		return ErrPayloadTooSmall
	}
	dst.Data = dst.Data[:outSize]
	copy(dst.Data, headerStrip)
	if _, err := io.ReadFull(&d.blockLimit, dst.Data[len(headerStrip):]); err != nil {
		return err
	}
	return d.finishTrackCodecPayload(track, dst)
}

const maxBlockFrameSize = 64 << 20

func (d *Demuxer) readBlockFrameScratch(frameSize int) ([]byte, error) {
	if frameSize < 0 || frameSize > maxBlockFrameSize {
		return nil, ErrInvalidData
	}
	if cap(d.contentBuffer) < frameSize {
		d.contentBuffer = make([]byte, frameSize)
	}
	frame := d.contentBuffer[:frameSize]
	if _, err := io.ReadFull(&d.blockLimit, frame); err != nil {
		return nil, err
	}
	return frame, nil
}

func (d *Demuxer) decodeContentEncodedBlockFrame(track Track, frame []byte, encoding blockContentEncodingInfo, dst *Packet) error {
	payload := frame
	var err error
	if encoding.encryptionSet {
		payload, err = d.decryptBlockPayload(encoding, payload)
		if err != nil {
			return err
		}
	}

	var decoded []byte
	switch encoding.compression {
	case blockContentTransformNone:
		if cap(dst.Data) < len(payload) {
			return ErrPayloadTooSmall
		}
		decoded = dst.Data[:len(payload)]
		copy(decoded, payload)
	case blockContentTransformZlib:
		decoded, err = zlibDecompressInto(dst.Data[:0], payload)
	case blockContentTransformBzlib:
		decoded, err = bzip2DecompressInto(dst.Data[:0], payload)
	default:
		return ErrUnsupportedContentEncoding
	}
	if err != nil {
		return err
	}
	decoded, err = prependContentEncodingHeader(decoded, encoding.headerSettings)
	if err != nil {
		return err
	}
	dst.Data = decoded
	return d.finishTrackCodecPayload(track, dst)
}

func prependContentEncodingHeader(data []byte, header []byte) ([]byte, error) {
	if len(header) == 0 {
		return data, nil
	}
	oldLen := len(data)
	outSize := oldLen + len(header)
	if cap(data) < outSize {
		return nil, ErrPayloadTooSmall
	}
	data = data[:outSize]
	copy(data[len(header):], data[:oldLen])
	copy(data, header)
	return data, nil
}

func (d *Demuxer) finishTrackCodecPayload(track Track, dst *Packet) error {
	if track.Codec == CodecH264 && len(track.CodecPrivate) != 0 {
		lengthSize, ok, err := h264TrackNALULengthSize(track)
		if err != nil {
			return err
		}
		if !ok {
			return ErrInvalidData
		}
		convertedSize, err := h264AVCToAnnexBSize(dst.Data, lengthSize)
		if err != nil {
			return err
		}
		if cap(dst.Data) < convertedSize {
			return ErrPayloadTooSmall
		}
		dst.Data, err = h264AVCToAnnexBInPlace(dst.Data, convertedSize, lengthSize)
		if err != nil {
			return err
		}
	}
	if track.Codec == CodecH265 && len(track.CodecPrivate) != 0 {
		lengthSize, ok, err := h265TrackNALULengthSize(track)
		if err != nil {
			return err
		}
		if !ok {
			return ErrInvalidData
		}
		convertedSize, err := h264AVCToAnnexBSize(dst.Data, lengthSize)
		if err != nil {
			return err
		}
		if cap(dst.Data) < convertedSize {
			return ErrPayloadTooSmall
		}
		dst.Data, err = h264AVCToAnnexBInPlace(dst.Data, convertedSize, lengthSize)
		if err != nil {
			return err
		}
	}
	return nil
}

func zlibDecompressInto(dst []byte, compressed []byte) ([]byte, error) {
	reader, err := zlib.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, ErrInvalidData
	}
	return readCompressedInto(dst, reader, reader.Close)
}

func bzip2DecompressInto(dst []byte, compressed []byte) ([]byte, error) {
	return readCompressedInto(dst, bzip2.NewReader(bytes.NewReader(compressed)), nil)
}

func readCompressedInto(dst []byte, reader io.Reader, close func() error) ([]byte, error) {
	out := dst[:0]
	for {
		if len(out) == cap(out) {
			var probe [1]byte
			n, readErr := reader.Read(probe[:])
			if n > 0 {
				if close != nil {
					_ = close()
				}
				return nil, ErrPayloadTooSmall
			}
			if readErr == io.EOF {
				if close != nil {
					if err := close(); err != nil {
						return nil, ErrInvalidData
					}
				}
				return out, nil
			}
			if readErr != nil {
				if close != nil {
					_ = close()
				}
				return nil, ErrInvalidData
			}
			continue
		}
		n, readErr := reader.Read(out[len(out):cap(out)])
		out = out[:len(out)+n]
		if readErr == io.EOF {
			if close != nil {
				if err := close(); err != nil {
					return nil, ErrInvalidData
				}
			}
			return out, nil
		}
		if readErr != nil {
			if close != nil {
				_ = close()
			}
			return nil, ErrInvalidData
		}
	}
}

func trackIDFromUint(value uint64) (uint32, error) {
	if value == 0 || value > maxTrackID {
		return 0, ErrInvalidData
	}
	return uint32(value), nil
}

func intFromUint(value uint64) (int, error) {
	if value > maxIntValue {
		return 0, ErrInvalidData
	}
	return int(value), nil
}

func readIntPayloadFromUInt(r io.Reader, size uint64) (int, error) {
	value, err := readUIntPayload(r, size)
	if err != nil {
		return 0, err
	}
	return intFromUint(value)
}

func nonZeroIntFromUint(value uint64) (int, error) {
	if value == 0 {
		return 0, ErrInvalidData
	}
	return intFromUint(value)
}

func readChromaticityPayload(r io.Reader, size uint64) (float64, error) {
	value, err := readFloatPayload(r, size)
	if err != nil {
		return 0, err
	}
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 1 {
		return 0, ErrInvalidData
	}
	return value, nil
}

func readNonNegativeFloatPayload(r io.Reader, size uint64) (float64, error) {
	value, err := readFloatPayload(r, size)
	if err != nil {
		return 0, err
	}
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return 0, ErrInvalidData
	}
	return value, nil
}

func readProjectionPosePayload(r io.Reader, size uint64, min float64, max float64) (float64, error) {
	value, err := readFloatPayload(r, size)
	if err != nil {
		return 0, err
	}
	if math.IsNaN(value) || math.IsInf(value, 0) || value < min || value > max {
		return 0, ErrInvalidData
	}
	return value, nil
}

func readNonZeroInt64Payload(r io.Reader, size uint64) (int64, error) {
	value, err := readUIntPayload(r, size)
	if err != nil {
		return 0, err
	}
	if value == 0 || value > uint64(math.MaxInt64) {
		return 0, ErrInvalidData
	}
	return int64(value), nil
}

func drainLimited(r *io.LimitedReader) error {
	if r.N <= 0 {
		return nil
	}
	_, err := io.CopyN(io.Discard, r, r.N)
	return err
}

func (d *Demuxer) readLacedBlockPayload(track Track, trackID uint32, timeNS int64, flags byte, lacing byte, dst *Packet, simple bool) error {
	frameCountByte, err := d.readBlockByte()
	if err != nil {
		return err
	}
	frameCount := int(frameCountByte) + 1
	if frameCount < 2 || frameCount > len(d.laceFrames) {
		return ErrLaceTooLarge
	}
	for i := range frameCount {
		d.laceFrames[i] = laceFrame{}
	}
	switch lacing {
	case simpleBlockLacingXiph:
		if err := d.readXiphLaceSizes(frameCount); err != nil {
			return err
		}
	case simpleBlockLacingFixed:
		if err := d.readFixedLaceSizes(frameCount); err != nil {
			return err
		}
	case simpleBlockLacingEBML:
		if err := d.readEBMLLaceSizes(frameCount); err != nil {
			return err
		}
	default:
		return ErrUnsupportedLacing
	}
	payloadSize := int(d.blockLimit.N)
	if payloadSize < 0 || payloadSize > len(d.laceBuffer) {
		return ErrLaceTooLarge
	}
	if err := d.validateLacePayloadSize(frameCount, payloadSize); err != nil {
		return err
	}
	if _, err := io.ReadFull(&d.blockLimit, d.laceBuffer[:payloadSize]); err != nil {
		return err
	}
	offset := 0
	for i := range frameCount {
		d.laceFrames[i].offset = offset
		offset += d.laceFrames[i].size
	}
	d.laceTrackID = trackID
	d.laceH264Length = 0
	if codecUsesLengthPrefixedSamples(track) {
		lengthSize, ok, err := trackNALULengthSize(track)
		if err != nil {
			d.clearLace()
			return err
		}
		if !ok {
			d.clearLace()
			return ErrInvalidData
		}
		d.laceH264Length = lengthSize
	}
	if len(track.ContentEncodings) != 0 {
		encoding, err := blockContentEncoding(track)
		if err != nil {
			d.clearLace()
			return err
		}
		d.laceContent = encoding
	}
	d.laceTimeNS = timeNS
	d.laceDurationNS = d.defaultDurationNS(trackID)
	d.laceFrameCount = frameCount
	d.laceFrameIndex = 0
	if d.laceSeekFrameIndex != 0 {
		if d.laceSeekFrameIndex < 0 || d.laceSeekFrameIndex >= frameCount {
			d.clearLace()
			return ErrInvalidData
		}
		d.laceFrameIndex = d.laceSeekFrameIndex
		d.laceSeekFrameIndex = 0
	}
	d.laceKeyframe = simple && flags&simpleBlockKeyframe != 0
	d.laceInvisible = flags&simpleBlockInvisible != 0
	d.laceDiscardable = simple && flags&simpleBlockDiscardable != 0
	return d.readNextLacedPacket(dst)
}

func (d *Demuxer) readXiphLaceSizes(frameCount int) error {
	total := 0
	for i := 0; i < frameCount-1; i++ {
		size := 0
		for {
			value, err := d.readBlockByte()
			if err != nil {
				return err
			}
			size += int(value)
			if size > len(d.laceBuffer) {
				return ErrLaceTooLarge
			}
			if value != 255 {
				break
			}
		}
		d.laceFrames[i].size = size
		total += size
	}
	if d.blockLimit.N < int64(total) {
		return ErrInvalidData
	}
	d.laceFrames[frameCount-1].size = int(d.blockLimit.N) - total
	return nil
}

func (d *Demuxer) readFixedLaceSizes(frameCount int) error {
	if d.blockLimit.N < 0 || d.blockLimit.N%int64(frameCount) != 0 {
		return ErrInvalidData
	}
	size := int(d.blockLimit.N) / frameCount
	for i := range frameCount {
		d.laceFrames[i].size = size
	}
	return nil
}

func (d *Demuxer) readEBMLLaceSizes(frameCount int) error {
	first, _, err := ebml.ReadUnsignedVINT(&d.blockLimit, &d.scratch)
	if err != nil {
		return err
	}
	if first > uint64(len(d.laceBuffer)) {
		return ErrLaceTooLarge
	}
	d.laceFrames[0].size = int(first)
	total := int(first)
	previous := int64(first)
	for i := 1; i < frameCount-1; i++ {
		delta, err := d.readSignedLaceVINT()
		if err != nil {
			return err
		}
		size := previous + delta
		if size < 0 || size > int64(len(d.laceBuffer)) {
			return ErrLaceTooLarge
		}
		d.laceFrames[i].size = int(size)
		total += int(size)
		previous = size
	}
	if d.blockLimit.N < int64(total) {
		return ErrInvalidData
	}
	d.laceFrames[frameCount-1].size = int(d.blockLimit.N) - total
	return nil
}

func (d *Demuxer) readSignedLaceVINT() (int64, error) {
	value, width, err := ebml.ReadUnsignedVINT(&d.blockLimit, &d.scratch)
	if err != nil {
		return 0, err
	}
	bias := (uint64(1) << uint(7*width-1)) - 1
	if value > uint64(math.MaxInt64)+bias {
		return 0, ErrInvalidData
	}
	return int64(value) - int64(bias), nil
}

func (d *Demuxer) validateLacePayloadSize(frameCount int, payloadSize int) error {
	total := 0
	for i := range frameCount {
		if d.laceFrames[i].size < 0 {
			return ErrInvalidData
		}
		total += d.laceFrames[i].size
		if total > payloadSize {
			return ErrInvalidData
		}
	}
	if total != payloadSize {
		return ErrInvalidData
	}
	return nil
}

func (d *Demuxer) readBlockByte() (byte, error) {
	if _, err := io.ReadFull(&d.blockLimit, d.scratch[:1]); err != nil {
		return 0, err
	}
	return d.scratch[0], nil
}

func (d *Demuxer) nextLacedPacket(dst *Packet) error {
	if d.laceFrameIndex >= d.laceFrameCount {
		return ErrInvalidData
	}
	frame := d.laceFrames[d.laceFrameIndex]
	frameData := d.laceBuffer[frame.offset : frame.offset+frame.size]
	if d.laceContent.encryptionSet ||
		d.laceContent.compression == blockContentTransformZlib ||
		d.laceContent.compression == blockContentTransformBzlib {
		track, ok := d.track(d.laceTrackID)
		if !ok {
			return ErrUnknownTrack
		}
		if err := d.decodeLacedContentEncodedBlockFrame(track, frameData, d.laceContent, dst); err != nil {
			return err
		}
	} else {
		var headerStrip []byte
		if d.laceContent.headerSet {
			headerStrip = d.laceContent.headerSettings
		}
		outSize := frame.size + len(headerStrip)
		if d.laceH264Length != 0 {
			size, err := h264AVCToAnnexBSize(frameData, d.laceH264Length)
			if err != nil {
				return err
			}
			outSize = size
		}
		if cap(dst.Data) < outSize {
			return ErrPayloadTooSmall
		}
		if d.laceH264Length != 0 {
			var err error
			dst.Data = dst.Data[:frame.size]
			copy(dst.Data, frameData)
			dst.Data, err = h264AVCToAnnexBInPlace(dst.Data, outSize, d.laceH264Length)
			if err != nil {
				return err
			}
		} else {
			dst.Data = dst.Data[:outSize]
			if len(headerStrip) != 0 {
				copy(dst.Data, headerStrip)
			}
			copy(dst.Data[len(headerStrip):], frameData)
		}
	}
	dst.TrackID = d.laceTrackID
	dst.TimeNS = d.laceTimeNS
	if d.laceDurationNS > 0 {
		dst.TimeNS += int64(d.laceFrameIndex) * d.laceDurationNS
		dst.DurationNS = d.laceDurationNS
	}
	dst.Keyframe = d.laceKeyframe
	dst.Invisible = d.laceInvisible
	dst.Discardable = d.laceDiscardable
	d.applyLaceGroupMetadata(dst)
	if d.laceFrameIndex == 0 {
		d.applyPendingClusterUnknownElements(dst)
	}
	d.lastLaceBaseTimeNS = d.laceTimeNS
	d.lastLaceFrameIndex = d.laceFrameIndex
	d.lastLaceFrameCount = d.laceFrameCount
	d.laceFrameIndex++
	if d.laceFrameIndex >= d.laceFrameCount {
		d.clearLace()
	}
	return nil
}

func (d *Demuxer) readNextLacedPacket(dst *Packet) error {
	err := d.nextLacedPacket(dst)
	if err != nil && !errors.Is(err, ErrPayloadTooSmall) && d.laceFrameIndex == 0 {
		d.pendingClusterUnknown = d.pendingClusterUnknown[:0]
	}
	return err
}

func (d *Demuxer) decodeLacedContentEncodedBlockFrame(track Track, frame []byte, encoding blockContentEncodingInfo, dst *Packet) error {
	payload := frame
	if encoding.encryptionSet {
		if cap(d.contentBuffer) < len(frame) {
			d.contentBuffer = make([]byte, len(frame))
		}
		payload = d.contentBuffer[:len(frame)]
		copy(payload, frame)
	}
	return d.decodeContentEncodedBlockFrame(track, payload, encoding, dst)
}

func (d *Demuxer) applyLaceGroupMetadata(dst *Packet) {
	if len(d.laceReferences) != 0 {
		dst.ReferenceBlockTimeNS = append(dst.ReferenceBlockTimeNS, d.laceReferences...)
	}
	dst.ReferencePriority = d.lacePriority
	dst.DiscardPaddingNS = d.laceDiscardPadNS
	if len(d.laceCodecState) != 0 {
		dst.CodecState = append(dst.CodecState, d.laceCodecState...)
	}
	if len(d.laceAdditions) != 0 {
		dst.BlockAdditions = clonePacketBlockAdditions(dst.BlockAdditions, d.laceAdditions)
	}
}

func (d *Demuxer) clearLace() {
	d.laceTrackID = 0
	d.laceH264Length = 0
	d.laceContent = blockContentEncodingInfo{}
	d.laceTimeNS = 0
	d.laceDurationNS = 0
	d.laceReferences = d.laceReferences[:0]
	d.lacePriority = 0
	d.laceDiscardPadNS = 0
	d.laceCodecState = d.laceCodecState[:0]
	d.laceAdditions = d.laceAdditions[:0]
	d.laceFrameCount = 0
	d.laceFrameIndex = 0
	d.laceSeekFrameIndex = 0
	d.laceKeyframe = false
	d.laceInvisible = false
	d.laceDiscardable = false
}
