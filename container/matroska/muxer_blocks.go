package matroska

import (
	"encoding/binary"
	"math"

	"github.com/thesyncim/goav/container/ebml"
)

// blockGroupMeta holds the optional sibling elements written after idBlock
// inside a BlockGroup, shared by single-frame and laced BlockGroups.
type blockGroupMeta struct {
	durationTicks        uint64
	keyframe             bool
	referenceBlockTimeNS []int64
	referencePriority    uint64
	discardPaddingNS     int64
	codecState           []byte
	blockAdditions       []BlockAddition
}

func (g blockGroupMeta) empty() bool {
	return g.durationTicks == 0 &&
		len(g.referenceBlockTimeNS) == 0 &&
		g.referencePriority == 0 &&
		g.discardPaddingNS == 0 &&
		len(g.codecState) == 0 &&
		len(g.blockAdditions) == 0
}

func (g blockGroupMeta) implicitReference() bool {
	return g.durationTicks != 0 && !g.keyframe && len(g.referenceBlockTimeNS) == 0
}

func (g blockGroupMeta) groupEncodedSize(blockPayloadSize uint64, scaleNS int64, track Track) (uint64, error) {
	blockHeaderSize, err := elementEncodedSize(idBlock, blockPayloadSize)
	if err != nil {
		return 0, err
	}
	groupSize := blockHeaderSize + blockPayloadSize
	addSize := func(sz uint64, err error) error {
		if err != nil {
			return err
		}
		groupSize, err = checkedAddUint64(groupSize, sz)
		return err
	}
	if len(g.blockAdditions) != 0 {
		if err := addSize(blockAdditionsElementEncodedSize(g.blockAdditions)); err != nil {
			return 0, err
		}
	}
	if g.durationTicks != 0 {
		if err := addSize(uintElementEncodedSize(idBlockDuration, g.durationTicks)); err != nil {
			return 0, err
		}
	}
	if g.referencePriority != 0 {
		if err := addSize(uintElementEncodedSize(idReferencePriority, g.referencePriority)); err != nil {
			return 0, err
		}
	}
	if g.implicitReference() {
		if err := addSize(intElementEncodedSize(idReferenceBlk, 0)); err != nil {
			return 0, err
		}
	}
	for i := range g.referenceBlockTimeNS {
		ticks := scaledReferenceBlockTicks(g.referenceBlockTimeNS[i], scaleNS, track)
		if err := addSize(intElementEncodedSize(idReferenceBlk, ticks)); err != nil {
			return 0, err
		}
	}
	if len(g.codecState) != 0 {
		if err := addSize(binaryElementEncodedSize(idCodecState, g.codecState)); err != nil {
			return 0, err
		}
	}
	if g.discardPaddingNS != 0 {
		if err := addSize(intElementEncodedSize(idDiscardPad, g.discardPaddingNS)); err != nil {
			return 0, err
		}
	}
	return groupSize, nil
}

func (g blockGroupMeta) writeExtra(w *ebml.Writer, scaleNS int64, track Track) error {
	if len(g.blockAdditions) != 0 {
		if err := writeBlockAdditions(w, g.blockAdditions); err != nil {
			return err
		}
	}
	if g.durationTicks != 0 {
		if err := w.WriteUInt(idBlockDuration, g.durationTicks); err != nil {
			return err
		}
	}
	if g.referencePriority != 0 {
		if err := w.WriteUInt(idReferencePriority, g.referencePriority); err != nil {
			return err
		}
	}
	if g.implicitReference() {
		if err := w.WriteInt(idReferenceBlk, 0); err != nil {
			return err
		}
	}
	for i := range g.referenceBlockTimeNS {
		ticks := scaledReferenceBlockTicks(g.referenceBlockTimeNS[i], scaleNS, track)
		if err := w.WriteInt(idReferenceBlk, ticks); err != nil {
			return err
		}
	}
	if len(g.codecState) != 0 {
		if err := writeBinary(w, idCodecState, g.codecState); err != nil {
			return err
		}
	}
	if g.discardPaddingNS != 0 {
		if err := w.WriteInt(idDiscardPad, g.discardPaddingNS); err != nil {
			return err
		}
	}
	return nil
}

func (m *Muxer) writeBlockGroup(packet Packet, blockTimecode int16, track Track) error {
	durationTicks, err := scaledDurationTicks(packet.DurationNS, m.options.TimecodeScaleNS, track)
	if err != nil {
		return err
	}
	meta := blockGroupMeta{
		durationTicks:        durationTicks,
		keyframe:             packet.Keyframe,
		referenceBlockTimeNS: packet.ReferenceBlockTimeNS,
		referencePriority:    packet.ReferencePriority,
		discardPaddingNS:     packet.DiscardPaddingNS,
		codecState:           packet.CodecState,
		blockAdditions:       packet.BlockAdditions,
	}
	if meta.empty() {
		return m.writeBlock(idSimpleBlock, packet, blockTimecode, simpleBlockFlags(packet), track)
	}
	var blockPayload []byte
	var payloadSize int
	if blockPayloadNeedsBuffering(track) {
		blockPayload, err = m.muxedBlockPayload(track, packet.Data, packet.ContentEncryptionPartitions)
		if err != nil {
			return err
		}
		payloadSize = len(blockPayload)
	} else {
		payloadSize, err = blockPayloadSize(track, packet.Data)
		if err != nil {
			return err
		}
	}
	trackWidth, err := ebml.UnsignedVINTWidth(uint64(packet.TrackID))
	if err != nil {
		return err
	}
	blockPayloadSize := uint64(trackWidth + 3 + payloadSize)
	groupSize, err := meta.groupEncodedSize(blockPayloadSize, m.options.TimecodeScaleNS, track)
	if err != nil {
		return err
	}
	if err := m.ebml.WriteHeader(idBlockGroup, groupSize); err != nil {
		return err
	}
	if blockPayload != nil {
		err = m.writeBlockPayload(idBlock, packet.TrackID, blockTimecode, blockFlags(packet), blockPayload)
	} else {
		err = m.writeBlock(idBlock, packet, blockTimecode, blockFlags(packet), track)
	}
	if err != nil {
		return err
	}
	return meta.writeExtra(m.ebml, m.options.TimecodeScaleNS, track)
}

func (m *Muxer) writeLacedBlockGroup(packet LacedPacket, blockTimecode int16, lacing byte, payloadSize uint64, track Track, muxedFrameSizes []int, muxedPayload []byte) error {
	durationTicks, err := scaledLacedBlockDurationTicks(packet.FrameDurationNS, len(packet.Frames), m.options.TimecodeScaleNS, track)
	if err != nil {
		return err
	}
	meta := blockGroupMeta{
		durationTicks:        durationTicks,
		keyframe:             packet.Keyframe,
		referenceBlockTimeNS: packet.ReferenceBlockTimeNS,
		referencePriority:    packet.ReferencePriority,
		discardPaddingNS:     packet.DiscardPaddingNS,
		codecState:           packet.CodecState,
		blockAdditions:       packet.BlockAdditions,
	}
	groupSize, err := meta.groupEncodedSize(payloadSize, m.options.TimecodeScaleNS, track)
	if err != nil {
		return err
	}
	if err := m.ebml.WriteHeader(idBlockGroup, groupSize); err != nil {
		return err
	}
	if err := m.writeLacedBlock(idBlock, packet, blockTimecode, lacedBlockGroupFlags(packet), lacing, payloadSize, track, muxedFrameSizes, muxedPayload); err != nil {
		return err
	}
	return meta.writeExtra(m.ebml, m.options.TimecodeScaleNS, track)
}

func writeBlockAdditions(w *ebml.Writer, additions []BlockAddition) error {
	payloadSize := uint64(0)
	for i := range additions {
		size, err := blockMoreElementEncodedSize(additions[i])
		if err != nil {
			return err
		}
		payloadSize, err = checkedAddUint64(payloadSize, size)
		if err != nil {
			return err
		}
	}
	if err := w.WriteHeader(idBlockAdditions, payloadSize); err != nil {
		return err
	}
	for i := range additions {
		if err := writeBlockMore(w, additions[i]); err != nil {
			return err
		}
	}
	return nil
}

func writeBlockMore(w *ebml.Writer, addition BlockAddition) error {
	payloadSize, err := binaryElementEncodedSize(idBlockAdditional, addition.Data)
	if err != nil {
		return err
	}
	id := blockAdditionID(addition.ID)
	if id != 1 {
		idSize, err := uintElementEncodedSize(idBlockAddID, id)
		if err != nil {
			return err
		}
		payloadSize, err = checkedAddUint64(payloadSize, idSize)
		if err != nil {
			return err
		}
	}
	if err := w.WriteHeader(idBlockMore, payloadSize); err != nil {
		return err
	}
	if id != 1 {
		if err := w.WriteUInt(idBlockAddID, id); err != nil {
			return err
		}
	}
	return writeBinary(w, idBlockAdditional, addition.Data)
}

func (m *Muxer) writeBlock(id ebml.ID, packet Packet, blockTimecode int16, flags byte, track Track) error {
	if len(track.ContentEncodings) != 0 && blockPayloadNeedsBuffering(track) {
		payload, err := m.muxedBlockPayload(track, packet.Data, packet.ContentEncryptionPartitions)
		if err != nil {
			return err
		}
		return m.writeBlockPayload(id, packet.TrackID, blockTimecode, flags, payload)
	}
	directPayload := len(track.ContentEncodings) == 0 && !codecUsesLengthPrefixedSamples(track)
	payloadSize := len(packet.Data)
	if !directPayload {
		var err error
		payloadSize, err = blockPayloadSize(track, packet.Data)
		if err != nil {
			return err
		}
	}
	trackWidth, err := ebml.UnsignedVINTWidth(uint64(packet.TrackID))
	if err != nil {
		return err
	}
	size := uint64(trackWidth + 3 + payloadSize)
	if err := m.ebml.WriteHeader(id, size); err != nil {
		return err
	}
	n, err := ebml.EncodeUnsignedVINT(m.blockScratch[:], uint64(packet.TrackID))
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint16(m.blockScratch[n:n+2], uint16(blockTimecode))
	m.blockScratch[n+2] = flags
	if _, err := m.ebml.Write(m.blockScratch[:n+3]); err != nil {
		return err
	}
	if directPayload {
		_, err := m.ebml.Write(packet.Data)
		return err
	}
	return m.writeBlockData(track, packet.Data)
}

func (m *Muxer) writeBlockPayload(id ebml.ID, trackID uint32, blockTimecode int16, flags byte, payload []byte) error {
	if err := m.writeBlockHeader(id, trackID, blockTimecode, flags, uint64(len(payload))); err != nil {
		return err
	}
	_, err := m.ebml.Write(payload)
	return err
}

func (m *Muxer) writeBlockHeader(id ebml.ID, trackID uint32, blockTimecode int16, flags byte, payloadSize uint64) error {
	trackWidth, err := ebml.UnsignedVINTWidth(uint64(trackID))
	if err != nil {
		return err
	}
	size := uint64(trackWidth+3) + payloadSize
	if err := m.ebml.WriteHeader(id, size); err != nil {
		return err
	}
	n, err := ebml.EncodeUnsignedVINT(m.blockScratch[:], uint64(trackID))
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint16(m.blockScratch[n:n+2], uint16(blockTimecode))
	m.blockScratch[n+2] = flags
	if _, err := m.ebml.Write(m.blockScratch[:n+3]); err != nil {
		return err
	}
	return nil
}

func (m *Muxer) writeBlockData(track Track, data []byte) error {
	if len(track.ContentEncodings) != 0 {
		headerStrip, _, err := blockHeaderStripping(track)
		if err != nil {
			return err
		}
		if len(headerStrip) != 0 {
			data = data[len(headerStrip):]
		}
	}
	if track.Codec == CodecH264 && len(track.CodecPrivate) != 0 {
		return h264WriteMuxedPayload(m.ebml, track, data, &m.blockScratch)
	}
	if track.Codec == CodecH265 && len(track.CodecPrivate) != 0 {
		return h265WriteMuxedPayload(m.ebml, track, data, &m.blockScratch)
	}
	_, err := m.ebml.Write(data)
	return err
}

func (m *Muxer) writeLacedBlock(id ebml.ID, packet LacedPacket, blockTimecode int16, flags byte, lacing byte, payloadSize uint64, track Track, muxedFrameSizes []int, muxedPayload []byte) error {
	if err := m.ebml.WriteHeader(id, payloadSize); err != nil {
		return err
	}
	n, err := ebml.EncodeUnsignedVINT(m.blockScratch[:], uint64(packet.TrackID))
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint16(m.blockScratch[n:n+2], uint16(blockTimecode))
	m.blockScratch[n+2] = flags | lacing
	if _, err := m.ebml.Write(m.blockScratch[:n+3]); err != nil {
		return err
	}
	m.blockScratch[0] = byte(len(packet.Frames) - 1)
	if _, err := m.ebml.Write(m.blockScratch[:1]); err != nil {
		return err
	}
	switch lacing {
	case simpleBlockLacingXiph:
		if err := m.writeXiphLaceSizes(packet.Frames, muxedFrameSizes); err != nil {
			return err
		}
	case simpleBlockLacingEBML:
		if err := m.writeEBMLLaceSizes(packet.Frames, muxedFrameSizes); err != nil {
			return err
		}
	case simpleBlockLacingFixed:
	default:
		return ErrUnsupportedLacing
	}
	directPayload := len(track.ContentEncodings) == 0 && !codecUsesLengthPrefixedSamples(track)
	muxedOffset := 0
	for i := range packet.Frames {
		if directPayload {
			if _, err := m.ebml.Write(packet.Frames[i]); err != nil {
				return err
			}
		} else if len(muxedPayload) != 0 {
			size := muxedFrameSizes[i]
			if size < 0 || muxedOffset > len(muxedPayload)-size {
				return ErrInvalidData
			}
			if _, err := m.ebml.Write(muxedPayload[muxedOffset : muxedOffset+size]); err != nil {
				return err
			}
			muxedOffset += size
		} else if err := m.writeBlockData(track, packet.Frames[i]); err != nil {
			return err
		}
	}
	if len(muxedPayload) != 0 && muxedOffset != len(muxedPayload) {
		return ErrInvalidData
	}
	return nil
}

func (m *Muxer) writeXiphLaceSizes(frames [][]byte, sizes []int) error {
	for i := 0; i < len(frames)-1; i++ {
		size := framePayloadSize(frames, sizes, i)
		for size >= 255 {
			m.blockScratch[0] = 255
			if _, err := m.ebml.Write(m.blockScratch[:1]); err != nil {
				return err
			}
			size -= 255
		}
		m.blockScratch[0] = byte(size)
		if _, err := m.ebml.Write(m.blockScratch[:1]); err != nil {
			return err
		}
	}
	return nil
}

func (m *Muxer) writeEBMLLaceSizes(frames [][]byte, sizes []int) error {
	n, err := ebml.EncodeUnsignedVINT(m.blockScratch[:], uint64(framePayloadSize(frames, sizes, 0)))
	if err != nil {
		return err
	}
	if _, err := m.ebml.Write(m.blockScratch[:n]); err != nil {
		return err
	}
	previous := framePayloadSize(frames, sizes, 0)
	for i := 1; i < len(frames)-1; i++ {
		current := framePayloadSize(frames, sizes, i)
		delta := current - previous
		n, err := encodeSignedLaceSizeVINT(m.blockScratch[:], int64(delta))
		if err != nil {
			return err
		}
		if _, err := m.ebml.Write(m.blockScratch[:n]); err != nil {
			return err
		}
		previous = current
	}
	return nil
}

func simpleBlockFlags(packet Packet) byte {
	flags := byte(0)
	if packet.Keyframe {
		flags |= simpleBlockKeyframe
	}
	if packet.Invisible {
		flags |= simpleBlockInvisible
	}
	if packet.Discardable {
		flags |= simpleBlockDiscardable
	}
	return flags
}

func lacedSimpleBlockFlags(packet LacedPacket) byte {
	flags := byte(0)
	if packet.Keyframe {
		flags |= simpleBlockKeyframe
	}
	if packet.Invisible {
		flags |= simpleBlockInvisible
	}
	if packet.Discardable {
		flags |= simpleBlockDiscardable
	}
	return flags
}

func lacedBlockGroupFlags(packet LacedPacket) byte {
	if packet.Invisible {
		return simpleBlockInvisible
	}
	return 0
}

func blockFlags(packet Packet) byte {
	if packet.Invisible {
		return simpleBlockInvisible
	}
	return 0
}

func scaledDurationTicks(durationNS int64, scaleNS int64, track Track) (uint64, error) {
	if durationNS <= 0 || scaleNS <= 0 {
		return 0, nil
	}
	unit, err := trackTickUnitNS(scaleNS, track)
	if err != nil {
		return 0, err
	}
	value := math.Ceil(float64(durationNS) / unit)
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > float64(math.MaxUint64) {
		return 0, ErrInvalidData
	}
	return uint64(value), nil
}

func scaledLacedBlockDurationTicks(frameDurationNS int64, frameCount int, scaleNS int64, track Track) (uint64, error) {
	if frameDurationNS <= 0 {
		return 0, nil
	}
	if frameCount <= 0 || scaleNS <= 0 {
		return 0, ErrInvalidData
	}
	frameTicks, err := scaledDurationTicks(frameDurationNS, scaleNS, track)
	if err != nil {
		return 0, err
	}
	if frameTicks > math.MaxUint64/uint64(frameCount) {
		return 0, ErrInvalidData
	}
	return frameTicks * uint64(frameCount), nil
}

func validateReferenceBlockTimes(references []int64, scaleNS int64, track Track) error {
	if scaleNS <= 0 {
		return ErrInvalidData
	}
	for i := range references {
		if !trackTickAligned(references[i], scaleNS, track) {
			return ErrInvalidData
		}
		if _, err := checkedScaledReferenceBlockTicks(references[i], scaleNS, track); err != nil {
			return err
		}
	}
	return nil
}

func validateLacedFrameDuration(frameDurationNS int64, scaleNS int64, track Track) error {
	if scaleNS <= 0 {
		return ErrInvalidData
	}
	if frameDurationNS > 0 && !trackTickAligned(frameDurationNS, scaleNS, track) {
		return ErrInvalidData
	}
	return nil
}

func trackTickAligned(value int64, scaleNS int64, track Track) bool {
	ticks, err := checkedScaledReferenceBlockTicks(value, scaleNS, track)
	if err != nil {
		return false
	}
	roundTrip, err := scaleTrackTicksNS(ticks, scaleNS, track)
	return err == nil && roundTrip == value
}

func scaledSegmentDurationTicks(durationNS int64, scaleNS int64) (uint64, error) {
	return scaledDurationTicks(durationNS, scaleNS, Track{})
}

func trackTickUnitNS(scaleNS int64, track Track) (float64, error) {
	if scaleNS <= 0 {
		return 0, ErrInvalidData
	}
	scale := trackTimestampScale(track)
	if scale <= 0 || math.IsNaN(scale) || math.IsInf(scale, 0) {
		return 0, ErrInvalidData
	}
	unit := float64(scaleNS) * scale
	if unit <= 0 || math.IsNaN(unit) || math.IsInf(unit, 0) {
		return 0, ErrInvalidData
	}
	return unit, nil
}

func cueSegmentTimecode(timeNS int64, scaleNS int64) (int64, error) {
	if timeNS < 0 || scaleNS <= 0 {
		return 0, ErrInvalidData
	}
	return timeNS / scaleNS, nil
}

func trackClusterTimecode(timeNS int64, track Track, scaleNS int64) (int64, error) {
	relativeNS, err := subInt64(timeNS, track.TrackOffsetNS)
	if err != nil || scaleNS <= 0 {
		return 0, ErrInvalidData
	}
	if relativeNS <= 0 {
		return 0, nil
	}
	return relativeNS / scaleNS, nil
}

func trackBlockTimecode(timeNS int64, track Track, clusterTimecode int64, scaleNS int64) (int16, error) {
	relativeNS, err := subInt64(timeNS, track.TrackOffsetNS)
	if err != nil {
		return 0, ErrInvalidData
	}
	unit, err := trackTickUnitNS(scaleNS, track)
	if err != nil {
		return 0, err
	}
	segmentTicks := float64(relativeNS) / float64(scaleNS)
	blockTicks := math.Round((segmentTicks - float64(clusterTimecode)) / trackTimestampScale(track))
	if math.IsNaN(blockTicks) || math.IsInf(blockTicks, 0) || blockTicks < math.MinInt16 || blockTicks > math.MaxInt16 {
		return 0, ErrTimecodeOverflow
	}
	if math.Abs(blockTicks*unit) > float64(math.MaxInt64) {
		return 0, ErrInvalidData
	}
	return int16(blockTicks), nil
}

func validateBlockAdditions(additions []BlockAddition) (uint64, error) {
	if len(additions) == 0 {
		return 0, nil
	}
	var maxID uint64
	for i := range additions {
		id := blockAdditionID(additions[i].ID)
		if id == 0 {
			return 0, ErrInvalidData
		}
		if hasBlockAdditionID(additions[:i], id) {
			return 0, ErrInvalidData
		}
		if id > maxID {
			maxID = id
		}
	}
	return maxID, nil
}

func hasBlockAdditionID(additions []BlockAddition, id uint64) bool {
	for i := range additions {
		if blockAdditionID(additions[i].ID) == id {
			return true
		}
	}
	return false
}

func blockAdditionID(id uint64) uint64 {
	if id == 0 {
		return 1
	}
	return id
}

func maxBlockAdditionMappingID(mappings []BlockAdditionMapping) (uint64, error) {
	if len(mappings) == 0 {
		return 0, nil
	}
	var maxID uint64
	for i := range mappings {
		id := mappings[i].IDValue
		if id < 2 {
			return 0, ErrInvalidData
		}
		for j := range i {
			if mappings[j].IDValue == id {
				return 0, ErrInvalidData
			}
		}
		if id > maxID {
			maxID = id
		}
	}
	return maxID, nil
}
