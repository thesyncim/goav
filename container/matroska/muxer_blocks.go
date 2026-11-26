package matroska

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"io"
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

func scaledReferenceBlockTicks(timeNS int64, scaleNS int64, track Track) int64 {
	ticks, _ := checkedScaledReferenceBlockTicks(timeNS, scaleNS, track)
	return ticks
}

func checkedScaledReferenceBlockTicks(timeNS int64, scaleNS int64, track Track) (int64, error) {
	unit, err := trackTickUnitNS(scaleNS, track)
	if err != nil {
		return 0, err
	}
	value := math.Round(float64(timeNS) / unit)
	if math.IsNaN(value) || math.IsInf(value, 0) || value < float64(math.MinInt64) || value > float64(math.MaxInt64) {
		return 0, ErrInvalidData
	}
	return int64(value), nil
}

func scaleTrackTicksNS(ticks int64, scaleNS int64, track Track) (int64, error) {
	unit, err := trackTickUnitNS(scaleNS, track)
	if err != nil {
		return 0, err
	}
	value := math.Round(float64(ticks) * unit)
	if math.IsNaN(value) || math.IsInf(value, 0) || value < float64(math.MinInt64) || value > float64(math.MaxInt64) {
		return 0, ErrInvalidData
	}
	return int64(value), nil
}

func trackBlockTimestampNS(clusterTimecode int64, blockTimecode int64, scaleNS int64, track Track) (int64, error) {
	if scaleNS <= 0 {
		return 0, ErrInvalidData
	}
	scale := trackTimestampScale(track)
	if scale <= 0 || math.IsNaN(scale) || math.IsInf(scale, 0) {
		return 0, ErrInvalidData
	}
	value := math.Round((float64(clusterTimecode) + float64(blockTimecode)*scale) * float64(scaleNS))
	if math.IsNaN(value) || math.IsInf(value, 0) || value < float64(math.MinInt64) || value > float64(math.MaxInt64) {
		return 0, ErrInvalidData
	}
	return addInt64(int64(value), track.TrackOffsetNS)
}

func addInt64(left int64, right int64) (int64, error) {
	if (right > 0 && left > math.MaxInt64-right) || (right < 0 && left < math.MinInt64-right) {
		return 0, ErrInvalidData
	}
	return left + right, nil
}

func subInt64(left int64, right int64) (int64, error) {
	if (right > 0 && left < math.MinInt64+right) || (right < 0 && left > math.MaxInt64+right) {
		return 0, ErrInvalidData
	}
	return left - right, nil
}

func lacingFlag(mode LacingMode, frames [][]byte, sizes []int) (byte, error) {
	if len(frames) < 2 || len(frames) > 256 {
		return 0, ErrInvalidData
	}
	switch mode {
	case LacingAuto:
		if equalFrameSizes(frames, sizes) {
			return simpleBlockLacingFixed, nil
		}
		return simpleBlockLacingEBML, nil
	case LacingXiph:
		return simpleBlockLacingXiph, nil
	case LacingEBML:
		return simpleBlockLacingEBML, nil
	case LacingFixed:
		if !equalFrameSizes(frames, sizes) {
			return 0, ErrInvalidData
		}
		return simpleBlockLacingFixed, nil
	default:
		return 0, ErrUnsupportedLacing
	}
}

func equalFrameSizes(frames [][]byte, sizes []int) bool {
	if len(frames) == 0 {
		return true
	}
	size := framePayloadSize(frames, sizes, 0)
	for i := 1; i < len(frames); i++ {
		if framePayloadSize(frames, sizes, i) != size {
			return false
		}
	}
	return true
}

func framePayloadSize(frames [][]byte, sizes []int, index int) int {
	if len(sizes) != 0 {
		return sizes[index]
	}
	return len(frames[index])
}

func (m *Muxer) lacedPacketEndTime(packet LacedPacket) (int64, error) {
	end := packet.TimeNS
	durationNS := packet.FrameDurationNS
	if durationNS <= 0 {
		durationNS = m.defaultDurationNS(packet.TrackID)
	}
	if durationNS <= 0 {
		return end, nil
	}
	if int64(len(packet.Frames)) > math.MaxInt64/durationNS {
		return 0, ErrInvalidData
	}
	totalDuration := int64(len(packet.Frames)) * durationNS
	if packet.TimeNS > math.MaxInt64-totalDuration {
		return 0, ErrInvalidData
	}
	return packet.TimeNS + totalDuration, nil
}

func lacedPacketNeedsBlockGroup(packet LacedPacket) bool {
	return packet.FrameDurationNS > 0 ||
		len(packet.ReferenceBlockTimeNS) != 0 ||
		packet.ReferencePriority != 0 ||
		packet.DiscardPaddingNS != 0 ||
		len(packet.CodecState) != 0 ||
		len(packet.BlockAdditions) != 0
}

func blockPayloadFrameSizes(frames [][]byte, track Track, sizes []int) error {
	if len(sizes) != len(frames) {
		return ErrInvalidData
	}
	for i := range frames {
		size, err := blockPayloadSize(track, frames[i])
		if err != nil {
			return err
		}
		sizes[i] = size
	}
	return nil
}

func blockPayloadsNeedSizing(track Track) bool {
	if codecUsesLengthPrefixedSamples(track) {
		return true
	}
	if len(track.ContentEncodings) == 0 {
		return false
	}
	encoding, err := blockContentEncoding(track)
	return err != nil || encoding.headerSet || encoding.compression != blockContentTransformNone || encoding.encryptionSet
}

func blockPayloadNeedsBuffering(track Track) bool {
	if len(track.ContentEncodings) == 0 {
		return false
	}
	encoding, err := blockContentEncoding(track)
	return err != nil || encoding.compression != blockContentTransformNone || encoding.encryptionSet
}

func blockPayloadSize(track Track, data []byte) (int, error) {
	if len(track.ContentEncodings) != 0 {
		encoding, err := blockContentEncoding(track)
		if err != nil {
			return 0, err
		}
		if encoding.headerSet {
			if !bytes.HasPrefix(data, encoding.headerSettings) {
				return 0, ErrInvalidData
			}
			data = data[len(encoding.headerSettings):]
		}
		switch encoding.compression {
		case blockContentTransformNone:
			if encoding.encryptionSet {
				return 0, ErrInvalidData
			}
		case blockContentTransformZlib:
			return 0, ErrInvalidData
		case blockContentTransformBzlib:
			return 0, ErrBzip2ContentEncodingWrite
		default:
			return 0, ErrUnsupportedContentEncoding
		}
	}
	if track.Codec == CodecH264 && len(track.CodecPrivate) != 0 {
		size, _, err := h264MuxedPayloadSize(track, data)
		return size, err
	}
	if track.Codec == CodecH265 && len(track.CodecPrivate) != 0 {
		size, _, err := h265MuxedPayloadSize(track, data)
		return size, err
	}
	return len(data), nil
}

func (m *Muxer) muxedBlockPayload(track Track, data []byte, partitions []uint32) ([]byte, error) {
	if len(track.ContentEncodings) == 0 {
		if len(partitions) != 0 {
			return nil, ErrInvalidData
		}
		return m.codecMuxedBlockPayload(track, data)
	}
	encoding, err := blockContentEncoding(track)
	if err != nil {
		return nil, err
	}
	if len(partitions) != 0 && !encoding.encryptionSet {
		return nil, ErrInvalidData
	}
	if encoding.headerSet {
		if !bytes.HasPrefix(data, encoding.headerSettings) {
			return nil, ErrInvalidData
		}
		data = data[len(encoding.headerSettings):]
	}
	payload, err := m.codecMuxedBlockPayload(track, data)
	if err != nil {
		return nil, err
	}
	switch encoding.compression {
	case blockContentTransformNone:
	case blockContentTransformZlib:
		payload, err = m.zlibCompressBlockPayload(payload)
	case blockContentTransformBzlib:
		return nil, ErrBzip2ContentEncodingWrite
	default:
		return nil, ErrUnsupportedContentEncoding
	}
	if err != nil {
		return nil, err
	}
	if encoding.encryptionSet {
		return m.encryptBlockPayload(encoding, payload, partitions)
	}
	return payload, nil
}

func (m *Muxer) codecMuxedBlockPayload(track Track, data []byte) ([]byte, error) {
	if !codecUsesLengthPrefixedSamples(track) {
		return data, nil
	}
	m.codecPayload.Reset()
	switch track.Codec {
	case CodecH264:
		if err := h264WriteMuxedPayload(&m.codecPayload, track, data, &m.blockScratch); err != nil {
			return nil, err
		}
	case CodecH265:
		if err := h265WriteMuxedPayload(&m.codecPayload, track, data, &m.blockScratch); err != nil {
			return nil, err
		}
	default:
		return nil, ErrInvalidData
	}
	return m.codecPayload.Bytes(), nil
}

func codecUsesLengthPrefixedSamples(track Track) bool {
	return len(track.CodecPrivate) != 0 && (track.Codec == CodecH264 || track.Codec == CodecH265)
}

func (m *Muxer) zlibCompressBlockPayload(data []byte) ([]byte, error) {
	m.blockPayload.Reset()
	if m.zlibWriter == nil {
		m.zlibWriter = zlib.NewWriter(&m.blockPayload)
	} else {
		m.zlibWriter.Reset(&m.blockPayload)
	}
	if _, err := m.zlibWriter.Write(data); err != nil {
		_ = m.zlibWriter.Close()
		return nil, err
	}
	if err := m.zlibWriter.Close(); err != nil {
		return nil, err
	}
	return m.blockPayload.Bytes(), nil
}

func (m *Muxer) prepareLacedBlockPayload(frames [][]byte, track Track, sizes []int) ([]byte, error) {
	if len(sizes) != len(frames) {
		return nil, ErrInvalidData
	}
	if _, err := blockContentEncoding(track); err != nil {
		return nil, err
	}
	m.lacePayload = m.lacePayload[:0]
	for i := range frames {
		payload, err := m.muxedBlockPayload(track, frames[i], nil)
		if err != nil {
			return nil, err
		}
		sizes[i] = len(payload)
		m.lacePayload = append(m.lacePayload, payload...)
	}
	return m.lacePayload, nil
}

func (m *Muxer) lacedBlockPayloadSize(packet LacedPacket, lacing byte, muxedFrameSizes []int) (uint64, error) {
	trackWidth, err := ebml.UnsignedVINTWidth(uint64(packet.TrackID))
	if err != nil {
		return 0, err
	}
	size := uint64(trackWidth + 3 + 1)
	laceSize, err := laceSizePayloadWidth(packet.Frames, muxedFrameSizes, lacing)
	if err != nil {
		return 0, err
	}
	size, err = checkedAddUint64(size, laceSize)
	if err != nil {
		return 0, err
	}
	for i := range packet.Frames {
		size, err = checkedAddUint64(size, uint64(framePayloadSize(packet.Frames, muxedFrameSizes, i)))
		if err != nil {
			return 0, err
		}
	}
	return size, nil
}

func laceSizePayloadWidth(frames [][]byte, sizes []int, lacing byte) (uint64, error) {
	switch lacing {
	case simpleBlockLacingXiph:
		var size uint64
		for i := 0; i < len(frames)-1; i++ {
			size += uint64(framePayloadSize(frames, sizes, i)/255 + 1)
		}
		return size, nil
	case simpleBlockLacingFixed:
		if !equalFrameSizes(frames, sizes) {
			return 0, ErrInvalidData
		}
		return 0, nil
	case simpleBlockLacingEBML:
		width, err := ebml.UnsignedVINTWidth(uint64(framePayloadSize(frames, sizes, 0)))
		if err != nil {
			return 0, err
		}
		size := uint64(width)
		previous := framePayloadSize(frames, sizes, 0)
		for i := 1; i < len(frames)-1; i++ {
			current := framePayloadSize(frames, sizes, i)
			delta := current - previous
			width, err := signedLaceSizeVINTWidth(int64(delta))
			if err != nil {
				return 0, err
			}
			size += uint64(width)
			previous = current
		}
		return size, nil
	default:
		return 0, ErrUnsupportedLacing
	}
}

func signedLaceSizeVINTWidth(value int64) (int, error) {
	for width := 1; width <= ebml.MaxSizeWidth; width++ {
		bias := int64((uint64(1) << uint(7*width-1)) - 1)
		if value > math.MaxInt64-bias {
			continue
		}
		encoded := value + bias
		if encoded < 0 {
			continue
		}
		if uint64(encoded) <= (uint64(1)<<uint(7*width))-2 {
			return width, nil
		}
	}
	return 0, ErrInvalidData
}

func encodeSignedLaceSizeVINT(dst []byte, value int64) (int, error) {
	width, err := signedLaceSizeVINTWidth(value)
	if err != nil {
		return 0, err
	}
	bias := int64((uint64(1) << uint(7*width-1)) - 1)
	return ebml.EncodeUnsignedVINTWidth(dst, uint64(value+bias), width)
}

func checkedAddUint64(left uint64, right uint64) (uint64, error) {
	if left > ^uint64(0)-right {
		return 0, ErrInvalidData
	}
	return left + right, nil
}

func elementEncodedSize(id ebml.ID, payloadSize uint64) (uint64, error) {
	idWidth, err := ebml.IDWidth(id)
	if err != nil {
		return 0, err
	}
	sizeWidth, err := ebml.SizeVINTWidth(payloadSize)
	if err != nil {
		return 0, err
	}
	return uint64(idWidth + sizeWidth), nil
}

func uintElementEncodedSize(id ebml.ID, value uint64) (uint64, error) {
	payloadSize := uint64(uintPayloadWidthLocal(value))
	headerSize, err := elementEncodedSize(id, payloadSize)
	if err != nil {
		return 0, err
	}
	return headerSize + payloadSize, nil
}

func intElementEncodedSize(id ebml.ID, value int64) (uint64, error) {
	payloadSize := uint64(intPayloadWidthLocal(value))
	headerSize, err := elementEncodedSize(id, payloadSize)
	if err != nil {
		return 0, err
	}
	return headerSize + payloadSize, nil
}

func binaryElementEncodedSize(id ebml.ID, value []byte) (uint64, error) {
	payloadSize := uint64(len(value))
	headerSize, err := elementEncodedSize(id, payloadSize)
	if err != nil {
		return 0, err
	}
	return headerSize + payloadSize, nil
}

func blockAdditionsElementEncodedSize(additions []BlockAddition) (uint64, error) {
	var payloadSize uint64
	for i := range additions {
		size, err := blockMoreElementEncodedSize(additions[i])
		if err != nil {
			return 0, err
		}
		payloadSize, err = checkedAddUint64(payloadSize, size)
		if err != nil {
			return 0, err
		}
	}
	headerSize, err := elementEncodedSize(idBlockAdditions, payloadSize)
	if err != nil {
		return 0, err
	}
	return headerSize + payloadSize, nil
}

func blockMoreElementEncodedSize(addition BlockAddition) (uint64, error) {
	payloadSize, err := binaryElementEncodedSize(idBlockAdditional, addition.Data)
	if err != nil {
		return 0, err
	}
	id := blockAdditionID(addition.ID)
	if id != 1 {
		idSize, err := uintElementEncodedSize(idBlockAddID, id)
		if err != nil {
			return 0, err
		}
		payloadSize, err = checkedAddUint64(payloadSize, idSize)
		if err != nil {
			return 0, err
		}
	}
	headerSize, err := elementEncodedSize(idBlockMore, payloadSize)
	if err != nil {
		return 0, err
	}
	return headerSize + payloadSize, nil
}

func uintPayloadWidthLocal(value uint64) int {
	switch {
	case value <= 0xff:
		return 1
	case value <= 0xffff:
		return 2
	case value <= 0xffffff:
		return 3
	case value <= 0xffffffff:
		return 4
	case value <= 0xffffffffff:
		return 5
	case value <= 0xffffffffffff:
		return 6
	case value <= 0xffffffffffffff:
		return 7
	default:
		return 8
	}
}

func intPayloadWidthLocal(value int64) int {
	for width := 1; width < 8; width++ {
		shift := uint(width * 8)
		min := int64(-1) << (shift - 1)
		max := int64(1)<<(shift-1) - 1
		if value >= min && value <= max {
			return width
		}
	}
	return 8
}

func writeFull(writer io.Writer, payload []byte) error {
	for len(payload) != 0 {
		n, err := writer.Write(payload)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		payload = payload[n:]
	}
	return nil
}

func buildElementBytes(id ebml.ID, payload []byte) ([]byte, error) {
	var out bytes.Buffer
	w := ebml.NewWriter(&out)
	if err := w.WriteElement(id, payload); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func buildElementBytesWithExtraSizeByte(id ebml.ID, payload []byte) ([]byte, error) {
	idWidth, err := ebml.IDWidth(id)
	if err != nil {
		return nil, err
	}
	sizeWidth, err := ebml.SizeVINTWidth(uint64(len(payload)))
	if err != nil {
		return nil, err
	}
	if sizeWidth == ebml.MaxSizeWidth {
		return nil, ErrSeekHeadTooSmall
	}
	out := make([]byte, idWidth+sizeWidth+1+len(payload))
	if _, err := ebml.EncodeID(out, id); err != nil {
		return nil, err
	}
	if _, err := ebml.EncodeSizeVINTWidth(out[idWidth:], uint64(len(payload)), sizeWidth+1); err != nil {
		return nil, err
	}
	copy(out[idWidth+sizeWidth+1:], payload)
	return out, nil
}

func writeVoidTotal(w *ebml.Writer, total int) error {
	if total == 0 {
		return nil
	}
	if total < 2 {
		return ErrInvalidData
	}
	_, payloadSize, err := voidLayout(total)
	if err != nil {
		return err
	}
	if err := w.WriteHeader(idVoid, uint64(payloadSize)); err != nil {
		return err
	}
	var zero [64]byte
	for payloadSize > 0 {
		n := min(payloadSize, len(zero))
		if _, err := w.Write(zero[:n]); err != nil {
			return err
		}
		payloadSize -= n
	}
	return nil
}

func writeVoidTotalRaw(writer io.Writer, total int) error {
	if total == 0 {
		return nil
	}
	if total < 2 {
		return ErrInvalidData
	}
	width, payloadSize, err := voidLayout(total)
	if err != nil {
		return err
	}
	var header [1 + ebml.MaxSizeWidth]byte
	header[0] = byte(idVoid)
	if _, err := ebml.EncodeSizeVINTWidth(header[1:], uint64(payloadSize), width); err != nil {
		return err
	}
	if err := writeFull(writer, header[:1+width]); err != nil {
		return err
	}
	var zero [64]byte
	for payloadSize > 0 {
		n := min(payloadSize, len(zero))
		if err := writeFull(writer, zero[:n]); err != nil {
			return err
		}
		payloadSize -= n
	}
	return nil
}

func voidLayout(total int) (int, int, error) {
	for width := 1; width <= ebml.MaxSizeWidth; width++ {
		payloadSize := total - 1 - width
		if payloadSize < 0 {
			continue
		}
		if uint64(payloadSize) <= maxKnownSize(width) {
			return width, payloadSize, nil
		}
	}
	return 0, 0, ErrInvalidData
}

func maxKnownSize(width int) uint64 {
	return (uint64(1) << uint(7*width)) - 2
}
