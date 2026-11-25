package matroska

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"math"

	"github.com/thesyncim/goav/container/ebml"
	"github.com/thesyncim/goav/internal/av1"
)

type Muxer struct {
	writer                 io.Writer
	ebml                   *ebml.Writer
	options                MuxerOptions
	tracks                 []Track
	pendingPackets         []pendingMuxerPacket
	cues                   []CuePoint
	headerWritten          bool
	clusterOpen            bool
	seekable               bool
	segmentData            int64
	seekHeadOffset         int64
	seekHeadReserve        int
	infoPosition           uint64
	tracksPosition         uint64
	attachPosition         uint64
	chapterPosition        uint64
	tagsPosition           uint64
	cuesPosition           uint64
	clusterPosition        uint64
	clusterBlock           uint64
	segmentPatch           ebml.SizePatch
	segmentSized           bool
	clusterPatch           ebml.SizePatch
	clusterSized           bool
	durationOffset         int64
	durationPatch          bool
	maxTimeNS              int64
	clusterTimecode        int64
	closed                 bool
	scratch                [codecPrivateScratchSize]byte
	blockScratch           [16]byte
	blockPayload           bytes.Buffer
	codecPayload           bytes.Buffer
	encryptionPayload      bytes.Buffer
	lacePayload            []byte
	zlibWriter             *zlib.Writer
	contentEncryptionIV    [contentEncryptionIVSize]byte
	contentEncryptionIVSet bool
}

type pendingMuxerPacket struct {
	laced       bool
	packet      Packet
	lacedPacket LacedPacket
}

func NewMuxer(w io.Writer, opts MuxerOptions) (*Muxer, error) {
	if w == nil {
		return nil, ErrNilWriter
	}
	opts = normalizeMuxerOptions(opts)
	if err := validateContentEncryptionOptions(opts.ContentEncryptionKeys, opts.ContentEncryptionInitialIV); err != nil {
		return nil, err
	}
	info, err := normalizeSegmentInfo(opts.Info)
	if err != nil {
		return nil, err
	}
	opts.Info = info
	if err := validateCuePolicy(opts.CuePolicy); err != nil {
		return nil, err
	}
	attachments, err := normalizeAttachments(opts.Attachments)
	if err != nil {
		return nil, err
	}
	opts.Attachments = attachments
	chapters, err := normalizeChapters(opts.Chapters)
	if err != nil {
		return nil, err
	}
	opts.Chapters = chapters
	tags, err := normalizeTags(opts.Tags)
	if err != nil {
		return nil, err
	}
	opts.Tags = tags
	unknownSegments, err := normalizeUnknownElements(opts.UnknownSegmentElements)
	if err != nil {
		return nil, err
	}
	opts.UnknownSegmentElements = unknownSegments
	unknownTracks, err := normalizeUnknownElementsFor(opts.UnknownTracksElements, isKnownTracksElement)
	if err != nil {
		return nil, err
	}
	opts.UnknownTracksElements = unknownTracks
	unknownAttachments, err := normalizeUnknownElementsFor(opts.UnknownAttachmentsElements, isKnownAttachmentsElement)
	if err != nil {
		return nil, err
	}
	opts.UnknownAttachmentsElements = unknownAttachments
	unknownChapters, err := normalizeUnknownElementsFor(opts.UnknownChaptersElements, isKnownChaptersElement)
	if err != nil {
		return nil, err
	}
	opts.UnknownChaptersElements = unknownChapters
	unknownTags, err := normalizeUnknownElementsFor(opts.UnknownTagsElements, isKnownTagsElement)
	if err != nil {
		return nil, err
	}
	opts.UnknownTagsElements = unknownTags
	m := &Muxer{}
	m.init(w, opts)
	return m, nil
}

func (m *Muxer) init(w io.Writer, opts MuxerOptions) {
	m.writer = w
	m.ebml = ebml.NewWriter(w)
	m.options = normalizeMuxerOptions(opts)
	m.options.Info = cloneSegmentInfo(m.options.Info)
	m.options.Attachments = cloneAttachments(m.options.Attachments)
	m.options.Chapters = cloneChapters(m.options.Chapters)
	m.options.Tags = cloneTags(m.options.Tags)
	m.options.UnknownSegmentElements = cloneUnknownElements(m.options.UnknownSegmentElements)
	m.options.UnknownTracksElements = cloneUnknownElements(m.options.UnknownTracksElements)
	m.options.UnknownAttachmentsElements = cloneUnknownElements(m.options.UnknownAttachmentsElements)
	m.options.UnknownChaptersElements = cloneUnknownElements(m.options.UnknownChaptersElements)
	m.options.UnknownTagsElements = cloneUnknownElements(m.options.UnknownTagsElements)
	m.options.ContentEncryptionKeys = cloneContentEncryptionKeys(m.options.ContentEncryptionKeys)
	if m.options.ContentEncryptionInitialIV != nil {
		m.options.ContentEncryptionInitialIV = append([]byte(nil), m.options.ContentEncryptionInitialIV...)
	}
	m.tracks = m.tracks[:0]
	m.clearPendingPackets()
	m.cues = m.cues[:0]
	m.headerWritten = false
	m.clusterOpen = false
	// A writer is seekable when it can report its position: an *os.File on a
	// pipe has a Seek method that fails.
	m.seekable = false
	if seeker, ok := w.(io.Seeker); ok {
		if _, err := seeker.Seek(0, io.SeekCurrent); err == nil {
			m.seekable = true
		}
	}
	m.segmentData = 0
	m.seekHeadOffset = 0
	m.seekHeadReserve = 0
	m.infoPosition = 0
	m.tracksPosition = 0
	m.attachPosition = 0
	m.chapterPosition = 0
	m.tagsPosition = 0
	m.cuesPosition = 0
	m.clusterPosition = 0
	m.clusterBlock = 0
	m.segmentPatch = ebml.SizePatch{}
	m.segmentSized = false
	m.clusterPatch = ebml.SizePatch{}
	m.clusterSized = false
	m.durationOffset = 0
	m.durationPatch = false
	m.maxTimeNS = 0
	if m.options.Info.DurationSet {
		m.maxTimeNS = m.options.Info.DurationNS
	}
	m.clusterTimecode = 0
	m.closed = false
	m.blockPayload.Reset()
	m.codecPayload.Reset()
	m.encryptionPayload.Reset()
	m.lacePayload = m.lacePayload[:0]
	m.contentEncryptionIV = [contentEncryptionIVSize]byte{}
	m.contentEncryptionIVSet = false
	if m.seekable && !m.options.Streaming {
		capacity := m.options.CueCapacity
		if capacity <= 0 {
			capacity = 256
		}
		if cap(m.cues) < capacity {
			m.cues = make([]CuePoint, 0, capacity)
		}
	}
}

func (m *Muxer) AddTrack(track Track) (uint32, error) {
	if m == nil || m.writer == nil {
		return 0, ErrNilWriter
	}
	if m.headerWritten || len(m.pendingPackets) != 0 {
		return 0, ErrTrackAfterWrite
	}
	if err := normalizeTrackBlockAdditionMetadata(&track); err != nil {
		return 0, err
	}
	unknownElements, err := normalizeUnknownElementsFor(track.UnknownElements, isKnownTrackEntryElement)
	if err != nil {
		return 0, ErrInvalidTrack
	}
	track.UnknownElements = unknownElements
	if err := validateTrack(track); err != nil {
		return 0, err
	}
	if track.ID == 0 {
		track.ID = uint32(len(m.tracks) + 1)
	}
	normalizeTrackIdentity(&track)
	for i := range m.tracks {
		if m.tracks[i].ID == track.ID || m.tracks[i].UID == track.UID {
			return 0, ErrInvalidTrack
		}
	}
	if track.Language == "" {
		track.Language = "und"
	}
	if track.TimebaseNum == 0 && track.TimebaseDen == 0 {
		track.TimebaseNum = 1
		track.TimebaseDen = int64(timeNS)
	}
	m.tracks = append(m.tracks, cloneTrack(track))
	return track.ID, nil
}

func normalizeTrackIdentity(track *Track) {
	if track.UID == 0 {
		track.UID = uint64(track.ID)
	}
	if !track.CodecDecodeAllSet {
		track.CodecDecodeAll = true
	}
	if !track.FlagEnabledSet {
		track.FlagEnabled = true
	}
	if !track.FlagDefaultSet {
		track.FlagDefault = true
	}
	if !track.FlagLacingSet {
		track.FlagLacing = true
	}
}

func normalizeTrackBlockAdditionMetadata(track *Track) error {
	maxMappingID, err := maxBlockAdditionMappingID(track.BlockAdditionMappings)
	if err != nil {
		return ErrInvalidTrack
	}
	if maxMappingID == 0 {
		return nil
	}
	if track.MaxBlockAdditionID == 0 {
		track.MaxBlockAdditionID = maxMappingID
		return nil
	}
	if maxMappingID > track.MaxBlockAdditionID {
		return ErrInvalidTrack
	}
	return nil
}

func validateTrackBlockAdditionMetadata(track Track) error {
	maxMappingID, err := maxBlockAdditionMappingID(track.BlockAdditionMappings)
	if err != nil {
		return err
	}
	if maxMappingID > track.MaxBlockAdditionID {
		return ErrInvalidData
	}
	return nil
}

func (m *Muxer) WritePacket(packet Packet) error {
	if m == nil || m.writer == nil {
		return ErrNilWriter
	}
	if m.closed {
		return ErrClosed
	}
	trackIndex, ok := m.trackIndex(packet.TrackID)
	if packet.TrackID == 0 || !ok {
		return ErrUnknownTrack
	}
	track := m.tracks[trackIndex]
	if packet.TimeNS < 0 {
		return ErrInvalidData
	}
	if packet.DurationNS < 0 || (packet.DurationNS > 0 && packet.TimeNS > math.MaxInt64-packet.DurationNS) {
		return ErrInvalidData
	}
	if packet.Keyframe && len(packet.ReferenceBlockTimeNS) != 0 {
		return ErrInvalidData
	}
	if err := validateReferenceBlockTimes(packet.ReferenceBlockTimeNS, m.options.TimecodeScaleNS, track); err != nil {
		return err
	}
	maxAdditionID, err := validateBlockAdditions(packet.BlockAdditions)
	if err != nil {
		return err
	}
	if len(packet.ContentEncryptionPartitions) != 0 {
		hasEncryption, err := trackHasBlockEncryption(track)
		if err != nil {
			return err
		}
		if !hasEncryption {
			return ErrInvalidData
		}
	}
	if err := validateUnknownElementsFor(packet.UnknownClusterElements, isKnownClusterElement); err != nil {
		return ErrInvalidData
	}
	if !m.headerWritten {
		if maxAdditionID > track.MaxBlockAdditionID {
			track.MaxBlockAdditionID = maxAdditionID
			m.tracks[trackIndex].MaxBlockAdditionID = maxAdditionID
		}
		var err error
		track, err = m.prepareTrackForHeader(trackIndex, [][]byte{packet.Data})
		if err != nil {
			return err
		}
		if !m.headerTracksReady() || len(m.pendingPackets) != 0 {
			return m.queuePacket(packet)
		}
		if err := m.writeHeader(); err != nil {
			return err
		}
	} else if codecRequiresPrivateForHeader(track.Codec) && len(track.CodecPrivate) == 0 {
		return ErrInvalidTrack
	} else if maxAdditionID > track.MaxBlockAdditionID {
		return ErrInvalidData
	}
	return m.writePacketReady(packet, track)
}

func (m *Muxer) writePacketReady(packet Packet, track Track) error {
	clusterTimecode, err := trackClusterTimecode(packet.TimeNS, track, m.options.TimecodeScaleNS)
	if err != nil {
		return err
	}
	cueTimecode, err := cueSegmentTimecode(packet.TimeNS, m.options.TimecodeScaleNS)
	if err != nil {
		return err
	}
	if m.shouldStartCluster(clusterTimecode) {
		if err := m.startCluster(clusterTimecode); err != nil {
			return err
		}
	}
	blockTimecode, err := trackBlockTimecode(packet.TimeNS, track, m.clusterTimecode, m.options.TimecodeScaleNS)
	if err == ErrTimecodeOverflow {
		if err := m.startCluster(clusterTimecode); err != nil {
			return err
		}
		blockTimecode, err = trackBlockTimecode(packet.TimeNS, track, m.clusterTimecode, m.options.TimecodeScaleNS)
	}
	if err != nil {
		return err
	}
	if err := writeUnknownElements(m.ebml, packet.UnknownClusterElements); err != nil {
		return err
	}
	relativePosition := m.relativeClusterPosition()
	blockNumber := m.clusterBlock + 1
	if err := m.writeSimpleBlock(packet, blockTimecode, track); err != nil {
		return err
	}
	m.clusterBlock = blockNumber
	m.updateMaxTime(packet)
	m.addCue(packet, track, cueTimecode, relativePosition, blockNumber)
	return nil
}

// WriteLacedPacket writes multiple frames for one track into one laced block.
// It uses SimpleBlock unless BlockGroup-only metadata is present.
// For timestamp-accurate SimpleBlock demuxing, the track should declare
// DefaultDurationNS. When FrameDurationNS is set, a BlockGroup is written and
// the demuxer returns that per-frame duration for each laced frame.
func (m *Muxer) WriteLacedPacket(packet LacedPacket) error {
	if m == nil || m.writer == nil {
		return ErrNilWriter
	}
	if m.closed {
		return ErrClosed
	}
	trackIndex, ok := m.trackIndex(packet.TrackID)
	if packet.TrackID == 0 || !ok {
		return ErrUnknownTrack
	}
	track := m.tracks[trackIndex]
	if packet.TimeNS < 0 {
		return ErrInvalidData
	}
	if packet.FrameDurationNS < 0 ||
		(packet.FrameDurationNS > 0 && len(packet.Frames) > 0 &&
			int64(len(packet.Frames)) > math.MaxInt64/packet.FrameDurationNS) {
		return ErrInvalidData
	}
	if err := validateLacedFrameDuration(packet.FrameDurationNS, m.options.TimecodeScaleNS, track); err != nil {
		return err
	}
	if packet.Keyframe && len(packet.ReferenceBlockTimeNS) != 0 {
		return ErrInvalidData
	}
	if err := validateReferenceBlockTimes(packet.ReferenceBlockTimeNS, m.options.TimecodeScaleNS, track); err != nil {
		return err
	}
	maxAdditionID, err := validateBlockAdditions(packet.BlockAdditions)
	if err != nil {
		return err
	}
	if packet.Discardable && lacedPacketNeedsBlockGroup(packet) {
		return ErrInvalidData
	}
	if err := validateUnknownElementsFor(packet.UnknownClusterElements, isKnownClusterElement); err != nil {
		return ErrInvalidData
	}
	if track.FlagLacingSet && !track.FlagLacing {
		return ErrInvalidTrack
	}
	if !m.headerWritten {
		if maxAdditionID > track.MaxBlockAdditionID {
			track.MaxBlockAdditionID = maxAdditionID
			m.tracks[trackIndex].MaxBlockAdditionID = maxAdditionID
		}
		var err error
		track, err = m.prepareTrackForHeader(trackIndex, packet.Frames)
		if err != nil {
			return err
		}
		if !m.headerTracksReady() || len(m.pendingPackets) != 0 {
			return m.queueLacedPacket(packet)
		}
	} else if codecRequiresPrivateForHeader(track.Codec) && len(track.CodecPrivate) == 0 {
		return ErrInvalidTrack
	} else if maxAdditionID > track.MaxBlockAdditionID {
		return ErrInvalidData
	}
	if blockPayloadsNeedSizing(track) {
		return m.writeSizedLacedPacket(packet, track)
	}
	return m.writeLacedPacket(packet, track, nil, nil)
}

func (m *Muxer) writeSizedLacedPacket(packet LacedPacket, track Track) error {
	if len(packet.Frames) < 2 || len(packet.Frames) > defaultMaxLaceFrames {
		return ErrInvalidData
	}
	var sizeScratch [defaultMaxLaceFrames]int
	muxedFrameSizes := sizeScratch[:len(packet.Frames)]
	var muxedPayload []byte
	if blockPayloadNeedsBuffering(track) {
		var err error
		muxedPayload, err = m.prepareLacedBlockPayload(packet.Frames, track, muxedFrameSizes)
		if err != nil {
			return err
		}
		return m.writeLacedPacket(packet, track, muxedFrameSizes, muxedPayload)
	}
	if err := blockPayloadFrameSizes(packet.Frames, track, muxedFrameSizes); err != nil {
		return err
	}
	return m.writeLacedPacket(packet, track, muxedFrameSizes, muxedPayload)
}

func (m *Muxer) writeLacedPacket(packet LacedPacket, track Track, muxedFrameSizes []int, muxedPayload []byte) error {
	lacing, err := lacingFlag(packet.Lacing, packet.Frames, muxedFrameSizes)
	if err != nil {
		return err
	}
	payloadSize, err := m.lacedBlockPayloadSize(packet, lacing, muxedFrameSizes)
	if err != nil {
		return err
	}
	endTime, err := m.lacedPacketEndTime(packet)
	if err != nil {
		return err
	}
	if !m.headerWritten {
		if err := m.writeHeader(); err != nil {
			return err
		}
	}
	clusterTimecode, err := trackClusterTimecode(packet.TimeNS, track, m.options.TimecodeScaleNS)
	if err != nil {
		return err
	}
	cueTimecode, err := cueSegmentTimecode(packet.TimeNS, m.options.TimecodeScaleNS)
	if err != nil {
		return err
	}
	if m.shouldStartCluster(clusterTimecode) {
		if err := m.startCluster(clusterTimecode); err != nil {
			return err
		}
	}
	blockTimecode, err := trackBlockTimecode(packet.TimeNS, track, m.clusterTimecode, m.options.TimecodeScaleNS)
	if err == ErrTimecodeOverflow {
		if err := m.startCluster(clusterTimecode); err != nil {
			return err
		}
		blockTimecode, err = trackBlockTimecode(packet.TimeNS, track, m.clusterTimecode, m.options.TimecodeScaleNS)
	}
	if err != nil {
		return err
	}
	if err := writeUnknownElements(m.ebml, packet.UnknownClusterElements); err != nil {
		return err
	}
	relativePosition := m.relativeClusterPosition()
	blockNumber := m.clusterBlock + 1
	if lacedPacketNeedsBlockGroup(packet) {
		if err := m.writeLacedBlockGroup(packet, blockTimecode, lacing, payloadSize, track, muxedFrameSizes, muxedPayload); err != nil {
			return err
		}
	} else if err := m.writeLacedBlock(idSimpleBlock, packet, blockTimecode, lacedSimpleBlockFlags(packet), lacing, payloadSize, track, muxedFrameSizes, muxedPayload); err != nil {
		return err
	}
	m.clusterBlock = blockNumber
	if endTime > m.maxTimeNS {
		m.maxTimeNS = endTime
	}
	cuePacket := Packet{
		TrackID:              packet.TrackID,
		TimeNS:               packet.TimeNS,
		ReferenceBlockTimeNS: packet.ReferenceBlockTimeNS,
		Keyframe:             packet.Keyframe,
	}
	if endTime > packet.TimeNS {
		cuePacket.DurationNS = endTime - packet.TimeNS
	}
	m.addCue(cuePacket, track, cueTimecode, relativePosition, blockNumber)
	return nil
}

func (m *Muxer) Close() error {
	if m == nil {
		return nil
	}
	if m.closed {
		return nil
	}
	if m.writer != nil && !m.headerWritten && len(m.tracks) != 0 {
		if len(m.pendingPackets) != 0 {
			if !m.headerTracksReady() {
				return ErrInvalidTrack
			}
			if err := m.flushPendingPackets(); err != nil {
				return err
			}
		} else {
			if err := m.writeHeader(); err != nil {
				return err
			}
		}
	}
	if m.clusterSized {
		if err := m.ebml.FinishSizedElement(m.clusterPatch); err != nil {
			return err
		}
		m.clusterSized = false
	}
	if m.hasCuesToWrite() {
		if err := m.writeCues(); err != nil {
			return err
		}
	}
	if m.durationPatch {
		if err := m.patchDuration(); err != nil {
			return err
		}
	}
	if m.seekHeadReserve != 0 {
		if err := m.patchSeekHead(); err != nil {
			return err
		}
	}
	if m.segmentSized {
		if err := m.ebml.FinishSizedElement(m.segmentPatch); err != nil {
			return err
		}
		m.segmentSized = false
	}
	m.closed = true
	return nil
}

func (m *Muxer) track(id uint32) (Track, bool) {
	index, ok := m.trackIndex(id)
	if !ok {
		return Track{}, false
	}
	return m.tracks[index], true
}

func (m *Muxer) trackIndex(id uint32) (int, bool) {
	for i := range m.tracks {
		if m.tracks[i].ID == id {
			return i, true
		}
	}
	return 0, false
}

func (m *Muxer) defaultDurationNS(trackID uint32) int64 {
	for i := range m.tracks {
		if m.tracks[i].ID == trackID {
			return m.tracks[i].DefaultDurationNS
		}
	}
	return 0
}

func (m *Muxer) shouldStartCluster(timecode int64) bool {
	if !m.clusterOpen {
		return true
	}
	delta := timecode - m.clusterTimecode
	if delta < math.MinInt16 || delta > math.MaxInt16 {
		return true
	}
	maxDelta := m.options.ClusterMaxDurationNS / m.options.TimecodeScaleNS
	return maxDelta > 0 && delta >= maxDelta
}

func (m *Muxer) writeHeader() error {
	if len(m.tracks) == 0 {
		return ErrInvalidTrack
	}
	if err := m.validateTracksForHeader(); err != nil {
		return err
	}
	if err := m.writeEBMLHeader(); err != nil {
		return err
	}
	if !m.options.Streaming && m.seekable {
		patch, err := m.ebml.StartSizedElement(idSegment, ebml.MaxSizeWidth)
		if err != nil {
			return err
		}
		m.segmentPatch = patch
		m.segmentSized = true
		m.segmentData = patch.Start
		if err := m.writeSeekHeadPlaceholder(); err != nil {
			return err
		}
	} else {
		if err := m.ebml.WriteUnknownHeader(idSegment, ebml.MaxSizeWidth); err != nil {
			return err
		}
		m.segmentData = m.ebml.Offset()
	}
	m.infoPosition = m.relativeSegmentPosition()
	if err := m.writeInfo(); err != nil {
		return err
	}
	m.tracksPosition = m.relativeSegmentPosition()
	if err := m.writeTracks(); err != nil {
		return err
	}
	if m.hasAttachmentsElement() {
		m.attachPosition = m.relativeSegmentPosition()
		if err := m.writeAttachments(); err != nil {
			return err
		}
	}
	if m.hasChaptersElement() {
		m.chapterPosition = m.relativeSegmentPosition()
		if err := m.writeChapters(); err != nil {
			return err
		}
	}
	if m.hasTagsElement() {
		m.tagsPosition = m.relativeSegmentPosition()
		if err := m.writeTags(); err != nil {
			return err
		}
	}
	if err := m.writeUnknownSegmentElements(); err != nil {
		return err
	}
	m.headerWritten = true
	return nil
}

func (m *Muxer) hasAttachmentsElement() bool {
	return len(m.options.Attachments) != 0 || len(m.options.UnknownAttachmentsElements) != 0
}

func (m *Muxer) hasChaptersElement() bool {
	return len(m.options.Chapters) != 0 || len(m.options.UnknownChaptersElements) != 0
}

func (m *Muxer) hasTagsElement() bool {
	return len(m.options.Tags) != 0 || len(m.options.UnknownTagsElements) != 0
}

func (m *Muxer) prepareTrackForHeader(trackIndex int, frames [][]byte) (Track, error) {
	track := m.tracks[trackIndex]
	switch {
	case track.Codec == CodecAV1 && len(track.CodecPrivate) == 0:
		private, err := av1.ConfigFromFrames(frames)
		if err != nil {
			return Track{}, fmt.Errorf("%w: %w", ErrInvalidData, err)
		}
		track.CodecPrivate = private
	case track.Codec == CodecH264 && len(track.CodecPrivate) == 0:
		private, err := h264AVCDecoderConfigurationRecordFromAnnexBFrames(frames)
		if err != nil {
			return Track{}, err
		}
		track.CodecPrivate = private
	case track.Codec == CodecH265 && len(track.CodecPrivate) == 0:
		private, err := h265HEVCDecoderConfigurationRecordFromAnnexBFrames(frames)
		if err != nil {
			return Track{}, err
		}
		track.CodecPrivate = private
	}
	m.tracks[trackIndex] = track
	return track, nil
}

func (m *Muxer) headerTracksReady() bool {
	for i := range m.tracks {
		if codecRequiresPrivateForHeader(m.tracks[i].Codec) && len(m.tracks[i].CodecPrivate) == 0 {
			return false
		}
	}
	return true
}

func (m *Muxer) queuePacket(packet Packet) error {
	m.pendingPackets = append(m.pendingPackets, pendingMuxerPacket{packet: clonePendingPacket(packet)})
	if !m.headerTracksReady() {
		return nil
	}
	return m.flushPendingPackets()
}

func (m *Muxer) queueLacedPacket(packet LacedPacket) error {
	m.pendingPackets = append(m.pendingPackets, pendingMuxerPacket{laced: true, lacedPacket: clonePendingLacedPacket(packet)})
	if !m.headerTracksReady() {
		return nil
	}
	return m.flushPendingPackets()
}

func (m *Muxer) flushPendingPackets() error {
	if !m.headerTracksReady() {
		return ErrInvalidTrack
	}
	pending := m.pendingPackets
	m.pendingPackets = nil
	if err := m.writeHeader(); err != nil {
		m.pendingPackets = pending
		return err
	}
	for i := range pending {
		if pending[i].laced {
			packet := pending[i].lacedPacket
			track, ok := m.track(packet.TrackID)
			if !ok {
				return ErrUnknownTrack
			}
			if blockPayloadsNeedSizing(track) {
				if err := m.writeSizedLacedPacket(packet, track); err != nil {
					return err
				}
			} else if err := m.writeLacedPacket(packet, track, nil, nil); err != nil {
				return err
			}
			continue
		}
		packet := pending[i].packet
		track, ok := m.track(packet.TrackID)
		if !ok {
			return ErrUnknownTrack
		}
		if err := m.writePacketReady(packet, track); err != nil {
			return err
		}
	}
	clearPendingPackets(pending)
	return nil
}

func (m *Muxer) clearPendingPackets() {
	clearPendingPackets(m.pendingPackets)
	m.pendingPackets = m.pendingPackets[:0]
}

func clearPendingPackets(packets []pendingMuxerPacket) {
	for i := range packets {
		packets[i] = pendingMuxerPacket{}
	}
}

func clonePendingPacket(packet Packet) Packet {
	out := packet
	out.Data = cloneBytes(packet.Data)
	out.ReferenceBlockTimeNS = cloneInt64s(packet.ReferenceBlockTimeNS)
	out.CodecState = cloneBytes(packet.CodecState)
	out.BlockAdditions = clonePacketBlockAdditions(nil, packet.BlockAdditions)
	out.ContentEncryptionPartitions = cloneUint32s(packet.ContentEncryptionPartitions)
	out.UnknownClusterElements = cloneUnknownElements(packet.UnknownClusterElements)
	return out
}

func clonePendingLacedPacket(packet LacedPacket) LacedPacket {
	out := packet
	out.ReferenceBlockTimeNS = cloneInt64s(packet.ReferenceBlockTimeNS)
	out.CodecState = cloneBytes(packet.CodecState)
	out.BlockAdditions = clonePacketBlockAdditions(nil, packet.BlockAdditions)
	out.UnknownClusterElements = cloneUnknownElements(packet.UnknownClusterElements)
	if len(packet.Frames) != 0 {
		out.Frames = make([][]byte, len(packet.Frames))
		for i := range packet.Frames {
			out.Frames[i] = cloneBytes(packet.Frames[i])
		}
	}
	return out
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte(nil), value...)
}

func cloneInt64s(values []int64) []int64 {
	if len(values) == 0 {
		return nil
	}
	return append([]int64(nil), values...)
}

func cloneUint32s(values []uint32) []uint32 {
	if len(values) == 0 {
		return nil
	}
	return append([]uint32(nil), values...)
}

func (m *Muxer) validateTracksForHeader() error {
	for i := range m.tracks {
		if codecRequiresPrivateForHeader(m.tracks[i].Codec) && len(m.tracks[i].CodecPrivate) == 0 {
			return ErrInvalidTrack
		}
	}
	return nil
}

func codecRequiresPrivateForHeader(codec Codec) bool {
	return codec == CodecAV1 || codec == CodecH264 || codec == CodecH265
}
