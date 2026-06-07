package matroska

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"

	"github.com/thesyncim/goav/container/ebml"
)

type Muxer struct {
	writer          io.Writer
	ebml            *ebml.Writer
	options         MuxerOptions
	tracks          []Track
	cues            []CuePoint
	headerWritten   bool
	clusterOpen     bool
	seekable        bool
	segmentData     int64
	seekHeadOffset  int64
	seekHeadReserve int
	infoPosition    uint64
	tracksPosition  uint64
	attachPosition  uint64
	chapterPosition uint64
	tagsPosition    uint64
	cuesPosition    uint64
	clusterPosition uint64
	clusterBlock    uint64
	segmentPatch    ebml.SizePatch
	segmentSized    bool
	clusterPatch    ebml.SizePatch
	clusterSized    bool
	durationOffset  int64
	durationPatch   bool
	maxTimeNS       int64
	clusterTimecode int64
	closed          bool
	scratch         [codecPrivateScratchSize]byte
	blockScratch    [16]byte
}

func NewMuxer(w io.Writer, opts MuxerOptions) (*Muxer, error) {
	if w == nil {
		return nil, ErrNilWriter
	}
	opts = normalizeMuxerOptions(opts)
	if err := validateSegmentInfo(opts.Info); err != nil {
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
	m.tracks = m.tracks[:0]
	m.cues = m.cues[:0]
	m.headerWritten = false
	m.clusterOpen = false
	_, m.seekable = w.(io.Seeker)
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
	m.clusterTimecode = 0
	m.closed = false
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
	if m.headerWritten {
		return 0, ErrTrackAfterWrite
	}
	if err := normalizeTrackBlockAdditionMetadata(&track); err != nil {
		return 0, err
	}
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
	if err := validateReferenceBlockTimes(packet.ReferenceBlockTimeNS, m.options.TimecodeScaleNS); err != nil {
		return err
	}
	maxAdditionID, err := validateBlockAdditions(packet.BlockAdditions)
	if err != nil {
		return err
	}
	if !m.headerWritten {
		if maxAdditionID > track.MaxBlockAdditionID {
			track.MaxBlockAdditionID = maxAdditionID
			m.tracks[trackIndex].MaxBlockAdditionID = maxAdditionID
		}
		var err error
		track, err = m.prepareTracksForHeader(trackIndex, [][]byte{packet.Data})
		if err != nil {
			return err
		}
		if err := m.writeHeader(); err != nil {
			return err
		}
	} else if codecRequiresPrivateForHeader(track.Codec) && len(track.CodecPrivate) == 0 {
		return ErrInvalidTrack
	} else if maxAdditionID > track.MaxBlockAdditionID {
		return ErrInvalidData
	}
	timecode := packet.TimeNS / m.options.TimecodeScaleNS
	if m.shouldStartCluster(timecode) {
		if err := m.startCluster(timecode); err != nil {
			return err
		}
	}
	delta := timecode - m.clusterTimecode
	if delta < math.MinInt16 || delta > math.MaxInt16 {
		return ErrTimecodeOverflow
	}
	relativePosition := m.relativeClusterPosition()
	blockNumber := m.clusterBlock + 1
	if err := m.writeSimpleBlock(packet, int16(delta), track); err != nil {
		return err
	}
	m.clusterBlock = blockNumber
	m.updateMaxTime(packet)
	m.addCue(packet, timecode, relativePosition, blockNumber)
	return nil
}

// WriteLacedPacket writes multiple frames for one track into one SimpleBlock.
// For timestamp-accurate demuxing, the track should declare DefaultDurationNS.
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
	if track.FlagLacingSet && !track.FlagLacing {
		return ErrInvalidTrack
	}
	if !m.headerWritten {
		var err error
		track, err = m.prepareTracksForHeader(trackIndex, packet.Frames)
		if err != nil {
			return err
		}
	} else if codecRequiresPrivateForHeader(track.Codec) && len(track.CodecPrivate) == 0 {
		return ErrInvalidTrack
	}
	if track.Codec == CodecH264 && len(track.CodecPrivate) != 0 {
		return m.writeH264LacedPacket(packet, track)
	}
	return m.writeLacedPacket(packet, track, nil)
}

func (m *Muxer) writeH264LacedPacket(packet LacedPacket, track Track) error {
	if len(packet.Frames) < 2 || len(packet.Frames) > defaultMaxLaceFrames {
		return ErrInvalidData
	}
	var sizeScratch [defaultMaxLaceFrames]int
	muxedFrameSizes := sizeScratch[:len(packet.Frames)]
	if err := h264LacedFrameSizes(packet.Frames, track, muxedFrameSizes); err != nil {
		return err
	}
	return m.writeLacedPacket(packet, track, muxedFrameSizes)
}

func (m *Muxer) writeLacedPacket(packet LacedPacket, track Track, muxedFrameSizes []int) error {
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
	timecode := packet.TimeNS / m.options.TimecodeScaleNS
	if m.shouldStartCluster(timecode) {
		if err := m.startCluster(timecode); err != nil {
			return err
		}
	}
	delta := timecode - m.clusterTimecode
	if delta < math.MinInt16 || delta > math.MaxInt16 {
		return ErrTimecodeOverflow
	}
	relativePosition := m.relativeClusterPosition()
	blockNumber := m.clusterBlock + 1
	if err := m.writeLacedBlock(packet, int16(delta), lacing, payloadSize, track, muxedFrameSizes); err != nil {
		return err
	}
	m.clusterBlock = blockNumber
	if endTime > m.maxTimeNS {
		m.maxTimeNS = endTime
	}
	m.addCue(Packet{TrackID: packet.TrackID, TimeNS: packet.TimeNS, Keyframe: packet.Keyframe}, timecode, relativePosition, blockNumber)
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
		if err := m.writeHeader(); err != nil {
			return err
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

func (m *Muxer) hasTrack(id uint32) bool {
	_, ok := m.track(id)
	return ok
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
	if len(m.options.Attachments) != 0 {
		m.attachPosition = m.relativeSegmentPosition()
		if err := m.writeAttachments(); err != nil {
			return err
		}
	}
	if len(m.options.Chapters) != 0 {
		m.chapterPosition = m.relativeSegmentPosition()
		if err := m.writeChapters(); err != nil {
			return err
		}
	}
	if len(m.options.Tags) != 0 {
		m.tagsPosition = m.relativeSegmentPosition()
		if err := m.writeTags(); err != nil {
			return err
		}
	}
	m.headerWritten = true
	return nil
}

func (m *Muxer) prepareTracksForHeader(trackIndex int, frames [][]byte) (Track, error) {
	track := m.tracks[trackIndex]
	switch {
	case track.Codec == CodecAV1 && len(track.CodecPrivate) == 0:
		private, err := av1CodecConfigurationRecordFromFrames(frames)
		if err != nil {
			return Track{}, err
		}
		track.CodecPrivate = private
	case track.Codec == CodecH264 && len(track.CodecPrivate) == 0:
		private, err := h264AVCDecoderConfigurationRecordFromAnnexBFrames(frames)
		if err != nil {
			return Track{}, err
		}
		track.CodecPrivate = private
	}
	for i := range m.tracks {
		candidate := m.tracks[i]
		if i == trackIndex {
			candidate = track
		}
		if codecRequiresPrivateForHeader(candidate.Codec) && len(candidate.CodecPrivate) == 0 {
			return Track{}, ErrInvalidTrack
		}
	}
	m.tracks[trackIndex] = track
	return track, nil
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
	return codec == CodecAV1 || codec == CodecH264
}

func (m *Muxer) writeEBMLHeader() error {
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	if err := w.WriteUInt(idEBMLVersion, 1); err != nil {
		return err
	}
	if err := w.WriteUInt(idEBMLReadVersion, 1); err != nil {
		return err
	}
	if err := w.WriteUInt(idEBMLMaxIDLength, ebml.MaxIDWidth); err != nil {
		return err
	}
	if err := w.WriteUInt(idEBMLMaxSizeLength, ebml.MaxSizeWidth); err != nil {
		return err
	}
	if err := w.WriteString(idDocType, m.options.DocType); err != nil {
		return err
	}
	if err := w.WriteUInt(idDocTypeVersion, m.options.DocTypeVersion); err != nil {
		return err
	}
	if err := w.WriteUInt(idDocTypeReadVersion, m.options.DocTypeReadVersion); err != nil {
		return err
	}
	return m.ebml.WriteElement(idEBML, payload.Bytes())
}

func (m *Muxer) writeInfo() error {
	if !m.options.Streaming && m.seekable {
		return m.writeSeekableInfo()
	}
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	if err := m.writeInfoFields(w, false); err != nil {
		return err
	}
	return m.ebml.WriteElement(idInfo, payload.Bytes())
}

func (m *Muxer) writeSeekableInfo() error {
	patch, err := m.ebml.StartSizedElement(idInfo, 4)
	if err != nil {
		return err
	}
	if err := m.writeInfoFields(m.ebml, true); err != nil {
		return err
	}
	return m.ebml.FinishSizedElement(patch)
}

func (m *Muxer) writeInfoFields(w *ebml.Writer, includeDuration bool) error {
	info := m.options.Info
	if err := writeOptionalBinary(w, idSegmentUUID, info.SegmentUUID); err != nil {
		return err
	}
	if info.SegmentFilename != "" {
		if err := w.WriteString(idSegmentFilename, info.SegmentFilename); err != nil {
			return err
		}
	}
	if err := writeOptionalBinary(w, idPrevUUID, info.PrevUUID); err != nil {
		return err
	}
	if info.PrevFilename != "" {
		if err := w.WriteString(idPrevFilename, info.PrevFilename); err != nil {
			return err
		}
	}
	if err := writeOptionalBinary(w, idNextUUID, info.NextUUID); err != nil {
		return err
	}
	if info.NextFilename != "" {
		if err := w.WriteString(idNextFilename, info.NextFilename); err != nil {
			return err
		}
	}
	if err := w.WriteUInt(idTimestampScale, uint64(m.options.TimecodeScaleNS)); err != nil {
		return err
	}
	if includeDuration {
		if err := w.WriteHeader(idDuration, 8); err != nil {
			return err
		}
		m.durationOffset = w.Offset()
		clear(m.blockScratch[:8])
		if _, err := w.Write(m.blockScratch[:8]); err != nil {
			return err
		}
		m.durationPatch = true
	}
	if info.DateUTCSet {
		if err := writeDate(w, idDateUTC, info.DateUTC); err != nil {
			return err
		}
	}
	if info.Title != "" {
		if err := w.WriteString(idTitle, info.Title); err != nil {
			return err
		}
	}
	if err := w.WriteString(idMuxingApp, m.options.MuxingApp); err != nil {
		return err
	}
	return w.WriteString(idWritingApp, m.options.WritingApp)
}

func writeOptionalBinary(w *ebml.Writer, id ebml.ID, value []byte) error {
	if len(value) == 0 {
		return nil
	}
	return writeBinary(w, id, value)
}

func validateSegmentInfo(info SegmentInfo) error {
	if err := validateSegmentUUID(info.SegmentUUID); err != nil {
		return err
	}
	if err := validateSegmentUUID(info.PrevUUID); err != nil {
		return err
	}
	if err := validateSegmentUUID(info.NextUUID); err != nil {
		return err
	}
	if len(info.SegmentUUID) != 0 {
		if bytes.Equal(info.SegmentUUID, info.PrevUUID) || bytes.Equal(info.SegmentUUID, info.NextUUID) {
			return ErrInvalidData
		}
	}
	if info.DateUTCSet {
		if _, err := ebmlDateNanos(info.DateUTC); err != nil {
			return err
		}
	}
	return nil
}

func validateSegmentUUID(value []byte) error {
	if len(value) == 0 {
		return nil
	}
	if len(value) != 16 {
		return ErrInvalidData
	}
	for i := range value {
		if value[i] != 0 {
			return nil
		}
	}
	return ErrInvalidData
}

func normalizeAttachments(attachments []Attachment) ([]Attachment, error) {
	if len(attachments) == 0 {
		return nil, nil
	}
	out := cloneAttachments(attachments)
	used := make(map[uint64]struct{}, len(out))
	for i := range out {
		if out[i].UID == 0 {
			continue
		}
		if _, ok := used[out[i].UID]; ok {
			return nil, ErrInvalidData
		}
		used[out[i].UID] = struct{}{}
	}
	var next uint64 = 1
	for i := range out {
		if out[i].UID == 0 {
			for {
				if _, ok := used[next]; !ok {
					break
				}
				next++
			}
			out[i].UID = next
			used[next] = struct{}{}
		}
		if err := validateAttachment(out[i]); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func validateAttachment(attachment Attachment) error {
	if attachment.UID == 0 || attachment.Filename == "" || attachment.MIMEType == "" || attachment.Data == nil {
		return ErrInvalidData
	}
	return nil
}

func normalizeChapters(editions []ChapterEdition) ([]ChapterEdition, error) {
	if len(editions) == 0 {
		return nil, nil
	}
	out := cloneChapters(editions)
	var nextUID uint64 = 1
	usedEditions := make(map[uint64]struct{}, len(out))
	usedChapters := make(map[uint64]struct{})
	for i := range out {
		if out[i].UID != 0 {
			if _, ok := usedEditions[out[i].UID]; ok {
				return nil, ErrInvalidData
			}
			usedEditions[out[i].UID] = struct{}{}
		}
		if len(out[i].Chapters) == 0 {
			return nil, ErrInvalidData
		}
	}
	for i := range out {
		if out[i].UID == 0 {
			for {
				if _, ok := usedEditions[nextUID]; !ok {
					break
				}
				nextUID++
			}
			out[i].UID = nextUID
			usedEditions[nextUID] = struct{}{}
		}
		if err := normalizeChapterList(out[i].Chapters, &nextUID, usedChapters); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func normalizeChapterList(chapters []Chapter, nextUID *uint64, used map[uint64]struct{}) error {
	for i := range chapters {
		if chapters[i].UID == 0 {
			for {
				if _, ok := used[*nextUID]; !ok {
					break
				}
				(*nextUID)++
			}
			chapters[i].UID = *nextUID
			used[*nextUID] = struct{}{}
		} else {
			if _, ok := used[chapters[i].UID]; ok {
				return ErrInvalidData
			}
			used[chapters[i].UID] = struct{}{}
		}
		if !chapters[i].EnabledSet {
			chapters[i].Enabled = true
			chapters[i].EnabledSet = true
		}
		if err := validateChapter(chapters[i]); err != nil {
			return err
		}
		for j := range chapters[i].Displays {
			if chapters[i].Displays[j].Language == "" {
				chapters[i].Displays[j].Language = "eng"
			}
		}
		if err := normalizeChapterList(chapters[i].Children, nextUID, used); err != nil {
			return err
		}
	}
	return nil
}

func validateChapter(chapter Chapter) error {
	if chapter.UID == 0 || chapter.StartNS < 0 || (chapter.EndSet && chapter.EndNS < chapter.StartNS) {
		return ErrInvalidData
	}
	for _, uid := range chapter.TrackUIDs {
		if uid == 0 {
			return ErrInvalidData
		}
	}
	for i := range chapter.Displays {
		if chapter.Displays[i].String == "" {
			return ErrInvalidData
		}
	}
	return nil
}

func normalizeTags(tags []Tag) ([]Tag, error) {
	if len(tags) == 0 {
		return nil, nil
	}
	out := cloneTags(tags)
	for i := range out {
		if len(out[i].Simple) == 0 {
			return nil, ErrInvalidData
		}
		if out[i].Target.TypeValue == 0 {
			out[i].Target.TypeValue = 50
		}
		if err := validateTagTarget(out[i].Target); err != nil {
			return nil, err
		}
		if err := normalizeSimpleTags(out[i].Simple); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func validateTagTarget(target TagTarget) error {
	if target.TypeValue > uint64(^uint32(0)) {
		return ErrInvalidData
	}
	return nil
}

func normalizeSimpleTags(tags []SimpleTag) error {
	for i := range tags {
		if tags[i].Name == "" || (tags[i].StringSet && tags[i].Binary != nil) {
			return ErrInvalidData
		}
		if tags[i].Language == "" {
			tags[i].Language = "und"
		}
		if !tags[i].DefaultSet {
			tags[i].Default = true
			tags[i].DefaultSet = true
		}
		if err := normalizeSimpleTags(tags[i].Children); err != nil {
			return err
		}
	}
	return nil
}

func (m *Muxer) writeTracks() error {
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	for i := range m.tracks {
		if err := writeTrackEntry(w, m.tracks[i], &m.scratch); err != nil {
			return err
		}
	}
	return m.ebml.WriteElement(idTracks, payload.Bytes())
}

func (m *Muxer) writeAttachments() error {
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	for i := range m.options.Attachments {
		if err := writeAttachedFile(w, m.options.Attachments[i]); err != nil {
			return err
		}
	}
	return m.ebml.WriteElement(idAttachments, payload.Bytes())
}

func (m *Muxer) writeChapters() error {
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	for i := range m.options.Chapters {
		if err := writeEditionEntry(w, m.options.Chapters[i]); err != nil {
			return err
		}
	}
	return m.ebml.WriteElement(idChapters, payload.Bytes())
}

func writeEditionEntry(w *ebml.Writer, edition ChapterEdition) error {
	var payload bytes.Buffer
	ew := ebml.NewWriter(&payload)
	if edition.UID != 0 {
		if err := ew.WriteUInt(idEditionUID, edition.UID); err != nil {
			return err
		}
	}
	if err := writeBoolElement(ew, idEditionFlagHidden, edition.Hidden); err != nil {
		return err
	}
	if err := writeBoolElement(ew, idEditionFlagDefault, edition.Default); err != nil {
		return err
	}
	if err := writeBoolElement(ew, idEditionFlagOrdered, edition.Ordered); err != nil {
		return err
	}
	for i := range edition.Chapters {
		if err := writeChapterAtom(ew, edition.Chapters[i]); err != nil {
			return err
		}
	}
	return w.WriteElement(idEditionEntry, payload.Bytes())
}

func writeChapterAtom(w *ebml.Writer, chapter Chapter) error {
	var payload bytes.Buffer
	cw := ebml.NewWriter(&payload)
	if err := cw.WriteUInt(idChapterUID, chapter.UID); err != nil {
		return err
	}
	if chapter.StringUID != "" {
		if err := cw.WriteString(idChapterStringUID, chapter.StringUID); err != nil {
			return err
		}
	}
	if err := cw.WriteUInt(idChapterTimeStart, uint64(chapter.StartNS)); err != nil {
		return err
	}
	if chapter.EndSet {
		if err := cw.WriteUInt(idChapterTimeEnd, uint64(chapter.EndNS)); err != nil {
			return err
		}
	}
	if err := writeBoolElement(cw, idChapterFlagHidden, chapter.Hidden); err != nil {
		return err
	}
	if err := writeBoolElement(cw, idChapterFlagEnabled, chapter.Enabled); err != nil {
		return err
	}
	if len(chapter.TrackUIDs) != 0 {
		if err := writeChapterTrack(cw, chapter.TrackUIDs); err != nil {
			return err
		}
	}
	for i := range chapter.Displays {
		if err := writeChapterDisplay(cw, chapter.Displays[i]); err != nil {
			return err
		}
	}
	for i := range chapter.Children {
		if err := writeChapterAtom(cw, chapter.Children[i]); err != nil {
			return err
		}
	}
	return w.WriteElement(idChapterAtom, payload.Bytes())
}

func writeChapterTrack(w *ebml.Writer, trackUIDs []uint64) error {
	var payload bytes.Buffer
	tw := ebml.NewWriter(&payload)
	for _, uid := range trackUIDs {
		if err := tw.WriteUInt(idChapterTrackUID, uid); err != nil {
			return err
		}
	}
	return w.WriteElement(idChapterTrack, payload.Bytes())
}

func writeChapterDisplay(w *ebml.Writer, display ChapterDisplay) error {
	var payload bytes.Buffer
	dw := ebml.NewWriter(&payload)
	if err := dw.WriteString(idChapString, display.String); err != nil {
		return err
	}
	language := display.Language
	if language == "" && display.LanguageBCP47 == "" {
		language = "eng"
	}
	if language != "" {
		if err := dw.WriteString(idChapLanguage, language); err != nil {
			return err
		}
	}
	if display.LanguageBCP47 != "" {
		if err := dw.WriteString(idChapLanguageBCP47, display.LanguageBCP47); err != nil {
			return err
		}
	}
	if display.Country != "" {
		if err := dw.WriteString(idChapCountry, display.Country); err != nil {
			return err
		}
	}
	return w.WriteElement(idChapterDisplay, payload.Bytes())
}

func (m *Muxer) writeTags() error {
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	for i := range m.options.Tags {
		if err := writeTag(w, m.options.Tags[i]); err != nil {
			return err
		}
	}
	return m.ebml.WriteElement(idTags, payload.Bytes())
}

func writeTag(w *ebml.Writer, tag Tag) error {
	var payload bytes.Buffer
	tw := ebml.NewWriter(&payload)
	if err := writeTagTargets(tw, tag.Target); err != nil {
		return err
	}
	for i := range tag.Simple {
		if err := writeSimpleTag(tw, tag.Simple[i]); err != nil {
			return err
		}
	}
	return w.WriteElement(idTag, payload.Bytes())
}

func writeTagTargets(w *ebml.Writer, target TagTarget) error {
	var payload bytes.Buffer
	tw := ebml.NewWriter(&payload)
	if target.TypeValue != 0 {
		if err := tw.WriteUInt(idTargetTypeValue, target.TypeValue); err != nil {
			return err
		}
	}
	if target.Type != "" {
		if err := tw.WriteString(idTargetType, target.Type); err != nil {
			return err
		}
	}
	for _, uid := range target.TrackUIDs {
		if err := tw.WriteUInt(idTagTrackUID, uid); err != nil {
			return err
		}
	}
	for _, uid := range target.EditionUIDs {
		if err := tw.WriteUInt(idTagEditionUID, uid); err != nil {
			return err
		}
	}
	for _, uid := range target.ChapterUIDs {
		if err := tw.WriteUInt(idTagChapterUID, uid); err != nil {
			return err
		}
	}
	for _, uid := range target.AttachmentUIDs {
		if err := tw.WriteUInt(idTagAttachmentUID, uid); err != nil {
			return err
		}
	}
	return w.WriteElement(idTargets, payload.Bytes())
}

func writeSimpleTag(w *ebml.Writer, tag SimpleTag) error {
	var payload bytes.Buffer
	tw := ebml.NewWriter(&payload)
	if err := tw.WriteString(idTagName, tag.Name); err != nil {
		return err
	}
	if tag.Language != "" {
		if err := tw.WriteString(idTagLanguage, tag.Language); err != nil {
			return err
		}
	}
	if tag.LanguageBCP47 != "" {
		if err := tw.WriteString(idTagLanguageBCP47, tag.LanguageBCP47); err != nil {
			return err
		}
	}
	if err := writeBoolElement(tw, idTagDefault, tag.Default); err != nil {
		return err
	}
	if tag.StringSet {
		if err := tw.WriteString(idTagString, tag.String); err != nil {
			return err
		}
	}
	if tag.Binary != nil {
		if err := writeBinary(tw, idTagBinary, tag.Binary); err != nil {
			return err
		}
	}
	for i := range tag.Children {
		if err := writeSimpleTag(tw, tag.Children[i]); err != nil {
			return err
		}
	}
	return w.WriteElement(idSimpleTag, payload.Bytes())
}

func writeBoolElement(w *ebml.Writer, id ebml.ID, value bool) error {
	if value {
		return w.WriteUInt(id, 1)
	}
	return w.WriteUInt(id, 0)
}

func writeAttachedFile(w *ebml.Writer, attachment Attachment) error {
	var payload bytes.Buffer
	aw := ebml.NewWriter(&payload)
	if attachment.Description != "" {
		if err := aw.WriteString(idFileDescription, attachment.Description); err != nil {
			return err
		}
	}
	if err := aw.WriteString(idFileName, attachment.Filename); err != nil {
		return err
	}
	if err := aw.WriteString(idFileMediaType, attachment.MIMEType); err != nil {
		return err
	}
	if err := writeBinary(aw, idFileData, attachment.Data); err != nil {
		return err
	}
	if err := aw.WriteUInt(idFileUID, attachment.UID); err != nil {
		return err
	}
	return w.WriteElement(idAttachedFile, payload.Bytes())
}

func (m *Muxer) startCluster(timecode int64) error {
	if m.clusterSized {
		if err := m.ebml.FinishSizedElement(m.clusterPatch); err != nil {
			return err
		}
		m.clusterSized = false
	}
	clusterOffset := m.ebml.Offset()
	if m.segmentData != 0 && clusterOffset >= m.segmentData {
		m.clusterPosition = uint64(clusterOffset - m.segmentData)
	}
	if !m.options.Streaming && m.seekable {
		patch, err := m.ebml.StartSizedElement(idCluster, ebml.MaxSizeWidth)
		if err != nil {
			return err
		}
		m.clusterPatch = patch
		m.clusterSized = true
	} else {
		if err := m.ebml.WriteUnknownHeader(idCluster, ebml.MaxSizeWidth); err != nil {
			return err
		}
	}
	if err := m.ebml.WriteUInt(idTimestamp, uint64(timecode)); err != nil {
		return err
	}
	m.clusterOpen = true
	m.clusterTimecode = timecode
	m.clusterBlock = 0
	return nil
}

func (m *Muxer) writeSeekHeadPlaceholder() error {
	const reserve = 512
	m.seekHeadOffset = m.ebml.Offset()
	m.seekHeadReserve = reserve
	return writeVoidTotal(m.ebml, reserve)
}

func (m *Muxer) writeSimpleBlock(packet Packet, blockTimecode int16, track Track) error {
	if packet.DurationNS > 0 ||
		len(packet.ReferenceBlockTimeNS) != 0 ||
		packet.ReferencePriority != 0 ||
		packet.DiscardPaddingNS != 0 ||
		len(packet.CodecState) != 0 ||
		len(packet.BlockAdditions) != 0 {
		return m.writeBlockGroup(packet, blockTimecode, track)
	}
	return m.writeBlock(idSimpleBlock, packet, blockTimecode, simpleBlockFlags(packet), track)
}

func (m *Muxer) addCue(packet Packet, timecode int64, relativePosition uint64, blockNumber uint64) {
	if !m.collectsCues() || !packet.Keyframe {
		return
	}
	position := CueTrackPosition{
		TrackID:             packet.TrackID,
		ClusterPosition:     m.clusterPosition,
		RelativePosition:    relativePosition,
		RelativePositionSet: true,
		BlockNumber:         blockNumber,
		BlockNumberSet:      blockNumber != 0,
	}
	if packet.DurationNS > 0 {
		position.DurationNS = packet.DurationNS
		position.DurationSet = true
	}
	cue := CuePoint{
		TimeNS:    timecode * m.options.TimecodeScaleNS,
		Positions: []CueTrackPosition{position},
	}
	applyCuePosition(&cue, position)
	m.cues = append(m.cues, cue)
}

func (m *Muxer) collectsCues() bool {
	return !m.options.Streaming && m.seekable
}

func (m *Muxer) hasCuesToWrite() bool {
	return m.collectsCues() && len(m.cues) != 0
}

func (m *Muxer) writeCues() error {
	m.cuesPosition = m.relativeSegmentPosition()
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	for i := range m.cues {
		if err := writeCuePoint(w, m.cues[i], m.options.TimecodeScaleNS); err != nil {
			return err
		}
	}
	return m.ebml.WriteElement(idCues, payload.Bytes())
}

func writeCuePoint(w *ebml.Writer, cue CuePoint, scaleNS int64) error {
	if scaleNS <= 0 || cue.TimeNS < 0 {
		return ErrInvalidData
	}
	var payload bytes.Buffer
	cw := ebml.NewWriter(&payload)
	if err := cw.WriteUInt(idCueTime, uint64(cue.TimeNS/scaleNS)); err != nil {
		return err
	}
	if len(cue.Positions) == 0 {
		if cue.TrackID == 0 {
			return ErrInvalidData
		}
		position := cueTrackPositionFromLegacy(cue)
		if err := writeCueTrackPosition(cw, position, scaleNS); err != nil {
			return err
		}
		return w.WriteElement(idCuePoint, payload.Bytes())
	}
	for i := range cue.Positions {
		if err := writeCueTrackPosition(cw, cue.Positions[i], scaleNS); err != nil {
			return err
		}
	}
	return w.WriteElement(idCuePoint, payload.Bytes())
}

func cueTrackPositionFromLegacy(cue CuePoint) CueTrackPosition {
	return CueTrackPosition{
		TrackID:             cue.TrackID,
		ClusterPosition:     cue.ClusterPosition,
		RelativePosition:    cue.RelativePosition,
		RelativePositionSet: cue.RelativePositionSet,
		DurationNS:          cue.DurationNS,
		DurationSet:         cue.DurationSet,
		BlockNumber:         cue.BlockNumber,
		BlockNumberSet:      cue.BlockNumberSet,
		CodecStatePosition:  cue.CodecStatePosition,
		CodecStateSet:       cue.CodecStateSet,
		References:          cue.References,
	}
}

func writeCueTrackPosition(w *ebml.Writer, position CueTrackPosition, scaleNS int64) error {
	if position.TrackID == 0 || position.DurationNS < 0 || (position.BlockNumberSet && position.BlockNumber == 0) {
		return ErrInvalidData
	}
	var positions bytes.Buffer
	pw := ebml.NewWriter(&positions)
	if err := pw.WriteUInt(idCueTrack, uint64(position.TrackID)); err != nil {
		return err
	}
	if err := pw.WriteUInt(idCueClusterPosition, position.ClusterPosition); err != nil {
		return err
	}
	if position.RelativePositionSet {
		if err := pw.WriteUInt(idCueRelativePos, position.RelativePosition); err != nil {
			return err
		}
	}
	if position.DurationSet {
		if err := pw.WriteUInt(idCueDuration, scaledDurationTicks(position.DurationNS, scaleNS)); err != nil {
			return err
		}
	}
	if position.BlockNumberSet {
		if err := pw.WriteUInt(idCueBlockNumber, position.BlockNumber); err != nil {
			return err
		}
	}
	if position.CodecStateSet {
		if err := pw.WriteUInt(idCueCodecState, position.CodecStatePosition); err != nil {
			return err
		}
	}
	for i := range position.References {
		if err := writeCueReference(pw, position.References[i], scaleNS); err != nil {
			return err
		}
	}
	return w.WriteElement(idCueTrackPositions, positions.Bytes())
}

func writeCueReference(w *ebml.Writer, reference CueReference, scaleNS int64) error {
	if reference.TimeNS < 0 || (reference.BlockNumberSet && reference.BlockNumber == 0) {
		return ErrInvalidData
	}
	var payload bytes.Buffer
	rw := ebml.NewWriter(&payload)
	if err := rw.WriteUInt(idCueRefTime, uint64(reference.TimeNS/scaleNS)); err != nil {
		return err
	}
	if err := rw.WriteUInt(idCueRefCluster, reference.ClusterPosition); err != nil {
		return err
	}
	if reference.BlockNumberSet {
		if err := rw.WriteUInt(idCueRefNumber, reference.BlockNumber); err != nil {
			return err
		}
	}
	if reference.CodecStateSet {
		if err := rw.WriteUInt(idCueRefCodecState, reference.CodecStatePosition); err != nil {
			return err
		}
	}
	return w.WriteElement(idCueReference, payload.Bytes())
}

func (m *Muxer) updateMaxTime(packet Packet) {
	end := packet.TimeNS
	if packet.DurationNS > 0 {
		end += packet.DurationNS
	}
	if end > m.maxTimeNS {
		m.maxTimeNS = end
	}
}

func (m *Muxer) patchDuration() error {
	seeker, ok := m.writer.(io.Seeker)
	if !ok {
		return nil
	}
	current := m.ebml.Offset()
	if _, err := seeker.Seek(m.durationOffset, io.SeekStart); err != nil {
		return err
	}
	binary.BigEndian.PutUint64(m.blockScratch[:8], math.Float64bits(float64(m.maxTimeNS)/float64(m.options.TimecodeScaleNS)))
	if err := writeFull(m.writer, m.blockScratch[:8]); err != nil {
		return err
	}
	if _, err := seeker.Seek(current, io.SeekStart); err != nil {
		return err
	}
	return nil
}

func (m *Muxer) patchSeekHead() error {
	seeker, ok := m.writer.(io.Seeker)
	if !ok {
		return nil
	}
	payload, err := m.buildSeekHeadPayload()
	if err != nil {
		return err
	}
	element, err := buildElementBytes(idSeekHead, payload)
	if err != nil {
		return err
	}
	if m.seekHeadReserve-len(element) == 1 {
		element, err = buildElementBytesWithExtraSizeByte(idSeekHead, payload)
		if err != nil {
			return err
		}
	}
	if len(element) > m.seekHeadReserve {
		return ErrSeekHeadTooSmall
	}
	current := m.ebml.Offset()
	if _, err := seeker.Seek(m.seekHeadOffset, io.SeekStart); err != nil {
		return err
	}
	if err := writeFull(m.writer, element); err != nil {
		return err
	}
	if err := writeVoidTotalRaw(m.writer, m.seekHeadReserve-len(element)); err != nil {
		return err
	}
	if _, err := seeker.Seek(current, io.SeekStart); err != nil {
		return err
	}
	return nil
}

func (m *Muxer) buildSeekHeadPayload() ([]byte, error) {
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	for _, entry := range []struct {
		id       ebml.ID
		position uint64
	}{
		{id: idInfo, position: m.infoPosition},
		{id: idTracks, position: m.tracksPosition},
		{id: idAttachments, position: m.attachPosition},
		{id: idChapters, position: m.chapterPosition},
		{id: idTags, position: m.tagsPosition},
		{id: idCues, position: m.cuesPosition},
	} {
		if entry.position == 0 {
			continue
		}
		if err := writeSeekEntry(w, entry.id, entry.position); err != nil {
			return nil, err
		}
	}
	return payload.Bytes(), nil
}

func writeSeekEntry(w *ebml.Writer, id ebml.ID, position uint64) error {
	var payload bytes.Buffer
	sw := ebml.NewWriter(&payload)
	var idPayload [ebml.MaxIDWidth]byte
	n, err := ebml.EncodeID(idPayload[:], id)
	if err != nil {
		return err
	}
	if err := writeBinary(sw, idSeekID, idPayload[:n]); err != nil {
		return err
	}
	if err := sw.WriteUInt(idSeekPosition, position); err != nil {
		return err
	}
	return w.WriteElement(idSeek, payload.Bytes())
}

func (m *Muxer) relativeSegmentPosition() uint64 {
	if m.segmentData == 0 {
		return 0
	}
	offset := m.ebml.Offset()
	if offset < m.segmentData {
		return 0
	}
	return uint64(offset - m.segmentData)
}

func (m *Muxer) relativeClusterPosition() uint64 {
	if !m.clusterOpen {
		return 0
	}
	offset := m.ebml.Offset()
	clusterData := m.clusterDataOffset()
	if offset < clusterData {
		return 0
	}
	return uint64(offset - clusterData)
}

func (m *Muxer) clusterDataOffset() int64 {
	if m.clusterSized {
		return m.clusterPatch.Start
	}
	return int64(m.clusterPosition) + m.segmentData + int64(ebml.MaxIDWidth+ebml.MaxSizeWidth)
}

func (m *Muxer) writeBlockGroup(packet Packet, blockTimecode int16, track Track) error {
	durationTicks := scaledDurationTicks(packet.DurationNS, m.options.TimecodeScaleNS)
	if durationTicks == 0 &&
		len(packet.ReferenceBlockTimeNS) == 0 &&
		packet.ReferencePriority == 0 &&
		packet.DiscardPaddingNS == 0 &&
		len(packet.CodecState) == 0 &&
		len(packet.BlockAdditions) == 0 {
		return m.writeBlock(idSimpleBlock, packet, blockTimecode, simpleBlockFlags(packet), track)
	}
	payloadSize := len(packet.Data)
	if track.Codec == CodecH264 && len(track.CodecPrivate) != 0 {
		var err error
		payloadSize, _, err = h264MuxedPayloadSize(track, packet.Data)
		if err != nil {
			return err
		}
	}
	trackWidth, err := ebml.UnsignedVINTWidth(uint64(packet.TrackID))
	if err != nil {
		return err
	}
	blockPayloadSize := uint64(trackWidth + 3 + payloadSize)
	blockHeaderSize, err := elementEncodedSize(idBlock, blockPayloadSize)
	if err != nil {
		return err
	}
	groupSize := blockHeaderSize + blockPayloadSize
	if len(packet.BlockAdditions) != 0 {
		additionsSize, err := blockAdditionsElementEncodedSize(packet.BlockAdditions)
		if err != nil {
			return err
		}
		groupSize, err = checkedAddUint64(groupSize, additionsSize)
		if err != nil {
			return err
		}
	}
	if durationTicks != 0 {
		durationElementSize, err := uintElementEncodedSize(idBlockDuration, durationTicks)
		if err != nil {
			return err
		}
		groupSize, err = checkedAddUint64(groupSize, durationElementSize)
		if err != nil {
			return err
		}
	}
	if packet.ReferencePriority != 0 {
		prioritySize, err := uintElementEncodedSize(idReferencePriority, packet.ReferencePriority)
		if err != nil {
			return err
		}
		groupSize, err = checkedAddUint64(groupSize, prioritySize)
		if err != nil {
			return err
		}
	}
	writeImplicitReference := durationTicks != 0 && !packet.Keyframe && len(packet.ReferenceBlockTimeNS) == 0
	if writeImplicitReference {
		referenceSize, err := intElementEncodedSize(idReferenceBlk, 0)
		if err != nil {
			return err
		}
		groupSize, err = checkedAddUint64(groupSize, referenceSize)
		if err != nil {
			return err
		}
	}
	for i := range packet.ReferenceBlockTimeNS {
		ticks := scaledReferenceBlockTicks(packet.ReferenceBlockTimeNS[i], m.options.TimecodeScaleNS)
		referenceSize, err := intElementEncodedSize(idReferenceBlk, ticks)
		if err != nil {
			return err
		}
		groupSize, err = checkedAddUint64(groupSize, referenceSize)
		if err != nil {
			return err
		}
	}
	if len(packet.CodecState) != 0 {
		stateSize, err := binaryElementEncodedSize(idCodecState, packet.CodecState)
		if err != nil {
			return err
		}
		groupSize, err = checkedAddUint64(groupSize, stateSize)
		if err != nil {
			return err
		}
	}
	if packet.DiscardPaddingNS != 0 {
		paddingSize, err := intElementEncodedSize(idDiscardPad, packet.DiscardPaddingNS)
		if err != nil {
			return err
		}
		groupSize, err = checkedAddUint64(groupSize, paddingSize)
		if err != nil {
			return err
		}
	}
	if err := m.ebml.WriteHeader(idBlockGroup, groupSize); err != nil {
		return err
	}
	if err := m.writeBlock(idBlock, packet, blockTimecode, blockFlags(packet), track); err != nil {
		return err
	}
	if len(packet.BlockAdditions) != 0 {
		if err := writeBlockAdditions(m.ebml, packet.BlockAdditions); err != nil {
			return err
		}
	}
	if durationTicks != 0 {
		if err := m.ebml.WriteUInt(idBlockDuration, durationTicks); err != nil {
			return err
		}
	}
	if packet.ReferencePriority != 0 {
		if err := m.ebml.WriteUInt(idReferencePriority, packet.ReferencePriority); err != nil {
			return err
		}
	}
	if writeImplicitReference {
		if err := m.ebml.WriteInt(idReferenceBlk, 0); err != nil {
			return err
		}
	}
	for i := range packet.ReferenceBlockTimeNS {
		ticks := scaledReferenceBlockTicks(packet.ReferenceBlockTimeNS[i], m.options.TimecodeScaleNS)
		if err := m.ebml.WriteInt(idReferenceBlk, ticks); err != nil {
			return err
		}
	}
	if len(packet.CodecState) != 0 {
		if err := writeBinary(m.ebml, idCodecState, packet.CodecState); err != nil {
			return err
		}
	}
	if packet.DiscardPaddingNS != 0 {
		if err := m.ebml.WriteInt(idDiscardPad, packet.DiscardPaddingNS); err != nil {
			return err
		}
	}
	return nil
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
	payloadSize := len(packet.Data)
	if track.Codec == CodecH264 && len(track.CodecPrivate) != 0 {
		var err error
		payloadSize, _, err = h264MuxedPayloadSize(track, packet.Data)
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
	if track.Codec == CodecH264 && len(track.CodecPrivate) != 0 {
		return h264WriteMuxedPayload(m.ebml, track, packet.Data, &m.blockScratch)
	}
	_, err = m.ebml.Write(packet.Data)
	return err
}

func (m *Muxer) writeLacedBlock(packet LacedPacket, blockTimecode int16, lacing byte, payloadSize uint64, track Track, muxedFrameSizes []int) error {
	if err := m.ebml.WriteHeader(idSimpleBlock, payloadSize); err != nil {
		return err
	}
	n, err := ebml.EncodeUnsignedVINT(m.blockScratch[:], uint64(packet.TrackID))
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint16(m.blockScratch[n:n+2], uint16(blockTimecode))
	m.blockScratch[n+2] = lacedBlockFlags(packet) | lacing
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
	for i := range packet.Frames {
		if track.Codec == CodecH264 && len(track.CodecPrivate) != 0 {
			if err := h264WriteMuxedPayload(m.ebml, track, packet.Frames[i], &m.blockScratch); err != nil {
				return err
			}
			continue
		}
		if _, err := m.ebml.Write(packet.Frames[i]); err != nil {
			return err
		}
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

func lacedBlockFlags(packet LacedPacket) byte {
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

func blockFlags(packet Packet) byte {
	if packet.Invisible {
		return simpleBlockInvisible
	}
	return 0
}

func scaledDurationTicks(durationNS int64, scaleNS int64) uint64 {
	if durationNS <= 0 || scaleNS <= 0 {
		return 0
	}
	value := durationNS / scaleNS
	if durationNS%scaleNS != 0 {
		value++
	}
	return uint64(value)
}

func validateReferenceBlockTimes(references []int64, scaleNS int64) error {
	if scaleNS <= 0 {
		return ErrInvalidData
	}
	for i := range references {
		_ = scaledReferenceBlockTicks(references[i], scaleNS)
	}
	return nil
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
		for j := 0; j < i; j++ {
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

func scaledReferenceBlockTicks(timeNS int64, scaleNS int64) int64 {
	return timeNS / scaleNS
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
	durationNS := m.defaultDurationNS(packet.TrackID)
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

func h264LacedFrameSizes(frames [][]byte, track Track, sizes []int) error {
	if len(sizes) != len(frames) {
		return ErrInvalidData
	}
	for i := range frames {
		size, _, err := h264MuxedPayloadSize(track, frames[i])
		if err != nil {
			return err
		}
		sizes[i] = size
	}
	return nil
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
		n := payloadSize
		if n > len(zero) {
			n = len(zero)
		}
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
		n := payloadSize
		if n > len(zero) {
			n = len(zero)
		}
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

func writeTrackEntry(w *ebml.Writer, track Track, scratch *[codecPrivateScratchSize]byte) error {
	var payload bytes.Buffer
	tw := ebml.NewWriter(&payload)
	if err := tw.WriteUInt(idTrackNumber, uint64(track.ID)); err != nil {
		return err
	}
	if err := tw.WriteUInt(idTrackUID, track.UID); err != nil {
		return err
	}
	switch track.Type {
	case TrackVideo:
		if err := tw.WriteUInt(idTrackType, matroskaTrackVideo); err != nil {
			return err
		}
	case TrackAudio:
		if err := tw.WriteUInt(idTrackType, matroskaTrackAudio); err != nil {
			return err
		}
	default:
		return ErrInvalidTrack
	}
	if err := tw.WriteUInt(idFlagEnabled, boolFlagUInt(track.FlagEnabled)); err != nil {
		return err
	}
	if err := tw.WriteUInt(idFlagDefault, boolFlagUInt(track.FlagDefault)); err != nil {
		return err
	}
	if err := tw.WriteUInt(idFlagForced, boolFlagUInt(track.FlagForced)); err != nil {
		return err
	}
	if err := writeOptionalBoolFlag(tw, idFlagHearingImpaired, track.FlagHearingImpaired, track.FlagHearingImpairedSet); err != nil {
		return err
	}
	if err := writeOptionalBoolFlag(tw, idFlagVisualImpaired, track.FlagVisualImpaired, track.FlagVisualImpairedSet); err != nil {
		return err
	}
	if err := writeOptionalBoolFlag(tw, idFlagTextDescriptions, track.FlagTextDescriptions, track.FlagTextDescriptionsSet); err != nil {
		return err
	}
	if err := writeOptionalBoolFlag(tw, idFlagOriginal, track.FlagOriginal, track.FlagOriginalSet); err != nil {
		return err
	}
	if err := writeOptionalBoolFlag(tw, idFlagCommentary, track.FlagCommentary, track.FlagCommentarySet); err != nil {
		return err
	}
	if err := tw.WriteUInt(idFlagLacing, boolFlagUInt(track.FlagLacing)); err != nil {
		return err
	}
	if track.Name != "" {
		if err := tw.WriteString(idName, track.Name); err != nil {
			return err
		}
	}
	if track.Language != "" {
		if err := tw.WriteString(idLanguage, track.Language); err != nil {
			return err
		}
	}
	if track.LanguageBCP47 != "" {
		if err := tw.WriteString(idLanguageBCP, track.LanguageBCP47); err != nil {
			return err
		}
	}
	codecID, err := matroskaCodecID(track.Codec)
	if err != nil {
		return err
	}
	if err := tw.WriteString(idCodecID, codecID); err != nil {
		return err
	}
	if track.CodecName != "" {
		if err := tw.WriteString(idCodecName, track.CodecName); err != nil {
			return err
		}
	}
	if track.MinCacheSet || track.MinCache != 0 {
		if err := tw.WriteUInt(idMinCache, track.MinCache); err != nil {
			return err
		}
	}
	if track.MaxCacheSet {
		if err := tw.WriteUInt(idMaxCache, track.MaxCache); err != nil {
			return err
		}
	}
	if track.DefaultDurationNS > 0 {
		if err := tw.WriteUInt(idDefaultDur, uint64(track.DefaultDurationNS)); err != nil {
			return err
		}
	}
	if track.DefaultDecodedFieldDurationNS > 0 {
		if err := tw.WriteUInt(idDefaultDecodedDur, uint64(track.DefaultDecodedFieldDurationNS)); err != nil {
			return err
		}
	}
	if track.MaxBlockAdditionID != 0 {
		if err := tw.WriteUInt(idMaxBlockAdditionID, track.MaxBlockAdditionID); err != nil {
			return err
		}
	}
	for i := range track.BlockAdditionMappings {
		if err := writeBlockAdditionMapping(tw, track.BlockAdditionMappings[i]); err != nil {
			return err
		}
	}
	if track.CodecDecodeAllSet {
		if err := tw.WriteUInt(idCodecDecodeAll, boolFlagUInt(track.CodecDecodeAll)); err != nil {
			return err
		}
	}
	for i := range track.TrackOverlays {
		if err := tw.WriteUInt(idTrackOverlay, track.TrackOverlays[i]); err != nil {
			return err
		}
	}
	private := track.CodecPrivate
	if len(private) == 0 {
		private = defaultCodecPrivate(track, scratch)
	}
	codecDelayNS, seekPreRollNS, err := trackCodecTiming(track, private)
	if err != nil {
		return err
	}
	if codecDelayNS > 0 || track.Codec == CodecOpus {
		if err := tw.WriteUInt(idCodecDelay, uint64(codecDelayNS)); err != nil {
			return err
		}
	}
	if seekPreRollNS > 0 || track.Codec == CodecOpus {
		if err := tw.WriteUInt(idSeekPreRoll, uint64(seekPreRollNS)); err != nil {
			return err
		}
	}
	for i := range track.TrackTranslates {
		if err := writeTrackTranslate(tw, track.TrackTranslates[i]); err != nil {
			return err
		}
	}
	if len(private) != 0 {
		if err := writeBinary(tw, idCodecPrivate, private); err != nil {
			return err
		}
	}
	switch track.Type {
	case TrackVideo:
		if err := writeVideo(tw, track.Video); err != nil {
			return err
		}
	case TrackAudio:
		if err := writeAudio(tw, track.Audio); err != nil {
			return err
		}
	}
	return w.WriteElement(idTrackEntry, payload.Bytes())
}

func writeTrackTranslate(w *ebml.Writer, translate TrackTranslate) error {
	if err := validateTrackTranslate(translate); err != nil {
		return err
	}
	var payload bytes.Buffer
	tw := ebml.NewWriter(&payload)
	if err := writeBinary(tw, idTrackTranslateTrack, translate.TrackID); err != nil {
		return err
	}
	if err := tw.WriteUInt(idTrackTranslateCodec, translate.Codec); err != nil {
		return err
	}
	for i := range translate.EditionUIDs {
		if err := tw.WriteUInt(idTrackTranslateEdit, translate.EditionUIDs[i]); err != nil {
			return err
		}
	}
	return w.WriteElement(idTrackTranslate, payload.Bytes())
}

func writeBlockAdditionMapping(w *ebml.Writer, mapping BlockAdditionMapping) error {
	var payload bytes.Buffer
	mw := ebml.NewWriter(&payload)
	if err := mw.WriteUInt(idBlockAddIDValue, mapping.IDValue); err != nil {
		return err
	}
	if mapping.Name != "" {
		if err := mw.WriteString(idBlockAddIDName, mapping.Name); err != nil {
			return err
		}
	}
	if err := mw.WriteUInt(idBlockAddIDType, mapping.Type); err != nil {
		return err
	}
	if mapping.ExtraData != nil {
		if err := writeBinary(mw, idBlockAddIDExtraData, mapping.ExtraData); err != nil {
			return err
		}
	}
	return w.WriteElement(idBlockAdditionMapping, payload.Bytes())
}

func trackCodecTiming(track Track, private []byte) (int64, int64, error) {
	codecDelayNS := track.CodecDelayNS
	seekPreRollNS := track.SeekPreRollNS
	if track.Codec != CodecOpus {
		return codecDelayNS, seekPreRollNS, nil
	}
	if codecDelayNS == 0 && len(private) != 0 {
		head, err := parseOpusHead(private)
		if err != nil {
			return 0, 0, err
		}
		codecDelayNS = opusCodecDelayNS(head.PreSkip)
	}
	if seekPreRollNS == 0 {
		seekPreRollNS = opusDefaultSeekPreRollNS
	}
	return codecDelayNS, seekPreRollNS, nil
}

func boolFlagUInt(value bool) uint64 {
	if value {
		return 1
	}
	return 0
}

func writeOptionalBoolFlag(w *ebml.Writer, id ebml.ID, value bool, set bool) error {
	if !set {
		return nil
	}
	return w.WriteUInt(id, boolFlagUInt(value))
}

func writeVideo(w *ebml.Writer, video VideoConfig) error {
	var payload bytes.Buffer
	vw := ebml.NewWriter(&payload)
	if err := writeOptionalUInt(vw, idFlagInterlaced, video.FlagInterlaced, video.FlagInterlacedSet); err != nil {
		return err
	}
	if err := writeOptionalUInt(vw, idFieldOrder, video.FieldOrder, video.FieldOrderSet); err != nil {
		return err
	}
	if video.StereoModeSet {
		if err := vw.WriteUInt(idStereoMode, uint64(video.StereoMode)); err != nil {
			return err
		}
	}
	if video.AlphaModeSet {
		if err := vw.WriteUInt(idAlphaMode, uint64(video.AlphaMode)); err != nil {
			return err
		}
	}
	if err := vw.WriteUInt(idPixelWidth, uint64(video.Width)); err != nil {
		return err
	}
	if err := vw.WriteUInt(idPixelHeight, uint64(video.Height)); err != nil {
		return err
	}
	if video.PixelCropBottom > 0 {
		if err := vw.WriteUInt(idPixelCropBottom, uint64(video.PixelCropBottom)); err != nil {
			return err
		}
	}
	if video.PixelCropTop > 0 {
		if err := vw.WriteUInt(idPixelCropTop, uint64(video.PixelCropTop)); err != nil {
			return err
		}
	}
	if video.PixelCropLeft > 0 {
		if err := vw.WriteUInt(idPixelCropLeft, uint64(video.PixelCropLeft)); err != nil {
			return err
		}
	}
	if video.PixelCropRight > 0 {
		if err := vw.WriteUInt(idPixelCropRight, uint64(video.PixelCropRight)); err != nil {
			return err
		}
	}
	if video.DisplayWidth > 0 {
		if err := vw.WriteUInt(idDisplayWidth, uint64(video.DisplayWidth)); err != nil {
			return err
		}
	}
	if video.DisplayHeight > 0 {
		if err := vw.WriteUInt(idDisplayHeight, uint64(video.DisplayHeight)); err != nil {
			return err
		}
	}
	if video.DisplayUnit > 0 {
		if err := vw.WriteUInt(idDisplayUnit, uint64(video.DisplayUnit)); err != nil {
			return err
		}
	}
	if err := writeOptionalUInt(vw, idAspectRatioType, video.AspectRatioType, video.AspectRatioTypeSet); err != nil {
		return err
	}
	if videoColourHasMetadata(video.Colour) {
		if err := writeColour(vw, video.Colour); err != nil {
			return err
		}
	}
	if videoProjectionHasMetadata(video.Projection) {
		if err := writeProjection(vw, video.Projection); err != nil {
			return err
		}
	}
	return w.WriteElement(idVideo, payload.Bytes())
}

func videoProjectionHasMetadata(projection VideoProjectionConfig) bool {
	return projection.Set ||
		projection.Type != 0 ||
		len(projection.Private) != 0 ||
		projection.PoseYaw != 0 ||
		projection.PosePitch != 0 ||
		projection.PoseRoll != 0
}

func writeProjection(w *ebml.Writer, projection VideoProjectionConfig) error {
	var payload bytes.Buffer
	pw := ebml.NewWriter(&payload)
	if err := pw.WriteUInt(idProjectionType, uint64(projection.Type)); err != nil {
		return err
	}
	if len(projection.Private) != 0 {
		if err := writeBinary(pw, idProjectionPrivate, projection.Private); err != nil {
			return err
		}
	}
	if err := pw.WriteFloat64(idProjectionPoseYaw, projection.PoseYaw); err != nil {
		return err
	}
	if err := pw.WriteFloat64(idProjectionPosePitch, projection.PosePitch); err != nil {
		return err
	}
	if err := pw.WriteFloat64(idProjectionPoseRoll, projection.PoseRoll); err != nil {
		return err
	}
	return w.WriteElement(idProjection, payload.Bytes())
}

func videoColourHasMetadata(colour VideoColourConfig) bool {
	return colour.MatrixCoefficientsSet ||
		colour.BitsPerChannelSet ||
		colour.ChromaSubsamplingHorzSet ||
		colour.ChromaSubsamplingVertSet ||
		colour.CbSubsamplingHorzSet ||
		colour.CbSubsamplingVertSet ||
		colour.ChromaSitingHorzSet ||
		colour.ChromaSitingVertSet ||
		colour.RangeSet ||
		colour.TransferCharacteristicsSet ||
		colour.PrimariesSet ||
		colour.MaxCLLSet ||
		colour.MaxFALLSet ||
		masteringMetadataHasValues(colour.MasteringMetadata)
}

func masteringMetadataHasValues(metadata VideoMasteringMetadataConfig) bool {
	return metadata.PrimaryRChromaticityXSet ||
		metadata.PrimaryRChromaticityYSet ||
		metadata.PrimaryGChromaticityXSet ||
		metadata.PrimaryGChromaticityYSet ||
		metadata.PrimaryBChromaticityXSet ||
		metadata.PrimaryBChromaticityYSet ||
		metadata.WhitePointChromaticityXSet ||
		metadata.WhitePointChromaticityYSet ||
		metadata.LuminanceMaxSet ||
		metadata.LuminanceMinSet
}

func writeColour(w *ebml.Writer, colour VideoColourConfig) error {
	var payload bytes.Buffer
	cw := ebml.NewWriter(&payload)
	if err := writeOptionalUInt(cw, idMatrixCoefficients, colour.MatrixCoefficients, colour.MatrixCoefficientsSet); err != nil {
		return err
	}
	if err := writeOptionalUInt(cw, idBitsPerChannel, colour.BitsPerChannel, colour.BitsPerChannelSet); err != nil {
		return err
	}
	if err := writeOptionalUInt(cw, idChromaSubsampleHorz, colour.ChromaSubsamplingHorz, colour.ChromaSubsamplingHorzSet); err != nil {
		return err
	}
	if err := writeOptionalUInt(cw, idChromaSubsampleVert, colour.ChromaSubsamplingVert, colour.ChromaSubsamplingVertSet); err != nil {
		return err
	}
	if err := writeOptionalUInt(cw, idCbSubsampleHorz, colour.CbSubsamplingHorz, colour.CbSubsamplingHorzSet); err != nil {
		return err
	}
	if err := writeOptionalUInt(cw, idCbSubsampleVert, colour.CbSubsamplingVert, colour.CbSubsamplingVertSet); err != nil {
		return err
	}
	if err := writeOptionalUInt(cw, idChromaSitingHorz, colour.ChromaSitingHorz, colour.ChromaSitingHorzSet); err != nil {
		return err
	}
	if err := writeOptionalUInt(cw, idChromaSitingVert, colour.ChromaSitingVert, colour.ChromaSitingVertSet); err != nil {
		return err
	}
	if err := writeOptionalUInt(cw, idColourRange, colour.Range, colour.RangeSet); err != nil {
		return err
	}
	if err := writeOptionalUInt(cw, idTransferChar, colour.TransferCharacteristics, colour.TransferCharacteristicsSet); err != nil {
		return err
	}
	if err := writeOptionalUInt(cw, idPrimaries, colour.Primaries, colour.PrimariesSet); err != nil {
		return err
	}
	if err := writeOptionalUInt(cw, idMaxCLL, colour.MaxCLL, colour.MaxCLLSet); err != nil {
		return err
	}
	if err := writeOptionalUInt(cw, idMaxFALL, colour.MaxFALL, colour.MaxFALLSet); err != nil {
		return err
	}
	if masteringMetadataHasValues(colour.MasteringMetadata) {
		if err := writeMasteringMetadata(cw, colour.MasteringMetadata); err != nil {
			return err
		}
	}
	return w.WriteElement(idColour, payload.Bytes())
}

func writeMasteringMetadata(w *ebml.Writer, metadata VideoMasteringMetadataConfig) error {
	var payload bytes.Buffer
	mw := ebml.NewWriter(&payload)
	if err := writeOptionalFloat64(mw, idPrimaryRX, metadata.PrimaryRChromaticityX, metadata.PrimaryRChromaticityXSet); err != nil {
		return err
	}
	if err := writeOptionalFloat64(mw, idPrimaryRY, metadata.PrimaryRChromaticityY, metadata.PrimaryRChromaticityYSet); err != nil {
		return err
	}
	if err := writeOptionalFloat64(mw, idPrimaryGX, metadata.PrimaryGChromaticityX, metadata.PrimaryGChromaticityXSet); err != nil {
		return err
	}
	if err := writeOptionalFloat64(mw, idPrimaryGY, metadata.PrimaryGChromaticityY, metadata.PrimaryGChromaticityYSet); err != nil {
		return err
	}
	if err := writeOptionalFloat64(mw, idPrimaryBX, metadata.PrimaryBChromaticityX, metadata.PrimaryBChromaticityXSet); err != nil {
		return err
	}
	if err := writeOptionalFloat64(mw, idPrimaryBY, metadata.PrimaryBChromaticityY, metadata.PrimaryBChromaticityYSet); err != nil {
		return err
	}
	if err := writeOptionalFloat64(mw, idWhitePointX, metadata.WhitePointChromaticityX, metadata.WhitePointChromaticityXSet); err != nil {
		return err
	}
	if err := writeOptionalFloat64(mw, idWhitePointY, metadata.WhitePointChromaticityY, metadata.WhitePointChromaticityYSet); err != nil {
		return err
	}
	if err := writeOptionalFloat64(mw, idLuminanceMax, metadata.LuminanceMax, metadata.LuminanceMaxSet); err != nil {
		return err
	}
	if err := writeOptionalFloat64(mw, idLuminanceMin, metadata.LuminanceMin, metadata.LuminanceMinSet); err != nil {
		return err
	}
	return w.WriteElement(idMasteringMetadata, payload.Bytes())
}

func writeOptionalUInt(w *ebml.Writer, id ebml.ID, value int, set bool) error {
	if !set {
		return nil
	}
	if value < 0 {
		return ErrInvalidTrack
	}
	return w.WriteUInt(id, uint64(value))
}

func writeOptionalFloat64(w *ebml.Writer, id ebml.ID, value float64, set bool) error {
	if !set {
		return nil
	}
	return w.WriteFloat64(id, value)
}

func writeAudio(w *ebml.Writer, audio AudioConfig) error {
	var payload bytes.Buffer
	aw := ebml.NewWriter(&payload)
	sampleRate := audio.SampleRate
	if sampleRate == 0 {
		sampleRate = 48000
	}
	channels := audio.Channels
	if channels == 0 {
		channels = 2
	}
	if err := aw.WriteFloat64(idSamplingFreq, float64(sampleRate)); err != nil {
		return err
	}
	if audio.OutputSampleRate > 0 {
		if err := aw.WriteFloat64(idOutputFreq, float64(audio.OutputSampleRate)); err != nil {
			return err
		}
	}
	if err := aw.WriteUInt(idChannels, uint64(channels)); err != nil {
		return err
	}
	if audio.BitDepth > 0 {
		if err := aw.WriteUInt(idBitDepth, uint64(audio.BitDepth)); err != nil {
			return err
		}
	}
	return w.WriteElement(idAudio, payload.Bytes())
}

func validateTrack(track Track) error {
	if track.Type != TrackAudio && track.Type != TrackVideo {
		return ErrInvalidTrack
	}
	if track.DefaultDurationNS < 0 || track.DefaultDecodedFieldDurationNS < 0 || track.CodecDelayNS < 0 || track.SeekPreRollNS < 0 {
		return ErrInvalidTrack
	}
	for i := range track.TrackOverlays {
		if track.TrackOverlays[i] == 0 {
			return ErrInvalidTrack
		}
	}
	for i := range track.TrackTranslates {
		if err := validateTrackTranslate(track.TrackTranslates[i]); err != nil {
			return err
		}
	}
	maxMappingID, err := maxBlockAdditionMappingID(track.BlockAdditionMappings)
	if err != nil || maxMappingID > track.MaxBlockAdditionID {
		return ErrInvalidTrack
	}
	if _, err := matroskaCodecID(track.Codec); err != nil {
		return err
	}
	switch track.Type {
	case TrackAudio:
		if track.Audio.SampleRate < 0 || track.Audio.OutputSampleRate < 0 || track.Audio.Channels < 0 || track.Audio.BitDepth < 0 {
			return ErrInvalidTrack
		}
		switch track.Codec {
		case CodecOpus:
			if len(track.CodecPrivate) == 0 && !canWriteDefaultOpusHead(track) {
				return ErrInvalidTrack
			}
		case CodecPCMU, CodecPCMA:
		default:
			return ErrInvalidTrack
		}
	case TrackVideo:
		if track.Video.Width < 0 || track.Video.Height < 0 ||
			track.Video.FlagInterlaced < 0 || track.Video.FieldOrder < 0 ||
			track.Video.StereoMode < 0 || track.Video.AlphaMode < 0 ||
			track.Video.PixelCropBottom < 0 || track.Video.PixelCropTop < 0 ||
			track.Video.PixelCropLeft < 0 || track.Video.PixelCropRight < 0 ||
			track.Video.DisplayWidth < 0 || track.Video.DisplayHeight < 0 ||
			track.Video.DisplayUnit < 0 || track.Video.AspectRatioType < 0 {
			return ErrInvalidTrack
		}
		if err := validateVideoColour(track.Video.Colour); err != nil {
			return err
		}
		if err := validateVideoProjection(track.Video.Projection); err != nil {
			return err
		}
		switch track.Codec {
		case CodecAV1:
			if len(track.CodecPrivate) != 0 {
				if _, err := parseAV1CodecConfigurationRecord(track.CodecPrivate); err != nil {
					return ErrInvalidTrack
				}
			}
		case CodecH264:
			if len(track.CodecPrivate) != 0 {
				if _, err := parseAVCDecoderConfigurationRecord(track.CodecPrivate); err != nil {
					return ErrInvalidTrack
				}
			}
		case CodecVP8, CodecVP9, CodecH265:
		default:
			return ErrInvalidTrack
		}
	}
	if (track.TimebaseNum == 0) != (track.TimebaseDen == 0) ||
		track.TimebaseNum < 0 || track.TimebaseDen < 0 {
		return ErrInvalidTrack
	}
	return nil
}

func validateTrackTranslate(translate TrackTranslate) error {
	if translate.TrackID == nil {
		return ErrInvalidTrack
	}
	return nil
}

func validateVideoColour(colour VideoColourConfig) error {
	if colour.MatrixCoefficients < 0 ||
		colour.BitsPerChannel < 0 ||
		colour.ChromaSubsamplingHorz < 0 ||
		colour.ChromaSubsamplingVert < 0 ||
		colour.CbSubsamplingHorz < 0 ||
		colour.CbSubsamplingVert < 0 ||
		colour.ChromaSitingHorz < 0 ||
		colour.ChromaSitingVert < 0 ||
		colour.Range < 0 ||
		colour.TransferCharacteristics < 0 ||
		colour.Primaries < 0 ||
		colour.MaxCLL < 0 ||
		colour.MaxFALL < 0 {
		return ErrInvalidTrack
	}
	return validateMasteringMetadata(colour.MasteringMetadata)
}

func validateVideoProjection(projection VideoProjectionConfig) error {
	if projection.Type < 0 {
		return ErrInvalidTrack
	}
	if len(projection.Private) != 0 && projection.Type == 0 {
		return ErrInvalidTrack
	}
	if projection.Type >= 1 && projection.Type <= 3 && len(projection.Private) == 0 {
		return ErrInvalidTrack
	}
	if err := validateProjectionPose(projection.PoseYaw, -180, 180); err != nil {
		return err
	}
	if err := validateProjectionPose(projection.PosePitch, -90, 90); err != nil {
		return err
	}
	return validateProjectionPose(projection.PoseRoll, -180, 180)
}

func validateProjectionPose(value float64, min float64, max float64) error {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < min || value > max {
		return ErrInvalidTrack
	}
	return nil
}

func validateMasteringMetadata(metadata VideoMasteringMetadataConfig) error {
	if err := validateChromaticity(metadata.PrimaryRChromaticityX, metadata.PrimaryRChromaticityXSet); err != nil {
		return err
	}
	if err := validateChromaticity(metadata.PrimaryRChromaticityY, metadata.PrimaryRChromaticityYSet); err != nil {
		return err
	}
	if err := validateChromaticity(metadata.PrimaryGChromaticityX, metadata.PrimaryGChromaticityXSet); err != nil {
		return err
	}
	if err := validateChromaticity(metadata.PrimaryGChromaticityY, metadata.PrimaryGChromaticityYSet); err != nil {
		return err
	}
	if err := validateChromaticity(metadata.PrimaryBChromaticityX, metadata.PrimaryBChromaticityXSet); err != nil {
		return err
	}
	if err := validateChromaticity(metadata.PrimaryBChromaticityY, metadata.PrimaryBChromaticityYSet); err != nil {
		return err
	}
	if err := validateChromaticity(metadata.WhitePointChromaticityX, metadata.WhitePointChromaticityXSet); err != nil {
		return err
	}
	if err := validateChromaticity(metadata.WhitePointChromaticityY, metadata.WhitePointChromaticityYSet); err != nil {
		return err
	}
	if err := validateNonNegativeFloat(metadata.LuminanceMax, metadata.LuminanceMaxSet); err != nil {
		return err
	}
	return validateNonNegativeFloat(metadata.LuminanceMin, metadata.LuminanceMinSet)
}

func validateChromaticity(value float64, set bool) error {
	if !set {
		return nil
	}
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 1 {
		return ErrInvalidTrack
	}
	return nil
}

func validateNonNegativeFloat(value float64, set bool) error {
	if !set {
		return nil
	}
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return ErrInvalidTrack
	}
	return nil
}

func canWriteDefaultOpusHead(track Track) bool {
	channels := track.Audio.Channels
	if channels == 0 {
		channels = 2
	}
	sampleRate := track.Audio.SampleRate
	if sampleRate == 0 {
		sampleRate = 48000
	}
	return channels >= 1 && channels <= 2 && uint64(sampleRate) <= uint64(^uint32(0))
}

const timeNS = 1000000000
