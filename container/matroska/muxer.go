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
	cuesPosition    uint64
	clusterPosition uint64
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
	m := &Muxer{}
	m.init(w, opts)
	return m, nil
}

func (m *Muxer) init(w io.Writer, opts MuxerOptions) {
	m.writer = w
	m.ebml = ebml.NewWriter(w)
	m.options = normalizeMuxerOptions(opts)
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
	m.cuesPosition = 0
	m.clusterPosition = 0
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
	if len(track.CodecPrivate) != 0 {
		track.CodecPrivate = append([]byte(nil), track.CodecPrivate...)
	}
	m.tracks = append(m.tracks, track)
	return track.ID, nil
}

func normalizeTrackIdentity(track *Track) {
	if track.UID == 0 {
		track.UID = uint64(track.ID)
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
	if !m.headerWritten {
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
	if err := m.writeSimpleBlock(packet, int16(delta), track); err != nil {
		return err
	}
	m.updateMaxTime(packet)
	m.addCue(packet, timecode, relativePosition)
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
	if err := m.writeLacedBlock(packet, int16(delta), lacing, payloadSize, track, muxedFrameSizes); err != nil {
		return err
	}
	if endTime > m.maxTimeNS {
		m.maxTimeNS = endTime
	}
	m.addCue(Packet{TrackID: packet.TrackID, TimeNS: packet.TimeNS, Keyframe: packet.Keyframe}, timecode, relativePosition)
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
	if err := w.WriteUInt(idTimestampScale, uint64(m.options.TimecodeScaleNS)); err != nil {
		return err
	}
	if err := w.WriteString(idMuxingApp, m.options.MuxingApp); err != nil {
		return err
	}
	if err := w.WriteString(idWritingApp, m.options.WritingApp); err != nil {
		return err
	}
	return m.ebml.WriteElement(idInfo, payload.Bytes())
}

func (m *Muxer) writeSeekableInfo() error {
	patch, err := m.ebml.StartSizedElement(idInfo, 4)
	if err != nil {
		return err
	}
	if err := m.ebml.WriteUInt(idTimestampScale, uint64(m.options.TimecodeScaleNS)); err != nil {
		return err
	}
	if err := m.ebml.WriteHeader(idDuration, 8); err != nil {
		return err
	}
	m.durationOffset = m.ebml.Offset()
	clear(m.blockScratch[:8])
	if _, err := m.ebml.Write(m.blockScratch[:8]); err != nil {
		return err
	}
	m.durationPatch = true
	if err := m.ebml.WriteString(idMuxingApp, m.options.MuxingApp); err != nil {
		return err
	}
	if err := m.ebml.WriteString(idWritingApp, m.options.WritingApp); err != nil {
		return err
	}
	return m.ebml.FinishSizedElement(patch)
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
	return nil
}

func (m *Muxer) writeSeekHeadPlaceholder() error {
	const reserve = 512
	m.seekHeadOffset = m.ebml.Offset()
	m.seekHeadReserve = reserve
	return writeVoidTotal(m.ebml, reserve)
}

func (m *Muxer) writeSimpleBlock(packet Packet, blockTimecode int16, track Track) error {
	if packet.DurationNS > 0 || len(packet.ReferenceBlockTimeNS) != 0 || packet.DiscardPaddingNS != 0 {
		return m.writeBlockGroup(packet, blockTimecode, track)
	}
	return m.writeBlock(idSimpleBlock, packet, blockTimecode, simpleBlockFlags(packet), track)
}

func (m *Muxer) addCue(packet Packet, timecode int64, relativePosition uint64) {
	if !m.collectsCues() || !packet.Keyframe {
		return
	}
	m.cues = append(m.cues, CuePoint{
		TrackID:             packet.TrackID,
		TimeNS:              timecode * m.options.TimecodeScaleNS,
		ClusterPosition:     m.clusterPosition,
		RelativePosition:    relativePosition,
		RelativePositionSet: true,
	})
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
	var payload bytes.Buffer
	cw := ebml.NewWriter(&payload)
	if err := cw.WriteUInt(idCueTime, uint64(cue.TimeNS/scaleNS)); err != nil {
		return err
	}
	var positions bytes.Buffer
	pw := ebml.NewWriter(&positions)
	if err := pw.WriteUInt(idCueTrack, uint64(cue.TrackID)); err != nil {
		return err
	}
	if err := pw.WriteUInt(idCueClusterPosition, cue.ClusterPosition); err != nil {
		return err
	}
	if cue.RelativePositionSet {
		if err := pw.WriteUInt(idCueRelativePos, cue.RelativePosition); err != nil {
			return err
		}
	}
	if err := cw.WriteElement(idCueTrackPositions, positions.Bytes()); err != nil {
		return err
	}
	return w.WriteElement(idCuePoint, payload.Bytes())
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
	if durationTicks == 0 && len(packet.ReferenceBlockTimeNS) == 0 && packet.DiscardPaddingNS == 0 {
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
	if durationTicks != 0 {
		durationElementSize, err := uintElementEncodedSize(idBlockDuration, durationTicks)
		if err != nil {
			return err
		}
		groupSize += durationElementSize
	}
	writeImplicitReference := durationTicks != 0 && !packet.Keyframe && len(packet.ReferenceBlockTimeNS) == 0
	if writeImplicitReference {
		referenceSize, err := intElementEncodedSize(idReferenceBlk, 0)
		if err != nil {
			return err
		}
		groupSize += referenceSize
	}
	for i := range packet.ReferenceBlockTimeNS {
		ticks := scaledReferenceBlockTicks(packet.ReferenceBlockTimeNS[i], m.options.TimecodeScaleNS)
		referenceSize, err := intElementEncodedSize(idReferenceBlk, ticks)
		if err != nil {
			return err
		}
		groupSize += referenceSize
	}
	if packet.DiscardPaddingNS != 0 {
		paddingSize, err := intElementEncodedSize(idDiscardPad, packet.DiscardPaddingNS)
		if err != nil {
			return err
		}
		groupSize += paddingSize
	}
	if err := m.ebml.WriteHeader(idBlockGroup, groupSize); err != nil {
		return err
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
	if durationTicks != 0 {
		if err := m.ebml.WriteUInt(idBlockDuration, durationTicks); err != nil {
			return err
		}
	}
	if packet.DiscardPaddingNS != 0 {
		if err := m.ebml.WriteInt(idDiscardPad, packet.DiscardPaddingNS); err != nil {
			return err
		}
	}
	return m.writeBlock(idBlock, packet, blockTimecode, blockFlags(packet), track)
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
	if track.DefaultDurationNS > 0 {
		if err := tw.WriteUInt(idDefaultDur, uint64(track.DefaultDurationNS)); err != nil {
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

func writeVideo(w *ebml.Writer, video VideoConfig) error {
	var payload bytes.Buffer
	vw := ebml.NewWriter(&payload)
	if err := vw.WriteUInt(idPixelWidth, uint64(video.Width)); err != nil {
		return err
	}
	if err := vw.WriteUInt(idPixelHeight, uint64(video.Height)); err != nil {
		return err
	}
	return w.WriteElement(idVideo, payload.Bytes())
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
	if track.DefaultDurationNS < 0 || track.CodecDelayNS < 0 || track.SeekPreRollNS < 0 {
		return ErrInvalidTrack
	}
	if _, err := matroskaCodecID(track.Codec); err != nil {
		return err
	}
	switch track.Type {
	case TrackAudio:
		if track.Audio.SampleRate < 0 || track.Audio.Channels < 0 || track.Audio.BitDepth < 0 {
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
		if track.Video.Width < 0 || track.Video.Height < 0 {
			return ErrInvalidTrack
		}
	}
	if (track.TimebaseNum == 0) != (track.TimebaseDen == 0) ||
		track.TimebaseNum < 0 || track.TimebaseDen < 0 {
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
