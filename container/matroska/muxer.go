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
	scratch         [18]byte
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
	for i := range m.tracks {
		if m.tracks[i].ID == track.ID {
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

func (m *Muxer) WritePacket(packet Packet) error {
	if m == nil || m.writer == nil {
		return ErrNilWriter
	}
	if m.closed {
		return ErrClosed
	}
	if packet.TrackID == 0 || !m.hasTrack(packet.TrackID) {
		return ErrUnknownTrack
	}
	if packet.TimeNS < 0 {
		return ErrInvalidData
	}
	if packet.DurationNS < 0 || (packet.DurationNS > 0 && packet.TimeNS > math.MaxInt64-packet.DurationNS) {
		return ErrInvalidData
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
	if err := m.writeSimpleBlock(packet, int16(delta)); err != nil {
		return err
	}
	m.updateMaxTime(packet)
	m.addCue(packet, timecode)
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
	if packet.TrackID == 0 || !m.hasTrack(packet.TrackID) {
		return ErrUnknownTrack
	}
	if packet.TimeNS < 0 {
		return ErrInvalidData
	}
	lacing, err := lacingFlag(packet.Lacing, packet.Frames)
	if err != nil {
		return err
	}
	payloadSize, err := m.lacedBlockPayloadSize(packet, lacing)
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
	if err := m.writeLacedBlock(packet, int16(delta), lacing, payloadSize); err != nil {
		return err
	}
	if endTime > m.maxTimeNS {
		m.maxTimeNS = endTime
	}
	m.addCue(Packet{TrackID: packet.TrackID, TimeNS: packet.TimeNS, Keyframe: packet.Keyframe}, timecode)
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
	for i := range m.tracks {
		if m.tracks[i].ID == id {
			return true
		}
	}
	return false
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

func (m *Muxer) writeSimpleBlock(packet Packet, blockTimecode int16) error {
	if packet.DurationNS > 0 {
		return m.writeBlockGroup(packet, blockTimecode)
	}
	return m.writeBlock(idSimpleBlock, packet, blockTimecode, simpleBlockFlags(packet))
}

func (m *Muxer) addCue(packet Packet, timecode int64) {
	if !m.collectsCues() || !packet.Keyframe {
		return
	}
	m.cues = append(m.cues, CuePoint{
		TrackID:         packet.TrackID,
		TimeNS:          timecode * m.options.TimecodeScaleNS,
		ClusterPosition: m.clusterPosition,
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

func (m *Muxer) writeBlockGroup(packet Packet, blockTimecode int16) error {
	durationTicks := scaledDurationTicks(packet.DurationNS, m.options.TimecodeScaleNS)
	if durationTicks == 0 {
		return m.writeBlock(idSimpleBlock, packet, blockTimecode, simpleBlockFlags(packet))
	}
	trackWidth, err := ebml.UnsignedVINTWidth(uint64(packet.TrackID))
	if err != nil {
		return err
	}
	blockPayloadSize := uint64(trackWidth + 3 + len(packet.Data))
	blockHeaderSize, err := elementEncodedSize(idBlock, blockPayloadSize)
	if err != nil {
		return err
	}
	durationElementSize, err := uintElementEncodedSize(idBlockDuration, durationTicks)
	if err != nil {
		return err
	}
	groupSize := blockHeaderSize + blockPayloadSize + durationElementSize
	if !packet.Keyframe {
		referenceSize, err := intElementEncodedSize(idReferenceBlk, 0)
		if err != nil {
			return err
		}
		groupSize += referenceSize
	}
	if err := m.ebml.WriteHeader(idBlockGroup, groupSize); err != nil {
		return err
	}
	if !packet.Keyframe {
		if err := m.ebml.WriteInt(idReferenceBlk, 0); err != nil {
			return err
		}
	}
	if err := m.ebml.WriteUInt(idBlockDuration, durationTicks); err != nil {
		return err
	}
	return m.writeBlock(idBlock, packet, blockTimecode, blockFlags(packet))
}

func (m *Muxer) writeBlock(id ebml.ID, packet Packet, blockTimecode int16, flags byte) error {
	trackWidth, err := ebml.UnsignedVINTWidth(uint64(packet.TrackID))
	if err != nil {
		return err
	}
	size := uint64(trackWidth + 3 + len(packet.Data))
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
	_, err = m.ebml.Write(packet.Data)
	return err
}

func (m *Muxer) writeLacedBlock(packet LacedPacket, blockTimecode int16, lacing byte, payloadSize uint64) error {
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
		if err := m.writeXiphLaceSizes(packet.Frames); err != nil {
			return err
		}
	case simpleBlockLacingEBML:
		if err := m.writeEBMLLaceSizes(packet.Frames); err != nil {
			return err
		}
	case simpleBlockLacingFixed:
	default:
		return ErrUnsupportedLacing
	}
	for i := range packet.Frames {
		if _, err := m.ebml.Write(packet.Frames[i]); err != nil {
			return err
		}
	}
	return nil
}

func (m *Muxer) writeXiphLaceSizes(frames [][]byte) error {
	for i := 0; i < len(frames)-1; i++ {
		size := len(frames[i])
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

func (m *Muxer) writeEBMLLaceSizes(frames [][]byte) error {
	n, err := ebml.EncodeUnsignedVINT(m.blockScratch[:], uint64(len(frames[0])))
	if err != nil {
		return err
	}
	if _, err := m.ebml.Write(m.blockScratch[:n]); err != nil {
		return err
	}
	previous := len(frames[0])
	for i := 1; i < len(frames)-1; i++ {
		delta := len(frames[i]) - previous
		n, err := encodeSignedLaceSizeVINT(m.blockScratch[:], int64(delta))
		if err != nil {
			return err
		}
		if _, err := m.ebml.Write(m.blockScratch[:n]); err != nil {
			return err
		}
		previous = len(frames[i])
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

func lacingFlag(mode LacingMode, frames [][]byte) (byte, error) {
	if len(frames) < 2 || len(frames) > 256 {
		return 0, ErrInvalidData
	}
	switch mode {
	case LacingAuto:
		if equalFrameSizes(frames) {
			return simpleBlockLacingFixed, nil
		}
		return simpleBlockLacingEBML, nil
	case LacingXiph:
		return simpleBlockLacingXiph, nil
	case LacingEBML:
		return simpleBlockLacingEBML, nil
	case LacingFixed:
		if !equalFrameSizes(frames) {
			return 0, ErrInvalidData
		}
		return simpleBlockLacingFixed, nil
	default:
		return 0, ErrUnsupportedLacing
	}
}

func equalFrameSizes(frames [][]byte) bool {
	if len(frames) == 0 {
		return true
	}
	size := len(frames[0])
	for i := 1; i < len(frames); i++ {
		if len(frames[i]) != size {
			return false
		}
	}
	return true
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

func (m *Muxer) lacedBlockPayloadSize(packet LacedPacket, lacing byte) (uint64, error) {
	trackWidth, err := ebml.UnsignedVINTWidth(uint64(packet.TrackID))
	if err != nil {
		return 0, err
	}
	size := uint64(trackWidth + 3 + 1)
	laceSize, err := laceSizePayloadWidth(packet.Frames, lacing)
	if err != nil {
		return 0, err
	}
	size, err = checkedAddUint64(size, laceSize)
	if err != nil {
		return 0, err
	}
	for i := range packet.Frames {
		size, err = checkedAddUint64(size, uint64(len(packet.Frames[i])))
		if err != nil {
			return 0, err
		}
	}
	return size, nil
}

func laceSizePayloadWidth(frames [][]byte, lacing byte) (uint64, error) {
	switch lacing {
	case simpleBlockLacingXiph:
		var size uint64
		for i := 0; i < len(frames)-1; i++ {
			size += uint64(len(frames[i])/255 + 1)
		}
		return size, nil
	case simpleBlockLacingFixed:
		if !equalFrameSizes(frames) {
			return 0, ErrInvalidData
		}
		return 0, nil
	case simpleBlockLacingEBML:
		width, err := ebml.UnsignedVINTWidth(uint64(len(frames[0])))
		if err != nil {
			return 0, err
		}
		size := uint64(width)
		previous := len(frames[0])
		for i := 1; i < len(frames)-1; i++ {
			delta := len(frames[i]) - previous
			width, err := signedLaceSizeVINTWidth(int64(delta))
			if err != nil {
				return 0, err
			}
			size += uint64(width)
			previous = len(frames[i])
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

func writeTrackEntry(w *ebml.Writer, track Track, scratch *[18]byte) error {
	var payload bytes.Buffer
	tw := ebml.NewWriter(&payload)
	if err := tw.WriteUInt(idTrackNumber, uint64(track.ID)); err != nil {
		return err
	}
	if err := tw.WriteUInt(idTrackUID, uint64(track.ID)); err != nil {
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
	if err := tw.WriteUInt(idFlagEnabled, 1); err != nil {
		return err
	}
	if err := tw.WriteUInt(idFlagDefault, 1); err != nil {
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
	if track.DefaultDurationNS < 0 {
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
		case CodecOpus, CodecPCMU, CodecPCMA:
		default:
			return ErrInvalidTrack
		}
	case TrackVideo:
		switch track.Codec {
		case CodecVP8, CodecVP9, CodecAV1, CodecH264, CodecH265:
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

const timeNS = 1000000000
