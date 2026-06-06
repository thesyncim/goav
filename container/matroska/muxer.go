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
	headerWritten   bool
	clusterOpen     bool
	seekable        bool
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
	m.headerWritten = false
	m.clusterOpen = false
	_, m.seekable = w.(io.Seeker)
	m.segmentPatch = ebml.SizePatch{}
	m.segmentSized = false
	m.clusterPatch = ebml.SizePatch{}
	m.clusterSized = false
	m.durationOffset = 0
	m.durationPatch = false
	m.maxTimeNS = 0
	m.clusterTimecode = 0
	m.closed = false
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
	m.updateMaxTime(packet)
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
	return m.writeSimpleBlock(packet, int16(delta))
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
	if m.durationPatch {
		if err := m.patchDuration(); err != nil {
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
	} else {
		if err := m.ebml.WriteUnknownHeader(idSegment, ebml.MaxSizeWidth); err != nil {
			return err
		}
	}
	if err := m.writeInfo(); err != nil {
		return err
	}
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

func (m *Muxer) writeSimpleBlock(packet Packet, blockTimecode int16) error {
	if packet.DurationNS > 0 {
		return m.writeBlockGroup(packet, blockTimecode)
	}
	return m.writeBlock(idSimpleBlock, packet, blockTimecode, simpleBlockFlags(packet))
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
	if _, err := matroskaCodecID(track.Codec); err != nil {
		return err
	}
	switch track.Type {
	case TrackAudio:
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
	if (track.TimebaseNum == 0) != (track.TimebaseDen == 0) {
		return ErrInvalidTrack
	}
	return nil
}

const timeNS = 1000000000
