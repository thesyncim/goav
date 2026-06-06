package matroska

import (
	"encoding/binary"
	"errors"
	"hash"
	"hash/crc32"
	"io"
	"math"

	"github.com/thesyncim/goav/container/ebml"
)

type Demuxer struct {
	reader          *ebml.Reader
	seeker          io.ReadSeeker
	options         DemuxerOptions
	docType         string
	segmentData     int64
	timecodeScaleNS int64
	info            SegmentInfo
	tracks          []Track
	attachments     []Attachment
	cues            []CuePoint
	seekEntries     []SeekEntry
	inSegment       bool
	inCluster       bool
	clusterUnknown  bool
	clusterEnd      int64
	clusterTimecode int64
	blockLimit      io.LimitedReader
	groupLimit      io.LimitedReader
	groupReader     *ebml.Reader
	blockHeader     [3]byte
	laceBuffer      []byte
	laceFrames      []laceFrame
	laceTrackID     uint32
	laceH264Length  int
	laceTimeNS      int64
	laceDurationNS  int64
	laceFrameCount  int
	laceFrameIndex  int
	laceKeyframe    bool
	laceInvisible   bool
	laceDiscardable bool
	scratch         [ebml.MaxSizeWidth]byte
	uintScratch     [8]byte
}

type laceFrame struct {
	offset int
	size   int
}

const (
	maxTrackID  = uint64(^uint32(0))
	maxIntValue = uint64(^uint(0) >> 1)
)

func NewDemuxer(r io.Reader, opts DemuxerOptions) (*Demuxer, error) {
	if r == nil {
		return nil, ErrNilReader
	}
	d := &Demuxer{}
	if err := d.init(r, opts); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *Demuxer) init(r io.Reader, opts DemuxerOptions) error {
	d.reader = ebml.NewReader(r, ebml.ReaderOptions{MaxElementSize: opts.MaxElementSize})
	d.seeker, _ = r.(io.ReadSeeker)
	if d.groupReader == nil {
		d.groupReader = ebml.NewReader(&d.groupLimit, ebml.ReaderOptions{MaxElementSize: opts.MaxElementSize})
	}
	d.options = opts
	if d.options.MaxLaceFrames <= 0 {
		d.options.MaxLaceFrames = defaultMaxLaceFrames
	}
	if d.options.MaxLacePayload <= 0 {
		d.options.MaxLacePayload = defaultMaxLacePayload
	}
	if cap(d.laceFrames) < d.options.MaxLaceFrames {
		d.laceFrames = make([]laceFrame, d.options.MaxLaceFrames)
	}
	d.laceFrames = d.laceFrames[:d.options.MaxLaceFrames]
	if cap(d.laceBuffer) < d.options.MaxLacePayload {
		d.laceBuffer = make([]byte, d.options.MaxLacePayload)
	}
	d.laceBuffer = d.laceBuffer[:d.options.MaxLacePayload]
	d.docType = ""
	d.segmentData = 0
	d.timecodeScaleNS = defaultTimecodeScaleNS
	d.info = SegmentInfo{}
	d.tracks = d.tracks[:0]
	d.attachments = d.attachments[:0]
	d.cues = d.cues[:0]
	d.seekEntries = d.seekEntries[:0]
	d.inSegment = false
	d.inCluster = false
	d.clusterUnknown = false
	d.clusterEnd = 0
	d.clusterTimecode = 0
	d.clearLace()
	return d.readPreamble()
}

func (d *Demuxer) SeekToTime(timeNS int64) error {
	if d == nil || d.reader == nil {
		return ErrNilReader
	}
	if d.seeker == nil {
		return ErrNonSeekableReader
	}
	if timeNS < 0 {
		return ErrInvalidData
	}
	if len(d.cues) == 0 {
		if err := d.loadCuesFromSeekHead(); err != nil {
			return err
		}
	}
	if len(d.cues) == 0 {
		return ErrInvalidData
	}
	cue := d.cues[0]
	for i := range d.cues {
		if d.cues[i].TimeNS > timeNS {
			break
		}
		cue = d.cues[i]
	}
	offset := d.segmentData + int64(cue.ClusterPosition)
	if _, err := d.seeker.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	d.reader.Reset(d.seeker, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize})
	header, err := d.reader.ReadHeader()
	if err != nil {
		return err
	}
	if header.ID != idCluster {
		return ErrInvalidData
	}
	if err := d.enterCluster(header); err != nil {
		return err
	}
	if cue.RelativePositionSet {
		if err := d.seekToCueRelativePosition(header, cue.RelativePosition); err != nil {
			return err
		}
	}
	d.clearLace()
	return nil
}

// ReadPacketAtTime seeks to the nearest preceding cue and reads forward until
// it finds the first packet at or after timeNS.
func (d *Demuxer) ReadPacketAtTime(timeNS int64, dst *Packet) error {
	if d == nil || d.reader == nil {
		return ErrNilReader
	}
	if dst == nil {
		return ErrNilPacket
	}
	if err := d.SeekToTime(timeNS); err != nil {
		return err
	}
	for {
		if err := d.ReadPacket(dst); err != nil {
			return err
		}
		if dst.TimeNS >= timeNS {
			return nil
		}
	}
}

func (d *Demuxer) seekToCueRelativePosition(cluster ebml.Header, relativePosition uint64) error {
	if relativePosition > uint64(math.MaxInt64) {
		return ErrInvalidData
	}
	target := cluster.DataOffset + int64(relativePosition)
	if target < cluster.DataOffset || (!cluster.Size.Unknown && target >= cluster.DataOffset+int64(cluster.Size.Value)) {
		return ErrInvalidData
	}
	for d.reader.Offset() < target {
		header, err := d.reader.ReadHeader()
		if err != nil {
			return err
		}
		end := header.DataOffset + int64(header.Size.Value)
		if header.Size.Unknown || end > target {
			return ErrInvalidData
		}
		switch header.ID {
		case idTimestamp:
			value, err := readUIntPayloadScratch(d.reader, header.Size.Value, &d.uintScratch)
			if err != nil {
				return err
			}
			if value > uint64(math.MaxInt64) {
				return ErrInvalidData
			}
			d.clusterTimecode = int64(value)
		default:
			if err := skipElement(d.reader, header); err != nil {
				return err
			}
		}
	}
	if d.reader.Offset() != target {
		return ErrInvalidData
	}
	return nil
}

func (d *Demuxer) Tracks() []Track {
	if d == nil || len(d.tracks) == 0 {
		return nil
	}
	tracks := make([]Track, len(d.tracks))
	for i := range d.tracks {
		tracks[i] = cloneTrack(d.tracks[i])
	}
	return tracks
}

func cloneTrack(track Track) Track {
	if len(track.CodecPrivate) != 0 {
		track.CodecPrivate = append([]byte(nil), track.CodecPrivate...)
	}
	if len(track.Video.Projection.Private) != 0 {
		track.Video.Projection.Private = append([]byte(nil), track.Video.Projection.Private...)
	}
	return track
}

func cloneSegmentInfo(info SegmentInfo) SegmentInfo {
	if len(info.SegmentUUID) != 0 {
		info.SegmentUUID = append([]byte(nil), info.SegmentUUID...)
	}
	if len(info.PrevUUID) != 0 {
		info.PrevUUID = append([]byte(nil), info.PrevUUID...)
	}
	if len(info.NextUUID) != 0 {
		info.NextUUID = append([]byte(nil), info.NextUUID...)
	}
	return info
}

func cloneAttachments(attachments []Attachment) []Attachment {
	if len(attachments) == 0 {
		return nil
	}
	out := make([]Attachment, len(attachments))
	for i := range attachments {
		out[i] = attachments[i]
		if attachments[i].Data != nil {
			out[i].Data = append([]byte(nil), attachments[i].Data...)
		}
	}
	return out
}

func (d *Demuxer) DocType() string {
	if d == nil {
		return ""
	}
	return d.docType
}

func (d *Demuxer) Info() SegmentInfo {
	if d == nil {
		return SegmentInfo{}
	}
	return cloneSegmentInfo(d.info)
}

func (d *Demuxer) Attachments() []Attachment {
	if d == nil || len(d.attachments) == 0 {
		return nil
	}
	return cloneAttachments(d.attachments)
}

func (d *Demuxer) Cues() []CuePoint {
	if d == nil || len(d.cues) == 0 {
		return nil
	}
	cues := make([]CuePoint, len(d.cues))
	copy(cues, d.cues)
	return cues
}

func (d *Demuxer) SeekEntries() []SeekEntry {
	if d == nil || len(d.seekEntries) == 0 {
		return nil
	}
	entries := make([]SeekEntry, len(d.seekEntries))
	copy(entries, d.seekEntries)
	return entries
}

func (d *Demuxer) ReadPacket(dst *Packet) error {
	if d == nil || d.reader == nil {
		return ErrNilReader
	}
	if dst == nil {
		return ErrNilPacket
	}
	dst.Reset()
	if d.laceFrameIndex < d.laceFrameCount {
		return d.nextLacedPacket(dst)
	}
	for {
		if d.inCluster && !d.clusterUnknown && d.reader.Offset() >= d.clusterEnd {
			d.inCluster = false
		}
		header, err := d.reader.ReadHeader()
		if err != nil {
			return err
		}
		if d.inCluster {
			if header.ID == idCluster {
				if err := d.enterCluster(header); err != nil {
					return err
				}
				continue
			}
			switch header.ID {
			case idTimestamp:
				value, err := readUIntPayloadScratch(d.reader, header.Size.Value, &d.uintScratch)
				if err != nil {
					return err
				}
				if value > uint64(math.MaxInt64) {
					return ErrInvalidData
				}
				d.clusterTimecode = int64(value)
			case idSimpleBlock:
				return d.readSimpleBlock(header, dst)
			case idBlockGroup:
				return d.readBlockGroup(header, dst)
			case idVoid, idCRC32:
				if err := skipElement(d.reader, header); err != nil {
					return err
				}
			default:
				if err := skipElement(d.reader, header); err != nil {
					return err
				}
			}
			continue
		}
		switch header.ID {
		case idSeekHead:
			if err := d.parseSeekHead(header); err != nil {
				return err
			}
		case idCluster:
			if err := d.enterCluster(header); err != nil {
				return err
			}
		case idInfo:
			if err := d.parseInfo(header); err != nil {
				return err
			}
		case idTracks:
			if err := d.parseTracks(header); err != nil {
				return err
			}
		case idAttachments:
			if err := d.parseAttachments(header); err != nil {
				return err
			}
		case idCues:
			if err := d.parseCues(header); err != nil {
				return err
			}
		case idVoid, idCRC32:
			if err := skipElement(d.reader, header); err != nil {
				return err
			}
		default:
			if err := skipElement(d.reader, header); err != nil {
				return err
			}
		}
	}
}

func (d *Demuxer) readPreamble() error {
	header, err := d.reader.ReadHeader()
	if err != nil {
		return err
	}
	if header.ID != idEBML {
		return ErrInvalidData
	}
	if err := d.parseEBMLHeader(header); err != nil {
		return err
	}
	for {
		header, err := d.reader.ReadHeader()
		if err != nil {
			return err
		}
		switch header.ID {
		case idSegment:
			d.inSegment = true
			d.segmentData = header.DataOffset
			return d.readSegmentHeaders()
		case idVoid, idCRC32:
			if err := skipElement(d.reader, header); err != nil {
				return err
			}
		default:
			if err := skipElement(d.reader, header); err != nil {
				return err
			}
		}
	}
}

func (d *Demuxer) loadCuesFromSeekHead() error {
	var cuesPosition uint64
	for i := range d.seekEntries {
		if d.seekEntries[i].ID == uint64(idCues) {
			cuesPosition = d.seekEntries[i].Position
			break
		}
	}
	if cuesPosition == 0 {
		return ErrInvalidData
	}
	if _, err := d.seeker.Seek(d.segmentData+int64(cuesPosition), io.SeekStart); err != nil {
		return err
	}
	d.reader.Reset(d.seeker, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize})
	header, err := d.reader.ReadHeader()
	if err != nil {
		return err
	}
	if header.ID != idCues {
		return ErrInvalidData
	}
	return d.parseCues(header)
}

func (d *Demuxer) readSegmentHeaders() error {
	for {
		header, err := d.reader.ReadHeader()
		if err != nil {
			return err
		}
		switch header.ID {
		case idSeekHead:
			if err := d.parseSeekHead(header); err != nil {
				return err
			}
		case idInfo:
			if err := d.parseInfo(header); err != nil {
				return err
			}
		case idTracks:
			if err := d.parseTracks(header); err != nil {
				return err
			}
		case idAttachments:
			if err := d.parseAttachments(header); err != nil {
				return err
			}
		case idCues:
			if err := d.parseCues(header); err != nil {
				return err
			}
		case idCluster:
			return d.enterCluster(header)
		case idVoid, idCRC32:
			if err := skipElement(d.reader, header); err != nil {
				return err
			}
		default:
			if err := skipElement(d.reader, header); err != nil {
				return err
			}
		}
	}
}

func (d *Demuxer) parseSeekHead(header ebml.Header) error {
	if header.Size.Unknown {
		return ErrInvalidData
	}
	master, err := d.checkedMasterReader(d.reader, header.Size.Value)
	if err != nil {
		return err
	}
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return err
		}
		switch child.ID {
		case idSeek:
			entry, err := d.parseSeekEntry(master.Reader(), child)
			if err != nil {
				return err
			}
			if entry.ID != 0 {
				d.seekEntries = append(d.seekEntries, entry)
			}
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return err
			}
		}
	}
	return master.Validate()
}

func (d *Demuxer) parseSeekEntry(parent io.Reader, header ebml.Header) (SeekEntry, error) {
	if header.Size.Unknown {
		return SeekEntry{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return SeekEntry{}, err
	}
	var entry SeekEntry
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return SeekEntry{}, err
		}
		switch child.ID {
		case idSeekID:
			value, err := readElementIDPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return SeekEntry{}, err
			}
			entry.ID = uint64(value)
		case idSeekPosition:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return SeekEntry{}, err
			}
			entry.Position = value
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return SeekEntry{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return SeekEntry{}, err
	}
	return entry, nil
}

func (d *Demuxer) parseEBMLHeader(header ebml.Header) error {
	if header.Size.Unknown {
		return ErrInvalidData
	}
	limited := d.reader.Limited(header.Size.Value)
	reader := ebml.NewReader(limited, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize})
	for limited.N > 0 {
		child, err := reader.ReadHeader()
		if err != nil {
			return err
		}
		switch child.ID {
		case idDocType:
			value, err := readStringPayload(reader, child.Size.Value)
			if err != nil {
				return err
			}
			d.docType = value
		default:
			if err := skipElement(reader, child); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *Demuxer) parseInfo(header ebml.Header) error {
	if header.Size.Unknown {
		return ErrInvalidData
	}
	master, err := d.checkedMasterReader(d.reader, header.Size.Value)
	if err != nil {
		return err
	}
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return err
		}
		if err := d.parseInfoChild(master.Reader(), child); err != nil {
			return err
		}
	}
	if err := master.Validate(); err != nil {
		return err
	}
	return validateSegmentInfo(d.info)
}

func (d *Demuxer) checkedMasterReader(parent io.Reader, size uint64) (*checkedMasterReader, error) {
	if size > uint64(math.MaxInt64) {
		return nil, ErrInvalidData
	}
	limited := &io.LimitedReader{R: parent, N: int64(size)}
	reader := ebml.NewReader(limited, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize})
	master := checkedMasterReader{limited: limited, reader: reader}
	if limited.N == 0 {
		return &master, nil
	}
	header, err := reader.ReadHeader()
	if err != nil {
		return nil, err
	}
	if header.ID != idCRC32 {
		master.pending = header
		master.hasPending = true
		return &master, nil
	}
	if header.Size.Unknown || header.Size.Value != 4 {
		return nil, ErrInvalidData
	}
	if err := reader.ReadFull(master.storedCRC[:]); err != nil {
		return nil, err
	}
	master.crc = crc32.NewIEEE()
	reader = ebml.NewReader(crc32HashReader{reader: limited, crc: master.crc}, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize})
	master.reader = reader
	return &master, nil
}

type checkedMasterReader struct {
	limited    *io.LimitedReader
	reader     *ebml.Reader
	pending    ebml.Header
	hasPending bool
	crc        hash.Hash32
	storedCRC  [4]byte
}

func (r *checkedMasterReader) Done() bool {
	return !r.hasPending && r.limited.N == 0
}

func (r *checkedMasterReader) Reader() *ebml.Reader {
	return r.reader
}

func (r *checkedMasterReader) ReadHeader() (ebml.Header, error) {
	if r.hasPending {
		r.hasPending = false
		return r.pending, nil
	}
	header, err := r.reader.ReadHeader()
	if err != nil {
		return ebml.Header{}, err
	}
	if header.ID == idCRC32 {
		return ebml.Header{}, ErrInvalidData
	}
	return header, nil
}

func (r *checkedMasterReader) Validate() error {
	if r.limited.N != 0 {
		return ErrInvalidData
	}
	if r.crc == nil {
		return nil
	}
	if binary.LittleEndian.Uint32(r.storedCRC[:]) != r.crc.Sum32() {
		return ErrInvalidData
	}
	return nil
}

type crc32HashReader struct {
	reader *io.LimitedReader
	crc    hash.Hash32
}

func (r crc32HashReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n != 0 {
		_, _ = r.crc.Write(p[:n])
	}
	return n, err
}

func (d *Demuxer) parseInfoChild(reader *ebml.Reader, child ebml.Header) error {
	switch child.ID {
	case idSegmentUUID:
		value, err := readBinaryPayload(reader, child.Size.Value)
		if err != nil {
			return err
		}
		d.info.SegmentUUID = value
	case idSegmentFilename:
		value, err := readStringPayload(reader, child.Size.Value)
		if err != nil {
			return err
		}
		d.info.SegmentFilename = value
	case idPrevUUID:
		value, err := readBinaryPayload(reader, child.Size.Value)
		if err != nil {
			return err
		}
		d.info.PrevUUID = value
	case idPrevFilename:
		value, err := readStringPayload(reader, child.Size.Value)
		if err != nil {
			return err
		}
		d.info.PrevFilename = value
	case idNextUUID:
		value, err := readBinaryPayload(reader, child.Size.Value)
		if err != nil {
			return err
		}
		d.info.NextUUID = value
	case idNextFilename:
		value, err := readStringPayload(reader, child.Size.Value)
		if err != nil {
			return err
		}
		d.info.NextFilename = value
	case idTimestampScale:
		value, err := readUIntPayload(reader, child.Size.Value)
		if err != nil {
			return err
		}
		if value == 0 || value > uint64(math.MaxInt64) {
			return ErrInvalidData
		}
		d.timecodeScaleNS = int64(value)
	case idDateUTC:
		value, err := readDatePayload(reader, child.Size.Value)
		if err != nil {
			return err
		}
		d.info.DateUTC = value
		d.info.DateUTCSet = true
	case idTitle:
		value, err := readStringPayload(reader, child.Size.Value)
		if err != nil {
			return err
		}
		d.info.Title = value
	case idMuxingApp:
		value, err := readStringPayload(reader, child.Size.Value)
		if err != nil {
			return err
		}
		d.info.MuxingApp = value
	case idWritingApp:
		value, err := readStringPayload(reader, child.Size.Value)
		if err != nil {
			return err
		}
		d.info.WritingApp = value
	default:
		if err := skipElement(reader, child); err != nil {
			return err
		}
	}
	return nil
}

func (d *Demuxer) parseTracks(header ebml.Header) error {
	if header.Size.Unknown {
		return ErrInvalidData
	}
	master, err := d.checkedMasterReader(d.reader, header.Size.Value)
	if err != nil {
		return err
	}
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return err
		}
		switch child.ID {
		case idTrackEntry:
			track, err := d.parseTrackEntry(master.Reader(), child)
			if err != nil {
				return err
			}
			if track.ID != 0 {
				d.upsertTrack(track)
			}
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return err
			}
		}
	}
	return master.Validate()
}

func (d *Demuxer) parseCues(header ebml.Header) error {
	if header.Size.Unknown {
		return ErrInvalidData
	}
	master, err := d.checkedMasterReader(d.reader, header.Size.Value)
	if err != nil {
		return err
	}
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return err
		}
		switch child.ID {
		case idCuePoint:
			cue, err := d.parseCuePoint(master.Reader(), child)
			if err != nil {
				return err
			}
			if cue.TrackID != 0 {
				d.cues = append(d.cues, cue)
			}
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return err
			}
		}
	}
	return master.Validate()
}

func (d *Demuxer) parseAttachments(header ebml.Header) error {
	if header.Size.Unknown {
		return ErrInvalidData
	}
	master, err := d.checkedMasterReader(d.reader, header.Size.Value)
	if err != nil {
		return err
	}
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return err
		}
		switch child.ID {
		case idAttachedFile:
			attachment, err := d.parseAttachedFile(master.Reader(), child)
			if err != nil {
				return err
			}
			d.attachments = append(d.attachments, attachment)
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return err
			}
		}
	}
	return master.Validate()
}

func (d *Demuxer) parseAttachedFile(parent io.Reader, header ebml.Header) (Attachment, error) {
	if header.Size.Unknown {
		return Attachment{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return Attachment{}, err
	}
	var attachment Attachment
	dataSeen := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return Attachment{}, err
		}
		switch child.ID {
		case idFileDescription:
			attachment.Description, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Attachment{}, err
			}
		case idFileName:
			attachment.Filename, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Attachment{}, err
			}
		case idFileMediaType:
			attachment.MIMEType, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Attachment{}, err
			}
		case idFileData:
			attachment.Data, err = readBinaryPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Attachment{}, err
			}
			dataSeen = true
		case idFileUID:
			attachment.UID, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Attachment{}, err
			}
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return Attachment{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return Attachment{}, err
	}
	if !dataSeen {
		return Attachment{}, ErrInvalidData
	}
	if err := validateAttachment(attachment); err != nil {
		return Attachment{}, ErrInvalidData
	}
	return attachment, nil
}

func (d *Demuxer) parseCuePoint(parent io.Reader, header ebml.Header) (CuePoint, error) {
	if header.Size.Unknown {
		return CuePoint{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return CuePoint{}, err
	}
	var cue CuePoint
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return CuePoint{}, err
		}
		switch child.ID {
		case idCueTime:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return CuePoint{}, err
			}
			if value > uint64(math.MaxInt64)/uint64(d.timecodeScaleNS) {
				return CuePoint{}, ErrInvalidData
			}
			cue.TimeNS = int64(value) * d.timecodeScaleNS
		case idCueTrackPositions:
			position, err := d.parseCueTrackPositions(master.Reader(), child)
			if err != nil {
				return CuePoint{}, err
			}
			cue.TrackID = position.TrackID
			cue.ClusterPosition = position.ClusterPosition
			cue.RelativePosition = position.RelativePosition
			cue.RelativePositionSet = position.RelativePositionSet
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return CuePoint{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return CuePoint{}, err
	}
	return cue, nil
}

type cueTrackPosition struct {
	TrackID             uint32
	ClusterPosition     uint64
	RelativePosition    uint64
	RelativePositionSet bool
}

func (d *Demuxer) parseCueTrackPositions(parent io.Reader, header ebml.Header) (cueTrackPosition, error) {
	if header.Size.Unknown {
		return cueTrackPosition{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return cueTrackPosition{}, err
	}
	var position cueTrackPosition
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return cueTrackPosition{}, err
		}
		switch child.ID {
		case idCueTrack:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return cueTrackPosition{}, err
			}
			position.TrackID, err = trackIDFromUint(value)
			if err != nil {
				return cueTrackPosition{}, err
			}
		case idCueClusterPosition:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return cueTrackPosition{}, err
			}
			position.ClusterPosition = value
		case idCueRelativePos:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return cueTrackPosition{}, err
			}
			position.RelativePosition = value
			position.RelativePositionSet = true
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return cueTrackPosition{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return cueTrackPosition{}, err
	}
	return position, nil
}

func (d *Demuxer) parseTrackEntry(parent io.Reader, header ebml.Header) (Track, error) {
	if header.Size.Unknown {
		return Track{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return Track{}, err
	}
	track := Track{Language: "und", TimebaseNum: 1, TimebaseDen: timeNS, FlagEnabled: true, FlagDefault: true, FlagLacing: true}
	var codecID string
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return Track{}, err
		}
		switch child.ID {
		case idTrackNumber:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			trackID, err := trackIDFromUint(value)
			if err != nil {
				return Track{}, err
			}
			track.ID = trackID
		case idTrackUID:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			if value == 0 {
				return Track{}, ErrInvalidData
			}
			track.UID = value
		case idTrackType:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			switch value {
			case matroskaTrackAudio:
				track.Type = TrackAudio
			case matroskaTrackVideo:
				track.Type = TrackVideo
			default:
				track.Type = TrackUnknown
			}
		case idFlagEnabled:
			value, err := readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.FlagEnabled = value
			track.FlagEnabledSet = true
		case idFlagDefault:
			value, err := readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.FlagDefault = value
			track.FlagDefaultSet = true
		case idFlagForced:
			value, err := readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.FlagForced = value
			track.FlagForcedSet = true
		case idFlagHearingImpaired:
			value, err := readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.FlagHearingImpaired = value
			track.FlagHearingImpairedSet = true
		case idFlagVisualImpaired:
			value, err := readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.FlagVisualImpaired = value
			track.FlagVisualImpairedSet = true
		case idFlagTextDescriptions:
			value, err := readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.FlagTextDescriptions = value
			track.FlagTextDescriptionsSet = true
		case idFlagOriginal:
			value, err := readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.FlagOriginal = value
			track.FlagOriginalSet = true
		case idFlagCommentary:
			value, err := readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.FlagCommentary = value
			track.FlagCommentarySet = true
		case idFlagLacing:
			value, err := readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.FlagLacing = value
			track.FlagLacingSet = true
		case idName:
			value, err := readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.Name = value
		case idLanguage:
			value, err := readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.Language = value
		case idLanguageBCP:
			value, err := readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.LanguageBCP47 = value
		case idCodecID:
			value, err := readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			codecID = value
		case idCodecName:
			value, err := readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.CodecName = value
		case idDefaultDur:
			value, err := readNonZeroInt64Payload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.DefaultDurationNS = value
		case idDefaultDecodedDur:
			value, err := readNonZeroInt64Payload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.DefaultDecodedFieldDurationNS = value
		case idCodecDelay:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			if value > uint64(math.MaxInt64) {
				return Track{}, ErrInvalidData
			}
			track.CodecDelayNS = int64(value)
		case idSeekPreRoll:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			if value > uint64(math.MaxInt64) {
				return Track{}, ErrInvalidData
			}
			track.SeekPreRollNS = int64(value)
		case idCodecPrivate:
			value, err := readBinaryPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.CodecPrivate = value
		case idVideo:
			video, err := d.parseVideo(master.Reader(), child)
			if err != nil {
				return Track{}, err
			}
			track.Video = video
		case idAudio:
			audio, err := d.parseAudio(master.Reader(), child)
			if err != nil {
				return Track{}, err
			}
			track.Audio = audio
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return Track{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return Track{}, err
	}
	if track.UID == 0 {
		track.UID = uint64(track.ID)
	}
	track.Codec = codecFromMatroskaID(codecID, track.CodecPrivate)
	if track.Codec == CodecOpus && len(track.CodecPrivate) != 0 {
		head, err := parseOpusHead(track.CodecPrivate)
		if err != nil {
			return Track{}, err
		}
		track.Audio.Channels = head.Channels
		if track.CodecDelayNS == 0 {
			track.CodecDelayNS = opusCodecDelayNS(head.PreSkip)
		}
		if track.SeekPreRollNS == 0 {
			track.SeekPreRollNS = opusDefaultSeekPreRollNS
		}
	}
	if track.Codec == CodecAV1 && len(track.CodecPrivate) != 0 {
		if _, err := parseAV1CodecConfigurationRecord(track.CodecPrivate); err != nil {
			return Track{}, err
		}
	}
	if track.Codec == CodecH264 && len(track.CodecPrivate) != 0 {
		if _, err := parseAVCDecoderConfigurationRecord(track.CodecPrivate); err != nil {
			return Track{}, err
		}
	}
	return track, nil
}

func (d *Demuxer) parseVideo(parent io.Reader, header ebml.Header) (VideoConfig, error) {
	if header.Size.Unknown {
		return VideoConfig{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return VideoConfig{}, err
	}
	var video VideoConfig
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return VideoConfig{}, err
		}
		switch child.ID {
		case idStereoMode:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.StereoMode, err = intFromUint(value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.StereoModeSet = true
		case idAlphaMode:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.AlphaMode, err = intFromUint(value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.AlphaModeSet = true
		case idPixelWidth:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.Width, err = nonZeroIntFromUint(value)
			if err != nil {
				return VideoConfig{}, err
			}
		case idPixelHeight:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.Height, err = nonZeroIntFromUint(value)
			if err != nil {
				return VideoConfig{}, err
			}
		case idPixelCropBottom:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.PixelCropBottom, err = intFromUint(value)
			if err != nil {
				return VideoConfig{}, err
			}
		case idPixelCropTop:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.PixelCropTop, err = intFromUint(value)
			if err != nil {
				return VideoConfig{}, err
			}
		case idPixelCropLeft:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.PixelCropLeft, err = intFromUint(value)
			if err != nil {
				return VideoConfig{}, err
			}
		case idPixelCropRight:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.PixelCropRight, err = intFromUint(value)
			if err != nil {
				return VideoConfig{}, err
			}
		case idDisplayWidth:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.DisplayWidth, err = nonZeroIntFromUint(value)
			if err != nil {
				return VideoConfig{}, err
			}
		case idDisplayHeight:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.DisplayHeight, err = nonZeroIntFromUint(value)
			if err != nil {
				return VideoConfig{}, err
			}
		case idDisplayUnit:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.DisplayUnit, err = intFromUint(value)
			if err != nil {
				return VideoConfig{}, err
			}
		case idColour:
			colour, err := d.parseColour(master.Reader(), child)
			if err != nil {
				return VideoConfig{}, err
			}
			video.Colour = colour
		case idProjection:
			projection, err := d.parseProjection(master.Reader(), child)
			if err != nil {
				return VideoConfig{}, err
			}
			video.Projection = projection
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return VideoConfig{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return VideoConfig{}, err
	}
	return video, nil
}

func (d *Demuxer) parseProjection(parent io.Reader, header ebml.Header) (VideoProjectionConfig, error) {
	if header.Size.Unknown {
		return VideoProjectionConfig{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return VideoProjectionConfig{}, err
	}
	projection := VideoProjectionConfig{Set: true}
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return VideoProjectionConfig{}, err
		}
		switch child.ID {
		case idProjectionType:
			projection.Type, err = readIntPayloadFromUInt(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoProjectionConfig{}, err
			}
		case idProjectionPrivate:
			value, err := readBinaryPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoProjectionConfig{}, err
			}
			projection.Private = value
		case idProjectionPoseYaw:
			projection.PoseYaw, err = readProjectionPosePayload(master.Reader(), child.Size.Value, -180, 180)
			if err != nil {
				return VideoProjectionConfig{}, err
			}
		case idProjectionPosePitch:
			projection.PosePitch, err = readProjectionPosePayload(master.Reader(), child.Size.Value, -90, 90)
			if err != nil {
				return VideoProjectionConfig{}, err
			}
		case idProjectionPoseRoll:
			projection.PoseRoll, err = readProjectionPosePayload(master.Reader(), child.Size.Value, -180, 180)
			if err != nil {
				return VideoProjectionConfig{}, err
			}
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return VideoProjectionConfig{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return VideoProjectionConfig{}, err
	}
	if err := validateVideoProjection(projection); err != nil {
		return VideoProjectionConfig{}, ErrInvalidData
	}
	return projection, nil
}

func (d *Demuxer) parseColour(parent io.Reader, header ebml.Header) (VideoColourConfig, error) {
	if header.Size.Unknown {
		return VideoColourConfig{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return VideoColourConfig{}, err
	}
	var colour VideoColourConfig
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return VideoColourConfig{}, err
		}
		switch child.ID {
		case idMatrixCoefficients:
			colour.MatrixCoefficients, err = readIntPayloadFromUInt(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoColourConfig{}, err
			}
			colour.MatrixCoefficientsSet = true
		case idBitsPerChannel:
			colour.BitsPerChannel, err = readIntPayloadFromUInt(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoColourConfig{}, err
			}
			colour.BitsPerChannelSet = true
		case idChromaSubsampleHorz:
			colour.ChromaSubsamplingHorz, err = readIntPayloadFromUInt(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoColourConfig{}, err
			}
			colour.ChromaSubsamplingHorzSet = true
		case idChromaSubsampleVert:
			colour.ChromaSubsamplingVert, err = readIntPayloadFromUInt(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoColourConfig{}, err
			}
			colour.ChromaSubsamplingVertSet = true
		case idCbSubsampleHorz:
			colour.CbSubsamplingHorz, err = readIntPayloadFromUInt(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoColourConfig{}, err
			}
			colour.CbSubsamplingHorzSet = true
		case idCbSubsampleVert:
			colour.CbSubsamplingVert, err = readIntPayloadFromUInt(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoColourConfig{}, err
			}
			colour.CbSubsamplingVertSet = true
		case idChromaSitingHorz:
			colour.ChromaSitingHorz, err = readIntPayloadFromUInt(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoColourConfig{}, err
			}
			colour.ChromaSitingHorzSet = true
		case idChromaSitingVert:
			colour.ChromaSitingVert, err = readIntPayloadFromUInt(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoColourConfig{}, err
			}
			colour.ChromaSitingVertSet = true
		case idColourRange:
			colour.Range, err = readIntPayloadFromUInt(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoColourConfig{}, err
			}
			colour.RangeSet = true
		case idTransferChar:
			colour.TransferCharacteristics, err = readIntPayloadFromUInt(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoColourConfig{}, err
			}
			colour.TransferCharacteristicsSet = true
		case idPrimaries:
			colour.Primaries, err = readIntPayloadFromUInt(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoColourConfig{}, err
			}
			colour.PrimariesSet = true
		case idMaxCLL:
			colour.MaxCLL, err = readIntPayloadFromUInt(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoColourConfig{}, err
			}
			colour.MaxCLLSet = true
		case idMaxFALL:
			colour.MaxFALL, err = readIntPayloadFromUInt(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoColourConfig{}, err
			}
			colour.MaxFALLSet = true
		case idMasteringMetadata:
			metadata, err := d.parseMasteringMetadata(master.Reader(), child)
			if err != nil {
				return VideoColourConfig{}, err
			}
			colour.MasteringMetadata = metadata
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return VideoColourConfig{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return VideoColourConfig{}, err
	}
	return colour, nil
}

func (d *Demuxer) parseMasteringMetadata(parent io.Reader, header ebml.Header) (VideoMasteringMetadataConfig, error) {
	if header.Size.Unknown {
		return VideoMasteringMetadataConfig{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return VideoMasteringMetadataConfig{}, err
	}
	var metadata VideoMasteringMetadataConfig
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return VideoMasteringMetadataConfig{}, err
		}
		switch child.ID {
		case idPrimaryRX:
			metadata.PrimaryRChromaticityX, err = readChromaticityPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.PrimaryRChromaticityXSet = true
		case idPrimaryRY:
			metadata.PrimaryRChromaticityY, err = readChromaticityPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.PrimaryRChromaticityYSet = true
		case idPrimaryGX:
			metadata.PrimaryGChromaticityX, err = readChromaticityPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.PrimaryGChromaticityXSet = true
		case idPrimaryGY:
			metadata.PrimaryGChromaticityY, err = readChromaticityPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.PrimaryGChromaticityYSet = true
		case idPrimaryBX:
			metadata.PrimaryBChromaticityX, err = readChromaticityPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.PrimaryBChromaticityXSet = true
		case idPrimaryBY:
			metadata.PrimaryBChromaticityY, err = readChromaticityPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.PrimaryBChromaticityYSet = true
		case idWhitePointX:
			metadata.WhitePointChromaticityX, err = readChromaticityPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.WhitePointChromaticityXSet = true
		case idWhitePointY:
			metadata.WhitePointChromaticityY, err = readChromaticityPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.WhitePointChromaticityYSet = true
		case idLuminanceMax:
			metadata.LuminanceMax, err = readNonNegativeFloatPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.LuminanceMaxSet = true
		case idLuminanceMin:
			metadata.LuminanceMin, err = readNonNegativeFloatPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.LuminanceMinSet = true
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return VideoMasteringMetadataConfig{}, err
	}
	return metadata, nil
}

func (d *Demuxer) parseAudio(parent io.Reader, header ebml.Header) (AudioConfig, error) {
	if header.Size.Unknown {
		return AudioConfig{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return AudioConfig{}, err
	}
	audio := AudioConfig{SampleRate: 48000, Channels: 2}
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return AudioConfig{}, err
		}
		switch child.ID {
		case idSamplingFreq:
			value, err := readFloatPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return AudioConfig{}, err
			}
			if invalidAudioFrequency(value) {
				return AudioConfig{}, ErrInvalidData
			}
			audio.SampleRate = int(value)
		case idOutputFreq:
			value, err := readFloatPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return AudioConfig{}, err
			}
			if invalidAudioFrequency(value) {
				return AudioConfig{}, ErrInvalidData
			}
			audio.OutputSampleRate = int(value)
		case idChannels:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return AudioConfig{}, err
			}
			audio.Channels, err = intFromUint(value)
			if err != nil {
				return AudioConfig{}, err
			}
		case idBitDepth:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return AudioConfig{}, err
			}
			audio.BitDepth, err = intFromUint(value)
			if err != nil {
				return AudioConfig{}, err
			}
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return AudioConfig{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return AudioConfig{}, err
	}
	return audio, nil
}

func invalidAudioFrequency(value float64) bool {
	return math.IsNaN(value) || math.IsInf(value, 0) || value <= 0 || value > float64(maxIntValue)
}

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

func scaleReferenceBlockTimeNS(ticks int64, scaleNS int64) (int64, error) {
	if scaleNS <= 0 || ticks > math.MaxInt64/scaleNS || ticks < math.MinInt64/scaleNS {
		return 0, ErrInvalidData
	}
	return ticks * scaleNS, nil
}

func (d *Demuxer) readBlockGroup(header ebml.Header, dst *Packet) error {
	if header.Size.Unknown {
		return ErrInvalidData
	}
	dst.Reset()
	d.groupLimit.R = d.reader
	d.groupLimit.N = int64(header.Size.Value)
	d.groupReader.Reset(&d.groupLimit, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize})
	haveBlock := false
	referenceSeen := false
	durationTicks := uint64(0)
	var payloadErr error
	for d.groupLimit.N > 0 {
		child, err := d.groupReader.ReadHeader()
		if err != nil {
			return err
		}
		switch child.ID {
		case idBlock:
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
			haveBlock = true
		case idBlockDuration:
			value, err := readUIntPayloadScratch(d.groupReader, child.Size.Value, &d.uintScratch)
			if err != nil {
				return err
			}
			durationTicks = value
		case idReferenceBlk:
			ticks, err := readIntPayload(d.groupReader, child.Size.Value)
			if err != nil {
				return err
			}
			timeNS, err := scaleReferenceBlockTimeNS(ticks, d.timecodeScaleNS)
			if err != nil {
				return err
			}
			dst.ReferenceBlockTimeNS = append(dst.ReferenceBlockTimeNS, timeNS)
			referenceSeen = true
		case idDiscardPad:
			paddingNS, err := readIntPayload(d.groupReader, child.Size.Value)
			if err != nil {
				return err
			}
			dst.DiscardPaddingNS = paddingNS
		default:
			if err := skipElement(d.groupReader, child); err != nil {
				return err
			}
		}
	}
	if !haveBlock {
		return ErrInvalidData
	}
	if durationTicks != 0 {
		if durationTicks > uint64(math.MaxInt64)/uint64(d.timecodeScaleNS) {
			return ErrInvalidData
		}
		durationNS := int64(durationTicks) * d.timecodeScaleNS
		if d.laceFrameCount > 0 {
			durationNS /= int64(d.laceFrameCount)
			d.laceDurationNS = durationNS
		}
		dst.DurationNS = durationNS
	}
	dst.Keyframe = !referenceSeen
	if d.laceFrameCount > 0 {
		d.laceKeyframe = !referenceSeen
	}
	if payloadErr != nil {
		return payloadErr
	}
	return nil
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
	timecode := d.clusterTimecode + int64(blockTimecode)
	if timecode < 0 {
		return ErrInvalidData
	}
	if lacing != 0 {
		return d.readLacedBlockPayload(track, trackNumber, timecode, flags, lacing, dst, simple)
	}
	frameSize := int(d.blockLimit.N)
	if cap(dst.Data) < frameSize {
		if err := drainLimited(&d.blockLimit); err != nil {
			return err
		}
		return ErrPayloadTooSmall
	}
	dst.Data = dst.Data[:frameSize]
	if _, err := io.ReadFull(&d.blockLimit, dst.Data); err != nil {
		return err
	}
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
	dst.TrackID = trackNumber
	dst.TimeNS = timecode * d.timecodeScaleNS
	if simple {
		dst.Keyframe = flags&simpleBlockKeyframe != 0
	}
	dst.Invisible = flags&simpleBlockInvisible != 0
	if simple {
		dst.Discardable = flags&simpleBlockDiscardable != 0
	}
	return nil
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

func (d *Demuxer) readLacedBlockPayload(track Track, trackID uint32, timecode int64, flags byte, lacing byte, dst *Packet, simple bool) error {
	frameCountByte, err := d.readBlockByte()
	if err != nil {
		return err
	}
	frameCount := int(frameCountByte) + 1
	if frameCount < 2 || frameCount > len(d.laceFrames) {
		return ErrLaceTooLarge
	}
	for i := 0; i < frameCount; i++ {
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
	for i := 0; i < frameCount; i++ {
		d.laceFrames[i].offset = offset
		offset += d.laceFrames[i].size
	}
	d.laceTrackID = trackID
	d.laceH264Length = 0
	if track.Codec == CodecH264 && len(track.CodecPrivate) != 0 {
		lengthSize, ok, err := h264TrackNALULengthSize(track)
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
	d.laceTimeNS = timecode * d.timecodeScaleNS
	d.laceDurationNS = d.defaultDurationNS(trackID)
	d.laceFrameCount = frameCount
	d.laceFrameIndex = 0
	d.laceKeyframe = simple && flags&simpleBlockKeyframe != 0
	d.laceInvisible = flags&simpleBlockInvisible != 0
	d.laceDiscardable = simple && flags&simpleBlockDiscardable != 0
	return d.nextLacedPacket(dst)
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
	for i := 0; i < frameCount; i++ {
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
	for i := 0; i < frameCount; i++ {
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
	outSize := frame.size
	if d.laceH264Length != 0 {
		size, err := h264AVCToAnnexBSize(d.laceBuffer[frame.offset:frame.offset+frame.size], d.laceH264Length)
		if err != nil {
			return err
		}
		outSize = size
	}
	if cap(dst.Data) < outSize {
		return ErrPayloadTooSmall
	}
	dst.Data = dst.Data[:frame.size]
	copy(dst.Data, d.laceBuffer[frame.offset:frame.offset+frame.size])
	if d.laceH264Length != 0 {
		var err error
		dst.Data, err = h264AVCToAnnexBInPlace(dst.Data, outSize, d.laceH264Length)
		if err != nil {
			return err
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
	d.laceFrameIndex++
	if d.laceFrameIndex >= d.laceFrameCount {
		d.clearLace()
	}
	return nil
}

func (d *Demuxer) clearLace() {
	d.laceTrackID = 0
	d.laceH264Length = 0
	d.laceTimeNS = 0
	d.laceDurationNS = 0
	d.laceFrameCount = 0
	d.laceFrameIndex = 0
	d.laceKeyframe = false
	d.laceInvisible = false
	d.laceDiscardable = false
}

func (d *Demuxer) defaultDurationNS(trackID uint32) int64 {
	for i := range d.tracks {
		if d.tracks[i].ID == trackID {
			return d.tracks[i].DefaultDurationNS
		}
	}
	return 0
}

func (d *Demuxer) hasTrack(id uint32) bool {
	_, ok := d.track(id)
	return ok
}

func (d *Demuxer) track(id uint32) (Track, bool) {
	for i := range d.tracks {
		if d.tracks[i].ID == id {
			return d.tracks[i], true
		}
	}
	return Track{}, false
}

func (d *Demuxer) upsertTrack(track Track) {
	for i := range d.tracks {
		if d.tracks[i].ID == track.ID {
			d.tracks[i] = track
			return
		}
	}
	d.tracks = append(d.tracks, track)
}

func isEOF(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}
