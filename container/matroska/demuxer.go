package matroska

import (
	"bytes"
	"compress/bzip2"
	"compress/zlib"
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
	chapters        []ChapterEdition
	tags            []Tag
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
	laceContent     blockContentEncodingInfo
	laceTimeNS      int64
	laceDurationNS  int64
	laceFrameCount  int
	laceFrameIndex  int
	laceKeyframe    bool
	laceInvisible   bool
	laceDiscardable bool
	scratch         [ebml.MaxSizeWidth]byte
	uintScratch     [8]byte
	contentBuffer   []byte
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
	d.chapters = d.chapters[:0]
	d.tags = d.tags[:0]
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
	track.BlockAdditionMappings = cloneBlockAdditionMappings(track.BlockAdditionMappings)
	if len(track.TrackOverlays) != 0 {
		track.TrackOverlays = append([]uint64(nil), track.TrackOverlays...)
	}
	track.TrackTranslates = cloneTrackTranslates(track.TrackTranslates)
	track.ContentEncodings = cloneContentEncodings(track.ContentEncodings)
	return track
}

func cloneBlockAdditionMappings(mappings []BlockAdditionMapping) []BlockAdditionMapping {
	if len(mappings) == 0 {
		return nil
	}
	out := make([]BlockAdditionMapping, len(mappings))
	for i := range mappings {
		out[i] = mappings[i]
		if mappings[i].ExtraData != nil {
			out[i].ExtraData = append([]byte(nil), mappings[i].ExtraData...)
		}
	}
	return out
}

func cloneTrackTranslates(translates []TrackTranslate) []TrackTranslate {
	if len(translates) == 0 {
		return nil
	}
	out := make([]TrackTranslate, len(translates))
	for i := range translates {
		out[i] = translates[i]
		if translates[i].TrackID != nil {
			out[i].TrackID = append([]byte(nil), translates[i].TrackID...)
		}
		if len(translates[i].EditionUIDs) != 0 {
			out[i].EditionUIDs = append([]uint64(nil), translates[i].EditionUIDs...)
		}
	}
	return out
}

func cloneContentEncodings(encodings []ContentEncoding) []ContentEncoding {
	if len(encodings) == 0 {
		return nil
	}
	out := make([]ContentEncoding, len(encodings))
	for i := range encodings {
		out[i] = encodings[i]
		if encodings[i].Compression.Settings != nil {
			out[i].Compression.Settings = append([]byte(nil), encodings[i].Compression.Settings...)
		}
		if encodings[i].Encryption.KeyID != nil {
			out[i].Encryption.KeyID = append([]byte(nil), encodings[i].Encryption.KeyID...)
		}
		if encodings[i].Encryption.Signature != nil {
			out[i].Encryption.Signature = append([]byte(nil), encodings[i].Encryption.Signature...)
		}
		if encodings[i].Encryption.SignatureKeyID != nil {
			out[i].Encryption.SignatureKeyID = append([]byte(nil), encodings[i].Encryption.SignatureKeyID...)
		}
	}
	return out
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

func cloneChapters(editions []ChapterEdition) []ChapterEdition {
	if len(editions) == 0 {
		return nil
	}
	out := make([]ChapterEdition, len(editions))
	for i := range editions {
		out[i] = editions[i]
		out[i].Chapters = cloneChapterList(editions[i].Chapters)
	}
	return out
}

func cloneChapterList(chapters []Chapter) []Chapter {
	if len(chapters) == 0 {
		return nil
	}
	out := make([]Chapter, len(chapters))
	for i := range chapters {
		out[i] = chapters[i]
		if len(chapters[i].TrackUIDs) != 0 {
			out[i].TrackUIDs = append([]uint64(nil), chapters[i].TrackUIDs...)
		}
		if len(chapters[i].Displays) != 0 {
			out[i].Displays = append([]ChapterDisplay(nil), chapters[i].Displays...)
		}
		out[i].Children = cloneChapterList(chapters[i].Children)
	}
	return out
}

func cloneTags(tags []Tag) []Tag {
	if len(tags) == 0 {
		return nil
	}
	out := make([]Tag, len(tags))
	for i := range tags {
		out[i] = tags[i]
		out[i].Target = cloneTagTarget(tags[i].Target)
		out[i].Simple = cloneSimpleTags(tags[i].Simple)
	}
	return out
}

func cloneCues(cues []CuePoint) []CuePoint {
	if len(cues) == 0 {
		return nil
	}
	out := make([]CuePoint, len(cues))
	for i := range cues {
		out[i] = cues[i]
		out[i].References = cloneCueReferences(cues[i].References)
		out[i].Positions = cloneCueTrackPositions(cues[i].Positions)
	}
	return out
}

func cloneCueTrackPositions(positions []CueTrackPosition) []CueTrackPosition {
	if len(positions) == 0 {
		return nil
	}
	out := make([]CueTrackPosition, len(positions))
	for i := range positions {
		out[i] = positions[i]
		out[i].References = cloneCueReferences(positions[i].References)
	}
	return out
}

func cloneCueReferences(references []CueReference) []CueReference {
	if len(references) == 0 {
		return nil
	}
	out := make([]CueReference, len(references))
	copy(out, references)
	return out
}

func cloneTagTarget(target TagTarget) TagTarget {
	if len(target.TrackUIDs) != 0 {
		target.TrackUIDs = append([]uint64(nil), target.TrackUIDs...)
	}
	if len(target.EditionUIDs) != 0 {
		target.EditionUIDs = append([]uint64(nil), target.EditionUIDs...)
	}
	if len(target.ChapterUIDs) != 0 {
		target.ChapterUIDs = append([]uint64(nil), target.ChapterUIDs...)
	}
	if len(target.AttachmentUIDs) != 0 {
		target.AttachmentUIDs = append([]uint64(nil), target.AttachmentUIDs...)
	}
	return target
}

func cloneSimpleTags(tags []SimpleTag) []SimpleTag {
	if len(tags) == 0 {
		return nil
	}
	out := make([]SimpleTag, len(tags))
	for i := range tags {
		out[i] = tags[i]
		if tags[i].Binary != nil {
			out[i].Binary = append([]byte(nil), tags[i].Binary...)
		}
		out[i].Children = cloneSimpleTags(tags[i].Children)
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

func (d *Demuxer) Chapters() []ChapterEdition {
	if d == nil || len(d.chapters) == 0 {
		return nil
	}
	return cloneChapters(d.chapters)
}

func (d *Demuxer) Tags() []Tag {
	if d == nil || len(d.tags) == 0 {
		return nil
	}
	return cloneTags(d.tags)
}

func (d *Demuxer) Cues() []CuePoint {
	if d == nil || len(d.cues) == 0 {
		return nil
	}
	return cloneCues(d.cues)
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
		case idChapters:
			if err := d.parseChapters(header); err != nil {
				return err
			}
		case idTags:
			if err := d.parseTags(header); err != nil {
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
		case idChapters:
			if err := d.parseChapters(header); err != nil {
				return err
			}
		case idTags:
			if err := d.parseTags(header); err != nil {
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

func (d *Demuxer) parseChapters(header ebml.Header) error {
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
		case idEditionEntry:
			edition, err := d.parseEditionEntry(master.Reader(), child)
			if err != nil {
				return err
			}
			d.chapters = append(d.chapters, edition)
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return err
			}
		}
	}
	return master.Validate()
}

func (d *Demuxer) parseEditionEntry(parent io.Reader, header ebml.Header) (ChapterEdition, error) {
	if header.Size.Unknown {
		return ChapterEdition{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return ChapterEdition{}, err
	}
	var edition ChapterEdition
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return ChapterEdition{}, err
		}
		switch child.ID {
		case idEditionUID:
			edition.UID, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ChapterEdition{}, err
			}
		case idEditionFlagHidden:
			edition.Hidden, err = readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ChapterEdition{}, err
			}
		case idEditionFlagDefault:
			edition.Default, err = readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ChapterEdition{}, err
			}
		case idEditionFlagOrdered:
			edition.Ordered, err = readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ChapterEdition{}, err
			}
		case idChapterAtom:
			chapter, err := d.parseChapterAtom(master.Reader(), child)
			if err != nil {
				return ChapterEdition{}, err
			}
			edition.Chapters = append(edition.Chapters, chapter)
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return ChapterEdition{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return ChapterEdition{}, err
	}
	if len(edition.Chapters) == 0 {
		return ChapterEdition{}, ErrInvalidData
	}
	return edition, nil
}

func (d *Demuxer) parseChapterAtom(parent io.Reader, header ebml.Header) (Chapter, error) {
	if header.Size.Unknown {
		return Chapter{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return Chapter{}, err
	}
	chapter := Chapter{Enabled: true}
	startSeen := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return Chapter{}, err
		}
		switch child.ID {
		case idChapterUID:
			chapter.UID, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Chapter{}, err
			}
		case idChapterStringUID:
			chapter.StringUID, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Chapter{}, err
			}
		case idChapterTimeStart:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Chapter{}, err
			}
			if value > uint64(math.MaxInt64) {
				return Chapter{}, ErrInvalidData
			}
			chapter.StartNS = int64(value)
			startSeen = true
		case idChapterTimeEnd:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Chapter{}, err
			}
			if value > uint64(math.MaxInt64) {
				return Chapter{}, ErrInvalidData
			}
			chapter.EndNS = int64(value)
			chapter.EndSet = true
		case idChapterFlagHidden:
			chapter.Hidden, err = readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Chapter{}, err
			}
		case idChapterFlagEnabled:
			chapter.Enabled, err = readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Chapter{}, err
			}
			chapter.EnabledSet = true
		case idChapterTrack:
			chapter.TrackUIDs, err = d.parseChapterTrack(master.Reader(), child)
			if err != nil {
				return Chapter{}, err
			}
		case idChapterDisplay:
			display, err := d.parseChapterDisplay(master.Reader(), child)
			if err != nil {
				return Chapter{}, err
			}
			chapter.Displays = append(chapter.Displays, display)
		case idChapterAtom:
			childChapter, err := d.parseChapterAtom(master.Reader(), child)
			if err != nil {
				return Chapter{}, err
			}
			chapter.Children = append(chapter.Children, childChapter)
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return Chapter{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return Chapter{}, err
	}
	if !startSeen || validateChapter(chapter) != nil {
		return Chapter{}, ErrInvalidData
	}
	return chapter, nil
}

func (d *Demuxer) parseChapterTrack(parent io.Reader, header ebml.Header) ([]uint64, error) {
	if header.Size.Unknown {
		return nil, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return nil, err
	}
	var trackUIDs []uint64
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return nil, err
		}
		switch child.ID {
		case idChapterTrackUID:
			uid, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return nil, err
			}
			if uid == 0 {
				return nil, ErrInvalidData
			}
			trackUIDs = append(trackUIDs, uid)
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return nil, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return nil, err
	}
	if len(trackUIDs) == 0 {
		return nil, ErrInvalidData
	}
	return trackUIDs, nil
}

func (d *Demuxer) parseChapterDisplay(parent io.Reader, header ebml.Header) (ChapterDisplay, error) {
	if header.Size.Unknown {
		return ChapterDisplay{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return ChapterDisplay{}, err
	}
	display := ChapterDisplay{Language: "eng"}
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return ChapterDisplay{}, err
		}
		switch child.ID {
		case idChapString:
			display.String, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ChapterDisplay{}, err
			}
		case idChapLanguage:
			display.Language, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ChapterDisplay{}, err
			}
		case idChapLanguageBCP47:
			display.LanguageBCP47, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ChapterDisplay{}, err
			}
		case idChapCountry:
			display.Country, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ChapterDisplay{}, err
			}
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return ChapterDisplay{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return ChapterDisplay{}, err
	}
	if display.String == "" {
		return ChapterDisplay{}, ErrInvalidData
	}
	return display, nil
}

func (d *Demuxer) parseTags(header ebml.Header) error {
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
		case idTag:
			tag, err := d.parseTag(master.Reader(), child)
			if err != nil {
				return err
			}
			d.tags = append(d.tags, tag)
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return err
			}
		}
	}
	return master.Validate()
}

func (d *Demuxer) parseTag(parent io.Reader, header ebml.Header) (Tag, error) {
	if header.Size.Unknown {
		return Tag{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return Tag{}, err
	}
	var tag Tag
	targetSeen := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return Tag{}, err
		}
		switch child.ID {
		case idTargets:
			tag.Target, err = d.parseTagTargets(master.Reader(), child)
			if err != nil {
				return Tag{}, err
			}
			targetSeen = true
		case idSimpleTag:
			simple, err := d.parseSimpleTag(master.Reader(), child)
			if err != nil {
				return Tag{}, err
			}
			tag.Simple = append(tag.Simple, simple)
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return Tag{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return Tag{}, err
	}
	if !targetSeen {
		tag.Target.TypeValue = 50
	}
	if len(tag.Simple) == 0 || validateTagTarget(tag.Target) != nil {
		return Tag{}, ErrInvalidData
	}
	return tag, nil
}

func (d *Demuxer) parseTagTargets(parent io.Reader, header ebml.Header) (TagTarget, error) {
	if header.Size.Unknown {
		return TagTarget{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return TagTarget{}, err
	}
	target := TagTarget{TypeValue: 50}
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return TagTarget{}, err
		}
		switch child.ID {
		case idTargetTypeValue:
			target.TypeValue, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return TagTarget{}, err
			}
		case idTargetType:
			target.Type, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return TagTarget{}, err
			}
		case idTagTrackUID:
			target.TrackUIDs, err = appendTagUID(master.Reader(), child, target.TrackUIDs)
			if err != nil {
				return TagTarget{}, err
			}
		case idTagEditionUID:
			target.EditionUIDs, err = appendTagUID(master.Reader(), child, target.EditionUIDs)
			if err != nil {
				return TagTarget{}, err
			}
		case idTagChapterUID:
			target.ChapterUIDs, err = appendTagUID(master.Reader(), child, target.ChapterUIDs)
			if err != nil {
				return TagTarget{}, err
			}
		case idTagAttachmentUID:
			target.AttachmentUIDs, err = appendTagUID(master.Reader(), child, target.AttachmentUIDs)
			if err != nil {
				return TagTarget{}, err
			}
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return TagTarget{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return TagTarget{}, err
	}
	if err := validateTagTarget(target); err != nil {
		return TagTarget{}, err
	}
	return target, nil
}

func appendTagUID(reader io.Reader, child ebml.Header, values []uint64) ([]uint64, error) {
	value, err := readUIntPayload(reader, child.Size.Value)
	if err != nil {
		return nil, err
	}
	return append(values, value), nil
}

func (d *Demuxer) parseSimpleTag(parent io.Reader, header ebml.Header) (SimpleTag, error) {
	if header.Size.Unknown {
		return SimpleTag{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return SimpleTag{}, err
	}
	tag := SimpleTag{Language: "und", Default: true}
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return SimpleTag{}, err
		}
		switch child.ID {
		case idTagName:
			tag.Name, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return SimpleTag{}, err
			}
		case idTagLanguage:
			tag.Language, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return SimpleTag{}, err
			}
		case idTagLanguageBCP47:
			tag.LanguageBCP47, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return SimpleTag{}, err
			}
		case idTagDefault:
			tag.Default, err = readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return SimpleTag{}, err
			}
			tag.DefaultSet = true
		case idTagString:
			tag.String, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return SimpleTag{}, err
			}
			tag.StringSet = true
		case idTagBinary:
			tag.Binary, err = readBinaryPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return SimpleTag{}, err
			}
		case idSimpleTag:
			childTag, err := d.parseSimpleTag(master.Reader(), child)
			if err != nil {
				return SimpleTag{}, err
			}
			tag.Children = append(tag.Children, childTag)
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return SimpleTag{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return SimpleTag{}, err
	}
	if err := normalizeSimpleTags([]SimpleTag{tag}); err != nil {
		return SimpleTag{}, err
	}
	return tag, nil
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
	timeSeen := false
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
			timeNS, err := scaleCueTicks(value, d.timecodeScaleNS)
			if err != nil {
				return CuePoint{}, ErrInvalidData
			}
			cue.TimeNS = timeNS
			timeSeen = true
		case idCueTrackPositions:
			position, err := d.parseCueTrackPositions(master.Reader(), child)
			if err != nil {
				return CuePoint{}, err
			}
			cue.Positions = append(cue.Positions, position)
			if len(cue.Positions) == 1 {
				applyCuePosition(&cue, position)
			}
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return CuePoint{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return CuePoint{}, err
	}
	if !timeSeen || len(cue.Positions) == 0 {
		return CuePoint{}, ErrInvalidData
	}
	return cue, nil
}

func applyCuePosition(cue *CuePoint, position CueTrackPosition) {
	cue.TrackID = position.TrackID
	cue.ClusterPosition = position.ClusterPosition
	cue.RelativePosition = position.RelativePosition
	cue.RelativePositionSet = position.RelativePositionSet
	cue.DurationNS = position.DurationNS
	cue.DurationSet = position.DurationSet
	cue.BlockNumber = position.BlockNumber
	cue.BlockNumberSet = position.BlockNumberSet
	cue.CodecStatePosition = position.CodecStatePosition
	cue.CodecStateSet = position.CodecStateSet
	cue.References = cloneCueReferences(position.References)
}

func scaleCueTicks(value uint64, scaleNS int64) (int64, error) {
	if scaleNS <= 0 || value > uint64(math.MaxInt64)/uint64(scaleNS) {
		return 0, ErrInvalidData
	}
	return int64(value) * scaleNS, nil
}

func (d *Demuxer) parseCueTrackPositions(parent io.Reader, header ebml.Header) (CueTrackPosition, error) {
	if header.Size.Unknown {
		return CueTrackPosition{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return CueTrackPosition{}, err
	}
	var position CueTrackPosition
	trackSeen := false
	clusterSeen := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return CueTrackPosition{}, err
		}
		switch child.ID {
		case idCueTrack:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return CueTrackPosition{}, err
			}
			position.TrackID, err = trackIDFromUint(value)
			if err != nil {
				return CueTrackPosition{}, err
			}
			trackSeen = true
		case idCueClusterPosition:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return CueTrackPosition{}, err
			}
			position.ClusterPosition = value
			clusterSeen = true
		case idCueRelativePos:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return CueTrackPosition{}, err
			}
			position.RelativePosition = value
			position.RelativePositionSet = true
		case idCueDuration:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return CueTrackPosition{}, err
			}
			position.DurationNS, err = scaleCueTicks(value, d.timecodeScaleNS)
			if err != nil {
				return CueTrackPosition{}, err
			}
			position.DurationSet = true
		case idCueBlockNumber:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return CueTrackPosition{}, err
			}
			if value == 0 {
				return CueTrackPosition{}, ErrInvalidData
			}
			position.BlockNumber = value
			position.BlockNumberSet = true
		case idCueCodecState:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return CueTrackPosition{}, err
			}
			position.CodecStatePosition = value
			position.CodecStateSet = true
		case idCueReference:
			reference, err := d.parseCueReference(master.Reader(), child)
			if err != nil {
				return CueTrackPosition{}, err
			}
			position.References = append(position.References, reference)
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return CueTrackPosition{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return CueTrackPosition{}, err
	}
	if !trackSeen || !clusterSeen {
		return CueTrackPosition{}, ErrInvalidData
	}
	return position, nil
}

func (d *Demuxer) parseCueReference(parent io.Reader, header ebml.Header) (CueReference, error) {
	if header.Size.Unknown {
		return CueReference{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return CueReference{}, err
	}
	reference := CueReference{BlockNumber: 1}
	timeSeen := false
	clusterSeen := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return CueReference{}, err
		}
		switch child.ID {
		case idCueRefTime:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return CueReference{}, err
			}
			reference.TimeNS, err = scaleCueTicks(value, d.timecodeScaleNS)
			if err != nil {
				return CueReference{}, err
			}
			timeSeen = true
		case idCueRefCluster:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return CueReference{}, err
			}
			reference.ClusterPosition = value
			clusterSeen = true
		case idCueRefNumber:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return CueReference{}, err
			}
			if value == 0 {
				return CueReference{}, ErrInvalidData
			}
			reference.BlockNumber = value
			reference.BlockNumberSet = true
		case idCueRefCodecState:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return CueReference{}, err
			}
			reference.CodecStatePosition = value
			reference.CodecStateSet = true
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return CueReference{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return CueReference{}, err
	}
	if !timeSeen || !clusterSeen {
		return CueReference{}, ErrInvalidData
	}
	return reference, nil
}

func (d *Demuxer) parseTrackEntry(parent io.Reader, header ebml.Header) (Track, error) {
	if header.Size.Unknown {
		return Track{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return Track{}, err
	}
	track := Track{Language: "und", TimebaseNum: 1, TimebaseDen: timeNS, CodecDecodeAll: true, FlagEnabled: true, FlagDefault: true, FlagLacing: true}
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
		case idMinCache:
			track.MinCache, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.MinCacheSet = true
		case idMaxCache:
			track.MaxCache, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.MaxCacheSet = true
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
		case idMaxBlockAdditionID:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.MaxBlockAdditionID = value
		case idBlockAdditionMapping:
			mapping, err := d.parseBlockAdditionMapping(master.Reader(), child)
			if err != nil {
				return Track{}, err
			}
			track.BlockAdditionMappings = append(track.BlockAdditionMappings, mapping)
		case idCodecDecodeAll:
			value, err := readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.CodecDecodeAll = value
			track.CodecDecodeAllSet = true
		case idTrackOverlay:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			if value == 0 {
				return Track{}, ErrInvalidData
			}
			track.TrackOverlays = append(track.TrackOverlays, value)
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
		case idTrackTranslate:
			translate, err := d.parseTrackTranslate(master.Reader(), child)
			if err != nil {
				return Track{}, err
			}
			track.TrackTranslates = append(track.TrackTranslates, translate)
		case idContentEncodings:
			encodings, err := d.parseContentEncodings(master.Reader(), child)
			if err != nil {
				return Track{}, err
			}
			track.ContentEncodings = encodings
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
	if err := validateTrackBlockAdditionMetadata(track); err != nil {
		return Track{}, err
	}
	if err := validateContentEncodings(track.ContentEncodings); err != nil {
		return Track{}, ErrInvalidData
	}
	return track, nil
}

func (d *Demuxer) parseTrackTranslate(parent io.Reader, header ebml.Header) (TrackTranslate, error) {
	if header.Size.Unknown {
		return TrackTranslate{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return TrackTranslate{}, err
	}
	var translate TrackTranslate
	trackIDSeen := false
	codecSeen := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return TrackTranslate{}, err
		}
		switch child.ID {
		case idTrackTranslateTrack:
			translate.TrackID, err = readBinaryPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return TrackTranslate{}, err
			}
			trackIDSeen = true
		case idTrackTranslateCodec:
			translate.Codec, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return TrackTranslate{}, err
			}
			codecSeen = true
		case idTrackTranslateEdit:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return TrackTranslate{}, err
			}
			translate.EditionUIDs = append(translate.EditionUIDs, value)
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return TrackTranslate{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return TrackTranslate{}, err
	}
	if !trackIDSeen || !codecSeen {
		return TrackTranslate{}, ErrInvalidData
	}
	return translate, nil
}

func (d *Demuxer) parseBlockAdditionMapping(parent io.Reader, header ebml.Header) (BlockAdditionMapping, error) {
	if header.Size.Unknown {
		return BlockAdditionMapping{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return BlockAdditionMapping{}, err
	}
	var mapping BlockAdditionMapping
	idSeen := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return BlockAdditionMapping{}, err
		}
		switch child.ID {
		case idBlockAddIDValue:
			mapping.IDValue, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return BlockAdditionMapping{}, err
			}
			idSeen = true
		case idBlockAddIDName:
			mapping.Name, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return BlockAdditionMapping{}, err
			}
		case idBlockAddIDType:
			mapping.Type, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return BlockAdditionMapping{}, err
			}
		case idBlockAddIDExtraData:
			mapping.ExtraData, err = readBinaryPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return BlockAdditionMapping{}, err
			}
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return BlockAdditionMapping{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return BlockAdditionMapping{}, err
	}
	if !idSeen || mapping.IDValue < 2 {
		return BlockAdditionMapping{}, ErrInvalidData
	}
	return mapping, nil
}

func (d *Demuxer) parseContentEncodings(parent io.Reader, header ebml.Header) ([]ContentEncoding, error) {
	if header.Size.Unknown {
		return nil, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return nil, err
	}
	var encodings []ContentEncoding
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return nil, err
		}
		switch child.ID {
		case idContentEncoding:
			encoding, err := d.parseContentEncoding(master.Reader(), child)
			if err != nil {
				return nil, err
			}
			encodings = append(encodings, encoding)
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return nil, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return nil, err
	}
	if len(encodings) == 0 {
		return nil, ErrInvalidData
	}
	if err := validateContentEncodings(encodings); err != nil {
		return nil, ErrInvalidData
	}
	return encodings, nil
}

func (d *Demuxer) parseContentEncoding(parent io.Reader, header ebml.Header) (ContentEncoding, error) {
	if header.Size.Unknown {
		return ContentEncoding{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return ContentEncoding{}, err
	}
	encoding := ContentEncoding{Scope: ContentEncodingScopeBlock}
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return ContentEncoding{}, err
		}
		switch child.ID {
		case idContentEncodingOrd:
			encoding.Order, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ContentEncoding{}, err
			}
		case idContentEncodingScope:
			encoding.Scope, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ContentEncoding{}, err
			}
		case idContentEncodingType:
			encoding.Type, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ContentEncoding{}, err
			}
		case idContentCompression:
			if encoding.CompressionSet {
				return ContentEncoding{}, ErrInvalidData
			}
			encoding.Compression, err = d.parseContentCompression(master.Reader(), child)
			if err != nil {
				return ContentEncoding{}, err
			}
			encoding.CompressionSet = true
		case idContentEncryption:
			if encoding.EncryptionSet {
				return ContentEncoding{}, ErrInvalidData
			}
			encoding.Encryption, err = d.parseContentEncryption(master.Reader(), child)
			if err != nil {
				return ContentEncoding{}, err
			}
			encoding.EncryptionSet = true
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return ContentEncoding{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return ContentEncoding{}, err
	}
	return encoding, nil
}

func (d *Demuxer) parseContentCompression(parent io.Reader, header ebml.Header) (ContentCompression, error) {
	if header.Size.Unknown {
		return ContentCompression{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return ContentCompression{}, err
	}
	var compression ContentCompression
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return ContentCompression{}, err
		}
		switch child.ID {
		case idContentCompAlgo:
			compression.Algorithm, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ContentCompression{}, err
			}
		case idContentCompSettings:
			compression.Settings, err = readBinaryPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ContentCompression{}, err
			}
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return ContentCompression{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return ContentCompression{}, err
	}
	return compression, nil
}

func (d *Demuxer) parseContentEncryption(parent io.Reader, header ebml.Header) (ContentEncryption, error) {
	if header.Size.Unknown {
		return ContentEncryption{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return ContentEncryption{}, err
	}
	var encryption ContentEncryption
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return ContentEncryption{}, err
		}
		switch child.ID {
		case idContentEncAlgo:
			encryption.Algorithm, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ContentEncryption{}, err
			}
		case idContentEncKeyID:
			encryption.KeyID, err = readBinaryPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ContentEncryption{}, err
			}
		case idContentEncAES:
			if encryption.AESSettingsSet {
				return ContentEncryption{}, ErrInvalidData
			}
			encryption.AESSettings, err = d.parseContentEncAESSettings(master.Reader(), child)
			if err != nil {
				return ContentEncryption{}, err
			}
			encryption.AESSettingsSet = true
		case idContentSignature:
			encryption.Signature, err = readBinaryPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ContentEncryption{}, err
			}
		case idContentSigKeyID:
			encryption.SignatureKeyID, err = readBinaryPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ContentEncryption{}, err
			}
		case idContentSigAlgo:
			encryption.SignatureAlgorithm, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ContentEncryption{}, err
			}
		case idContentSigHashAlgo:
			encryption.SignatureHashAlgorithm, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ContentEncryption{}, err
			}
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return ContentEncryption{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return ContentEncryption{}, err
	}
	return encryption, nil
}

func (d *Demuxer) parseContentEncAESSettings(parent io.Reader, header ebml.Header) (ContentEncAESSettings, error) {
	if header.Size.Unknown {
		return ContentEncAESSettings{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return ContentEncAESSettings{}, err
	}
	var settings ContentEncAESSettings
	cipherSeen := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return ContentEncAESSettings{}, err
		}
		switch child.ID {
		case idContentEncAESCipher:
			settings.CipherMode, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ContentEncAESSettings{}, err
			}
			cipherSeen = true
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return ContentEncAESSettings{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return ContentEncAESSettings{}, err
	}
	if !cipherSeen {
		return ContentEncAESSettings{}, ErrInvalidData
	}
	return settings, nil
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
		case idFlagInterlaced:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.FlagInterlaced, err = intFromUint(value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.FlagInterlacedSet = true
		case idFieldOrder:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.FieldOrder, err = intFromUint(value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.FieldOrderSet = true
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
		case idAspectRatioType:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.AspectRatioType, err = intFromUint(value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.AspectRatioTypeSet = true
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
		case idBlockAdditions:
			additions, err := d.parseBlockAdditions(d.groupReader, child)
			if err != nil {
				return err
			}
			dst.BlockAdditions = append(dst.BlockAdditions, additions...)
		case idReferencePriority:
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
		case idCodecState:
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
	if maxAdditionID, err := validateBlockAdditions(dst.BlockAdditions); err != nil {
		return err
	} else if maxAdditionID != 0 {
		track, ok := d.track(dst.TrackID)
		if !ok {
			return ErrUnknownTrack
		}
		if maxAdditionID > track.MaxBlockAdditionID {
			return ErrInvalidData
		}
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
	haveAdditional := false
	for limit.N > 0 {
		child, err := reader.ReadHeader()
		if err != nil {
			return BlockAddition{}, err
		}
		switch child.ID {
		case idBlockAddID:
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
	timecode := d.clusterTimecode + int64(blockTimecode)
	if timecode < 0 {
		return ErrInvalidData
	}
	if lacing != 0 {
		return d.readLacedBlockPayload(track, trackNumber, timecode, flags, lacing, dst, simple)
	}
	frameSize := int(d.blockLimit.N)
	if len(track.ContentEncodings) != 0 {
		encoding, err := blockContentEncoding(track)
		if err != nil {
			return err
		}
		switch encoding.transform {
		case blockContentTransformNone:
			if err := d.readPlainBlockFrame(track, frameSize, nil, dst); err != nil {
				return err
			}
		case blockContentTransformHeaderStripping:
			if err := d.readPlainBlockFrame(track, frameSize, encoding.settings, dst); err != nil {
				return err
			}
		case blockContentTransformZlib:
			frame, err := d.readBlockFrameScratch(frameSize)
			if err != nil {
				return err
			}
			if err := d.decodeZlibBlockFrame(track, frame, dst); err != nil {
				return err
			}
		case blockContentTransformBzlib:
			frame, err := d.readBlockFrameScratch(frameSize)
			if err != nil {
				return err
			}
			if err := d.decodeBzlibBlockFrame(track, frame, dst); err != nil {
				return err
			}
		default:
			return ErrUnsupportedContentEncoding
		}
	} else if err := d.readPlainBlockFrame(track, frameSize, nil, dst); err != nil {
		return err
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

func (d *Demuxer) readBlockFrameScratch(frameSize int) ([]byte, error) {
	if frameSize < 0 {
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

func (d *Demuxer) decodeZlibBlockFrame(track Track, frame []byte, dst *Packet) error {
	decoded, err := zlibDecompressInto(dst.Data[:0], frame)
	if err != nil {
		return err
	}
	dst.Data = decoded
	return d.finishTrackCodecPayload(track, dst)
}

func (d *Demuxer) decodeBzlibBlockFrame(track Track, frame []byte, dst *Packet) error {
	decoded, err := bzip2DecompressInto(dst.Data[:0], frame)
	if err != nil {
		return err
	}
	dst.Data = decoded
	return d.finishTrackCodecPayload(track, dst)
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
	if len(track.ContentEncodings) != 0 {
		encoding, err := blockContentEncoding(track)
		if err != nil {
			d.clearLace()
			return err
		}
		d.laceContent = encoding
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
	frameData := d.laceBuffer[frame.offset : frame.offset+frame.size]
	if d.laceContent.transform == blockContentTransformZlib || d.laceContent.transform == blockContentTransformBzlib {
		var decoded []byte
		var err error
		if d.laceContent.transform == blockContentTransformZlib {
			decoded, err = zlibDecompressInto(dst.Data[:0], frameData)
		} else {
			decoded, err = bzip2DecompressInto(dst.Data[:0], frameData)
		}
		if err != nil {
			return err
		}
		dst.Data = decoded
		if err := d.finishLacedCodecPayload(dst); err != nil {
			return err
		}
	} else {
		headerStrip := d.laceContent.settings
		if d.laceContent.transform != blockContentTransformHeaderStripping {
			headerStrip = nil
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
	d.laceFrameIndex++
	if d.laceFrameIndex >= d.laceFrameCount {
		d.clearLace()
	}
	return nil
}

func (d *Demuxer) finishLacedCodecPayload(dst *Packet) error {
	if d.laceH264Length != 0 {
		outSize, err := h264AVCToAnnexBSize(dst.Data, d.laceH264Length)
		if err != nil {
			return err
		}
		dst.Data, err = h264AVCToAnnexBInPlace(dst.Data, outSize, d.laceH264Length)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *Demuxer) clearLace() {
	d.laceTrackID = 0
	d.laceH264Length = 0
	d.laceContent = blockContentEncodingInfo{}
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
