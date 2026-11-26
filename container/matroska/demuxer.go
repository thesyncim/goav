package matroska

import (
	"bytes"
	"errors"
	"io"
	"math"

	"github.com/thesyncim/goav/container/ebml"
)

type clusterState uint8

const (
	clusterNone clusterState = iota
	clusterSized
	clusterUnknownSize
)

type Demuxer struct {
	reader                *ebml.Reader
	seeker                io.ReadSeeker
	options               DemuxerOptions
	docType               string
	segmentData           int64
	segmentSize           uint64
	segmentUnknown        bool
	timecodeScaleNS       int64
	info                  SegmentInfo
	tracks                []Track
	attachments           []Attachment
	chapters              []ChapterEdition
	tags                  []Tag
	unknownSegments       []UnknownElement
	unknownTracks         []UnknownElement
	unknownAttachments    []UnknownElement
	unknownChapters       []UnknownElement
	unknownTags           []UnknownElement
	pendingClusterUnknown []UnknownElement
	cues                  []CuePoint
	cuesSeen              bool
	cuesSorted            bool
	seekEntries           []SeekEntry
	preloadedTopLevel     []preloadedTopLevelElement
	clusterIndex          []clusterIndexEntry
	clusterIndexBuilt     bool
	packetIndex           []packetIndexEntry
	packetTrackIndex      []packetTrackIndex
	packetIndexBuilt      bool
	seekHeadCount         int
	infoSeen              bool
	tracksSeen            bool
	attachmentsSeen       bool
	chaptersSeen          bool
	tagsSeen              bool
	inSegment             bool
	cluster               clusterState
	clusterEnd            int64
	clusterTimecode       int64
	blockLimit            io.LimitedReader
	groupLimit            io.LimitedReader
	groupReader           *ebml.Reader
	blockHeader           [3]byte
	laceBuffer            []byte
	laceFrames            []laceFrame
	laceTrackID           uint32
	laceH264Length        int
	laceContent           blockContentEncodingInfo
	laceTimeNS            int64
	laceDurationNS        int64
	laceReferences        []int64
	lacePriority          uint64
	laceDiscardPadNS      int64
	laceCodecState        []byte
	laceAdditions         []BlockAddition
	laceFrameCount        int
	laceFrameIndex        int
	laceSeekFrameIndex    int
	lastLaceBaseTimeNS    int64
	lastLaceFrameIndex    int
	lastLaceFrameCount    int
	laceKeyframe          bool
	laceInvisible         bool
	laceDiscardable       bool
	scratch               [ebml.MaxSizeWidth]byte
	uintScratch           [8]byte
	contentBuffer         []byte
	contentPartitions     []uint32
	contentCiphers        []cachedContentCipher
	zlibInput             bytes.Reader
	zlibReader            io.ReadCloser
	pendingHeader         ebml.Header
	pendingHeaderSet      bool
}

type laceFrame struct {
	offset int
	size   int
}

type clusterIndexEntry struct {
	TimeNS   int64
	Position uint64
}

type packetIndexEntry struct {
	TimeNS          int64
	TrackID         uint32
	TrackIndex      int
	ClusterPosition uint64
	BlockPosition   uint64
	ClusterTimecode int64
	FrameIndex      int
}

type packetTrackIndex struct {
	TrackID uint32
	Entries []int
}

type preloadedTopLevelElement struct {
	ID     ebml.ID
	Offset int64
}

type indexedBlockInfo struct {
	TrackID         uint32
	TrackIndex      int
	TimeNS          int64
	FrameCount      int
	FrameDurationNS int64
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
	if err := validateContentEncryptionOptions(opts.ContentEncryptionKeys, nil); err != nil {
		return err
	}
	d.reader = ebml.NewReader(r, ebml.ReaderOptions{MaxElementSize: opts.MaxElementSize})
	// A reader with a Seek method may still not reposition (an *os.File over
	// a pipe): ask it where it is, which a pipe refuses.
	if seeker, ok := r.(io.ReadSeeker); ok {
		if _, err := seeker.Seek(0, io.SeekCurrent); err == nil {
			d.seeker = seeker
		}
	}
	if d.groupReader == nil {
		d.groupReader = ebml.NewReader(&d.groupLimit, ebml.ReaderOptions{MaxElementSize: opts.MaxElementSize})
	}
	d.options = opts
	d.options.ContentEncryptionKeys = cloneContentEncryptionKeys(d.options.ContentEncryptionKeys)
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
	d.segmentSize = 0
	d.segmentUnknown = false
	d.timecodeScaleNS = defaultTimecodeScaleNS
	d.info = SegmentInfo{}
	d.tracks = d.tracks[:0]
	d.attachments = d.attachments[:0]
	d.chapters = d.chapters[:0]
	d.tags = d.tags[:0]
	d.unknownSegments = d.unknownSegments[:0]
	d.unknownTracks = d.unknownTracks[:0]
	d.unknownAttachments = d.unknownAttachments[:0]
	d.unknownChapters = d.unknownChapters[:0]
	d.unknownTags = d.unknownTags[:0]
	d.pendingClusterUnknown = d.pendingClusterUnknown[:0]
	d.cues = d.cues[:0]
	d.cuesSeen = false
	d.cuesSorted = true
	d.seekEntries = d.seekEntries[:0]
	d.preloadedTopLevel = d.preloadedTopLevel[:0]
	d.clusterIndex = d.clusterIndex[:0]
	d.clusterIndexBuilt = false
	d.packetIndex = d.packetIndex[:0]
	d.packetTrackIndex = d.packetTrackIndex[:0]
	d.packetIndexBuilt = false
	d.seekHeadCount = 0
	d.infoSeen = false
	d.tracksSeen = false
	d.attachmentsSeen = false
	d.chaptersSeen = false
	d.tagsSeen = false
	d.inSegment = false
	d.cluster = clusterNone
	d.clusterEnd = 0
	d.clusterTimecode = 0
	d.pendingHeader = ebml.Header{}
	d.pendingHeaderSet = false
	d.clearLace()
	return d.readPreamble()
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

func (d *Demuxer) UnknownSegmentElements() []UnknownElement {
	if d == nil || len(d.unknownSegments) == 0 {
		return nil
	}
	return cloneUnknownElements(d.unknownSegments)
}

func (d *Demuxer) UnknownTracksElements() []UnknownElement {
	if d == nil || len(d.unknownTracks) == 0 {
		return nil
	}
	return cloneUnknownElements(d.unknownTracks)
}

func (d *Demuxer) UnknownAttachmentsElements() []UnknownElement {
	if d == nil || len(d.unknownAttachments) == 0 {
		return nil
	}
	return cloneUnknownElements(d.unknownAttachments)
}

func (d *Demuxer) UnknownChaptersElements() []UnknownElement {
	if d == nil || len(d.unknownChapters) == 0 {
		return nil
	}
	return cloneUnknownElements(d.unknownChapters)
}

func (d *Demuxer) UnknownTagsElements() []UnknownElement {
	if d == nil || len(d.unknownTags) == 0 {
		return nil
	}
	return cloneUnknownElements(d.unknownTags)
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

// ReadPacket reads the next packet into dst. The read is zero-allocation by
// contract: the payload is written into dst.Data's existing capacity (Reset
// keeps the backing array), so the caller sizes dst.Data once — for example
// make([]byte, 0, maxPacketSize) — and a payload that does not fit is refused
// with ErrPayloadTooSmall instead of growing the buffer. io.EOF reports the
// end of the media.
func (d *Demuxer) ReadPacket(dst *Packet) error {
	if d == nil || d.reader == nil {
		return ErrNilReader
	}
	if dst == nil {
		return ErrNilPacket
	}
	dst.Reset()
	if d.laceFrameIndex < d.laceFrameCount {
		return d.readNextLacedPacket(dst)
	}
	for {
		if d.cluster == clusterSized && d.reader.Offset() >= d.clusterEnd {
			d.cluster = clusterNone
		}
		header, err := d.nextPacketHeader()
		if err != nil {
			return err
		}
		if d.cluster != clusterNone {
			if d.cluster == clusterUnknownSize && isUnknownClusterTerminator(header.ID) {
				d.cluster = clusterNone
				d.pendingClusterUnknown = d.pendingClusterUnknown[:0]
			} else {
				done, err := d.readClusterElement(header, dst)
				if err != nil || done {
					return err
				}
				continue
			}
		}
		if err := d.readSegmentElement(header); err != nil {
			return err
		}
	}
}

func (d *Demuxer) readClusterElement(header ebml.Header, dst *Packet) (bool, error) {
	switch header.ID {
	case idCluster:
		return false, d.enterCluster(header)
	case idTimestamp:
		value, err := readUIntPayloadScratch(d.reader, header.Size.Value, &d.uintScratch)
		if err != nil {
			return false, err
		}
		if value > uint64(math.MaxInt64) {
			return false, ErrInvalidData
		}
		d.clusterTimecode = int64(value)
		return false, nil
	case idSimpleBlock:
		return true, d.finishClusterPacket(d.readSimpleBlock(header, dst), dst)
	case idBlockGroup:
		return true, d.finishClusterPacket(d.readBlockGroup(header, dst), dst)
	case idVoid, idCRC32:
		return false, skipElement(d.reader, header)
	default:
		return false, d.readUnknownClusterElement(header)
	}
}

func (d *Demuxer) readSegmentElement(header ebml.Header) error {
	switch header.ID {
	case idSeekHead:
		return d.parseSegmentSeekHead(header)
	case idInfo:
		return d.parseSegmentInfo(header)
	case idTracks:
		return d.parseSegmentTracks(header)
	case idAttachments:
		return d.parseSegmentAttachments(header)
	case idChapters:
		return d.parseSegmentChapters(header)
	case idTags:
		return d.parseSegmentTags(header)
	case idCues:
		return d.parseSegmentCues(header)
	case idCluster:
		return d.enterCluster(header)
	default:
		return skipElement(d.reader, header)
	}
}

func (d *Demuxer) readUnknownSegmentElement(header ebml.Header) error {
	element, err := readUnknownElementPayload(d.reader, header)
	if err != nil {
		return err
	}
	if isKnownSegmentElement(ebml.ID(element.ID)) {
		return ErrInvalidData
	}
	d.unknownSegments = append(d.unknownSegments, element)
	return nil
}

func (d *Demuxer) readUnknownClusterElement(header ebml.Header) error {
	if isStructuralElement(header.ID) {
		return skipElement(d.reader, header)
	}
	if isKnownClusterElement(header.ID) {
		return ErrInvalidData
	}
	element, err := readUnknownElementPayload(d.reader, header)
	if err != nil {
		return err
	}
	d.pendingClusterUnknown = append(d.pendingClusterUnknown, element)
	return nil
}

func (d *Demuxer) finishClusterPacket(err error, dst *Packet) error {
	if err != nil {
		if !errors.Is(err, ErrPayloadTooSmall) || d.laceFrameCount == 0 {
			d.pendingClusterUnknown = d.pendingClusterUnknown[:0]
		}
		return err
	}
	d.applyPendingClusterUnknownElements(dst)
	return nil
}

func (d *Demuxer) applyPendingClusterUnknownElements(dst *Packet) {
	if len(d.pendingClusterUnknown) == 0 {
		return
	}
	dst.UnknownClusterElements = append(dst.UnknownClusterElements, d.pendingClusterUnknown...)
	d.pendingClusterUnknown = d.pendingClusterUnknown[:0]
}

func appendUnknownChildElement(reader *ebml.Reader, header ebml.Header, isKnown func(ebml.ID) bool, elements *[]UnknownElement) error {
	if isStructuralElement(header.ID) {
		return skipElement(reader, header)
	}
	if isKnown != nil && isKnown(header.ID) {
		return ErrInvalidData
	}
	element, err := readUnknownElementPayload(reader, header)
	if err != nil {
		return err
	}
	*elements = append(*elements, element)
	return nil
}

func isStructuralElement(id ebml.ID) bool {
	return id == idVoid || id == idCRC32
}

func (d *Demuxer) nextPacketHeader() (ebml.Header, error) {
	if d.pendingHeaderSet {
		header := d.pendingHeader
		d.pendingHeader = ebml.Header{}
		d.pendingHeaderSet = false
		return header, nil
	}
	return d.reader.ReadHeader()
}

func (d *Demuxer) defaultDurationNS(trackID uint32) int64 {
	index, ok := d.trackIndex(trackID)
	if !ok {
		return 0
	}
	return d.tracks[index].DefaultDurationNS
}

func (d *Demuxer) hasTrack(id uint32) bool {
	_, ok := d.trackIndex(id)
	return ok
}

func (d *Demuxer) hasTrackUID(uid uint64) bool {
	if uid == 0 {
		return false
	}
	for i := range d.tracks {
		if d.tracks[i].UID == uid {
			return true
		}
	}
	return false
}

func (d *Demuxer) track(id uint32) (Track, bool) {
	t, ok := d.trackPtr(id)
	if !ok {
		return Track{}, false
	}
	return *t, true
}

func (d *Demuxer) trackPtr(id uint32) (*Track, bool) {
	index, ok := d.trackIndex(id)
	if !ok {
		return nil, false
	}
	return &d.tracks[index], true
}

func (d *Demuxer) trackIndex(id uint32) (int, bool) {
	for i := range d.tracks {
		if d.tracks[i].ID == id {
			return i, true
		}
	}
	return 0, false
}

func isEOF(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}
