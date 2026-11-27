package matroska

import (
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
	"math"

	"github.com/thesyncim/goav/container/ebml"
)

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
			d.segmentUnknown = header.Size.Unknown
			d.segmentSize = 0
			if !header.Size.Unknown {
				d.segmentSize = header.Size.Value
			}
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
	if d.cuesState != cuesUnseen {
		return ErrInvalidData
	}
	return d.loadTopLevelElementFromSeekHead(idCues)
}

func (d *Demuxer) readSegmentHeaders() error {
	for {
		header, err := d.reader.ReadHeader()
		if err != nil {
			return err
		}
		switch header.ID {
		case idSeekHead:
			if err := d.parseSegmentSeekHead(header); err != nil {
				return err
			}
		case idInfo:
			if err := d.parseSegmentInfo(header); err != nil {
				return err
			}
		case idTracks:
			if err := d.parseSegmentTracks(header); err != nil {
				return err
			}
		case idAttachments:
			if err := d.parseSegmentAttachments(header); err != nil {
				return err
			}
		case idChapters:
			if err := d.parseSegmentChapters(header); err != nil {
				return err
			}
		case idTags:
			if err := d.parseSegmentTags(header); err != nil {
				return err
			}
		case idCues:
			if err := d.parseSegmentCues(header); err != nil {
				return err
			}
		case idCluster:
			if err := d.ensureRequiredSegmentHeaders(header); err != nil {
				return err
			}
			return d.enterCluster(header)
		case idVoid, idCRC32:
			if err := skipElement(d.reader, header); err != nil {
				return err
			}
		default:
			if err := d.readUnknownSegmentElement(header); err != nil {
				return err
			}
		}
	}
}

func (d *Demuxer) parseSegmentSeekHead(header ebml.Header) error {
	if d.shouldSkipPreloadedTopLevelElement(header) {
		return skipElement(d.reader, header)
	}
	if d.seekHeadCount >= 2 {
		return ErrInvalidData
	}
	d.seekHeadCount++
	return d.parseSeekHead(header)
}

func (d *Demuxer) parseSegmentInfo(header ebml.Header) error {
	if d.shouldSkipPreloadedTopLevelElement(header) {
		return skipElement(d.reader, header)
	}
	if d.infoSeen {
		return ErrInvalidData
	}
	d.infoSeen = true
	return d.parseInfo(header)
}

func (d *Demuxer) parseSegmentTracks(header ebml.Header) error {
	if d.shouldSkipPreloadedTopLevelElement(header) {
		return skipElement(d.reader, header)
	}
	if d.tracksSeen {
		return ErrInvalidData
	}
	d.tracksSeen = true
	return d.parseTracks(header)
}

func (d *Demuxer) parseSegmentAttachments(header ebml.Header) error {
	if d.shouldSkipPreloadedTopLevelElement(header) {
		return skipElement(d.reader, header)
	}
	if d.attachmentsSeen {
		return ErrInvalidData
	}
	d.attachmentsSeen = true
	return d.parseAttachments(header)
}

func (d *Demuxer) parseSegmentChapters(header ebml.Header) error {
	if d.shouldSkipPreloadedTopLevelElement(header) {
		return skipElement(d.reader, header)
	}
	if d.chaptersSeen {
		return ErrInvalidData
	}
	d.chaptersSeen = true
	return d.parseChapters(header)
}

func (d *Demuxer) parseSegmentTags(header ebml.Header) error {
	if d.shouldSkipPreloadedTopLevelElement(header) {
		return skipElement(d.reader, header)
	}
	d.tagsSeen = true
	return d.parseTags(header)
}

func (d *Demuxer) parseSegmentCues(header ebml.Header) error {
	if d.shouldSkipPreloadedTopLevelElement(header) {
		return skipElement(d.reader, header)
	}
	if d.cuesState != cuesUnseen {
		return ErrInvalidData
	}
	d.cuesState = cuesSorted
	return d.parseCues(header)
}

func (d *Demuxer) shouldSkipPreloadedTopLevelElement(header ebml.Header) bool {
	for i := range d.preloadedTopLevel {
		if d.preloadedTopLevel[i].ID == header.ID && d.preloadedTopLevel[i].Offset == header.Offset {
			return true
		}
	}
	return false
}

func (d *Demuxer) ensureRequiredSegmentHeaders(cluster ebml.Header) error {
	loadedFromSeekHead := false
	if !d.infoSeen {
		if d.seeker == nil {
			return ErrInvalidData
		}
		if err := d.loadTopLevelElementFromSeekHead(idInfo); err != nil {
			return err
		}
		loadedFromSeekHead = true
	}
	if !d.tracksSeen {
		if d.seeker == nil {
			return ErrInvalidData
		}
		if err := d.loadTopLevelElementFromSeekHead(idTracks); err != nil {
			return err
		}
		loadedFromSeekHead = true
	}
	loadedOptional, err := d.loadOptionalTopLevelMetadataFromSeekHead()
	if err != nil {
		return err
	}
	loadedFromSeekHead = loadedFromSeekHead || loadedOptional
	if !loadedFromSeekHead {
		return nil
	}
	if _, err := d.seeker.Seek(cluster.DataOffset, io.SeekStart); err != nil {
		return err
	}
	d.reader.ResetAt(d.seeker, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize}, cluster.DataOffset)
	return nil
}

func (d *Demuxer) loadOptionalTopLevelMetadataFromSeekHead() (bool, error) {
	if d.seeker == nil {
		return false, nil
	}
	loaded := false
	if !d.attachmentsSeen {
		ok, err := d.loadOptionalTopLevelElementFromSeekHead(idAttachments)
		if err != nil {
			return false, err
		}
		loaded = loaded || ok
	}
	if !d.chaptersSeen {
		ok, err := d.loadOptionalTopLevelElementFromSeekHead(idChapters)
		if err != nil {
			return false, err
		}
		loaded = loaded || ok
	}
	if !d.tagsSeen {
		ok, err := d.loadOptionalTopLevelElementFromSeekHead(idTags)
		if err != nil {
			return false, err
		}
		loaded = loaded || ok
	}
	return loaded, nil
}

func (d *Demuxer) loadOptionalTopLevelElementFromSeekHead(id ebml.ID) (bool, error) {
	if _, ok := d.seekHeadPosition(id); !ok {
		return false, nil
	}
	if err := d.loadTopLevelElementFromSeekHead(id); err != nil {
		return false, err
	}
	return true, nil
}

func (d *Demuxer) loadTopLevelElementFromSeekHead(id ebml.ID) error {
	position, ok := d.seekHeadPosition(id)
	if !ok {
		return ErrInvalidData
	}
	if position > uint64(math.MaxInt64) || int64(position) > math.MaxInt64-d.segmentData {
		return ErrInvalidData
	}
	offset := d.segmentData + int64(position)
	if _, err := d.seeker.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	d.reader.ResetAt(d.seeker, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize}, offset)
	header, err := d.reader.ReadHeader()
	if err != nil {
		return err
	}
	if header.ID != id {
		return ErrInvalidData
	}
	if err := d.parseLoadedTopLevelElement(header); err != nil {
		return err
	}
	d.preloadedTopLevel = append(d.preloadedTopLevel, preloadedTopLevelElement{
		ID:     id,
		Offset: header.Offset,
	})
	return nil
}

func (d *Demuxer) seekHeadPosition(id ebml.ID) (uint64, bool) {
	for i := range d.seekEntries {
		if d.seekEntries[i].ID == uint64(id) {
			return d.seekEntries[i].Position, true
		}
	}
	return 0, false
}

func (d *Demuxer) parseLoadedTopLevelElement(header ebml.Header) error {
	switch header.ID {
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
	default:
		return ErrInvalidData
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
	var idSet bool
	var positionSet bool
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return SeekEntry{}, err
		}
		switch child.ID {
		case idSeekID:
			if idSet {
				return SeekEntry{}, ErrInvalidData
			}
			value, err := readElementIDPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return SeekEntry{}, err
			}
			entry.ID = uint64(value)
			idSet = true
		case idSeekPosition:
			if positionSet {
				return SeekEntry{}, ErrInvalidData
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return SeekEntry{}, err
			}
			entry.Position = value
			positionSet = true
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return SeekEntry{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return SeekEntry{}, err
	}
	if !idSet || !positionSet {
		return SeekEntry{}, ErrInvalidData
	}
	return entry, nil
}

func (d *Demuxer) parseEBMLHeader(header ebml.Header) error {
	if header.Size.Unknown {
		return ErrInvalidData
	}
	limited := d.reader.Limited(header.Size.Value)
	reader := ebml.NewReader(limited, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize})
	state := ebmlHeaderState{
		ebmlVersion:        1,
		ebmlReadVersion:    1,
		ebmlMaxIDLength:    ebml.MaxIDWidth,
		ebmlMaxSizeLength:  ebml.MaxSizeWidth,
		docTypeVersion:     1,
		docTypeReadVersion: 1,
	}
	for limited.N > 0 {
		child, err := reader.ReadHeader()
		if err != nil {
			return err
		}
		if child.Size.Unknown || child.Size.Value > uint64(limited.N) {
			return ErrInvalidData
		}
		switch child.ID {
		case idEBMLVersion:
			if state.ebmlVersionSet {
				return ErrInvalidData
			}
			value, err := readUIntPayload(reader, child.Size.Value)
			if err != nil {
				return err
			}
			state.ebmlVersion = value
			state.ebmlVersionSet = true
		case idEBMLReadVersion:
			if state.ebmlReadVersionSet {
				return ErrInvalidData
			}
			value, err := readUIntPayload(reader, child.Size.Value)
			if err != nil {
				return err
			}
			state.ebmlReadVersion = value
			state.ebmlReadVersionSet = true
		case idEBMLMaxIDLength:
			if state.ebmlMaxIDLengthSet {
				return ErrInvalidData
			}
			value, err := readUIntPayload(reader, child.Size.Value)
			if err != nil {
				return err
			}
			state.ebmlMaxIDLength = value
			state.ebmlMaxIDLengthSet = true
		case idEBMLMaxSizeLength:
			if state.ebmlMaxSizeLengthSet {
				return ErrInvalidData
			}
			value, err := readUIntPayload(reader, child.Size.Value)
			if err != nil {
				return err
			}
			state.ebmlMaxSizeLength = value
			state.ebmlMaxSizeLengthSet = true
		case idDocType:
			if state.docTypeSet {
				return ErrInvalidData
			}
			value, err := readStringPayload(reader, child.Size.Value)
			if err != nil {
				return err
			}
			state.docType = value
			state.docTypeSet = true
		case idDocTypeVersion:
			if state.docTypeVersionSet {
				return ErrInvalidData
			}
			value, err := readUIntPayload(reader, child.Size.Value)
			if err != nil {
				return err
			}
			state.docTypeVersion = value
			state.docTypeVersionSet = true
		case idDocTypeReadVersion:
			if state.docTypeReadVersionSet {
				return ErrInvalidData
			}
			value, err := readUIntPayload(reader, child.Size.Value)
			if err != nil {
				return err
			}
			state.docTypeReadVersion = value
			state.docTypeReadVersionSet = true
		default:
			if err := skipElement(reader, child); err != nil {
				return err
			}
		}
	}
	if err := validateEBMLHeaderState(state); err != nil {
		return err
	}
	d.docType = state.docType
	return nil
}

type ebmlHeaderState struct {
	ebmlVersion           uint64
	ebmlReadVersion       uint64
	ebmlMaxIDLength       uint64
	ebmlMaxSizeLength     uint64
	docType               string
	docTypeVersion        uint64
	docTypeReadVersion    uint64
	ebmlVersionSet        bool
	ebmlReadVersionSet    bool
	ebmlMaxIDLengthSet    bool
	ebmlMaxSizeLengthSet  bool
	docTypeSet            bool
	docTypeVersionSet     bool
	docTypeReadVersionSet bool
}

func validateEBMLHeaderState(state ebmlHeaderState) error {
	if state.ebmlVersion == 0 || state.ebmlReadVersion == 0 || state.ebmlReadVersion > state.ebmlVersion || state.ebmlReadVersion > 1 {
		return ErrInvalidData
	}
	if state.ebmlMaxIDLength != ebml.MaxIDWidth || state.ebmlMaxSizeLength != ebml.MaxSizeWidth {
		return ErrInvalidData
	}
	switch state.docType {
	case "matroska", "webm":
	default:
		return ErrInvalidData
	}
	if !state.docTypeSet || state.docTypeVersion == 0 || state.docTypeReadVersion == 0 {
		return ErrInvalidData
	}
	if state.docTypeReadVersion > state.docTypeVersion || state.docTypeReadVersion > defaultDocTypeVersion {
		return ErrInvalidData
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
	var fields segmentInfoFieldState
	durationTicks := float64(0)
	durationSet := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return err
		}
		if err := d.parseInfoChild(master.Reader(), child, &fields, &durationTicks, &durationSet); err != nil {
			return err
		}
	}
	if err := master.Validate(); err != nil {
		return err
	}
	if durationSet {
		d.info.DurationNS, err = scaleSegmentDurationTicks(durationTicks, d.timecodeScaleNS)
		if err != nil {
			return err
		}
		d.info.DurationSet = true
	}
	return validateSegmentInfo(d.info)
}

type segmentInfoFieldState struct {
	segmentUUIDSet     bool
	segmentFilenameSet bool
	prevUUIDSet        bool
	prevFilenameSet    bool
	nextUUIDSet        bool
	nextFilenameSet    bool
	timestampScaleSet  bool
	durationSet        bool
	dateUTCSet         bool
	titleSet           bool
	muxingAppSet       bool
	writingAppSet      bool
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
		header := r.pending
		if !header.Size.Unknown && header.Size.Value > uint64(r.limited.N) {
			return ebml.Header{}, ErrInvalidData
		}
		return header, nil
	}
	header, err := r.reader.ReadHeader()
	if err != nil {
		return ebml.Header{}, err
	}
	if header.ID == idCRC32 {
		return ebml.Header{}, ErrInvalidData
	}
	if !header.Size.Unknown && header.Size.Value > uint64(r.limited.N) {
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

func markElementSeen(seen *bool) error {
	if *seen {
		return ErrInvalidData
	}
	*seen = true
	return nil
}

func (d *Demuxer) parseInfoChild(reader *ebml.Reader, child ebml.Header, fields *segmentInfoFieldState, durationTicks *float64, durationSet *bool) error {
	switch child.ID {
	case idSegmentUUID:
		if err := markElementSeen(&fields.segmentUUIDSet); err != nil {
			return err
		}
		value, err := readBinaryPayload(reader, child.Size.Value)
		if err != nil {
			return err
		}
		d.info.SegmentUUID = value
	case idSegmentFilename:
		if err := markElementSeen(&fields.segmentFilenameSet); err != nil {
			return err
		}
		value, err := readStringPayload(reader, child.Size.Value)
		if err != nil {
			return err
		}
		d.info.SegmentFilename = value
	case idPrevUUID:
		if err := markElementSeen(&fields.prevUUIDSet); err != nil {
			return err
		}
		value, err := readBinaryPayload(reader, child.Size.Value)
		if err != nil {
			return err
		}
		d.info.PrevUUID = value
	case idPrevFilename:
		if err := markElementSeen(&fields.prevFilenameSet); err != nil {
			return err
		}
		value, err := readStringPayload(reader, child.Size.Value)
		if err != nil {
			return err
		}
		d.info.PrevFilename = value
	case idNextUUID:
		if err := markElementSeen(&fields.nextUUIDSet); err != nil {
			return err
		}
		value, err := readBinaryPayload(reader, child.Size.Value)
		if err != nil {
			return err
		}
		d.info.NextUUID = value
	case idNextFilename:
		if err := markElementSeen(&fields.nextFilenameSet); err != nil {
			return err
		}
		value, err := readStringPayload(reader, child.Size.Value)
		if err != nil {
			return err
		}
		d.info.NextFilename = value
	case idTimestampScale:
		if err := markElementSeen(&fields.timestampScaleSet); err != nil {
			return err
		}
		value, err := readUIntPayload(reader, child.Size.Value)
		if err != nil {
			return err
		}
		if value == 0 || value > uint64(math.MaxInt64) {
			return ErrInvalidData
		}
		d.timecodeScaleNS = int64(value)
	case idDuration:
		if err := markElementSeen(&fields.durationSet); err != nil {
			return err
		}
		value, err := readSegmentDurationTicksPayload(reader, child.Size.Value)
		if err != nil {
			return err
		}
		*durationTicks = value
		*durationSet = true
	case idDateUTC:
		if err := markElementSeen(&fields.dateUTCSet); err != nil {
			return err
		}
		value, err := readDatePayload(reader, child.Size.Value)
		if err != nil {
			return err
		}
		d.info.DateUTC = value
		d.info.DateUTCSet = true
	case idTitle:
		if err := markElementSeen(&fields.titleSet); err != nil {
			return err
		}
		value, err := readStringPayload(reader, child.Size.Value)
		if err != nil {
			return err
		}
		d.info.Title = value
	case idMuxingApp:
		if err := markElementSeen(&fields.muxingAppSet); err != nil {
			return err
		}
		value, err := readStringPayload(reader, child.Size.Value)
		if err != nil {
			return err
		}
		d.info.MuxingApp = value
	case idWritingApp:
		if err := markElementSeen(&fields.writingAppSet); err != nil {
			return err
		}
		value, err := readStringPayload(reader, child.Size.Value)
		if err != nil {
			return err
		}
		d.info.WritingApp = value
	default:
		if err := appendUnknownChildElement(reader, child, isKnownInfoElement, &d.info.UnknownElements); err != nil {
			return err
		}
	}
	return nil
}

func readSegmentDurationTicksPayload(r io.Reader, size uint64) (float64, error) {
	value, err := readFloatPayload(r, size)
	if err != nil {
		return 0, err
	}
	if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, ErrInvalidData
	}
	return value, nil
}

func scaleSegmentDurationTicks(value float64, scaleNS int64) (int64, error) {
	if scaleNS <= 0 || value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, ErrInvalidData
	}
	duration := value * float64(scaleNS)
	if math.IsInf(duration, 0) {
		return 0, ErrInvalidData
	}
	rounded := math.Round(duration)
	if rounded < 0 || rounded >= float64(math.MaxInt64) {
		return 0, ErrInvalidData
	}
	return int64(rounded), nil
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
			if d.hasTrack(track.ID) || d.hasTrackUID(track.UID) {
				return ErrInvalidData
			}
			d.tracks = append(d.tracks, track)
		default:
			if err := appendUnknownChildElement(master.Reader(), child, isKnownTracksElement, &d.unknownTracks); err != nil {
				return err
			}
		}
	}
	return master.Validate()
}

