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
	if d.cuesSeen {
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
	if d.cuesSeen {
		return ErrInvalidData
	}
	d.cuesSeen = true
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
				if len(d.cues) != 0 && cue.TimeNS < d.cues[len(d.cues)-1].TimeNS {
					d.cuesSorted = false
				}
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
	usedUIDs := make(map[uint64]struct{}, len(d.attachments))
	for i := range d.attachments {
		if d.attachments[i].UID == 0 {
			return ErrInvalidData
		}
		if _, ok := usedUIDs[d.attachments[i].UID]; ok {
			return ErrInvalidData
		}
		usedUIDs[d.attachments[i].UID] = struct{}{}
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
			if _, ok := usedUIDs[attachment.UID]; ok {
				return ErrInvalidData
			}
			usedUIDs[attachment.UID] = struct{}{}
			d.attachments = append(d.attachments, attachment)
		default:
			if err := appendUnknownChildElement(master.Reader(), child, isKnownAttachmentsElement, &d.unknownAttachments); err != nil {
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
	var descriptionSeen bool
	var filenameSeen bool
	var mediaTypeSeen bool
	var dataSeen bool
	var uidSeen bool
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return Attachment{}, err
		}
		switch child.ID {
		case idFileDescription:
			if err := markElementSeen(&descriptionSeen); err != nil {
				return Attachment{}, err
			}
			attachment.Description, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Attachment{}, err
			}
		case idFileName:
			if err := markElementSeen(&filenameSeen); err != nil {
				return Attachment{}, err
			}
			attachment.Filename, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Attachment{}, err
			}
		case idFileMediaType:
			if err := markElementSeen(&mediaTypeSeen); err != nil {
				return Attachment{}, err
			}
			attachment.MIMEType, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Attachment{}, err
			}
		case idFileData:
			if err := markElementSeen(&dataSeen); err != nil {
				return Attachment{}, err
			}
			attachment.Data, err = readBinaryPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Attachment{}, err
			}
		case idFileUID:
			if err := markElementSeen(&uidSeen); err != nil {
				return Attachment{}, err
			}
			attachment.UID, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Attachment{}, err
			}
		default:
			if err := appendUnknownChildElement(master.Reader(), child, isKnownAttachedFileElement, &attachment.UnknownElements); err != nil {
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
	usedEditions := make(map[uint64]struct{}, len(d.chapters))
	usedChapters := make(map[uint64]struct{})
	for i := range d.chapters {
		if d.chapters[i].UID != 0 {
			if _, ok := usedEditions[d.chapters[i].UID]; ok {
				return ErrInvalidData
			}
			usedEditions[d.chapters[i].UID] = struct{}{}
		}
		if err := collectChapterUIDs(d.chapters[i].Chapters, usedChapters); err != nil {
			return err
		}
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
			if edition.UID != 0 {
				if _, ok := usedEditions[edition.UID]; ok {
					return ErrInvalidData
				}
				usedEditions[edition.UID] = struct{}{}
			}
			if err := collectChapterUIDs(edition.Chapters, usedChapters); err != nil {
				return err
			}
			d.chapters = append(d.chapters, edition)
		default:
			if err := appendUnknownChildElement(master.Reader(), child, isKnownChaptersElement, &d.unknownChapters); err != nil {
				return err
			}
		}
	}
	return master.Validate()
}

func collectChapterUIDs(chapters []Chapter, used map[uint64]struct{}) error {
	for i := range chapters {
		if chapters[i].UID == 0 {
			return ErrInvalidData
		}
		if _, ok := used[chapters[i].UID]; ok {
			return ErrInvalidData
		}
		used[chapters[i].UID] = struct{}{}
		if err := collectChapterUIDs(chapters[i].Children, used); err != nil {
			return err
		}
	}
	return nil
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
	var uidSeen bool
	var hiddenSeen bool
	var defaultSeen bool
	var orderedSeen bool
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return ChapterEdition{}, err
		}
		switch child.ID {
		case idEditionUID:
			if err := markElementSeen(&uidSeen); err != nil {
				return ChapterEdition{}, err
			}
			edition.UID, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ChapterEdition{}, err
			}
		case idEditionFlagHidden:
			if err := markElementSeen(&hiddenSeen); err != nil {
				return ChapterEdition{}, err
			}
			edition.Hidden, err = readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ChapterEdition{}, err
			}
		case idEditionFlagDefault:
			if err := markElementSeen(&defaultSeen); err != nil {
				return ChapterEdition{}, err
			}
			edition.Default, err = readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ChapterEdition{}, err
			}
		case idEditionFlagOrdered:
			if err := markElementSeen(&orderedSeen); err != nil {
				return ChapterEdition{}, err
			}
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
			if err := appendUnknownChildElement(master.Reader(), child, isKnownEditionEntryElement, &edition.UnknownElements); err != nil {
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
	if err := validateUnknownElementsFor(edition.UnknownElements, isKnownEditionEntryElement); err != nil {
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
	var uidSeen bool
	var stringUIDSeen bool
	var endSeen bool
	var hiddenSeen bool
	var enabledSeen bool
	var trackSeen bool
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return Chapter{}, err
		}
		switch child.ID {
		case idChapterUID:
			if err := markElementSeen(&uidSeen); err != nil {
				return Chapter{}, err
			}
			chapter.UID, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Chapter{}, err
			}
		case idChapterStringUID:
			if err := markElementSeen(&stringUIDSeen); err != nil {
				return Chapter{}, err
			}
			chapter.StringUID, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Chapter{}, err
			}
		case idChapterTimeStart:
			if err := markElementSeen(&startSeen); err != nil {
				return Chapter{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Chapter{}, err
			}
			if value > uint64(math.MaxInt64) {
				return Chapter{}, ErrInvalidData
			}
			chapter.StartNS = int64(value)
		case idChapterTimeEnd:
			if err := markElementSeen(&endSeen); err != nil {
				return Chapter{}, err
			}
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
			if err := markElementSeen(&hiddenSeen); err != nil {
				return Chapter{}, err
			}
			chapter.Hidden, err = readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Chapter{}, err
			}
		case idChapterFlagEnabled:
			if err := markElementSeen(&enabledSeen); err != nil {
				return Chapter{}, err
			}
			chapter.Enabled, err = readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Chapter{}, err
			}
			chapter.EnabledSet = true
		case idChapterTrack:
			if err := markElementSeen(&trackSeen); err != nil {
				return Chapter{}, err
			}
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
			if err := appendUnknownChildElement(master.Reader(), child, isKnownChapterAtomElement, &chapter.UnknownElements); err != nil {
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
	var stringSeen bool
	var languageBCP47Seen bool
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return ChapterDisplay{}, err
		}
		switch child.ID {
		case idChapString:
			if err := markElementSeen(&stringSeen); err != nil {
				return ChapterDisplay{}, err
			}
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
			if err := markElementSeen(&languageBCP47Seen); err != nil {
				return ChapterDisplay{}, err
			}
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
			if err := appendUnknownChildElement(master.Reader(), child, isKnownChapterDisplayElement, &display.UnknownElements); err != nil {
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
	if err := validateUnknownElementsFor(display.UnknownElements, isKnownChapterDisplayElement); err != nil {
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
			if err := appendUnknownChildElement(master.Reader(), child, isKnownTagsElement, &d.unknownTags); err != nil {
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
			if err := markElementSeen(&targetSeen); err != nil {
				return Tag{}, err
			}
			tag.Target, err = d.parseTagTargets(master.Reader(), child)
			if err != nil {
				return Tag{}, err
			}
		case idSimpleTag:
			simple, err := d.parseSimpleTag(master.Reader(), child)
			if err != nil {
				return Tag{}, err
			}
			tag.Simple = append(tag.Simple, simple)
		default:
			if err := appendUnknownChildElement(master.Reader(), child, isKnownTagElement, &tag.UnknownElements); err != nil {
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
	if len(tag.Simple) == 0 || validateTagTarget(tag.Target) != nil ||
		validateUnknownElementsFor(tag.UnknownElements, isKnownTagElement) != nil {
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
	var typeValueSeen bool
	var typeSeen bool
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return TagTarget{}, err
		}
		switch child.ID {
		case idTargetTypeValue:
			if err := markElementSeen(&typeValueSeen); err != nil {
				return TagTarget{}, err
			}
			target.TypeValue, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return TagTarget{}, err
			}
		case idTargetType:
			if err := markElementSeen(&typeSeen); err != nil {
				return TagTarget{}, err
			}
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
			if err := appendUnknownChildElement(master.Reader(), child, isKnownTagTargetsElement, &target.UnknownElements); err != nil {
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
	var nameSeen bool
	var languageSeen bool
	var languageBCP47Seen bool
	var defaultSeen bool
	var stringSeen bool
	var binarySeen bool
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return SimpleTag{}, err
		}
		switch child.ID {
		case idTagName:
			if err := markElementSeen(&nameSeen); err != nil {
				return SimpleTag{}, err
			}
			tag.Name, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return SimpleTag{}, err
			}
		case idTagLanguage:
			if err := markElementSeen(&languageSeen); err != nil {
				return SimpleTag{}, err
			}
			tag.Language, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return SimpleTag{}, err
			}
		case idTagLanguageBCP47:
			if err := markElementSeen(&languageBCP47Seen); err != nil {
				return SimpleTag{}, err
			}
			tag.LanguageBCP47, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return SimpleTag{}, err
			}
		case idTagDefault:
			if err := markElementSeen(&defaultSeen); err != nil {
				return SimpleTag{}, err
			}
			tag.Default, err = readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return SimpleTag{}, err
			}
			tag.DefaultSet = true
		case idTagString:
			if err := markElementSeen(&stringSeen); err != nil {
				return SimpleTag{}, err
			}
			tag.String, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return SimpleTag{}, err
			}
			tag.StringSet = true
		case idTagBinary:
			if err := markElementSeen(&binarySeen); err != nil {
				return SimpleTag{}, err
			}
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
			if err := appendUnknownChildElement(master.Reader(), child, isKnownSimpleTagElement, &tag.UnknownElements); err != nil {
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
			if err := markElementSeen(&timeSeen); err != nil {
				return CuePoint{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return CuePoint{}, err
			}
			timeNS, err := scaleCueTicks(value, d.timecodeScaleNS)
			if err != nil {
				return CuePoint{}, ErrInvalidData
			}
			cue.TimeNS = timeNS
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
	relativeSeen := false
	durationSeen := false
	blockNumberSeen := false
	codecStateSeen := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return CueTrackPosition{}, err
		}
		switch child.ID {
		case idCueTrack:
			if err := markElementSeen(&trackSeen); err != nil {
				return CueTrackPosition{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return CueTrackPosition{}, err
			}
			position.TrackID, err = trackIDFromUint(value)
			if err != nil {
				return CueTrackPosition{}, err
			}
		case idCueClusterPosition:
			if err := markElementSeen(&clusterSeen); err != nil {
				return CueTrackPosition{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return CueTrackPosition{}, err
			}
			position.ClusterPosition = value
		case idCueRelativePos:
			if err := markElementSeen(&relativeSeen); err != nil {
				return CueTrackPosition{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return CueTrackPosition{}, err
			}
			position.RelativePosition = value
			position.RelativePositionSet = true
		case idCueDuration:
			if err := markElementSeen(&durationSeen); err != nil {
				return CueTrackPosition{}, err
			}
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
			if err := markElementSeen(&blockNumberSeen); err != nil {
				return CueTrackPosition{}, err
			}
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
			if err := markElementSeen(&codecStateSeen); err != nil {
				return CueTrackPosition{}, err
			}
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
	numberSeen := false
	codecStateSeen := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return CueReference{}, err
		}
		switch child.ID {
		case idCueRefTime:
			if err := markElementSeen(&timeSeen); err != nil {
				return CueReference{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return CueReference{}, err
			}
			reference.TimeNS, err = scaleCueTicks(value, d.timecodeScaleNS)
			if err != nil {
				return CueReference{}, err
			}
		case idCueRefCluster:
			if err := markElementSeen(&clusterSeen); err != nil {
				return CueReference{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return CueReference{}, err
			}
			reference.ClusterPosition = value
		case idCueRefNumber:
			if err := markElementSeen(&numberSeen); err != nil {
				return CueReference{}, err
			}
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
			if err := markElementSeen(&codecStateSeen); err != nil {
				return CueReference{}, err
			}
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
