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
	"sort"

	"github.com/thesyncim/goav/container/ebml"
	"github.com/woozymasta/lzo"
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
	inSegment             bool
	inCluster             bool
	clusterUnknown        bool
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
	d.seeker, _ = r.(io.ReadSeeker)
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
	d.inSegment = false
	d.inCluster = false
	d.clusterUnknown = false
	d.clusterEnd = 0
	d.clusterTimecode = 0
	d.pendingHeader = ebml.Header{}
	d.pendingHeaderSet = false
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
	if err := d.ensureCues(); err != nil {
		if d.canUseClusterIndexAfterCueError(err) {
			if err := d.seekToPacketAtOrBeforeTime(timeNS); err == nil {
				return nil
			}
			return d.seekToClusterAtTime(timeNS)
		}
		return err
	}
	cue := d.cueForTime(timeNS)
	if cue.TimeNS > timeNS {
		if err := d.seekToPacketAtOrBeforeTime(timeNS); err == nil {
			return nil
		}
		if err := d.seekToClusterAtTime(timeNS); err == nil {
			return nil
		}
	}
	position, err := firstCuePosition(cue)
	if err != nil {
		return err
	}
	return d.seekToCuePosition(position)
}

// SeekToTrackTime seeks to the nearest preceding cue for trackID and timeNS.
// If the requested time is before that track's first cue, it seeks to the
// first cue for the track.
func (d *Demuxer) SeekToTrackTime(trackID uint32, timeNS int64) error {
	if d == nil || d.reader == nil {
		return ErrNilReader
	}
	if d.seeker == nil {
		return ErrNonSeekableReader
	}
	if timeNS < 0 {
		return ErrInvalidData
	}
	if trackID == 0 || !d.hasTrack(trackID) {
		return ErrUnknownTrack
	}
	if err := d.ensureCues(); err != nil {
		if d.canUseClusterIndexAfterCueError(err) {
			if err := d.seekToTrackPacketAtOrBeforeTime(trackID, timeNS); err == nil {
				return nil
			}
			return d.seekToClusterAtTime(timeNS)
		}
		return err
	}
	cue, position, ok := d.cueForTrackTime(trackID, timeNS)
	if !ok {
		if err := d.seekToTrackPacketAtOrBeforeTime(trackID, timeNS); err == nil {
			return nil
		}
		return d.seekToClusterAtTime(timeNS)
	}
	if cue.TimeNS > timeNS {
		if err := d.seekToTrackPacketAtOrBeforeTime(trackID, timeNS); err == nil {
			return nil
		}
		if err := d.seekToClusterAtTime(timeNS); err == nil {
			return nil
		}
	}
	return d.seekToCuePosition(position)
}

func (d *Demuxer) ensureCues() error {
	if len(d.cues) == 0 {
		if err := d.loadCuesFromSeekHead(); err != nil {
			return err
		}
	}
	if len(d.cues) == 0 {
		return ErrInvalidData
	}
	return nil
}

func (d *Demuxer) canUseClusterIndexAfterCueError(err error) bool {
	return errors.Is(err, ErrInvalidData) && len(d.cues) == 0 && !d.cuesSeen && !d.hasCuesSeekEntry()
}

func (d *Demuxer) hasCuesSeekEntry() bool {
	for i := range d.seekEntries {
		if d.seekEntries[i].ID == uint64(idCues) {
			return true
		}
	}
	return false
}

func (d *Demuxer) seekToCuePosition(position CueTrackPosition) error {
	header, err := d.seekToClusterPosition(position.ClusterPosition)
	if err != nil {
		return err
	}
	if position.RelativePositionSet {
		if err := d.seekToCueRelativePosition(header, position.RelativePosition); err != nil {
			return err
		}
	} else if position.BlockNumberSet {
		if err := d.seekToCueBlockNumber(header, position.BlockNumber); err != nil {
			return err
		}
	}
	d.clearLace()
	return nil
}

func (d *Demuxer) seekToClusterAtTime(timeNS int64) error {
	entry, err := d.clusterForTime(timeNS)
	if err != nil {
		return err
	}
	_, err = d.seekToClusterPosition(entry.Position)
	return err
}

func (d *Demuxer) seekToClusterPosition(position uint64) (ebml.Header, error) {
	if position > uint64(math.MaxInt64) ||
		int64(position) > math.MaxInt64-d.segmentData {
		return ebml.Header{}, ErrInvalidData
	}
	d.pendingHeader = ebml.Header{}
	d.pendingHeaderSet = false
	d.pendingClusterUnknown = d.pendingClusterUnknown[:0]
	offset := d.segmentData + int64(position)
	if _, err := d.seeker.Seek(offset, io.SeekStart); err != nil {
		return ebml.Header{}, err
	}
	d.reader.Reset(d.seeker, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize})
	header, err := d.reader.ReadHeader()
	if err != nil {
		return ebml.Header{}, err
	}
	if header.ID != idCluster {
		return ebml.Header{}, ErrInvalidData
	}
	if err := d.enterCluster(header); err != nil {
		return ebml.Header{}, err
	}
	d.clearLace()
	return header, nil
}

func firstCuePosition(cue CuePoint) (CueTrackPosition, error) {
	if len(cue.Positions) != 0 {
		return cue.Positions[0], nil
	}
	if cue.TrackID == 0 {
		return CueTrackPosition{}, ErrInvalidData
	}
	return cueTrackPositionFromLegacy(cue), nil
}

func (d *Demuxer) cueForTime(timeNS int64) CuePoint {
	if d.cuesSorted {
		index := sort.Search(len(d.cues), func(i int) bool {
			return d.cues[i].TimeNS > timeNS
		})
		if index == 0 {
			return d.cues[0]
		}
		return d.cues[index-1]
	}
	cue := d.cues[0]
	for i := range d.cues {
		if d.cues[i].TimeNS > timeNS {
			break
		}
		cue = d.cues[i]
	}
	return cue
}

func (d *Demuxer) cueForTrackTime(trackID uint32, timeNS int64) (CuePoint, CueTrackPosition, bool) {
	if d.cuesSorted {
		index := sort.Search(len(d.cues), func(i int) bool {
			return d.cues[i].TimeNS > timeNS
		})
		for i := index - 1; i >= 0; i-- {
			if position, ok := cuePositionForTrack(d.cues[i], trackID); ok {
				return d.cues[i], position, true
			}
		}
		for i := index; i < len(d.cues); i++ {
			if position, ok := cuePositionForTrack(d.cues[i], trackID); ok {
				return d.cues[i], position, true
			}
		}
		return CuePoint{}, CueTrackPosition{}, false
	}
	var cue CuePoint
	var position CueTrackPosition
	found := false
	for i := range d.cues {
		if d.cues[i].TimeNS > timeNS {
			break
		}
		if candidate, ok := cuePositionForTrack(d.cues[i], trackID); ok {
			cue = d.cues[i]
			position = candidate
			found = true
		}
	}
	if found {
		return cue, position, true
	}
	for i := range d.cues {
		if candidate, ok := cuePositionForTrack(d.cues[i], trackID); ok {
			return d.cues[i], candidate, true
		}
	}
	return CuePoint{}, CueTrackPosition{}, false
}

func cuePositionForTrack(cue CuePoint, trackID uint32) (CueTrackPosition, bool) {
	if len(cue.Positions) == 0 {
		if cue.TrackID != trackID {
			return CueTrackPosition{}, false
		}
		return cueTrackPositionFromLegacy(cue), true
	}
	for i := range cue.Positions {
		if cue.Positions[i].TrackID == trackID {
			return cue.Positions[i], true
		}
	}
	return CueTrackPosition{}, false
}

// ReadCuedPacketAtTime seeks to the first cue at or after timeNS and reads the
// cued packet. Exact block cues jump to the block; cluster-only cues scan
// within the referenced Cluster until the cue's track/time is reached. It does
// not scan uncued packets between cues; use ReadPacketAtTime when uncued packets
// should be considered too.
func (d *Demuxer) ReadCuedPacketAtTime(timeNS int64, dst *Packet) error {
	if d == nil || d.reader == nil {
		return ErrNilReader
	}
	if dst == nil {
		return ErrNilPacket
	}
	if d.seeker == nil {
		return ErrNonSeekableReader
	}
	if timeNS < 0 {
		return ErrInvalidData
	}
	if err := d.ensureCues(); err != nil {
		return err
	}
	cue, position, ok := d.cueAtOrAfterTime(timeNS)
	if !ok {
		return ErrInvalidData
	}
	return d.readCuedPacket(cue, position, 0, timeNS, dst)
}

// ReadPacketAtTime reads the first packet at or after timeNS. Seekable readers
// use a direct packet index when Cues are absent or too sparse to identify
// uncued packets without scanning.
func (d *Demuxer) ReadPacketAtTime(timeNS int64, dst *Packet) error {
	if d == nil || d.reader == nil {
		return ErrNilReader
	}
	if dst == nil {
		return ErrNilPacket
	}
	if d.seeker == nil {
		return ErrNonSeekableReader
	}
	if timeNS < 0 {
		return ErrInvalidData
	}
	if err := d.ensureCues(); err != nil {
		if d.canUseClusterIndexAfterCueError(err) {
			if err := d.readIndexedPacketAtOrAfterTime(timeNS, dst); err == nil {
				return nil
			}
		} else {
			return err
		}
	} else {
		if err := d.readIndexedPacketAtOrAfterTime(timeNS, dst); err == nil {
			return nil
		}
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

// ReadCuedTrackPacketAtTime seeks to the first cue for trackID at or after
// timeNS and reads that cued packet. Exact block cues jump to the block;
// cluster-only cues scan within the referenced Cluster until the cue's
// track/time is reached. It does not scan uncued packets between cues; use
// ReadTrackPacketAtTime when uncued packets should be considered too.
func (d *Demuxer) ReadCuedTrackPacketAtTime(trackID uint32, timeNS int64, dst *Packet) error {
	if d == nil || d.reader == nil {
		return ErrNilReader
	}
	if dst == nil {
		return ErrNilPacket
	}
	if d.seeker == nil {
		return ErrNonSeekableReader
	}
	if timeNS < 0 {
		return ErrInvalidData
	}
	if trackID == 0 || !d.hasTrack(trackID) {
		return ErrUnknownTrack
	}
	if err := d.ensureCues(); err != nil {
		return err
	}
	cue, position, ok := d.cueForTrackAtOrAfterTime(trackID, timeNS)
	if !ok {
		return ErrInvalidData
	}
	return d.readCuedPacket(cue, position, trackID, timeNS, dst)
}

// ReadTrackPacketAtTime reads the first packet for trackID at or after timeNS.
// Seekable readers use a direct packet index when Cues are absent or too sparse
// for that track.
func (d *Demuxer) ReadTrackPacketAtTime(trackID uint32, timeNS int64, dst *Packet) error {
	if d == nil || d.reader == nil {
		return ErrNilReader
	}
	if dst == nil {
		return ErrNilPacket
	}
	if d.seeker == nil {
		return ErrNonSeekableReader
	}
	if timeNS < 0 {
		return ErrInvalidData
	}
	if trackID == 0 || !d.hasTrack(trackID) {
		return ErrUnknownTrack
	}
	if err := d.ensureCues(); err != nil {
		if d.canUseClusterIndexAfterCueError(err) {
			if err := d.readIndexedTrackPacketAtOrAfterTime(trackID, timeNS, dst); err == nil {
				return nil
			}
		} else {
			return err
		}
	} else {
		if err := d.readIndexedTrackPacketAtOrAfterTime(trackID, timeNS, dst); err == nil {
			return nil
		}
	}
	if err := d.SeekToTrackTime(trackID, timeNS); err != nil {
		return err
	}
	for {
		if err := d.ReadPacket(dst); err != nil {
			return err
		}
		if dst.TrackID == trackID && dst.TimeNS >= timeNS {
			return nil
		}
	}
}

func (d *Demuxer) readCuedPacket(cue CuePoint, position CueTrackPosition, trackID uint32, timeNS int64, dst *Packet) error {
	if cue.TimeNS < timeNS {
		return ErrInvalidData
	}
	targetTrackID := position.TrackID
	if trackID != 0 {
		targetTrackID = trackID
	}
	if targetTrackID == 0 {
		return ErrInvalidData
	}
	if !cuePositionIsDirect(position) {
		return d.scanCuedPacket(cue, position, targetTrackID, timeNS, dst)
	}
	if err := d.seekToCuePosition(position); err != nil {
		return err
	}
	if err := d.ReadPacket(dst); err != nil {
		return err
	}
	if dst.TimeNS < timeNS || dst.TimeNS < cue.TimeNS {
		return ErrInvalidData
	}
	if dst.TrackID != targetTrackID {
		return ErrInvalidData
	}
	return nil
}

func (d *Demuxer) scanCuedPacket(cue CuePoint, position CueTrackPosition, trackID uint32, timeNS int64, dst *Packet) error {
	if err := d.seekToCuePosition(position); err != nil {
		return err
	}
	for {
		if err := d.ReadPacket(dst); err != nil {
			if isEOF(err) {
				return ErrInvalidData
			}
			return err
		}
		if dst.TrackID == trackID && dst.TimeNS >= cue.TimeNS {
			if dst.TimeNS < timeNS {
				return ErrInvalidData
			}
			return nil
		}
		if !d.clusterUnknown && d.reader.Offset() >= d.clusterEnd && d.laceFrameIndex >= d.laceFrameCount {
			return ErrInvalidData
		}
	}
}

func (d *Demuxer) cueAtOrAfterTime(timeNS int64) (CuePoint, CueTrackPosition, bool) {
	if d.cuesSorted {
		index := sort.Search(len(d.cues), func(i int) bool {
			return d.cues[i].TimeNS >= timeNS
		})
		for i := index; i < len(d.cues); i++ {
			if position, ok := firstCueTrackPosition(d.cues[i]); ok {
				return d.cues[i], position, true
			}
		}
		return CuePoint{}, CueTrackPosition{}, false
	}
	var best CuePoint
	var bestPosition CueTrackPosition
	found := false
	for i := range d.cues {
		if d.cues[i].TimeNS < timeNS {
			continue
		}
		position, ok := firstCueTrackPosition(d.cues[i])
		if !ok {
			continue
		}
		if !found || d.cues[i].TimeNS < best.TimeNS {
			best = d.cues[i]
			bestPosition = position
			found = true
		}
	}
	return best, bestPosition, found
}

func (d *Demuxer) cueForTrackAtOrAfterTime(trackID uint32, timeNS int64) (CuePoint, CueTrackPosition, bool) {
	if d.cuesSorted {
		index := sort.Search(len(d.cues), func(i int) bool {
			return d.cues[i].TimeNS >= timeNS
		})
		for i := index; i < len(d.cues); i++ {
			if position, ok := cuePositionForTrack(d.cues[i], trackID); ok {
				return d.cues[i], position, true
			}
		}
		return CuePoint{}, CueTrackPosition{}, false
	}
	var best CuePoint
	var bestPosition CueTrackPosition
	found := false
	for i := range d.cues {
		if d.cues[i].TimeNS < timeNS {
			continue
		}
		position, ok := cuePositionForTrack(d.cues[i], trackID)
		if !ok {
			continue
		}
		if !found || d.cues[i].TimeNS < best.TimeNS {
			best = d.cues[i]
			bestPosition = position
			found = true
		}
	}
	return best, bestPosition, found
}

func firstCueTrackPosition(cue CuePoint) (CueTrackPosition, bool) {
	if len(cue.Positions) == 0 {
		return cueTrackPositionFromLegacy(cue), true
	}
	return cue.Positions[0], true
}

func cuePositionIsDirect(position CueTrackPosition) bool {
	return position.RelativePositionSet || position.BlockNumberSet
}

func (d *Demuxer) readIndexedPacketAtOrAfterTime(timeNS int64, dst *Packet) error {
	entry, err := d.packetAtOrAfterTime(timeNS)
	if err != nil {
		return err
	}
	if err := d.seekToPacketIndexEntry(entry); err != nil {
		return err
	}
	if err := d.ReadPacket(dst); err != nil {
		return err
	}
	if dst.TimeNS < timeNS {
		return ErrInvalidData
	}
	return nil
}

func (d *Demuxer) readIndexedTrackPacketAtOrAfterTime(trackID uint32, timeNS int64, dst *Packet) error {
	entry, err := d.trackPacketAtOrAfterTime(trackID, timeNS)
	if err != nil {
		return err
	}
	if err := d.seekToPacketIndexEntry(entry); err != nil {
		return err
	}
	if err := d.ReadPacket(dst); err != nil {
		return err
	}
	if dst.TrackID != trackID || dst.TimeNS < timeNS {
		return ErrInvalidData
	}
	return nil
}

func (d *Demuxer) seekToPacketAtOrBeforeTime(timeNS int64) error {
	entry, err := d.packetAtOrBeforeTime(timeNS)
	if err != nil {
		return err
	}
	return d.seekToPacketIndexEntry(entry)
}

func (d *Demuxer) seekToTrackPacketAtOrBeforeTime(trackID uint32, timeNS int64) error {
	entry, err := d.trackPacketAtOrBeforeTime(trackID, timeNS)
	if err != nil {
		return err
	}
	return d.seekToPacketIndexEntry(entry)
}

func (d *Demuxer) packetAtOrBeforeTime(timeNS int64) (packetIndexEntry, error) {
	if err := d.ensurePacketIndex(); err != nil {
		return packetIndexEntry{}, err
	}
	index := sort.Search(len(d.packetIndex), func(i int) bool {
		return d.packetIndex[i].TimeNS > timeNS
	})
	if index == 0 {
		return d.packetIndex[0], nil
	}
	return d.packetIndex[index-1], nil
}

func (d *Demuxer) packetAtOrAfterTime(timeNS int64) (packetIndexEntry, error) {
	if err := d.ensurePacketIndex(); err != nil {
		return packetIndexEntry{}, err
	}
	index := sort.Search(len(d.packetIndex), func(i int) bool {
		return d.packetIndex[i].TimeNS >= timeNS
	})
	if index == len(d.packetIndex) {
		return packetIndexEntry{}, ErrInvalidData
	}
	return d.packetIndex[index], nil
}

func (d *Demuxer) trackPacketAtOrBeforeTime(trackID uint32, timeNS int64) (packetIndexEntry, error) {
	if err := d.ensurePacketIndex(); err != nil {
		return packetIndexEntry{}, err
	}
	entries, ok := d.packetIndexEntriesForTrack(trackID)
	if !ok || len(entries) == 0 {
		return packetIndexEntry{}, ErrInvalidData
	}
	index := sort.Search(len(entries), func(i int) bool {
		return d.packetIndex[entries[i]].TimeNS > timeNS
	})
	if index == 0 {
		return d.packetIndex[entries[0]], nil
	}
	return d.packetIndex[entries[index-1]], nil
}

func (d *Demuxer) trackPacketAtOrAfterTime(trackID uint32, timeNS int64) (packetIndexEntry, error) {
	if err := d.ensurePacketIndex(); err != nil {
		return packetIndexEntry{}, err
	}
	entries, ok := d.packetIndexEntriesForTrack(trackID)
	if !ok || len(entries) == 0 {
		return packetIndexEntry{}, ErrInvalidData
	}
	index := sort.Search(len(entries), func(i int) bool {
		return d.packetIndex[entries[i]].TimeNS >= timeNS
	})
	if index == len(entries) {
		return packetIndexEntry{}, ErrInvalidData
	}
	return d.packetIndex[entries[index]], nil
}

func (d *Demuxer) packetIndexEntriesForTrack(trackID uint32) ([]int, bool) {
	for i := range d.packetTrackIndex {
		if d.packetTrackIndex[i].TrackID == trackID {
			return d.packetTrackIndex[i].Entries, true
		}
	}
	return nil, false
}

func (d *Demuxer) seekToPacketIndexEntry(entry packetIndexEntry) error {
	if entry.BlockPosition < entry.ClusterPosition {
		return ErrInvalidData
	}
	cluster, err := d.seekToClusterPosition(entry.ClusterPosition)
	if err != nil {
		return err
	}
	blockOffset := int64(entry.BlockPosition - entry.ClusterPosition)
	if blockOffset < cluster.DataOffset {
		return ErrInvalidData
	}
	if !cluster.Size.Unknown && blockOffset >= cluster.DataOffset+int64(cluster.Size.Value) {
		return ErrInvalidData
	}
	if entry.BlockPosition > uint64(math.MaxInt64) ||
		int64(entry.BlockPosition) > math.MaxInt64-d.segmentData {
		return ErrInvalidData
	}
	if _, err := d.seeker.Seek(d.segmentData+int64(entry.BlockPosition), io.SeekStart); err != nil {
		return err
	}
	d.reader.ResetAt(d.seeker, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize}, blockOffset)
	header, err := d.reader.ReadHeader()
	if err != nil {
		return err
	}
	if header.ID != idSimpleBlock && header.ID != idBlockGroup {
		return ErrInvalidData
	}
	d.clusterTimecode = entry.ClusterTimecode
	d.pendingHeader = header
	d.pendingHeaderSet = true
	d.laceSeekFrameIndex = entry.FrameIndex
	return nil
}

func (d *Demuxer) ensurePacketIndex() error {
	if d.packetIndexBuilt {
		if len(d.packetIndex) == 0 {
			return ErrInvalidData
		}
		return nil
	}
	if d.seeker == nil {
		return ErrNonSeekableReader
	}
	d.packetIndex = d.packetIndex[:0]
	buildClusters := !d.clusterIndexBuilt
	if buildClusters {
		d.clusterIndex = d.clusterIndex[:0]
	}
	if _, err := d.seeker.Seek(d.segmentData, io.SeekStart); err != nil {
		return err
	}
	indexReader := ebml.NewReader(d.seeker, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize})
	indexReader.ResetAt(d.seeker, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize}, 0)
	for {
		if d.segmentIndexDone(indexReader.Offset()) {
			break
		}
		header, err := indexReader.ReadHeader()
		if err != nil {
			if d.segmentUnknown && isEOF(err) {
				break
			}
			return err
		}
		if err := d.validateSegmentIndexHeader(header); err != nil {
			return err
		}
		switch header.ID {
		case idCluster:
			entry, err := d.readClusterPacketIndex(indexReader, header)
			if err != nil {
				return err
			}
			if buildClusters {
				d.clusterIndex = append(d.clusterIndex, entry)
			}
		default:
			if err := d.skipSegmentIndexElement(indexReader, header); err != nil {
				return err
			}
		}
	}
	d.packetIndexBuilt = true
	if len(d.packetIndex) == 0 {
		return ErrInvalidData
	}
	sort.SliceStable(d.packetIndex, func(i, j int) bool {
		if d.packetIndex[i].TimeNS == d.packetIndex[j].TimeNS {
			if d.packetIndex[i].BlockPosition == d.packetIndex[j].BlockPosition {
				return d.packetIndex[i].FrameIndex < d.packetIndex[j].FrameIndex
			}
			return d.packetIndex[i].BlockPosition < d.packetIndex[j].BlockPosition
		}
		return d.packetIndex[i].TimeNS < d.packetIndex[j].TimeNS
	})
	if err := d.buildPacketTrackIndex(); err != nil {
		return err
	}
	if buildClusters {
		d.clusterIndexBuilt = true
		if len(d.clusterIndex) != 0 {
			sort.SliceStable(d.clusterIndex, func(i, j int) bool {
				if d.clusterIndex[i].TimeNS == d.clusterIndex[j].TimeNS {
					return d.clusterIndex[i].Position < d.clusterIndex[j].Position
				}
				return d.clusterIndex[i].TimeNS < d.clusterIndex[j].TimeNS
			})
		}
	}
	return nil
}

func (d *Demuxer) buildPacketTrackIndex() error {
	if cap(d.packetTrackIndex) < len(d.tracks) {
		d.packetTrackIndex = make([]packetTrackIndex, len(d.tracks))
	} else {
		d.packetTrackIndex = d.packetTrackIndex[:len(d.tracks)]
	}
	for i := range d.packetTrackIndex {
		d.packetTrackIndex[i].TrackID = d.tracks[i].ID
		d.packetTrackIndex[i].Entries = d.packetTrackIndex[i].Entries[:0]
	}
	for i := range d.packetIndex {
		entry := d.packetIndex[i]
		if entry.TrackIndex < 0 || entry.TrackIndex >= len(d.packetTrackIndex) ||
			d.packetTrackIndex[entry.TrackIndex].TrackID != entry.TrackID {
			return ErrInvalidData
		}
		d.packetTrackIndex[entry.TrackIndex].Entries = append(d.packetTrackIndex[entry.TrackIndex].Entries, i)
	}
	kept := 0
	for i := range d.packetTrackIndex {
		if len(d.packetTrackIndex[i].Entries) == 0 {
			continue
		}
		d.packetTrackIndex[kept] = d.packetTrackIndex[i]
		kept++
	}
	for i := kept; i < len(d.packetTrackIndex); i++ {
		d.packetTrackIndex[i] = packetTrackIndex{}
	}
	d.packetTrackIndex = d.packetTrackIndex[:kept]
	return nil
}

func (d *Demuxer) readClusterPacketIndex(reader *ebml.Reader, header ebml.Header) (clusterIndexEntry, error) {
	if header.Offset < 0 {
		return clusterIndexEntry{}, ErrInvalidData
	}
	clusterPosition := uint64(header.Offset)
	if header.Size.Unknown {
		return d.readUnknownClusterPacketIndex(reader, clusterPosition)
	}
	clusterEnd, err := d.segmentIndexElementEnd(header)
	if err != nil {
		return clusterIndexEntry{}, err
	}
	clusterTimecode := int64(0)
	clusterTimeNS := int64(0)
	for reader.Offset() < clusterEnd {
		child, err := reader.ReadHeader()
		if err != nil {
			return clusterIndexEntry{}, err
		}
		if child.ID == idCluster {
			return clusterIndexEntry{}, ErrInvalidData
		}
		if err := d.validateClusterIndexHeader(child, clusterEnd); err != nil {
			return clusterIndexEntry{}, err
		}
		switch child.ID {
		case idTimestamp:
			clusterTimeNS, err = d.readClusterIndexTimestamp(reader, child, clusterEnd)
			if err != nil {
				return clusterIndexEntry{}, err
			}
			clusterTimecode = clusterTimeNS / d.timecodeScaleNS
		case idSimpleBlock:
			info, err := d.readIndexedBlock(reader, child, clusterTimecode)
			if err != nil {
				return clusterIndexEntry{}, err
			}
			if err := d.appendPacketIndexEntries(clusterPosition, uint64(child.Offset), clusterTimecode, info); err != nil {
				return clusterIndexEntry{}, err
			}
		case idBlockGroup:
			if err := d.readIndexedBlockGroup(reader, child, clusterPosition, clusterTimecode); err != nil {
				return clusterIndexEntry{}, err
			}
		default:
			if err := d.skipClusterIndexElement(reader, child, clusterEnd); err != nil {
				return clusterIndexEntry{}, err
			}
		}
	}
	if err := d.resetSegmentIndexReader(reader, clusterEnd); err != nil {
		return clusterIndexEntry{}, err
	}
	return clusterIndexEntry{TimeNS: clusterTimeNS, Position: clusterPosition}, nil
}

func (d *Demuxer) readUnknownClusterPacketIndex(reader *ebml.Reader, clusterPosition uint64) (clusterIndexEntry, error) {
	clusterTimecode := int64(0)
	clusterTimeNS := int64(0)
	for {
		if d.segmentIndexDone(reader.Offset()) {
			return clusterIndexEntry{TimeNS: clusterTimeNS, Position: clusterPosition}, nil
		}
		child, err := reader.ReadHeader()
		if err != nil {
			if d.segmentUnknown && isEOF(err) {
				return clusterIndexEntry{TimeNS: clusterTimeNS, Position: clusterPosition}, nil
			}
			return clusterIndexEntry{}, err
		}
		if isUnknownClusterTerminator(child.ID) {
			if err := d.resetSegmentIndexReader(reader, child.Offset); err != nil {
				return clusterIndexEntry{}, err
			}
			return clusterIndexEntry{TimeNS: clusterTimeNS, Position: clusterPosition}, nil
		}
		if err := d.validateSegmentIndexHeader(child); err != nil {
			return clusterIndexEntry{}, err
		}
		switch child.ID {
		case idTimestamp:
			clusterTimeNS, err = d.readClusterIndexTimestamp(reader, child, 0)
			if err != nil {
				return clusterIndexEntry{}, err
			}
			clusterTimecode = clusterTimeNS / d.timecodeScaleNS
		case idSimpleBlock:
			info, err := d.readIndexedBlock(reader, child, clusterTimecode)
			if err != nil {
				return clusterIndexEntry{}, err
			}
			if err := d.appendPacketIndexEntries(clusterPosition, uint64(child.Offset), clusterTimecode, info); err != nil {
				return clusterIndexEntry{}, err
			}
		case idBlockGroup:
			if err := d.readIndexedBlockGroup(reader, child, clusterPosition, clusterTimecode); err != nil {
				return clusterIndexEntry{}, err
			}
		default:
			if err := d.skipSegmentIndexElement(reader, child); err != nil {
				return clusterIndexEntry{}, err
			}
		}
	}
}

func (d *Demuxer) readIndexedBlockGroup(reader *ebml.Reader, header ebml.Header, clusterPosition uint64, clusterTimecode int64) error {
	if header.Size.Unknown {
		return ErrInvalidData
	}
	groupEnd, err := d.segmentIndexElementEnd(header)
	if err != nil {
		return err
	}
	limit := io.LimitedReader{R: reader, N: int64(header.Size.Value)}
	groupReader := ebml.NewReader(&limit, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize})
	var info indexedBlockInfo
	haveBlock := false
	durationNS := int64(0)
	for limit.N > 0 {
		child, err := groupReader.ReadHeader()
		if err != nil {
			return err
		}
		switch child.ID {
		case idBlock:
			parsed, err := d.readIndexedBlock(groupReader, child, clusterTimecode)
			if err != nil {
				return err
			}
			info = parsed
			haveBlock = true
		case idBlockDuration:
			value, err := readUIntPayloadScratch(groupReader, child.Size.Value, &d.uintScratch)
			if err != nil {
				return err
			}
			durationNS, err = scaleCueTicks(value, d.timecodeScaleNS)
			if err != nil {
				return err
			}
		default:
			if err := skipElement(groupReader, child); err != nil {
				return err
			}
		}
	}
	if limit.N != 0 {
		return ErrInvalidData
	}
	if !haveBlock {
		return ErrInvalidData
	}
	if durationNS > 0 && info.FrameCount > 1 {
		info.FrameDurationNS = durationNS / int64(info.FrameCount)
	}
	if err := d.appendPacketIndexEntries(clusterPosition, uint64(header.Offset), clusterTimecode, info); err != nil {
		return err
	}
	return d.resetSegmentIndexReader(reader, groupEnd)
}

func (d *Demuxer) readIndexedBlock(reader *ebml.Reader, header ebml.Header, clusterTimecode int64) (indexedBlockInfo, error) {
	if header.Size.Unknown || header.Size.Value > uint64(math.MaxInt64) {
		return indexedBlockInfo{}, ErrInvalidData
	}
	limit := io.LimitedReader{R: reader, N: int64(header.Size.Value)}
	trackID, _, err := ebml.ReadUnsignedVINT(&limit, &d.scratch)
	if err != nil {
		return indexedBlockInfo{}, err
	}
	trackNumber, err := trackIDFromUint(trackID)
	if err != nil {
		return indexedBlockInfo{}, err
	}
	trackIndex, ok := d.trackIndex(trackNumber)
	if !ok {
		return indexedBlockInfo{}, ErrUnknownTrack
	}
	track := d.tracks[trackIndex]
	if _, err := io.ReadFull(&limit, d.blockHeader[:]); err != nil {
		return indexedBlockInfo{}, err
	}
	flags := d.blockHeader[2]
	lacing := flags & simpleBlockLacingMask
	if lacing != 0 && track.FlagLacingSet && !track.FlagLacing {
		return indexedBlockInfo{}, ErrInvalidData
	}
	blockTimecode := int16(binary.BigEndian.Uint16(d.blockHeader[:2]))
	timecode := clusterTimecode + int64(blockTimecode)
	if timecode < 0 || timecode > math.MaxInt64/d.timecodeScaleNS {
		return indexedBlockInfo{}, ErrInvalidData
	}
	info := indexedBlockInfo{
		TrackID:    trackNumber,
		TrackIndex: trackIndex,
		TimeNS:     timecode * d.timecodeScaleNS,
		FrameCount: 1,
	}
	if lacing != 0 {
		frameCount, err := d.readIndexedLace(&limit, lacing)
		if err != nil {
			return indexedBlockInfo{}, err
		}
		info.FrameCount = frameCount
		info.FrameDurationNS = d.defaultDurationNS(trackNumber)
	}
	if err := drainLimited(&limit); err != nil {
		return indexedBlockInfo{}, err
	}
	return info, nil
}

func (d *Demuxer) readIndexedLace(limit *io.LimitedReader, lacing byte) (int, error) {
	var frameCountByte [1]byte
	if _, err := io.ReadFull(limit, frameCountByte[:]); err != nil {
		return 0, err
	}
	frameCount := int(frameCountByte[0]) + 1
	if frameCount < 2 || frameCount > len(d.laceFrames) {
		return 0, ErrLaceTooLarge
	}
	switch lacing {
	case simpleBlockLacingXiph:
		total := 0
		for i := 0; i < frameCount-1; i++ {
			size := 0
			for {
				var value [1]byte
				if _, err := io.ReadFull(limit, value[:]); err != nil {
					return 0, err
				}
				size += int(value[0])
				if size > len(d.laceBuffer) {
					return 0, ErrLaceTooLarge
				}
				if value[0] != 255 {
					break
				}
			}
			total += size
		}
		if limit.N < int64(total) {
			return 0, ErrInvalidData
		}
	case simpleBlockLacingFixed:
		if limit.N < 0 || limit.N%int64(frameCount) != 0 {
			return 0, ErrInvalidData
		}
	case simpleBlockLacingEBML:
		first, _, err := ebml.ReadUnsignedVINT(limit, &d.scratch)
		if err != nil {
			return 0, err
		}
		if first > uint64(len(d.laceBuffer)) {
			return 0, ErrLaceTooLarge
		}
		total := int(first)
		previous := int64(first)
		for i := 1; i < frameCount-1; i++ {
			delta, err := d.readIndexedSignedLaceVINT(limit)
			if err != nil {
				return 0, err
			}
			size := previous + delta
			if size < 0 || size > int64(len(d.laceBuffer)) {
				return 0, ErrLaceTooLarge
			}
			total += int(size)
			previous = size
		}
		if limit.N < int64(total) {
			return 0, ErrInvalidData
		}
	default:
		return 0, ErrUnsupportedLacing
	}
	return frameCount, nil
}

func (d *Demuxer) readIndexedSignedLaceVINT(limit *io.LimitedReader) (int64, error) {
	value, width, err := ebml.ReadUnsignedVINT(limit, &d.scratch)
	if err != nil {
		return 0, err
	}
	bias := (uint64(1) << uint(7*width-1)) - 1
	if value > uint64(math.MaxInt64)+bias {
		return 0, ErrInvalidData
	}
	return int64(value) - int64(bias), nil
}

func (d *Demuxer) appendPacketIndexEntries(clusterPosition uint64, blockPosition uint64, clusterTimecode int64, info indexedBlockInfo) error {
	for frame := 0; frame < info.FrameCount; frame++ {
		timeNS := info.TimeNS
		if frame != 0 && info.FrameDurationNS > 0 {
			if int64(frame) > (math.MaxInt64-timeNS)/info.FrameDurationNS {
				return ErrInvalidData
			}
			timeNS += int64(frame) * info.FrameDurationNS
		}
		d.packetIndex = append(d.packetIndex, packetIndexEntry{
			TimeNS:          timeNS,
			TrackID:         info.TrackID,
			TrackIndex:      info.TrackIndex,
			ClusterPosition: clusterPosition,
			BlockPosition:   blockPosition,
			ClusterTimecode: clusterTimecode,
			FrameIndex:      frame,
		})
	}
	return nil
}

func (d *Demuxer) clusterForTime(timeNS int64) (clusterIndexEntry, error) {
	if err := d.ensureClusterIndex(); err != nil {
		return clusterIndexEntry{}, err
	}
	index := sort.Search(len(d.clusterIndex), func(i int) bool {
		return d.clusterIndex[i].TimeNS > timeNS
	})
	if index == 0 {
		return d.clusterIndex[0], nil
	}
	return d.clusterIndex[index-1], nil
}

func (d *Demuxer) ensureClusterIndex() error {
	if d.clusterIndexBuilt {
		if len(d.clusterIndex) == 0 {
			return ErrInvalidData
		}
		return nil
	}
	if d.seeker == nil {
		return ErrNonSeekableReader
	}
	d.clusterIndex = d.clusterIndex[:0]
	if _, err := d.seeker.Seek(d.segmentData, io.SeekStart); err != nil {
		return err
	}
	indexReader := ebml.NewReader(d.seeker, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize})
	indexReader.ResetAt(d.seeker, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize}, 0)
	for {
		if d.segmentIndexDone(indexReader.Offset()) {
			break
		}
		header, err := indexReader.ReadHeader()
		if err != nil {
			if d.segmentUnknown && isEOF(err) {
				break
			}
			return err
		}
		if err := d.validateSegmentIndexHeader(header); err != nil {
			return err
		}
		switch header.ID {
		case idCluster:
			entry, err := d.readClusterIndexEntry(indexReader, header)
			if err != nil {
				return err
			}
			d.clusterIndex = append(d.clusterIndex, entry)
		default:
			if err := d.skipSegmentIndexElement(indexReader, header); err != nil {
				return err
			}
		}
	}
	d.clusterIndexBuilt = true
	if len(d.clusterIndex) == 0 {
		return ErrInvalidData
	}
	sort.SliceStable(d.clusterIndex, func(i, j int) bool {
		if d.clusterIndex[i].TimeNS == d.clusterIndex[j].TimeNS {
			return d.clusterIndex[i].Position < d.clusterIndex[j].Position
		}
		return d.clusterIndex[i].TimeNS < d.clusterIndex[j].TimeNS
	})
	return nil
}

func (d *Demuxer) segmentIndexDone(offset int64) bool {
	return !d.segmentUnknown && offset >= 0 && uint64(offset) >= d.segmentSize
}

func (d *Demuxer) validateSegmentIndexHeader(header ebml.Header) error {
	if header.Offset < 0 || header.DataOffset < header.Offset {
		return ErrInvalidData
	}
	if !d.segmentUnknown && uint64(header.DataOffset) > d.segmentSize {
		return ErrInvalidData
	}
	return nil
}

func (d *Demuxer) readClusterIndexEntry(reader *ebml.Reader, header ebml.Header) (clusterIndexEntry, error) {
	if header.Offset < 0 {
		return clusterIndexEntry{}, ErrInvalidData
	}
	entry := clusterIndexEntry{Position: uint64(header.Offset)}
	var err error
	if header.Size.Unknown {
		entry.TimeNS, err = d.readUnknownClusterIndexTime(reader)
	} else {
		entry.TimeNS, err = d.readKnownClusterIndexTime(reader, header)
	}
	if err != nil {
		return clusterIndexEntry{}, err
	}
	return entry, nil
}

func (d *Demuxer) readKnownClusterIndexTime(reader *ebml.Reader, header ebml.Header) (int64, error) {
	clusterEnd, err := d.segmentIndexElementEnd(header)
	if err != nil {
		return 0, err
	}
	timeNS := int64(0)
	for reader.Offset() < clusterEnd {
		child, err := reader.ReadHeader()
		if err != nil {
			return 0, err
		}
		if child.ID == idCluster {
			return 0, ErrInvalidData
		}
		if err := d.validateClusterIndexHeader(child, clusterEnd); err != nil {
			return 0, err
		}
		if child.ID == idTimestamp {
			timeNS, err = d.readClusterIndexTimestamp(reader, child, clusterEnd)
			if err != nil {
				return 0, err
			}
			return timeNS, d.resetSegmentIndexReader(reader, clusterEnd)
		}
		if err := d.skipClusterIndexElement(reader, child, clusterEnd); err != nil {
			return 0, err
		}
	}
	return timeNS, d.resetSegmentIndexReader(reader, clusterEnd)
}

func (d *Demuxer) readUnknownClusterIndexTime(reader *ebml.Reader) (int64, error) {
	timeNS := int64(0)
	for {
		if d.segmentIndexDone(reader.Offset()) {
			return timeNS, nil
		}
		child, err := reader.ReadHeader()
		if err != nil {
			if d.segmentUnknown && isEOF(err) {
				return timeNS, nil
			}
			return 0, err
		}
		if isUnknownClusterTerminator(child.ID) {
			return timeNS, d.resetSegmentIndexReader(reader, child.Offset)
		}
		if err := d.validateSegmentIndexHeader(child); err != nil {
			return 0, err
		}
		if child.ID == idTimestamp {
			var err error
			timeNS, err = d.readClusterIndexTimestamp(reader, child, 0)
			if err != nil {
				return 0, err
			}
			continue
		}
		if err := d.skipSegmentIndexElement(reader, child); err != nil {
			return 0, err
		}
	}
}

func isUnknownClusterTerminator(id ebml.ID) bool {
	switch id {
	case idCluster, idSeekHead, idInfo, idTracks, idAttachments, idChapters, idTags, idCues:
		return true
	default:
		return false
	}
}

func (d *Demuxer) readClusterIndexTimestamp(reader *ebml.Reader, header ebml.Header, clusterEnd int64) (int64, error) {
	if header.Size.Unknown {
		return 0, ErrInvalidData
	}
	end, err := d.segmentIndexElementEnd(header)
	if err != nil {
		return 0, err
	}
	if clusterEnd != 0 && end > clusterEnd {
		return 0, ErrInvalidData
	}
	value, err := readUIntPayloadScratch(reader, header.Size.Value, &d.uintScratch)
	if err != nil {
		return 0, err
	}
	return scaleCueTicks(value, d.timecodeScaleNS)
}

func (d *Demuxer) validateClusterIndexHeader(header ebml.Header, clusterEnd int64) error {
	if err := d.validateSegmentIndexHeader(header); err != nil {
		return err
	}
	if clusterEnd != 0 && header.DataOffset > clusterEnd {
		return ErrInvalidData
	}
	return nil
}

func (d *Demuxer) skipClusterIndexElement(reader *ebml.Reader, header ebml.Header, clusterEnd int64) error {
	end, err := d.segmentIndexElementEnd(header)
	if err != nil {
		return err
	}
	if end > clusterEnd {
		return ErrInvalidData
	}
	return d.resetSegmentIndexReader(reader, end)
}

func (d *Demuxer) skipSegmentIndexElement(reader *ebml.Reader, header ebml.Header) error {
	end, err := d.segmentIndexElementEnd(header)
	if err != nil {
		return err
	}
	return d.resetSegmentIndexReader(reader, end)
}

func (d *Demuxer) segmentIndexElementEnd(header ebml.Header) (int64, error) {
	if header.Size.Unknown {
		return 0, ErrUnsupportedElement
	}
	if header.Size.Value > uint64(math.MaxInt64) {
		return 0, ErrInvalidData
	}
	size := int64(header.Size.Value)
	if header.DataOffset > math.MaxInt64-size {
		return 0, ErrInvalidData
	}
	end := header.DataOffset + size
	if !d.segmentUnknown && uint64(end) > d.segmentSize {
		return 0, ErrInvalidData
	}
	return end, nil
}

func (d *Demuxer) resetSegmentIndexReader(reader *ebml.Reader, offset int64) error {
	if offset < 0 || d.segmentData < 0 || offset > math.MaxInt64-d.segmentData {
		return ErrInvalidData
	}
	if !d.segmentUnknown && uint64(offset) > d.segmentSize {
		return ErrInvalidData
	}
	if _, err := d.seeker.Seek(d.segmentData+offset, io.SeekStart); err != nil {
		return err
	}
	reader.ResetAt(d.seeker, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize}, offset)
	return nil
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

func (d *Demuxer) seekToCueBlockNumber(cluster ebml.Header, blockNumber uint64) error {
	if blockNumber == 0 {
		return ErrInvalidData
	}
	blockIndex := uint64(0)
	for {
		if !cluster.Size.Unknown && d.reader.Offset() >= cluster.DataOffset+int64(cluster.Size.Value) {
			return ErrInvalidData
		}
		header, err := d.reader.ReadHeader()
		if err != nil {
			if isEOF(err) {
				return ErrInvalidData
			}
			return err
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
		case idSimpleBlock, idBlockGroup:
			blockIndex++
			if blockIndex == blockNumber {
				d.pendingHeader = header
				d.pendingHeaderSet = true
				return nil
			}
			if err := skipElement(d.reader, header); err != nil {
				return err
			}
		case idCluster:
			return ErrInvalidData
		default:
			if err := skipElement(d.reader, header); err != nil {
				return err
			}
		}
	}
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
	track.UnknownElements = cloneUnknownElements(track.UnknownElements)
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
	info.UnknownElements = cloneUnknownElements(info.UnknownElements)
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
		out[i].UnknownElements = cloneUnknownElements(attachments[i].UnknownElements)
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
		out[i].UnknownElements = cloneUnknownElements(editions[i].UnknownElements)
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
		out[i].Displays = cloneChapterDisplays(chapters[i].Displays)
		out[i].Children = cloneChapterList(chapters[i].Children)
		out[i].UnknownElements = cloneUnknownElements(chapters[i].UnknownElements)
	}
	return out
}

func cloneChapterDisplays(displays []ChapterDisplay) []ChapterDisplay {
	if len(displays) == 0 {
		return nil
	}
	out := make([]ChapterDisplay, len(displays))
	for i := range displays {
		out[i] = displays[i]
		out[i].UnknownElements = cloneUnknownElements(displays[i].UnknownElements)
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
		out[i].UnknownElements = cloneUnknownElements(tags[i].UnknownElements)
	}
	return out
}

func cloneUnknownElements(elements []UnknownElement) []UnknownElement {
	if len(elements) == 0 {
		return nil
	}
	out := make([]UnknownElement, len(elements))
	for i := range elements {
		out[i] = elements[i]
		if elements[i].Raw != nil {
			out[i].Raw = append([]byte(nil), elements[i].Raw...)
		}
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
	target.UnknownElements = cloneUnknownElements(target.UnknownElements)
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
		out[i].UnknownElements = cloneUnknownElements(tags[i].UnknownElements)
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
		if d.inCluster && !d.clusterUnknown && d.reader.Offset() >= d.clusterEnd {
			d.inCluster = false
		}
		header, err := d.nextPacketHeader()
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
				return d.finishClusterPacket(d.readSimpleBlock(header, dst), dst)
			case idBlockGroup:
				return d.finishClusterPacket(d.readBlockGroup(header, dst), dst)
			case idVoid, idCRC32:
				if err := skipElement(d.reader, header); err != nil {
					return err
				}
			default:
				if err := d.readUnknownClusterElement(header); err != nil {
					return err
				}
			}
			continue
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
			if err := d.parseTags(header); err != nil {
				return err
			}
		case idCues:
			if err := d.parseSegmentCues(header); err != nil {
				return err
			}
		case idCluster:
			if err := d.enterCluster(header); err != nil {
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
			if err := d.parseTags(header); err != nil {
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
	if d.infoSeen && d.tracksSeen {
		return nil
	}
	if d.seeker == nil {
		return ErrInvalidData
	}
	if !d.infoSeen {
		if err := d.loadTopLevelElementFromSeekHead(idInfo); err != nil {
			return err
		}
	}
	if !d.tracksSeen {
		if err := d.loadTopLevelElementFromSeekHead(idTracks); err != nil {
			return err
		}
	}
	if _, err := d.seeker.Seek(cluster.DataOffset, io.SeekStart); err != nil {
		return err
	}
	d.reader.ResetAt(d.seeker, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize}, cluster.DataOffset)
	return nil
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
	durationTicks := float64(0)
	durationSet := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return err
		}
		if err := d.parseInfoChild(master.Reader(), child, &durationTicks, &durationSet); err != nil {
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

func (d *Demuxer) parseInfoChild(reader *ebml.Reader, child ebml.Header, durationTicks *float64, durationSet *bool) error {
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
	case idDuration:
		value, err := readSegmentDurationTicksPayload(reader, child.Size.Value)
		if err != nil {
			return err
		}
		*durationTicks = value
		*durationSet = true
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
	trackNumberSeen := false
	trackUIDSeen := false
	trackTypeSeen := false
	codecIDSeen := false
	videoSeen := false
	audioSeen := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return Track{}, err
		}
		switch child.ID {
		case idTrackNumber:
			if trackNumberSeen {
				return Track{}, ErrInvalidData
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			trackID, err := trackIDFromUint(value)
			if err != nil {
				return Track{}, err
			}
			track.ID = trackID
			trackNumberSeen = true
		case idTrackUID:
			if trackUIDSeen {
				return Track{}, ErrInvalidData
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			if value == 0 {
				return Track{}, ErrInvalidData
			}
			track.UID = value
			trackUIDSeen = true
		case idTrackType:
			if trackTypeSeen {
				return Track{}, ErrInvalidData
			}
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
			trackTypeSeen = true
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
			if codecIDSeen {
				return Track{}, ErrInvalidData
			}
			value, err := readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			codecID = value
			codecIDSeen = true
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
			if videoSeen {
				return Track{}, ErrInvalidData
			}
			video, err := d.parseVideo(master.Reader(), child)
			if err != nil {
				return Track{}, err
			}
			track.Video = video
			videoSeen = true
		case idAudio:
			if audioSeen {
				return Track{}, ErrInvalidData
			}
			audio, err := d.parseAudio(master.Reader(), child)
			if err != nil {
				return Track{}, err
			}
			track.Audio = audio
			audioSeen = true
		default:
			if err := appendUnknownChildElement(master.Reader(), child, isKnownTrackEntryElement, &track.UnknownElements); err != nil {
				return Track{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return Track{}, err
	}
	if !trackNumberSeen || !trackTypeSeen || !codecIDSeen {
		return Track{}, ErrInvalidData
	}
	if (track.Type == TrackVideo && !videoSeen) || (track.Type == TrackAudio && !audioSeen) {
		return Track{}, ErrInvalidData
	}
	if track.UID == 0 {
		track.UID = uint64(track.ID)
	}
	track.Codec = codecFromMatroskaID(codecID, track.CodecPrivate)
	if codecID == codecIDMS {
		format, err := parseMSACMWaveFormat(track.CodecPrivate)
		if err != nil {
			return Track{}, err
		}
		track.Codec = codecFromMSACMTag(format.FormatTag)
		track.Audio.SampleRate = format.SampleRate
		track.Audio.Channels = format.Channels
		track.Audio.BitDepth = format.BitsPerSample
		if track.Codec == CodecPCMU || track.Codec == CodecPCMA {
			if err := validateG711MSACMWaveFormat(format, track.Codec); err != nil {
				return Track{}, err
			}
		}
	}
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
	if track.Codec == CodecH265 && len(track.CodecPrivate) != 0 {
		if _, err := parseHEVCDecoderConfigurationRecord(track.CodecPrivate); err != nil {
			return Track{}, err
		}
	}
	if err := validateTrackBlockAdditionMetadata(track); err != nil {
		return Track{}, err
	}
	if err := validateContentEncodings(track.ContentEncodings); err != nil {
		return Track{}, ErrInvalidData
	}
	if err := validateUnknownElementsFor(track.UnknownElements, isKnownTrackEntryElement); err != nil {
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

func (d *Demuxer) readBlockGroup(header ebml.Header, dst *Packet) (err error) {
	if header.Size.Unknown {
		return ErrInvalidData
	}
	defer func() {
		if err != nil && !errors.Is(err, ErrPayloadTooSmall) {
			d.clearLace()
		}
	}()
	dst.Reset()
	d.lastLaceFrameIndex = -1
	d.lastLaceFrameCount = 0
	d.lastLaceBaseTimeNS = 0
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
		if d.lastLaceFrameCount > 0 {
			durationNS /= int64(d.lastLaceFrameCount)
			d.laceDurationNS = durationNS
			dst.TimeNS = d.lastLaceBaseTimeNS + int64(d.lastLaceFrameIndex)*durationNS
		} else if d.laceFrameCount > 0 {
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
		d.captureLaceGroupMetadata(dst)
	}
	if payloadErr != nil {
		return payloadErr
	}
	return nil
}

func (d *Demuxer) captureLaceGroupMetadata(src *Packet) {
	d.laceReferences = append(d.laceReferences[:0], src.ReferenceBlockTimeNS...)
	d.lacePriority = src.ReferencePriority
	d.laceDiscardPadNS = src.DiscardPaddingNS
	d.laceCodecState = append(d.laceCodecState[:0], src.CodecState...)
	d.laceAdditions = clonePacketBlockAdditions(d.laceAdditions, src.BlockAdditions)
}

func clonePacketBlockAdditions(dst []BlockAddition, src []BlockAddition) []BlockAddition {
	if len(src) == 0 {
		return dst[:0]
	}
	dst = append(dst[:0], src...)
	for i := range src {
		if src[i].Data != nil {
			dst[i].Data = append([]byte(nil), src[i].Data...)
		}
	}
	return dst
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
		if encoding.compression == blockContentTransformNone && !encoding.encryptionSet {
			var headerStrip []byte
			if encoding.headerSet {
				headerStrip = encoding.headerSettings
			}
			if err := d.readPlainBlockFrame(track, frameSize, headerStrip, dst); err != nil {
				return err
			}
		} else {
			frame, err := d.readBlockFrameScratch(frameSize)
			if err != nil {
				return err
			}
			if err := d.decodeContentEncodedBlockFrame(track, frame, encoding, dst); err != nil {
				return err
			}
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

func (d *Demuxer) decodeContentEncodedBlockFrame(track Track, frame []byte, encoding blockContentEncodingInfo, dst *Packet) error {
	payload := frame
	var err error
	if encoding.encryptionSet {
		payload, err = d.decryptBlockPayload(encoding, payload)
		if err != nil {
			return err
		}
	}

	var decoded []byte
	switch encoding.compression {
	case blockContentTransformNone:
		if cap(dst.Data) < len(payload) {
			return ErrPayloadTooSmall
		}
		decoded = dst.Data[:len(payload)]
		copy(decoded, payload)
	case blockContentTransformZlib:
		decoded, err = zlibDecompressInto(dst.Data[:0], payload)
	case blockContentTransformBzlib:
		decoded, err = bzip2DecompressInto(dst.Data[:0], payload)
	case blockContentTransformLZO1X:
		decoded, err = lzoDecompressInto(dst.Data[:0], payload)
	default:
		return ErrUnsupportedContentEncoding
	}
	if err != nil {
		return err
	}
	decoded, err = prependContentEncodingHeader(decoded, encoding.headerSettings)
	if err != nil {
		return err
	}
	dst.Data = decoded
	return d.finishTrackCodecPayload(track, dst)
}

func prependContentEncodingHeader(data []byte, header []byte) ([]byte, error) {
	if len(header) == 0 {
		return data, nil
	}
	oldLen := len(data)
	outSize := oldLen + len(header)
	if cap(data) < outSize {
		return nil, ErrPayloadTooSmall
	}
	data = data[:outSize]
	copy(data[len(header):], data[:oldLen])
	copy(data, header)
	return data, nil
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
	if track.Codec == CodecH265 && len(track.CodecPrivate) != 0 {
		lengthSize, ok, err := h265TrackNALULengthSize(track)
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

func lzoDecompressInto(dst []byte, compressed []byte) ([]byte, error) {
	out, read, err := lzo.DecompressNInto(compressed, dst[:cap(dst)])
	if err != nil {
		if errors.Is(err, lzo.ErrOutputOverrun) {
			return nil, ErrPayloadTooSmall
		}
		return nil, ErrInvalidData
	}
	if read != len(compressed) {
		return nil, ErrInvalidData
	}
	return out, nil
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
	if codecUsesLengthPrefixedSamples(track) {
		lengthSize, ok, err := trackNALULengthSize(track)
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
	if d.laceSeekFrameIndex != 0 {
		if d.laceSeekFrameIndex < 0 || d.laceSeekFrameIndex >= frameCount {
			d.clearLace()
			return ErrInvalidData
		}
		d.laceFrameIndex = d.laceSeekFrameIndex
		d.laceSeekFrameIndex = 0
	}
	d.laceKeyframe = simple && flags&simpleBlockKeyframe != 0
	d.laceInvisible = flags&simpleBlockInvisible != 0
	d.laceDiscardable = simple && flags&simpleBlockDiscardable != 0
	return d.readNextLacedPacket(dst)
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
	if d.laceContent.encryptionSet ||
		d.laceContent.compression == blockContentTransformZlib ||
		d.laceContent.compression == blockContentTransformBzlib ||
		d.laceContent.compression == blockContentTransformLZO1X {
		track, ok := d.track(d.laceTrackID)
		if !ok {
			return ErrUnknownTrack
		}
		if err := d.decodeLacedContentEncodedBlockFrame(track, frameData, d.laceContent, dst); err != nil {
			return err
		}
	} else {
		var headerStrip []byte
		if d.laceContent.headerSet {
			headerStrip = d.laceContent.headerSettings
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
	d.applyLaceGroupMetadata(dst)
	if d.laceFrameIndex == 0 {
		d.applyPendingClusterUnknownElements(dst)
	}
	d.lastLaceBaseTimeNS = d.laceTimeNS
	d.lastLaceFrameIndex = d.laceFrameIndex
	d.lastLaceFrameCount = d.laceFrameCount
	d.laceFrameIndex++
	if d.laceFrameIndex >= d.laceFrameCount {
		d.clearLace()
	}
	return nil
}

func (d *Demuxer) readNextLacedPacket(dst *Packet) error {
	err := d.nextLacedPacket(dst)
	if err != nil && !errors.Is(err, ErrPayloadTooSmall) && d.laceFrameIndex == 0 {
		d.pendingClusterUnknown = d.pendingClusterUnknown[:0]
	}
	return err
}

func (d *Demuxer) decodeLacedContentEncodedBlockFrame(track Track, frame []byte, encoding blockContentEncodingInfo, dst *Packet) error {
	payload := frame
	if encoding.encryptionSet {
		if cap(d.contentBuffer) < len(frame) {
			d.contentBuffer = make([]byte, len(frame))
		}
		payload = d.contentBuffer[:len(frame)]
		copy(payload, frame)
	}
	return d.decodeContentEncodedBlockFrame(track, payload, encoding, dst)
}

func (d *Demuxer) applyLaceGroupMetadata(dst *Packet) {
	if len(d.laceReferences) != 0 {
		dst.ReferenceBlockTimeNS = append(dst.ReferenceBlockTimeNS, d.laceReferences...)
	}
	dst.ReferencePriority = d.lacePriority
	dst.DiscardPaddingNS = d.laceDiscardPadNS
	if len(d.laceCodecState) != 0 {
		dst.CodecState = append(dst.CodecState, d.laceCodecState...)
	}
	if len(d.laceAdditions) != 0 {
		dst.BlockAdditions = clonePacketBlockAdditions(dst.BlockAdditions, d.laceAdditions)
	}
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
	d.laceReferences = d.laceReferences[:0]
	d.lacePriority = 0
	d.laceDiscardPadNS = 0
	d.laceCodecState = d.laceCodecState[:0]
	d.laceAdditions = d.laceAdditions[:0]
	d.laceFrameCount = 0
	d.laceFrameIndex = 0
	d.laceSeekFrameIndex = 0
	d.laceKeyframe = false
	d.laceInvisible = false
	d.laceDiscardable = false
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
	index, ok := d.trackIndex(id)
	if !ok {
		return Track{}, false
	}
	return d.tracks[index], true
}

func (d *Demuxer) trackIndex(id uint32) (int, bool) {
	for i := range d.tracks {
		if d.tracks[i].ID == id {
			return i, true
		}
	}
	return 0, false
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
