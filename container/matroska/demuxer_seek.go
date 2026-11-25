package matroska

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sort"

	"github.com/thesyncim/goav/container/ebml"
)

// CanSeek reports whether the reader can reposition: it is an io.Seeker
// that knows its position, which a pipe does not.
func (d *Demuxer) CanSeek() bool { return d != nil && d.seeker != nil }

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
	// ResetAt keeps the reader's logical offsets absolute (file offsets), the
	// same convention as the sequential read and every other reposition site.
	// A plain Reset would restart offsets at zero, so a top-level element
	// already preloaded through the seek head (the trailing Cues, typically)
	// would no longer match its recorded offset when the post-seek read
	// reaches it — tripping the duplicate-element guard instead of skipping.
	d.reader.ResetAt(d.seeker, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize}, offset)
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
	best := d.cues[0]
	earliest := d.cues[0]
	found := false
	for i := range d.cues {
		if d.cues[i].TimeNS < earliest.TimeNS {
			earliest = d.cues[i]
		}
		if d.cues[i].TimeNS <= timeNS && (!found || d.cues[i].TimeNS > best.TimeNS) {
			best = d.cues[i]
			found = true
		}
	}
	if found {
		return best
	}
	return earliest
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
	var firstCue CuePoint
	var firstPosition CueTrackPosition
	found := false
	firstFound := false
	for i := range d.cues {
		if candidate, ok := cuePositionForTrack(d.cues[i], trackID); ok {
			if !firstFound || d.cues[i].TimeNS < firstCue.TimeNS {
				firstCue = d.cues[i]
				firstPosition = candidate
				firstFound = true
			}
			if d.cues[i].TimeNS <= timeNS && (!found || d.cues[i].TimeNS > cue.TimeNS) {
				cue = d.cues[i]
				position = candidate
				found = true
			}
		}
	}
	if found {
		return cue, position, true
	}
	if firstFound {
		return firstCue, firstPosition, true
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
	if entry.BlockPosition > uint64(math.MaxInt64) ||
		int64(entry.BlockPosition) > math.MaxInt64-d.segmentData {
		return ErrInvalidData
	}
	// The reader's logical offsets are absolute file offsets (the same
	// convention as the sequential read), so the block bounds are checked and
	// the reader repositioned in absolute terms too.
	blockOffset := d.segmentData + int64(entry.BlockPosition)
	if blockOffset < cluster.DataOffset {
		return ErrInvalidData
	}
	if !cluster.Size.Unknown && blockOffset >= cluster.DataOffset+int64(cluster.Size.Value) {
		return ErrInvalidData
	}
	if _, err := d.seeker.Seek(blockOffset, io.SeekStart); err != nil {
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

func (d *Demuxer) ensurePacketIndex() (retErr error) {
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
	saved := d.reader.Offset()
	defer func() {
		if retErr != nil {
			_, _ = d.seeker.Seek(saved, io.SeekStart)
		}
	}()
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
	durationTicks := uint64(0)
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
			durationTicks = value
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
	durationNS := int64(0)
	if durationTicks != 0 {
		if durationTicks > uint64(math.MaxInt64) {
			return ErrInvalidData
		}
		durationNS, err = scaleTrackTicksNS(int64(durationTicks), d.timecodeScaleNS, d.tracks[info.TrackIndex])
		if err != nil {
			return err
		}
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
	timeNS, err := trackBlockTimestampNS(clusterTimecode, int64(blockTimecode), d.timecodeScaleNS, track)
	if err != nil || timeNS < 0 {
		return indexedBlockInfo{}, ErrInvalidData
	}
	info := indexedBlockInfo{
		TrackID:    trackNumber,
		TrackIndex: trackIndex,
		TimeNS:     timeNS,
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

func (d *Demuxer) ensureClusterIndex() (retErr error) {
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
	saved := d.reader.Offset()
	defer func() {
		if retErr != nil {
			_, _ = d.seeker.Seek(saved, io.SeekStart)
		}
	}()
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
