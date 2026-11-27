package matroska

import (
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
	return errors.Is(err, ErrInvalidData) && len(d.cues) == 0 && d.cuesState == cuesUnseen && !d.hasCuesSeekEntry()
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
	prevOffset := d.reader.Offset()
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
		_, _ = d.seeker.Seek(prevOffset, io.SeekStart)
		d.reader.ResetAt(d.seeker, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize}, prevOffset)
		return ebml.Header{}, err
	}
	if header.ID != idCluster {
		_, _ = d.seeker.Seek(prevOffset, io.SeekStart)
		d.reader.ResetAt(d.seeker, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize}, prevOffset)
		return ebml.Header{}, ErrInvalidData
	}
	d.pendingHeader = ebml.Header{}
	d.pendingHeaderSet = false
	d.pendingClusterUnknown = d.pendingClusterUnknown[:0]
	if err := d.enterCluster(header); err != nil {
		_, _ = d.seeker.Seek(prevOffset, io.SeekStart)
		d.reader.ResetAt(d.seeker, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize}, prevOffset)
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
	if d.cuesState == cuesSorted {
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
	if d.cuesState == cuesSorted {
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
		if d.cluster == clusterSized && d.reader.Offset() >= d.clusterEnd && d.laceFrameIndex >= d.laceFrameCount {
			return ErrInvalidData
		}
	}
}

func (d *Demuxer) cueAtOrAfterTime(timeNS int64) (CuePoint, CueTrackPosition, bool) {
	if d.cuesState == cuesSorted {
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
	if d.cuesState == cuesSorted {
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

