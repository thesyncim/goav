package webm

import (
	"io"

	"github.com/thesyncim/goav/container/matroska"
)

type Demuxer struct {
	inner      *matroska.Demuxer
	trackTimes []packetTimeState
}

func NewDemuxer(r io.Reader, opts DemuxerOptions) (*Demuxer, error) {
	demuxer, err := matroska.NewDemuxer(r, opts)
	if err != nil {
		return nil, err
	}
	if demuxer.DocType() != "webm" {
		return nil, ErrUnsupportedWebMDocType
	}
	tracks := demuxer.Tracks()
	trackTimes := make([]packetTimeState, 0, len(tracks))
	for _, track := range tracks {
		if err := validateTrack(track); err != nil {
			return nil, err
		}
		trackTimes = append(trackTimes, packetTimeState{trackID: track.ID})
	}
	if err := validateDemuxerMetadata(demuxer); err != nil {
		return nil, err
	}
	return &Demuxer{inner: demuxer, trackTimes: trackTimes}, nil
}

func (d *Demuxer) Tracks() []Track {
	if d == nil || d.inner == nil {
		return nil
	}
	return d.inner.Tracks()
}

func (d *Demuxer) Info() SegmentInfo {
	if d == nil || d.inner == nil {
		return SegmentInfo{}
	}
	return d.inner.Info()
}

func (d *Demuxer) Cues() []CuePoint {
	if d == nil || d.inner == nil {
		return nil
	}
	return d.inner.Cues()
}

func (d *Demuxer) SeekEntries() []SeekEntry {
	if d == nil || d.inner == nil {
		return nil
	}
	return d.inner.SeekEntries()
}

func (d *Demuxer) Tags() []Tag {
	if d == nil || d.inner == nil {
		return nil
	}
	return d.inner.Tags()
}

func (d *Demuxer) UnknownSegmentElements() []UnknownElement {
	if d == nil || d.inner == nil {
		return nil
	}
	return d.inner.UnknownSegmentElements()
}

func (d *Demuxer) UnknownTracksElements() []UnknownElement {
	if d == nil || d.inner == nil {
		return nil
	}
	return d.inner.UnknownTracksElements()
}

func (d *Demuxer) ReadPacket(dst *Packet) error {
	if err := d.inner.ReadPacket(dst); err != nil {
		return err
	}
	return d.validatePacketTime(dst.TrackID, dst.TimeNS)
}

func (d *Demuxer) SeekToTime(timeNS int64) error {
	if err := d.inner.SeekToTime(timeNS); err != nil {
		return err
	}
	d.resetPacketTime()
	return nil
}

func (d *Demuxer) SeekToTrackTime(trackID uint32, timeNS int64) error {
	if err := d.inner.SeekToTrackTime(trackID, timeNS); err != nil {
		return err
	}
	d.resetPacketTime()
	return nil
}

// ReadCuedPacketAtTime seeks to the first cue at or after timeNS and reads the
// cued packet. Exact block cues jump to the block; cluster-only cues scan
// within the referenced Cluster until the cue's track/time is reached. It does
// not scan uncued packets between cues; use ReadPacketAtTime when uncued packets
// should be considered too.
func (d *Demuxer) ReadCuedPacketAtTime(timeNS int64, dst *Packet) error {
	d.resetPacketTime()
	if err := d.inner.ReadCuedPacketAtTime(timeNS, dst); err != nil {
		return err
	}
	return d.validatePacketTime(dst.TrackID, dst.TimeNS)
}

// ReadPacketAtTime seeks to the nearest preceding cue and reads forward until
// it finds the first packet at or after timeNS.
func (d *Demuxer) ReadPacketAtTime(timeNS int64, dst *Packet) error {
	d.resetPacketTime()
	if err := d.inner.ReadPacketAtTime(timeNS, dst); err != nil {
		return err
	}
	return d.validatePacketTime(dst.TrackID, dst.TimeNS)
}

// ReadCuedTrackPacketAtTime seeks to the first cue for trackID at or after
// timeNS and reads that cued packet. Exact block cues jump to the block;
// cluster-only cues scan within the referenced Cluster until the cue's
// track/time is reached. It does not scan uncued packets between cues; use
// ReadTrackPacketAtTime when uncued packets should be considered too.
func (d *Demuxer) ReadCuedTrackPacketAtTime(trackID uint32, timeNS int64, dst *Packet) error {
	d.resetPacketTime()
	if err := d.inner.ReadCuedTrackPacketAtTime(trackID, timeNS, dst); err != nil {
		return err
	}
	return d.validatePacketTime(dst.TrackID, dst.TimeNS)
}

// ReadTrackPacketAtTime seeks to the nearest preceding cue for trackID and
// reads forward until it finds the first packet for trackID at or after timeNS.
func (d *Demuxer) ReadTrackPacketAtTime(trackID uint32, timeNS int64, dst *Packet) error {
	d.resetPacketTime()
	if err := d.inner.ReadTrackPacketAtTime(trackID, timeNS, dst); err != nil {
		return err
	}
	return d.validatePacketTime(dst.TrackID, dst.TimeNS)
}

func (d *Demuxer) validatePacketTime(trackID uint32, timeNS int64) error {
	index, ok := findPacketTimeState(d.trackTimes, trackID)
	if !ok {
		return nil
	}
	state := d.trackTimes[index]
	if state.set && timeNS < state.lastTimeNS {
		return ErrNonMonotonicWebMTimecode
	}
	d.trackTimes[index].lastTimeNS = timeNS
	d.trackTimes[index].set = true
	return nil
}

func (d *Demuxer) resetPacketTime() {
	for i := range d.trackTimes {
		d.trackTimes[i].lastTimeNS = 0
		d.trackTimes[i].set = false
	}
}
