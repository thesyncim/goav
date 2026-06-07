package webm

import (
	"io"

	"github.com/thesyncim/goav/container/matroska"
)

type Demuxer struct {
	inner *matroska.Demuxer
}

func NewDemuxer(r io.Reader, opts DemuxerOptions) (*Demuxer, error) {
	demuxer, err := matroska.NewDemuxer(r, opts)
	if err != nil {
		return nil, err
	}
	if demuxer.DocType() != "webm" {
		return nil, ErrUnsupportedWebMDocType
	}
	for _, track := range demuxer.Tracks() {
		if err := validateTrack(track); err != nil {
			return nil, err
		}
	}
	return &Demuxer{inner: demuxer}, nil
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

func (d *Demuxer) ReadPacket(dst *Packet) error {
	return d.inner.ReadPacket(dst)
}

func (d *Demuxer) SeekToTime(timeNS int64) error {
	return d.inner.SeekToTime(timeNS)
}

func (d *Demuxer) SeekToTrackTime(trackID uint32, timeNS int64) error {
	return d.inner.SeekToTrackTime(trackID, timeNS)
}

// ReadPacketAtTime seeks to the nearest preceding cue and reads forward until
// it finds the first packet at or after timeNS.
func (d *Demuxer) ReadPacketAtTime(timeNS int64, dst *Packet) error {
	return d.inner.ReadPacketAtTime(timeNS, dst)
}

// ReadTrackPacketAtTime seeks to the nearest preceding cue for trackID and
// reads forward until it finds the first packet for trackID at or after timeNS.
func (d *Demuxer) ReadTrackPacketAtTime(trackID uint32, timeNS int64, dst *Packet) error {
	return d.inner.ReadTrackPacketAtTime(trackID, timeNS, dst)
}
