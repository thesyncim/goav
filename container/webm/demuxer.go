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

func (d *Demuxer) ReadPacket(dst *Packet) error {
	return d.inner.ReadPacket(dst)
}

func (d *Demuxer) SeekToTime(timeNS int64) error {
	return d.inner.SeekToTime(timeNS)
}

// ReadPacketAtTime seeks to the nearest preceding cue and reads forward until
// it finds the first packet at or after timeNS.
func (d *Demuxer) ReadPacketAtTime(timeNS int64, dst *Packet) error {
	return d.inner.ReadPacketAtTime(timeNS, dst)
}
