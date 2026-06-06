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
