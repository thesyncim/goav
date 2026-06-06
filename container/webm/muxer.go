package webm

import (
	"io"

	"github.com/thesyncim/goav/container/matroska"
)

type Muxer struct {
	inner *matroska.Muxer
}

func NewMuxer(w io.Writer, opts MuxerOptions) (*Muxer, error) {
	muxer, err := matroska.NewMuxer(w, matroskaOptions(opts))
	if err != nil {
		return nil, err
	}
	return &Muxer{inner: muxer}, nil
}

func (m *Muxer) AddTrack(track Track) (uint32, error) {
	if err := validateTrack(track); err != nil {
		return 0, err
	}
	return m.inner.AddTrack(track)
}

func (m *Muxer) WritePacket(packet Packet) error {
	return m.inner.WritePacket(packet)
}

func (m *Muxer) Close() error {
	if m == nil || m.inner == nil {
		return nil
	}
	return m.inner.Close()
}
