package webm

import (
	"io"

	"github.com/thesyncim/goav/container/matroska"
)

type Muxer struct {
	inner      *matroska.Muxer
	trackTimes []packetTimeState
}

func NewMuxer(w io.Writer, opts MuxerOptions) (*Muxer, error) {
	if err := validateMuxerOptions(opts); err != nil {
		return nil, err
	}
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
	trackID, err := m.inner.AddTrack(track)
	if err != nil {
		return 0, err
	}
	m.trackTimes = append(m.trackTimes, packetTimeState{trackID: trackID})
	return trackID, nil
}

func (m *Muxer) WritePacket(packet Packet) error {
	if err := m.validatePacketTime(packet.TrackID, packet.TimeNS); err != nil {
		return err
	}
	if err := m.inner.WritePacket(packet); err != nil {
		return err
	}
	m.markPacketTime(packet.TrackID, packet.TimeNS)
	return nil
}

func (m *Muxer) WriteLacedPacket(packet LacedPacket) error {
	if err := m.validatePacketTime(packet.TrackID, packet.TimeNS); err != nil {
		return err
	}
	if err := m.inner.WriteLacedPacket(packet); err != nil {
		return err
	}
	m.markPacketTime(packet.TrackID, packet.TimeNS)
	return nil
}

func (m *Muxer) Close() error {
	if m == nil || m.inner == nil {
		return nil
	}
	return m.inner.Close()
}

func (m *Muxer) validatePacketTime(trackID uint32, timeNS int64) error {
	index, ok := findPacketTimeState(m.trackTimes, trackID)
	if !ok {
		return nil
	}
	state := m.trackTimes[index]
	if state.set && timeNS < state.lastTimeNS {
		return ErrNonMonotonicWebMTimecode
	}
	return nil
}

func (m *Muxer) markPacketTime(trackID uint32, timeNS int64) {
	index, ok := findPacketTimeState(m.trackTimes, trackID)
	if !ok {
		return
	}
	m.trackTimes[index].lastTimeNS = timeNS
	m.trackTimes[index].set = true
}
