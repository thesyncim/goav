package webm

import (
	"io"

	"github.com/thesyncim/goav/container/matroska"
)

type Muxer struct {
	inner      *matroska.Muxer
	trackTimes map[uint32]packetTimeState
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
	trackID, err := m.inner.AddTrack(track)
	if err != nil {
		return 0, err
	}
	if m.trackTimes == nil {
		m.trackTimes = make(map[uint32]packetTimeState)
	}
	m.trackTimes[trackID] = packetTimeState{}
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
	state, ok := m.trackTimes[trackID]
	if ok && state.set && timeNS < state.lastTimeNS {
		return ErrNonMonotonicWebMTimecode
	}
	return nil
}

func (m *Muxer) markPacketTime(trackID uint32, timeNS int64) {
	if m.trackTimes == nil {
		m.trackTimes = make(map[uint32]packetTimeState)
	}
	m.trackTimes[trackID] = packetTimeState{lastTimeNS: timeNS, set: true}
}
