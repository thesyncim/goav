package matroska

import (
	"bytes"
	"math"
	"slices"
	"sort"

	"github.com/thesyncim/goav/container/ebml"
)

func (m *Muxer) addCue(packet Packet, track Track, timecode int64, relativePosition uint64, blockNumber uint64) {
	if !m.shouldCuePacket(packet, track) {
		return
	}
	position := CueTrackPosition{
		TrackID:             packet.TrackID,
		ClusterPosition:     m.clusterPosition,
		RelativePosition:    relativePosition,
		RelativePositionSet: true,
		BlockNumber:         blockNumber,
		BlockNumberSet:      blockNumber != 0,
	}
	if packet.DurationNS > 0 {
		position.DurationNS = packet.DurationNS
		position.DurationSet = true
	}
	if len(packet.ReferenceBlockTimeNS) != 0 {
		position.References = m.cueReferences(packet)
	}
	m.addCuePosition(timecode*m.options.TimecodeScaleNS, position)
}

func (m *Muxer) addCuePosition(timeNS int64, position CueTrackPosition) {
	for i := range m.cues {
		if m.cues[i].TimeNS != timeNS {
			continue
		}
		if cueHasTrackPosition(m.cues[i], position.TrackID) {
			continue
		}
		if len(m.cues[i].Positions) == 0 && m.cues[i].TrackID != 0 {
			m.cues[i].Positions = append(m.cues[i].Positions, cueTrackPositionFromLegacy(m.cues[i]))
		}
		m.cues[i].Positions = append(m.cues[i].Positions, position)
		return
	}
	cue := CuePoint{
		TimeNS:    timeNS,
		Positions: []CueTrackPosition{position},
	}
	applyCuePosition(&cue, position)
	m.cues = append(m.cues, cue)
}

func cueHasTrackPosition(cue CuePoint, trackID uint32) bool {
	if len(cue.Positions) == 0 {
		return cue.TrackID == trackID
	}
	for i := range cue.Positions {
		if cue.Positions[i].TrackID == trackID {
			return true
		}
	}
	return false
}

func (m *Muxer) cueReferences(packet Packet) []CueReference {
	var references []CueReference
	for i := range packet.ReferenceBlockTimeNS {
		timeNS, ok := cueReferenceTimeNS(packet.TimeNS, packet.ReferenceBlockTimeNS[i])
		if !ok {
			continue
		}
		position, ok := m.cuePositionForReference(packet.TrackID, timeNS)
		if !ok {
			continue
		}
		references = append(references, CueReference{
			TimeNS:             timeNS,
			ClusterPosition:    position.ClusterPosition,
			BlockNumber:        position.BlockNumber,
			BlockNumberSet:     position.BlockNumberSet,
			CodecStatePosition: position.CodecStatePosition,
			CodecStateSet:      position.CodecStateSet,
		})
	}
	return references
}

func cueReferenceTimeNS(packetTimeNS int64, offsetNS int64) (int64, bool) {
	if offsetNS > 0 && packetTimeNS > math.MaxInt64-offsetNS {
		return 0, false
	}
	if offsetNS < 0 {
		if offsetNS == math.MinInt64 || packetTimeNS < -offsetNS {
			return 0, false
		}
	}
	return packetTimeNS + offsetNS, true
}

func (m *Muxer) cuePositionForReference(trackID uint32, timeNS int64) (CueTrackPosition, bool) {
	for _, v := range slices.Backward(m.cues) {
		if v.TimeNS != timeNS {
			continue
		}
		if position, ok := cuePositionForTrack(v, trackID); ok {
			return position, true
		}
	}
	return CueTrackPosition{}, false
}

func (m *Muxer) shouldCuePacket(packet Packet, track Track) bool {
	if !m.collectsCues() {
		return false
	}
	switch m.options.CuePolicy {
	case CuePolicyNone:
		return false
	case CuePolicyAllPackets:
		return true
	case CuePolicyKeyframes:
		return packet.Keyframe
	default:
		return packet.Keyframe || track.Type == TrackAudio
	}
}

func (m *Muxer) collectsCues() bool {
	return !m.options.Streaming && m.seekable && m.options.CuePolicy != CuePolicyNone
}

func (m *Muxer) hasCuesToWrite() bool {
	return m.collectsCues() && len(m.cues) != 0
}

func (m *Muxer) writeCues() error {
	m.cuesPosition = m.relativeSegmentPosition()
	m.sortCues()
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	for i := range m.cues {
		if err := writeCuePoint(w, m.cues[i], m.options.TimecodeScaleNS); err != nil {
			return err
		}
	}
	return m.writeMasterElement(idCues, payload.Bytes())
}

func (m *Muxer) sortCues() {
	sort.SliceStable(m.cues, func(i, j int) bool {
		return m.cues[i].TimeNS < m.cues[j].TimeNS
	})
}

func writeCuePoint(w *ebml.Writer, cue CuePoint, scaleNS int64) error {
	if scaleNS <= 0 || cue.TimeNS < 0 {
		return ErrInvalidData
	}
	var payload bytes.Buffer
	cw := ebml.NewWriter(&payload)
	if err := cw.WriteUInt(idCueTime, uint64(cue.TimeNS/scaleNS)); err != nil {
		return err
	}
	if len(cue.Positions) == 0 {
		if cue.TrackID == 0 {
			return ErrInvalidData
		}
		position := cueTrackPositionFromLegacy(cue)
		if err := writeCueTrackPosition(cw, position, scaleNS); err != nil {
			return err
		}
		return w.WriteElement(idCuePoint, payload.Bytes())
	}
	for i := range cue.Positions {
		if err := writeCueTrackPosition(cw, cue.Positions[i], scaleNS); err != nil {
			return err
		}
	}
	return w.WriteElement(idCuePoint, payload.Bytes())
}

func cueTrackPositionFromLegacy(cue CuePoint) CueTrackPosition {
	return CueTrackPosition{
		TrackID:             cue.TrackID,
		ClusterPosition:     cue.ClusterPosition,
		RelativePosition:    cue.RelativePosition,
		RelativePositionSet: cue.RelativePositionSet,
		DurationNS:          cue.DurationNS,
		DurationSet:         cue.DurationSet,
		BlockNumber:         cue.BlockNumber,
		BlockNumberSet:      cue.BlockNumberSet,
		CodecStatePosition:  cue.CodecStatePosition,
		CodecStateSet:       cue.CodecStateSet,
		References:          cue.References,
	}
}

func writeCueTrackPosition(w *ebml.Writer, position CueTrackPosition, scaleNS int64) error {
	if position.TrackID == 0 || position.DurationNS < 0 || (position.BlockNumberSet && position.BlockNumber == 0) {
		return ErrInvalidData
	}
	var positions bytes.Buffer
	pw := ebml.NewWriter(&positions)
	if err := pw.WriteUInt(idCueTrack, uint64(position.TrackID)); err != nil {
		return err
	}
	if err := pw.WriteUInt(idCueClusterPosition, position.ClusterPosition); err != nil {
		return err
	}
	if position.RelativePositionSet {
		if err := pw.WriteUInt(idCueRelativePos, position.RelativePosition); err != nil {
			return err
		}
	}
	if position.DurationSet {
		durationTicks, err := scaledSegmentDurationTicks(position.DurationNS, scaleNS)
		if err != nil {
			return err
		}
		if err := pw.WriteUInt(idCueDuration, durationTicks); err != nil {
			return err
		}
	}
	if position.BlockNumberSet {
		if err := pw.WriteUInt(idCueBlockNumber, position.BlockNumber); err != nil {
			return err
		}
	}
	if position.CodecStateSet {
		if err := pw.WriteUInt(idCueCodecState, position.CodecStatePosition); err != nil {
			return err
		}
	}
	for i := range position.References {
		if err := writeCueReference(pw, position.References[i], scaleNS); err != nil {
			return err
		}
	}
	return w.WriteElement(idCueTrackPositions, positions.Bytes())
}

func writeCueReference(w *ebml.Writer, reference CueReference, scaleNS int64) error {
	if reference.TimeNS < 0 || (reference.BlockNumberSet && reference.BlockNumber == 0) {
		return ErrInvalidData
	}
	var payload bytes.Buffer
	rw := ebml.NewWriter(&payload)
	if err := rw.WriteUInt(idCueRefTime, uint64(reference.TimeNS/scaleNS)); err != nil {
		return err
	}
	if err := rw.WriteUInt(idCueRefCluster, reference.ClusterPosition); err != nil {
		return err
	}
	if reference.BlockNumberSet {
		if err := rw.WriteUInt(idCueRefNumber, reference.BlockNumber); err != nil {
			return err
		}
	}
	if reference.CodecStateSet {
		if err := rw.WriteUInt(idCueRefCodecState, reference.CodecStatePosition); err != nil {
			return err
		}
	}
	return w.WriteElement(idCueReference, payload.Bytes())
}
