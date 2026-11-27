package matroska

import (
	"io"
	"math"

	"github.com/thesyncim/goav/container/ebml"
)

type cuesState uint8

const (
	cuesUnseen cuesState = iota
	cuesSorted
	cuesUnsorted
)

func (d *Demuxer) parseCues(header ebml.Header) error {
	if header.Size.Unknown {
		return ErrInvalidData
	}
	master, err := d.checkedMasterReader(d.reader, header.Size.Value)
	if err != nil {
		return err
	}
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return err
		}
		switch child.ID {
		case idCuePoint:
			cue, err := d.parseCuePoint(master.Reader(), child)
			if err != nil {
				return err
			}
			if cue.TrackID != 0 {
				if len(d.cues) != 0 && cue.TimeNS < d.cues[len(d.cues)-1].TimeNS {
					d.cuesState = cuesUnsorted
				}
				d.cues = append(d.cues, cue)
			}
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return err
			}
		}
	}
	return master.Validate()
}


func (d *Demuxer) parseCuePoint(parent io.Reader, header ebml.Header) (CuePoint, error) {
	if header.Size.Unknown {
		return CuePoint{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return CuePoint{}, err
	}
	var cue CuePoint
	timeSeen := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return CuePoint{}, err
		}
		switch child.ID {
		case idCueTime:
			if err := markElementSeen(&timeSeen); err != nil {
				return CuePoint{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return CuePoint{}, err
			}
			timeNS, err := scaleCueTicks(value, d.timecodeScaleNS)
			if err != nil {
				return CuePoint{}, ErrInvalidData
			}
			cue.TimeNS = timeNS
		case idCueTrackPositions:
			position, err := d.parseCueTrackPositions(master.Reader(), child)
			if err != nil {
				return CuePoint{}, err
			}
			cue.Positions = append(cue.Positions, position)
			if len(cue.Positions) == 1 {
				applyCuePosition(&cue, position)
			}
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return CuePoint{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return CuePoint{}, err
	}
	if !timeSeen || len(cue.Positions) == 0 {
		return CuePoint{}, ErrInvalidData
	}
	return cue, nil
}

func applyCuePosition(cue *CuePoint, position CueTrackPosition) {
	cue.TrackID = position.TrackID
	cue.ClusterPosition = position.ClusterPosition
	cue.RelativePosition = position.RelativePosition
	cue.RelativePositionSet = position.RelativePositionSet
	cue.DurationNS = position.DurationNS
	cue.DurationSet = position.DurationSet
	cue.BlockNumber = position.BlockNumber
	cue.BlockNumberSet = position.BlockNumberSet
	cue.CodecStatePosition = position.CodecStatePosition
	cue.CodecStateSet = position.CodecStateSet
	cue.References = cloneCueReferences(position.References)
}

func scaleCueTicks(value uint64, scaleNS int64) (int64, error) {
	if scaleNS <= 0 || value > uint64(math.MaxInt64)/uint64(scaleNS) {
		return 0, ErrInvalidData
	}
	return int64(value) * scaleNS, nil
}

func (d *Demuxer) parseCueTrackPositions(parent io.Reader, header ebml.Header) (CueTrackPosition, error) {
	if header.Size.Unknown {
		return CueTrackPosition{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return CueTrackPosition{}, err
	}
	var position CueTrackPosition
	trackSeen := false
	clusterSeen := false
	relativeSeen := false
	durationSeen := false
	blockNumberSeen := false
	codecStateSeen := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return CueTrackPosition{}, err
		}
		switch child.ID {
		case idCueTrack:
			if err := markElementSeen(&trackSeen); err != nil {
				return CueTrackPosition{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return CueTrackPosition{}, err
			}
			position.TrackID, err = trackIDFromUint(value)
			if err != nil {
				return CueTrackPosition{}, err
			}
		case idCueClusterPosition:
			if err := markElementSeen(&clusterSeen); err != nil {
				return CueTrackPosition{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return CueTrackPosition{}, err
			}
			position.ClusterPosition = value
		case idCueRelativePos:
			if err := markElementSeen(&relativeSeen); err != nil {
				return CueTrackPosition{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return CueTrackPosition{}, err
			}
			position.RelativePosition = value
			position.RelativePositionSet = true
		case idCueDuration:
			if err := markElementSeen(&durationSeen); err != nil {
				return CueTrackPosition{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return CueTrackPosition{}, err
			}
			position.DurationNS, err = scaleCueTicks(value, d.timecodeScaleNS)
			if err != nil {
				return CueTrackPosition{}, err
			}
			position.DurationSet = true
		case idCueBlockNumber:
			if err := markElementSeen(&blockNumberSeen); err != nil {
				return CueTrackPosition{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return CueTrackPosition{}, err
			}
			if value == 0 {
				return CueTrackPosition{}, ErrInvalidData
			}
			position.BlockNumber = value
			position.BlockNumberSet = true
		case idCueCodecState:
			if err := markElementSeen(&codecStateSeen); err != nil {
				return CueTrackPosition{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return CueTrackPosition{}, err
			}
			position.CodecStatePosition = value
			position.CodecStateSet = true
		case idCueReference:
			reference, err := d.parseCueReference(master.Reader(), child)
			if err != nil {
				return CueTrackPosition{}, err
			}
			position.References = append(position.References, reference)
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return CueTrackPosition{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return CueTrackPosition{}, err
	}
	if !trackSeen || !clusterSeen {
		return CueTrackPosition{}, ErrInvalidData
	}
	return position, nil
}

func (d *Demuxer) parseCueReference(parent io.Reader, header ebml.Header) (CueReference, error) {
	if header.Size.Unknown {
		return CueReference{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return CueReference{}, err
	}
	reference := CueReference{BlockNumber: 1}
	timeSeen := false
	clusterSeen := false
	numberSeen := false
	codecStateSeen := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return CueReference{}, err
		}
		switch child.ID {
		case idCueRefTime:
			if err := markElementSeen(&timeSeen); err != nil {
				return CueReference{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return CueReference{}, err
			}
			reference.TimeNS, err = scaleCueTicks(value, d.timecodeScaleNS)
			if err != nil {
				return CueReference{}, err
			}
		case idCueRefCluster:
			if err := markElementSeen(&clusterSeen); err != nil {
				return CueReference{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return CueReference{}, err
			}
			reference.ClusterPosition = value
		case idCueRefNumber:
			if err := markElementSeen(&numberSeen); err != nil {
				return CueReference{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return CueReference{}, err
			}
			if value == 0 {
				return CueReference{}, ErrInvalidData
			}
			reference.BlockNumber = value
			reference.BlockNumberSet = true
		case idCueRefCodecState:
			if err := markElementSeen(&codecStateSeen); err != nil {
				return CueReference{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return CueReference{}, err
			}
			reference.CodecStatePosition = value
			reference.CodecStateSet = true
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return CueReference{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return CueReference{}, err
	}
	if !timeSeen || !clusterSeen {
		return CueReference{}, ErrInvalidData
	}
	return reference, nil
}
