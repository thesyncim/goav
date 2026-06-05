package rtpav

import "github.com/pion/rtp"

type SequenceDetector struct {
	initialized bool
	ssrc        uint32
	cycles      uint32
	expected    uint16
}

func (d *SequenceDetector) Reset() {
	d.initialized = false
	d.ssrc = 0
	d.cycles = 0
	d.expected = 0
}

func (d *SequenceDetector) Push(pkt *rtp.Packet, missing []uint16) SequenceState {
	missing = missing[:0]
	state := SequenceState{
		SSRC:     pkt.SSRC,
		Cycles:   d.cycles,
		Expected: d.expected,
		Missing:  missing,
	}

	if !d.initialized {
		d.init(pkt)
		state.Cycles = d.cycles
		state.Expected = d.expected
		return state
	}

	if pkt.SSRC != d.ssrc {
		d.init(pkt)
		state.Discontinuous = true
		state.Cycles = d.cycles
		state.Expected = d.expected
		return state
	}

	seq := pkt.SequenceNumber
	delta := seq - d.expected
	switch {
	case delta == 0:
		d.advance(seq)
	case delta < 0x8000:
		state.LossBefore = true
		if seq < d.expected {
			d.cycles++
		}
		for cur := d.expected; cur != seq; cur++ {
			if len(missing) == cap(missing) {
				state.MissingTruncated = true
				continue
			}
			n := len(missing)
			missing = missing[:n+1]
			missing[n] = cur
		}
		d.advanceNoCycle(seq)
	default:
		// Late or reordered packet. Expected sequence remains unchanged.
	}

	state.Cycles = d.cycles
	state.Expected = d.expected
	state.Missing = missing
	return state
}

func (d *SequenceDetector) init(pkt *rtp.Packet) {
	d.initialized = true
	d.ssrc = pkt.SSRC
	d.cycles = 0
	d.expected = pkt.SequenceNumber + 1
}

func (d *SequenceDetector) advance(seq uint16) {
	if seq == 0xffff {
		d.cycles++
	}
	d.expected = seq + 1
}

func (d *SequenceDetector) advanceNoCycle(seq uint16) {
	d.expected = seq + 1
}
