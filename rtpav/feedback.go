package rtpav

import "github.com/pion/rtcp"

// FeedbackResult is a caller-owned, allocation-free builder for outgoing
// RTCP feedback: AddNACK/AddPLI/AddFIR append into preallocated capacity and
// Packets is what a FeedbackWriter sends.
type FeedbackResult struct {
	Packets    []rtcp.Packet
	NACKPairs  []rtcp.NackPair
	FIREntries []rtcp.FIREntry

	nack rtcp.TransportLayerNack
	pli  rtcp.PictureLossIndication
	fir  rtcp.FullIntraRequest
}

// Reset clears the result for reuse, keeping allocated capacity.
func (r *FeedbackResult) Reset() {
	for i := range r.Packets {
		r.Packets[i] = nil
	}
	for i := range r.NACKPairs {
		r.NACKPairs[i] = rtcp.NackPair{}
	}
	for i := range r.FIREntries {
		r.FIREntries[i] = rtcp.FIREntry{}
	}
	r.Packets = r.Packets[:0]
	r.NACKPairs = r.NACKPairs[:0]
	r.FIREntries = r.FIREntries[:0]
	r.nack = rtcp.TransportLayerNack{}
	r.pli = rtcp.PictureLossIndication{}
	r.fir = rtcp.FullIntraRequest{}
}

// AddNACK appends one transport-layer NACK covering the missing sequence
// numbers (compressed into NACK pairs).
func (r *FeedbackResult) AddNACK(senderSSRC uint32, mediaSSRC uint32, missing []uint16) error {
	if len(missing) == 0 {
		return nil
	}
	start := len(r.NACKPairs)
	if err := r.appendNACKPair(rtcp.NackPair{PacketID: missing[0]}); err != nil {
		return err
	}
	for i := 1; i < len(missing); i++ {
		seq := missing[i]
		current := &r.NACKPairs[len(r.NACKPairs)-1]
		delta := seq - current.PacketID
		if delta == 0 {
			continue
		}
		if delta > 16 {
			if err := r.appendNACKPair(rtcp.NackPair{PacketID: seq}); err != nil {
				r.NACKPairs = r.NACKPairs[:start]
				return err
			}
			continue
		}
		current.LostPackets |= 1 << (delta - 1)
	}
	r.nack = rtcp.TransportLayerNack{
		SenderSSRC: senderSSRC,
		MediaSSRC:  mediaSSRC,
		Nacks:      r.NACKPairs[start:],
	}
	return r.appendPacket(&r.nack)
}

// AddPLI appends one picture-loss indication asking the sender for a new
// keyframe.
func (r *FeedbackResult) AddPLI(senderSSRC uint32, mediaSSRC uint32) error {
	r.pli = rtcp.PictureLossIndication{
		SenderSSRC: senderSSRC,
		MediaSSRC:  mediaSSRC,
	}
	return r.appendPacket(&r.pli)
}

// AddFIR appends one full-intra request with the given FIR sequence number.
func (r *FeedbackResult) AddFIR(senderSSRC uint32, mediaSSRC uint32, sequenceNumber uint8) error {
	if len(r.FIREntries) == cap(r.FIREntries) {
		return ErrResultFull
	}
	r.FIREntries = append(r.FIREntries, rtcp.FIREntry{
		SSRC:           mediaSSRC,
		SequenceNumber: sequenceNumber,
	})
	r.fir = rtcp.FullIntraRequest{
		SenderSSRC: senderSSRC,
		MediaSSRC:  mediaSSRC,
		FIR:        r.FIREntries[len(r.FIREntries)-1:],
	}
	return r.appendPacket(&r.fir)
}

func (r *FeedbackResult) appendNACKPair(pair rtcp.NackPair) error {
	if len(r.NACKPairs) == cap(r.NACKPairs) {
		return ErrResultFull
	}
	r.NACKPairs = append(r.NACKPairs, pair)
	return nil
}

func (r *FeedbackResult) appendPacket(packet rtcp.Packet) error {
	if len(r.Packets) == cap(r.Packets) {
		return ErrResultFull
	}
	r.Packets = append(r.Packets, packet)
	return nil
}
