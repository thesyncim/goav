package rtpav

import "github.com/pion/rtcp"

// FeedbackResult is a caller-owned, allocation-free builder for outgoing
// RTCP feedback: AddNACK/AddPLI/AddFIR append into preallocated capacity and
// Packets is what a FeedbackWriter sends.
type FeedbackResult struct {
	Packets    []rtcp.Packet
	NACKPairs  []rtcp.NackPair
	FIREntries []rtcp.FIREntry

	nacks     [4]rtcp.TransportLayerNack
	nackCount int
	plis      [4]rtcp.PictureLossIndication
	pliCount  int
	firs      [4]rtcp.FullIntraRequest
	firCount  int
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
	for i := range r.nackCount {
		r.nacks[i] = rtcp.TransportLayerNack{}
	}
	for i := range r.pliCount {
		r.plis[i] = rtcp.PictureLossIndication{}
	}
	for i := range r.firCount {
		r.firs[i] = rtcp.FullIntraRequest{}
	}
	r.nackCount = 0
	r.pliCount = 0
	r.firCount = 0
}

// AddNACK appends one transport-layer NACK covering the missing sequence
// numbers (compressed into NACK pairs).
func (r *FeedbackResult) AddNACK(senderSSRC uint32, mediaSSRC uint32, missing []uint16) error {
	if len(missing) == 0 {
		return nil
	}
	if len(r.Packets) == cap(r.Packets) {
		return ErrResultFull
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
	pkt := rtcp.TransportLayerNack{
		SenderSSRC: senderSSRC,
		MediaSSRC:  mediaSSRC,
		Nacks:      r.NACKPairs[start:],
	}
	var ptr *rtcp.TransportLayerNack
	if r.nackCount < len(r.nacks) {
		r.nacks[r.nackCount] = pkt
		ptr = &r.nacks[r.nackCount]
		r.nackCount++
	} else {
		p := pkt
		ptr = &p
	}
	return r.appendPacket(ptr)
}

// AddPLI appends one picture-loss indication asking the sender for a new
// keyframe.
func (r *FeedbackResult) AddPLI(senderSSRC uint32, mediaSSRC uint32) error {
	if len(r.Packets) == cap(r.Packets) {
		return ErrResultFull
	}
	pkt := rtcp.PictureLossIndication{
		SenderSSRC: senderSSRC,
		MediaSSRC:  mediaSSRC,
	}
	var ptr *rtcp.PictureLossIndication
	if r.pliCount < len(r.plis) {
		r.plis[r.pliCount] = pkt
		ptr = &r.plis[r.pliCount]
		r.pliCount++
	} else {
		p := pkt
		ptr = &p
	}
	return r.appendPacket(ptr)
}

// AddFIR appends one full-intra request with the given FIR sequence number.
func (r *FeedbackResult) AddFIR(senderSSRC uint32, mediaSSRC uint32, sequenceNumber uint8) error {
	if len(r.Packets) == cap(r.Packets) || len(r.FIREntries) == cap(r.FIREntries) {
		return ErrResultFull
	}
	r.FIREntries = append(r.FIREntries, rtcp.FIREntry{
		SSRC:           mediaSSRC,
		SequenceNumber: sequenceNumber,
	})
	pkt := rtcp.FullIntraRequest{
		SenderSSRC: senderSSRC,
		MediaSSRC:  mediaSSRC,
		FIR:        r.FIREntries[len(r.FIREntries)-1:],
	}
	var ptr *rtcp.FullIntraRequest
	if r.firCount < len(r.firs) {
		r.firs[r.firCount] = pkt
		ptr = &r.firs[r.firCount]
		r.firCount++
	} else {
		p := pkt
		ptr = &p
	}
	return r.appendPacket(ptr)
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
