package rtpav

import (
	"context"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
)

// JitterConfig sizes a JitterRing: Capacity is the reorder window in
// packets.
type JitterConfig struct {
	Capacity int
}

// JitterRing is a fixed-capacity, allocation-free JitterBuffer: packets slot
// by their distance from the next expected sequence number and release in
// order, with loss and reordering surfaced as events and stats. Positioning
// is relative to expected, never raw sequence modulo capacity, so any
// capacity orders correctly across the uint16 wrap.
type JitterRing struct {
	slots       []jitterSlot
	initialized bool
	ssrc        uint32
	expected    uint16
	// head is the slot holding expected; it moves only with expected.
	head  int
	stats JitterStats
}

type jitterSlot struct {
	seq    uint16
	packet *rtp.Packet
	full   bool
}

// NewJitterRing builds a ring with the configured reorder window.
func NewJitterRing(config JitterConfig) (*JitterRing, error) {
	if config.Capacity <= 0 {
		return nil, ErrInvalidJitterCapacity
	}
	return &JitterRing{
		slots: make([]jitterSlot, config.Capacity),
	}, nil
}

// Reset clears the ring for reuse with a new stream.
func (b *JitterRing) Reset() {
	for i := range b.slots {
		b.slots[i] = jitterSlot{}
	}
	b.initialized = false
	b.ssrc = 0
	b.expected = 0
	b.head = 0
	b.stats = JitterStats{}
}

// PushInto admits one packet and appends whatever became in-order to out,
// recording loss, lateness, and SSRC resets along the way.
func (b *JitterRing) PushInto(ctx context.Context, pkt *rtp.Packet, out *JitterResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var lossBefore bool
	var discontinuous bool
	if !b.initialized {
		b.init(pkt)
	}
	if pkt.SSRC != b.ssrc {
		b.Reset()
		b.init(pkt)
		discontinuous = true
		if err := appendJitterEvent(out, av.Event{Type: av.EventDiscontinuity}); err != nil {
			return err
		}
	}

	seq := pkt.SequenceNumber
	delta := seq - b.expected
	if delta >= 0x8000 {
		b.stats.Late++
		return nil
	}
	if int(delta) >= len(b.slots) {
		// Buffered packets are not lost: they arrived, and are discarded with the ring.
		if lost := int(delta) - b.buffered(); lost > 0 {
			b.stats.Lost += uint64(lost)
		}
		lossBefore = true
		if err := appendJitterEvent(out, av.Event{Type: av.EventPacketLoss}); err != nil {
			return err
		}
		b.dropUntil(seq)
	}

	slot := &b.slots[b.slotFor(seq)]
	if slot.full {
		b.stats.Reordered++
	} else {
		b.stats.Buffered++
	}
	slot.seq = seq
	slot.packet = pkt
	slot.full = true

	if err := b.drainReady(out); err != nil {
		return err
	}
	out.State = SequenceState{
		SSRC:          b.ssrc,
		Expected:      b.expected,
		LossBefore:    lossBefore,
		Discontinuous: discontinuous,
	}
	b.stats.Expected = b.expected
	b.stats.Buffered = b.buffered()
	return nil
}

// PopInto drains one ready in-order packet into pkt, reporting whether one
// was available.
func (b *JitterRing) PopInto(ctx context.Context, pkt *rtp.Packet) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !b.initialized {
		return false, nil
	}
	slot := &b.slots[b.head]
	if !slot.full || slot.seq != b.expected {
		return false, nil
	}
	*pkt = *slot.packet
	slot.full = false
	slot.packet = nil
	b.advance()
	b.stats.Expected = b.expected
	b.stats.Buffered = b.buffered()
	return true, nil
}

// FlushInto drains every buffered packet in sequence order at end of stream.
func (b *JitterRing) FlushInto(ctx context.Context, out *JitterResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Walk from head: slot order relative to head is sequence order. Gaps
	// are skipped, and expected ends past the last packet released.
	for remaining := b.buffered(); remaining > 0; b.advance() {
		slot := &b.slots[b.head]
		if !slot.full {
			continue
		}
		if err := appendReady(out, slot.packet); err != nil {
			return err
		}
		slot.full = false
		slot.packet = nil
		remaining--
	}
	b.stats.Expected = b.expected
	b.stats.Buffered = 0
	return nil
}

// Stats reports the ring's current counters.
func (b *JitterRing) Stats() JitterStats {
	stats := b.stats
	stats.SSRC = b.ssrc
	stats.Expected = b.expected
	stats.Buffered = b.buffered()
	return stats
}

func (b *JitterRing) init(pkt *rtp.Packet) {
	b.initialized = true
	b.ssrc = pkt.SSRC
	b.expected = pkt.SequenceNumber
	b.head = 0
}

func (b *JitterRing) dropUntil(seq uint16) {
	for i := range b.slots {
		b.slots[i] = jitterSlot{}
	}
	b.expected = seq
	b.head = 0
	b.stats.Buffered = 0
}

func (b *JitterRing) drainReady(out *JitterResult) error {
	for {
		slot := &b.slots[b.head]
		if !slot.full || slot.seq != b.expected {
			return nil
		}
		if err := appendReady(out, slot.packet); err != nil {
			return err
		}
		slot.full = false
		slot.packet = nil
		b.advance()
	}
}

// slotFor positions seq by its distance from expected; callers have checked
// that distance is inside the window.
func (b *JitterRing) slotFor(seq uint16) int {
	return (b.head + int(seq-b.expected)) % len(b.slots)
}

// advance moves expected and head together.
func (b *JitterRing) advance() {
	b.expected++
	b.head++
	if b.head == len(b.slots) {
		b.head = 0
	}
}

func (b *JitterRing) buffered() int {
	var count int
	for i := range b.slots {
		if b.slots[i].full {
			count++
		}
	}
	return count
}

func appendReady(out *JitterResult, pkt *rtp.Packet) error {
	if len(out.Ready) == cap(out.Ready) {
		return ErrResultFull
	}
	out.Ready = append(out.Ready, pkt)
	return nil
}

func appendJitterEvent(out *JitterResult, event av.Event) error {
	if len(out.Events) == cap(out.Events) {
		return ErrResultFull
	}
	out.Events = append(out.Events, event)
	return nil
}
