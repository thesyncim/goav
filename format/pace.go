package format

import (
	"context"
	"time"

	"github.com/thesyncim/goav/av"
)

// maxPacingSleep caps one pacing wait. A forward media-time gap larger than
// the cap (a sparse stream, a broken timeline) sleeps the cap once and
// re-anchors at the far side instead of stalling delivery for the whole gap;
// a packet more than the cap behind schedule re-anchors instead of letting
// every following packet burst out unpaced. Within the cap, late packets emit
// immediately (normal catch-up after a slow read) and the anchor holds.
const maxPacingSleep = time.Second

// demuxPacer paces a realtime demux pump on a clock: it anchors clock time t0
// to media time m0 and, before each packet at media time m, sleeps until
// t0 + (m-m0). Packets are paced on the emission timeline the demuxer already
// interleaved — never reordered — using DTS when the container carries one
// (the monotone decode timeline) and PTS otherwise.
//
// The pacer holds no rate: it paces media deltas 1:1 against whatever clock it
// is given. Playback rate lives on the task's shared timeline clock, which
// scales every paced consumer together; a rate change re-derives in-flight
// sleeps there, so the pacer never re-anchors for pacing changes.
//
// Concurrency: every field is Start-loop-local; Control communicates with the
// loop only through DemuxSource.pending. reset (seek/segment/discontinuity)
// clears the anchor so the next packet re-anchors instead of sleeping toward a
// stale timeline.
type demuxPacer struct {
	clock    av.Clock
	anchored bool
	wall0    time.Duration
	media0   time.Duration
}

func newDemuxPacer(clock av.Clock) *demuxPacer {
	return &demuxPacer{clock: clock}
}

// reset drops the anchor so the next packet re-anchors at its own media time.
// Only the Start loop calls it — after a seek/segment reposition and on a
// demuxer-reported discontinuity — so a jump never causes a giant sleep (new
// position far ahead) or an unpaced burst (new position behind).
func (p *demuxPacer) reset() {
	p.anchored = false
}

// pace blocks until the packet's media time is due on the clock. A packet
// without a comparable media time emits immediately — pacing cannot justify
// stalling media it cannot place on the timeline.
func (p *demuxPacer) pace(ctx context.Context, packet *av.Packet) error {
	media, ok := packetMediaTime(packet)
	if !ok {
		return nil
	}
	if !p.anchored {
		p.anchor(media)
		return nil
	}
	wait := p.wall0 + (media - p.media0) - p.clock.Now()
	switch {
	case wait > maxPacingSleep:
		// Pathological forward gap: sleep the cap, then treat the gap as a
		// timeline discontinuity and re-anchor at this packet.
		if err := p.clock.Sleep(ctx, maxPacingSleep); err != nil {
			return err
		}
		p.anchor(media)
		return nil
	case wait > 0:
		return p.clock.Sleep(ctx, wait)
	case wait < -maxPacingSleep:
		// Far behind schedule (a backward timeline jump the demuxer did not
		// flag): emit now and re-anchor so following packets pace normally.
		p.anchor(media)
		return nil
	default:
		// Slightly late: emit immediately and let delivery catch up against
		// the existing anchor.
		return nil
	}
}

func (p *demuxPacer) anchor(media time.Duration) {
	p.wall0 = p.clock.Now()
	p.media0 = media
	p.anchored = true
}

// packetMediaTime places a packet on the emission timeline: DTS when the
// container carries one (monotone under frame reordering), else PTS (the
// common case — matroska blocks carry presentation time only).
func packetMediaTime(packet *av.Packet) (time.Duration, bool) {
	if packet == nil {
		return 0, false
	}
	if dts, ok := packet.DTS.ToDuration(); ok {
		return dts, true
	}
	return packet.PTS.ToDuration()
}
