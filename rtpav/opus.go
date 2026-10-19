package rtpav

import (
	"context"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
)

// OpusDepacketizer turns Opus RTP payloads into av.Packets — one packet per
// RTP payload, stamped with the stream's clock-rate timing.
type OpusDepacketizer struct {
	stream av.Stream
}

// NewOpusDepacketizer builds a depacketizer for the given stream, defaulting
// the clock rate to 48000 when undeclared.
func NewOpusDepacketizer(stream av.Stream) *OpusDepacketizer {
	if stream.Codec.ClockRate == 0 {
		stream.Codec.ClockRate = 48000
	}
	stream.Codec.ID = av.CodecOpus
	stream.Type = av.MediaAudio
	return &OpusDepacketizer{stream: stream}
}

// Codec reports the codec this depacketizer reassembles.
func (d *OpusDepacketizer) Codec() av.CodecID {
	return av.CodecOpus
}

// PushInto converts one RTP packet into one Opus av.Packet appended to out.
func (d *OpusDepacketizer) PushInto(ctx context.Context, pkt *rtp.Packet, payload PayloadCodec, out *DepacketizeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	clockRate := payload.ClockRate
	if clockRate == 0 {
		clockRate = d.stream.Codec.ClockRate
	}
	if clockRate == 0 {
		clockRate = 48000
	}
	if len(out.Packets) == cap(out.Packets) {
		return ErrResultFull
	}
	packet := av.Packet{
		StreamID:   d.stream.ID,
		Type:       d.stream.Type,
		CodecEpoch: d.stream.Epoch,
		Payload: av.Buffer{
			Bytes: pkt.Payload,
		},
		PTS: av.RTPToTimestamp(pkt.Timestamp, clockRate),
	}
	out.Packets = append(out.Packets, packet)
	return nil
}

// FlushInto is a no-op: Opus payloads never span RTP packets.
func (d *OpusDepacketizer) FlushInto(context.Context, *DepacketizeResult) error {
	return nil
}

// HandleEvent observes in-band events; Opus depacketizing keeps no state to
// reset.
func (d *OpusDepacketizer) HandleEvent(ctx context.Context, event *av.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	applyCodecChangedEvent(&d.stream, av.CodecOpus, event)
	return nil
}

// opusPacketizer sends one Opus packet per RTP packet (RFC 7587): Opus
// frames never fragment, so a packet larger than the MTU is refused.
type opusPacketizer struct {
	budget int
	clock  uint32
}

func (p *opusPacketizer) Codec() av.CodecID { return av.CodecOpus }

func (p *opusPacketizer) PacketizeInto(ctx context.Context, packet *av.Packet, payload PayloadCodec, out *PacketizeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data := packet.Payload.Bytes
	if len(data) == 0 {
		return ErrInvalidPayload
	}
	if len(data) > p.budget {
		return ErrFrameTooLarge
	}
	ts, err := rtpTimestamp(packet, payload, p.clock)
	if err != nil {
		return err
	}
	out.open(payload.PayloadType, ts)
	out.write(data...)
	out.close()
	out.seal(false)
	return nil
}
