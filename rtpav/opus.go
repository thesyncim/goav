package rtpav

import (
	"context"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
)

const MIMEOpus = "audio/opus"

type OpusDepacketizer struct {
	stream av.Stream
}

func NewOpusDepacketizer(stream av.Stream) *OpusDepacketizer {
	if stream.Codec.ClockRate == 0 {
		stream.Codec.ClockRate = 48000
	}
	stream.Codec.ID = av.CodecOpus
	stream.Type = av.MediaAudio
	return &OpusDepacketizer{stream: stream}
}

func (d *OpusDepacketizer) Codec() av.CodecID {
	return av.CodecOpus
}

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
		CodecEpoch: d.stream.Epoch,
		Payload: av.Buffer{
			Bytes:     pkt.Payload,
			Ownership: av.BufferBorrowed,
		},
		PTS: av.RTPToTimestamp(pkt.Timestamp, clockRate),
	}
	out.Packets = append(out.Packets, packet)
	return nil
}

func (d *OpusDepacketizer) FlushInto(context.Context, *DepacketizeResult) error {
	return nil
}

func (d *OpusDepacketizer) HandleEvent(context.Context, *av.Event) error {
	return nil
}
