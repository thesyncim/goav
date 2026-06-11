package rtpav

import (
	"context"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
)

// The negotiated MIME type names payload maps and SDP carry for the
// supported codecs.
const (
	MIMEOpus = "audio/opus"
	MIMEVP8  = "video/vp8"
	MIMEVP9  = "video/vp9"
	MIMEH264 = "video/h264"
	MIMEAV1  = "video/av1"
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
			Bytes:     pkt.Payload,
			Ownership: av.BufferBorrowed,
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
